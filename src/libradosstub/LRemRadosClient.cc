// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemRadosClient.h"
#include "LRemIoCtxImpl.h"
#include "librados/AioCompletionImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "include/ceph_assert.h"
#include "common/ceph_json.h"
#include "common/Finisher.h"
#include "common/async/context_pool.h"
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <errno.h>

#include <atomic>
#include <functional>
#include <sstream>

static int get_concurrency() {
  int concurrency = 0;
  char *env = getenv("LIBRADOS_CONCURRENCY");
  if (env != NULL) {
    concurrency = atoi(env);
  }
  if (concurrency == 0) {
    concurrency = boost::thread::thread::hardware_concurrency();
  }
  if (concurrency == 0) {
    concurrency = 1;
  }
  return concurrency;
}

using namespace std::placeholders;

namespace librados {

namespace {

const char *config_keys[] = {
  "librados_thread_count",
  NULL
};

} // anonymous namespace

static void finish_aio_completion(AioCompletionImpl *c, int r) {
  c->lock.lock();
  c->complete = true;
  c->rval = r;
  c->lock.unlock();

  rados_callback_t cb_complete = c->callback_complete;
  void *cb_complete_arg = c->callback_complete_arg;
  if (cb_complete) {
    cb_complete(c, cb_complete_arg);
  }

  rados_callback_t cb_safe = c->callback_safe;
  void *cb_safe_arg = c->callback_safe_arg;
  if (cb_safe) {
    cb_safe(c, cb_safe_arg);
  }

  c->lock.lock();
  c->callback_complete = NULL;
  c->callback_safe = NULL;
  c->cond.notify_all();
  c->put_unlock();
}

class AioFunctionContext : public Context {
public:
  AioFunctionContext(const LRemRadosClient::AioFunction &callback,
                     Finisher *finisher, AioCompletionImpl *c)
    : m_callback(callback), m_finisher(finisher), m_comp(c)
  {
    if (m_comp != NULL) {
      m_comp->get();
    }
  }

  void finish(int r) override {
    int ret = m_callback();
    if (m_comp != NULL) {
      if (m_finisher != NULL) {
        m_finisher->queue(new LambdaContext(std::bind(
          &finish_aio_completion, m_comp, ret)));
      } else {
        finish_aio_completion(m_comp, ret);
      }
    }
  }
private:
  LRemRadosClient::AioFunction m_callback;
  Finisher *m_finisher;
  AioCompletionImpl *m_comp;
};

static void finish_pool_aio_completion(PoolAsyncCompletionImpl *c, int r) {
  std::unique_lock l(c->lock);
  c->rval = r;
  c->done = true;
  c->cond.notify_all();

  if (c->callback) {
    rados_callback_t cb = c->callback;
    c->callback = nullptr;
    void *cb_arg = c->callback_arg;
    l.unlock();
    cb(c, cb_arg);
    l.lock();
  }
}

class PoolAioFunctionContext : public Context {
public:
  PoolAioFunctionContext(const LRemRadosClient::AioFunction &callback,
                     Finisher *finisher, PoolAsyncCompletionImpl *c)
    : m_callback(callback), m_finisher(finisher), m_comp(c)
  {
    if (m_comp != NULL) {
      m_comp->get();
    }
  }

  void finish(int r) override {
    int ret = m_callback();
    if (m_comp != NULL) {
      if (m_finisher != NULL) {
        m_finisher->queue(new LambdaContext(std::bind(
          &finish_pool_aio_completion, m_comp, ret)));
      } else {
        finish_pool_aio_completion(m_comp, ret);
      }
    }
  }
private:
  LRemRadosClient::AioFunction m_callback;
  Finisher *m_finisher;
  PoolAsyncCompletionImpl *m_comp;
};

LRemRadosClient::LRemRadosClient(CephContext *cct,
                                 LRemWatchNotify *watch_notify)
  : m_cct(cct->get()), m_watch_notify(watch_notify),
    m_aio_finisher(new Finisher(m_cct)),
    m_io_context_pool(std::make_unique<ceph::async::io_context_pool>())
{
  get();

  // simulate multiple OSDs
  int concurrency = get_concurrency();
  for (int i = 0; i < concurrency; ++i) {
    m_finishers.push_back(new Finisher(m_cct));
    m_finishers.back()->start();
  }

  // replicate AIO callback processingyy
  m_aio_finisher->start();

  // finisher for pool aio ops
  m_pool_finisher = new Finisher(cct);
  m_pool_finisher->start();

  // replicate neorados callback processing
  m_cct->_conf.add_observer(this);
  m_io_context_pool->start(m_cct->_conf.get_val<uint64_t>(
    "librados_thread_count"));
}

LRemRadosClient::~LRemRadosClient() {
  flush_aio_operations();
  flush_pool_aio_operations();

  for (size_t i = 0; i < m_finishers.size(); ++i) {
    m_finishers[i]->stop();
    delete m_finishers[i];
  }

  m_pool_finisher->stop();
  delete m_pool_finisher;

  m_aio_finisher->stop();
  delete m_aio_finisher;

  m_cct->_conf.remove_observer(this);
  m_io_context_pool->stop();

  m_cct->put();
  m_cct = NULL;
}

boost::asio::io_context& LRemRadosClient::get_io_context() {
  return m_io_context_pool->get_io_context();
}

const char** LRemRadosClient::get_tracked_conf_keys() const {
  return config_keys;
}

void LRemRadosClient::handle_conf_change(
    const ConfigProxy& conf, const std::set<std::string> &changed) {
  if (changed.count("librados_thread_count")) {
    m_io_context_pool->stop();
    m_io_context_pool->start(conf.get_val<std::uint64_t>(
      "librados_thread_count"));
  }
}

void LRemRadosClient::get() {
  m_refcount++;
}

void LRemRadosClient::put() {
  if (--m_refcount == 0) {
    shutdown();
    delete this;
  }
}

CephContext *LRemRadosClient::cct() {
  return m_cct;
}

int LRemRadosClient::connect() {
  return 0;
}

void LRemRadosClient::shutdown() {
}

int LRemRadosClient::wait_for_latest_osdmap() {
  return 0;
}

int LRemRadosClient::mon_command(const std::vector<std::string>& cmd,
                                 const bufferlist &inbl,
                                 bufferlist *outbl, std::string *outs) {
  for (std::vector<std::string>::const_iterator it = cmd.begin();
       it != cmd.end(); ++it) {
    JSONParser parser;
    if (!parser.parse(it->c_str(), it->length())) {
      return -EINVAL;
    }

    JSONObjIter j_it = parser.find("prefix");
    if (j_it.end()) {
      return -EINVAL;
    }

    if ((*j_it)->get_data() == "osd tier add") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier cache-mode") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier set-overlay") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier remove-overlay") {
      return 0;
    } else if ((*j_it)->get_data() == "osd tier remove") {
      return 0;
    } else if ((*j_it)->get_data() == "config-key rm") {
      return 0;
    } else if ((*j_it)->get_data() == "config set") {
      return 0;
    } else if ((*j_it)->get_data() == "df") {
      std::stringstream str;
      str << R"({"pools": [)";

      std::list<std::pair<int64_t, std::string>> pools;
      pool_list(pools);
      for (auto& pool : pools) {
        if (pools.begin()->first != pool.first) {
          str << ",";
        }
        str << R"({"name": ")" << pool.second << R"(", "stats": )"
            << R"({"percent_used": 1.0, "bytes_used": 0, "max_avail": 0}})";
      }

      str << "]}";
      outbl->append(str.str());
      return 0;
    } else if ((*j_it)->get_data() == "osd blocklist") {
      auto op_it = parser.find("blocklistop");
      if (!op_it.end() && (*op_it)->get_data() == "add") {
        uint32_t expire = 0;
        auto expire_it = parser.find("expire");
        if (!expire_it.end()) {
          expire = boost::lexical_cast<uint32_t>((*expire_it)->get_data());
        }

        auto addr_it = parser.find("addr");
        return blocklist_add((*addr_it)->get_data(), expire);
      }
    }
  }
  return -ENOSYS;
}

int LRemRadosClient::pool_create_async(const char *name, PoolAsyncCompletionImpl *c) {
  add_pool_aio_operation(true,
                         std::bind(&LRemRadosClient::pool_create, this, name), c);
  return 0;
}

void LRemRadosClient::add_aio_operation(const std::string& oid,
                                        bool queue_callback,
				        const AioFunction &aio_function,
                                        AioCompletionImpl *c) {
  AioFunctionContext *ctx = new AioFunctionContext(
    aio_function, queue_callback ? m_aio_finisher : NULL, c);
  get_finisher(oid)->queue(ctx);
}

void LRemRadosClient::add_pool_aio_operation(bool queue_callback,
                                             const AioFunction &aio_function,
                                             PoolAsyncCompletionImpl *c) {
  PoolAioFunctionContext *ctx = new PoolAioFunctionContext(
    aio_function, queue_callback ? m_aio_finisher : NULL, c);
  m_pool_finisher->queue(ctx);
}

struct WaitForFlush {
  int flushed() {
    if (--count == 0) {
      aio_finisher->queue(new LambdaContext(std::bind(
        &finish_aio_completion, c, 0)));
      delete this;
    }
    return 0;
  }

  std::atomic<int64_t> count = { 0 };
  Finisher *aio_finisher;
  AioCompletionImpl *c;
};

void LRemRadosClient::flush_aio_operations() {
  AioCompletionImpl *comp = new AioCompletionImpl();
  flush_aio_operations(comp);
  comp->wait_for_complete();
  comp->put();
}

void LRemRadosClient::flush_aio_operations(AioCompletionImpl *c) {
  c->get();

  WaitForFlush *wait_for_flush = new WaitForFlush();
  wait_for_flush->count = m_finishers.size();
  wait_for_flush->aio_finisher = m_aio_finisher;
  wait_for_flush->c = c;

  for (size_t i = 0; i < m_finishers.size(); ++i) {
    AioFunctionContext *ctx = new AioFunctionContext(
      std::bind(&WaitForFlush::flushed, wait_for_flush),
      nullptr, nullptr);
    m_finishers[i]->queue(ctx);
  }
}

struct WaitForPoolFlush {
  int flushed() {
    aio_finisher->queue(new LambdaContext(std::bind( &finish_pool_aio_completion, c, 0)));
    delete this;
    return 0;
  }

  Finisher *aio_finisher;
  PoolAsyncCompletionImpl *c;
};

void LRemRadosClient::flush_pool_aio_operations() {
  PoolAsyncCompletionImpl *comp = new PoolAsyncCompletionImpl();
  flush_pool_aio_operations(comp);
  comp->wait();
  comp->put();
}

void LRemRadosClient::flush_pool_aio_operations(PoolAsyncCompletionImpl *c) {
  c->get();

  auto *wait_for_flush = new WaitForPoolFlush();
  wait_for_flush->aio_finisher = m_aio_finisher;
  wait_for_flush->c = c;

  PoolAioFunctionContext *ctx = new PoolAioFunctionContext(
    std::bind(&WaitForPoolFlush::flushed, wait_for_flush),
    nullptr, nullptr);
  m_pool_finisher->queue(ctx);
}

int LRemRadosClient::aio_watch_flush(AioCompletionImpl *c) {
  c->get();
  Context *ctx = new LambdaContext(std::bind(
    &LRemRadosClient::finish_aio_completion, this, c, std::placeholders::_1));
  get_watch_notify()->aio_flush(this, ctx);
  return 0;
}

void LRemRadosClient::finish_aio_completion(AioCompletionImpl *c, int r) {
  librados::finish_aio_completion(c, r);
}

Finisher *LRemRadosClient::get_finisher(const std::string &oid) {
  std::size_t h = m_hash(oid);
  return m_finishers[h % m_finishers.size()];
}

} // namespace librados