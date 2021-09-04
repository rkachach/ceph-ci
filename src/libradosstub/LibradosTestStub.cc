// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LibradosTestStub.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/snap_types.h"
#include "common/armor.h"
#include "librados/AioCompletionImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "log/Log.h"
#include "TestClassHandler.h"
#include "TestIoCtxImpl.h"
#include "TestRadosClient.h"
#include "TestMemCluster.h"
#include "TestMemRadosClient.h"
#include "objclass/objclass.h"
#include "osd/osd_types.h"
#include <arpa/inet.h>
#include <boost/shared_ptr.hpp>
#include <deque>
#include <functional>
#include <list>
#include <vector>
#include "include/ceph_assert.h"
#include "include/compat.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rados

namespace librados {

MockTestMemIoCtxImpl &get_mock_io_ctx(IoCtx &ioctx) {
  MockTestMemIoCtxImpl **mock =
    reinterpret_cast<MockTestMemIoCtxImpl **>(&ioctx);
  return **mock;
}

} // namespace librados

namespace librados_stub {

TestClusterRef &cluster() {
  static TestClusterRef s_cluster;
  return s_cluster;
}

void set_cluster(TestClusterRef cluster_ref) {
  cluster() = cluster_ref;
}

TestClusterRef get_cluster() {
  auto &cluster_ref = cluster();
  if (cluster_ref.get() == nullptr) {
    cluster_ref.reset(new librados::TestMemCluster());
  }
  return cluster_ref;
}

librados::TestClassHandler *get_class_handler() {
  static boost::shared_ptr<librados::TestClassHandler> s_class_handler;
  if (!s_class_handler) {
    s_class_handler.reset(new librados::TestClassHandler());
    s_class_handler->open_all_classes();
  }
  return s_class_handler.get();
}

} // namespace librados_stub

namespace {

void do_out_buffer(bufferlist& outbl, char **outbuf, size_t *outbuflen) {
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen) {
    *outbuflen = outbl.length();
  }
}

void do_out_buffer(string& outbl, char **outbuf, size_t *outbuflen) {
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen) {
    *outbuflen = outbl.length();
  }
}

librados::TestRadosClient *create_rados_client() {
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf.parse_env(cct->get_module_type());
  cct->_conf.apply_changes(nullptr);
  cct->_log->start();

  auto rados_client =
    librados_stub::get_cluster()->create_rados_client(cct);
  cct->put();
  return rados_client;
}

} // anonymous namespace

#if defined(HAVE_ASM_SYMVER) || defined(HAVE_ATTR_SYMVER)
// prefer __attribute__() over global asm(".symver"). because the latter
// is not parsed by the compiler and is partitioned away by GCC if
// lto-partitions is enabled, in other words, these asm() statements
// are dropped by the -flto option by default. the way to address it is
// to use __attribute__. so this information can be processed by the
// C compiler, and be preserved after LTO partitions the code
#ifdef HAVE_ATTR_SYMVER
#define LIBRADOS_C_API_BASE(fn)               \
  extern __typeof (_##fn##_base) _##fn##_base __attribute__((__symver__ (#fn "@")))
#define LIBRADOS_C_API_BASE_DEFAULT(fn)       \
  extern __typeof (_##fn) _##fn __attribute__((__symver__ (#fn "@@")))
#define LIBRADOS_C_API_DEFAULT(fn, ver)       \
  extern __typeof (_##fn) _##fn __attribute__((__symver__ (#fn "@@LIBRADOS_" #ver)))
#else
#define LIBRADOS_C_API_BASE(fn)               \
  asm(".symver _" #fn "_base, " #fn "@")
#define LIBRADOS_C_API_BASE_DEFAULT(fn)       \
  asm(".symver _" #fn ", " #fn "@@")
#define LIBRADOS_C_API_DEFAULT(fn, ver)       \
  asm(".symver _" #fn ", " #fn "@@LIBRADOS_" #ver)
#endif

#define LIBRADOS_C_API_BASE_F(fn) _ ## fn ## _base
#define LIBRADOS_C_API_DEFAULT_F(fn) _ ## fn

#else
#define LIBRADOS_C_API_BASE(fn)
#define LIBRADOS_C_API_BASE_DEFAULT(fn)
#define LIBRADOS_C_API_DEFAULT(fn, ver)

#define LIBRADOS_C_API_BASE_F(fn) _ ## fn ## _base
// There shouldn't be multiple default versions of the same
// function.
#define LIBRADOS_C_API_DEFAULT_F(fn) fn
#endif

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_create_completion2)(void *cb_arg,
					    rados_callback_t cb_complete,
					    rados_completion_t *pc)
{
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete) {
    c->set_complete_callback(cb_arg, cb_complete);
  }
  *pc = c;
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_create_completion2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_get_return_value)(rados_completion_t c) {
  return reinterpret_cast<librados::AioCompletionImpl*>(c)->get_return_value();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_get_return_value);

extern "C" rados_config_t LIBRADOS_C_API_DEFAULT_F(rados_cct)(rados_t cluster)
{
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  return reinterpret_cast<rados_config_t>(client->cct());
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cct);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_set)(rados_t cluster, const char *option,
                              const char *value) {
  librados::TestRadosClient *impl =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  CephContext *cct = impl->cct();
  return cct->_conf.set_val(option, value);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_set);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_parse_env)(rados_t cluster, const char *var) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  auto& conf = client->cct()->_conf;
  conf.parse_env(client->cct()->get_module_type(), var);
  conf.apply_changes(NULL);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_parse_env);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_read_file)(rados_t cluster, const char *path) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  auto& conf = client->cct()->_conf;
  int ret = conf.parse_config_files(path, NULL, 0);
  if (ret == 0) {
    conf.parse_env(client->cct()->get_module_type());
    conf.apply_changes(NULL);
    conf.complain_about_parse_error(client->cct());
  } else if (ret == -ENOENT) {
    // ignore missing client config
    return 0;
  }
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_read_file);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_connect)(rados_t cluster) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  return client->connect();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_connect);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create)(rados_t *cluster, const char * const id) {
  *cluster = create_rados_client();
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create_with_context)(rados_t *cluster,
                                         rados_config_t cct_) {
  auto cct = reinterpret_cast<CephContext*>(cct_);
  *cluster = librados_stub::get_cluster()->create_rados_client(cct);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create_with_context);

extern "C" rados_config_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_cct)(rados_ioctx_t ioctx)
{
  librados::TestIoCtxImpl *ctx =
    reinterpret_cast<librados::TestIoCtxImpl*>(ioctx);
  return reinterpret_cast<rados_config_t>(ctx->get_rados_client()->cct());
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_cct);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_create)(rados_t cluster, const char *pool_name,
                                  rados_ioctx_t *ioctx) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);

  int64_t pool_id = client->pool_lookup(pool_name);
  if (pool_id < 0) {
    return static_cast<int>(pool_id);
  }

  *ioctx = reinterpret_cast<rados_ioctx_t>(
      client->create_ioctx(pool_id, pool_name));
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_create2)(rados_t cluster, int64_t pool_id,
                                   rados_ioctx_t *ioctx)
{
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);

  std::list<std::pair<int64_t, std::string> > pools;
  int r = client->pool_list(pools);
  if (r < 0) {
    return r;
  }

  for (std::list<std::pair<int64_t, std::string> >::iterator it =
       pools.begin(); it != pools.end(); ++it) {
    if (it->first == pool_id) {
      *ioctx = reinterpret_cast<rados_ioctx_t>(
	client->create_ioctx(pool_id, it->second));
      return 0;
    }
  }
  return -ENOENT;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_create2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_ioctx_destroy)(rados_ioctx_t io) {
  librados::TestIoCtxImpl *ctx =
    reinterpret_cast<librados::TestIoCtxImpl*>(io);
  ctx->put();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_destroy);

extern "C" rados_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_get_cluster)(rados_ioctx_t io) {
  librados::TestIoCtxImpl *ctx =
    reinterpret_cast<librados::TestIoCtxImpl*>(io);
  return reinterpret_cast<rados_t>(ctx->get_rados_client());
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_get_cluster);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_mon_command)(rados_t cluster, const char **cmd,
                                 size_t cmdlen, const char *inbuf,
                                 size_t inbuflen, char **outbuf,
                                 size_t *outbuflen, char **outs,
                                 size_t *outslen) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);

  vector<string> cmdvec;
  for (size_t i = 0; i < cmdlen; i++) {
    cmdvec.push_back(cmd[i]);
  }

  bufferlist inbl;
  inbl.append(inbuf, inbuflen);

  bufferlist outbl;
  string outstring;
  int ret = client->mon_command(cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_mon_command);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_open)(rados_ioctx_t io,
                                        rados_list_ctx_t *ctx) {
  librados::TestIoCtxImpl *io_ctx =
    reinterpret_cast<librados::TestIoCtxImpl*>(io);
  librados::TestRadosClient *client = io_ctx->get_rados_client();

  std::list<librados::TestRadosClient::Object> *list =
    new std::list<librados::TestRadosClient::Object>();
  
  client->object_list(io_ctx->get_id(), list);
  list->push_front(librados::TestRadosClient::Object());
  *ctx = reinterpret_cast<rados_list_ctx_t>(list);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_open);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_next)(rados_list_ctx_t ctx,
                                        const char **entry,
                                        const char **key,
                                        const char **nspace) {
  std::list<librados::TestRadosClient::Object> *list =
    reinterpret_cast<std::list<librados::TestRadosClient::Object> *>(ctx);
  if (!list->empty()) {
    list->pop_front();
  }
  if (list->empty()) {
    return -ENOENT;
  }

  librados::TestRadosClient::Object &obj = list->front();
  if (entry != NULL) {
    *entry = obj.oid.c_str();
  }
  if (key != NULL) {
    *key = obj.locator.c_str();
  }
  if (nspace != NULL) {
    *nspace = obj.nspace.c_str();
  }
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_next);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_close)(rados_list_ctx_t ctx) {
  std::list<librados::TestRadosClient::Object> *list =
    reinterpret_cast<std::list<librados::TestRadosClient::Object> *>(ctx);
  delete list;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_close);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_create)(rados_t cluster, const char *pool_name) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  return client->pool_create(pool_name);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_delete)(rados_t cluster, const char *pool_name) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  return client->pool_delete(pool_name);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_delete);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_shutdown)(rados_t cluster) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  client->put();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_shutdown);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_wait_for_latest_osdmap)(rados_t cluster) {
  librados::TestRadosClient *client =
    reinterpret_cast<librados::TestRadosClient*>(cluster);
  return client->wait_for_latest_osdmap();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_wait_for_latest_osdmap);

using namespace std::placeholders;

namespace librados {

librados::PoolAsyncCompletion::PoolAsyncCompletion::~PoolAsyncCompletion()
{
  auto c = reinterpret_cast<PoolAsyncCompletionImpl *>(pc);
  c->release();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::set_callback(void *cb_arg,
                                                                     rados_callback_t cb)
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->set_callback(cb_arg, cb);
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::wait()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->wait();
}

bool librados::PoolAsyncCompletion::PoolAsyncCompletion::is_complete()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->is_complete();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::get_return_value()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->get_return_value();
}

void librados::PoolAsyncCompletion::PoolAsyncCompletion::release()
{
  delete this;
}

AioCompletion::~AioCompletion()
{
  auto c = reinterpret_cast<AioCompletionImpl *>(pc);
  c->release();
}

void AioCompletion::release() {
  delete this;
}

int AioCompletion::AioCompletion::get_return_value()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_return_value();
}

int AioCompletion::AioCompletion::wait_for_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete();
}


IoCtx::IoCtx() : io_ctx_impl(NULL) {
}

IoCtx::~IoCtx() {
  close();
}

IoCtx::IoCtx(const IoCtx& rhs) {
  io_ctx_impl = rhs.io_ctx_impl;
  if (io_ctx_impl) {
    TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
    ctx->get();
  }
}

IoCtx::IoCtx(IoCtx&& rhs) noexcept : io_ctx_impl(std::exchange(rhs.io_ctx_impl, nullptr))
{
}

IoCtx& IoCtx::operator=(const IoCtx& rhs) {
  if (io_ctx_impl) {
    TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
    ctx->put();
  }

  io_ctx_impl = rhs.io_ctx_impl;
  if (io_ctx_impl) {
    TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
    ctx->get();
  }
  return *this;
}

librados::IoCtx& librados::IoCtx::operator=(IoCtx&& rhs) noexcept
{
  if (io_ctx_impl) {
    TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
    ctx->put();
  }

  io_ctx_impl = std::exchange(rhs.io_ctx_impl, nullptr);
  return *this;
}

int IoCtx::aio_flush() {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->aio_flush();
  return 0;
}

int IoCtx::aio_flush_async(AioCompletion *c) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->aio_flush_async(c->pc);
  return 0;
}

int IoCtx::aio_notify(const std::string& oid, AioCompletion *c, bufferlist& bl,
                      uint64_t timeout_ms, bufferlist *pbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->aio_notify(oid, c->pc, bl, timeout_ms, pbl);
  return 0;
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectReadOperation *op, bufferlist *pbl) {
  return aio_operate(oid, c, op, 0, pbl);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectReadOperation *op, int flags,
                       bufferlist *pbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->aio_operate_read(oid, *ops, c->pc, flags, pbl,
                               ctx->get_snap_read(), nullptr);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectReadOperation *op, int flags,
                       bufferlist *pbl, const blkin_trace_info *trace_info) {
  return aio_operate(oid, c, op, flags, pbl);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, int flags) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->aio_operate(oid, *ops, c->pc, NULL, flags);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->aio_operate(oid, *ops, c->pc, NULL, 0);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, snap_t seq,
                       std::vector<snap_t>& snaps, int flags,
                       const blkin_trace_info *trace_info) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);

  std::vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (size_t i = 0; i < snaps.size(); ++i)
    snv[i] = snaps[i];
  SnapContext snapc(seq, snv);

  return ctx->aio_operate(oid, *ops, c->pc, &snapc, flags);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, snap_t seq,
                       std::vector<snap_t>& snaps) {
  return aio_operate(oid, c, op, seq, snaps, 0, nullptr);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, snap_t seq,
                       std::vector<snap_t>& snaps,
		       const blkin_trace_info *trace_info) {
  return aio_operate(oid, c, op, seq, snaps, 0, trace_info);
}

int IoCtx::aio_append(const std::string& oid, librados::AioCompletion *c,
                      const bufferlist& bl, size_t len)
{
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_append(oid, c->pc, bl, len);
}

int IoCtx::aio_remove(const std::string& oid, AioCompletion *c) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_remove(oid, c->pc);
}

int IoCtx::aio_remove(const std::string& oid, AioCompletion *c, int flags) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_remove(oid, c->pc, flags);
}

int IoCtx::aio_watch(const std::string& o, AioCompletion *c, uint64_t *handle,
                     librados::WatchCtx2 *watch_ctx) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_watch(o, c->pc, handle, watch_ctx);
}

int IoCtx::aio_unwatch(uint64_t handle, AioCompletion *c) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_unwatch(handle, c->pc);
}

int IoCtx::aio_exec(const std::string& oid, AioCompletion *c,
                    const char *cls, const char *method,
                    bufferlist& inbl, bufferlist *outbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_exec(oid, c->pc, librados_stub::get_class_handler(), 
                       cls, method, inbl, outbl);
}

config_t IoCtx::cct() {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return reinterpret_cast<config_t>(ctx->get_rados_client()->cct());
}

void IoCtx::close() {
  if (io_ctx_impl) {
    TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
    ctx->put();
  }
  io_ctx_impl = NULL;
}

int IoCtx::create(const std::string& oid, bool exclusive) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::create, _1, _2, exclusive,
                     ctx->get_snap_context()));
}

void IoCtx::dup(const IoCtx& rhs) {
  close();
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(rhs.io_ctx_impl);
  io_ctx_impl = reinterpret_cast<IoCtxImpl*>(ctx->clone());
}

int IoCtx::exec(const std::string& oid, const char *cls, const char *method,
                bufferlist& inbl, bufferlist& outbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  auto trans = make_op_transaction();
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::exec, _1, _2,
                     librados_stub::get_class_handler(), cls,
                     method, inbl, &outbl, ctx->get_snap_read(),
                     ctx->get_snap_context(), trans));
}

void IoCtx::from_rados_ioctx_t(rados_ioctx_t p, IoCtx &io) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(p);
  ctx->get();

  io.close();
  io.io_ctx_impl = reinterpret_cast<IoCtxImpl*>(ctx);
}

uint64_t IoCtx::get_instance_id() const {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->get_instance_id();
}

int64_t IoCtx::get_id() {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->get_id();
}

uint64_t IoCtx::get_last_version() {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->get_last_version();
}

std::string IoCtx::get_pool_name() {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->get_pool_name();
}
bool IoCtx::pool_requires_alignment() {
  return false;
}
int IoCtx::pool_requires_alignment2(bool * req) {
  *req = pool_requires_alignment();
  return 0;
}
uint64_t IoCtx::pool_required_alignment() {
  return 0;
}
int IoCtx::pool_required_alignment2(uint64_t * alignment) {
  *alignment = pool_required_alignment();
  return 0;
}

int IoCtx::list_snaps(const std::string& o, snap_set_t *out_snaps) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    o, std::bind(&TestIoCtxImpl::list_snaps, _1, _2, out_snaps));
}

int IoCtx::list_watchers(const std::string& o,
                         std::list<obj_watch_t> *out_watchers) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    o, std::bind(&TestIoCtxImpl::list_watchers, _1, _2, out_watchers));
}

int IoCtx::notify(const std::string& o, uint64_t ver, bufferlist& bl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->notify(o, bl, 0, NULL);
}

int IoCtx::notify2(const std::string& o, bufferlist& bl,
                   uint64_t timeout_ms, bufferlist *pbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->notify(o, bl, timeout_ms, pbl);
}

void IoCtx::notify_ack(const std::string& o, uint64_t notify_id,
                       uint64_t handle, bufferlist& bl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->notify_ack(o, notify_id, handle, bl);
}

int IoCtx::omap_get_vals(const std::string& oid,
                         const std::string& start_after,
                         uint64_t max_return,
                         std::map<std::string, bufferlist> *out_vals) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::omap_get_vals, _1, _2, start_after, "",
                     max_return, out_vals));
}
int IoCtx::omap_get_vals_by_keys(const std::string& oid,
                                 const std::set<std::string>& keys,
                                 std::map<std::string, bufferlist> *vals) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::omap_get_vals_by_keys, _1, _2, keys, vals));
}
int IoCtx::omap_set(const std::string& oid,
                    const std::map<std::string, bufferlist>& m) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::omap_set, _1, _2, m));
}

int IoCtx::omap_rm_keys(const std::string& oid,
                        const std::set<std::string>& keys) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::omap_rm_keys, _1, _2, keys));
}

int IoCtx::omap_clear(const std::string& oid) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::omap_clear, _1, _2));
}

int IoCtx::operate(const std::string& oid, ObjectWriteOperation *op) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->operate(oid, *ops, 0);
}

int IoCtx::operate(const std::string& oid, ObjectWriteOperation *op, int flags) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->operate(oid, *ops, flags);
}

int IoCtx::operate(const std::string& oid, ObjectReadOperation *op,
                   bufferlist *pbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->operate_read(oid, *ops, pbl, 0);
}
int IoCtx::operate(const std::string& oid, ObjectReadOperation *op,
                   bufferlist *pbl, int flags) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  TestObjectOperationImpl *ops = reinterpret_cast<TestObjectOperationImpl*>(op->impl);
  return ctx->operate_read(oid, *ops, pbl, flags);
}

int IoCtx::read(const std::string& oid, bufferlist& bl, size_t len,
                uint64_t off) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::read, _1, _2, len, off, &bl,
                     ctx->get_snap_read(), nullptr));
}

int IoCtx::remove(const std::string& oid) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::remove, _1, _2, ctx->get_snap_context()));
}

int IoCtx::selfmanaged_snap_create(uint64_t *snapid) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->selfmanaged_snap_create(snapid);
}

void IoCtx::aio_selfmanaged_snap_create(uint64_t *snapid, AioCompletion* c) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->aio_selfmanaged_snap_create(snapid, c->pc);
}

int IoCtx::selfmanaged_snap_remove(uint64_t snapid) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->selfmanaged_snap_remove(snapid);
}

void IoCtx::aio_selfmanaged_snap_remove(uint64_t snapid, AioCompletion* c) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->aio_selfmanaged_snap_remove(snapid, c->pc);
}

int IoCtx::selfmanaged_snap_rollback(const std::string& oid,
                                     uint64_t snapid) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->selfmanaged_snap_rollback(oid, snapid);
}

int IoCtx::selfmanaged_snap_set_write_ctx(snap_t seq,
                                          std::vector<snap_t>& snaps) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->selfmanaged_snap_set_write_ctx(seq, snaps);
}

void IoCtx::snap_set_read(snap_t seq) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->set_snap_read(seq);
}

int IoCtx::sparse_read(const std::string& oid, std::map<uint64_t,uint64_t>& m,
                       bufferlist& bl, size_t len, uint64_t off) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::sparse_read, _1, _2, off, len, &m, &bl,
                     ctx->get_snap_read()));
}

int IoCtx::stat(const std::string& oid, uint64_t *psize, time_t *pmtime) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::stat, _1, _2, psize, pmtime));
}

int IoCtx::stat2(const std::string& oid, uint64_t *psize, struct timespec *pts) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::stat2, _1, _2, psize, pts));
}

int IoCtx::tmap_update(const std::string& oid, bufferlist& cmdbl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::tmap_update, _1, _2, cmdbl));
}

int IoCtx::trunc(const std::string& oid, uint64_t off) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::truncate, _1, _2, off,
                     ctx->get_snap_context()));
}

int IoCtx::unwatch2(uint64_t handle) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->unwatch(handle);
}

int IoCtx::unwatch(const std::string& o, uint64_t handle) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->unwatch(handle);
}

int IoCtx::watch(const std::string& o, uint64_t ver, uint64_t *handle,
                 librados::WatchCtx *wctx) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->watch(o, handle, wctx, NULL);
}

int IoCtx::watch2(const std::string& o, uint64_t *handle,
                  librados::WatchCtx2 *wctx) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->watch(o, handle, NULL, wctx);
}

int IoCtx::write(const std::string& oid, bufferlist& bl, size_t len,
                 uint64_t off) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::write, _1, _2, bl, len, off,
                     ctx->get_snap_context()));
}

int IoCtx::write_full(const std::string& oid, bufferlist& bl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::write_full, _1, _2, bl,
                     ctx->get_snap_context()));
}

int IoCtx::writesame(const std::string& oid, bufferlist& bl, size_t len,
                     uint64_t off) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::writesame, _1, _2, bl, len, off,
                     ctx->get_snap_context()));
}

int IoCtx::cmpext(const std::string& oid, uint64_t off, bufferlist& cmp_bl) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&TestIoCtxImpl::cmpext, _1, _2, off, cmp_bl,
                     ctx->get_snap_read()));
}

int IoCtx::application_enable(const std::string& app_name, bool force) {
  return 0;
}

int IoCtx::application_enable_async(const std::string& app_name,
                                    bool force, PoolAsyncCompletion *c) {
  return -EOPNOTSUPP;
}

int IoCtx::application_list(std::set<std::string> *app_names) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_get(const std::string& app_name,
                                    const std::string &key,
                                    std::string *value) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_set(const std::string& app_name,
                                    const std::string &key,
                                    const std::string& value) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_remove(const std::string& app_name,
                                       const std::string &key) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_list(const std::string& app_name,
                                     std::map<std::string, std::string> *values) {
  return -EOPNOTSUPP;
}

void IoCtx::locator_set_key(const std::string& key) {
#warning locator missing implementation
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->locator_set_key(key);
}

void IoCtx::set_namespace(const std::string& nspace) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  ctx->set_namespace(nspace);
}

std::string IoCtx::get_namespace() const {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(io_ctx_impl);
  return ctx->get_namespace();
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
    const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.get_next();
  return iter;
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
  uint32_t pos, const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.seek(pos);
  return iter;
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
  const ObjectCursor& cursor, const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.seek(cursor);
  return iter;
}

const librados::NObjectIterator& librados::IoCtx::nobjects_end() const
{
  return NObjectIterator::__EndObjectIterator;
}

static int save_operation_result(int result, int *pval) {
  if (pval != NULL) {
    *pval = result;
  }
  return result;
}

ObjectOperation::ObjectOperation() {
  TestObjectOperationImpl *o = new TestObjectOperationImpl();
  o->get();
  impl = reinterpret_cast<ObjectOperationImpl*>(o);
}

librados::ObjectOperation::ObjectOperation(ObjectOperation&& rhs)
  : impl(rhs.impl) {
  rhs.impl = nullptr;
}

ObjectOperation::~ObjectOperation() {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  if (o) {
    o->put();
    o = NULL;
  }
}

librados::ObjectOperation&
librados::ObjectOperation::operator =(ObjectOperation&& rhs) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  if (o) {
    o->put();
  }
  impl = rhs.impl;
  rhs.impl = nullptr;
  return *this;
}

void ObjectOperation::assert_exists() {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::assert_exists, _1, _2, _4));
}

void ObjectOperation::assert_version(uint64_t ver) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::assert_version, _1, _2, ver));
}

void ObjectOperation::exec(const char *cls, const char *method,
                           bufferlist& inbl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::exec, _1, _2,
			       librados_stub::get_class_handler(), cls,
			       method, inbl, _3, _4, _5, _7));
}

void ObjectOperation::exec(const char *cls, const char *method,
                           bufferlist& inbl,
                           bufferlist *outbl,
                           int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::exec, _1, _2,
                                          librados_stub::get_class_handler(), cls,
                                          method, inbl, outbl, _4, _5, _7);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

class ObjectOpCompletionCtx {
  librados::ObjectOperationCompletion *completion;
  bufferlist bl;
public:
  ObjectOpCompletionCtx(librados::ObjectOperationCompletion *c) : completion(c) {}
  ~ObjectOpCompletionCtx() {
    delete completion;
  }
  void finish(int r) {
    completion->handle_completion(r, bl);
    delete completion;
    completion = nullptr;
  }

  bufferlist *outbl() {
    return &bl;
  }
};

static int handle_operation_completion(int result, ObjectOpCompletionCtx *ctx) {
  ctx->finish(result);
  delete ctx;
  return result;
}

void ObjectOperation::exec(const char *cls, const char *method,
                           bufferlist& inbl, ObjectOperationCompletion *completion) {
  ObjectOpCompletionCtx *ctx{nullptr};
  bufferlist *outbl{nullptr};

  if (completion) {
    ctx = new ObjectOpCompletionCtx(completion);
    outbl = ctx->outbl();
  }

  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::exec, _1, _2,
                                          librados_stub::get_class_handler(), cls,
                                          method, inbl, outbl, _4, _5, _7);
  if (ctx) {
    op = std::bind(handle_operation_completion,
                   std::bind(op, _1, _2, _3, _4, _5, _6, _7), ctx);
  }
  o->ops.push_back(op);
}

void ObjectOperation::set_op_flags2(int flags) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  if (o->ops.empty()) {
    return;
  }

  /* insert op before the last operation so that it affects it */
  auto it = o->ops.end();
  --it;
  o->ops.insert(it, std::bind(&TestIoCtxImpl::set_op_flags, _1, _7, flags));
}

size_t ObjectOperation::size() {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  return o->ops.size();
}

void ObjectOperation::cmpext(uint64_t off, const bufferlist& cmp_bl,
                             int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::cmpext, _1, _2, off,
                                           cmp_bl, _4);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectOperation::cmpxattr(const char *name, uint8_t op, const bufferlist& v)
{
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::cmpxattr_str, _1, _2, name, op, v));
}

void ObjectOperation::cmpxattr(const char *name, uint8_t op, uint64_t v)
{
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::cmpxattr, _1, _2, name, op, v));
}

void ObjectReadOperation::list_snaps(snap_set_t *out_snaps, int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::list_snaps, _1, _2,
                                           out_snaps);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::list_watchers(std::list<obj_watch_t> *out_watchers,
                                        int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::list_watchers, _1,
                                           _2, out_watchers);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::read(size_t off, uint64_t len, bufferlist *pbl,
                               int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op;
  if (pbl != NULL) {
    op = std::bind(&TestIoCtxImpl::read, _1, _2, len, off, pbl, _4, nullptr);
  } else {
    op = std::bind(&TestIoCtxImpl::read, _1, _2, len, off, _3, _4, nullptr);
  }

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::sparse_read(uint64_t off, uint64_t len,
                                      std::map<uint64_t,uint64_t> *m,
                                      bufferlist *pbl, int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op;
  if (pbl != NULL) {
    op = std::bind(&TestIoCtxImpl::sparse_read, _1, _2, off, len, m, pbl, _4);
  } else {
    op = std::bind(&TestIoCtxImpl::sparse_read, _1, _2, off, len, m, _3, _4);
  }

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::stat(uint64_t *psize, time_t *pmtime, int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::stat, _1, _2,
                                           psize, pmtime);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::stat2(uint64_t *psize, struct timespec *pts, int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::stat2, _1, _2,
                                           psize, pts);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::getxattrs(map<string, bufferlist> *pattrs, int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::xattr_get, _1, _2,
                                         pattrs);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::getxattr(const char *name, bufferlist *pbl, int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::getxattr, _1, _2,
                                         name, pbl);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::omap_get_keys2(const std::string &start_after,
                                         uint64_t max_return,
                                         std::set<std::string> *out_keys,
                                         bool *pmore,
                                         int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::omap_get_keys2, _1, _2,
                                         start_after, max_return,
                                         out_keys, pmore);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::omap_get_vals2(const std::string &start_after,
                                         const std::string &filter_prefix,
                                         uint64_t max_return,
                                         std::map<std::string, bufferlist> *out_vals,
                                         bool *pmore,
                                         int *prval) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);

  ObjectOperationTestImpl op = std::bind(&TestIoCtxImpl::omap_get_vals2, _1, _2,
                                         start_after, filter_prefix, max_return,
                                         out_vals, pmore);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::omap_get_vals2(const std::string &start_after,
                                         uint64_t max_return,
                                         std::map<std::string, bufferlist> *out_vals,
                                         bool *pmore,
                                         int *prval) {
  omap_get_vals2(start_after, string(), max_return,
                 out_vals, pmore, prval);
}

void ObjectWriteOperation::append(const bufferlist &bl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::append, _1, _2, bl, _5));
}

void ObjectWriteOperation::create(bool exclusive) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::create, _1, _2, exclusive, _5));
}

void ObjectWriteOperation::omap_set(const std::map<std::string, bufferlist> &map) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::omap_set, _1, _2, boost::ref(map)));
}

void ObjectWriteOperation::omap_set_header(const bufferlist& bl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::omap_set_header, _1, _2, bl));
}

void ObjectWriteOperation::omap_rm_keys(const std::set<std::string>& keys) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::omap_rm_keys, _1, _2, keys));
}

void ObjectWriteOperation::omap_clear() {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::omap_clear, _1, _2));
}

void ObjectWriteOperation::remove() {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::remove, _1, _2, _5));
}

void ObjectWriteOperation::selfmanaged_snap_rollback(uint64_t snapid) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::selfmanaged_snap_rollback,
			       _1, _2, snapid));
}

void ObjectWriteOperation::set_alloc_hint(uint64_t expected_object_size,
                                          uint64_t expected_write_size) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::set_alloc_hint, _1, _2,
			       expected_object_size, expected_write_size, 0,
                               _5));
}

void ObjectWriteOperation::set_alloc_hint2(uint64_t expected_object_size,
                                           uint64_t expected_write_size,
                                           uint32_t flags) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::set_alloc_hint, _1, _2,
			       expected_object_size, expected_write_size, flags,
                               _5));
}

void ObjectWriteOperation::tmap_update(const bufferlist& cmdbl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::tmap_update, _1, _2,
                               cmdbl));
}

void ObjectWriteOperation::truncate(uint64_t off) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::truncate, _1, _2, off, _5));
}

void ObjectWriteOperation::write(uint64_t off, const bufferlist& bl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::write, _1, _2, bl, bl.length(),
			       off, _5));
}

void ObjectWriteOperation::write_full(const bufferlist& bl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::write_full, _1, _2, bl, _5));
}

void ObjectWriteOperation::writesame(uint64_t off, uint64_t len,
                                     const bufferlist& bl) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::writesame, _1, _2, bl, len,
			       off, _5));
}

void ObjectWriteOperation::zero(uint64_t off, uint64_t len) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::zero, _1, _2, off, len, _5));
}

void ObjectWriteOperation::mtime2(struct timespec *pts) {
  if (!pts) {
    return;
  }
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::mtime2, _1, _2, *pts, _5));
}

void ObjectWriteOperation::setxattr(const char *name, const bufferlist& v) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::setxattr, _1, _2, name, v));
}

void ObjectWriteOperation::rmxattr(const char *name) {
  TestObjectOperationImpl *o = reinterpret_cast<TestObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&TestIoCtxImpl::rmxattr, _1, _2, name));
}


Rados::Rados() : client(NULL) {
}

Rados::Rados(IoCtx& ioctx) {
  TestIoCtxImpl *ctx = reinterpret_cast<TestIoCtxImpl*>(ioctx.io_ctx_impl);
  TestRadosClient *impl = ctx->get_rados_client();
  impl->get();

  client = reinterpret_cast<RadosClient*>(impl);
  ceph_assert(client != NULL);
}

Rados::~Rados() {
  shutdown();
}

void Rados::from_rados_t(rados_t p, Rados &rados) {
  if (rados.client != nullptr) {
    reinterpret_cast<TestRadosClient*>(rados.client)->put();
    rados.client = nullptr;
  }

  auto impl = reinterpret_cast<TestRadosClient*>(p);
  if (impl) {
    impl->get();
    rados.client = reinterpret_cast<RadosClient*>(impl);
  }
}

PoolAsyncCompletion *Rados::pool_async_create_completion()
{
  PoolAsyncCompletionImpl *c = new PoolAsyncCompletionImpl;
  return new PoolAsyncCompletion(c);
}

AioCompletion *Rados::aio_create_completion(void *cb_arg,
                                            callback_t cb_complete) {
  AioCompletionImpl *c;
  int r = rados_aio_create_completion2(cb_arg, cb_complete,
      reinterpret_cast<void**>(&c));
  ceph_assert(r == 0);
  return new AioCompletion(c);
}

int Rados::aio_watch_flush(AioCompletion* c) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->aio_watch_flush(c->pc);
}

int Rados::blocklist_add(const std::string& client_address,
			 uint32_t expire_seconds) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->blocklist_add(client_address, expire_seconds);
}

config_t Rados::cct() {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return reinterpret_cast<config_t>(impl->cct());
}

int Rados::cluster_fsid(std::string* fsid) {
  *fsid = "00000000-1111-2222-3333-444444444444";
  return 0;
}

int Rados::conf_set(const char *option, const char *value) {
  return rados_conf_set(reinterpret_cast<rados_t>(client), option, value);
}

int Rados::conf_get(const char *option, std::string &val) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  CephContext *cct = impl->cct();

  char *str = NULL;
  int ret = cct->_conf.get_val(option, &str, -1);
  if (ret != 0) {
    free(str);
    return ret;
  }

  val = str;
  free(str);
  return 0;
}

int Rados::conf_parse_env(const char *env) const {
  return rados_conf_parse_env(reinterpret_cast<rados_t>(client), env);
}

int Rados::conf_read_file(const char * const path) const {
  return rados_conf_read_file(reinterpret_cast<rados_t>(client), path);
}

int Rados::connect() {
  return rados_connect(reinterpret_cast<rados_t>(client));
}

uint64_t Rados::get_instance_id() {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->get_instance_id();
}

int Rados::get_min_compatible_osd(int8_t* require_osd_release) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->get_min_compatible_osd(require_osd_release);
}

int Rados::get_min_compatible_client(int8_t* min_compat_client,
                                     int8_t* require_min_compat_client) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->get_min_compatible_client(min_compat_client,
                                         require_min_compat_client);
}

int Rados::init(const char * const id) {
  return rados_create(reinterpret_cast<rados_t *>(&client), id);
}

int Rados::init_with_context(config_t cct_) {
  return rados_create_with_context(reinterpret_cast<rados_t *>(&client), cct_);
}

int Rados::ioctx_create(const char *name, IoCtx &io) {
  rados_ioctx_t p;
  int ret = rados_ioctx_create(reinterpret_cast<rados_t>(client), name, &p);
  if (ret) {
    return ret;
  }

  io.close();
  io.io_ctx_impl = reinterpret_cast<IoCtxImpl*>(p);
  return 0;
}

int Rados::ioctx_create2(int64_t pool_id, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_create2(reinterpret_cast<rados_t>(client), pool_id, &p);
  if (ret) {
    return ret;
  }

  io.close();
  io.io_ctx_impl = reinterpret_cast<IoCtxImpl*>(p);
  return 0;
}

int Rados::mon_command(std::string cmd, const bufferlist& inbl,
                       bufferlist *outbl, std::string *outs) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);

  std::vector<std::string> cmds;
  cmds.push_back(cmd);
  return impl->mon_command(cmds, inbl, outbl, outs);
}

int Rados::service_daemon_register(const std::string& service,
                                   const std::string& name,
                                   const std::map<std::string,std::string>& metadata) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->service_daemon_register(service, name, metadata);
}

int Rados::service_daemon_update_status(std::map<std::string,std::string>&& status) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->service_daemon_update_status(std::move(status));
}

int Rados::pool_create(const char *name) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_create(name);
}

int Rados::pool_create_async(const char *name, PoolAsyncCompletion *c) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_create_async(name, c->pc);
}


int Rados::pool_delete(const char *name) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_delete(name);
}

int Rados::pool_get_base_tier(int64_t pool, int64_t* base_tier) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_get_base_tier(pool, base_tier);
}

int Rados::pool_list(std::list<std::string>& v) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  std::list<std::pair<int64_t, std::string> > pools;
  int r = impl->pool_list(pools);
  if (r < 0) {
    return r;
  }

  v.clear();
  for (std::list<std::pair<int64_t, std::string> >::iterator it = pools.begin();
       it != pools.end(); ++it) {
    v.push_back(it->second);
  }
  return 0;
}

int Rados::pool_list2(std::list<std::pair<int64_t, std::string> >& v)
{
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_list(v);
}

int64_t Rados::pool_lookup(const char *name) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_lookup(name);
}

int Rados::pool_reverse_lookup(int64_t id, std::string *name) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->pool_reverse_lookup(id, name);
}

int Rados::cluster_stat(cluster_stat_t& result) {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->cluster_stat(result);
};

void Rados::shutdown() {
  if (client == NULL) {
    return;
  }
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  impl->put();
  client = NULL;
}

void Rados::test_blocklist_self(bool set) {
}

int Rados::wait_for_latest_osdmap() {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->wait_for_latest_osdmap();
}

int Rados::watch_flush() {
  TestRadosClient *impl = reinterpret_cast<TestRadosClient*>(client);
  return impl->watch_flush();
}

WatchCtx::~WatchCtx() {
}

WatchCtx2::~WatchCtx2() {
}

#warning ListObjec impl

const std::string& librados::ListObject::get_nspace() const
{
  return impl->get_nspace();
}

const std::string& librados::ListObject::get_oid() const
{
  return impl->get_oid();
}

const std::string& librados::ListObject::get_locator() const
{
  return impl->get_locator();
}

#warning ObjectCursor impl

librados::ObjectCursor::ObjectCursor()
{
  c_cursor = (rados_object_list_cursor)new hobject_t();
}

librados::ObjectCursor::~ObjectCursor()
{
  hobject_t *h = (hobject_t *)c_cursor;
  delete h;
}

string librados::ObjectCursor::to_str() const
{
  stringstream ss;
  ss << *(hobject_t *)c_cursor;
  return ss.str();
}

bool librados::ObjectCursor::from_str(const string& s)
{
  if (s.empty()) {
    *(hobject_t *)c_cursor = hobject_t();
    return true;
  }
  return ((hobject_t *)c_cursor)->parse(s);
}


} // namespace librados

int cls_cxx_create(cls_method_context_t hctx, bool exclusive) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->create(ctx->oid, exclusive, ctx->snapc);
}

int cls_cxx_remove(cls_method_context_t hctx) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->remove(ctx->oid, ctx->io_ctx_impl->get_snap_context());
}

int cls_cxx_stat2(cls_method_context_t hctx, uint64_t *size, ceph::real_time *mtime) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  struct timespec ts;
  int r = ctx->io_ctx_impl->stat2(ctx->oid, size, &ts);
  if (r < 0) {
    return r;
  }

  if (mtime) {
    *mtime = ceph::real_clock::from_timespec(ts);
  }

  return 0;
}

int cls_get_request_origin(cls_method_context_t hctx, entity_inst_t *origin) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);

  librados::TestRadosClient *rados_client =
    ctx->io_ctx_impl->get_rados_client();

  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = 0;
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);

  entity_addr_t entity_addr(entity_addr_t::TYPE_DEFAULT,
                            rados_client->get_nonce());
  entity_addr.in4_addr() = sin;

  *origin = entity_inst_t(
    entity_name_t::CLIENT(rados_client->get_instance_id()),
    entity_addr);
  return 0;
}

int cls_cxx_getxattr(cls_method_context_t hctx, const char *name,
                     bufferlist *outbl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->getxattr(ctx->oid, name, outbl);
}

int cls_cxx_getxattrs(cls_method_context_t hctx, std::map<string, bufferlist> *attrset) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->xattr_get(ctx->oid, attrset);
}

int cls_cxx_map_get_keys(cls_method_context_t hctx, const string &start_obj,
                         uint64_t max_to_get, std::set<string> *keys, bool *more) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->omap_get_keys2(ctx->oid, start_obj, max_to_get,
                                          keys, more);
}

int cls_cxx_map_get_val(cls_method_context_t hctx, const string &key,
                        bufferlist *outbl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);

  std::map<string, bufferlist> vals;
  int r = ctx->io_ctx_impl->omap_get_vals(ctx->oid, "", key, 1024, &vals);
  if (r < 0) {
    return r;
  }

  std::map<string, bufferlist>::iterator it = vals.find(key);
  if (it == vals.end()) {
    return -ENOENT;
  }

  *outbl = it->second;
  return 0;
}

int cls_cxx_map_get_vals(cls_method_context_t hctx, const string &start_obj,
                         const string &filter_prefix, uint64_t max_to_get,
                         std::map<string, bufferlist> *vals, bool *more) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  int r = ctx->io_ctx_impl->omap_get_vals2(ctx->oid, start_obj, filter_prefix,
					  max_to_get, vals, more);
  if (r < 0) {
    if (r == -ENOENT) {
      return 0;
    }
    return r;
  }
  return vals->size();
}

int cls_cxx_map_remove_key(cls_method_context_t hctx, const string &key) {
  std::set<std::string> keys;
  keys.insert(key);

  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->omap_rm_keys(ctx->oid, keys);
}

int cls_cxx_map_remove_range(cls_method_context_t hctx,
                             const std::string& key_begin,
                             const std::string& key_end) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->omap_rm_range(ctx->oid, key_begin, key_end);
}


int cls_cxx_map_set_val(cls_method_context_t hctx, const string &key,
                        bufferlist *inbl) {
  std::map<std::string, bufferlist> m;
  m[key] = *inbl;
  return cls_cxx_map_set_vals(hctx, &m);
}

int cls_cxx_map_set_vals(cls_method_context_t hctx,
                         const std::map<string, bufferlist> *map) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->omap_set(ctx->oid, *map);
}

int cls_cxx_map_clear(cls_method_context_t hctx) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->omap_clear(ctx->oid);
}

int cls_cxx_read(cls_method_context_t hctx, int ofs, int len,
                 bufferlist *outbl) {
  return cls_cxx_read2(hctx, ofs, len, outbl, 0);
}

int cls_cxx_read2(cls_method_context_t hctx, int ofs, int len,
                  bufferlist *outbl, uint32_t op_flags) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->read(
          ctx->oid, len, ofs, outbl, ctx->snap_id, nullptr);
}

int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                     bufferlist *inbl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->setxattr(ctx->oid, name, *inbl);
}

int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->stat(ctx->oid, size, mtime);
}

int cls_cxx_write(cls_method_context_t hctx, int ofs, int len,
                  bufferlist *inbl) {
  return cls_cxx_write2(hctx, ofs, len, inbl, 0);
}

int cls_cxx_write2(cls_method_context_t hctx, int ofs, int len,
                   bufferlist *inbl, uint32_t op_flags) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->write(ctx->oid, *inbl, len, ofs, ctx->snapc);
}

int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *inbl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->write_full(ctx->oid, *inbl, ctx->snapc);
}

int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len,
                    bufferlist *inbl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  int r = ctx->io_ctx_impl->truncate(ctx->oid, 0, ctx->snapc);
  if (r < 0) {
    return r;
  }
  return ctx->io_ctx_impl->write(ctx->oid, *inbl, len, ofs, ctx->snapc);
}

int cls_cxx_truncate(cls_method_context_t hctx, int ofs) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->truncate(ctx->oid, ofs, ctx->snapc);
}

int cls_cxx_write_zero(cls_method_context_t hctx, int ofs, int len) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->io_ctx_impl->zero(ctx->oid, len, ofs, ctx->snapc);
}

int cls_cxx_list_watchers(cls_method_context_t hctx,
			  obj_list_watch_response_t *watchers) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);

  std::list<obj_watch_t> obj_watchers;
  int r = ctx->io_ctx_impl->list_watchers(ctx->oid, &obj_watchers);
  if (r < 0) {
    return r;
  }

  for (auto &w : obj_watchers) {
    watch_item_t watcher;
    watcher.name = entity_name_t::CLIENT(w.watcher_id);
    watcher.cookie = w.cookie;
    watcher.timeout_seconds = w.timeout_seconds;
    watcher.addr.parse(w.addr);
    watchers->entries.push_back(watcher);
  }

  return 0;
}

uint64_t cls_get_features(cls_method_context_t hctx) {
  return CEPH_FEATURES_SUPPORTED_DEFAULT;
}

uint64_t cls_get_client_features(cls_method_context_t hctx) {
  return CEPH_FEATURES_SUPPORTED_DEFAULT;
}

int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  librados::snap_set_t snapset;
  int r = ctx->io_ctx_impl->list_snaps(ctx->oid, &snapset);
  if (r < 0) {
    return r;
  }

  *snap_seq = snapset.seq;
  return 0;
}

int cls_log(int level, const char *format, ...) {
  int size = 256;
  va_list ap;
  while (1) {
    char buf[size];
    va_start(ap, format);
    int n = vsnprintf(buf, size, format, ap);
    va_end(ap);
    if ((n > -1 && n < size) || size > 8196) {
      dout(ceph::dout::need_dynamic(level)) << buf << dendl;
      return n;
    }
    size *= 2;
  }
  return 0;
}

int cls_register(const char *name, cls_handle_t *handle) {
  librados::TestClassHandler *cls = librados_stub::get_class_handler();
  return cls->create(name, handle);
}

int cls_register_cxx_method(cls_handle_t hclass, const char *method,
    int flags,
    cls_method_cxx_call_t class_call,
    cls_method_handle_t *handle) {
  librados::TestClassHandler *cls = librados_stub::get_class_handler();
  return cls->create_method(hclass, method, class_call, handle);
}

int cls_register_cxx_filter(cls_handle_t hclass,
                            const std::string &filter_name,
                            cls_cxx_filter_factory_t fn,
                            cls_filter_handle_t *)
{
  librados::TestClassHandler *cls = librados_stub::get_class_handler();
  return cls->create_filter(hclass, filter_name, fn);
}

ceph_release_t cls_get_required_osd_release(cls_handle_t hclass) {
  return ceph_release_t::nautilus;
}

ceph_release_t cls_get_min_compatible_client(cls_handle_t hclass) {
  return ceph_release_t::nautilus;
}

// stubs to silence TestClassHandler::open_class()
PGLSFilter::~PGLSFilter()
{}

int cls_gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  ret = cls_gen_random_bytes(buf, sizeof(buf));
  if (ret < 0) {
    derr << "cannot get random bytes: " << ret << dendl;
    return -1;
  }

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
                   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    derr << "ceph_armor failed" << dendl;
    return -1;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size-1] = '\0';

  return 0;
}

int cls_cxx_chunk_write_and_set(cls_method_handle_t, int,
				int, bufferlist *,
				uint32_t, bufferlist *, int) {
  return -ENOTSUP;
}

int cls_cxx_map_read_header(cls_method_handle_t hctx, bufferlist *bl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  int r = ctx->io_ctx_impl->omap_get_header(ctx->oid, bl);
  if (r == -ENOENT) {
    bl->clear();
    r = 0;
  }
  return r;
}

int cls_cxx_map_write_header(cls_method_context_t hctx, ceph::buffer::list *inbl) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  bufferlist _bl;
  bufferlist& bl = (inbl ? *inbl : _bl);
  return ctx->io_ctx_impl->omap_set_header(ctx->oid, bl);
}

uint64_t cls_current_version(cls_method_context_t hctx) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  uint64_t ver;
  int r = ctx->io_ctx_impl->get_current_ver(ctx->oid, &ver);
  if (r < 0) {
    return (uint64_t)r;
  }

  return ver;
}

int cls_current_subop_num(cls_method_context_t hctx) {
  librados::TestClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::TestClassHandler::MethodContext*>(hctx);
  return ctx->trans->op_id;
}

uint64_t cls_get_osd_min_alloc_size(cls_method_context_t hctx) {
  return 0;
}

uint64_t cls_get_pool_stripe_width(cls_method_context_t hctx) {
  return 0;
}

int cls_gen_random_bytes(char *buf, int size)
{
  g_ceph_context->random()->get_bytes(buf, size);
  return 0;
}

