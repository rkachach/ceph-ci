// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "block_driver.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

class FSDriver final : public BlockDriver {
public:
  FSDriver(config_t config) : config(config) {}
  ~FSDriver() final {}

  bufferptr get_buffer(size_t size) final {
    return ceph::buffer::create_page_aligned(size);
  }

  seastar::future<> write(
    off_t offset,
    bufferptr ptr) final;

  seastar::future<bufferlist> read(
    off_t offset,
    size_t size) final;

  size_t get_size() const {
    return size;
  }

  seastar::future<> mount() final;

  seastar::future<> close() final;

private:
  size_t size = 0;
  const config_t config;
  std::unique_ptr<crimson::os::FuturizedStore> fs;
  std::map<unsigned, crimson::os::CollectionRef> collections;

  struct offset_mapping_t {
    crimson::os::CollectionRef chandle;
    ghobject_t object;
    off_t offset;
  };
  offset_mapping_t map_offset(off_t offset);

  seastar::future<> mkfs();
  void init();
};
