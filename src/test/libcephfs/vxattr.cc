// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "mds/mdstypes.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <string.h>

#include "common/Clock.h"
#include "common/ceph_json.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include <fmt/format.h>
#include <map>
#include <vector>
#include <thread>
#include <regex>

#ifndef ALLPERMS
#define ALLPERMS (S_ISUID|S_ISGID|S_ISVTX|S_IRWXU|S_IRWXG|S_IRWXO)
#endif

using namespace std;

TEST(LibCephFS, LayoutVerifyDefaultLayout) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    char value[1024] = "";
    int r = 0;

    // check for default layout
    r = ceph_getxattr(cmount, "/", "ceph.dir.layout", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    std::clog << "layout:" << value << std::endl;
    ASSERT_STRNE((char*)NULL, strstr(value, "\"inheritance\": \"@default\""));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetAndVerifyNewAndInheritedLayout) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    char value[1024] = "";
    int r = 0;

    // set a new layout and verify the same
    const char *new_layout = "{"
      "\"stripe_unit\": 65536, "
      "\"stripe_count\": 1, "
      "\"object_size\": 65536, "
      "\"pool_name\": \"cephfs.a.data\", "
      "}";
    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout", (void*)new_layout, strlen(new_layout), XATTR_REPLACE));
    r = ceph_getxattr(cmount, "test/d0", "ceph.dir.layout", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    std::clog << "layout:" << value << std::endl;
    ASSERT_STRNE((char*)NULL, strstr(value, "\"inheritance\": \"@set\""));

    // now check that the subdir layout is inherited
    r = ceph_getxattr(cmount, "test/d0/subdir", "ceph.dir.layout", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    std::clog << "layout:" << value << std::endl;
    ASSERT_STRNE((char*)NULL, strstr(value, "\"inheritance\": \"@inherited\""));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetBadJSON) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // set a new layout and verify the same
    const char *new_layout = "" // bad json without starting brace
      "\"stripe_unit\": 65536, "
      "\"stripe_count\": 1, "
      "\"object_size\": 65536, "
      "\"pool_name\": \"cephfs.a.data\", "
      "}";
    // try to set a malformed JSON, eg. without an open brace
    ASSERT_EQ(-EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout", (void*)new_layout, strlen(new_layout), XATTR_REPLACE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetBadPoolName) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try setting a bad pool name
    ASSERT_EQ(-EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.pool_name", (void*)"UglyPoolName", 12, XATTR_REPLACE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetWrongPoolName) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try setting a wrong pool name
    ASSERT_EQ(-EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.pool_name", (void*)"cephfs.a.meta", 13, XATTR_REPLACE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetBadPoolId) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try setting a bad pool id
    ASSERT_EQ(-EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.pool_id", (void*)"300", 3, XATTR_REPLACE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetInvalidFieldName) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try to set in invalid field
    ASSERT_EQ(-ENODATA, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.bad_field", (void*)"300", 3, XATTR_REPLACE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetAndSetDirPin) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d1", 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, "test/d1", "ceph.dir.pin", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("-1", value);
  }

  {
    char value[1024] = "";
    int r = -1;

    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d1", "ceph.dir.pin", (void*)"1", 1, XATTR_CREATE));

    r = ceph_getxattr(cmount, "test/d1", "ceph.dir.pin", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("1", value);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d1"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetAndSetDirDistribution) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d2", 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, "test/d2", "ceph.dir.pin.distributed", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("0", value);
  }

  {
    char value[1024] = "";
    int r = -1;

    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d2", "ceph.dir.pin.distributed", (void*)"1", 1, XATTR_CREATE));

    r = ceph_getxattr(cmount, "test/d2", "ceph.dir.pin.distributed", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("1", value);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d2"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetAndSetDirRandom) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d3", 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, "test/d3", "ceph.dir.pin.random", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("0", value);
  }

  {
    double val = (double)1.0/(double)128.0;
    std::stringstream ss;
    ss << val;
    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d3", "ceph.dir.pin.random", (void*)ss.str().c_str(), strlen(ss.str().c_str()), XATTR_CREATE));

    char value[1024] = "";
    int r = -1;
    
    r = ceph_getxattr(cmount, "test/d3", "ceph.dir.pin.random", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ(ss.str().c_str(), value);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d3"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

