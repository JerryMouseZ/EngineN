#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <cassert>
#include <libpmem.h>
#include <unistd.h>

struct User{
  int64_t id = 0;
  char user_id[128] = {};
  char name[128] = {};
  int64_t salary = 0;
};

using location_type = std::atomic<size_t>;
const size_t USER_LEN = sizeof(User);

/*
 * Data file
 * int64_t next_location = 8; // 初始化成8不用每次都加
 * ---------------------
 * User users[max_num]
 */

// 其实按照他的最大数据量来就好了，省点AEP的空间，性能还更好
const size_t DATA_LEN = USER_LEN * 50 * 1000000;

class Data
{
public:
  Data() {}
  ~Data() {}
  
  void open(const std::string &filename) {
    size_t map_len;
    int is_pmem;
    bool new_create = false;

    if (access(filename.c_str(), F_OK)) {
      new_create = true;
    }

    ptr = reinterpret_cast<char *>(pmem_map_file(filename.c_str(), DATA_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
    assert(ptr);

    if (new_create) {
      pmem_memset_nodrain(ptr, 0, DATA_LEN);
    }
    
    // 初始化下一个位置
    size_t *next_location = reinterpret_cast<size_t *>(ptr);
    *next_location = sizeof(size_t);
  }

  // data read and data write
  const User *data_read(size_t offset) {
    const User *user = reinterpret_cast<const User *>(ptr + offset + 8);
    return user;
  }

  size_t data_write(const User &user) {
    // maybe cache here
    location_type *next_location = reinterpret_cast<location_type *>(ptr);
    size_t write_offset = next_location->fetch_add(USER_LEN);
    memcpy(ptr + write_offset, &user, USER_LEN);
    return write_offset;
  }

private:
  char *ptr = nullptr;
};

