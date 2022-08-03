#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <libpmem.h>
#include <unistd.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>

enum UserColumn{Id=0, Userid, Name, Salary};
#define DEBUG

#ifdef DEBUG
#define DEBUG_PRINTF(condition, ...) \
  do { \
    if(!(condition)) \
    { \
      /* fprintf(stderr,"\nIn %s - function %s at line %d: ", __FILE__, __func__, __LINE__); \ */ \
      fprintf(stderr,__VA_ARGS__); \
    } \
  } while(0)
#else
#define DEBUG_PRINTF(...) (void)0
#endif


class UserString {
public:
  char ptr[128];
  bool operator==(const UserString &other) {
    return std::string(ptr, 128) == std::string(other.ptr, 128);
  }
};

bool operator==(const UserString &l, const UserString &r);

template <>
struct std::hash<UserString>
{
  uint64_t operator()(const UserString& k) const
  {
    return (hash<string>()(string(k.ptr, 128)));
  }
};


struct User{
  int64_t id = 0;
  char user_id[128] = {};
  char name[128] = {};
  int64_t salary = 0;
};


using location_type = std::atomic<uint64_t>;
const uint64_t ENTRY_LEN = sizeof(User);

/*
 * Data file
 * int64_t next_location = 8; // 初始化成8不用每次都加
 * ---------------------
 * User users[max_num]
 */

// 其实按照他的最大数据量来就好了，省点AEP的空间，性能还更好
const uint64_t DATA_LEN = ENTRY_LEN * 60 * 1000000;

class Data
{
public:
  Data() {}
  ~Data() {}

  void open(const std::string &filename) {
    uint64_t map_len;
    int is_pmem;
    bool new_create = false;

    if (access(filename.c_str(), F_OK)) {
      new_create = true;
    }

    ptr = reinterpret_cast<char *>(pmem_map_file(filename.c_str(), DATA_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
    DEBUG_PRINTF(ptr, "%s open mmaped failed", filename.c_str());

    if (new_create) {
      pmem_memset_nodrain(ptr, 0, DATA_LEN);
    }

    // 初始化下一个位置
    uint64_t *next_location = reinterpret_cast<uint64_t *>(ptr);
    *next_location = sizeof(uint64_t);
  }

  // data read and data write
  const User *data_read(uint64_t offset) {
    const User *user = reinterpret_cast<const User *>(ptr + offset);
    return user;
  }

  uint64_t data_write(const User &user) {
    // maybe cache here
    location_type *next_location = reinterpret_cast<location_type *>(ptr);
    uint64_t write_offset = next_location->fetch_add(ENTRY_LEN);
    if (write_offset >= DATA_LEN) {
      // file size overflow
      fprintf(stderr, "data file overflow!\n");
      assert(0);
    }

    // 可以留到flag一起drain
    pmem_memcpy_persist(ptr + write_offset, &user, sizeof(User));
    return write_offset;
  }

private:
  char *ptr = nullptr;
};

class DataFlag{
private:
  char *ptr;
  int fd_;
public:
  static const int DATA_NUM = 60 * 1000000;
  DataFlag() : ptr(nullptr), fd_(0) {}
  ~DataFlag() {
    if (ptr) {
      munmap(ptr, DATA_NUM);
    }
    if (fd_) {
      close(fd_);
    }
  }

  void Open(const std::string &filename) {
    bool hash_create = false;
    if (access(filename.c_str(), F_OK)) {
      hash_create = true;
    }

    fd_ = open(filename.c_str(), O_CREAT | O_RDWR, 0777);
    DEBUG_PRINTF(fd_, "%s open error", filename.c_str());
    ptr = reinterpret_cast<char*>(mmap(0, DATA_NUM, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    DEBUG_PRINTF(ptr, "%s mmaped error\n", filename.c_str());
    if (hash_create) {
      int ret = ftruncate(fd_, DATA_NUM);
      DEBUG_PRINTF(ret >= 0, "%s ftruncate\n",  filename.c_str());
      memset(ptr, 0, DATA_NUM);
    }
  }

  void set_flag(uint64_t offset) {
    size_t index = (offset - 8) / sizeof(User);
    ptr[index] = 1;
  }

  bool get_flag(uint64_t offset) {
    size_t index = (offset - 8) / sizeof(User);
    return ptr[index];
  }

};
