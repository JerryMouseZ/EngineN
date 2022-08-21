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
      fprintf(stderr,__VA_ARGS__); \
    } \
  } while(0)
#else
#define DEBUG_PRINTF(...) (void)0
#endif

// close log
#define LOG 1

class UserString {
public:
  char ptr[128];
  bool operator==(const UserString &other) {
    return memcmp(ptr, other.ptr, 128) == 0;
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

static inline void *map_file(const char *path, size_t len)
{
  bool hash_create = false;
  if (access(path, F_OK)) {
    hash_create = true;
  }

  int fd = open(path, O_CREAT | O_RDWR, 0777);
  DEBUG_PRINTF(fd, "%s open error", path);
  if (hash_create) {
    int ret = ftruncate(fd, len);
    DEBUG_PRINTF(ret >= 0, "%s ftruncate\n", path);
  }

  char *ptr = reinterpret_cast<char*>(mmap(0, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
  DEBUG_PRINTF(ptr, "%s mmaped error\n", path);

  if (hash_create) {
    memset(ptr, 0, len);
  } else {
    // prefault
    /* volatile long reader = 0; */
    /* for (long i = 0; i < len ; i += 4096) { */
    /*   reader += ptr[i]; */
    /* } */
  }
  
  /* madvise(ptr, len, MADV_HUGEPAGE); */
  close(fd);
  return ptr;
}

/* Flag file
 * char flags[DATA_NUM]
 */
class DataFlag{
private:
  volatile uint8_t *ptr;
public:
  static const int DATA_NUM = 60 * 1000000;
  DataFlag() : ptr(nullptr) {}
  ~DataFlag() {
    if (ptr) {
      munmap((void *)ptr, DATA_NUM);
    }
  }

  void Open(const std::string &filename) {
    ptr = reinterpret_cast<volatile uint8_t *>(map_file(filename.c_str(), DATA_NUM));
  }

  void set_flag(uint64_t offset) {
    size_t index = (offset - 64) / sizeof(User);
    ptr[index] = 1;
  }

  bool get_flag(uint64_t offset) {
    size_t index = (offset - 64) / sizeof(User);
    return ptr[index];
  }
};


/*
 * Data file
 * int64_t next_location = 8; // 初始化成8不用每次都加
 * ---------------------
 * User users[DATA_NUM]
 */
const uint64_t DATA_LEN = ENTRY_LEN * 52 * 1000000;
const uint64_t CACHE_LEN = ENTRY_LEN * 8 * 1000000 + 64;
class Data
{
public:
  Data() {}
  ~Data() {
    pmem_unmap(pmem_ptr, DATA_LEN);
  }

  void open(const std::string &fdata, const std::string &fcache, const std::string &fflag) {
    uint64_t map_len;
    int is_pmem;
    bool new_create = false;

    if (access(fdata.c_str(), F_OK)) {
      new_create = true;
    }

    pmem_ptr = reinterpret_cast<char *>(pmem_map_file(fdata.c_str(), DATA_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
    DEBUG_PRINTF(pmem_ptr, "%s open mmaped failed", fdata.c_str());


    if (new_create) {
      // 初始化下一个位置
      pmem_memset_nodrain(pmem_ptr, 0, DATA_LEN);
      cache_ptr = reinterpret_cast<char *>(map_file(fcache.c_str(), CACHE_LEN));
      uint64_t *next_location = reinterpret_cast<uint64_t *>(cache_ptr);
      *next_location = 64;
    } else {
      cache_ptr = reinterpret_cast<char *>(map_file(fcache.c_str(), CACHE_LEN));
    }

    flags = new DataFlag();
    flags->Open(fflag);
  }

  // data read and data write
  const User *data_read(uint64_t offset) {
    if (flags->get_flag(offset)) {
      User *user;
      if (offset < CACHE_LEN)
        user = reinterpret_cast<User *>(cache_ptr + offset);
      else
        user = reinterpret_cast<User *>(pmem_ptr + offset - CACHE_LEN);
      return user;
    }

    return nullptr;
  }

  uint64_t data_write(const User &user) {
    // maybe cache here
    location_type *next_location = reinterpret_cast<location_type *>(cache_ptr);
    uint64_t write_offset = next_location->fetch_add(ENTRY_LEN);
    if (write_offset >= DATA_LEN + CACHE_LEN) {
      // file size overflow
      fprintf(stderr, "data file overflow!\n");
      assert(0);
    }

    // prefetch write
    /* __builtin_prefetch(ptr + write_offset, 1, 0); */
    // 可以留到flag一起drain
    if (write_offset < CACHE_LEN)
      memcpy(cache_ptr + write_offset, &user, sizeof(User));
    else
      pmem_memcpy_persist(pmem_ptr + write_offset - CACHE_LEN, &user, sizeof(User));
    return write_offset;
  }

  void put_flag(uint64_t offset) {
    flags->set_flag(offset);
  }

private:
  char *pmem_ptr = nullptr;
  char *cache_ptr = nullptr;
  DataFlag *flags;
};
