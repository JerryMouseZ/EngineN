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
#include <thread>

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

#define START 64

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
  size_t operator()(const UserString& k) const
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


using location_type = std::atomic<size_t>;
const size_t ENTRY_LEN = sizeof(User);

static inline void prefault(char *ptr, size_t len)
{
  volatile long *reader = new long;
  std::thread *threads[8];
  size_t per_thread = len / 8;
  for (int i = 0; i < 8; ++i) {
    threads[i] = new std::thread([=]{
      for (size_t j = i * per_thread; j < (i + 1) * per_thread  && j < len; j += 4096) {
        __sync_fetch_and_add(reader, ptr[j]);
        __builtin_prefetch(ptr + 4096 * 2, 1, 0);
      }
    });
  }

  for (int i = 0; i < 8; ++i) {
    threads[i]->join();
    delete threads[i];
  }
  delete reader;
}

static inline void *map_file(const char *path, size_t len)
{
  bool hash_create = false;
  if (access(path, F_OK)) {
    hash_create = true;
  }

  int fd = open(path, O_CREAT | O_RDWR, 0777);
  DEBUG_PRINTF(fd, "%s open error", path);
  if (hash_create) {
    int ret = posix_fallocate(fd, 0, len);
    DEBUG_PRINTF(ret >= 0, "%s ftruncate\n", path);
  }

  char *ptr = reinterpret_cast<char*>(mmap(0, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
  DEBUG_PRINTF(ptr, "%s mmaped error\n", path);

  if (hash_create) {
    // 其实会自动置零，这里相当于一个prefault
    memset(ptr, 0, len);
  } else {
    // prefault
    prefault(ptr, len);
  }

  /* madvise(ptr, len, MADV_HUGEPAGE); */
  close(fd);
  return ptr;
}

/* static inline size_t get_index(size_t offset) { */
/*   return (offset - START) / sizeof(User); */
/* } */

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

  void set_flag(uint32_t index) {
    ptr[index - 1] = 1;
  }

  bool get_flag(size_t index) {
    return ptr[index - 1];
  }
};


/*
 * Data file
 * int64_t next_location = 8; // 初始化成8不用每次都加
 * ---------------------
 * User users[DATA_NUM]
 */
const size_t DATA_LEN = ENTRY_LEN * 52 * 1000000;
const size_t CACHE_LEN = START;
class Data
{
public:
  Data() {}
  ~Data() {
    pmem_unmap(pmem_ptr, DATA_LEN);
  }

  void open(const std::string &fdata, const std::string &fcache, const std::string &fflag) {
    size_t map_len;
    int is_pmem;
    bool new_create = false;

    if (access(fdata.c_str(), F_OK)) {
      new_create = true;
    }

    pmem_ptr = reinterpret_cast<char *>(pmem_map_file(fdata.c_str(), DATA_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
    DEBUG_PRINTF(pmem_ptr, "%s open mmaped failed", fdata.c_str());
    pmem_users = (User *)pmem_ptr;


    if (new_create) {
      // 其实会自动置零的，这里相当于是一个populate
      pmem_memset_nodrain(pmem_ptr, 0, DATA_LEN);
      char *cache_ptr = reinterpret_cast<char *>(map_file(fcache.c_str(), CACHE_LEN));
      next_location = reinterpret_cast<std::atomic<size_t> *>(cache_ptr);
      *next_location = 1;
    } else {
      char *cache_ptr = reinterpret_cast<char *>(map_file(fcache.c_str(), CACHE_LEN));
      next_location = reinterpret_cast<std::atomic<size_t> *>(cache_ptr);
    }

    flags = new DataFlag();
    flags->Open(fflag);
  }

  // data read and data write
  const User *data_read(uint32_t index) {
    if (flags->get_flag(index)) {
      User *user;
      index -= 1;
      return pmem_users + index;
    }
    return nullptr;
  }

  uint32_t data_write(const User &user) {
    // maybe cache here
    uint32_t write_index = next_location->fetch_add(1);
    if (write_index >= 56000000) {
      // file size overflow
      fprintf(stderr, "data file overflow!\n");
      assert(0);
    }

    // prefetch write
    // 可以留到flag一起drain
    uint32_t index = write_index - 1;
    pmem_memcpy_persist(pmem_users + index, &user, sizeof(User));
    if ((index + 1) % 15 == 0)
      __builtin_prefetch(pmem_users + index + 15, 1, 0);
    return write_index;
  }


  void put_flag(uint32_t index) {
    flags->set_flag(index);
  }

  bool get_flag(uint32_t index) {
    return flags->get_flag(index);
  }
  
  User *get_pmem_users() {
    return pmem_users;
  }
private:
  char *pmem_ptr = nullptr;
  User *pmem_users = nullptr;
  std::atomic<size_t> *next_location = nullptr;
  DataFlag *flags;
};
