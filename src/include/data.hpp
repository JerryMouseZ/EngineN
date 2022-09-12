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
#include <thread>

#include "util.hpp"
#include "config.hpp"
#include "commit_array.hpp"

enum UserColumn{Id=0, Userid, Name, Salary};

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


using UserArray = CommitArray<User, QCMT_ALIGN>;
using location_type = std::atomic<size_t>;
constexpr size_t ENTRY_LEN = sizeof(User);

constexpr size_t NR_USER = 52 * 1000000;
constexpr size_t EACH_NR_USER = ROUND_DIV(NR_USER, MAX_NR_CONSUMER);
constexpr size_t EACH_NR_USER_ARRAY = (EACH_NR_USER + UserArray::N_DATA - 1) / UserArray::N_DATA;
constexpr size_t EACH_DATA_FILE_LEN = EACH_NR_USER_ARRAY * UserArray::DALIGN;

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
  DataFlag() : ptr(nullptr) {}
  ~DataFlag() {
    if (ptr) {
      munmap((void *)ptr, EACH_NR_USER);
    }
  }

  void Open(const std::string &filename) {
    ptr = reinterpret_cast<volatile uint8_t *>(map_file(filename.c_str(), EACH_NR_USER, nullptr));
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
class Data
{
public:
  Data() {}
  ~Data() {
    pmem_unmap(pmem_ptr, EACH_DATA_FILE_LEN);
  }

  void open(const std::string &fdata, const std::string &fcache, const std::string &fflag) {
    size_t map_len;
    int is_pmem;
    bool new_create = false;

    if (access(fdata.c_str(), F_OK)) {
      new_create = true;
    }

    pmem_ptr = reinterpret_cast<char *>(pmem_map_file(fdata.c_str(), EACH_DATA_FILE_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
    DEBUG_PRINTF(pmem_ptr, "%s open mmaped failed", fdata.c_str());
    pmem_users = (UserArray *)pmem_ptr;


    if (new_create) {
      // 其实会自动置零的，这里相当于是一个populate
      pmem_memset_nodrain(pmem_ptr, 0, EACH_DATA_FILE_LEN);
    } 

    flags = new DataFlag();
    flags->Open(fflag);
  }

  // data read and data write
  const User *data_read(uint32_t index) {
    // 让log来检查，这里就不重复检查了
    uint64_t ca_pos = index / UserArray::N_DATA;
    uint64_t inner_ca_pos = index % UserArray::N_DATA;
    return &pmem_users[ca_pos].data[inner_ca_pos];
  }

  void put_flag(uint32_t index) {
    flags->set_flag(index);
  }

  bool get_flag(uint32_t index) {
    return flags->get_flag(index);
  }

  UserArray *get_pmem_users() {
    return pmem_users;
  }

private:
  char *pmem_ptr = nullptr;
  UserArray *pmem_users = nullptr;
  DataFlag *flags;
};
