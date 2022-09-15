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
  bool operator==(const UserString &other);
};

template <>
struct std::hash<UserString>
{
  size_t operator()(const UserString& k) const;
};


bool operator==(const UserString &l, const UserString &r);

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
  DataFlag();
  ~DataFlag();

  void Open(const std::string &filename);

  void set_flag(uint32_t index);

  bool get_flag(size_t index);
};


/*
 * Data file
 * ---------------------
 * User users[DATA_NUM]
 */
class Data
{
public:
  Data();
  ~Data();
  void open(const std::string &fdata, const std::string &fcache, const std::string &fflag);
  // data read and data write
  const User *data_read(uint32_t index);

  void put_flag(uint32_t index);

  bool get_flag(uint32_t index);

  UserArray *get_pmem_users();
private:
  char *pmem_ptr = nullptr;
  UserArray *pmem_users = nullptr;
  DataFlag *flags;
};
