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
#include "comm.h"

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

#ifdef LOCAL
constexpr size_t NR_USER = 52 * 1000000;
#else
constexpr size_t NR_USER = 202 * 1000000;
#endif

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
  size_t *next_location;
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
  void open(const std::string &fdata, const std::string &fflag);
  // data read and data write
  const User *data_read(uint32_t index);

  void put_flag(uint32_t index);

  bool get_flag(uint32_t index);

  UserArray *get_pmem_users();
private:
  char *pmem_ptr = nullptr;
  /* size_t *next_location; */
  UserArray *pmem_users = nullptr;
  DataFlag *flags;
};

struct query{
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  void *res;
  uint64_t unique_id; // 这个是给对端确认的，对面原样发回来就知道
  uint8_t select_column;
  uint8_t where_column;
  void *column_key;
};


class RemoteState {
public:
  void open(std::string fname, bool *is_new_create);

  volatile uint32_t *get_next_user_index() { return next_user_index; }

private:
  volatile uint32_t *next_user_index;
};

struct TransControl {
  char *src;
  uint64_t rest;
  const char *name = nullptr;

  bool update_check_finished(uint64_t cnt) {
    if (name) {
      DEBUG_PRINTF(VLOG, "%s: %s cnt/rest = %ld/%ld\n", this_host_info, name, cnt, rest);
    }
    if (rest == cnt) {
      return true;
    }
    rest -= cnt;
    src += cnt;
    return false;
  }
};

struct ArrayTransControl {
  TransControl ctrls[MAX_NR_CONSUMER];
  const char *name = nullptr;
  int cur;

  bool update_check_finished(uint64_t cnt) {
    DEBUG_PRINTF(VLOG, "%s: %s [%d] cnt/rest = %ld/%ld\n", this_host_info, name, cur, cnt, ctrls[cur].rest);
    bool finished = ctrls[cur].update_check_finished(cnt);
    if (finished) {
      while (++cur < MAX_NR_CONSUMER) {
        if (ctrls[cur].rest > 0) {
          break;
        }
      }
      return cur == MAX_NR_CONSUMER;
    } else {
      return false;
    }
  }
};
