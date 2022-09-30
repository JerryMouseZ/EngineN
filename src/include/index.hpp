#pragma once
#include "data_access.hpp"
#include <cassert>
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <unistd.h>
#include <unordered_map>
#include <pthread.h>
#include <string.h>
#include <string>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <atomic>
/*
 * Index file
 * Bucket buckets[max_num];
 */

/*
 * Overflow chain file
 * atomic<size_t> next_location = 8; //地址
 * Bucket buckets[max_num];
 */

static const int BUCKET_NUM = 1 << 24;
static const int OVER_NUM = 1 << 20;
static const int ENTRY_NUM = 6;

// 64-byte allign
struct Bucket {
  // reset to zero at memset
  std::atomic<uint32_t> next_free;
  uint16_t is_overflow;
  uint32_t bucket_next; // offset = (bucket_next - 1) * sizeof(Bucket) + 8
  // 54
  uint32_t entries[ENTRY_NUM];
  uint32_t inlinekeys[ENTRY_NUM];
  uint8_t extra[ENTRY_NUM];
};

static inline size_t key_hash(const void *key, int column) {
  size_t val = 0;
  switch(column) {
  case Id:
    val = std::hash<int64_t>()(*(int64_t *)key);
    break;
  case Userid:
    val = std::hash<UserString>()(*(UserString *)key);
    break;
  case Name:
    val = std::hash<UserString>()(*(UserString *)key);
    break;
  case Salary:
    val = std::hash<int64_t>()(*(int64_t *)key);
    break;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return val;
}


static inline bool compare(const void *key, const User *user, int column) {
  switch(column) {
  case Id:
    return *(int64_t *)key == user->id;
  case Userid:
    return *(UserString *)key == *reinterpret_cast<const UserString*>(user->user_id);
  case Name:
    return *(UserString *)key == *reinterpret_cast<const UserString*>(user->name);
  case Salary:
    return *(int64_t *)key == user->salary;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return 0;
}

static inline bool inlinecompare(size_t hashval, const void *key, const User *user, int column, uint8_t extra, uint32_t inlinekey) {
  size_t val = extra;
  val <<= 32;
  val += inlinekey;
  hashval >>= 24;
  if (val != hashval)
    return 0;

  switch(column) {
  case Id:
    return 1;
  case Userid:
    return *(UserString *)key == *reinterpret_cast<const UserString*>(user->user_id);
  case Name:
    return *(UserString *)key == *reinterpret_cast<const UserString*>(user->name);
  case Salary:
    return 1;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return 0;
}

static void *res_copy(const User *user, void *res, int32_t select_column) {
  switch(select_column) {
  case Id: 
    *(int64_t *) res = user->id;
    res = (char *)res + 8; 
    break;
  case Userid: 
    memcpy(res, user->user_id, 128); 
    res = (char *)res + 128; 
    break;
  case Name: 
    memcpy(res, user->name, 128); 
    res = (char *)res + 128; 
    break; 
  case Salary: 
    *(int64_t *) res = user->salary;
    res = (char *)res + 8; 
    break;
  default: 
    DEBUG_PRINTF(LOG, "column error"); // wrong
  }
  return res;
}



class OverflowIndex{
public:
  volatile std::atomic<size_t> *next_location; // open to index
  OverflowIndex(const std::string &filename, const DataAccess &accessor)
    : accessor(accessor) {
    bool new_create = false;
    if (access(filename.c_str(), F_OK))
      new_create = true;
    ptr = reinterpret_cast<char *>(map_file(filename.c_str(), OVER_NUM * sizeof(Bucket) + 64, nullptr));
    madvise(ptr, OVER_NUM * sizeof(Bucket), MADV_RANDOM);
    next_location = reinterpret_cast<std::atomic<size_t> *>(ptr);

    if (new_create)
      next_location->store(64, std::memory_order_release);
  }

  void put(uint64_t over_offset, size_t hash_val, uint32_t data_offset) {
    volatile Bucket *bucket = reinterpret_cast<volatile Bucket *>(ptr + over_offset);
    if (!bucket->is_overflow) {
      size_t next_free = bucket->next_free.fetch_add(1, std::memory_order_acq_rel);
      if (next_free < ENTRY_NUM) {
        bucket->entries[next_free] = data_offset;
        bucket->extra[next_free] = hash_val >> 56; // 高8位
        bucket->inlinekeys[next_free] = (hash_val >> 24) & 0xffffffff; // 中间32
        return;
      }

      if (next_free == ENTRY_NUM) {
        uint64_t maybe_next = next_location->fetch_add(sizeof(Bucket));
        assert(maybe_next < OVER_NUM * sizeof(Bucket));
        uint32_t index = (maybe_next - 64) / sizeof(Bucket) + 1;
        bucket->bucket_next = index;
        bucket->is_overflow = 1;
      }
    }

    size_t index;
    while ((index = bucket->bucket_next) == 0);
    uint64_t next_offset = 64 + (index - 1) * sizeof(Bucket);
    put(next_offset, hash_val, data_offset);
  }


  size_t get(size_t over_offset, size_t hash_val, const void *key, int where_column, int select_column, void *res, bool multi) {
    int count = 0;
    Bucket *bucket = reinterpret_cast<Bucket *>(ptr + over_offset);
    for (int i = 0; i < bucket->next_free.load(std::memory_order_relaxed); ++i) {
      uint64_t offset = bucket->entries[i];
      const User *tmp = accessor.read(offset);
      /* __builtin_prefetch(tmp, 0, 0); */
      if (tmp && inlinecompare(hash_val, key, tmp, where_column, bucket->extra[i], bucket->inlinekeys[i])) {
        res = res_copy(tmp, res, select_column);
        count++;
        if (!multi)
          return count;
      }
    }

    size_t index;
    if ((index = bucket->bucket_next) == 0)
      return count;

    uint64_t next_offset = 64 + (index - 1) * sizeof(Bucket);
    count += get(next_offset, hash_val, key, where_column, select_column, res, multi);
    return count;

  }

  ~OverflowIndex() {
    if (ptr) {
      munmap(ptr, BUCKET_NUM * sizeof(Bucket));
    }
  }

private:
  char *ptr;
  DataAccess accessor;
};

class Index
{
public:
  Index(const std::string &path, Data *datas, UserQueue *qs)
    : accessor(datas, qs) {
      std::string hash_file = path + ".hash";
      std::string over_file = path + ".over";

      hash_ptr = reinterpret_cast<char *>(map_file(hash_file.c_str(), BUCKET_NUM * sizeof(Bucket), nullptr));
      madvise(hash_ptr, BUCKET_NUM * sizeof(Bucket), MADV_RANDOM);
      overflowindex = new OverflowIndex(over_file, accessor);
    }

  ~Index() {
    if (hash_ptr)
      munmap(hash_ptr, sizeof(Bucket) * BUCKET_NUM);
  }


  void put(size_t hash_val, uint32_t data_offset) {
    size_t bucket_location = hash_val & (BUCKET_NUM - 1);
    volatile Bucket *bucket = reinterpret_cast<volatile Bucket *>(hash_ptr + bucket_location * sizeof(Bucket));
    if (!bucket->is_overflow) {
      size_t next_free = bucket->next_free.fetch_add(1, std::memory_order_acq_rel); // both read write
      if (next_free < ENTRY_NUM) {
        bucket->entries[next_free] = data_offset;
        bucket->extra[next_free] = hash_val >> 56; // 高8位
        bucket->inlinekeys[next_free] = (hash_val >> 24) & 0xffffffff; // 中间32
        return;
      }

      if (next_free == ENTRY_NUM) {
        uint64_t maybe_next = overflowindex->next_location->fetch_add(sizeof(Bucket), std::memory_order_acq_rel);
        assert(maybe_next < sizeof(Bucket) * OVER_NUM);
        uint32_t index = (maybe_next - 64) / sizeof(Bucket) + 1;
        bucket->bucket_next = index;
        bucket->is_overflow = 1;
      }
    }

    // waiting for bucket allocation
    size_t index;
    while ((index = bucket->bucket_next) == 0);
    uint64_t next_offset = 64 + (index - 1) * sizeof(Bucket);
    overflowindex->put(next_offset, hash_val, data_offset);
  }


  int get(const void *key, int where_column, int select_column, void *res, bool multi) {
    size_t hash_val = key_hash(key, where_column);
    size_t bucket_location = hash_val & (BUCKET_NUM - 1);
    int count = 0;
    Bucket *bucket = reinterpret_cast<Bucket *>(hash_ptr + bucket_location * sizeof(Bucket));
    for (int i = 0; i < bucket->next_free.load(std::memory_order_relaxed); ++i) {
      size_t offset = bucket->entries[i];
      const User *tmp = accessor.read(offset);
      /* __builtin_prefetch(tmp, 0, 0); */
      if (tmp && inlinecompare(hash_val, key, tmp, where_column, bucket->extra[i], bucket->inlinekeys[i])) {
        res = res_copy(tmp, res, select_column);
        count++;
        if (!multi) {
          return count;
        }
      }
    }

    size_t index;
    if ((index = bucket->bucket_next) == 0) {
      return count;
    }

    uint64_t next_offset = 64 + (index - 1) * sizeof(Bucket);
    count += overflowindex->get(next_offset, hash_val, key, where_column, select_column, res, multi);
    return count;
  }

private:
  char *hash_ptr;
  OverflowIndex *overflowindex;
  DataAccess accessor;
};
