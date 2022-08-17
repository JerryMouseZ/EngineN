#pragma once
#include "data.hpp"
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

static const int BUCKET_NUM = 1 << 23;
static const int ENTRY_NUM = 7;

// 64-byte allign
struct Bucket {
  // reset to zero at memset
  std::atomic<uint32_t> next_free;
  std::atomic<uint32_t> bucket_next; // offset = (bucket_next - 1) * sizeof(Bucket) + 8
  uint64_t entries[ENTRY_NUM];
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


static bool compare(const void *key, const User *user, int column) {
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

static void *res_copy(const User *user, void *res, int32_t select_column) {
  switch(select_column) {
  case Id: 
    memcpy(res, &user->id, 8); 
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
    memcpy(res, &user->salary, 8); 
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
  OverflowIndex(const std::string &filename, Data *data) {
    this->data = data;
    bool new_create = false;
    if (access(filename.c_str(), F_OK))
      new_create = true;
    ptr = reinterpret_cast<char *>(map_file(filename.c_str(), BUCKET_NUM * sizeof(Bucket)));
    next_location = reinterpret_cast<std::atomic<size_t> *>(ptr);

    if (new_create)
      next_location->store(8, std::memory_order_release);
  }

  void put(uint64_t over_offset, uint64_t data_offset) {
    volatile Bucket *bucket = reinterpret_cast<volatile Bucket *>(ptr + over_offset);
    size_t next_free = bucket->next_free.fetch_add(1, std::memory_order_acq_rel);
    if (next_free < ENTRY_NUM) {
      bucket->entries[next_free] = data_offset;
      /* bucket->entries[next_free].store(data_offset, std::memory_order_release); */
      return;
    }

    if (next_free == ENTRY_NUM) {
      uint64_t maybe_next = next_location->fetch_add(sizeof(Bucket));
      uint32_t index = (maybe_next - 8) / sizeof(Bucket) + 1;
      bucket->bucket_next.store(index, std::memory_order_release);
    }

    size_t index;
    while ((index = bucket->bucket_next.load(std::memory_order_acquire)) == 0);
    uint64_t next_offset = 8 + (index - 1) * sizeof(Bucket);
    put(next_offset, data_offset);
  }


  size_t get(size_t over_offset, const void *key, int where_column, int select_column, void *res, bool multi) {
    int count = 0;
    Bucket *bucket = reinterpret_cast<Bucket *>(ptr + over_offset);
    __builtin_prefetch(bucket, 0, 0);
    for (int i = 0; i < ENTRY_NUM; ++i) {
      uint64_t offset = bucket->entries[i];
      if (offset == 0) {
        return count;
      } else {
        const User *tmp = data->data_read(offset);
        if (tmp && compare(key, tmp, where_column)) {
          res = res_copy(tmp, res, select_column);
          count++;
          if (!multi)
            return count;
        }
      }
    }

    size_t index;
    if ((index = bucket->bucket_next.load(std::memory_order_acquire)) == 0)
      return count;

    uint64_t next_offset = 8 + (index - 1) * sizeof(Bucket);
    count += get(next_offset, key, where_column, select_column, res, multi);
    return count;

  }

  ~OverflowIndex() {
    if (ptr) {
      munmap(ptr, BUCKET_NUM * sizeof(Bucket));
    }
  }

private:
  char *ptr;
  Data *data;
};

class Index
{
public:
  Index(const std::string &path, Data *data) {
    std::string hash_file = path + ".hash";
    std::string over_file = path + ".over";
    
    hash_ptr = reinterpret_cast<char *>(map_file(hash_file.c_str(), BUCKET_NUM * sizeof(Bucket)));
    this->data = data;
    overflowindex = new OverflowIndex(over_file, data);
  }

  ~Index() {
    if (hash_ptr)
      munmap(hash_ptr, sizeof(Bucket) * BUCKET_NUM);
  }


  void put(size_t hash_val, uint64_t data_offset) {
    size_t bucket_location = hash_val & (BUCKET_NUM - 1);
    volatile Bucket *bucket = reinterpret_cast<volatile Bucket *>(hash_ptr + bucket_location * sizeof(Bucket));
    size_t next_free = bucket->next_free.fetch_add(1, std::memory_order_acq_rel); // both read write
    if (next_free < ENTRY_NUM) {
      bucket->entries[next_free] = data_offset;
      return;
    }

    if (next_free == ENTRY_NUM) {
      uint64_t maybe_next = overflowindex->next_location->fetch_add(sizeof(Bucket), std::memory_order_acq_rel);
      uint32_t index = (maybe_next - 8) / sizeof(Bucket) + 1;
      bucket->bucket_next.store(index, std::memory_order_release);
    }

    // waiting for bucket allocation
    size_t index;
    while ((index = bucket->bucket_next.load(std::memory_order_acquire)) == 0);
    uint64_t next_offset = 8 + (index - 1) * sizeof(Bucket);
    overflowindex->put(next_offset, data_offset);
  }


  int get(const void *key, int where_column, int select_column, void *res, bool multi) {
    size_t bucket_location = key_hash(key, where_column) & (BUCKET_NUM - 1);
    int count = 0;
    Bucket *bucket = reinterpret_cast<Bucket *>(hash_ptr + bucket_location * sizeof(Bucket));
    __builtin_prefetch(bucket, 0, 0);
    for (int i = 0; i < ENTRY_NUM; ++i) {
      size_t offset = bucket->entries[i];
      if (offset == 0) {
        return count;
      }
      const User *tmp = data->data_read(offset);
      if (tmp && compare(key, tmp, where_column)) {
        res = res_copy(tmp, res, select_column);
        count++;
        if (!multi) {
          return count;
        }
      }
    }
    
    size_t index;
    if ((index = bucket->bucket_next.load(std::memory_order_acquire)) == 0) {
      return count;
    }

    uint64_t next_offset = 8 + (index - 1) * sizeof(Bucket);
    count += overflowindex->get(next_offset, key, where_column, select_column, res, multi);
    return count;
  }

private:
  char *hash_ptr;
  OverflowIndex *overflowindex;
  Data *data;
};

