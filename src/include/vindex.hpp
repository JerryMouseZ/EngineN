#pragma once
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
#include "util.hpp"
#include <atomic>
#include "data.hpp"
/*
 * Index file
 * VBucket VBuckets[max_num];
 */

/*
 * Overflow chain file
 * atomic<size_t> next_location = 8; //地址
 * VBucket VBuckets[max_num];
 */

static const int VBUCKET_NUM = 1 << 26;
static const int VOVER_NUM = 1 << 23;
static const uint32_t VENTRY_NUM = 4;


// 64-byte allign
struct VBucket {
  // reset to zero at memset
  std::atomic<uint32_t> next_free;
  uint32_t bucket_next; // offset = (VBucket_next - 1) * sizeof(VBucket) + 8

  // 4 * 13 = 52
  uint64_t entries[VENTRY_NUM];
  uint32_t inlinekeys[VENTRY_NUM];
  uint8_t extra[VENTRY_NUM];
  uint32_t is_overflow;
};


static inline bool rm_inlinecompare(size_t hashval, uint8_t extra, uint32_t inlinekey) {
  size_t val = extra;
  val <<= 32;
  val += inlinekey;
  hashval >>= 24;
  if (val != hashval)
    return 0;
  return 1;
}


class VOverflowIndex{
public:
  volatile std::atomic<size_t> *next_location; // open to index
  VOverflowIndex() {}

  void open() {
    ptr = reinterpret_cast<char *>(map_anonymouse(VOVER_NUM * sizeof(VBucket) + 64));
    /* madvise(ptr, VOVER_NUM * sizeof(VBucket), MADV_RANDOM); */
    next_location = reinterpret_cast<std::atomic<size_t> *>(ptr);
    next_location->store(64, std::memory_order_release);
  }

  void put(uint64_t over_offset, size_t hash_val, size_t data) {
    volatile VBucket *bucket = reinterpret_cast<volatile VBucket *>(ptr + over_offset);
    if (!bucket->is_overflow) {
      size_t next_free = bucket->next_free.fetch_add(1, std::memory_order_acq_rel);
      if (next_free < VENTRY_NUM) {
        bucket->entries[next_free] = data;
        bucket->extra[next_free] = hash_val >> 56; // 高8位
        bucket->inlinekeys[next_free] = (hash_val >> 24) & 0xffffffff; // 中间32
        return;
      }

      if (next_free == VENTRY_NUM) {
        uint64_t maybe_next = next_location->fetch_add(sizeof(VBucket));
        uint32_t index = (maybe_next - 64) / sizeof(VBucket) + 1;
        bucket->bucket_next = index;
        bucket->is_overflow = 1;
      }
    }

    size_t index;
    while ((index = bucket->bucket_next) == 0);
    uint64_t next_offset = 64 + (index - 1) * sizeof(VBucket);
    put(next_offset, hash_val, data);
  }


  size_t get(size_t over_offset, size_t hash_val, void *res, bool multi) {
    int count = 0;
    VBucket *bucket = reinterpret_cast<VBucket *>(ptr + over_offset);
    uint32_t num = std::min(VENTRY_NUM, bucket->next_free.load(std::memory_order_acquire));
    for (int i = 0; i < num; ++i) {
      size_t data = bucket->entries[i];
      if (rm_inlinecompare(hash_val, bucket->extra[i], bucket->inlinekeys[i])) {
        *(int64_t *) res = data;
        res = (char *)res + 8; 
        count++;
        if (!multi)
          return count;
      }
    }

    size_t index;
    if ((index = bucket->bucket_next) == 0)
      return count;

    uint64_t next_offset = 64 + (index - 1) * sizeof(VBucket);
    count += get(next_offset, hash_val, res, multi);
    return count;

  }
  ~VOverflowIndex() {
    if (ptr) {
      munmap(ptr, VBUCKET_NUM * sizeof(VBucket) + 64);
    }
  }

private:
  char *ptr;
};

class VIndex
{
public:
  VIndex() {
    overflowindex = new VOverflowIndex;
  }

  void open() {
    hash_ptr = reinterpret_cast<char *>(map_anonymouse(VBUCKET_NUM * sizeof(VBucket)));
    /* madvise(hash_ptr, VBUCKET_NUM * sizeof(VBucket), MADV_RANDOM); */ // 感觉也是有局部性的
    overflowindex->open();
  }

  ~VIndex() {
    if (hash_ptr)
      munmap(hash_ptr, sizeof(VBucket) * VBUCKET_NUM);
  }


  void put(size_t hash_val, size_t data) {
    size_t bucket_location = hash_val & (VBUCKET_NUM - 1);
    volatile VBucket *bucket = reinterpret_cast<volatile VBucket *>(hash_ptr + bucket_location * sizeof(VBucket));
    if (!bucket->is_overflow) {
      size_t next_free = bucket->next_free.fetch_add(1, std::memory_order_acq_rel); // both read write
      if (next_free < VENTRY_NUM) {
        bucket->entries[next_free] = data;
        bucket->extra[next_free] = hash_val >> 56; // 高8位
        bucket->inlinekeys[next_free] = (hash_val >> 24) & 0xffffffff; // 中间32
        return;
      }

      if (next_free == VENTRY_NUM) {
        uint64_t maybe_next = overflowindex->next_location->fetch_add(sizeof(VBucket), std::memory_order_acq_rel);
        uint32_t index = (maybe_next - 64) / sizeof(VBucket) + 1;
        bucket->bucket_next = index;
        bucket->is_overflow = 1;
      }
    }

    // waiting for VBucket allocation
    size_t index;
    while ((index = bucket->bucket_next) == 0);
    uint64_t next_offset = 64 + (index - 1) * sizeof(VBucket);
    overflowindex->put(next_offset, hash_val, data);
  }


  int get(size_t hash_val, void *res, bool multi) {
    size_t VBucket_location = hash_val & (VBUCKET_NUM - 1);
    int count = 0;
    VBucket *bucket = reinterpret_cast<VBucket *>(hash_ptr + VBucket_location * sizeof(VBucket));
    uint32_t num = std::min(VENTRY_NUM, bucket->next_free.load(std::memory_order_acquire));
    for (int i = 0; i < num; ++i) {
      size_t offset = bucket->entries[i];
      if (rm_inlinecompare(hash_val, bucket->extra[i], bucket->inlinekeys[i])) {
        *(uint64_t *) res = offset;
        res = (char *) res + 8;
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

    uint64_t next_offset = 64 + (index - 1) * sizeof(VBucket);
    count += overflowindex->get(next_offset, hash_val, res, multi);
    return count;
  }


private:
  char *hash_ptr;
  VOverflowIndex *overflowindex;
};
