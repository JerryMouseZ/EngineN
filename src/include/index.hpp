#pragma once
#include "data.hpp"
#include <cassert>
#include <cstdint>
#include <cstddef>
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

static const int ENTRY_NUM = 30;
// 溢出链只是保证正确性的东西，没指望他省多少内存
struct Bucket
{
  // 直接在下一个位置写，也能知道是不是满了
  std::atomic<uint64_t> next_location; // 为了对齐，不然uint8就够用了，数组下标，初始化为0，不是地址!!!
  uint64_t entries[ENTRY_NUM];
  std::atomic<uint64_t> next;
};

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
    assert(0);
  }
  return 0;
}


static void * res_copy(const User *user, void *res, int32_t select_column) {
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
  default: assert(0); // wrong
  }
  return res;
}



const int BUCKET_NUM = 2560000;
class OverflowIndex {
public:
  OverflowIndex() : ptr(nullptr) {}


  ~OverflowIndex() {
    if (ptr) {
      munmap(ptr, BUCKET_NUM * sizeof(Bucket));
    }
  }


  void map_file(const std::string &filename) {
    bool hash_create = false;
    if (access(filename.c_str(), F_OK)) {
      hash_create = true;
    }

    int fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
    assert(fd);
    ftruncate(fd, BUCKET_NUM * sizeof(Bucket));
    ptr = reinterpret_cast<char*>(mmap(0, BUCKET_NUM * sizeof(Bucket), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    if (hash_create)
      memset(ptr, 0, BUCKET_NUM * sizeof(Bucket));
    next_location = reinterpret_cast<std::atomic<size_t> *>(ptr);
    *next_location = 8;
  }

  void put(uint64_t over_offset, uint64_t data_offset) {
    Bucket *bucket = reinterpret_cast<Bucket *>(ptr + over_offset);
    size_t bucket_next = bucket->next_location.fetch_add(1);
    if (bucket_next < ENTRY_NUM) {
      bucket->entries[bucket_next] = data_offset;
      return;
    }

    if (bucket->next == 0) {
      // 这样保证同一个桶的溢出链不会分布在两个桶上，而且next不会丢失其中一个，不过这样可能会在溢出链的文件中留下不被使用的空洞，考虑到这种情况应该很少，就算了
      size_t old = 0;
      uint64_t maybe_next = next_location->fetch_add(sizeof(Bucket));
      bucket->next.compare_exchange_weak(old, maybe_next);
    }
    put(bucket->next, data_offset);
  }

  int get(uint64_t over_offset, const void *key, Data *data, int column, int select, void *res) {
    Bucket *bucket = reinterpret_cast<Bucket *>(ptr + over_offset);
    for (int i = 0; i < ENTRY_NUM; ++i) {
      uint64_t offset = bucket->entries[i];
      if (offset == 0) {
        return 0;
      } else {
        const User *tmp = data->data_read(offset);
        if (compare(key, tmp, column)) {
          res_copy(tmp, res, select);
        }
        return 1;
      }
    }
    if (bucket->next == 0)
      return 0;
    return get(over_offset, key, data, column, select, res);
  }

  char *ptr;
  std::atomic<size_t> *next_location;
};


template<class K>
static size_t calc_index(const K &key) {
  return std::hash<K>()(key);
}

template<class K>
class Index{
public:
  Index() {
    overflowindex = new OverflowIndex();
  }


  ~Index() {
    if (hash_ptr)
      munmap(hash_ptr, sizeof(Bucket) * BUCKET_NUM);
    delete overflowindex;
  }


  void Open(const std::string &path, const std::string &prefix) {
    std::string base_dir = path;
    if (path[path.size() - 1] != '/')
      base_dir.push_back('/');
    base_dir += prefix;

    std::string hash_file = base_dir + ".hash";
    std::string over_file = base_dir + ".over";

    bool hash_create = false;
    if (access(hash_file.c_str(), F_OK)) {
      hash_create = true;
    }

    int hash_fd = open(hash_file.c_str(), O_CREAT | O_RDWR, 0666);
    assert(hash_fd);
    ftruncate(hash_fd, BUCKET_NUM * sizeof(Bucket));
    hash_ptr = reinterpret_cast<char*>(mmap(0, BUCKET_NUM * sizeof(Bucket), PROT_READ | PROT_WRITE, MAP_SHARED, hash_fd, 0));
    if (hash_create)
      memset(hash_ptr, 0, BUCKET_NUM * sizeof(Bucket));

    overflowindex = new OverflowIndex();
    overflowindex->map_file(over_file);
  }


  void put(const K &key, uint64_t data_offset) {
    Bucket *bucket = reinterpret_cast<Bucket*>(hash_ptr);
    int64_t bucket_location = calc_index(key);
    size_t bucket_next = bucket->next_location.fetch_add(1);
    if (bucket_next < ENTRY_NUM) {
      bucket[bucket_location].entries[bucket_next] = data_offset;
      return;
    }

    // overflow
    if (bucket[bucket_location].next == 0) {
      // 这样保证同一个桶的溢出链不会分布在两个桶上，而且next不会丢失其中一个，不过这样可能会在溢出链的文件中留下不被使用的空洞，考虑到这种情况应该很少，就算了
      size_t old = 0;
      uint64_t maybe_next = overflowindex->next_location->fetch_add(sizeof(Bucket));
      bucket[bucket_location].next.compare_exchange_weak(old, maybe_next);
    }
    overflowindex->put(bucket[bucket_location].next, data_offset);
  }


  int get(const void *key, Data *data, int column, int select, void *res) {
    int64_t bucket_location = calc_index(key);
    Bucket *bucket = reinterpret_cast<Bucket*>(hash_ptr);
    for (int i = 0; i < ENTRY_NUM; ++i) {
      uint64_t offset = bucket[bucket_location].entries[i];
      if (offset == 0) {
        return 0;
      } else {
        const User *tmp = data->data_read(offset);
        if (compare(key, tmp, column)) {
          res_copy(tmp, res, select);
        }
        return 1;
      }
    }

    // overflow
    if (bucket[bucket_location].next == 0)
      return 0;
    return overflowindex->get(bucket[bucket_location].next, key, data, column, select, res);
  }

private:
  char *hash_ptr;
  OverflowIndex *overflowindex;
};
