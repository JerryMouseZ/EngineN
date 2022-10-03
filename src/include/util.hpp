#pragma once

#include <cstddef>
#include <sys/mman.h>
#include <fcntl.h>
#include <thread>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define ROUND_DIV(x, CONST_N) (((x) + CONST_N - 1) / CONST_N)
#define ROUND_UP(x, CONST_N) (ROUND_DIV(x, CONST_N) * CONST_N)

// if assert fail print
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
#define LOG 0
#define VLOG 1
#define QINFO 1
#define QDEBUG 1

#define TIME_RECORD

#define START 64

static void prefault(char *ptr, size_t len)
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

static void *map_file(const char *path, size_t len, bool *is_new_create)
{
  bool hash_create = false;
  if (access(path, F_OK)) {
    hash_create = true;
  }

  int fd = open(path, O_CREAT | O_RDWR, 0777);
  DEBUG_PRINTF(fd, "%s open error", path);
  if (hash_create) {
    int ret = posix_fallocate(fd, 0, len);
    DEBUG_PRINTF(ret >= 0, "%s posix_fallocate\n", path);
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

  if (is_new_create != nullptr) {
    *is_new_create = hash_create;    
  }

  return ptr;
}

static void *map_anonymouse(size_t len) {
  char *ptr = reinterpret_cast<char*>(mmap(0, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_POPULATE, -1, 0));
  memset(ptr, 0, len);
  return ptr;
}
