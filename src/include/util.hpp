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
#define LOG 0 // 打印不应该出现的error（如column error, host index error）的打印
#define VLOG 1 // 控制打印每个请求都会显示的信息的打印，比较多（select等等）
#define QINFO 1 // 控制write buffer queue信息的打印
#define QDEBUG 1 // 控制write buffer queue debug信息的打印
#define INIT 0 // 控制本地初始化过程信息(queu data的打开等等)的打印
#define CONNECT 0 // 控制网络连接初始化过程信息(tcp连接、fd的初始化)的打印
#define SYNC_DATA 0 // 控制之前版本在构造函数中sync_data的信息打印
#define NETWORK 0 // 控制req、resp等每次过程中发生错误信息的打印
#define WBREAD 0 // 当发生write buffer还没写回就尝试读时，block信息的打印
#define COMHEAD 1 // 控制queue发生compact head时的信息打印
#define QROLLBACK 0 // 控制queue发生rollback时的信息打印
#define VSYNC 1 // 比较verbose的sync queue数据同步信息
#define SYNCSTATE 0 // 节点进入/退出sync状态的信息
#define ALIVE 0 // 节点alive状态退出信息

#define BROADCAST 1

// #define LOCAL 1
// #define BIND_PORT

// #define CHECK_THREAD_CNT
#define TIME_RECORD

#define START 64

static void prefault(char *ptr, size_t len, bool memset_clear)
{
  volatile long *reader = new long;
  std::thread *threads[16];
  size_t per_thread = len / 16;

  for (int i = 0; i < 16; ++i) {
    threads[i] = new std::thread([=]{
      if (memset_clear) {
        memset(&ptr[i * per_thread], 0, per_thread);
      } else {
        for (size_t j = i * per_thread; j < (i + 1) * per_thread  && j < len; j += 4096) {
          __sync_fetch_and_add(reader, ptr[j]);
          __builtin_prefetch(ptr + 4096 * 2, 1, 0);
        }
      }
    });
  }
  
  if (memset_clear) {
    int cnt = per_thread * 16;
    int rest = len - cnt;
    memset(&ptr[cnt], 0, rest);
  }

  for (int i = 0; i < 16; ++i) {
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

  prefault(ptr, len, hash_create);

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
