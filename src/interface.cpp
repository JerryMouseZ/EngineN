#include "../inc/interface.h"
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string.h>
#include "include/data.hpp"
#include "include/engine.hpp"
#include <sys/time.h>

timeval begin, end;

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
  gettimeofday(&begin, NULL);
  Engine *engine = new Engine();
  bool is_new_creat = engine->open(aep_dir, disk_dir);
  // 应该初始化磁盘好了再去连接
  engine->connect(host_info, peer_host_info, peer_host_info_num, is_new_creat);
  gettimeofday(&end, NULL);
  DEBUG_PRINTF(0, "init time %lf s\n", (end.tv_usec - begin.tv_usec) / (double) (1000000) + end.tv_sec - begin.tv_sec);
  return engine;
}

void engine_deinit(void *ctx) {
  gettimeofday(&begin, NULL);
  DEBUG_PRINTF(0, "operating time %lf s\n", (begin.tv_usec - end.tv_usec) / (double) (1000000) + begin.tv_sec - end.tv_sec);
  if (ctx)
    delete reinterpret_cast<Engine*>(ctx);
}

void engine_write(void *ctx, const void *data, size_t len) {
  Engine *engine = (Engine *)ctx; 
  engine->write((const User *)data);
}


size_t engine_read(void *ctx, int32_t select_column,
                   int32_t where_column, const void *column_key, size_t column_key_len, void *res) {

  Engine *engine = (Engine *)ctx; 
  return engine->read(select_column, where_column, column_key, column_key_len, res);
}

bool operator==(const UserString &l, const UserString &r) {
    return memcmp(l.ptr, r.ptr, 128) == 0;
}
