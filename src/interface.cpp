#include "../inc/interface.h"
#include <cstdint>
#include <iostream>
#include <string.h>
#include "include/engine.hpp"


void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
  Engine *engine = new Engine();
  engine->open(aep_dir, disk_dir);
  return engine;
}

void engine_deinit(void *ctx) {
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
