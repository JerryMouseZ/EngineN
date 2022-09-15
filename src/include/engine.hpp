#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <thread>
#include "queue.hpp"

#include "index.hpp"
#include "data.hpp"
#include "cs.hpp"

extern thread_local UserQueue *consumer_q;

class Engine
{
public:
  Engine();
  ~Engine();

  void open(std::string aep_path, std::string disk_path);
  
  // 创建listen socket，尝试和别的机器建立两条连接
  void connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num);
  
  void write(const User *user);


  size_t read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res);
  

private:
  static std::string column_str(int column);


private:
  Data *datas;
  Index *id_r;
  Index *uid_r;
  // salary need multi-index
  Index *sala_r;
  Connector *conn;
  UserQueue *qs;
  std::thread *consumers;
};
