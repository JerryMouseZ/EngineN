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
#include <liburing.h>
#include "comm.h"
#include "send_recv.hpp"

extern thread_local UserQueue *consumer_q;

class Engine
{
public:
  Engine();
  ~Engine();

  void open(std::string aep_path, std::string disk_path);
  
  void write(const User *user);

  size_t local_read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res);
  
  size_t read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  // for remote
  // 创建listen socket，尝试和别的机器建立两条连接
  void connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num);

  size_t remote_read(uint8_t select_column, uint8_t where_column, const void *column_key, size_t key_len, void *res);

private:
  static std::string column_str(int column);

  void connect(std::vector<info_type> &infos, int num, int host_index);

  int get_backup_index();

  int get_request_index();

  int get_another_request_index();

  void start_handlers();

  void request_sender();

  void response_recvier();

  void request_handler();
  
  void term_sending_request();

  void poll_send_req_cqe();

  void poll_send_response_cqe();

  void invalidate_fd(int sock);

  void disconnect();

private:
  Data *datas;
  Index *id_r;
  Index *uid_r;
  // salary need multi-index
  Index *sala_r;
  UserQueue *qs;
  std::thread *consumers;

  // for connection
  io_uring send_request_ring;
  io_uring recv_response_ring;

  io_uring recv_request_ring;
  io_uring send_response_ring;
  int host_index;
  int listen_fd;
  bool alive[4];
  int send_fds[4];
  int recv_fds[4];
  int data_fd;
  int data_recv_fd;
  CircularFifo<1<<16> *send_fifo;
  std::thread *req_sender;
  std::thread *rep_recvier;
  std::thread *req_handler;
};
