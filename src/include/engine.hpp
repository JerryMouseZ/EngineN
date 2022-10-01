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

struct DataTransMeta {
  uint32_t ca_start;
  uint32_t ca_cnt;
  uint32_t user_start;
  uint32_t user_cnt;
};


class Engine
{
public:
  Engine();
  ~Engine();

  bool open(std::string aep_path, std::string disk_path);
  
  void write(const User *user);

  size_t local_read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  size_t backup_read(int32_t select_column,
                          int32_t where_column, const void *column_key, size_t column_key_len, void *res);
  
  size_t read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  // for remote
  // 创建listen socket，尝试和别的机器建立两条连接
  void connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num, bool is_new_create);

  void do_sync();

  size_t remote_read(uint8_t select_column, uint8_t where_column, const void *column_key, size_t key_len, void *res);

  int get_request_index();

  int get_another_request_index();

  int get_backup_index();

  void do_peer_data_sync();

private:
  static std::string column_str(int column);

  void connect(std::vector<info_type> &infos, int num, int host_index, bool is_new_create);

  void start_handlers();

  /* void request_sender(); */

  /* void response_recvier(); */

  void request_handler(int index, int req_recv_fds[], io_uring &ring);
  
  void term_sending_request();

  void invalidate_fd(int sock);

  void disconnect();

  void ask_peer_quit();
  // for backup
  /* void do_send_data(const ArrayTransControl &ctrl); */
  /* void do_recv_data(ArrayTransControl &ctrl); */
  /* void do_send_resp(const TransControl &ctrl); */
  /* void do_recv_resp(TransControl &ctrl); */
  bool finish_recv_meta(const DataTransMeta *recv_meta, ArrayTransControl &recv_data_ctrl);
  void finish_recv_data(const DataTransMeta *recv_meta);
  void finish_recv_resp(uint32_t *newest_remote_next);


private:
  Data *datas;
  Data *remote_datas;
  
  // indexes
  Index *id_r;
  Index *uid_r;
  Index *sala_r;

  // remote indexes
  Index *remote_id_r;
  Index *remote_uid_r;
  Index *remote_sala_r;
 
  // write buffer
  UserQueue *qs;
  std::thread *consumers;
  
  io_uring req_recv_ring[10];
  io_uring req_weak_recv_ring[10];

  int host_index;
  int listen_fd;
  volatile bool alive[4];
  /* int send_fds[4]; */
  /* int recv_fds[4]; */
  int data_fd;
  int data_recv_fd;
  /* std::thread *req_sender; */
  /* std::thread *rep_recvier; */
  std::thread *req_handler[10];
  std::thread *req_weak_handler[10];
  /* volatile bool exited; */
  RemoteState remote_state;

  int req_send_fds[50];
  int req_weak_send_fds[50];
  int req_recv_fds[50];
  int req_weak_recv_fds[50];
};
