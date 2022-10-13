#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <pthread.h>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <thread>
#include "config.hpp"
#include "queue.hpp"

#include "index.hpp"
#include "vindex.hpp"
#include "data.hpp"
#include "comm.h"
#include "uv.h"

extern thread_local UserQueue *consumer_q;
struct DataTransMeta {
  uint32_t local_user_cnt; // 本地有的user数量
  uint32_t recived_user_cnt; // 之前已经接收过的user数量
};

std::string column_str(int column);

class Engine
{
public:
  Engine();
  ~Engine();

  bool open(std::string aep_path, std::string disk_path);

  void write(const User *user);

  size_t local_read(int32_t select_column,
                    int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  size_t sync_read(int32_t select_column,
                    int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  size_t read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  // for remote
  // 创建listen socket，尝试和别的机器建立两条连接
  void connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num, bool is_new_create, const char *aep_dir);

  size_t remote_read_once(int nb_idx, uint8_t select_column, uint8_t where_column, const void *column_key, size_t key_len, void *res);

  size_t remote_read_broadcast(uint8_t select_column, uint8_t where_column, const void *column_key, size_t key_len, void *res);

  int get_request_index();

  int get_another_request_index();

  int get_backup_index();

  void do_peer_data_sync();

  bool try_notify_enter(int neighbor_idx, int qid);
  void end_notify_enter();
  void notify_local_queue_exit_sync(int neighbor_idx, int qid);
  void notify_remote_queue_exit_sync(int neighbor_idx, int qid);

  void sync_send_handler(int qid);

  void init_set_peer_sync();
private:

  void connect(std::vector<info_type> &infos, int num, bool is_new_create);

  void start_handlers();

  /* void request_sender(); */

  /* void response_recvier(); */

  void request_handler(int index, int *req_recv_fds);

  void term_sending_request();

  void invalidate_fd(int sock);

  void disconnect();

  void ask_peer_quit();
  // for backup

  int do_exchange_meta(DataTransMeta send_meta[MAX_NR_CONSUMER], DataTransMeta recv_meta[MAX_NR_CONSUMER], int index);
  int do_exchange_data(DataTransMeta send_meta[MAX_NR_CONSUMER], DataTransMeta recv_meta[MAX_NR_CONSUMER], int index);
  void build_index(int qid, int begin, int end, Index *id_index, Index *uid_index, Index *salary_index, Data *datap);
  void start_sync_handlers();

  void sync_resp_handler(int qid);

  bool any_local_in_sync();
  bool any_rm_in_sync();

  void waiting_all_exit_sync();

public:
  std::atomic<uint64_t> local_in_sync_cnt;
  std::atomic<uint64_t> remote_in_sync_cnt;
  // 标记每个队列的同步状态
  std::atomic<bool> remote_in_sync[4][MAX_NR_CONSUMER];
  std::atomic<bool> local_in_sync[MAX_NR_CONSUMER];
  /* std::atomic<bool> in_sync_visible; */


private:
  volatile bool exited;
  Data *datas;

  // indexes
  Index *id_r;
  Index *uid_r;
  Index *sala_r;

    // write buffer
  UserQueue *qs;
  SyncQueue *sync_qs;
  std::thread consumers[MAX_NR_CONSUMER];
  std::thread sync_senders[MAX_NR_CONSUMER];

  int host_index;
  int neighbor_index[3];
  int listen_fd;
  volatile bool alive[4];
  std::thread *req_handler[10];
  std::thread *req_weak_handler[10];
  std::thread *req_handlerall[4 * 10];
  std::thread *sync_send_thread[MAX_NR_CONSUMER];
  std::thread *sync_resp_thread[MAX_NR_CONSUMER];
  
  RemoteState remote_state; // 用来存当前有多少remote的user吧

  int req_send_fds[50];
  int req_weak_send_fds[50];
  int req_recv_fds[50];
  int req_weak_recv_fds[50];
  int send_fdall[4][50];
  int recv_fdall[4][50];
  int sync_send_fdall[4][MAX_NR_CONSUMER];
  int sync_recv_fdall[4][MAX_NR_CONSUMER];
public:
// remote indexes
  VIndex remote_id_r[4];
  VIndex remote_sala_r[4];
};
