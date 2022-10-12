#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <thread>
#include "include/comm.h"
#include "include/config.hpp"
#include "include/engine.hpp"
#include "include/sync_queue.hpp"
#include "include/time.hpp"
#include "include/data.hpp"
#include "include/util.hpp"

enum CqeType { 
  SEND_DATA = 0,
  RECV_DATA,
  SEND_RESP,
  RECV_RESP,
  CQE_TYPE_CNT  
};

void print_elapse(const char *desc, const duration_t &dur) {
  DEBUG_PRINTF(0, "%s: %s elapse time: %lf s\n",
    this_host_info, desc, dur.count());
}

void RemoteState::open(std::string fname, bool *is_new_create) {
  next_user_index = static_cast<volatile uint32_t *>(calloc(MAX_NR_CONSUMER, sizeof(uint32_t)));
}

int Engine::do_exchange_meta(DataTransMeta send_meta[MAX_NR_CONSUMER], DataTransMeta recv_meta[MAX_NR_CONSUMER], int index) {
  DEBUG_PRINTF(SYNC_DATA, "%s start peer data sync\n", this_host_info);
  int len = sizeof(DataTransMeta) * MAX_NR_CONSUMER;
  memset(send_meta, 0, len);
  memset(recv_meta, 0, len);
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    auto remote_user_cnt = 0;
    auto user_cnt = qs[i].head->load();
    send_meta[i].local_user_cnt = user_cnt;
    send_meta[i].recived_user_cnt = remote_user_cnt;
  }
  
  // send and recv
  int ret = send_all(sync_send_fdall[index][0], send_meta, len, 0);
  DEBUG_PRINTF(ret == len, "send meta error\n");
  if (ret < 0)
    return -1;
  assert(ret == len);
  DEBUG_PRINTF(SYNC_DATA, "%s send_meta done\n", this_host_info);
  ret = recv_all(sync_recv_fdall[index][0], recv_meta, len, MSG_WAITALL);
  DEBUG_PRINTF(ret == len, "recv meta error\n");
  if (ret <= 0)
    return -1;
  assert(ret == len);
  DEBUG_PRINTF(SYNC_DATA, "%s recv_meta done\n", this_host_info);
  return 0;
}

// 不用再去管对不对齐的问题了
int Engine::do_exchange_data(DataTransMeta local[MAX_NR_CONSUMER], DataTransMeta remote[MAX_NR_CONSUMER], int index) {
  bool need_to_send = false,
       need_to_recv = false;
  bool recv_success = true;
  bool send_success = true;
  // 需要检查写入到了多少，还要检查索引建立到了多少
  auto recv_fn = [&](int i) {
    int ret = 0;
    if (need_to_recv) {
      int recv_count = remote[i].local_user_cnt - local[i].recived_user_cnt;
      RemoteUser *recv_buffer = (RemoteUser *) map_anonymouse(recv_count * sizeof(RemoteUser));
      void *src;
      int len = recv_count * sizeof(RemoteUser);
      int ret = recv_all(sync_recv_fdall[index][i], recv_buffer, len, MSG_WAITALL);
      if (ret != len) {
        recv_success = false;
        DEBUG_PRINTF(INIT, "send sync failed\n");
        return;
      }
      for (int j = 0; j < recv_count; ++j) {
        /* fprintf(stderr, "get remote data %ld-%ld\n", recv_buffer[j].id, recv_buffer[j].salary); */
        remote_id_r[index].put(recv_buffer[j].id, recv_buffer[j].salary);
        remote_sala_r[index].put(recv_buffer[j].salary, recv_buffer[j].id);
      }
      munmap(recv_buffer, recv_count * sizeof(RemoteUser));
    } // end if need to recv
  };

  auto exchange_fn = [&] (int i) {
    RemoteUser *send_buffer = nullptr;
    int send_count = 0;
    int recv_count = 0;
    if (local[i].local_user_cnt > remote[i].recived_user_cnt) {
      need_to_send = true;
      send_count = local[i].local_user_cnt - remote[i].recived_user_cnt;
      send_buffer = (RemoteUser *)map_anonymouse(send_count * sizeof(RemoteUser));
      for (int j = 0; j < send_count; ++j) {
        const User *user = datas[i].data_read(j);
        send_buffer[j].id = user->id;
        send_buffer[j].salary = user->salary;
      }
    }
    if (remote[i].local_user_cnt > local[i].recived_user_cnt) {
      need_to_recv = true;
      recv_count = remote[i].local_user_cnt - local[i].recived_user_cnt;
    }
    std::thread *reciver = nullptr;
    if (need_to_recv) {
      reciver = new std::thread(recv_fn, i);
    }

    if (need_to_send) {
      int ret = 0;
      void *src;
      int len = send_count * sizeof(RemoteUser);
      ret = send_all(sync_send_fdall[index][i], send_buffer, len, 0);
      munmap(send_buffer, send_count * sizeof(RemoteUser));
      if (ret != len) {
        send_success = false;
        DEBUG_PRINTF(INIT, "send sync failed\n");
        return;
      }
    }

    // waiting for reciver
    if (reciver) {
      reciver->join();
      delete reciver;
      reciver = nullptr;
    }

    if (recv_success) {
      remote_state.get_next_user_index()[i] = remote[i].local_user_cnt;
    }
  };


  /* std::thread *exchange_workers[16]; */
#pragma omp parallel for num_threads(16)
  for (int i = 0; i < 16; ++i) {
    exchange_fn(i);
  }

  if (!recv_success) {
    return -1;
  }

  return 0;
}

// 其实就是要发16个queue而已，我们可以开16个线程来做这件事
void Engine::do_peer_data_sync() {
  duration_t dsync_time(0);
  start_time_record();

  DataTransMeta send_meta[3][MAX_NR_CONSUMER];
  DataTransMeta recv_meta[3][MAX_NR_CONSUMER];
#pragma omp parallel for num_threads(3)
  for (int i = 0; i < 3; ++i) {
    int index = neighbor_index[i];
    int ret = do_exchange_meta(send_meta[i], recv_meta[i], index);
    if (ret) {
      DEBUG_PRINTF(INIT, "[sync] exchange meta failed\n");
      continue;
    }

    ret = do_exchange_data(send_meta[i], recv_meta[i], index);
    if (ret) {
      DEBUG_PRINTF(INIT, "exchange data failed\n");
      continue;
    }

    for (int j = 0; j < MAX_NR_CONSUMER; j++) {
      DEBUG_PRINTF(SYNC_DATA, "\t[%d] user: [%d, %d)\n", 
                   i, send_meta[i][j].recived_user_cnt, recv_meta[i][j].local_user_cnt);
    }
  }
  end_time_record(&dsync_time);
  print_elapse("peer data sync", dsync_time);
}

