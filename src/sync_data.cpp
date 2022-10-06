#include <cassert>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <liburing.h>
#include <sys/socket.h>
#include <thread>
#include "include/comm.h"
#include "include/config.hpp"
#include "include/engine.hpp"
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
  next_user_index = static_cast<volatile uint32_t *>(map_file(fname.c_str(), MAX_NR_CONSUMER * sizeof(uint32_t), is_new_create));
}

int Engine::do_exchange_meta(DataTransMeta send_meta[MAX_NR_CONSUMER], DataTransMeta recv_meta[MAX_NR_CONSUMER]) {
  DEBUG_PRINTF(0, "%s start peer data sync\n", this_host_info);
  int len = sizeof(DataTransMeta) * MAX_NR_CONSUMER;
  memset(send_meta, 0, len);
  memset(recv_meta, 0, len);
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    auto remote_user_cnt = remote_state.get_next_user_index()[i];
    auto user_cnt = qs[i].head->load();
    send_meta[i].local_user_cnt = user_cnt;
    send_meta[i].recived_user_cnt = remote_user_cnt;
  }
  
  // send and recv
  int ret = send_all(data_fd[0], send_meta, len, 0);
  DEBUG_PRINTF(ret == len, "send meta error\n");
  if (ret < 0)
    return -1;
  assert(ret == len);
  DEBUG_PRINTF(0, "%s send_meta done\n", this_host_info);
  ret = recv_all(data_recv_fd[0], recv_meta, len, MSG_WAITALL);
  DEBUG_PRINTF(ret == len, "recv meta error\n");
  if (ret <= 0)
    return -1;
  assert(ret == len);
  DEBUG_PRINTF(0, "%s recv_meta done\n", this_host_info);
  return 0;
}

int Engine::do_exchange_data(DataTransMeta local[MAX_NR_CONSUMER], DataTransMeta remote[MAX_NR_CONSUMER]) {
  bool need_to_send = false,
       need_to_recv = false;
  int send_chunks[MAX_NR_CONSUMER] = {0};
  int recv_chunks[MAX_NR_CONSUMER] = {0};
  int send_unaligned1[MAX_NR_CONSUMER] = {0};
  int recv_unaligned1[MAX_NR_CONSUMER] = {0};
  int send_unaligned2[MAX_NR_CONSUMER] = {0};
  int recv_unaligned2[MAX_NR_CONSUMER] = {0};


  bool recv_success = true;
  bool send_success = true;
  // 需要检查写入到了多少，还要检查索引建立到了多少
  auto recv_fn = [&](int i) {
    int ret = 0;
    /* for (int i = 0; i < 16; ++i) { */
    if (need_to_recv) {
      int start_chunk = remote[i].recived_user_cnt / 15;
      void *src;
      int len = 0;
      if (recv_unaligned1[i]) {
        int start_position = remote[i].recived_user_cnt % 15;
        UserArray *users = remote_datas[i].get_pmem_users();
        src = &users[start_chunk].data[start_position];
        len = recv_unaligned1[i] * sizeof(User);
        ret = recv_all(data_recv_fd[i], src, len, MSG_WAITALL);
        if (ret != len) {
          recv_success = false;
          fprintf(stderr, "recv sync failed\n");
          return;
        }
        start_chunk++;
      }
      if (recv_chunks[i]) {
        UserArray *users = remote_datas[i].get_pmem_users();
        src = &users[start_chunk];
        len = 4096 * recv_chunks[i];
        int ret = recv_all(data_recv_fd[i], src, len, MSG_WAITALL);
        /* for (int j = start_chunk; j < start_chunk + recv_chunks[i]; ++j) { */
        /*   for (int k = 0; k < 15; ++k) { */
        /*     fprintf(stderr, "recving id = %ld\n", users[j].data[k].id); */
        /*   } */
        /* } */
        if (ret != len) {
          recv_success = false;
          fprintf(stderr, "recv sync failed\n");
          return;
        }
        start_chunk += recv_chunks[i];
      }
      if (recv_unaligned2[i]) {
        UserArray *users = remote_datas[i].get_pmem_users();
        src = &users[start_chunk];
        len = recv_unaligned2[i] * sizeof(User);
        int ret = recv_all(data_recv_fd[i], src, len, MSG_WAITALL);
        /* for (int j = 0; j < recv_unaligned2[i]; ++j) { */
        /*   fprintf(stderr, "recving id = %ld\n", users[start_chunk].data[j].id); */
        /* } */
        if (ret != len) {
          recv_success = false;
          fprintf(stderr, "recv sync failed\n");
          return;
        } // if ret
      }// if recv unaligned2
    } // end if need to recv
    /* } // end for */
  };

  auto exchange_fn = [&] (int i) {
    if (local[i].local_user_cnt > remote[i].recived_user_cnt) {
      need_to_send = true;
      int count = local[i].local_user_cnt - remote[i].recived_user_cnt;
      send_unaligned1[i] = (15 - (remote[i].recived_user_cnt) % 15) % 15;
      count -= send_unaligned1[i];
      send_chunks[i] = count / 15;
      send_unaligned2[i] = count % 15;
    }
    if (remote[i].local_user_cnt > local[i].recived_user_cnt) {
      need_to_recv = true;
      int count = remote[i].local_user_cnt - local[i].recived_user_cnt;
      recv_unaligned1[i] = (15 - (local[i].recived_user_cnt % 15)) % 15;
      count -= recv_unaligned1[i];
      recv_chunks[i] = count / 15;
      recv_unaligned2[i] = count % 15;
    }
    std::thread *reciver = nullptr;
    if (need_to_recv) {
      reciver = new std::thread(recv_fn, i);
    }

    if (need_to_send) {
      int ret = 0;
      if (send_chunks[i] || send_unaligned1[i] || send_unaligned2[i]) {
        int start_chunk = local[i].recived_user_cnt / 15;
        int start_position = local[i].recived_user_cnt % 15;
        void *src;
        int len = 0;
        if (send_unaligned1[i]) {
          UserArray *users = datas[i].get_pmem_users();
          src = &users[start_chunk].data[start_position];
          len = send_unaligned1[i] * sizeof(User);
          ret = send_all(data_fd[i], src, len, 0);
          if (ret != len) {
            send_success = false;
            fprintf(stderr, "send sync failed\n");
            return;
          }
          start_chunk++;
        }
        if (send_chunks[i]) {
          UserArray *users = datas[i].get_pmem_users();
          src = &users[start_chunk];
          /* for (int j = start_chunk; j < start_chunk + send_chunks[i]; ++j) { */
          /*   for (int k = 0; k < 15; ++k) { */
          /*     fprintf(stderr, "sending id = %ld\n", users[j].data[k].id); */
          /*   } */
          /* } */
          len = 4096 * send_chunks[i];
          ret = send_all(data_fd[i], src, len, 0);
          if (ret != len) {
            send_success = false;
            fprintf(stderr, "send sync failed\n");
            return;
          }
          start_chunk += send_chunks[i];
        }
        if (send_unaligned2[i]) {
          UserArray *users = datas[i].get_pmem_users();
          src = &users[start_chunk];
          /* for (int j = 0; j < send_unaligned2[i]; ++j) { */
          /*   fprintf(stderr, "sending id = %ld\n", users[start_chunk].data[j].id); */
          /* } */
          len = send_unaligned2[i] * sizeof(User);
          ret = send_all(data_fd[i], src, len, 0);
          if (ret != len) {
            send_success = false;
            fprintf(stderr, "send sync failed\n");
            return;
          }
        }
      }
    }

    // waiting for reciver
    if (reciver) {
      reciver->join();
      delete reciver;
      reciver = nullptr;
    }

    if (recv_success) {
      if (recv_chunks[i] || recv_unaligned1[i] || recv_unaligned2[i]) {
        remote_state.get_next_user_index()[i] = remote[i].local_user_cnt;
      }
    }
  };


  std::thread *exchange_workers[16];

  for (int i = 0; i < 16; ++i) {
    exchange_workers[i] = new std::thread(exchange_fn, i);
  }

  for (int i = 0; i < 16; ++i) {
    exchange_workers[i]->join();
    DEBUG_PRINTF(INIT, "start build remote index[%d] range [0, %d)\n", 
      i, remote[i].local_user_cnt);
    build_index(i, 0, remote[i].local_user_cnt, remote_id_r, remote_uid_r, remote_sala_r, &remote_datas[i]);
    delete exchange_workers[i];
  }

  if (recv_success) {
    return 0;
  }

  return -1;
}

// 其实就是要发16个queue而已，我们可以开16个线程来做这件事
void Engine::do_peer_data_sync() {
  duration_t dsync_time(0);
  start_time_record();

  DataTransMeta send_meta[MAX_NR_CONSUMER];
  DataTransMeta recv_meta[MAX_NR_CONSUMER];
  int ret = do_exchange_meta(send_meta, recv_meta);
  if (ret) {
    fprintf(stderr, "[sync] exchange meta failed\n");
    return;
  }

  ret = do_exchange_data(send_meta, recv_meta);
  if (ret) {
    fprintf(stderr, "exchange data failed\n");
    return;
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(0, "\t[%d] user: [%d, %d)\n", 
                 i, send_meta[i].recived_user_cnt, recv_meta[i].local_user_cnt);
  }
  end_time_record(&dsync_time);
  print_elapse("peer data sync", dsync_time);
}
