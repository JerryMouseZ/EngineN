#include "include/sync_queue.hpp"
#include "include/comm.h"
#include "include/config.hpp"
#include "include/engine.hpp"
#include "include/util.hpp"
#include "uv.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <sys/mman.h>
#include <sys/socket.h>

constexpr size_t EACH_REMOTE_DATA_FILE_LEN = EACH_NR_USER * sizeof(RemoteUser);

struct sync_send_param {
  ssize_t rest;
  char *write_pos;
  volatile bool *alive;
  Engine *eg;
  char *buf;
  size_t cur_buf_size;
  uint32_t current_send_cnt;
  int recv_fd;
  int qid;
  int neighbor_idx;
};

struct sync_send_client {
  uv_tcp_t handler;
  sync_send_param param;
};


void Engine::sync_send_handler(int qid) {
  SyncQueue &queue = sync_qs[qid];
  RemoteUser *send_start;
  int64_t sync_flag;
  int waiting_times = 0;
  while (true) {
    size_t pos = queue.tail;
    int64_t pop_cnt = queue.head - pos;
    pop_cnt = pop_cnt < 2048 ? pop_cnt : 2048; // 64k is the best package size
    if (pop_cnt > 0) {
      // 重新进入同步状态
      bool v = false;
      if (local_in_sync[qid].compare_exchange_weak(v, true)) {
        local_in_sync_cnt.fetch_add(1);
      }

      // 要向3个发
      for (int i = 0; i < 3; ++i) {
        int neighbor_idx = neighbor_index[i];
        int ret = send(sync_send_fdall[neighbor_idx][qid], &pop_cnt, sizeof(pop_cnt), 0);
        if (ret < 0) {
          alive[neighbor_idx] = false;
          continue;
        }
        ret = send_all(sync_send_fdall[neighbor_idx][qid], &queue.data[pos], pop_cnt * 16, MSG_ZEROCOPY);
        assert(ret == pop_cnt * 16);
        if (ret < 0)
          alive[neighbor_idx] = false;
      }
      queue.tail += pop_cnt;
    } else { // pop_cnt == 0
      if (waiting_times++ >= 20) {
        waiting_times = 0;
        // send sync flag
        sync_flag = 0;
        for (int i = 0; i < 3; ++i) {
          int neighbor_idx = neighbor_index[i];
          int ret = send(sync_send_fdall[neighbor_idx][qid], &sync_flag, sizeof(sync_flag), 0);
          if (ret < 0) {
            alive[neighbor_idx] = false;
            continue;
          }
        }
        // 阻塞接下来的本地写
        // set local flag
        bool v = true;
        if (local_in_sync[qid].compare_exchange_weak(v, false)) {
          local_in_sync_cnt.fetch_sub(1);
        }
        // waiting for wake up
        queue.consumer_yield();
        if (exited)
          return;
        // 有没有可能刚被唤醒但是东西还没到writer buffer呢，可以在writer buffer那里阻塞住，然后这样唤醒的时候就一定有东西
        for (int i = 0; i < 3; ++i) {
          int neighbor_idx = neighbor_index[i];
          sync_flag = -1;
          int ret = send(sync_send_fdall[neighbor_idx][qid], &sync_flag, sizeof(sync_flag), 0);
          if (ret < 0) {
            alive[neighbor_idx] = false;
            continue;
          }
        }
        // 接收response
        /* for (int i = 0; i < 3; ++i) { */
        /*   int neighbor_idx = neighbor_index[i]; */
        /*   int ret = recv(sync_send_fdall[neighbor_idx][qid], &sync_flag, sizeof(sync_flag), 0); */
        /*   if (ret < 0) { */
        /*     alive[neighbor_idx] = false; */
        /*     continue; */
        /*   } */
        /* } */
      } else {
        usleep(1);
      } // waiting time > 20
    } // pop cnt > 0
  } // while
}


void Engine::init_set_peer_sync() {
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    auto local_cnt = qs[i].head->load();
    sync_send msg;
    RemoteUser *buf;
    const User *user;
    int ret, nbytes;

    msg.cnt = 0;
    // 向所有节点发送同步完成的请求
    for (int nb_i = 0; nb_i < 3; nb_i++) {
      int neighbor_idx = neighbor_index[nb_i];
      ret = send(sync_send_fdall[neighbor_idx][i], &msg, sizeof(msg), 0);
      if (ret < 0) {
        DEBUG_PRINTF(0, "init send header sync error\n");
      }
      assert(ret == sizeof(msg));
    }
    DEBUG_PRINTF(local_cnt, "%s: send to 3 peers q[%d].local_cnt = %ld\n", this_host_info, i, local_cnt);
  }
}


struct sync_resp_param {
  SyncQueue *sync_q;
  Engine *eg;
  size_t cur_buf_size;
  char *buf;
  int neighbor_idx;
  int qid;
  int neighbor_index[3];
  size_t rest; // 一个包剩下的大小
};

void sync_resp_alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  sync_resp_param *param = (sync_resp_param *)handle->data;
  buf->base = param->buf;
  buf->len = param->cur_buf_size;
}

// 一个线程处理所有queue的resp
void process_sync_resp(uv_stream_t *client, ssize_t nread, const uv_buf_t *uv_buf) {
  sync_resp_param *param = (sync_resp_param *)client->data;
  if (nread < 0) {
    if (nread != UV_EOF)
      DEBUG_PRINTF(0, "Read error %s\n", uv_err_name(nread));
    uv_read_stop(client);
    return;
  }
  // TODO: 何时会读到0？
  if (nread == 0) {
    DEBUG_PRINTF(0, "Read error nread = 0\n");
    return;
  }


  int qid = param->qid;
  char *buf = uv_buf->base;
  uint32_t *neighbor_local_cnt = param->sync_q->neighbor_local_cnt;
  uint32_t resp_cnt;

  // 收到退出的sync的请求的时候需要回应
  while (nread > 0) {
    if (param->rest == 0) { // 接收header
      resp_cnt = ((sync_resp *)buf)->cnt;
      nread -= sizeof(sync_resp);
      buf += sizeof(sync_resp);
      // 进入sync状态的标记
      if (resp_cnt == -1 || resp_cnt > 0) {
        bool v = false;
        if (param->eg->remote_in_sync[param->neighbor_idx][qid].compare_exchange_weak(v, true)) {
          param->eg->remote_in_sync_cnt.fetch_add(1);
        }
        continue;
      }

      // 退出sync状态的标记
      param->rest = resp_cnt;
      if (resp_cnt == 0) {
        // 如果没有更多的数据了就认为同步完了
        if(nread == 0) {
          DEBUG_PRINTF(0, "remote exit sync flag %d:%d\n", param->neighbor_idx, qid);
          bool v = true;
          if (param->eg->remote_in_sync[param->neighbor_idx][qid].compare_exchange_weak(v, false)) {
            size_t cur = param->eg->remote_in_sync_cnt.fetch_sub(1);
            DEBUG_PRINTF(0, "cur : %ld\n", cur);
          } else {
            DEBUG_PRINTF(0, "remote queue %d:%d already exit: res : %ld, status : %d\n", param->neighbor_idx, qid, param->eg->remote_in_sync_cnt.load(), param->eg->remote_in_sync[param->neighbor_idx][qid].load());
          }
        }
      }
    }
    else { // 接收数据
      RemoteUser *user = (RemoteUser *) buf;
      int recv_user_cnt = nread / 16;
      for (int i = 0; i < recv_user_cnt; ++i) {
        param->eg->remote_id_r->put(user[i].id, user[i].salary);
        param->eg->remote_sala_r->put(user[i].salary, user[i].id);
      }
      nread -= recv_user_cnt * 16;
      param->rest -= recv_user_cnt;
    }
  }

  /* auto now_min = neighbor_local_cnt[param->neighbor_index[0]]; */
  /* for (int nb_i = 1; nb_i < 3; nb_i++) { */
  /*   int neighbor_idx = param->neighbor_index[nb_i]; */
  /*   if (neighbor_local_cnt[neighbor_idx] < now_min) { */
  /*     now_min = neighbor_local_cnt[neighbor_idx]; */
  /*   } */
  /* } */
  /* param->sync_q->advance_tail(now_min); */
}

void Engine::sync_resp_handler() {
  uv_loop_t *loop = (uv_loop_s *)malloc(sizeof(uv_loop_t));
  uv_loop_init(loop);
  uv_tcp_t handler[3][MAX_NR_CONSUMER];
  sync_resp_param param[3][MAX_NR_CONSUMER];
  int ret;

  for (int nb_i = 0; nb_i < 3; nb_i++) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < MAX_NR_CONSUMER; i++) {
      ret = uv_tcp_init(loop, &handler[nb_i][i]);
      DEBUG_PRINTF(ret == 0, "sync resp handler uv tcp init error\n");
      ret = uv_tcp_open(&handler[nb_i][i], sync_recv_fdall[neighbor_idx][i]);
      DEBUG_PRINTF(ret == 0, "sync resp handler uv tcp open error\n");

      param[nb_i][i].sync_q = &sync_qs[i];
      param[nb_i][i].eg = this;
      param[nb_i][i].neighbor_idx = neighbor_idx;
      param[nb_i][i].qid = i;
      for (int j = 0; j < 3; j++) {
        param[nb_i][i].neighbor_index[j] = neighbor_index[j];
      }

      // 64k是最佳的buffer大小
      param[nb_i][i].cur_buf_size = 1 << 15;
      param[nb_i][i].buf = (char *)map_anonymouse(1 << 16);
      handler[nb_i][i].data = &param[nb_i][i]; 
      uv_read_start((uv_stream_t *)&handler[nb_i][i], sync_resp_alloc_buffer, process_sync_resp);
    }
  }

  uv_run(loop, UV_RUN_DEFAULT);
  uv_loop_close(loop);
  free(loop);
  for (int i = 0; i < 3; ++i) {
    int neighbor_idx = neighbor_index[i];
    for (int j = 0; j < MAX_NR_CONSUMER; ++j) {
      munmap(param[i][j].buf, 1 << 16);
    }
  }
}


void Engine::start_sync_handlers() {
  remote_in_sync_cnt = 3 * MAX_NR_CONSUMER;
  auto sync_resp_fn = [&] () {
    sync_resp_handler();
  };
  sync_resp_thread = new std::thread(sync_resp_fn);

  init_set_peer_sync();
  waiting_all_exit_sync();

  auto sync_sender_fn = [&] (int qid) {
    sync_send_handler(qid);
  };
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    sync_send_thread[i] = new std::thread(sync_sender_fn, i);
  }
}


bool Engine::any_local_in_sync() {
  return local_in_sync_cnt.load() > 0;
}

bool Engine::any_rm_in_sync() {
  return remote_in_sync_cnt.load() > 0;
}

void Engine::waiting_all_exit_sync() {
  DEBUG_PRINTF(0, "%s: start waiting for remote\n", this_host_info);

  while (any_rm_in_sync()) {
    sched_yield();
  }

  DEBUG_PRINTF(0, "%s: start waiting for local\n", this_host_info);

  while (any_local_in_sync()) {
    sched_yield();
  }
  DEBUG_PRINTF(0, "%s: end waiting\n", this_host_info);
}

/* bool Engine::try_notify_enter(int neighbor_idx, int qid) { */
/*   bool b = false; */
/*   return in_sync[neighbor_idx][qid].compare_exchange_strong(b, true); */
/* } */

/* void Engine::end_notify_enter() { */
/*   return in_sync_visible.store(true); */
/* } */
