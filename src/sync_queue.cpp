#include "include/sync_queue.hpp"
#include "include/comm.h"
#include "include/config.hpp"
#include "include/engine.hpp"
#include "include/util.hpp"
#include "uv.h"
#include "uv/unix.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

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
  int waiting_times = 0;
  size_t pop_cnt;
  int ret;
  while ((pop_cnt = queue.pop(&send_start)) > 0) {
    for (int i = 0; i < pop_cnt; ++i) {
      DEBUG_PRINTF(VPROT, "sending %ld, %ld\n", send_start[i].id, send_start[i].salary);
    }
    // 要向3个发
    for (int i = 0; i < 3; ++i) {
      int neighbor_idx = neighbor_index[i];
      if (alive[neighbor_idx]) {
        ret = send_all(sync_send_fdall[neighbor_idx][qid], send_start, pop_cnt * 16, 0);
        if (ret < 0) {
          alive[neighbor_idx] = false;
          continue;
        }
        DEBUG_PRINTF(ret == pop_cnt * 16, "send buffer not enough : %d < %ld\n", ret, pop_cnt * 16);
        assert(ret == pop_cnt * 16);
      }
    }

    // 更新tail
    std::atomic_thread_fence(std::memory_order_release);
    queue.tail += pop_cnt;
  }
}


/* void Engine::init_set_peer_sync() { */
/*   for (int i = 0; i < MAX_NR_CONSUMER; i++) { */
/*     auto local_cnt = qs[i].head->load(); */
/*     sync_send msg; */
/*     RemoteUser *buf; */
/*     const User *user; */
/*     int ret, nbytes; */

/*     msg.cnt = 0; */
/*     // 向所有节点发送同步完成的请求 */
/*     for (int nb_i = 0; nb_i < 3; nb_i++) { */
/*       int neighbor_idx = neighbor_index[nb_i]; */
/*       ret = send(sync_send_fdall[neighbor_idx][i], &msg, sizeof(msg), 0); */
/*       if (ret < 0) { */
/*         DEBUG_PRINTF(0, "init send header sync error\n"); */
/*       } */
/*       assert(ret == sizeof(msg)); */
/*     } */
/*     DEBUG_PRINTF(local_cnt, "%s: send to 3 peers q[%d].local_cnt = %ld\n", this_host_info, i, local_cnt); */
/*   } */
/* } */


struct sync_resp_param {
  Engine *eg;
  size_t cur_buf_size;
  char *buf;
  int neighbor_idx;
  int qid;
  uv_buf_t write_buf;
  char rest[16]; // 一个包剩下的大小
  size_t restn;
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
    DEBUG_PRINTF(VPROT, "Read error nread = 0, %s\n", strerror(errno));
    return;
  }

  DEBUG_PRINTF(VPROT, "recv %ld bytes\n", nread);
  int qid = param->qid;
  char *buf = uv_buf->base;
  int64_t resp_cnt;
  
  if (param->restn) {
    int64_t unaligned[2];
    memcpy(unaligned, param->rest, param->restn);
    size_t cnt = 16 - param->restn;
    memcpy((char *)unaligned + param->restn, buf, cnt);
    nread -= cnt;
    buf += cnt;
    param->restn = 0;
    param->eg->remote_id_r[param->neighbor_idx].put(unaligned[0], unaligned[1]);
    param->eg->remote_sala_r[param->neighbor_idx].put(unaligned[1], unaligned[0]);
  }
  // 收到退出的sync的请求的时候需要回应
  if(nread & 15) {
    param->restn = nread & 15;
    memcpy(param->rest, buf + (nread / 16) * 16, param->restn);
    nread -= param->restn;
  }
  assert((nread % 16) == 0);
  // nread 为什么会是2呢
  RemoteUser *user = (RemoteUser *) buf;
  size_t recv_user_cnt = nread / 16;
#pragma omp parallel for num_threads(4)
  for (int i = 0; i < recv_user_cnt; ++i) {
    // build index
    DEBUG_PRINTF(VPROT, "recving %ld, %ld from %d\n", user[i].id, user[i].salary, param->neighbor_idx);
    param->eg->remote_id_r[param->neighbor_idx].put(user[i].id, user[i].salary);
    param->eg->remote_sala_r[param->neighbor_idx].put(user[i].salary, user[i].id);
  }
}

void Engine::sync_resp_handler(int qid) {
  uv_loop_t *loop = (uv_loop_s *)malloc(sizeof(uv_loop_t));
  uv_loop_init(loop);
  uv_tcp_t handler[3];
  sync_resp_param param[3];
  int ret;

  for (int nb_i = 0; nb_i < 3; nb_i++) {
    int neighbor_idx = neighbor_index[nb_i];
    ret = uv_tcp_init(loop, &handler[nb_i]);
    DEBUG_PRINTF(ret == 0, "sync resp handler uv tcp init error\n");
    ret = uv_tcp_open(&handler[nb_i], sync_recv_fdall[neighbor_idx][qid]);
    DEBUG_PRINTF(ret == 0, "sync resp handler uv tcp open error\n");

    /* param[nb_i].sync_q = &sync_qs[qid]; */
    param[nb_i].eg = this;
    param[nb_i].neighbor_idx = neighbor_index[nb_i];
    param[nb_i].qid = qid;
    memset(param[nb_i].rest, 0, 16);
    param[nb_i].restn = 0;
    /* for (int j = 0; j < 3; j++) { */
    /*   param[nb_i][i].neighbor_index[j] = neighbor_index[j]; */
    /* } */

    // 64k是最佳的buffer大小
    param[nb_i].cur_buf_size = 1 << 17;
    param[nb_i].buf = (char *)map_anonymouse(1 << 17);
    handler[nb_i].data = &param[nb_i]; 
    uv_read_start((uv_stream_t *)&handler[nb_i], sync_resp_alloc_buffer, process_sync_resp);
  }

  uv_run(loop, UV_RUN_DEFAULT);
  uv_loop_close(loop);
  free(loop);
  for (int i = 0; i < 3; ++i) {
    int neighbor_idx = neighbor_index[i];
    for (int j = 0; j < MAX_NR_CONSUMER; ++j) {
      munmap(param[i].buf, 1 << 17);
    }
  }
}


void Engine::start_sync_handlers() {
  remote_in_sync_cnt = 3 * MAX_NR_CONSUMER;
  auto sync_resp_fn = [&] (int qid) {
    sync_resp_handler(qid);
  };

  for (int i = 0; i < MAX_NR_CONSUMER; i++)
    sync_resp_thread[i] = new std::thread(sync_resp_fn, i);

  // 或许没有必要，因为send queue空了自动会发
  /* init_set_peer_sync(); */
  auto sync_sender_fn = [&] (int qid) {
    sync_send_handler(qid);
  };
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    sync_send_thread[i] = new std::thread(sync_sender_fn, i);
  }
  /* waiting_all_exit_sync(); */
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
  /* DEBUG_PRINTF(0, "%s: start waiting for local\n", this_host_info); */

  /* while (any_local_in_sync()) { */
  /*   sched_yield(); */
  /* } */
  DEBUG_PRINTF(0, "%s: end waiting\n", this_host_info);
}

/* bool Engine::try_notify_enter(int neighbor_idx, int qid) { */
/*   bool b = false; */
/*   return in_sync[neighbor_idx][qid].compare_exchange_strong(b, true); */
/* } */

/* void Engine::end_notify_enter() { */
/*   return in_sync_visible.store(true); */
/* } */
