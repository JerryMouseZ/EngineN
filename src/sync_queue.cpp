#include "include/sync_queue.hpp"
#include "include/engine.hpp"

constexpr size_t EACH_REMOTE_DATA_FILE_LEN = EACH_NR_USER * sizeof(RemoteUser);

RemoteData::~RemoteData() {
  if (users != nullptr) {
    pmem_unmap(users, EACH_REMOTE_DATA_FILE_LEN);
  }  
}

void RemoteData::open(const std::string &fdata) {
  users = reinterpret_cast<RemoteUser *>(pmem_map_file(fdata.c_str(), EACH_REMOTE_DATA_FILE_LEN, PMEM_FILE_CREATE, 0666, nullptr, nullptr));
  DEBUG_PRINTF(users, "%s open mmaped failed", fdata.c_str());

  prefault((char *)users, EACH_REMOTE_DATA_FILE_LEN, false);
}

struct sync_send_param {
  RemoteData *rmdata;
  Index *rm_id_r;
  Index *rm_sala_r;
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

void build_remote_index(int qid, int begin, int end, Index *id_index, Index *salary_index, RemoteData *rmdatap) {
  for (auto i = begin; i < end; i++) {
    const RemoteUser *user = rmdatap->data_read(i);
    uint32_t encoded_index = (qid << 28) | i;
    id_index->put(user->id, encoded_index);
    salary_index->put(user->salary, encoded_index);
  }
}

// TODO: 会不会泄露内存？
void sync_send_alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  sync_send_param *param = (sync_send_param *)handle->data;

  if (suggested_size > param->cur_buf_size) {
    free(param->buf);
    param->buf = (char *)malloc(suggested_size);
    param->cur_buf_size = suggested_size;
  }

  buf->base = param->buf;
  buf->len = param->cur_buf_size;
}

void process_sync_send(uv_stream_t *client, ssize_t nread, const uv_buf_t *uv_buf) {
  sync_send_param *param = (sync_send_param *)client->data;
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

  sync_resp resp;
  ssize_t rest = param->rest;
  const char *buf = uv_buf->base;
  char *write_pos = param->write_pos;
  RemoteData *rmdata = param->rmdata;
  uint32_t current_send_cnt = param->current_send_cnt;
  int ret;

  if (unlikely(param->eg->try_notify_enter(param->neighbor_idx, param->qid))) {
    param->eg->local_in_sync_cnt.fetch_add(1);
    param->eg->remote_in_sync_cnt.fetch_add(1);
    param->eg->end_notify_enter();
  }


  do {
    if (rest == 0) {
      if (unlikely(nread < sizeof(sync_send))) {
        DEBUG_PRINTF(0, "Read error read send header fragmented nread = %ld\n", nread);
      }
      current_send_cnt = ((sync_send *)buf)->cnt;
      write_pos = (char *)&rmdata->users[rmdata->local_cnt];
      nread -= sizeof(sync_send);
      buf += sizeof(sync_send);
      rest = current_send_cnt * sizeof(RemoteUser);

      if (current_send_cnt == 0) {
        param->eg->notify_local_queue_exit_sync(param->neighbor_idx, param->qid);

        DEBUG_PRINTF(0, "%s: recv end send from index = %d to q[%d] final local_cnt = %d, start send end resp\n",
          this_host_info, param->neighbor_idx, param->qid, rmdata->local_cnt);

        resp.cnt = 0;
        ret = send_all(param->recv_fd, &resp, sizeof(resp), 0);
        if (ret < 0) {
          *param->alive = false;
          return;
        }
      }
    }

    if (nread == 0) {
      break;
    }

    if (rest > nread) {
      memcpy(write_pos, buf, nread);

      write_pos += nread;
      rest -= nread;
      buf += nread;
      nread = 0;
    } else{
      memcpy(write_pos, buf, rest);

      build_remote_index(param->qid,
        rmdata->local_cnt, rmdata->local_cnt + current_send_cnt,
        param->rm_id_r, param->rm_sala_r, rmdata);

      rmdata->local_cnt += current_send_cnt;

      DEBUG_PRINTF(current_send_cnt, "%s: recv from index = %d to q[%d] cnt = %d, now local_cnt = %d\n",
        this_host_info, param->neighbor_idx, param->qid, current_send_cnt, rmdata->local_cnt);

      resp.cnt = current_send_cnt;
      ret = send_all(param->recv_fd, &resp, sizeof(resp), 0);
      if (ret < 0) {
        *param->alive = false;
        return;
      }

      write_pos += rest;
      nread -= rest;
      buf += rest;
      rest = 0;
    }
  } while (nread > 0);

finish_process_sync_send:
  param->current_send_cnt = current_send_cnt;
  param->rest = rest;
  param->write_pos = write_pos;
}

void sync_send_handler(
  int neighbor_idx,
  int qid_start,
  int *fds,
  RemoteData *rmdata,
  Index *rm_id_r,
  Index *rm_sala_r,
  volatile bool *alive,
  Engine *eg) {

  sync_send_client clients[NR_FD_EACH_SYNC_HANDLER];
  uv_loop_t *loop = (uv_loop_s *)malloc(sizeof(uv_loop_t));
  uv_tcp_t *handler;
  sync_send_param *param;
  int ret;

  uv_loop_init(loop);

  for (int i = 0; i < NR_FD_EACH_SYNC_HANDLER; i++) {
    handler = &clients[i].handler;
    param = &clients[i].param;
    ret = uv_tcp_init(loop, handler);
    DEBUG_PRINTF(ret == 0, "sync send handler uv tcp init error\n");
    ret = uv_tcp_open(handler, fds[i]);
    DEBUG_PRINTF(ret == 0, "sync send handler uv tcp open error\n");

    param->rmdata = &rmdata[i];
    param->rm_id_r = rm_id_r;
    param->rm_sala_r = rm_sala_r;
    param->rest = 0;
    param->alive = alive;
    param->eg = eg;
    param->recv_fd = fds[i];
    param->qid = qid_start + i;
    param->neighbor_idx = neighbor_idx;
    param->cur_buf_size = 4096 + 8;
    param->buf = (char *)malloc(param->cur_buf_size);
    handler->data = param; 

    uv_read_start((uv_stream_t *)handler, sync_send_alloc_buffer, process_sync_send);
  }

  uv_run(loop, UV_RUN_DEFAULT);
}

struct sync_resp_param {
  RemoteData *rmdata;
  SyncQueue *sync_q;
  Engine *eg;
  size_t cur_buf_size;
  char *buf;
  int neighbor_idx;
  int qid;
  int neighbor_index[3];
};

struct sync_resp_client {
  uv_tcp_t handler;
  sync_resp_param param;
};

void sync_resp_alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  sync_resp_param *param = (sync_resp_param *)handle->data;

  if (suggested_size > param->cur_buf_size) {
    free(param->buf);
    param->buf = (char *)malloc(suggested_size);
    param->cur_buf_size = suggested_size;
  }

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
  const char *buf = uv_buf->base;
  uint32_t *neighbor_local_cnt = param->sync_q->neighbor_local_cnt;
  uint32_t resp_cnt;

  do {
    if (unlikely(nread < sizeof(sync_resp))) {
      DEBUG_PRINTF(0, "Read error read resp header fragmented nread = %ld\n", nread);
    }

    resp_cnt = ((sync_resp *)buf)->cnt;
    nread -= sizeof(sync_resp);

    DEBUG_PRINTF(0, "%s: recv resp from index = %d to q[%d] resp_cnt = %d\n",
      this_host_info, param->neighbor_idx, qid, resp_cnt);

    if (resp_cnt == 0) {
      param->eg->notify_remote_queue_exit_sync(param->neighbor_idx, qid);
      // 不应该还有这条信道上的数据了
      assert(nread == 0);
      break;
    }
    
    neighbor_local_cnt[param->neighbor_idx] += resp_cnt;
  } while (nread > 0);
  
  auto origin_min= param->rmdata->local_cnt;
  auto now_min = neighbor_local_cnt[param->neighbor_index[0]];

  for (int nb_i = 1; nb_i < 3; nb_i++) {
    int neighbor_idx = param->neighbor_index[nb_i];
    if (neighbor_local_cnt[neighbor_idx] < now_min) {
      now_min = neighbor_local_cnt[neighbor_idx];
    }
  }
  param->sync_q->advance_tail(now_min);
}

void sync_resp_handler(
  int neighbor_index[3], 
  int sync_send_fdall[4][MAX_NR_CONSUMER], 
  RemoteData rmdatas[4][MAX_NR_CONSUMER],
  SyncQueue sync_qs[MAX_NR_CONSUMER],
  Engine *eg) {

  sync_resp_client clients[4][MAX_NR_CONSUMER];
  uv_loop_t *loop = (uv_loop_s *)malloc(sizeof(uv_loop_t));
  uv_tcp_t *handler;
  sync_resp_param *param;
  int ret;

  uv_loop_init(loop);

  for (int nb_i = 0; nb_i < 3; nb_i++) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < MAX_NR_CONSUMER; i++) {
      handler = &clients[neighbor_idx][i].handler;
      param = &clients[neighbor_idx][i].param;
      ret = uv_tcp_init(loop, handler);
      DEBUG_PRINTF(ret == 0, "sync resp handler uv tcp init error\n");
      ret = uv_tcp_open(handler, sync_send_fdall[neighbor_idx][i]);
      DEBUG_PRINTF(ret == 0, "sync resp handler uv tcp open error\n");

      param->rmdata = &rmdatas[neighbor_idx][i];
      param->sync_q = &sync_qs[i];
      param->eg = eg;
      param->neighbor_idx = neighbor_idx;
      param->qid = i;
      for (int j = 0; j < 3; j++) {
        param->neighbor_index[j] = neighbor_index[j];
      }
      param->cur_buf_size = 64;
      param->buf = (char *)malloc(param->cur_buf_size);
      handler->data = param; 

      uv_read_start((uv_stream_t *)handler, sync_resp_alloc_buffer, process_sync_resp);
    }
  }

  uv_run(loop, UV_RUN_DEFAULT);
}

void Engine::start_sync_handlers() {
  sync_resp_thread = new std::thread(sync_resp_handler,
    neighbor_index, sync_send_fdall, remote_datas,
    sync_qs, this);

  for (int nb_i = 0; nb_i < 3; nb_i++) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < NR_SYNC_HANDLER_EACH_NB; i++) {
      int qid_start = i * NR_FD_EACH_SYNC_HANDLER;
      sync_send_thread[neighbor_idx][i] = new std::thread(sync_send_handler,
        neighbor_idx, qid_start, &sync_recv_fdall[neighbor_idx][qid_start],
        &remote_datas[neighbor_idx][qid_start], &remote_id_r[neighbor_idx],
        &remote_sala_r[neighbor_idx], &alive[neighbor_idx],
        this);
    }
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    auto local_cnt = qs[i].head->load();
    sync_send msg;
    RemoteUser *buf;
    const User *user;
    int ret, nbytes;

    msg.cnt = local_cnt;

    for (int nb_i = 0; nb_i < 3; nb_i++) {
      int neighbor_idx = neighbor_index[nb_i];
      ret = send_all(sync_send_fdall[neighbor_idx][i], &msg, sizeof(msg), 0);
      if (ret < 0) {
        DEBUG_PRINTF(0, "init send header sync error\n");
      }
      assert(ret == sizeof(msg));
    }


    if (local_cnt > 0) {
      nbytes = local_cnt * sizeof(RemoteUser);
      buf = (RemoteUser *)malloc(nbytes);

      for (int v = 0; v < local_cnt; v++) {
        user = datas[i].data_read(v);
        buf[v].id = user->id;
        buf[v].salary = user->salary;
      }

      for (int nb_i = 0; nb_i < 3; nb_i++) {
        int neighbor_idx = neighbor_index[nb_i];
        ret = send_all(sync_send_fdall[neighbor_idx][i], buf, nbytes, 0);
        if (ret < 0) {
          DEBUG_PRINTF(0, "init send data sync error\n");
        }
        assert(ret == nbytes);
      }

      sync_qs[i].sync_to(local_cnt);

      free(buf);
    }

    DEBUG_PRINTF(local_cnt, "%s: send to 3 peers q[%d].local_cnt = %ld\n", this_host_info, i, local_cnt);
  }

  waiting_all_exit_sync();
}

void Engine::notify_local_queue_exit_sync(int neighbor_idx, int qid) {
  uint64_t rest = local_in_sync_cnt.fetch_sub(1);
  DEBUG_PRINTF(0, "%s: local[%d][%d] exits, rest = %ld\n", this_host_info, neighbor_idx, qid, rest - 1);
}


void Engine::notify_remote_queue_exit_sync(int neighbor_idx, int qid) {
  uint64_t rest = remote_in_sync_cnt.fetch_sub(1);
  DEBUG_PRINTF(0, "%s: remote[%d][%d] exits, rest = %ld\n", this_host_info, neighbor_idx, qid, rest - 1);
}

bool Engine::any_local_in_sync() {
  return local_in_sync_cnt.load() > 0;
}

bool Engine::any_rm_in_sync() {
  return remote_in_sync_cnt.load() > 0;
}

void Engine::waiting_all_exit_sync() {
  DEBUG_PRINTF(0, "%s: start waiting for remote\n", this_host_info);

  while (any_rm_in_sync() || !in_sync_visible) {
    sched_yield();
  }

  DEBUG_PRINTF(0, "%s: start waiting for local\n", this_host_info);

  while (any_local_in_sync()) {
    sched_yield();
  }

  DEBUG_PRINTF(0, "%s: end waiting\n", this_host_info);
}

bool Engine::try_notify_enter(int neighbor_idx, int qid) {
  bool b = false;
  return in_sync[neighbor_idx][qid].compare_exchange_strong(b, true);
}

void Engine::end_notify_enter() {
  return in_sync_visible.store(true);
}