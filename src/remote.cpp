#include "include/engine.hpp"
#include "include/comm.h"
#include "include/data.hpp"
#include "include/thread_id.hpp"
#include "include/util.hpp"
#include <bits/types/struct_iovec.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <uv.h>
#include <uv/unix.h>

Engine *engine = nullptr;

// 创建listen socket，尝试和别的机器建立两条连接
void Engine::connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num, bool is_new_create) {
  if (host_info == NULL || peer_host_info == NULL)
    return;
  this_host_info = host_info;
  std::vector<info_type> infos;
  const char *split_index = strstr(host_info, ":");
  int host_ip_len = split_index - host_info;
  std::string host_ip = std::string(host_info, host_ip_len);
  int host_port = atoi(split_index + 1);
  fprintf(stderr, "host info : %s %d\n", host_ip.c_str(), host_port);
  infos.emplace_back(info_type(host_ip, host_port));

  // peer ips
  for (int i = 0; i < peer_host_info_num; ++i) {
    const char *split_index = strstr(peer_host_info[i], ":");
    int ip_len = split_index - peer_host_info[i];
    std::string ip = std::string(peer_host_info[i], ip_len);
    int port = atoi(split_index + 1);
    infos.emplace_back(info_type(ip, port)); 
  }

  // 按ip地址排序
  std::sort(infos.begin(), infos.end(), [](const info_type &a, const info_type &b){ return a.first < b.first; });
  int neighbor_cnt = 0;
  for (int i = 0; i < peer_host_info_num + 1; ++i) {
    if (infos[i].first == host_ip) {
      host_index = i;
    } else {
      neighbor_index[neighbor_cnt++] = i;
      DEBUG_PRINTF(INIT, "%s: neighbor_index[%d] = %d\n", this_host_info, neighbor_cnt - 1, neighbor_index[neighbor_cnt - 1]);
    }
  }

  connect(infos, peer_host_info_num + 1, is_new_create);
}

void Engine::connect(std::vector<info_type> &infos, int num, bool is_new_create) {
  int ret;
  signal(SIGPIPE, SIG_IGN);
  listen_fd = setup_listening_socket(infos[host_index].first.c_str(), infos[host_index].second);
  sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  char client_addr_str[60];

  // 建立同步数据的连接
  for (int i = 0; i < 4; ++i)
    alive[i] = true;

  std::thread listen_thread(listener, listen_fd, &infos, (int **)recv_fdall);

  auto this_host_ip = infos[host_index].first.c_str();

  // 每个neighbor都发50个
  for (int nb_i = 0; nb_i < 3; nb_i++) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < MAX_NR_PRODUCER; i++) {
      send_fdall[neighbor_idx][i] = 
       connect_to_server(this_host_ip, infos[neighbor_idx].first.c_str(), infos[neighbor_idx].second);
      DEBUG_PRINTF(INIT, "%s: neighbor index = %d senf_fd[%d] = %d to %s\n",
        this_host_info, neighbor_idx, i, send_fdall[neighbor_idx][i], infos[neighbor_idx].first.c_str());
    }
  }

  listen_thread.join();
  start_handlers(); // 先start handlers
}


size_t Engine::remote_read(uint8_t select_column, uint8_t where_column, const void *column_key, size_t column_key_len, void *res) {
  if (unlikely(!have_reader_id())) {
    init_reader_id();
  }

  bool multiple = where_column == Salary;
  bool single_already_recived = false;
  int ret = 0;

  data_request data;
  data.fifo_id = 1;
  data.select_column = select_column;
  data.where_column = where_column;
  memcpy(data.key, column_key, column_key_len);

  int send_success_cnt = 0;
  int len, neighbor_idx;
  for (int nb_i = 0; nb_i < 3; nb_i++) {
    neighbor_idx = neighbor_index[nb_i];

    if (!alive[neighbor_idx]) {
      continue;
    }

    len = send_all(send_fdall[neighbor_idx][reader_id], &data, sizeof(data), MSG_NOSIGNAL);
    if (len < 0) {
      DEBUG_PRINTF(0, "%s: send error %d to node %d, mark as inalive\n", this_host_info, len, neighbor_idx);
      alive[neighbor_idx] = false;
      continue;
    }
    assert(len == sizeof(data_request));

    send_success_cnt++;
  }

  if (send_success_cnt == 0) {
    return 0;
  }

  response_header header;
  for (int nb_i = 0; nb_i < 3; nb_i++) {
    neighbor_idx = neighbor_index[nb_i];

    if (!alive[neighbor_idx]) {
      continue;
    }

    len = recv_all(send_fdall[neighbor_idx][reader_id], &header, sizeof(header), MSG_WAITALL);
    if (len <= 0) {
      DEBUG_PRINTF(0, "%s: recv header error %d to node %d, mark as inalive\n", this_host_info, len, neighbor_idx);
      alive[neighbor_idx] = false;
      continue;
    }
    assert(len == sizeof(header));

    if (header.res_len == 0) {
      continue;
    }

    if (single_already_recived) {
      DEBUG_PRINTF(0, "%s: wierd, single result read already gets result, previous result will be overwritten\n", this_host_info);
    }

    len = recv_all(send_fdall[neighbor_idx][reader_id], res, header.res_len, MSG_WAITALL);
    if (len <= 0) {
      alive[neighbor_idx] = false;
      continue;
    }
    assert(len == header.res_len);

    // 请求成功
    if (multiple) {
      ret += header.ret;      
      res = ((char *)res) + len;
    } else {
      ret = header.ret;      
      single_already_recived = true;
    }
  }

  return ret;
}


struct recv_cqe_data{
  int type;
  int fd;
};


int get_column_len(int column) {
  switch(column) {
  case Id:
    return 8;
    break;
  case Userid:
    return 128;
    break;
  case Name:
    return 128;
    break;
  case Salary:
    return 8;
    break;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return -1;
}

void Engine::ask_peer_quit() {
  data_request req;
  req.select_column = 22;
  req.where_column = 22;
  for (int nb_i = 0; nb_i < 3; nb_i++) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < 10; ++i) {
      send_all(send_fdall[neighbor_idx][i * 5], &req, sizeof(req), MSG_NOSIGNAL);
    }
  }
}


struct uv_param{
  void *buf;
  uv_write_t req;
  uv_buf_t *uv_buf;
  void *resp_buf;
  Engine *engine;
};

void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    buf->base = (char *)handle->data;
    buf->len = sizeof(data_request);
}

typedef struct {
    uv_write_t req;
    uv_buf_t buf;
} write_req_t;

void on_close(uv_handle_t* handle) {
    free(handle);
}

void echo_write(uv_write_t *req, int status) {
  if (status) {
    fprintf(stderr, "Write error %s\n", uv_strerror(status));
  }
}

void process_request(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
  if (nread < 0) {
    if (nread != UV_EOF)
      fprintf(stderr, "Read error %s\n", uv_err_name(nread));
    uv_close((uv_handle_t*) client, on_close);
    return;
  }
  if (nread == 0) {
    return;
  }
  assert(nread == sizeof(data_request));

  // do data read
  uv_param *param = (uv_param *)client->data;
  uint8_t select_column, where_column;
  uint32_t fifo_id;
  void *key;
  data_request *req = (data_request *)buf->base;
  select_column = req->select_column;
  where_column = req->where_column;
  key = req->key;
  fifo_id = req->fifo_id;
  if (select_column == 22 && where_column == 22) {
    uv_close((uv_handle_t*) client, on_close);
    return;
  }
  if (where_column == Id || where_column == Salary)
    DEBUG_PRINTF(VLOG, "recv request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)key);
  else
    DEBUG_PRINTF(VLOG, "recv request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)key);
  
  response_buffer *res_buffer = (response_buffer *)param->resp_buf;
  int num = param->engine->local_read(select_column, where_column, key, 128, res_buffer->body);
  res_buffer->header.fifo_id = fifo_id;
  res_buffer->header.ret = num;
  res_buffer->header.res_len = get_column_len(select_column) * num;

  param->uv_buf->len = sizeof(response_header) + res_buffer->header.res_len;
  param->uv_buf->base = (char *)res_buffer;
  uv_write(&param->req, client, param->uv_buf, 1, echo_write);
}

void init_uv(uv_tcp_t *handler, void *recv_buf, void *resp_buf, Engine *engine, uv_param *param) {
  param->buf = recv_buf;
  param->resp_buf = resp_buf;
  param->engine = engine;
}

// 传参是个大问题
void Engine::request_handler(int node, int *fds){
  // 用一个ring来存request，然后可以异步处理，是一个SPMC的模型，那就不能用之前的队列了
  data_request req[5];
  response_buffer res_buffer[5];
  uv_param params[5];
  uv_loop_t loop;
  uv_tcp_t uv_handlers[5];
  uv_loop_init(&loop);
  for (int i = 0; i < 5; ++i) {
    uv_tcp_init(&loop, &uv_handlers[i]);
    uv_tcp_open(&uv_handlers[i], fds[i]);
    init_uv(&uv_handlers[i], &req[i], &res_buffer[i], this, &params[i]);
    uv_read_start((uv_stream_t *)&uv_handlers[i], alloc_buffer, process_request);
  }

  uv_run(&loop, UV_RUN_DEFAULT);
}

void Engine::start_handlers() {
  auto req_handler_fn = [&](int index, int *fds) {
    request_handler(index, fds);
  };

  for (int nb_i = 0; nb_i < 3; ++nb_i) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < 10; i++) {
      int hdi = neighbor_idx * 10 + i;
      req_handlerall[hdi] = new std::thread(req_handler_fn, neighbor_idx, recv_fdall[neighbor_idx] + i * 5);
    }
  }
}


void Engine::disconnect() {
  // wake up request sender
  ask_peer_quit();
  DEBUG_PRINTF(0, "socket close, waiting handlers\n");

  for (int nb_i = 0; nb_i < 3 ; ++nb_i) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < 10; i++) {
      req_handlerall[neighbor_idx * 10 + i]->join();
    }
  }

  for (int nb_i = 0; nb_i < 3 ; ++nb_i) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < MAX_NR_PRODUCER; i++) {
      shutdown(send_fdall[neighbor_idx][i], SHUT_RDWR);
      shutdown(recv_fdall[neighbor_idx][i], SHUT_RDWR);
    }
  }

  close(listen_fd);

  for (int nb_i = 0; nb_i < 3 ; ++nb_i) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < MAX_NR_PRODUCER; i++) {
      close(send_fdall[neighbor_idx][i]);
      close(recv_fdall[neighbor_idx][i]);
    }
  }
}


int Engine::get_backup_index() {
  switch(host_index) {
  case 0:
    return 1;
  case 1:
    return 0;
  case 2:
    return 3;
  case 3:
    return 2;
  }
  fprintf(stderr, "error host index %d\n", host_index);
  return -1;
}

// 因为两两有备份，所以只需要向另外一组请求即可。这里做一个负载均衡，使得请求能平均分布到4台机器上
int Engine::get_request_index() {
  switch(host_index) {
  case 0:
    return 2;
  case 1:
    return 3;
  case 2:
    return 0;
  case 3:
    return 1;
  }
  return -1;
}

int Engine::get_another_request_index() {
  switch(host_index) {
  case 0:
    return 3;
  case 1:
    return 2;
  case 2:
    return 1;
  case 3:
    return 0;
  }
  return -1;
}

void Engine::do_sync() {
  // handshake with remote
}
