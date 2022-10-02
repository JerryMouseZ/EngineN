#include "include/engine.hpp"
#include "include/comm.h"
#include "include/data.hpp"
#include "include/send_recv.hpp"
#include "include/thread_id.hpp"
#include "include/util.hpp"
#include "liburing.h"
#include <bits/types/struct_iovec.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>


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
  int my_index = -1;
  for (int i = 0; i < peer_host_info_num + 1; ++i) {
    if (infos[i].first == host_ip) {
      my_index = i;
      break;
    }
  }

  connect(infos, peer_host_info_num + 1, my_index, is_new_create);
}


void Engine::connect(std::vector<info_type> &infos, int num, int host_index, bool is_new_create) {
  this->host_index = host_index;
  for (int i = 0; i < 10; ++i) {
    io_uring_queue_init(QUEUE_DEPTH, &req_recv_ring[i], 0);
    io_uring_queue_init(QUEUE_DEPTH, &req_weak_recv_ring[i], 0);
    io_uring_queue_init(QUEUE_DEPTH, &resp_ring[i], 0);
    io_uring_queue_init(QUEUE_DEPTH, &resp_weak_ring[i], 0);
  }
  listen_fd = setup_listening_socket(infos[host_index].first.c_str(), infos[host_index].second);
  sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  char client_addr_str[60];
  std::thread listen_thread(listener, listen_fd, &infos, &data_recv_fd, get_backup_index(), host_index, req_recv_fds, req_weak_recv_fds, get_request_index(), get_another_request_index());

  // 建立同步数据的连接
  for (int i = 0; i < 4; ++i)
    alive[i] = true;

  data_fd = connect_to_server(infos[host_index].first.c_str(), infos[get_backup_index()].first.c_str(), infos[get_backup_index()].second);

  for (int i = 0; i < 50; ++i) {
    req_send_fds[i] = connect_to_server(infos[host_index].first.c_str(), infos[get_request_index()].first.c_str(), infos[get_request_index()].second);
  }

  for (int i = 0; i < 50; ++i) {
    req_weak_send_fds[i] = connect_to_server(infos[host_index].first.c_str(), infos[get_another_request_index()].first.c_str(), infos[get_another_request_index()].second);
  }

  listen_thread.join();
  // move to after connect
  if (!is_new_create)
    do_peer_data_sync();
  start_handlers(); // 先start handlers
}


size_t Engine::remote_read(uint8_t select_column, uint8_t where_column, const void *column_key, size_t column_key_len, void *res) {
  if (unlikely(!have_reader_id())) {
    init_reader_id();
  }
  int fd = -1;
  int current_req_node = -1;
  if (alive[get_request_index()]) {
    fd = req_send_fds[reader_id];
    current_req_node = get_request_index();
  } else if (alive[get_another_request_index()]) {
    fd = req_weak_send_fds[reader_id];
    current_req_node = get_backup_index();
  } else {
    // 两个节点都失效了，返回0
    return 0;
  }

  data_request data;
  data.fifo_id = 1;
  data.select_column = select_column;
  data.where_column = where_column;
  memcpy(data.key, column_key, column_key_len);
  int len = send_all(fd, &data, sizeof(data), MSG_NOSIGNAL);
  if (len < 0) {
    alive[current_req_node] = false;
    return remote_read(select_column, where_column, column_key, column_key_len, res);
  }
  assert(len == sizeof(data_request));

  if (where_column == Id || where_column == Salary)
    DEBUG_PRINTF(LOG, "add remote read request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)column_key);
  else
    DEBUG_PRINTF(LOG, "add send remote read request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)column_key);

  response_header header;
  len = recv_all(fd, &header, sizeof(header), MSG_WAITALL);
  if (len < 0) {
    alive[current_req_node] = false;
    return remote_read(select_column, where_column, column_key, column_key_len, res);
  }
  assert(len == sizeof(header));

  len = recv_all(fd, res, header.res_len, MSG_WAITALL);
  if (len < 0) {
    alive[current_req_node] = false;
    return remote_read(select_column, where_column, column_key, column_key_len, res);
  }
  assert(len == header.res_len);
  return header.ret;
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
  for (int i = 0; i < 10; ++i) {
    /* fprintf(stderr, "ask node %d to quit\n", get_request_index()); */
    send_all(req_send_fds[i * 5], &req, sizeof(req), MSG_NOSIGNAL);
    /* fprintf(stderr, "ask node %d to quit\n", get_another_request_index()); */
    send_all(req_weak_send_fds[i * 5], &req, sizeof(req), MSG_NOSIGNAL);
  }
}

void Engine::request_handler(int node, int *fds, io_uring &ring){
  // 用一个ring来存request，然后可以异步处理，是一个SPMC的模型，那就不能用之前的队列了
  data_request req[5];
  response_buffer res_buffer[5];
  int lens[5];
  iovec iov[5];
  iovec send_iov[5];
  for (int i = 0; i < 5; ++i) {
    iov[i].iov_base = &req[i];
    iov[i].iov_len = sizeof(data_request);
    add_read_request(ring, fds[i], &iov[i], i << 16);
  }
  io_uring_cqe *cqe;
  while (1) {
    cqe = wait_cqe_fast(&ring);
    assert(cqe);
    if (cqe->res <= 0) {
      fprintf(stderr, "recv error %d\n", cqe->res);
      alive[node] = false;
      break;
    }
    int id = cqe->user_data >> 16;
    int type = cqe->user_data & 1;
    int len = cqe->res;
    io_uring_cqe_seen(&ring, cqe);
    if (type == 1) {
      if (len < 0) {
        fprintf(stderr, "send error %d\n", len);
        alive[node] = false;
        break;
      }
      DEBUG_PRINTF(len == send_iov[id].iov_len, "send %d, res %ld\n", len, send_iov[id].iov_len); // 可能需要一个自增的id，不然这个send_iov可能是下一个包的
    } else {
      uint8_t select_column, where_column;
      uint32_t fifo_id;
      void *key;
      select_column = req[id].select_column;
      where_column = req[id].where_column;
      key = req[id].key;
      fifo_id = req[id].fifo_id;
      if (select_column == 22 && where_column == 22) {
        /* fprintf(stderr, "handlers for node %d quiting\n", node); */
        return;
      }
      if (where_column == Id || where_column == Salary)
        DEBUG_PRINTF(LOG, "recv request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)key);
      else
        DEBUG_PRINTF(LOG, "recv request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)key);
      int num = local_read(select_column, where_column, key, 128, res_buffer[id].body);
      res_buffer[id].header.fifo_id = fifo_id;
      res_buffer[id].header.ret = num;
      res_buffer[id].header.res_len = get_column_len(select_column) * num;

      send_iov[id].iov_len = sizeof(response_header) + res_buffer[id].header.res_len;
      send_iov[id].iov_base = &res_buffer[id];
      add_write_request(ring, fds[id], &send_iov[id], (id << 16) | 1);
      /* int len = send_all(fds[id], &res_buffer[id], sizeof(response_header) + res_buffer[id].header.res_len, MSG_NOSIGNAL); */
      /* if (len < 0) { */
      /*   alive[node] = false; */
      /*   break; */
      /* } */
      /* assert(len == sizeof(response_header) + res_buffer[id].header.res_len); */
      DEBUG_PRINTF(LOG, "send res ret = %d\n", res_buffer[id].header.ret);
      // add new request
      iov[id].iov_base = &req[id];
      iov[id].iov_len = sizeof(data_request);
      add_read_request(ring, fds[id], &iov[id], id << 16);
    }
  }
}


void Engine::start_handlers() {
  auto req_handler_fn = [&](int index, int *fds, io_uring *ring) {
    request_handler(index, fds, *ring);
  };

  for (int i = 0; i < 10; ++i) {
    req_handler[i] = new std::thread(req_handler_fn, get_request_index(), req_recv_fds + i * 5, &req_recv_ring[i]);
    req_weak_handler[i] = new std::thread(req_handler_fn, get_another_request_index(), req_weak_recv_fds + i * 5, &req_weak_recv_ring[i]);
  }
}

void Engine::disconnect() {
  // wake up request sender
  ask_peer_quit();
  DEBUG_PRINTF(0, "socket close, waiting handlers\n");

  for (int i = 0; i < 10; ++i) {
    req_handler[i]->join();
    req_weak_handler[i]->join();
  }

  for (int i = 0; i < 50; ++i) {
    shutdown(req_send_fds[i], SHUT_RDWR);
    shutdown(req_recv_fds[i], SHUT_RDWR);
    shutdown(req_weak_send_fds[i], SHUT_RDWR);
    shutdown(req_weak_recv_fds[i], SHUT_RDWR);
  }

  close(data_fd);
  close(data_recv_fd);
  close(listen_fd);

  for (int i = 0; i < 4; ++i) {
    close(req_send_fds[i]);
    close(req_recv_fds[i]);
    close(req_weak_send_fds[i]);
    close(req_weak_recv_fds[i]);
  }

  for (int i = 0; i < 10; ++i) {
    io_uring_queue_exit(&req_recv_ring[i]);
    io_uring_queue_exit(&req_weak_recv_ring[i]);
    io_uring_queue_exit(&resp_ring[i]);
    io_uring_queue_exit(&resp_weak_ring[i]);
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
