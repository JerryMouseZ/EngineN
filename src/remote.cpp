#include "include/engine.hpp"
#include "include/comm.h"
#include "include/data.hpp"
#include "include/thread_id.hpp"
#include "include/util.hpp"
#include "liburing.h"
#include <bits/types/struct_iovec.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <liburing.h>
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
  for (int i = 0; i < 4 * 10; ++i) {
    ret = io_uring_queue_init(QUEUE_DEPTH, &req_recv_ringall[i], 0);
    DEBUG_PRINTF(ret == 0, "queue init error %d:%s\n", errno, strerror(errno));
    assert(ret == 0);
  }
  signal(SIGPIPE, SIG_IGN);
  listen_fd = setup_listening_socket(infos[host_index].first.c_str(), infos[host_index].second);
  sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(client_addr);
  char client_addr_str[60];

  // 建立同步数据的连接
  for (int i = 0; i < 4; ++i)
    alive[i] = true;

  std::thread listen_thread(listener, listen_fd, &infos, recv_uvh);

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


#ifndef BROADCAST

size_t Engine::remote_read(uint8_t select_column, uint8_t where_column, const void *column_key, size_t column_key_len, void *res, int seq) {
  if (unlikely(!have_reader_id())) {
    init_reader_id();
  }
  int fd = -1;
  int current_req_node = -1;
  if (seq == 0) {
    if (alive[get_request_index()]) {
      fd = req_send_fds[reader_id];
      current_req_node = get_request_index();
    } else {
      return 0;
    }
  }
  else if (seq == 1) {
    if (alive[get_another_request_index()]) {
      fd = req_weak_send_fds[reader_id];
      current_req_node = get_another_request_index();
    } else {
      return 0;
    }
  }
  else if (seq == 2){
    if (alive[get_backup_index()]) {
      fd = req_backup_send_fds[reader_id];
      current_req_node = get_backup_index();
    } else {
      return 0;
    }
  } else {
    return 0;
  }

  data_request data;
  data.fifo_id = 1;
  data.select_column = select_column;
  data.where_column = where_column;
  memcpy(data.key, column_key, column_key_len);
  int len = send_all(fd, &data, sizeof(data), MSG_NOSIGNAL);
  if (len < 0) {
    fprintf(stderr, "send error %d to node %d, retry request\n", len, current_req_node);
    alive[current_req_node] = false;
    return remote_read(select_column, where_column, column_key, column_key_len, res, seq + 1);
  }
  assert(len == sizeof(data_request));

  if (where_column == Id || where_column == Salary)
    DEBUG_PRINTF(VLOG, "add remote read request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)column_key);
  else
    DEBUG_PRINTF(VLOG, "add send remote read request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)column_key);

  response_header header;
  len = recv_all(fd, &header, sizeof(header), MSG_WAITALL);
  if (len <= 0) {
    fprintf(stderr, "recv header error %d from node %d, retry request\n", len, current_req_node);
    alive[current_req_node] = false;
    return remote_read(select_column, where_column, column_key, column_key_len, res, seq + 1);
  }
  assert(len == sizeof(header));

  if (header.res_len == 0) {
    return remote_read(select_column, where_column, column_key, column_key_len, res, seq + 1);
  }
  len = recv_all(fd, res, header.res_len, MSG_WAITALL);
  if (len <= 0) {
    fprintf(stderr, "recv body error %d from node %d, retry request\n", len, current_req_node);
    alive[current_req_node] = false;
    return remote_read(select_column, where_column, column_key, column_key_len, res, seq + 1);
  }
  assert(len == header.res_len);
  if (where_column == Salary) {
    return header.ret + remote_read(select_column, where_column, column_key, column_key_len, (char *)res + header.res_len, seq + 1);
  }
  return header.ret;
}

#else

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

#endif

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

#ifndef BROADCAST

void Engine::ask_peer_quit() {
  data_request req;
  req.select_column = 22;
  req.where_column = 22;
  for (int i = 0; i < 10; ++i) {
    send_all(req_send_fds[i * 5], &req, sizeof(req), MSG_NOSIGNAL);
    send_all(req_weak_send_fds[i * 5], &req, sizeof(req), MSG_NOSIGNAL);
    send_all(req_backup_send_fds[i * 5], &req, sizeof(req), MSG_NOSIGNAL);
  }
}

#else

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

#endif

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
  while (1) {
    io_uring_submit_and_wait(&ring, 1);
    unsigned head;
    unsigned count = 0;
    io_uring_cqe *cqe;
    io_uring_for_each_cqe(&ring, head, cqe) {
      ++count;
      int id = cqe->user_data >> 16;
      assert(id < 5);
      int type = cqe->user_data & 1;
      int len = cqe->res;
      if (type == 1) {
        if (len < 0) {
          fprintf(stderr, "send error %d to node %d\n", len, node);
          return;
        }
        // 看起来不会出现只发一部分的情况，先不管了
        /* DEBUG_PRINTF(len == send_iov[id].iov_len, "send %d, res %ld\n", len, send_iov[id].iov_len); // 可能需要一个自增的id，不然这个send_iov可能是下一个包的 */
      } else {
        if (len <= 0) {
          fprintf(stderr, "recv error %d from node %d\n", len, node);
          return;
        }
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
          DEBUG_PRINTF(VLOG, "recv request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)key);
        else
          DEBUG_PRINTF(VLOG, "recv request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)key);
        int num = local_read(select_column, where_column, key, 128, res_buffer[id].body);
        res_buffer[id].header.fifo_id = fifo_id;
        res_buffer[id].header.ret = num;
        res_buffer[id].header.res_len = get_column_len(select_column) * num;

        send_iov[id].iov_len = sizeof(response_header) + res_buffer[id].header.res_len;
        send_iov[id].iov_base = &res_buffer[id];
        add_write_request(ring, fds[id], &send_iov[id], (id << 16) | 1);
        DEBUG_PRINTF(VLOG, "send res ret = %d\n", res_buffer[id].header.ret);
        // add new request
        iov[id].iov_base = &req[id];
        iov[id].iov_len = sizeof(data_request);
        add_read_request(ring, fds[id], &iov[id], id << 16);
      } // end for
    } // end for
    io_uring_cq_advance(&ring, count);
  } // end while
}

#ifndef BROADCAST

void Engine::start_handlers() {
  auto req_handler_fn = [&](int index, int *fds, io_uring *ring) {
    request_handler(index, fds, *ring);
  };

  for (int i = 0; i < 10; ++i) {
    req_handler[i] = new std::thread(req_handler_fn, get_request_index(), req_recv_fds + i * 5, &req_recv_ring[i]);
    req_weak_handler[i] = new std::thread(req_handler_fn, get_another_request_index(), req_weak_recv_fds + i * 5, &req_weak_recv_ring[i]);
    req_backup_handler[i] = new std::thread(req_handler_fn, get_backup_index(), req_backup_recv_fds + i * 5, &req_backup_recv_ring[i]);
  }
}

#else

void Engine::start_handlers() {
  auto req_handler_fn = [&](int index, int *fds, io_uring *ring) {
    request_handler(index, fds, *ring);
  };

  for (int nb_i = 0; nb_i < 3; ++nb_i) {
    int neighbor_idx = neighbor_index[nb_i];
    for (int i = 0; i < 10; i++) {
      int hdi = neighbor_idx * 10 + i;
      req_handlerall[hdi] = new std::thread(req_handler_fn, neighbor_idx, recv_fdall[neighbor_idx] + i * 5, &req_recv_ringall[hdi]);
    }
  }
}

#endif

#ifndef BROADCAST

void Engine::disconnect() {
  // wake up request sender
  ask_peer_quit();
  DEBUG_PRINTF(0, "socket close, waiting handlers\n");

  for (int i = 0; i < 10; ++i) {
    req_handler[i]->join();
    req_weak_handler[i]->join();
    req_backup_handler[i]->join();
  }

  for (int i = 0; i < 50; ++i) {
    shutdown(req_send_fds[i], SHUT_RDWR);
    shutdown(req_recv_fds[i], SHUT_RDWR);
    shutdown(req_backup_send_fds[i], SHUT_RDWR);
    shutdown(req_backup_recv_fds[i], SHUT_RDWR);
    shutdown(req_weak_send_fds[i], SHUT_RDWR);
    shutdown(req_weak_recv_fds[i], SHUT_RDWR);
  }

  /* for (int i = 0; i < 16; ++i) { */
  /*   close(data_fd[i]); */
  /*   close(data_recv_fd[i]); */
  /* } */
  close(listen_fd);

  for (int i = 0; i < 50; ++i) {
    close(req_send_fds[i]);
    close(req_recv_fds[i]);
    close(req_backup_send_fds[i]);
    close(req_backup_recv_fds[i]);
    close(req_weak_send_fds[i]);
    close(req_weak_recv_fds[i]);
  }

  fprintf(stderr, "queue exit\n");
  for (int i = 0; i < 10; ++i) {
    io_uring_queue_exit(&req_recv_ring[i]);
    io_uring_queue_exit(&req_weak_recv_ring[i]);
    io_uring_queue_exit(&req_backup_recv_ring[i]);
  }
}

#else

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
 
  fprintf(stderr, "queue exit\n");
  for (int i = 0; i < 4 * 10; ++i) {
    io_uring_queue_exit(&req_recv_ringall[i]);
  }
}

#endif

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
