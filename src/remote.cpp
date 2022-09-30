#include "include/engine.hpp"
#include "include/comm.h"
#include "include/data.hpp"
#include "include/send_recv.hpp"
#include "include/thread_id.hpp"
#include "include/util.hpp"
#include "liburing.h"
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
  io_uring_queue_init(QUEUE_DEPTH, &req_recv_ring, 0);
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
  // 如果远端都关掉了就不要再查了
  if (get_request_index() == -1)
    return 0;
  // 用一个队列装request
  data_request data = data_request();
  data.fifo_id = 0;
  data.select_column = select_column;
  data.where_column = where_column;
  memcpy(data.key, column_key, column_key_len);
  int len = send_all(req_send_fds[reader_id], &data, sizeof(data), MSG_NOSIGNAL);
  if (len < 0) {
    fprintf(stderr, "send error : %d, errno : %d\n", len, errno);
  }
  assert(len == sizeof(data_request));

  if (where_column == Id || where_column == Salary)
    DEBUG_PRINTF(LOG, "add remote read request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)column_key);
  else
    DEBUG_PRINTF(LOG, "add send remote read request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)column_key);

  response_header header;
  len = recv_all(req_send_fds[reader_id], &header, sizeof(header), MSG_WAITALL);
  if (len < 0) {
    fprintf(stderr, "recv error : %d, error : %d\n", len, errno);
  }
  assert(len == sizeof(header));

  len = recv_all(req_send_fds[reader_id], res, header.res_len, MSG_WAITALL);
  if (len < 0) {
    fprintf(stderr, "recv error : %d, error : %d\n", len, errno);
  }
  assert(len == header.res_len);
  return header.ret;
}

/* void Engine::term_sending_request() { */
/*   send_entry *entry; */
/*   while (1) { */
/*     entry = send_fifo->pop(); */
/*     if (entry == nullptr) */
/*       break; */
/*     // 唤醒等待的线程 */
/*     entry->has_come = 1; */
/*     entry->ret = 0; */
/*     pthread_mutex_lock(&entry->mutex); */
/*     pthread_cond_signal(&entry->cond); */
/*     pthread_mutex_unlock(&entry->mutex); */
/*   } */
/* } */


/* void Engine::request_sender(){ */
/*   data_request *reqv; */
/*   send_entry *metav; */
/*   io_uring_cqe *cqe; */
/*   unsigned head; */
/*   int ret; */
/*   int send_req_index; */
/*   while (1) { */
/*     DEBUG_PRINTF(LOG, "waiting for fifo send\n"); */
/*     // 30大概是4020，能凑4096 */
/*     reqv = send_fifo->prepare_send(1, &metav); // 读是会阻塞的，如果要等前面的请求完成才有后面的请求的话可能不太行，毕竟read没有tail commit */
/*     if (exited) */
/*       return; */

/*     send_req_index = get_request_index(); */
/*     if (send_req_index < 0) { */
/*       // stop remote read, wake up threads */
/*       term_sending_request(); */
/*       return; */
/*     } */

/*     send(send_fds[send_req_index], reqv, sizeof(data_request), MSG_NOSIGNAL); */
/*     if (reqv->where_column == Id || reqv->where_column == Salary) */
/*       DEBUG_PRINTF(LOG, "send remote read request [%d - %d] select %s where %s = %ld\n", reqv->fifo_id, reqv->fifo_id, column_str(reqv->select_column).c_str(), column_str(reqv->where_column).c_str(), *(uint64_t *)reqv->key); */
/*     else */
/*       DEBUG_PRINTF(LOG, "send remote read request [%d - %d] select %s where %s = %s\n", reqv->fifo_id, reqv->fifo_id, column_str(reqv->select_column).c_str(), column_str(reqv->where_column).c_str(), (char *)reqv->key); */
/*     metav->socket = send_fds[send_req_index]; */
/*   } */
/* } */

struct recv_cqe_data{
  int type;
  int fd;
};

/* void Engine::invalidate_fd(int sock) { */
/*   for (int i = 0; i < 4; ++i) { */
/*     if (send_fds[i] == sock || recv_fds[i] == sock) { */
/*       if (alive[i]) { */
/*         alive[i] = false; */
/*       } */
/*       break; */
/*     } */
/*   } */
/* } */


// 有一个问题，就是可能recv和send的不是同一个socket，但是没有关系，我们只负责接收
/* void Engine::response_recvier() { */
/*   response_header header; */
/*   send_entry *entry; */
/*   int req_fd_index = get_request_index(); */
/*   // 如果两个request node都挂了，其实不需要发request了 */
/*   if (req_fd_index < 0) */
/*     return; */
/*   while (1) { */
/*     int len = recv(send_fds[req_fd_index], &header, sizeof(response_header), 0); */
/*     if (exited) */
/*       return; */
/*     // socket close */
/*     if (len <= 0) { */
/*       invalidate_fd(req_fd_index); */
/*       return; */
/*     } */

/*     entry = send_fifo->get_meta(header.fifo_id); */
/*     // recv body */
/*     len = recv(send_fds[req_fd_index], entry->res, header.res_len, 0); */
/*     assert(len == header.res_len); */
/*     // body */
/*     // 设置返回值以及标记，唤醒等待线程 */
/*     entry->ret = header.ret; */
/*     entry->has_come = 1; */
/*     DEBUG_PRINTF(LOG, "receiving read response [%d] ret = %d\n", header.fifo_id, entry->ret); */
/*     pthread_mutex_lock(&entry->mutex); */
/*     pthread_cond_signal(&entry->cond); */
/*     pthread_mutex_unlock(&entry->mutex); */
/*     DEBUG_PRINTF(LOG, "waking up [%d] %p\n", header.fifo_id, &entry->cond); */
/*     req_fd_index = get_request_index(); */
/*     if (req_fd_index < 0) { */
/*       return; */
/*     } */
/*   } */
/* } */

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

response_buffer res_buffer;
void Engine::request_handler(){
  // 用一个ring来存request，然后可以异步处理，是一个SPMC的模型，那就不能用之前的队列了
  data_request req[50];
  for (int i = 0; i < 50; ++i) {
    add_read_request(req_recv_ring, req_recv_fds[i], &req[i], sizeof(data_request), i);
  }
  io_uring_cqe *cqe;
  while (1) {
    cqe = wait_cqe_fast(&req_recv_ring);
    if (exited)
      return;
    if (cqe == nullptr) {
      break;
    }
    
    if (cqe->res <= 0) {
      break;
    }
    assert(cqe->res == sizeof(data_request));
    int index = cqe->user_data;
    add_read_request(req_recv_ring, req_recv_fds[index], &req[index], sizeof(data_request), index);
    // process data
    uint8_t select_column, where_column;
    uint32_t fifo_id;
    void *key;
    select_column = req[index].select_column;
    where_column = req[index].where_column;
    key = req[index].key;
    fifo_id = req[index].fifo_id;
    if (where_column == Id || where_column == Salary)
      DEBUG_PRINTF(LOG, "recv request select %s where %s = %ld\n", column_str(select_column).c_str(), column_str(where_column).c_str(), *(uint64_t *)key);
    else
      DEBUG_PRINTF(LOG, "recv request select %s where %s = %s\n", column_str(select_column).c_str(), column_str(where_column).c_str(), (char *)key);
    int num = local_read(select_column, where_column, key, 128, res_buffer.body);
    res_buffer.header.fifo_id = fifo_id;
    res_buffer.header.ret = num;
    res_buffer.header.res_len = get_column_len(select_column) * num;
    /* int ret = send(cqe->user_data, &res_buffer, sizeof(response_header) + res_buffer.header.res_len, MSG_NOSIGNAL); */
    /* assert(ret == sizeof(response_header) + res_buffer.header.res_len); */
    int len = send(req_recv_fds[index], &res_buffer, sizeof(response_header) + res_buffer.header.res_len, MSG_NOSIGNAL);
    assert(len == sizeof(response_header) + res_buffer.header.res_len);
    DEBUG_PRINTF(LOG, "send res ret = %d\n", res_buffer.header.ret);
    io_uring_cqe_seen(&req_recv_ring, cqe);
  }
}


void Engine::start_handlers() {
  /* auto req_sender_fn = [&]() { */
  /*   request_sender(); */
  /* }; */

  /* auto rep_recver_fn = [&]() { */
  /*   response_recvier(); */
  /* }; */

  auto req_handler_fn = [&]() {
    request_handler();
  };
  // std::thread不能直接运行类的成员函数，用lambda稍微封装一下
  /* req_sender = new std::thread(req_sender_fn); */
  /* rep_recvier = new std::thread(rep_recver_fn); */
  req_handler = new std::thread(req_handler_fn);
}

void Engine::disconnect() {
  // wake up request sender
  exited = true;
  send_fifo->exit(); // wake up send fifo
  for (int i = 0; i < 50; ++i) {
    shutdown(req_send_fds[i], SHUT_RDWR);
    shutdown(req_weak_send_fds[i], SHUT_RDWR);
    shutdown(req_weak_send_fds[i], SHUT_RDWR);
    shutdown(req_weak_recv_fds[i], SHUT_RDWR);
  }
  close(data_fd);
  close(data_recv_fd);
  close(listen_fd);

  DEBUG_PRINTF(LOG, "socket close, waiting handlers\n");

  req_sender->join();
  rep_recvier->join();
  req_handler->join();

  for (int i = 0; i < 4; ++i) {
    close(req_send_fds[i]);
    close(req_weak_send_fds[i]);
    close(req_weak_send_fds[i]);
    close(req_weak_recv_fds[i]);
  }
  io_uring_queue_exit(&req_recv_ring);
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
