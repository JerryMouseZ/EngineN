#include "include/engine.hpp"
#include "include/comm.h"
#include "include/data.hpp"
#include "include/send_recv.hpp"
#include "include/util.hpp"
#include "liburing.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <pthread.h>

Engine::Engine(): datas(nullptr), id_r(nullptr), uid_r(nullptr), sala_r(nullptr), consumers(nullptr) {
  qs = static_cast<UserQueue *>(mmap(0, MAX_NR_CONSUMER * sizeof(UserQueue), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    new (&qs[i])UserQueue;
  }
  DEBUG_PRINTF(qs, "Fail to mmap consumer queues\n");
}

Engine::~Engine() {
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    qs[i].notify_producers_exit();
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    if (consumers[i].joinable()) {
      consumers[i].join();
    }
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    qs[i].tail_commit();
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    qs[i].statistics(i);
    qs[i].~LocklessQueue();
  }

  delete[] datas;
  delete id_r;
  delete uid_r;
  delete sala_r;
}

void Engine::open(std::string aep_path, std::string disk_path) {
  std::string data_prefix = aep_path;
  if (data_prefix[data_prefix.size() - 1] != '/')
    data_prefix.push_back('/');
  datas = new Data[MAX_NR_CONSUMER];
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    datas[i].open(data_prefix + "user.data" + std::to_string(i), disk_path + "cache", disk_path + "flag" + std::to_string(i));
  }

  id_r = new Index(disk_path + "id", datas, qs);
  uid_r = new Index(disk_path + "uid", datas, qs);
  sala_r = new Index(disk_path + "salary", datas, qs);

  bool q_is_new_create;
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    if (qs[i].open(disk_path + "queue" + std::to_string(i), &q_is_new_create, datas[i].get_pmem_users(), i)) {
      return;
    }

    if (!q_is_new_create && qs[i].need_rollback()) {
      qs[i].tail_commit();
    }

    qs[i].reset_thread_states();
  }

  consumers = new std::thread[MAX_NR_CONSUMER];
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    consumers[i] = std::thread([this]{
                               init_consumer_id();
                               consumer_q = &qs[consumer_id];
                               while (consumer_q->pop())
                               ;
                               });
  }
  send_fifo = new CircularFifo<1<<16>();
}

void Engine::write(const User *user) {
  if (unlikely(!have_producer_id())) {
    init_producer_id();
  }

  DEBUG_PRINTF(LOG, "write %ld %ld %ld %ld\n", user->id, std::hash<std::string>()(std::string(user->name, 128)), std::hash<std::string>()(std::string(user->user_id, 128)), user->salary);

  uint32_t qid = user->id % MAX_NR_CONSUMER;
  uint32_t index = qs[qid].push(user);
  size_t encoded_index = (qid << 28) | index; 

  id_r->put(user->id, encoded_index);
  uid_r->put(std::hash<UserString>()(*(UserString *)(user->user_id)), encoded_index);
  sala_r->put(user->salary, encoded_index);

  // 发送到备份节点
  datas[qid].put_flag(index);
}


size_t Engine::remote_read(uint8_t select_column, uint8_t where_column, const void *column_key, size_t column_key_len, void *res) {
  // 如果远端都关掉了就不要再查了
  if (get_request_index() == -1)
    return 0;
  // 用一个队列装request
  size_t id = send_fifo->push(select_column, where_column, column_key, column_key_len, res);
  send_entry *entry = send_fifo->get_meta(id);
  // cond wait
  pthread_mutex_lock(&entry->mutex);
  while (!entry->has_come) {
    pthread_cond_wait(&entry->cond, &entry->mutex);
  }
  pthread_mutex_unlock(&entry->mutex);
  size_t ret = entry->ret;
  send_fifo->invalidate(id);
  return ret;
}

void Engine::poll_send_req_cqe() {
  data_request *reqv = nullptr;
  send_entry *metav = nullptr;
  io_uring_cqe *cqe = nullptr;
  unsigned head = -1;
  int ret = -1;
  int send_req_index = -1;

  io_uring_for_each_cqe(&send_request_ring, head, cqe) {
    if (cqe->res <= 0) {
      reqv = (data_request *)(cqe->user_data);
      metav = send_fifo->get_meta(reqv->fifo_id);
      send_req_index = get_request_index();
      if (send_req_index >= 0) {
        if ((reqv->fifo_id + 1) % 30 == 0)
          add_write_request(send_request_ring, send_fds[send_req_index], reqv, 30 * sizeof(data_request), (__u64)reqv);
        else
          add_write_request(send_request_ring, send_fds[send_req_index], reqv, sizeof(data_request), (__u64)reqv);
      } else {
        // 唤醒所有线程
      }
    }
    io_uring_cqe_seen(&send_request_ring, cqe);
  }
}

void Engine::request_sender(){
  data_request *reqv;
  send_entry *metav;
  io_uring_cqe *cqe;
  unsigned head;
  int ret;
  int send_req_index;
  while (1) {
    // 30大概是4020，能凑4096
    reqv = send_fifo->prepare_send(30, metav);
    // send to io_uring
    if (reqv == nullptr) {
      int count = 0;
      while (count < 30) {
        reqv = send_fifo->prepare_send(1, metav);
        if (reqv == nullptr)
          continue;
        count++;
        send_req_index = get_request_index();
        if (send_req_index < 0) {
          // 唤醒等待的线程
          return;
        }
        add_write_request(send_request_ring, send_fds[send_req_index], reqv, sizeof(data_request), (__u64)reqv);
        metav->socket = send_fds[send_req_index];
      }
    }
    send_req_index = get_request_index();
    if (send_req_index < 0) {
      // 唤醒线程
      return;
    }
    add_write_request(send_request_ring, send_fds[send_req_index], reqv, 30 * sizeof(data_request), (__u64)reqv);
    metav->socket = send_fds[send_req_index];
    poll_send_req_cqe();
  }
}

struct recv_cqe_data{
  int type;
  int fd;
};

void Engine::invalidate_fd(int sock) {
  for (int i = 0; i < 4; ++i) {
    if (send_fds[i] == sock || recv_fds[i] == sock) {
      if (alive[i]) {
        alive[i] = false;
        close(send_fds[i]);
      }
      break;
    }
  }
}

void Engine::response_recvier() {
  io_uring_cqe *cqe;
  /* char buffer[1024]; */
  response_header header;
  send_entry *entry;
  int req_fd_index = get_request_index();
  // 如果两个request node都挂了，其实不需要发request了
  if (req_fd_index < 0)
    return;
  add_read_request(recv_response_ring, send_fds[req_fd_index], &header, sizeof(response_header), send_fds[req_fd_index]);
  while (1) {
    // can be replace with fast wait cqe
    int ret = io_uring_wait_cqe(&recv_response_ring, &cqe);
    if (ret < 0) {
      fprintf(stderr, "io_uring error %d\n", __LINE__);
      break;
    }

    // socket close
    if (cqe->res <= 0) {
      // socket return
      int socket = cqe->user_data & 0xffffffff;
      invalidate_fd(socket);
      continue;
    }

    int type = (cqe->user_data >> 32);
    if (type == 0) {
      entry = send_fifo->get_meta(header.fifo_id);
      req_fd_index = get_request_index();
      if (req_fd_index < 0)
        return;
      add_read_request(recv_response_ring, send_fds[req_fd_index], entry->res, header.res_len, (1L << 32) | send_fds[req_fd_index]);
      // header
    } else if(type == 1) {
      // body
      // 设置返回值以及标记，唤醒等待线程
      entry->ret = header.ret;
      entry->has_come = 1;
      pthread_mutex_lock(&entry->mutex);
      pthread_cond_signal(&entry->cond);
      pthread_mutex_unlock(&entry->mutex);
      req_fd_index = get_request_index();
      if (req_fd_index < 0) {
        return;
      }
      add_read_request(recv_response_ring, send_fds[req_fd_index], &header, sizeof(response_header), send_fds[req_fd_index]);
    } else{
      // error
      assert(0);
    }
    io_uring_cqe_seen(&recv_response_ring, cqe);
  }
}

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

void Engine::poll_send_response_cqe() {
  io_uring_cqe *cqe = nullptr;
  unsigned head = -1;

  // 似乎不用做任何处理，要是不要了就发了
  io_uring_for_each_cqe(&send_response_ring, head, cqe) {
    if (cqe->res <= 0) {
    }
    io_uring_cqe_seen(&send_request_ring, cqe);
  }
}


response_buffer res_buffer;
// 太烦了，我们单线程处理请求吧
void Engine::request_handler(){
  // 用一个ring来存request，然后可以异步处理，是一个SPMC的模型，那就不能用之前的队列了
  data_request req[2];
  int req_fd_index = get_request_index();
  int req_another_index = get_another_request_index();
  assert(req_fd_index >= 0);
  assert(req_another_index >= 0);

  // userdata 用0和1来区分两个节点的请求
  // 同时读两个节点的请求
  add_read_request(recv_request_ring, recv_fds[req_fd_index], req, sizeof(data_request), req_fd_index);
  add_read_request(recv_request_ring, recv_fds[req_another_index], &req[1], sizeof(data_request), req_another_index); // 正常情况下这个请求是不会被用到的

  io_uring_cqe *cqe;
  while (1) {
    int ret = io_uring_wait_cqe(&recv_request_ring, &cqe);
    DEBUG_PRINTF(ret < 0, "io_uring error line %d\n", __LINE__);
    if (cqe->res <= 0) {
      invalidate_fd(cqe->user_data);
    } else {
      // process data
      uint8_t select_column, where_column;
      uint32_t fifo_id;
      void *key;
      if (cqe->user_data == req_fd_index) {
        select_column = req[0].select_column;
        where_column = req[0].where_column;
        key = req[0].key;
        fifo_id = req[0].fifo_id;
      } else {
        select_column = req[1].select_column;
        where_column = req[1].where_column;
        key = req[1].key;
        fifo_id = req[1].fifo_id;
      }

      int num = local_read(select_column, where_column, key, 128, res_buffer.body);
      res_buffer.header.fifo_id = fifo_id;
      res_buffer.header.ret = num;
      res_buffer.header.res_len = get_column_len(select_column) * num;
      add_write_request(send_response_ring, cqe->user_data, &res_buffer, sizeof(response_header) + res_buffer.header.res_len, 0);
    }

    // add another read request
    int index = cqe->user_data;
    if (alive[index]) {
      if (index == req_fd_index)
        add_read_request(recv_request_ring, recv_fds[index], req, sizeof(data_request), index);
      else
        add_read_request(recv_request_ring, recv_fds[req_another_index], &req[1], sizeof(data_request), req_another_index); // 正常情况下这个请求是不会被用到的
    }

    io_uring_cqe_seen(&recv_request_ring, cqe);

    poll_send_response_cqe();
    if (!alive[req_fd_index] && !alive[req_another_index])
      break;
  }
}


size_t Engine::local_read(int32_t select_column,
                          int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
  size_t result = 0;
  switch(where_column) {
  case Id:
    result = id_r->get(column_key, where_column, select_column, res, false);
    DEBUG_PRINTF(LOG, "select %s where ID = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
    break;
  case Userid:
    result = uid_r->get(column_key, where_column, select_column, res, false);
    DEBUG_PRINTF(LOG, "select %s where UID = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
    break;
  case Name:
    assert(0);
    /* result = name_r->get(column_key, where_column, select_column, res, false); */
    DEBUG_PRINTF(LOG, "select %s where Name = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
    break;
  case Salary:
    result = sala_r->get(column_key, where_column, select_column, res, true);
    DEBUG_PRINTF(LOG, "select %s where salary = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
    break;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return result;
}

size_t Engine::read(int32_t select_column,
                    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
  size_t result = 0;
  result = local_read(select_column, where_column, column_key, column_key_len, res);
  if (result == 0) {
    return remote_read(select_column, where_column, column_key, column_key_len, res);
  }
  return result;
}


std::string Engine::column_str(int column)
{
  switch(column) {
  case Id:
    return "ID";
    break;
  case Userid:
    return "UID";
    break;
  case Name:
    return "Name";
    break;
  case Salary:
    return "Salary";
    break;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return "";
}


// 创建listen socket，尝试和别的机器建立两条连接
void Engine::connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num) {
  if (host_info == NULL || peer_host_info == NULL)
    return;
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

  connect(infos, peer_host_info_num + 1, my_index);
}


void Engine::connect(std::vector<info_type> &infos, int num, int host_index) {
  this->host_index = host_index;
  volatile bool flag = false;
  listen_fd = setup_listening_socket(infos[host_index].first.c_str(), infos[host_index].second);
  sockaddr_in client_addrs[3];
  socklen_t client_addr_lens[3];
  // 添加3个accept请求
  std::thread listen_thread(listener, listen_fd, recv_fds, &infos, &flag, &data_recv_fd);
  // 向其它节点发送连接请求
  for (int i = 0; i < 4; ++i) {
    if (i != host_index) {
      send_fds[i] = connect_to_server(infos[i].first.c_str(), infos[i].second);
      fprintf(stderr, "connect to %s success\n", infos[i].first.c_str());
    }
  }

  flag = true;
  data_fd = connect_to_server(infos[get_backup_index()].first.c_str(), infos[get_backup_index()].second);
  // 建立同步数据的连接
  listen_thread.join();
  fprintf(stderr, "connection done\n");
  io_uring_queue_init(QUEUE_DEPTH, &send_request_ring, 0);
  io_uring_queue_init(QUEUE_DEPTH, &recv_response_ring, 0);
  io_uring_queue_init(QUEUE_DEPTH, &recv_request_ring, 0);
  io_uring_queue_init(QUEUE_DEPTH, &send_response_ring, 0);
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
    if (alive[2])
      return 2;
    if (alive[3])
      return 3;
    break;
  case 1:
    if (alive[3])
      return 3;
    if (alive[2])
      return 2;
    break;
  case 2:
    if (alive[0])
      return 0;
    if (alive[1])
      return 1;
    break;
  case 3:
    if (alive[1])
      return 1;
    if (alive[0])
      return 0;
    break;
  }
  return -1;
}

int Engine::get_another_request_index() {
  switch(host_index) {
  case 0:
    if (alive[3])
      return 3;
    break;
  case 1:
    if (alive[2])
      return 2;
    break;
  case 2:
    if (alive[1])
      return 0;
    break;
  case 3:
    if (alive[0])
      return 1;
    break;
  }
  return -1;
}
