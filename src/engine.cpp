#include "include/engine.hpp"
#include "include/data.hpp"
#include <cstddef>
#include <cstdint>
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

struct query{
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  void *res;
  uint64_t unique_id; // 这个是给对端确认的，对面原样发回来就知道
  uint8_t select_column;
  uint8_t where_column;
  void *column_key;
};


size_t Engine::send_query(uint8_t select_column, uint8_t where_column, const void *column_key, void *res) {
  // 用一个队列装request

  // cond wait
  return 0;
}

size_t Engine::read(int32_t select_column,
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

  if (result == 0) {
    // send query
    return send_query(select_column, where_column, column_key, res);
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
  /* io_uring_queue_init(QUEUE_DEPTH, &send_ring, 0); */
  /* io_uring_queue_init(QUEUE_DEPTH, &recv_ring, 0); */
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
    return 1;
  case 3:
    return 0;
  }
  fprintf(stderr, "error host index %d\n", host_index);
  return -1;
}
