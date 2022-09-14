#ifndef CS_H
#define CS_H
#include <arpa/inet.h>
#include <cstdio>
#include <liburing.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <thread>
#include "comm.h"
#include "liburing/io_uring.h"

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, int *recv_fds, std::vector<info_type> *infos, volatile bool *flag, int *data_recv_fd);

class Connector {
public:
  Connector() {}
  ~Connector() {
    io_uring_queue_exit(&send_ring);
    io_uring_queue_exit(&recv_ring);
  }


  void connect(std::vector<info_type> &infos, int num, int host_index) {
    this->host_index = host_index;
    io_uring_queue_init(QUEUE_DEPTH, &send_ring, 0);
    io_uring_queue_init(QUEUE_DEPTH, &recv_ring, 0);
    volatile bool flag = false;
    listen_fd = setup_listening_socket(infos[host_index].first.c_str(), infos[host_index].second);
    sockaddr_in client_addrs[3];
    socklen_t client_addr_lens[3];
    // 添加3个accept请求
    std::thread listen_thread(listener, listen_fd, recv_fds, &infos, &flag, &data_recv_fd);
    // 向其它节点发送连接请求
    for (int i = 0; i < 4; ++i) {
      if (i != host_index) {
        send_fds[i] = Connect(infos[i].first.c_str(), infos[i].second);
        fprintf(stderr, "connect to %s success\n", infos[i].first.c_str());
      }
    }
    
    flag = true;
    data_fd = Connect(infos[get_backup_index()].first.c_str(), infos[get_backup_index()].second);
    // 建立同步数据的连接
    listen_thread.join();
    fprintf(stderr, "connection done\n");
  }


private:
  // 两两备份数据，0向1备份，2向3备份
  int get_backup_index() {
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
  int get_request_index() {
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


private:
  io_uring send_ring;
  io_uring recv_ring;
  int host_index;

  int listen_fd;
  bool alive[4];
  int send_fds[4];
  int recv_fds[4];
  int data_fd;
  int data_recv_fd;
};
#endif

