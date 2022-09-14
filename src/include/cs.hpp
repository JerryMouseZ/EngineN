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
#include "comm.h"
#include "liburing/io_uring.h"

class Connector {
public:
  Connector() {}
  ~Connector() {
    io_uring_queue_exit(&send_ring);
    io_uring_queue_exit(&recv_ring);
  }


  using info_type = std::pair<std::string, int>;
  void connect(std::vector<info_type> &infos, int num, int host_index) {
    this->host_index = host_index;
    io_uring_queue_init(QUEUE_DEPTH, &send_ring, 0);
    io_uring_queue_init(QUEUE_DEPTH, &recv_ring, 0);
    listen_fd = setup_listening_socket(infos[host_index].first.c_str(), infos[host_index].second);
    sockaddr_in client_addrs[3];
    socklen_t client_addr_lens[3];
    // 添加3个accept请求
    for (int i = 0; i < 3; ++i) {
      add_accept_request(recv_ring, listen_fd, &client_addrs[i], &client_addr_lens[i]);
    }
    // 等待其它节点也开始listen
    sleep(1);
    
    // 向其它节点发送连接请求
    for (int i = 0; i < 4; ++i) {
      if (i != host_index) {
        send_fds[i] = add_connect_request(send_ring, infos[i].first.c_str(), infos[i].second);
      }
    }
    
    int client_fds[3];
    // 处理其它节点的连接请求
    for (int i = 0; i < 4; ++i) {
      if (i == host_index)
        continue;
      io_uring_cqe *cqe;
      int ret = io_uring_wait_cqe(&recv_ring, &cqe);
      if (ret < 0) {
        fprintf(stderr, "io_uring_wait_cqe error\n");
        exit(1);
      }

      // client fd会在cqe的res中
      client_fds[i] = cqe->res;
      if (client_fds[i] < 0) {
        fprintf(stderr, "accept failed\n");
      }
      io_uring_cqe_seen(&recv_ring, cqe);
    }
    
    // 给接受的连接请求排序，保证fd在它的相应序号上
    sockaddr_in addr;
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 4; ++j) {
        if (j == host_index)
          continue;
        inet_pton(AF_INET, infos[j].first.c_str(), &addr.sin_addr);
        if (memcmp(&addr, &client_addrs[i], sizeof(sockaddr_in)) == 0) {
          recv_fds[j] = client_fds[i];
        }
      }
    }
    
    for (int i = 0; i < 4; ++i) {
      if (i == host_index)
        continue;
      io_uring_cqe *cqe;
      int ret = io_uring_wait_cqe(&send_ring, &cqe);
      if (ret < 0) {
        fprintf(stderr, "io_uring_wait_cqe error\n");
        exit(1);
      }
      if (cqe->res < 0) {
        fprintf(stderr, "connect to %s failed\n", infos[i].first.c_str());
      }
      io_uring_cqe_seen(&recv_ring, cqe);
    }

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
};
#endif

