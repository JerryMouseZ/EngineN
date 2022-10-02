#include "include/comm.h"
#include "liburing.h"
#include <asm-generic/socket.h>
#include <bits/types/struct_iovec.h>
#include <cstring>
#include <ctime>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <assert.h>
#include "include/util.hpp"

const char *this_host_info = nullptr;
void fatal_error(const char *syscall) {
  perror(syscall);
  exit(1);
}

int setup_listening_socket(const char *ip, int port) {
  int sock;
  struct sockaddr_in srv_addr;

  sock = socket(PF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (sock == -1) {
    fatal_error("socket() error");
  }

  int enable = 1;
  assert (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) >= 0);
  assert (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) >= 0);

  memset(&srv_addr, 0, sizeof(srv_addr));
  srv_addr.sin_family = AF_INET;
  srv_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip, &srv_addr.sin_addr) <= 0) {
    fatal_error("invalid ip");
  }

  /* We bind to a port and turn this socket into a listening
   * socket.
   * */
  if (bind(sock, (const struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0)
    fatal_error("bind()");

  if (listen(sock, 20) < 0)
    fatal_error("listen()");

  return sock;
}


int add_accept_request(io_uring &ring, int server_socket, struct sockaddr_in *client_addr, socklen_t *client_addr_len) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  io_uring_prep_accept(sqe, server_socket, (struct sockaddr *) client_addr,
                       client_addr_len, 0);
  io_uring_submit(&ring);
  return 0;
}


int connect_to_server(const char *this_host_ip, const char *ip, int port) {
  int sock = socket(PF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  assert(sock > 0);

  // bind
  sockaddr_in client_addr;
  int client_port = 0;
  memset(&client_addr, 0, sizeof(client_addr));
  client_addr.sin_family = AF_INET;
  client_addr.sin_port = htons(client_port);
  assert (inet_pton(AF_INET, this_host_ip, &client_addr.sin_addr) > 0);
  assert (bind(sock, (const struct sockaddr *)&client_addr, sizeof(client_addr)) >= 0);

  // set nodelay
  int enable = 1;
  int ret;
  ret = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable));
  assert(ret != -1);
  
  // set nonblocking
  int32_t flags = fcntl(sock, F_GETFL, 0);
  assert(flags != -1);
  flags |= O_NONBLOCK;
  ret = fcntl(sock, F_SETFL, flags);
  assert(ret != -1);
  
  // connect and retry
  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  assert (inet_pton(AF_INET, ip, &server_addr.sin_addr) > 0);
  ret = connect(sock, (sockaddr *) &server_addr, sizeof(server_addr));
  while (ret != 0) {
    ret = connect(sock, (sockaddr *) &server_addr, sizeof(server_addr));
    usleep(500);
  }

  // reset blocking
	flags = fcntl(sock, F_GETFL, 0);
	assert(flags != -1);
	flags &= ~O_NONBLOCK;
	ret = fcntl(sock, F_SETFL, flags);
	assert(ret != -1);
  return sock;
}


int add_read_request(io_uring &ring, int client_socket, iovec *iov, __u64 udata) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  assert(sqe);
  io_uring_prep_readv(sqe, client_socket, iov, 1, 0);
  io_uring_sqe_set_data64(sqe, udata);
  int ret = io_uring_submit(&ring);
  // 加上SQPOLL以后可能会返回或者大于1的值
  DEBUG_PRINTF(ret >= 0, "io_uring submit error %s\n", strerror(-ret));
  /* assert(ret == 1); */
  return 0;
}


int add_write_request(io_uring &ring, int client_socket, void *buffer, size_t len, __u64 udata) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  io_uring_prep_send(sqe, client_socket, buffer, len, MSG_NOSIGNAL);
  io_uring_sqe_set_data64(sqe, udata);
  io_uring_submit(&ring);
  return 0;
}

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, std::vector<info_type> *infos, int *data_recv_fd, int data_peer_index, int host_index, int req_recv_fds[], int req_weak_recv_fds[], int req_recv_index, int req_weak_recv_index) {
  sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(sockaddr_in);
  int num = 0, req_recv_fd_cnt = 0, req_weak_recv_fd_cnt = 0;
  while (num < 1 + 2 * 50) {
    int client_fd = accept(listen_fd, (sockaddr *)&client_addr, &client_addr_len);
    if (client_fd < 0) {
      usleep(50);
      continue;
    }

    // set nodelay
    int enable = 1;
    int ret;
    ret = setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable));
    assert(ret != -1);

    for (int j = 0; j < 4; ++j) {
      sockaddr_in addr;
      inet_pton(AF_INET, (*infos)[j].first.c_str(), &addr.sin_addr);
      if (memcmp(&addr.sin_addr, &client_addr.sin_addr, sizeof(sockaddr_in::sin_addr)) == 0) {
        if (j == data_peer_index ) {
          *data_recv_fd = client_fd;
          DEBUG_PRINTF(0, "data_recv_fd from %s\n", (*infos)[j].first.c_str());
        } 
        else if (j == req_recv_index) {
          req_recv_fds[req_recv_fd_cnt++] = client_fd;
          DEBUG_PRINTF(0, "[%d <- %d] req_recv_fd[%d] from %s\n", host_index, j, req_recv_fd_cnt, (*infos)[j].first.c_str());
        } 
        else if (j == req_weak_recv_index) {
          req_weak_recv_fds[req_weak_recv_fd_cnt++] = client_fd;
          DEBUG_PRINTF(0, "[%d <- %d] req_weak_recv_fd[%d] from %s\n", host_index, j, req_weak_recv_fd_cnt, (*infos)[j].first.c_str());
        } 
        else {
          assert(0);
        }
      }
    }
    num++;
  }
}


io_uring_cqe *wait_cqe_fast(struct io_uring *ring)
{
  struct io_uring_cqe *cqe;
  unsigned head;
  int ret;

  io_uring_for_each_cqe(ring, head, cqe)
    return cqe;

  ret = io_uring_wait_cqe(ring, &cqe);
  if (ret) {
    fprintf(stderr, "wait cqe %d\n", ret);
    return nullptr;
  }
  return cqe;
}

int send_all(int fd, const void *src, size_t n, int flag) {
  auto rest = n;
  int sent = 0, ret;
  while (rest > 0) {
    ret = send(fd, ((const char *)src) + sent, rest, flag);
    if (ret < 0) {
      return ret;
    }
    rest -= ret;
    sent += ret;
  }

  return n;
}

int recv_all(int fd, void *dst, size_t n, int flag) {
  auto rest = n;
  int recv_cnt = 0, ret;
  while (rest > 0) {
    ret = recv(fd, ((char *)dst) + recv_cnt, rest, flag);
    if (ret < 0) {
      return ret;
    }
    rest -= ret;
    recv_cnt += ret;
  }

  return n;
}
