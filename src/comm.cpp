#include "include/comm.h"
#include <cerrno>
#include <cstdio>
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
#include "include/config.hpp"
#include "uv.h"

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


/* int add_accept_request(io_uring &ring, int server_socket, struct sockaddr_in *client_addr, socklen_t *client_addr_len) { */
/*   struct io_uring_sqe *sqe = io_uring_get_sqe(&ring); */
/*   io_uring_prep_accept(sqe, server_socket, (struct sockaddr *) client_addr, */
/*                        client_addr_len, 0); */
/*   io_uring_submit(&ring); */
/*   return 0; */
/* } */


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

  if (setsockopt(sock, SOL_SOCKET, SO_ZEROCOPY, &enable, sizeof(enable)))
    fprintf(stderr,"setsockopt zerocopy %s\n", strerror(errno));

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
    usleep(20);
  }

  // reset blocking
  flags = fcntl(sock, F_GETFL, 0);
  assert(flags != -1);
  flags &= ~O_NONBLOCK;
  ret = fcntl(sock, F_SETFL, flags);
  assert(ret != -1);
  return sock;
}


/* int add_read_request(io_uring &ring, int client_socket, iovec *iov, __u64 udata) { */
/*   struct io_uring_sqe *sqe = io_uring_get_sqe(&ring); */
/*   assert(sqe); */
/*   io_uring_prep_readv(sqe, client_socket, iov, 1, 0); */
/*   io_uring_sqe_set_data64(sqe, udata); */
/*   /1* io_uring_submit(&ring); *1/ */
/*   return 0; */
/* } */


/* int add_write_request(io_uring &ring, int client_socket, iovec *iov, __u64 udata) { */
/*   struct io_uring_sqe *sqe = io_uring_get_sqe(&ring); */
/*   assert(sqe); */
/*   io_uring_prep_writev(sqe, client_socket, iov, 1, 0); */
/*   io_uring_sqe_set_data64(sqe, udata); */
/*   /1* io_uring_submit(&ring); *1/ */
/*   return 0; */
/* } */

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, std::vector<info_type> *infos, int recv_fdall[4][50], int sync_recv_fdall[4][MAX_NR_CONSUMER]) {
  sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(sockaddr_in);
  int num = 0;
  int cnts[4];
  memset(cnts, 0, sizeof(cnts));

  while (num < 3 * (MAX_NR_PRODUCER + MAX_NR_CONSUMER)) {
    int client_fd = accept(listen_fd, (sockaddr *)&client_addr, &client_addr_len);
    if (client_fd < 0) {
      usleep(20);
      continue;
    }

    /* // set nodelay */
    /* int enable = 1; */
    /* int ret; */
    /* ret = setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable)); */
    /* assert(ret != -1); */

    int idx;
    for (int j = 0; j < 4; ++j) {
      sockaddr_in addr;
      inet_pton(AF_INET, (*infos)[j].first.c_str(), &addr.sin_addr);
      if (memcmp(&addr.sin_addr, &client_addr.sin_addr, sizeof(sockaddr_in::sin_addr)) == 0) {
        /* uv_tcp_open(&recv_fdall[j][cnts[j]++], client_fd); */
        if (cnts[j] < MAX_NR_PRODUCER) {
          recv_fdall[j][cnts[j]++] = client_fd;
          DEBUG_PRINTF(INIT, "%s: neighbor index = %d recv_fd[%d] = %d from %s\n",
                       this_host_info, j, cnts[j] - 1, client_fd, (*infos)[j].first.c_str());
        } else {
          idx = cnts[j] - MAX_NR_PRODUCER;
          sync_recv_fdall[j][idx] = client_fd;
          cnts[j]++;
          DEBUG_PRINTF(INIT, "%s: neighbor index = %d sync_recv_fd[%d] = %d from %s\n",
                       this_host_info, j, idx, client_fd, (*infos)[j].first.c_str());
        }
        break;
      }
    }
    num++;
  }
}



/* io_uring_cqe *wait_cqe_fast(struct io_uring *ring) */
/* { */
/*   struct io_uring_cqe *cqe; */
/*   unsigned head; */
/*   int ret; */

/*   io_uring_for_each_cqe(ring, head, cqe) */
/*     return cqe; */

/*   ret = io_uring_wait_cqe(ring, &cqe); */
/*   if (ret) { */
/*     fprintf(stderr, "wait cqe %d\n", ret); */
/*     return nullptr; */
/*   } */
/*   return cqe; */
/* } */

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
    if (ret <= 0) {
      return ret;
    }
    rest -= ret;
    recv_cnt += ret;
  }

  return n;
}
