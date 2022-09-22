#include "include/comm.h"
#include "liburing.h"
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

void fatal_error(const char *syscall) {
  perror(syscall);
  exit(1);
}

int setup_listening_socket(const char *ip, int port) {
  int sock;
  struct sockaddr_in srv_addr;

  sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    fatal_error("socket() error");
  }

  int enable = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
    fatal_error("setsockopt(SO_REUSEADDR)");

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
  int sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    fatal_error("socket() error");
  }

  // bind
  sockaddr_in client_addr;
  int client_port = 0;
  memset(&client_addr, 0, sizeof(client_addr));
  client_addr.sin_family = AF_INET;
  client_addr.sin_port = htons(client_port);
  if (inet_pton(AF_INET, this_host_ip, &client_addr.sin_addr) <= 0) {
    fatal_error("invalid ip for client");
  }
  if (bind(sock, (const struct sockaddr *)&client_addr, sizeof(client_addr)) < 0)
    fatal_error("bind() for client");

  // set nodelay
  int enable = 1;
  int ret = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable));
  assert(ret != -1);

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
    fatal_error("invalid ip");
  }

  ret = connect(sock, (sockaddr *) &server_addr, sizeof(server_addr));
  while (ret != 0) {
    ret = connect(sock, (sockaddr *) &server_addr, sizeof(server_addr));
    usleep(500);
  }
  return sock;
}


int add_read_request(io_uring &ring, int client_socket, void *buffer, size_t len, __u64 udata) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  assert(sqe);
  io_uring_prep_recv(sqe, client_socket, buffer, len, 0);
  io_uring_sqe_set_data64(sqe, udata);
  assert(io_uring_submit(&ring) == 1);
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
void listener(int listen_fd, int recv_fds[], std::vector<info_type> *infos, int *data_recv_fd, int data_peer_index) {
  sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(sockaddr_in);
  int num = 0, recv_fd_cnt = 0;
  bool peer_recv_fd_set = false;
  while (num < 4) {
    int client_fd = accept(listen_fd, (sockaddr *)&client_addr, &client_addr_len);
    if (client_fd < 0) {
      usleep(50);
      continue;
    }
    for (int j = 0; j < 4; ++j) {
      sockaddr_in addr;
      inet_pton(AF_INET, (*infos)[j].first.c_str(), &addr.sin_addr);
      if (memcmp(&addr.sin_addr, &client_addr.sin_addr, sizeof(sockaddr_in::sin_addr)) == 0) {
        if (j == data_peer_index && peer_recv_fd_set) {
          *data_recv_fd = client_fd;
          DEBUG_PRINTF(0, "data_recv_fd from %s\n", (*infos)[j].first.c_str());
        } else {
          recv_fds[j] = client_fd;
          DEBUG_PRINTF(0, "recv_fd[%d] from %s\n", recv_fd_cnt, (*infos)[j].first.c_str());
          recv_fd_cnt++;
          if (j == data_peer_index) {
            peer_recv_fd_set = true;
          }
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

