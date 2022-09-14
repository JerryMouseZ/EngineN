#include "include/comm.h"
#include "liburing.h"
#include <cstring>
#include <ctime>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string>
#include <unistd.h>
#include <vector>

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
  if (setsockopt(sock,
                 SOL_SOCKET, SO_REUSEADDR,
                 &enable, sizeof(int)) < 0)
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


int Connect(const char *ip, int port) {
  int sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    fatal_error("socket() error");
  }

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
    fatal_error("invalid ip");
  }

  int ret = connect(sock, (sockaddr *) &server_addr, sizeof(server_addr));
  while (ret != 0) {
    ret = connect(sock, (sockaddr *) &server_addr, sizeof(server_addr));
  }
  return sock;
}


int add_read_request(io_uring &ring, int client_socket, char *buffer, size_t len) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  io_uring_prep_recv(sqe, client_socket, buffer, len, 0);
  io_uring_submit(&ring);
  return 0;
}


int add_write_request(io_uring &ring, int client_socket, char *buffer, size_t len) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  io_uring_prep_send(sqe, client_socket, buffer, len, 0);
  io_uring_submit(&ring);
  return 0;
}

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, int recv_fds[], std::vector<info_type> *infos, volatile bool *flag, int *data_recv_fd) {
  sockaddr_in client_addr;
  socklen_t client_addr_len;
  int num = 0;
  while (num < 3) {
    int client_fd = accept(listen_fd, (sockaddr *)&client_addr, &client_addr_len);
    if (client_fd < 0) {
      usleep(50);
      continue;
    }
    for (int j = 0; j < 4; ++j) {
      sockaddr_in addr;
      inet_pton(AF_INET, (*infos)[j].first.c_str(), &addr.sin_addr);
      if (memcmp(&addr, &client_addr, sizeof(sockaddr_in)) == 0) {
        recv_fds[j] = client_fd;
      }
    }
    num++;
  }
  
  while (*flag == false) {
    usleep(50);
  }

  while (1) {
    *data_recv_fd = accept(listen_fd, (sockaddr *)&client_addr, &client_addr_len);
    if (*data_recv_fd < 0) {
      usleep(50);
      continue;
    }
    break;
  }
}
