#include "include/communicator.h"

void fatal_error(const char *syscall) {
  perror(syscall);
  exit(1);
}

static int setup_listening_socket(const char *ip, int port) {
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
  if (bind(sock,
           (const struct sockaddr *)&srv_addr,
           sizeof(srv_addr)) < 0)
    fatal_error("bind()");

  if (listen(sock, 20) < 0)
    fatal_error("listen()");

  return sock;
}


int add_accept_request(io_uring &ring, int server_socket, struct sockaddr_in *client_addr, socklen_t *client_addr_len) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  io_uring_prep_accept(sqe, server_socket, (struct sockaddr *) client_addr,
                       client_addr_len, 0);
  /* request *req = reinterpret_cast<request *>(malloc(sizeof(*req))); */
  /* req->event_type = EVENT_TYPE_ACCEPT; */
  /* io_uring_sqe_set_data(sqe, req); */
  io_uring_submit(&ring);
  return 0;
}


int add_read_request(io_uring &ring, int client_socket, char *buffer, size_t len) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  /* Linux kernel 5.5 has support for readv, but not for recv() or read() */
  io_uring_prep_recv(sqe, client_socket, buffer, len, 0);
  /* io_uring_sqe_set_data(sqe, req); */
  io_uring_submit(&ring);
  return 0;
}


int add_write_request(io_uring &ring, int client_socket, char *buffer, size_t len) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  io_uring_prep_send(sqe, client_socket, buffer, len, 0);
  /* io_uring_sqe_set_data(sqe, req); */
  io_uring_submit(&ring);
  return 0;
}
