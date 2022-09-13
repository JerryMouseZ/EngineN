#ifndef COMM_H
#define COMM_H
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <netinet/in.h>
#include <liburing.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#define EVENT_TYPE_ACCEPT       0
#define EVENT_TYPE_READ         1
#define EVENT_TYPE_WRITE        2

class Communicator{
public:
  struct request {
    int event_type;
    int client_socket;
    struct iovec iov;
  };

#define QUEUE_DEPTH 256
  Communicator(char *ip, int port, char **peers) {
    io_uring_queue_init(QUEUE_DEPTH, &ring, 0);
    listen_fd = setup_listening_socket(ip, port);
    sleep(1);

    // connect to others
  }

private:
  static void fatal_error(const char *syscall) {
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

    if (listen(sock, 10) < 0)
      fatal_error("listen()");

    return sock;
  }

  static int connect_to_remote(const char *ip, int port) {
    int sock = 0;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
      printf("\n Socket creation error \n");
      return -1;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary
    // form
    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
      printf("\nInvalid address/ Address not supported \n");
      return -1;
    }

    int client_fd;
    if ((client_fd = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr))) < 0) {
      printf("\nConnection Failed \n");
      return -1;
    }

    return client_fd;
  }

  int add_accept_request(int server_socket, struct sockaddr_in *client_addr, socklen_t *client_addr_len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    io_uring_prep_accept(sqe, server_socket, (struct sockaddr *) client_addr,
                         client_addr_len, 0);
    request *req = reinterpret_cast<request *>(malloc(sizeof(*req)));
    req->event_type = EVENT_TYPE_ACCEPT;
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&ring);
    return 0;
  }


  int add_read_request(int client_socket, char *buffer, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    struct request *req = reinterpret_cast<request *>(malloc(sizeof(request)));
    req->iov.iov_base = buffer;
    req->iov.iov_len = len;
    req->event_type = EVENT_TYPE_READ;
    req->client_socket = client_socket;
    /* Linux kernel 5.5 has support for readv, but not for recv() or read() */
    io_uring_prep_readv(sqe, client_socket, &req->iov, 1, 0);
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&ring);
    return 0;
  }


  int add_write_request(int client_socket, char *buffer, size_t len) {
    struct request *req = reinterpret_cast<request *>(malloc(sizeof(request)));
    req->client_socket = client_socket;
    req->iov.iov_base = buffer;
    req->iov.iov_len = len;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    req->event_type = EVENT_TYPE_WRITE;
    io_uring_prep_writev(sqe, req->client_socket, &req->iov, 1, 0);
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&ring);
    return 0;
  }


  io_uring ring;
  int listen_fd;
  int self_index;
};

#endif
