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
#include <unordered_map>
#include <string>
#include <vector>
#include <uv.h>

extern const char *this_host_info;
#define QUEUE_DEPTH 256

int send_all(int fd, const void *src, size_t n, int flag);

int recv_all(int fd, void *dst, size_t n, int flag);

int setup_listening_socket(const char *ip, int port);

int add_accept_request(io_uring &ring, int server_socket, struct sockaddr_in *client_addr, socklen_t *client_addr_len);

int connect_to_server(const char *this_host_ip, const char *ip, int port);

int add_write_request(io_uring &ring, int client_socket, iovec *iov, __u64 udata);

/* int add_read_request(io_uring &ring, int client_socket, void *buffer, size_t len, __u64 udata); */

int add_read_request(io_uring &ring, int client_socket, iovec *iov, __u64 udata);

io_uring_cqe *wait_cqe_fast(struct io_uring *ring);

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, std::vector<info_type> *infos, uv_tcp_t **recv_fdall);

struct data_request{
  uint32_t fifo_id;
  uint8_t select_column;
  uint8_t where_column;
  char key[128];
};

// for fifo and sqe
struct send_entry{
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  void *res;
  bool has_come;
  int ret;
  int socket;
};

struct response_header{
  uint32_t fifo_id;
  uint16_t res_len;
  uint32_t ret;
};

struct response_buffer{
  response_header header;
  char body[1024];
};

extern const char *this_host_info;
extern char this_host_ip[60];
extern int my_index;
extern int data_peer_index;
extern int req_send_index;
extern int req_recv_index;
extern int req_weak_send_index;
extern int req_weak_recv_index;
#endif
