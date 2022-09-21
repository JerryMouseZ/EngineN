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

#define QUEUE_DEPTH 4096

int setup_listening_socket(const char *ip, int port);

int add_accept_request(io_uring &ring, int server_socket, struct sockaddr_in *client_addr, socklen_t *client_addr_len);

int connect_to_server(const char *ip, int port);

int add_write_request(io_uring &ring, int client_socket, void *buffer, size_t len, __u64 udata);

int add_read_request(io_uring &ring, int client_socket, void *buffer, size_t len, __u64 udata);

static inline io_uring_cqe *wait_cqe_fast(struct io_uring *ring);

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, int *recv_fds, std::vector<info_type> *infos, volatile bool *flag, int *data_recv_fd);

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
};

struct response_header{
  uint32_t fifo_id;
  uint16_t res_len;
  uint32_t ret;
};

#endif
