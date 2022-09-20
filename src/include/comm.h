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

int Connect(const char *ip, int port);

int add_write_request(io_uring &ring, int client_socket, char *buffer, size_t len);

using info_type = std::pair<std::string, int>;
void listener(int listen_fd, int *recv_fds, std::vector<info_type> *infos, volatile bool *flag, int *data_recv_fd);
#endif
