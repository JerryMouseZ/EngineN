#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <ostream>
#include <string>
#include <string.h>
#include "../inc/interface.h"

char ips[4][32] {
    "192.168.1.38:",
    "192.168.1.39:",
    "192.168.1.40:",
    "192.168.1.41:"
};

int main(int argc, char **argv)
{
  if (argc != 3) {
    fprintf(stderr, "usage %s num_node port\n", argv[0]);
    exit(1);
  }

  int index = atoi(argv[1]);
  int port = atoi(argv[2]);
  char peer_info[3][32] = {};
  char *const_peer_info[3] = {nullptr};
  int base = 0;
  for (int i = 0; i < 4; ++i) {
    if (i != index) {
      std::string info = std::string(ips[i]) + argv[2];
      strcpy(peer_info[base], info.c_str());
      const_peer_info[base] = peer_info[base];
      base++;
    }
  }
  char aep_path[16], disk_path[16];
  sprintf(aep_path, "/mnt/aep/node%d", index);
  sprintf(disk_path, "/mnt/disk/node%d", index);
  std::string host_info = std::string(ips[index]) + argv[2];
  void *context = engine_init(host_info.c_str() , const_peer_info, 3, aep_path, disk_path);
  engine_deinit(context);
}

