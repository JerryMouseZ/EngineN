#include <cstdio>
#include <cstdlib>
#include <iostream>
#include "../inc/interface.h"

char *infos[] {
    "192.168.1.39:9091",
    "192.168.1.38:9091",
    "192.168.1.40:9091",
    "192.168.1.41:9091"
};

int main(int argc, char **argv)
{
  if (argc != 2) {
    fprintf(stderr, "usage %s num_node\n", argv[0]);
    exit(1);
  }

  int index = atoi(argv[1]);
  char *peer_info[3] = {nullptr};
  int base = 0;
  for (int i = 0; i < 4; ++i) {
    if (i != index) {
      peer_info[base++] = infos[i];
    }
  }
  char aep_path[16], disk_path[16];
  sprintf(aep_path, "/mnt/aep/node%d", index);
  sprintf(disk_path, "/mnt/disk/node%d", index);
  void *context = engine_init(infos[index], peer_info, 3, aep_path, disk_path);
  engine_deinit(context);
}

