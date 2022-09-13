#include <iostream>
#include "../inc/interface.h"

int main()
{
  char *peer_info[] {
    "192.168.1.39:9000",
    "192.168.1.38:9000",
    "192.168.1.40:9000"
  };
  void *context = engine_init("192.168.1.41:9000", peer_info, 3, "/mnt/aep/", "/mnt/disk/");
  engine_deinit(context);
}

