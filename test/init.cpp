#include <iostream>
#include "../inc/interface.h"

int main()
{
  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
  engine_deinit(context);
}

