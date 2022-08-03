#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include "include/data.hpp"

using namespace std;

void test_data_write()
{
  Data *data = new Data();
  data->open("/mnt/aep/test.data");
  vector<uint64_t> offsets;
  for (int i = 0; i < 100; ++i) {
    User user;
    user.id = i;
    memset(user.name, 0, 128);
    strncpy(user.name, std::to_string(i).c_str(), 128);
    user.salary = i * 20 + 50;
    uint64_t offset = data->data_write(user);
    offsets.emplace_back(offset);
  }
  delete data;
}

void test_data_read()
{
  Data *data = new Data();
  data->open("/mnt/aep/test.data");
  for (int i = 0; i < 100; ++i) {
    const User *res = data->data_read(8 + i * (sizeof(User) + 4));
    assert(res->id == i);
    assert(std::to_string(i) == res->name);
    assert(res->salary == i * 20 + 50);
  }
  delete data;
}

int main(int argc, char **argv)
{
  if (argc < 2) {
    fprintf(stderr, "usage %s read/write\n", argv[0]);
    exit(0);
  }

  if (argv[1][0] == 'r') {
    test_data_read();
  } else {
    test_data_write();
  }
}

