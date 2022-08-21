#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string.h>
#include <string>
#include <thread>
#include <algorithm>
#include <vector>
#include <random>
#include "../src/include/data.hpp"
#include <thread>
class TestUser
{
public:
  int64_t id;
  char user_id[128];
  char name[128];
  int64_t salary;
};

void test_data(size_t num)
{
  Data data;
  data.open("/mnt/aep/test.data", "/mnt/disk/test.cache", "/mnt/disk/test.flag");
  User user;
  memset(&user, 0, sizeof(TestUser));
  user.id = num;
  memset(user.name, 0, 128);
  memset(user.user_id, 0, 128);
  strcpy(user.name, std::to_string(num).c_str());
  strcpy(user.user_id, std::to_string(num).c_str());
  user.salary = num / 4;

  int index = data.data_write(user);
  data.put_flag(index);
  const User *res_user = data.data_read(index);
  assert(res_user->id == user.id);
  assert(std::string(res_user->name) == user.name);
  assert(std::string(res_user->user_id) == user.user_id);
  assert(res_user->salary == user.salary);
}

void test_data_large_threads(size_t num)
{
  Data *data = new Data();
  data->open("/mnt/aep/test.data", "/mnt/disk/test.cache", "/mnt/disk/test.flag");
  assert(num % 50 == 0);
  long per_thread = num / 50;
  std::thread *threads[50];
  for (long tid = 0; tid < 50; ++tid) {
      threads[tid] = new std::thread([=]{
      long data_begin = tid * per_thread, data_end = (tid + 1) * per_thread;
      for (long i = data_begin; i < data_end; ++i) {
        User user;
        memset(&user, 0, sizeof(TestUser));
        user.id = i;
        memset(user.name, 0, 128);
        memset(user.user_id, 0, 128);
        strcpy(user.name, std::to_string(num).c_str());
        strcpy(user.user_id, std::to_string(num).c_str());
        user.salary = num / 4;

        int index = data->data_write(user);
        data->put_flag(index);
        const User *res_user = data->data_read(index);
        assert(res_user->id == user.id);
        assert(std::string(res_user->name) == user.name);
        assert(std::string(res_user->user_id) == user.user_id);
        assert(res_user->salary == user.salary);
      }
    });
  }
  for (int tid = 0; tid < 50; tid++) {
    threads[tid]->join();
    delete threads[tid];
  }
  free(data);
}

void test_data_large(size_t num)
{
  Data *data = new Data();
  data->open("/mnt/aep/test.data", "/mnt/disk/test.cache", "/mnt/disk/test.flag");
  for (long i = 0; i < num; ++i) {
    User user;
    memset(&user, 0, sizeof(TestUser));
    user.id = i;
    memset(user.name, 0, 128);
    memset(user.user_id, 0, 128);
    strcpy(user.name, std::to_string(num).c_str());
    strcpy(user.user_id, std::to_string(num).c_str());
    user.salary = num / 4;

    int index = data->data_write(user);
    data->put_flag(index);
    const User *res_user = data->data_read(index);
    assert(res_user->id == user.id);
    assert(std::string(res_user->name) == user.name);
    assert(std::string(res_user->user_id) == user.user_id);
    assert(res_user->salary == user.salary);
  }
  free(data);
}




int main(int argc, char **argv)
{
  if (argc < 3) {
    fprintf(stderr, "usage: %s read/write num\n", argv[0]);
    exit(0);
  }

  int num = atol(argv[2]);
  if (argv[1][0] == 's') {
    test_data(num);
  } else if (argv[1][0] == 'l'){
    test_data_large(num);
  } else {
    test_data_large_threads(num);
  }
  return 0;
}

