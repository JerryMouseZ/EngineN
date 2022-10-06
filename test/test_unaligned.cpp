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
#include "../inc/interface.h"

class TestUser
{
public:
  int64_t id;
  char user_id[128];
  char name[128];
  int64_t salary;
};

enum Column{Id=0, Userid, Name, Salary};

void test_engine_write(int index, size_t num, int iter)
{
  char aep_path[16];
  char disk_path[16];
  sprintf(aep_path, "/mnt/aep/node%d", index);
  sprintf(disk_path, "/mnt/disk/node%d", index);
  void *context = engine_init(nullptr, nullptr, 0, aep_path, disk_path);
  long data_begin = iter * num, data_end = (iter + 1) * (num + 1);
  for (long i = data_begin; i < data_end; ++i) {
    TestUser user;
    memset(&user, 0, sizeof(TestUser));
    user.id = i;
    memset(user.name, 0, 128);
    memset(user.user_id, 0, 128);
    strcpy(user.name, std::to_string(i).c_str());
    strcpy(user.user_id, std::to_string(i).c_str());
    user.salary = i;
    engine_write(context, &user, sizeof(user));
  }
}

void print_user(const TestUser &user) {
  printf("User:\n");
  printf("  Id: %lu\n", user.id);
  printf("  Uid: %s\n", user.user_id);
  printf("  Name: %s\n", user.name);
  printf("  Salary: %lu\n", user.salary);
}


void test_engine_read(int index, size_t num)
{
  char aep_path[16];
  char disk_path[16];
  sprintf(aep_path, "/mnt/aep/node%d", index);
  sprintf(disk_path, "/mnt/disk/node%d", index);
  void *context = engine_init(nullptr, nullptr, 0, aep_path, disk_path);
  long data_begin = 0, data_end = num;

  fprintf(stderr, "from %ld to %ld\n", data_begin, data_end);
  for (long i = data_begin; i < data_end; ++i) {
    TestUser user;

    // Select Uid from ... where Id
    memset(&user, 0, sizeof(user));
    int ret = engine_read(context, Userid, Id, &i, sizeof(user.id), (void *)user.user_id);
    if (ret == 0)
      fprintf(stderr, "Line %d  %ld\n", __LINE__, i);
    assert(ret);
    if(std::to_string(i) != user.user_id) {
      fprintf(stderr, "Line %d  %ld\n", __LINE__, i);
      print_user(user);
    }
    assert(std::to_string(i) == user.user_id);

    // Select Id from ... where Uid
    memset(&user, 0, sizeof(user));
    char uid_buffer[128] = {0};
    std::string i2string = std::to_string(i);
    strncpy(uid_buffer, i2string.c_str(), i2string.size());
    ret = engine_read(context, Id, Userid, uid_buffer, 128, &user.id);
    if (ret == 0)
      fprintf(stderr, "Line %d  %ld\n", __LINE__, i);
    assert(ret);
    if(i != user.id) {
      fprintf(stderr, "Line %d  %ld\n", __LINE__, i);
    }
    assert(i == user.id);

    // Select Id from ... where Salary
    memset(&user, 0, sizeof(user));
    long salary = i;
    int64_t ids[4];
    ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids);
    if (ret != 1) {
      fprintf(stderr, "Line %d %ld %d\n", __LINE__, i, ret);
    }
  }
  engine_deinit(context);
}


int main(int argc, char **argv)
{
  if (argc < 5) {
    fprintf(stderr, "usage: %s [index] [read/write] [iter] [num]\n", argv[0]);
    exit(0);
  }

  int num = atol(argv[4]);
  int iter = atol(argv[3]);
  if (argv[2][0] == 'r') {
    test_engine_read(atoi(argv[1]), num * iter); // 现在local应该有两个人的数量
  } else if (argv[2][0] == 'w'){
    test_engine_write(atoi(argv[1]), num, iter);
  }  
  return 0;
}

