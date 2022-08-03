#include <cassert>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string.h>
#include <string>
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

void test_overflow_write()
{
  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
#pragma omp parallel for
  for (int i = 0; i < 500000; ++i) {
    TestUser user;
    memset(&user, 0, sizeof(TestUser));
    user.id = i;
    memset(user.name, 0, 128);
    memset(user.user_id, 0, 128);
    strcpy(user.name, std::to_string(i).c_str());
    strcpy(user.user_id, std::to_string(i).c_str());
    user.salary = i * 20 + 50;
    engine_write(context, &user, sizeof(user));
  }

  engine_deinit(context);
}


void test_overflow_read()
{
  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
#pragma omp parallel for
  for (long i = 0; i < 500000; ++i) {
    printf("%ld\n", i);
    TestUser user;
    memset(&user, 0, sizeof(user));
    int ret = engine_read(context, Userid, Id, &i, sizeof(user.id), (void *)user.user_id);
    assert(ret);
    assert(std::to_string(i) == user.user_id);

    memset(&user, 0, sizeof(user));
    char uid_buffer[128] = {0};
    std::string i2string = std::to_string(i);
    strncpy(uid_buffer, i2string.c_str(), i2string.size());
    ret = engine_read(context, Id, Userid, uid_buffer, 128, &user.id);
    assert(ret);
    assert(i == user.id);

    memset(&user, 0, sizeof(user));
    long salary = i * 20 + 50;
    ret = engine_read(context, Id, Salary, &salary, sizeof(salary), &user.id);
    assert(ret);
    assert(i == user.id);
  }
  engine_deinit(context);
}


int main(int argc, char **argv)
{
  if (argc < 2) {
    fprintf(stderr, "usage: %s read/write\n", argv[0]);
    exit(0);
  }
  
  if (argv[1][0] == 'r') {
    test_overflow_read();
  } else {
    test_overflow_write();
  }
  return 0;
}

