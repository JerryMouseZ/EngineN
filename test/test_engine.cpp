#include <cassert>
#include <cstddef>
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

void test_engine_write(size_t num)
{
  void *context = engine_init(nullptr, nullptr, 0, "/tmp/aep/", "/tmp/disk/");
/* #pragma omp parallel for num_threads(50) */
  for (int i = 0; i < num; ++i) {
    TestUser user;
    memset(&user, 0, sizeof(TestUser));
    user.id = i;
    memset(user.name, 0, 128);
    memset(user.user_id, 0, 128);
    strcpy(user.name, std::to_string(i).c_str());
    strcpy(user.user_id, std::to_string(i).c_str());
    user.salary = i / 2000;
    engine_write(context, &user, sizeof(user));
  }

  engine_deinit(context);
}


void test_engine_read(size_t num)
{
  void *context = engine_init(nullptr, nullptr, 0, "/tmp/aep/", "/tmp/disk/");
#pragma omp parallel for num_threads(50)
  for (long i = 0; i < num; ++i) {
    TestUser user;
    memset(&user, 0, sizeof(user));
    int ret = engine_read(context, Userid, Id, &i, sizeof(user.id), (void *)user.user_id);
    if (ret == 0)
      fprintf(stderr, "Line %d  %ld\n", __LINE__, i);
    assert(ret);
    assert(std::to_string(i) == user.user_id);

    memset(&user, 0, sizeof(user));
    char uid_buffer[128] = {0};
    std::string i2string = std::to_string(i);
    strncpy(uid_buffer, i2string.c_str(), i2string.size());
    ret = engine_read(context, Id, Userid, uid_buffer, 128, &user.id);
    if (ret == 0)
      fprintf(stderr, "Line %d  %ld\n", __LINE__, i);
    assert(ret);
    assert(i == user.id);

    memset(&user, 0, sizeof(user));
    long salary = i / 2000;
    int64_t ids[2000];
    ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids);
    if (ret != 2000) {
        fprintf(stderr, "Line %d %ld\n", __LINE__, i);
    }
    assert(ret == 2000);
  }
  engine_deinit(context);
}

void test_engine_read_salary(int i)
{
  void *context = engine_init(nullptr, nullptr, 0, "/tmp/aep/", "/tmp/disk/");
  long salary = i / 2000;
  int64_t ids[2000] = {0};
  int ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids);
  fprintf(stderr, "%d\n", ret);
  engine_deinit(context);
}


int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s read/write num\n", argv[0]);
        exit(0);
    }

    int num = atol(argv[2]);
    if (argv[1][0] == 'r') {
        test_engine_read(num);
    } else {
        test_engine_write(num);
    }
    return 0;
}

