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

void test_engine_write(size_t num)
{
  auto rng = std::default_random_engine {};
  std::vector<long> iters(num);
  for (int i = 0; i < num; ++i)
    iters[i] = i;
  /* std::shuffle(iters.begin(), iters.end(), rng); */


  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
  assert(num % 50 == 0);
  long per_thread = num / 50;
  std::thread *threads[50];
  for (long tid = 0; tid < 50; ++tid) {
    /* threads[tid] = new std::thread([=]{ */
      long data_begin = tid * per_thread, data_end = (tid + 1) * per_thread;
      for (long i = data_begin; i < data_end; ++i) {
        TestUser user;
        memset(&user, 0, sizeof(TestUser));
        user.id = iters[i];
        memset(user.name, 0, 128);
        memset(user.user_id, 0, 128);
        strcpy(user.name, std::to_string(iters[i]).c_str());
        strcpy(user.user_id, std::to_string(iters[i]).c_str());
        user.salary = iters[i] / 4;
        engine_write(context, &user, sizeof(user));
      }
    /* }); */
  }

  for (int tid = 0; tid < 50; tid++) {
    /* threads[tid]->join(); */
    /* delete threads[tid]; */
  }
  engine_deinit(context);
}


void test_engine_read(size_t num)
{
  auto rng = std::default_random_engine {};
  std::vector<long> iters(num);
  for (int i = 0; i < num; ++i)
    iters[i] = i;
  /* std::shuffle(iters.begin(), iters.end(), rng); */


  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
  assert(num % 50 == 0);
  int per_thread = num / 50;
  std::thread *threads[50];
  for (int tid = 0; tid < 50; ++tid) {
    /* threads[tid] = new std::thread([=]{ */
      for (long i = tid * per_thread; i < (tid + 1) * per_thread; ++i) {
        TestUser user;
        memset(&user, 0, sizeof(user));
        int ret = engine_read(context, Userid, Id, &iters[i], sizeof(user.id), (void *)user.user_id);
        if (ret == 0)
          fprintf(stderr, "Line %d  %ld\n", __LINE__, iters[i]);
        assert(ret);
        if(std::to_string(iters[i]) != user.user_id)
          fprintf(stderr, "Line %d  %ld\n", __LINE__, iters[i]);
        assert(std::to_string(iters[i]) == user.user_id);

        memset(&user, 0, sizeof(user));
        char uid_buffer[128] = {0};
        std::string i2string = std::to_string(iters[i]);
        strncpy(uid_buffer, i2string.c_str(), i2string.size());
        ret = engine_read(context, Id, Userid, uid_buffer, 128, &user.id);
        if (ret == 0)
          fprintf(stderr, "Line %d  %ld\n", __LINE__, iters[i]);
        assert(ret);
        if(iters[i] != user.id) {
          fprintf(stderr, "Line %d  %ld\n", __LINE__, iters[i]);
        }
        assert(iters[i] == user.id);

        memset(&user, 0, sizeof(user));
        long salary = iters[i] / 4;
        int64_t ids[4];
        ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids);
        if (ret != 4) {
          fprintf(stderr, "Line %d %ld %d\n", __LINE__, iters[i], ret);
        }
        assert(ret == 4);
      }
      /* }); */
  }
  for (int tid = 0; tid < 50; tid++) {
    /* threads[tid]->join(); */
    /* delete threads[tid]; */
  }
  engine_deinit(context);
}

/* void test_read_write(int num) */
/* { */
/*   void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/"); */

/*   for (int j = 0; j < 10; ++j) { */
/*     //write */
/*     for (long i = j * num / 10; i < (j + 1) * num / 10; ++i) { */
/*       TestUser user; */
/*       memset(&user, 0, sizeof(TestUser)); */
/*       user.id = i; */
/*       memset(user.name, 0, 128); */
/*       memset(user.user_id, 0, 128); */
/*       strcpy(user.name, std::to_string(i).c_str()); */
/*       strcpy(user.user_id, std::to_string(i).c_str()); */
/*       user.salary = i / 4; */
/*       engine_write(context, &user, sizeof(user)); */
/*     } */
/*     // read */
/*     for (long i = j * num / 10; i < (j + 1) * num / 10; ++i) { */
/*       TestUser user; */
/*       memset(&user, 0, sizeof(user)); */
/*       int ret = engine_read(context, Userid, Id, &i, sizeof(user.id), (void *)user.user_id); */
/*       if (ret == 0) */
/*         fprintf(stderr, "Line %d  %ld\n", __LINE__, i); */
/*       assert(ret); */
/*       assert(std::to_string(i) == user.user_id); */

/*       memset(&user, 0, sizeof(user)); */
/*       char uid_buffer[128] = {0}; */
/*       std::string i2string = std::to_string(i); */
/*       strncpy(uid_buffer, i2string.c_str(), i2string.size()); */
/*       ret = engine_read(context, Id, Userid, uid_buffer, 128, &user.id); */
/*       if (ret == 0) */
/*         fprintf(stderr, "Line %d  %ld\n", __LINE__, i); */
/*       assert(ret); */
/*       assert(i == user.id); */

/*       memset(&user, 0, sizeof(user)); */
/*       long salary = i / 4; */
/*       int64_t ids[4]; */
/*       ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids); */
/*       if (ret != 4) { */
/*         fprintf(stderr, "Line %d %ld\n", __LINE__, i); */
/*       } */
/*       assert(ret == 4); */
/*     } */
/*   } */
/*   engine_deinit(context); */
/* } */

/* void test_engine_read_salary(int i) */
/* { */
/*   void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/"); */
/*   long salary = i / 4; */
/*   int64_t ids[4] = {0}; */
/*   int ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids); */
/*   fprintf(stderr, "%d\n", ret); */
/*   engine_deinit(context); */
/* } */


int main(int argc, char **argv)
{
  if (argc < 3) {
    fprintf(stderr, "usage: %s read/write num\n", argv[0]);
    exit(0);
  }

  int num = atol(argv[2]);
  if (argv[1][0] == 'r') {
    test_engine_read(num);
  } else if (argv[1][0] == 'w'){
    test_engine_write(num);
  } else {
    /* test_read_write(num); */
  }
  return 0;
}

