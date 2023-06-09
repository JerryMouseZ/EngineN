#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <ostream>
#include <string>
#include <string.h>
#include <thread>
#include "../inc/interface.h"
#include "../src/include/engine.hpp"
#include <assert.h>
#include <unistd.h>

char ips[4][32] {
    "192.168.1.38:",
    "192.168.1.39:",
    "192.168.1.40:",
    "192.168.1.41:"
};

class TestUser
{
public:
  int64_t id;
  char user_id[128];
  char name[128];
  int64_t salary;
};

void test_engine_write(void *context, int index, size_t num)
{
  assert(num % 50 == 0);
  long per_thread = num / 50;
  std::thread *threads[50];
  for (long tid = 0; tid < 50; ++tid) {
    threads[tid] = new std::thread([=]{
      long data_begin = index * num + tid * per_thread, data_end = index * num + (tid + 1) * per_thread;
      for (long i = data_begin; i < data_end; ++i) {
        TestUser user;
        memset(&user, 0, sizeof(TestUser));
        user.id = i;
        memset(user.name, 0, 128);
        memset(user.user_id, 0, 128);
        strcpy(user.name, std::to_string(i).c_str());
        strcpy(user.user_id, std::to_string(i).c_str());
        user.salary = i % num;
        engine_write(context, &user, sizeof(user));
      }
    });
  }

  for (int tid = 0; tid < 50; tid++) {
    threads[tid]->join();
    delete threads[tid];
  }
}

// both for local or remote
void test_engine_read(void *context, size_t num)
{
  assert(num % 50 == 0);
  int per_thread = num / 50;
  std::thread *threads[50];
  assert(num % 4 == 0);
  int per_node = num / 4;
  long begin = 0, end = num;
  for (int tid = 0; tid < 50; ++tid) {
    threads[tid] = new std::thread([=]{
      long data_begin = tid * per_thread, data_end = (tid + 1) * per_thread;
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
        long salary = i % per_node;
        int64_t ids[4];
        ret = engine_read(context, Id, Salary, &salary, sizeof(salary), ids);
        if (ret < 3) {
          fprintf(stderr, "Line %d %ld %d\n", __LINE__, i, ret);
        }
        assert(ret >= 3);
      }
    });
  }
  for (int tid = 0; tid < 50; tid++) {
    threads[tid]->join();
    delete threads[tid];
  }
}



int main(int argc, char **argv)
{
  if (argc != 5) {
    fprintf(stderr, "usage %s [index] [port] [read/write] [num]\n", argv[0]);
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
  char aep_path[30], disk_path[30];
  sprintf(aep_path, "/mnt/aep/node%d/", index);
  sprintf(disk_path, "/mnt/disk/node%d/", index);
  std::string host_info = std::string(ips[index]) + argv[2];
  void *context = engine_init(host_info.c_str() , const_peer_info, 3, aep_path, disk_path);
  Engine *engine = (Engine *)context;
  int num = atoi(argv[4]);
  if (argv[3][0] == 'w') {
    test_engine_write(context, index, num);
  } else {
    if (index == 3)
      return 0;
    test_engine_read(context, num * 3);
    engine_deinit(context);
  }
  // delete deinit to test crash
}

