#include <memory.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <thread>
#include <cassert>
#include <mutex>

#include "../inc/interface.h"
#include "../src/include/data.hpp"
#include "../src/include/util.hpp"

const char *success_record_file = "/mnt/disk/success_record";
constexpr int NR_WRITE_THREAD = 50;
constexpr int NR_USER_PER_THREAD = 1000000;
constexpr int NR_MAX_TEST_USER = NR_WRITE_THREAD * NR_USER_PER_THREAD;
constexpr int RECORD_FILE_SIZE = (1 + NR_MAX_TEST_USER) * sizeof(int);

volatile int *cnt, *next;
std::mutex record_file_lock;

bool open_record_file() {
  bool new_create;
  volatile int *map_ptr = static_cast<volatile int *>(map_file(success_record_file, RECORD_FILE_SIZE, &new_create));
  if (map_ptr == nullptr) {
    printf("Fail to mmap %s\n", success_record_file);
    exit(-1);
  }

  cnt = map_ptr;
  next = map_ptr + 1;

  return new_create;
}

void set_user(User *user, int64_t id) {
  memset(user, 0, sizeof(User));
  user->id = id;
  strcpy(user->name, std::to_string(id).c_str());
  strcpy(user->user_id, std::to_string(id).c_str());
  user->salary = id * 20 + 50;
}

void read_user(void *context, int64_t i) {
  User user;
  char uid_buffer[128] = {0};
  
  memset(&user, 0, sizeof(user));
  int ret = engine_read(context, Userid, Id, &i, sizeof(user.id), (void *)user.user_id);
  assert(ret);
  assert(std::to_string(i) == user.user_id);
  
  memset(&user, 0, sizeof(user));
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

void test_write(void *context) {
  printf("Start write stage\n");

  std::thread *write_threads[NR_WRITE_THREAD];
  
  for (int tid = 0; tid < NR_WRITE_THREAD; tid++) {
    write_threads[tid] = new std::thread([context, tid]{
      User user;
      for (int id = tid * NR_USER_PER_THREAD; id < (tid + 1) * NR_USER_PER_THREAD; id++) {
        set_user(&user, id);
        engine_write(context, &user, sizeof(User));
        record_file_lock.lock();
        *next++ = id;
        (*cnt)++;
        record_file_lock.unlock();
        usleep(1);
      }
    });
  }

  for (int tid = 0; tid < NR_WRITE_THREAD; tid++) {
    write_threads[tid]->join();
    delete write_threads[tid];
  }
}

void test_read_after_crash(void *context) {
  open_record_file();
  int success_cnt = *cnt;
  printf("Start read stage, success count = %d\n", success_cnt);
  for (int i = 0; i < success_cnt; i++) {
    read_user(context, next[i]);
  }
}

int main()
{
  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
  if (!context) {
    return -1;
  }

  bool new_create = open_record_file();

  if (new_create) {
    test_write(context);
  } else {
    test_read_after_crash(context);
  }

  engine_deinit(context);
}