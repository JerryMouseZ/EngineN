#include <cassert>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string.h>
#include <string>
#include "../inc/interface.h"
#include "../src/include/data.hpp"

static void print_user(const User &user, int64_t i) {
  printf("User %lu:\n", i);
  printf("  Id: %lu\n", user.id);
  printf("  Uid: %s\n", user.user_id);
  printf("  Name: %s\n", user.name);
  printf("  Salary: %lu\n", user.salary);
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
  
  const auto i_str = std::to_string(i);
  memset(&user, 0, sizeof(user));
  int ret = engine_read(context, Userid, Id, &i, sizeof(user.id), (void *)user.user_id);
  assert(ret);
  if (i_str != user.user_id) {
    print_user(user, i);
    assert(i_str == user.user_id);
  }
  
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

void test_engine_write()
{
  void *context = engine_init(nullptr, nullptr, 0, "/mnt/aep/", "/mnt/disk/");
  if (!context) {
    return;
  }
  for (int i = 0; i < 50000; ++i) {
    User user;
    for (int j = 0; j < 10; j++) {
      set_user(&user, i * 10 + j);
      engine_write(context, &user, sizeof(user));
    }
    read_user(context, (i * 10) % 79);
    read_user(context, i * 10 + 5);
  }

  engine_deinit(context);
}

int main()
{
  test_engine_write();
  return 0;
}