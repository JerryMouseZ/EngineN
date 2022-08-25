#include <cstddef>
#include <iostream>
#include "../src/include/log.hpp"
#include "include/data.hpp"

void test_log_large(size_t num) {
  Data *data = new Data();
  data->open("/mnt/aep/user.data", "/mnt/disk/cache", "/mnt/disk/flag");
  CircularFifo *log = new CircularFifo("/mnt/disk/log", data);
  
  for (size_t i = 0; i < num; ++i) {
    User user;
    memset(&user, 0, sizeof(User));
    user.id = i;
    memset(user.name, 0, 128);
    memset(user.user_id, 0, 128);
    strcpy(user.name, std::to_string(num).c_str());
    strcpy(user.user_id, std::to_string(num).c_str());
    user.salary = num / 4;

    int index = log->push(user);
    data->put_flag(index);
    const User *res_user = log->read(index);
    assert(res_user->id == user.id);
    assert(std::string(res_user->name) == user.name);
    assert(std::string(res_user->user_id) == user.user_id);
    assert(res_user->salary == user.salary);
  }
  delete log;
  delete data;
}

void test_log_read(size_t num) {
  Data *data = new Data();
  data->open("/mnt/aep/user.data", "/mnt/disk/cache", "/mnt/disk/flag");
  CircularFifo *log = new CircularFifo("/mnt/disk/log", data);
  
  for (size_t i = 0; i < num; ++i) {
    User user;
    memset(&user, 0, sizeof(User));
    user.id = i;
    memset(user.name, 0, 128);
    memset(user.user_id, 0, 128);
    strcpy(user.name, std::to_string(num).c_str());
    strcpy(user.user_id, std::to_string(num).c_str());
    user.salary = num / 4;

    int index = i + 1;
    const User *res_user = log->read(index);
    assert(res_user->id == user.id);
    assert(std::string(res_user->name) == user.name);
    assert(std::string(res_user->user_id) == user.user_id);
    assert(res_user->salary == user.salary);
  }
  delete log;
  delete data;
}


int main(int argc, char **argv)
{
  if (argc < 3) {
    fprintf(stderr, "usage: %s normal/parallel num\n", argv[0]);
    exit(0);
  }
  int num = atol(argv[2]);
  if (argv[1][0] == 'n') {
    test_log_large(num);
  } else {
    test_log_read(num);
  }
  return 0;
}

