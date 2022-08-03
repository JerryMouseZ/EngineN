#include "include/index.hpp"
#include <cstdint>
#include <assert.h>
#include <iostream>
#include <string>
#include <vector>

class User
{
public:
  int64_t id = 0;
  char user_id[128] = {};
  char name[128] = {};
  int64_t salary = 0;
};

class UserString {
public:
  char ptr[128];
  bool operator==(const UserString & other) {
    return std::string(ptr) == std::string(other.ptr);
  }
};

bool operator==(const UserString &l, const UserString &r)
{
  return std::string(l.ptr) == std::string(r.ptr);
}

template <>
struct std::hash<UserString>
{
  std::size_t operator()(const UserString& k) const
  {
    return (hash<string>()(k.ptr));
  }
};

void test_write()
{
  // test id
  Index<int64_t, User, ID_BUCKET_NUM, ID_BUCKET_SIZE> id;
  int ret = id.open("/mnt/aep/test", "id");
  assert(ret == 0);

  Index<UserString, int64_t, UID_BUCKET_NUM, UID_BUCKET_SIZE> uid;
  ret = uid.open("/mnt/aep/test", "uid");
  assert(ret == 0);

  for (int i = 0; i < 5000; ++i) {
    User user;
    user.id = i;
    strcpy(user.name, std::to_string(i).c_str());
    strcpy(user.user_id, std::to_string(i).c_str());
    user.salary = i * 20 + 50;

    id.write(user.id, user);
    uid.write(*reinterpret_cast<UserString*>(user.user_id), user.id);
  }
}

void test_read()
{
  Index<int64_t, User, ID_BUCKET_NUM, ID_BUCKET_SIZE> id;
  int ret = id.open("/mnt/aep/test", "id");
  assert(ret == 0);

  Index<UserString, int64_t, UID_BUCKET_NUM, UID_BUCKET_SIZE> uid;
  ret = uid.open("/mnt/aep/test", "uid");
  assert(ret == 0);

#pragma omp parallel for
  for (int i = 0; i < 5000; ++i) {
    std::vector<User> users;
    std::vector<int64_t> ids;
    int ret = id.read(i, users, false);
    assert(ret);
    assert(users[0].id == i);

    ret = uid.read(*reinterpret_cast<UserString*>(users[0].user_id), ids, true);
    assert(ids[0] == i);
  }
}

int main()
{ 
  test_read();
  return 0;
}

