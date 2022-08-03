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
  Index<int64_t, int64_t, SALARY_BUCKET_NUM, SALARY_BUCKET_SIZE> salary;
  int ret = salary.open("/mnt/aep/test", "sala");
  assert(ret == 0);

  for (int i = 0; i < 50; ++i) {
    User user;
    user.id = i;
    strcpy(user.name, std::to_string(i).c_str());
    strcpy(user.user_id, std::to_string(i).c_str());
    user.salary = 20;
    salary.write(user.salary, user.id);
  }
}

void test_read()
{
  Index<int64_t, int64_t, SALARY_BUCKET_NUM, SALARY_BUCKET_SIZE> salary;
  int ret = salary.open("/mnt/aep/test", "sala");
  assert(ret == 0);

  std::vector<int64_t> ids;
  ret = salary.read(20, ids, true);
  assert(ret);

  for (int i = 0; i < 50; ++i) {
    assert(ids[i] == i);
  }
}

int main()
{ 
  test_write();
  test_read();
  return 0;
}

