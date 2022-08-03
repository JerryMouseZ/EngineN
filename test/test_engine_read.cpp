#include <iostream>
#include "../inc/interface.h"

enum Column{Id=0, Userid, Name, Salary};
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
    int i = atoi(argv[1]);
    test_engine_read_salary(i);
    return 0;
}

