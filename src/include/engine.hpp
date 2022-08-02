#pragma once

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include "index.hpp"
#include "data.hpp"

class Engine
{
public:
  Engine() {
    data = new Data();
    id_r = new Index<int64_t>();
    uid_r = new Index<UserString>();
    sala_r = new Index<int64_t>();
    name_r = new Index<UserString>();
  }


  ~Engine() {
    delete data;
    delete id_r;
    delete uid_r;
    delete name_r;
    delete sala_r;
  }


  void open(std::string aep_path, std::string disk_path) {
    std::string data_prefix = aep_path;
    if (data_prefix[data_prefix.size() - 1] != '/')
      data_prefix.push_back('/');
    data->open(data_prefix + "user.data");
    
    id_r->Open(disk_path, "id");
    uid_r->Open(disk_path, "uid");
    name_r->Open(disk_path, "name");
    sala_r->Open(disk_path, "salary");
  }


  void write(const User *user) {
    uint64_t offset = data->data_write(*user);
    id_r->put(user->id, offset);
    uid_r->put(*(UserString *)(user->user_id), offset);
    name_r->put(*(UserString *)(user->name), offset);
    sala_r->put(user->salary, offset);
    
    // validate flag
    data->put_flag(offset);
  }


  size_t read(int32_t select_column,
            int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    size_t result = 0;
    switch(where_column) {
      case Id:
        result = id_r->get(column_key, data, where_column, select_column, res, false);
        break;
      case Userid:
        result = uid_r->get(column_key, data, where_column, select_column, res, false);
        break;
      case Name:
        result = name_r->get(column_key, data, where_column, select_column, res, false);
        break;
      case Salary:
        result = sala_r->get(column_key, data, where_column, select_column, res, true);
        break;
      default:
        fprintf(stderr, "unimplemented\n");
        assert(0);
    }
    return result;
  }


private:
  Data *data;
  Index<int64_t> *id_r;
  Index<UserString> *uid_r;
  Index<UserString> *name_r;
  // salary need multi-index
  Index<int64_t> *sala_r;
};
