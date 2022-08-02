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
  }


  ~Engine() {
    delete data;
    delete id_r;
    delete uid_r;
  }


  void open(std::string aep_path, std::string disk_path) {
    std::string data_prefix = aep_path;
    if (data_prefix[data_prefix.size() - 1] != '/')
      data_prefix.push_back('/');
    data->open(data_prefix + ".data");
    
    id_r->open(disk_path, "id");
    uid_r->open(disk_path, "uid");
  }


  void write(const User *user) {
    uint64_t offset = data->data_write(*user);
    id_r->put(user->id, offset);
    uid_r->put(*(UserString*)(user->user_id), offset);
  }


  size_t read(int32_t select_column,
            int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    size_t result = 0;
    switch(where_column) {
      case Id:
        result = id_r->get(*(int64_t*)column_key, data, where_column, select_column, res);
        break;
      case Userid:
        result = uid_r->get(*(UserString *)column_key, data, where_column, select_column, res);
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
  // salary need multi-index
};
