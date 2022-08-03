#pragma once
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
    flags = new DataFlag();
  }


  ~Engine() {
    fprintf(stderr, "exiting\n");
    delete flags;
    delete data;
    delete id_r;
    delete uid_r;
    delete sala_r;
    delete name_r;
  }


  void open(std::string aep_path, std::string disk_path) {
    std::string data_prefix = aep_path;
    if (data_prefix[data_prefix.size() - 1] != '/')
      data_prefix.push_back('/');
    data->open(data_prefix + "user.data");
    flags->Open(data_prefix + "user.flag");

    id_r->Open(disk_path, "id");
    uid_r->Open(disk_path, "uid");
    name_r->Open(disk_path, "name");
    sala_r->Open(disk_path, "salary");
  }


  void write(const User *user) {
    DEBUG_PRINTF(LOG, "write %ld %ld %ld %ld\n", user->id, std::hash<std::string>()(std::string(user->name, 128)), std::hash<std::string>()(std::string(user->user_id, 128)), user->salary);
    uint64_t offset = data->data_write(*user);
    id_r->put(user->id, offset);
    uid_r->put(*(UserString *)(user->user_id), offset);
    name_r->put(*(UserString *)(user->name), offset);
    sala_r->put(user->salary, offset);

    // validate flag
    flags->set_flag(offset);
  }

  std::string column_str(int column)
  {
    switch(column) {
    case Id:
      return "ID";
      break;
    case Userid:
      return "UID";
      break;
    case Name:
      return "Name";
      break;
    case Salary:
      return "Salary";
      break;
    default:
      DEBUG_PRINTF(LOG, "column error");
    }
    return "";
  }

  size_t read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    size_t result = 0;
    switch(where_column) {
    case Id:
      result = id_r->get(column_key, data, flags, where_column, select_column, res, false);
      DEBUG_PRINTF(LOG, "select %s where ID = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
      break;
    case Userid:
      result = uid_r->get(column_key, data, flags, where_column, select_column, res, false);
      DEBUG_PRINTF(LOG, "select %s where UID = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
      break;
    case Name:
      result = name_r->get(column_key, data, flags, where_column, select_column, res, false);
      DEBUG_PRINTF(LOG, "select %s where Name = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
      break;
    case Salary:
      result = sala_r->get(column_key, data, flags, where_column, select_column, res, true);
      DEBUG_PRINTF(LOG, "select %s where salary = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
      break;
    default:
      DEBUG_PRINTF(LOG, "column error");
    }
    return result;
  }


private:
  Data *data;
  DataFlag *flags;
  Index<int64_t> *id_r;
  Index<UserString> *uid_r;
  Index<UserString> *name_r;
  // salary need multi-index
  Index<int64_t> *sala_r;
};
