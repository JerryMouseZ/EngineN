#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include "log.hpp"

#include "index.hpp"
#include "data.hpp"
#include "cs.hpp"

class Engine
{
public:
  Engine(): data(nullptr), id_r(nullptr), uid_r(nullptr), sala_r(nullptr) {
  }


  ~Engine() {
    delete conn;
    delete log;
    delete data;
    delete id_r;
    delete uid_r;
    delete sala_r;
  }


  void open(std::string aep_path, std::string disk_path) {
    std::string data_prefix = aep_path;
    if (data_prefix[data_prefix.size() - 1] != '/')
      data_prefix.push_back('/');
    data = new Data();
    data->open(data_prefix + "user.data", disk_path + "cache", disk_path + "flag");
    log = new CircularFifo(disk_path + "log", data);

    id_r = new Index(disk_path + "id", log);
    uid_r = new Index(disk_path + "uid", log);
    sala_r = new Index(disk_path + "salary", log);
  }
  
  using info_type = std::pair<std::string, int>;
  // 创建listen socket，尝试和别的机器建立两条连接
  void connect(const char *host_info, const char *const *peer_host_info, size_t peer_host_info_num) {
    std::vector<info_type> infos;
    const char *split_index = strstr(host_info, ":");
    int host_ip_len = split_index - host_info;
    std::string host_ip = std::string(host_info, host_ip_len);
    int host_port = atoi(split_index + 1);
    fprintf(stderr, "host info : %s %d\n", host_ip.c_str(), host_port);
    infos.emplace_back(info_type(host_ip, host_port));
    
    // peer ips
    for (int i = 0; i < peer_host_info_num; ++i) {
      const char *split_index = strstr(peer_host_info[i], ":");
      int ip_len = split_index - peer_host_info[i];
      std::string ip = std::string(peer_host_info[i], ip_len);
      int port = atoi(split_index + 1);
      infos.emplace_back(info_type(ip, port)); 
    }

    std::sort(infos.begin(), infos.end(), [](const info_type &a, const info_type &b){ return a.first < b.first; });
    int my_index = -1;
    for (int i = 0; i < peer_host_info_num + 1; ++i) {
      if (infos[i].first == host_ip) {
        my_index = i;
        break;
      }
    }
    
    conn = new Connector();
    conn->connect(infos, peer_host_info_num + 1, my_index);
    exit(-1);
  }


  void write(const User *user) {
    DEBUG_PRINTF(LOG, "write %ld %ld %ld %ld\n", user->id, std::hash<std::string>()(std::string(user->name, 128)), std::hash<std::string>()(std::string(user->user_id, 128)), user->salary);
    size_t offset = log->push(*user);
    /* uint64_t offset = data->data_write(*user); */
    id_r->put(user->id, offset);
    uid_r->put(std::hash<UserString>()(*(UserString *)(user->user_id)), offset);
    sala_r->put(user->salary, offset);

    data->put_flag(offset);
  }

  size_t read(int32_t select_column,
              int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    size_t result = 0;
    switch(where_column) {
    case Id:
      result = id_r->get(column_key, where_column, select_column, res, false);
      DEBUG_PRINTF(LOG, "select %s where ID = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
      break;
    case Userid:
      result = uid_r->get(column_key, where_column, select_column, res, false);
      DEBUG_PRINTF(LOG, "select %s where UID = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
      break;
    case Name:
      assert(0);
      /* result = name_r->get(column_key, where_column, select_column, res, false); */
      DEBUG_PRINTF(LOG, "select %s where Name = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
      break;
    case Salary:
      result = sala_r->get(column_key, where_column, select_column, res, true);
      DEBUG_PRINTF(LOG, "select %s where salary = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
      break;
    default:
      DEBUG_PRINTF(LOG, "column error");
    }
    return result;
  }

private:
  static std::string column_str(int column)
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



private:
  Data *data;
  Index *id_r;
  Index *uid_r;
  // salary need multi-index
  Index *sala_r;
  CircularFifo *log;
  Connector *conn;
};
