#pragma once
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>
#include <thread>
#include "queue.hpp"

#include "index.hpp"
#include "data.hpp"

extern UserArray *pmem_users;

void cmt_work(const UserArray *src, uint64_t ca_index) {
  pmem_memcpy_persist(&pmem_users[ca_index], src, QCMT_ALIGN);
}

void tail_cmt_work(const UserArray *src, uint64_t ca_index, uint64_t pop_cnt) {
  pmem_memcpy_persist(&pmem_users[ca_index], src, pop_cnt * sizeof(User));
}

class Engine
{
public:
  Engine(): data(nullptr), id_r(nullptr), uid_r(nullptr), sala_r(nullptr), consumers(nullptr) {
  }


  ~Engine() {
    
    q.notify_producers_exit();

    for (int i = 0; i < MAX_NR_CONSUMER; i++) {
        if (consumers[i].joinable()) {
            consumers[i].join();
        }
    }

    q.tail_commit(cmt_work, tail_cmt_work);

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

    pmem_users = data->get_pmem_users();

    bool q_is_new_create;
    if (q.open(disk_path + "queue", &q_is_new_create)) {
      return;
    }
    
    printf("Init success\n");

    id_r = new Index(disk_path + "id", data, &q);
    uid_r = new Index(disk_path + "uid", data, &q);
    sala_r = new Index(disk_path + "salary", data, &q);

    if (!q_is_new_create && q.need_rollback()) {
      q.tail_commit(cmt_work, tail_cmt_work);
    }

    q.reset_thread_states();

    consumers = new std::thread[MAX_NR_CONSUMER];
    for (int i = 0; i < MAX_NR_CONSUMER; i++) {
      consumers[i] = std::thread([this]{
        init_consumer_id();
        while (q.pop(cmt_work))
          ;
      });
    }
  }


  void write(const User *user) {
    if (unlikely(!have_producer_id())) {
      init_producer_id();
    }

    DEBUG_PRINTF(LOG, "write %ld %ld %ld %ld\n", user->id, std::hash<std::string>()(std::string(user->name, 128)), std::hash<std::string>()(std::string(user->user_id, 128)), user->salary);
    size_t offset = q.push(user);
    id_r->put(user->id, offset);
    uid_r->put(std::hash<UserString>()(*(UserString *)(user->user_id)), offset);
    sala_r->put(user->salary, offset);

    data->put_flag(offset);
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
  Data *data;
  Index *id_r;
  Index *uid_r;
  // salary need multi-index
  Index *sala_r;
  UserQueue q;
  std::thread *consumers;
};
