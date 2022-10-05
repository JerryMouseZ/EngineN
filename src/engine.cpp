#include "include/engine.hpp"
#include "include/comm.h"
#include "include/config.hpp"
#include "include/data.hpp"
#include "include/send_recv.hpp"
#include "include/util.hpp"
#include "liburing.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <fcntl.h>
#include <pthread.h>
#include <string>
#include <thread>

Engine::Engine(): datas(nullptr), id_r(nullptr), uid_r(nullptr), sala_r(nullptr), consumers(nullptr) {
  host_index = -1;
  qs = static_cast<UserQueue *>(mmap(0, MAX_NR_CONSUMER * sizeof(UserQueue), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    new (&qs[i])UserQueue;
  }
  DEBUG_PRINTF(qs, "Fail to mmap consumer queues\n");
}

Engine::~Engine() {
  // disconnect all socket and handlers
  if (host_index != -1)
    disconnect();
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    qs[i].notify_producers_exit();
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    if (consumers[i].joinable()) {
      consumers[i].join();
    }
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    qs[i].tail_commit();
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    qs[i].statistics(i);
    qs[i].~LocklessQueue();
  }

  delete[] datas;
  delete id_r;
  delete uid_r;
  delete sala_r;
}

bool Engine::open(std::string aep_path, std::string disk_path) {
  if (aep_path[aep_path.size() - 1] != '/')
    aep_path.push_back('/');

  if (disk_path[disk_path.size() - 1] != '/')
    disk_path.push_back('/');

  datas = new Data[MAX_NR_CONSUMER];
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(INIT, "start open datas[%d]\n", i);
    datas[i].open(aep_path + "user.data" + std::to_string(i));
  }
  
  // remote data

  id_r = new Index(datas, qs);
  uid_r = new Index(datas, qs);
  sala_r = new Index(datas, qs);

  bool q_is_new_create;
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(INIT, "start open queue[%d]\n", i);
    if (qs[i].open(disk_path + "queue" + std::to_string(i), &q_is_new_create, datas[i].get_pmem_users(), i)) {
      return false;
    }

    if (!q_is_new_create && qs[i].need_rollback()) {
      qs[i].tail_commit();
    }

    qs[i].reset_thread_states();
  }

  for (int qid = 0; qid < MAX_NR_CONSUMER; qid++) {
    DEBUG_PRINTF(INIT, "start build local index[%d] range [0, %ld)\n", qid, qs[qid].head->load());
    build_index(qid, 0, qs[qid].head->load(), id_r, uid_r, sala_r, &datas[qid]);
  }

  consumers = new std::thread[MAX_NR_CONSUMER];
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    consumers[i] = std::thread([this]{
                               init_consumer_id();
                               consumer_q = &qs[consumer_id];
                               while (consumer_q->pop())
                               ;
                               });
  }
  
  // for remote
  bool remote_state_is_new_create;
  DEBUG_PRINTF(INIT, "start open remote_state\n");
  remote_state.open(disk_path + "remote_state", &remote_state_is_new_create);

  remote_datas = new Data[MAX_NR_CONSUMER];
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(INIT, "start open remote_datas[%d]\n", i);
    remote_datas[i].open(aep_path + "user.remote_data" + std::to_string(i));
  }

  remote_id_r = new Index(remote_datas, nullptr);
  remote_uid_r = new Index(remote_datas, nullptr);
  remote_sala_r = new Index(remote_datas, nullptr);

  return remote_state_is_new_create;
}

void Engine::build_index(int qid, int begin, int end, Index *id_index, Index *uid_index, Index *salary_index, Data *datap) {
  for (auto i = begin; i < end; i++) {
    const User *user = datap->data_read(i);
    uint32_t encoded_index = (qid << 28) | i;
    id_index->put(user->id, encoded_index);
    uid_index->put(std::hash<UserString>()(*(UserString *)(user->user_id)), encoded_index);
    salary_index->put(user->salary, encoded_index);
  }
}

void Engine::write(const User *user) {
  if (unlikely(!have_producer_id())) {
    init_producer_id();
  }

  DEBUG_PRINTF(VLOG, "write %ld %ld %ld %ld\n", user->id, std::hash<std::string>()(std::string(user->name, 128)), std::hash<std::string>()(std::string(user->user_id, 128)), user->salary);
  
  uint32_t qid = user->id % MAX_NR_CONSUMER;
  uint32_t index = qs[qid].push(user);
  size_t encoded_index = (qid << 28) | index;

  id_r->put(user->id, encoded_index);
  uid_r->put(std::hash<UserString>()(*(UserString *)(user->user_id)), encoded_index);
  sala_r->put(user->salary, encoded_index);
}

constexpr int key_len[4] = {8, 128, 128, 8};


size_t Engine::local_read(int32_t select_column,
                          int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
  size_t result = 0;
  switch(where_column) {
  case Id:
    result = id_r->get(column_key, where_column, select_column, res, false);
    if (!result)
      result = remote_id_r->get(column_key, where_column, select_column, res, false);
    DEBUG_PRINTF(VLOG, "select %s where ID = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
    break;
  case Userid:
    result = uid_r->get(column_key, where_column, select_column, res, false);
    if (!result)
      result = remote_uid_r->get(column_key, where_column, select_column, res, false);
    DEBUG_PRINTF(VLOG, "select %s where UID = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
    break;
  case Name:
    assert(0);
    /* result = name_r->get(column_key, where_column, select_column, res, false); */
    DEBUG_PRINTF(VLOG, "select %s where Name = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
    break;
  case Salary:
    result = sala_r->get(column_key, where_column, select_column, res, true);
    res = ((char *)res) + result * key_len[select_column];
    result += remote_sala_r->get(column_key, where_column, select_column, res, true);
    DEBUG_PRINTF(VLOG, "select %s where salary = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
    break;
  default:
    DEBUG_PRINTF(LOG, "column error");
  }
  return result;
}

size_t Engine::read(int32_t select_column,
                    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
  size_t result = 0;
  result = local_read(select_column, where_column, column_key, column_key_len, res);
  if (result == 0 || where_column == Salary) {
    res = (char *) res + result * key_len[select_column];
    result += remote_read(select_column, where_column, column_key, column_key_len, res);
  }
  return result;
}


std::string Engine::column_str(int column)
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
