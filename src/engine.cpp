#include "include/engine.hpp"
#include "include/comm.h"
#include "include/config.hpp"
#include "include/data.hpp"
#include "include/util.hpp"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <fcntl.h>
#include <pthread.h>
#include <string>
#include <thread>

Engine::Engine(): datas(nullptr), id_r(nullptr), uid_r(nullptr), sala_r(nullptr), consumers(nullptr), alive{false}, neighbor_index{0} {
  host_index = -1;
  qs = static_cast<UserQueue *>(mmap(0, MAX_NR_CONSUMER * sizeof(UserQueue), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    new (&qs[i])UserQueue;
  }
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < MAX_NR_PRODUCER; j++) {
      send_fdall[i][j] = -1;
      recv_fdall[i][j] = -1;
    }
    for (int j = 0; j < MAX_NR_CONSUMER; j++) {
      sync_send_fdall[i][j] = -1;
      sync_recv_fdall[i][j] = -1;
    }
    for (int j = 0; j < MAX_NR_CONSUMER; j++) {
      in_sync[i][j] = false;
    }
    in_sync_visible = false;
    local_in_sync_cnt = 0;
    remote_in_sync_cnt = 0;
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
  
  // local index
  id_r = new Index;
  id_r->open(datas, qs);
  uid_r = new Index;
  uid_r->open(datas, qs);
  sala_r = new Index;
  sala_r->open(datas, qs);

  bool q_is_new_create[MAX_NR_CONSUMER];
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(INIT, "start open queue[%d]\n", i);
    if (qs[i].open(disk_path + "queue" + std::to_string(i), &q_is_new_create[i], datas[i].get_pmem_users(), &sync_qs[i], i)) {
      return false;
    }
  }

#pragma omp parallel for num_threads(16)
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    if (!q_is_new_create[i] && qs[i].need_rollback()) {
      fprintf(stderr, "rollback commit : %ld -> %ld\n", *qs[i].tail, qs[i].head->load());
      qs[i].compact_head();
      qs[i].tail_commit();
    }
    qs[i].reset_thread_states();
    DEBUG_PRINTF(INIT, "start build local index[%d] range [0, %ld)\n", i, qs[i].head->load());
    build_index(i, 0, qs[i].head->load(), id_r, uid_r, sala_r, &datas[i]);
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
    if (select_column == Salary) {
      for (int i = 0; !result && i < 3; i++) {
        result = remote_id_r[neighbor_index[i]].cache_get(column_key, where_column, select_column, res, false);
      }
    }
#ifndef BROADCAST
    if (!result)
      result = remote_id_r->get(column_key, where_column, select_column, res, false);
#endif
    DEBUG_PRINTF(VLOG, "select %s where ID = %ld, res = %ld\n", column_str(select_column).c_str(), *(int64_t *) column_key, result);
    break;
  case Userid:
    result = uid_r->get(column_key, where_column, select_column, res, false);
#ifndef BROADCAST
    if (!result)
      result = remote_uid_r->get(column_key, where_column, select_column, res, false);
#endif
    DEBUG_PRINTF(VLOG, "select %s where UID = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
    break;
  case Name:
    assert(0);
    /* result = name_r->get(column_key, where_column, select_column, res, false); */
    DEBUG_PRINTF(VLOG, "select %s where Name = %ld, res = %ld\n", column_str(select_column).c_str(), std::hash<std::string>()(std::string((char *) column_key, 128)), result);
    break;
  case Salary:
    result = sala_r->get(column_key, where_column, select_column, res, true);
    if (select_column == Id) {
      for (int i = 0; i < 3; i++) {
        res = ((char *)res) + result * key_len[select_column];
        result += remote_sala_r[neighbor_index[i]].cache_get(column_key, where_column, select_column, res, true);
      }
    }
#ifndef BROADCAST
    res = ((char *)res) + result * key_len[select_column];
    result += remote_sala_r->get(column_key, where_column, select_column, res, true);
#endif
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

  if ((select_column == Salary && where_column == Id) || (where_column == Salary && select_column == Id)) {
    return result;
  }

  if (result == 0 || where_column == Salary) {
    res = (char *) res + result * key_len[select_column];
    result += remote_read(select_column, where_column, column_key, column_key_len, res);
  }
  return result;
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
