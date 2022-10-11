#pragma once

#include "data.hpp"
#include "queue.hpp"
#include <cassert>

using UserQueue = LocklessQueue<QCMT_ALIGN>;

extern thread_local User race_data;

class DataAccess {
public:
  DataAccess()
    : datas(nullptr), qs(nullptr) {}

  DataAccess(Data *datas, UserQueue *qs)
    : datas(datas), qs(qs) {}

  const User *read(uint32_t encoded_index) {
    uint32_t qid = encoded_index >> 28;
    uint32_t index = encoded_index & ((1 << 28) - 1);
    auto data = &datas[qid];
    
    if (qs == nullptr)
      return data->data_read(index);

    // for local
    auto q = &qs[qid];
    // for remote

    if (likely(index < q->min_uncommitted_data_index())) {
      return data->data_read(index);
    }

    q->pop_at(&race_data, index);

    std::atomic_thread_fence(std::memory_order_release);

    /*
    DEBUG_PRINTF(WBREAD, "Try to read from write buffer: qid = %u, index = %u, but blocked\n", qid, index);

    int yield_cnt = 0;
    while (index >= q->min_uncommitted_data_index() && yield_cnt < 50) {
      sched_yield();
      yield_cnt++;
    }

    DEBUG_PRINTF(WBREAD, "Block released from write buffer: qid = %u, index = %u\n", qid, index);
    */
    if (unlikely(index >= q->min_uncommitted_data_index())) {
      DEBUG_PRINTF(WBREAD, "Read from write buffer: qid = %u, index = %u\n", qid, index);
      return &race_data;
    }

    return data->data_read(index);      
  }

private:
  Data *datas;
  UserQueue *qs;
};

class RemoteDataAccess {
public:
  RemoteDataAccess()
    : rmdatas(nullptr) { }

  RemoteDataAccess(RemoteData *rmdatas)
    : rmdatas(rmdatas) {}

  const RemoteUser *read(uint32_t encoded_index) {
    uint32_t qid = encoded_index >> 28;
    uint32_t index = encoded_index & ((1 << 28) - 1);
    return rmdatas[qid].data_read(index);
  }

private:
  RemoteData *rmdatas;
};

