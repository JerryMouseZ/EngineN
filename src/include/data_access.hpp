#pragma once

#include "data.hpp"
#include "queue.hpp"
#include <cassert>
#include <cstddef>
#include <cstdint>

using UserQueue = LocklessQueue<QCMT_ALIGN>;

extern thread_local User race_data;

class DataAccess {
public:
  DataAccess()
    : datas(nullptr), qs(nullptr) {}

  DataAccess(Data *datas, UserQueue *qs)
    : datas(datas), qs(qs) {}

  const User *read(uint32_t encoded_index, size_t &uncommited_index) {
    uint32_t qid = encoded_index >> 28;
    uint32_t index = encoded_index & ((1 << 28) - 1);
    auto data = &datas[qid];
    uncommited_index = UINT64_MAX;
    if (qs == nullptr)
      return data->data_read(index);

    auto q = &qs[qid];
    if (likely(index < q->min_uncommitted_data_index())) {
      return data->data_read(index);
    }

    /* DEBUG_PRINTF(WBREAD, "Try to read from write buffer: qid = %u, index = %u, but blocked\n", qid, index); */

    /* int yield_cnt = 0; */
    /* while (index >= q->min_uncommitted_data_index() && yield_cnt < 50) { */
    /*   sched_yield(); */
    /*   yield_cnt++; */
    /* } */

    /* DEBUG_PRINTF(WBREAD, "Block released from write buffer: qid = %u, index = %u\n", qid, index); */
    q->pop_at(&race_data, index);
    std::atomic_thread_fence(std::memory_order_release);
    uncommited_index = q->min_uncommitted_data_index();
    if (index >= uncommited_index) {
      DEBUG_PRINTF(QDEBUG, "Read from write buffer: qid = %u, index = %u\n", qid, index);
      return &race_data;
    }

    return data->data_read(index);
  }

  bool need_to_retry(uint32_t encoded_index, size_t uncommited_index) {
    uint32_t qid = encoded_index >> 28;
    uint32_t index = encoded_index & ((1 << 28) - 1);
    auto data = &datas[qid];
    auto q = &qs[qid];
    if (qs == nullptr)
      return false;
    return index >= uncommited_index && index < q->min_uncommitted_data_index();
  }


private:
  Data *datas;
  UserQueue *qs;
};

