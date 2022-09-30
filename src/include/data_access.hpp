#pragma once

#include "data.hpp"
#include "queue.hpp"

using UserQueue = LocklessQueue<User, QCMT_ALIGN>;

extern thread_local User race_data;

class DataAccess {
public:
  DataAccess(Data *datas, UserQueue *qs)
    : datas(datas), qs(qs) {}

  const User *read(uint32_t encoded_index) {
    uint32_t qid = encoded_index >> 28;
    uint32_t index = encoded_index & ((1 << 28) - 1);
    auto data = &datas[qid];

    // for remote
    if (qs == nullptr)
      return data->data_read(index);

    // for local
    auto q = &qs[qid];

    if (!data->get_flag(index))
      return nullptr;
    index -= 1;

    if (likely(index < q->min_uncommitted_data_index())) {
      return data->data_read(index);
    }

    q->pop_at(&race_data, index);

    std::atomic_thread_fence(std::memory_order_release);

    if (index >= q->min_uncommitted_data_index()) {
      DEBUG_PRINTF(QDEBUG, "Read from write buffer: qid = %u, index = %u\n", qid, index);
      return &race_data;
    }
    
    return data->data_read(index);      
  }

private:
    Data *datas;
    UserQueue *qs;
};
