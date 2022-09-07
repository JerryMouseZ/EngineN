#pragma once

#include "data.hpp"
#include "queue.hpp"

extern thread_local User race_data;

class DataAccess {
public:
  DataAccess(Data *data, LocklessQueue<User> *q)
    : data(data), q(q) {}

  const User *read(uint32_t index) {
    if (!data->get_flag(index))
      return nullptr;
    index -= 1;

    if (likely(index < q->min_uncommitted_data_index())) {
      return data->data_read(index);
    }

    q->pop_at(&race_data, index);

    std::atomic_thread_fence(std::memory_order_release);

    if (index >= q->min_uncommitted_data_index()) {
      printf("Read from write buffer: index = %u\n", index);
      return &race_data;
    }
    
    return data->data_read(index);      
  }

private:
    Data *data;
    LocklessQueue<User> *q;
};