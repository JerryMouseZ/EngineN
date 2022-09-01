#pragma once

#include <atomic>

extern std::atomic<int> nr_producer;
extern std::atomic<int> nr_cosumer;
extern thread_local int producer_id;
extern thread_local int consumer_id;

static inline bool have_producer_id() {
    return producer_id >= 0;
}

static inline void init_producer_id() {
    producer_id = nr_producer.fetch_add(1, std::memory_order_consume);
}

static inline bool have_consumer_id() {
    return consumer_id >= 0;
}

static inline void init_consumer_id() {
    consumer_id = nr_cosumer.fetch_add(1, std::memory_order_consume);
}