#pragma once

#include <atomic>

constexpr uint64_t MAX_NR_PRODUCER = 50;
constexpr uint64_t MAX_NR_CONSUMER = 5;
constexpr uint64_t NR_POP_BATCH = 30;

extern std::atomic<int> nr_producer;
extern std::atomic<int> nr_cosumer;
extern thread_local int producer_id;
extern thread_local int consumer_id;

bool have_producer_id();
void init_producer_id();
bool have_consumer_id();
void init_consumer_id();