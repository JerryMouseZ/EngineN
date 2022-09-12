#include "include/thread_id.hpp"
#include "include/data.hpp"
#include "include/util.hpp"
#include "include/data_access.hpp"

std::atomic<int> nr_producer = {0};
std::atomic<int> nr_cosumer = {0};
thread_local int producer_id = -1;
thread_local int consumer_id = -1;

thread_local User race_data;
thread_local UserQueue *consumer_q = nullptr;

bool have_producer_id() {
    return producer_id >= 0;
}

void init_producer_id() {
    producer_id = nr_producer.fetch_add(1, std::memory_order_consume);
    DEBUG_PRINTF(producer_id < MAX_NR_PRODUCER, "Too many producers\n");
}

bool have_consumer_id() {
    return consumer_id >= 0;
}

void init_consumer_id() {
    consumer_id = nr_cosumer.fetch_add(1, std::memory_order_consume);

    DEBUG_PRINTF(consumer_id < MAX_NR_CONSUMER, "Too many consumers\n");
}