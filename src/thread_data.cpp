#include "include/thread_id.hpp"
#include "include/data.hpp"
#include "include/util.hpp"
#include "include/data_access.hpp"
#include "include/time.hpp"

std::atomic<int> nr_producer = {0};
std::atomic<int> nr_cosumer = {0};
thread_local int producer_id = -1;
thread_local int consumer_id = -1;
std::atomic<int> nr_reader = {0};
thread_local int reader_id = -1;

thread_local User race_data;
thread_local UserQueue *consumer_q = nullptr;
thread_local time_point_t time_record_start_point;

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

bool have_reader_id() {
    return reader_id >= 0;
}

void init_reader_id() {
    reader_id = nr_reader.fetch_add(1, std::memory_order_consume);
    DEBUG_PRINTF(reader_id < MAX_NR_PRODUCER, "Too many readers\n");
}
