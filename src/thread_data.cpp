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

thread_local int stat_thread_id = -1;
std::atomic<int> nr_stat_thread = {0};

thread_local User race_data;
thread_local UserQueue *consumer_q = nullptr;
thread_local time_point_t time_record_start_point;

#ifdef CHECK_THREAD_CNT

const char *where_name[9] = {
    "producer",
    "consumer",
    "reader",
    "request_handler",
    "process_request",
    "sync_send_handler",
    "process_sync_send",
    "sync_resp_handler",
    "process_sync_resp",
};

std::atomic<int32_t> where_thread_cnt[9] = {{0}};

void check_thread_spawn(int where_index) {
    if (unlikely(stat_thread_id < 0)) {
        stat_thread_id = nr_stat_thread.fetch_add(1);
        int type_thread_id = where_thread_cnt[where_index].fetch_add(1);
        DEBUG_PRINTF(0, "%s: Thread %d spawn at %s cnt = %d\n", this_host_info, stat_thread_id, where_name[where_index], type_thread_id + 1);
    }
}
#else 
void check_thread_spawn(int where_index) { }
#endif

bool have_producer_id() {
    return producer_id >= 0;
}

void init_producer_id() {
    check_thread_spawn(0);
    producer_id = nr_producer.fetch_add(1, std::memory_order_consume);
    DEBUG_PRINTF(producer_id < MAX_NR_PRODUCER, "Too many producers\n");
}

bool have_consumer_id() {
    return consumer_id >= 0;
}

void init_consumer_id() {
    check_thread_spawn(1);
    consumer_id = nr_cosumer.fetch_add(1, std::memory_order_consume);
    DEBUG_PRINTF(consumer_id < MAX_NR_CONSUMER, "Too many consumers\n");
}

bool have_reader_id() {
    return reader_id >= 0;
}

void init_reader_id() {
    check_thread_spawn(2);
    reader_id = nr_reader.fetch_add(1, std::memory_order_consume);
    DEBUG_PRINTF(reader_id < MAX_NR_PRODUCER, "Too many readers\n");
}
