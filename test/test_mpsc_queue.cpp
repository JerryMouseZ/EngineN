#include <cassert>
#include <string>
#include <thread>
#include <ctime>
#include <ratio>
#include <chrono>
#include "../src/include/mpsc_queue.hpp"

using std::string;

// 如需修改，需要一并修改queue.hpp
constexpr uint64_t NR_TEST_PRODUCER = 50;
constexpr uint64_t NR_DATA_PER_PRODUCER = 1000000;
constexpr uint64_t NR_TEST_DATA = NR_TEST_PRODUCER * NR_DATA_PER_PRODUCER;

bool data_ready[NR_TEST_DATA] = {false};
uint64_t already_pop_cnt = 0;

struct TestUser {
    int64_t id;
    char uid[128];
    char name[128];
    int64_t salary;
};

MPSCQueue<TestUser> queue;

int64_t id2salary(int64_t id) {
    return id * 10;
}

void id2str(int64_t id, char *buf) {
    string idstr = std::to_string(id);
    memset(buf, 0, 128);
    strncpy(buf, idstr.c_str(), idstr.size());
}

void set_user(TestUser &user, int64_t id) {
    user.id = id;
    user.salary = id2salary(id);
    id2str(id, user.uid);
    memcpy(user.name, user.uid, 128); 
}

void assert_user(const TestUser &user) {
    bool equal;
    char buf[128];
    
    equal = user.salary == id2salary(user.id);
    if (!equal) {
        printf("buf: %ld salary: %ld\n", user.id, user.salary);
        exit(-1);
    }

    id2str(user.id, buf);
    equal = memcmp(buf, user.name, 128) == 0;
    if (!equal) {
        printf("buf: %s name: %s\n", buf, user.name);
        exit(-1);
        // assert(false);
    }

    equal = memcmp(buf, user.uid, 128) == 0;
    if (!equal) {
        printf("buf: %s uid: %s\n", buf, user.uid);
        exit(-1);
        // assert(false);
    }
}

void producer_work() {
    init_producer_id();

    TestUser user;
    int64_t id = producer_id * NR_DATA_PER_PRODUCER;
    for (int i = 0; i < NR_DATA_PER_PRODUCER; i++, id++) {
        set_user(user, id);
        queue.push(&user);
    }
}

constexpr uint64_t ARRARY_CAP = 40;
TestUser test_data[ARRARY_CAP];

void pop_work(const TestUser *data, uint64_t pop_cnt) {
    memcpy(test_data, data, pop_cnt * sizeof(TestUser));

    for (uint64_t i = 0; i < pop_cnt; i++) {
        TestUser &user = test_data[i];
        assert(user.id < NR_TEST_DATA && user.id >= 0);
        assert(!data_ready[user.id]);
        assert_user(user);
        data_ready[user.id] = true;
    }

    already_pop_cnt += pop_cnt;
}

void consumer_work() {
    init_consumer_id();

    while (already_pop_cnt < NR_TEST_DATA) {
        queue.pop(pop_work, ARRARY_CAP);
    }
}


int main() {
    using namespace std::chrono;

    std::thread producer_threads[NR_TEST_PRODUCER];
    std::thread consumer_thread;

    auto t1 = high_resolution_clock::now(); 

    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        producer_threads[i] = std::thread(producer_work);
    }


    consumer_thread = std::thread(consumer_work);

    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        if (producer_threads[i].joinable()) {
            producer_threads[i].join();
        }
    }

    if (consumer_thread.joinable()) {
        consumer_thread.join();
    }

    auto t2 = high_resolution_clock::now(); 

    auto time_span = duration_cast<duration<double>>(t2 - t1);
    printf("Time: %lfms\n", time_span.count() * 1000);
    
    uint64_t nr_producers_total_yield_cnt = 0;
    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        nr_producers_total_yield_cnt += queue.producer_yield_cnts[i].value;
    }

    printf("Yield cnt: producers = %lu, consumer = %lu\n", nr_producers_total_yield_cnt, queue.consumer_yield_cnt.value);
}