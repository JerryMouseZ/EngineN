#include <cassert>
#include <string>
#include <thread>
#include "../src/include/queue.hpp"

using std::string;

constexpr uint64_t NR_TEST_PRODUCER = 8;
constexpr uint64_t NR_DATA_PER_PRODUCER = 1000000;
constexpr uint64_t NR_TEST_DATA = NR_TEST_PRODUCER * NR_DATA_PER_PRODUCER;

constexpr uint64_t NR_TEST_CONSUMER = 4;
constexpr uint64_t NR_DATA_PER_CONSUMER = NR_TEST_DATA / NR_TEST_CONSUMER;

bool data_ready[NR_TEST_DATA] = {false};
std::atomic<uint64_t> pop_cnt = {0};

struct TestUser {
    int64_t id;
    char uid[128];
    char name[128];
    int64_t salary;
};

LocklessQueue<TestUser> queue;

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
    assert(user.salary == id2salary(user.id));
    char buf[128];
    id2str(user.id, buf);
    assert(memcmp(buf, user.name, 128) == 0);
    assert(memcmp(buf, user.uid, 128) == 0);
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

void consumer_work() {
    init_consumer_id();

    TestUser user;
    while (pop_cnt.load(std::memory_order_acquire) < NR_TEST_DATA) {
        queue.pop(&user);
        assert(user.id < NR_TEST_DATA && user.id >= 0);
        assert(!data_ready[user.id]);
        assert_user(user);
        data_ready[user.id] = true;
    }
}


int main() {
    std::thread producer_threads[NR_TEST_PRODUCER];
    std::thread consumer_threads[NR_TEST_CONSUMER];

    for (int i = 0; i < NR_DATA_PER_PRODUCER; i++) {
        producer_threads[i] = std::thread(producer_work);
    }

    for (int i = 0; i < NR_DATA_PER_CONSUMER; i++) {
        consumer_threads[i] = std::thread(consumer_work);
    }

    for (int i = 0; i < NR_DATA_PER_PRODUCER; i++) {
        if (producer_threads[i].joinable()) {
            producer_threads[i].join();
        }
    }

    for (int i = 0; i < NR_DATA_PER_CONSUMER; i++) {
        if (consumer_threads[i].joinable()) {
            consumer_threads[i].join();
        }
    }
}