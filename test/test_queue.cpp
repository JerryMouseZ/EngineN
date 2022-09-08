#include <cassert>
#include <string>
#include <thread>
#include <ctime>
#include <ratio>
#include <chrono>
#include "../src/include/queue.hpp"

using std::string;

// 如需修改，需要一并修改queue.hpp
constexpr uint64_t NR_TEST_PRODUCER = 50;
constexpr uint64_t NR_DATA_PER_PRODUCER = 10000;
constexpr uint64_t NR_TEST_DATA = NR_TEST_PRODUCER * NR_DATA_PER_PRODUCER;

constexpr uint64_t NR_TEST_CONSUMER = 5;

constexpr uint64_t TEST_ALIGN = 8192;

bool data_ready[NR_TEST_DATA] = {false};

struct TestUser {
    int64_t id;
    char uid[128];
    char name[128];
    int64_t salary;
};

LocklessQueue<TestUser, TEST_ALIGN> queue;

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

using TestArray = CommitArray<TestUser, TEST_ALIGN>;
thread_local TestArray *consumer_buffer;

void do_batch_pop(const TestArray *data, uint64_t first_index) {
    memcpy(consumer_buffer, data, TEST_ALIGN);
    
    for (int i = 0; i < TestArray::N_DATA; i++) {
        const TestUser &user = consumer_buffer->data[i];
        assert(user.id < NR_TEST_DATA && user.id >= 0);
        assert(!data_ready[user.id]);
        assert_user(user);
        data_ready[user.id] = true;
    }
}

TestArray *map_ptr;

void do_tail_pop(const TestArray *data, uint64_t first_index, uint64_t pop_cnt) {
    memcpy(map_ptr, data, pop_cnt * sizeof(TestUser));

    for (int i = 0; i < pop_cnt; i++) {
        const TestUser &user = map_ptr->data[i];
        assert(user.id < NR_TEST_DATA && user.id >= 0);
        // assert(!data_ready[user.id]);
        assert_user(user);
        data_ready[user.id] = true;
    }
}

void consumer_work() {
    init_consumer_id();

    while (queue.pop(do_batch_pop))
        ;
}

int main() {
    using namespace std::chrono;

    map_ptr = static_cast<TestArray *>(mmap(0, NR_TEST_CONSUMER * TEST_ALIGN, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));

    std::thread producer_threads[NR_TEST_PRODUCER];
    std::thread consumer_threads[NR_TEST_CONSUMER];

    bool is_new_create;
    queue.open("/mnt/disk/queue", &is_new_create);

    queue.reset_thread_states();

    auto t1 = high_resolution_clock::now(); 

    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        producer_threads[i] = std::thread(producer_work);
    }

    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        consumer_threads[i] = std::thread([i]() {
            consumer_buffer = &map_ptr[i];
            consumer_work();
        });
    }

    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        if (producer_threads[i].joinable()) {
            producer_threads[i].join();
        }
    }


    queue.notify_producers_exit();

    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        if (consumer_threads[i].joinable()) {
            consumer_threads[i].join();
        }
    }

    queue.tail_commit(do_batch_pop, do_tail_pop);

    auto t2 = high_resolution_clock::now(); 

    auto time_span = duration_cast<duration<double>>(t2 - t1);
    printf("Time: %lfms\n", time_span.count() * 1000);
}