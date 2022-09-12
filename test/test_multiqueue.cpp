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

constexpr uint64_t NR_TEST_CONSUMER = 8;

constexpr uint64_t TEST_ALIGN = 8192;

volatile bool data_ready[NR_TEST_DATA];

struct TestUser {
    int64_t id;
    char uid[128];
    char name[128];
    int64_t salary;
};

using TestUserQueue = LocklessQueue<TestUser, TEST_ALIGN>;
TestUserQueue queues[NR_TEST_CONSUMER];

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
    int64_t id = 1 + producer_id * NR_DATA_PER_PRODUCER;
    for (int i = 0; i < NR_DATA_PER_PRODUCER; i++, id++) {
        set_user(user, id);
        queues[id % NR_TEST_CONSUMER].push(&user);
    }
}

using TestArray = CommitArray<TestUser, TEST_ALIGN>;
thread_local TestUserQueue *test_consumer_q;
thread_local TestArray *consumer_mock_pmem_data;

constexpr uint64_t EACH_NR_TEST_DATA_ARRAY = ROUND_DIV(ROUND_DIV(NR_TEST_DATA, NR_TEST_CONSUMER), TestArray::N_DATA);

TestArray *mock_pmem_data[NR_TEST_CONSUMER];
volatile uint32_t ca_poses[NR_TEST_CONSUMER] = {0};
volatile uint32_t rest_pass_cnts[NR_TEST_CONSUMER] = {0};

bool full_check(TestArray *data_array) {
    for (int i = 0; i < TestArray::N_DATA; i++) {
        if (data_array->data[i].id == 0) {
            return false;
        }
    }

    for (int i = 0; i < TestArray::N_DATA; i++) {
        const TestUser &user = data_array->data[i];
        assert(user.id <= NR_TEST_DATA && user.id > 0);
        assert_user(user);
        data_ready[user.id - 1] = true;
    }

    return true;
}

int nonfull_check(TestArray *data_array) {
    for (int i = 0; i < TestArray::N_DATA; i++) {
        if (data_array->data[i].id == 0) {
            return i;
        }
        const TestUser &user = data_array->data[i];
        assert(user.id < NR_TEST_DATA && user.id >= 0);
        assert_user(user);
        data_ready[user.id - 1] = true;
    }

    return TestArray::N_DATA;
}

void consumer_work() {
    init_consumer_id();

    test_consumer_q = &queues[consumer_id];
    consumer_mock_pmem_data = mock_pmem_data[consumer_id]; 

    auto &ca_pos = ca_poses[consumer_id];
    while (test_consumer_q->pop()) {
        while (ca_pos < EACH_NR_TEST_DATA_ARRAY - 1) {
            if (!full_check(&consumer_mock_pmem_data[ca_pos])) {
                break;
            }
            ca_pos++;
        }
    }

    printf("Consumer %d finishes work\n", consumer_id);
}

void tail_consumer_work() {
    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        auto &ca_pos = ca_poses[i];
        printf("Consumer %u start tail from %u/%lu\n", i, ca_pos, EACH_NR_TEST_DATA_ARRAY - 1);
        for (; ca_pos < EACH_NR_TEST_DATA_ARRAY - 1; ca_pos++) {
            full_check(&mock_pmem_data[i][ca_pos]);
        }

        rest_pass_cnts[i] = nonfull_check(&mock_pmem_data[i][ca_pos]);
    }
}

int main() {
    using namespace std::chrono;

    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        mock_pmem_data[i] = static_cast<TestArray *>(mmap(0, EACH_NR_TEST_DATA_ARRAY * TEST_ALIGN, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
        memset(mock_pmem_data[i], 0, EACH_NR_TEST_DATA_ARRAY * TEST_ALIGN);
    }

    std::thread producer_threads[NR_TEST_PRODUCER];
    std::thread consumer_threads[NR_TEST_CONSUMER];

    bool is_new_create;
    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        queues[i].open("/mnt/disk/queue" + std::to_string(i), &is_new_create, mock_pmem_data[i], i);
        if (!is_new_create) {
            printf("Run clean.sh before run\n");
            exit(-1);
        }
        queues[i].reset_thread_states();
    }


    auto t1 = high_resolution_clock::now(); 

    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        producer_threads[i] = std::thread(producer_work);
    }

    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        consumer_threads[i] = std::thread(consumer_work);
    }

    for (int i = 0; i < NR_TEST_PRODUCER; i++) {
        if (producer_threads[i].joinable()) {
            producer_threads[i].join();
        }
    }


    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        queues[i].notify_producers_exit();
    }

    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        if (consumer_threads[i].joinable()) {
            consumer_threads[i].join();
        }
    }

    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        queues[i].tail_commit();
    }
    
    tail_consumer_work();

    uint32_t full_total = NR_TEST_CONSUMER * (EACH_NR_TEST_DATA_ARRAY - 1) * TestArray::N_DATA;
    uint32_t rest_total = 0;
    for (int i = 0; i < NR_TEST_CONSUMER; i++) {
        rest_total += rest_pass_cnts[i];
    }

    printf("Expected rest total = %lu, Actual rest total = %u\n", NR_TEST_DATA - full_total, rest_total);
    if (full_total + rest_total != NR_TEST_DATA) {
        for (int i = 0; i < NR_TEST_CONSUMER; i++) {
            printf("Consumer %d rest: %u\n", i, rest_pass_cnts[i]);
        }
        assert(full_total + rest_total == NR_TEST_DATA);
    }

    for (int i = 0; i < NR_TEST_DATA; i++) {
        assert(data_ready[i]);
    }

    auto t2 = high_resolution_clock::now(); 

    auto time_span = duration_cast<duration<double>>(t2 - t1);
    printf("Time: %lfms\n", time_span.count() * 1000);
}