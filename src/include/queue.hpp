#pragma once

#include <memory.h>
#include <sched.h>
#include <unistd.h>
#include <libpmem.h>

#include "thread_id.hpp"
#include "commit_array.hpp"
#include "util.hpp"

// 测试性能，需要调参
constexpr uint64_t QBITS = 14;
// 测试正确性
// constexpr uint64_t QBITS = 10;
constexpr uint64_t QSIZE = 1 << QBITS;
constexpr uint64_t QMASK = QSIZE - 1;

// OPT8
struct alignas(CACHELINE_SIZE) cache_aligned_uint64 {
    uint64_t value;
};

constexpr uint64_t PER_QUEUE_CONSUMER = 1;

template<typename T, uint64_t CMT_ALIGN>
class alignas(CACHELINE_SIZE) LocklessQueue {
public:

    using DataArray = CommitArray<T, CMT_ALIGN>;
    using cmt_func_t = void (*)(const DataArray *, uint64_t);
    using tail_cmt_func_t = void (*)(const DataArray *, uint64_t, uint64_t);

    static constexpr uint64_t CMT_BATCH_CNT = DataArray::N_DATA;
    static constexpr uint64_t UNALIGNED_META_SIZE = 2 * (64 + 64) + (MAX_NR_PRODUCER + PER_QUEUE_CONSUMER) * 64;
    static constexpr uint64_t QMETA_SIZE = ROUND_UP(UNALIGNED_META_SIZE, CMT_ALIGN);

    LocklessQueue() 
        : producers_exit(false), yield_producer_cnt({0}) {

        pthread_mutex_init(&mtx, NULL);
        pthread_cond_init(&cond_var, NULL);

        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            producer_yield_cnts[i].value = 0;
        }
        for (int i = 0; i < PER_QUEUE_CONSUMER; i++) {
            consumer_yield_cnts[i].value = 0;
        }

	    first = false;
        consumer_maybe_waiting = false;
    }

    int open(std::string fname, bool *is_new_create, DataArray *pmem_data, uint32_t id) {

        char *map_ptr = reinterpret_cast<char *>(map_file(fname.c_str(), QMETA_SIZE + QSIZE * sizeof(DataArray), is_new_create));
        
        if (map_ptr == nullptr) {
            return 1;
        }

        // std::atomic<uint64_t> head;
        // volatile uint64_t thread_heads[MAX_NR_PRODUCER];
        // volatile uint64_t last_tail;

        // std::atomic<uint64_t> tail;
        // volatile uint64_t thread_tails[MAX_NR_CONSUMER];
        // volatile uint64_t last_head;

        // T data[QSIZE];

        head = reinterpret_cast<std::atomic<uint64_t> *>(map_ptr);
        thread_heads = reinterpret_cast<volatile cache_aligned_uint64 *>(map_ptr + 64);
        last_tail = reinterpret_cast<volatile uint64_t *>(thread_heads + MAX_NR_PRODUCER);

        tail = (std::atomic<uint64_t> *)(last_tail + 8); // cast away volatile qualifier
        thread_tails = reinterpret_cast<volatile cache_aligned_uint64 *>(((char *)tail) + 64);
        last_head = reinterpret_cast<volatile uint64_t *>(thread_tails + PER_QUEUE_CONSUMER);

        data = (DataArray *)(map_ptr + QMETA_SIZE);
        this->pmem_data = pmem_data;
        this->id = id;

        printf("Open queue: last_tail = %lu, tail = %lu, last_head = %lu, head = %lu\n", *last_tail, tail->load(), *last_head, head->load());

        return 0;
    }

    uint64_t push(const T *new_data) {
        this_thread_head() = head->load(std::memory_order_consume);

        uint64_t pos = head->fetch_add(1, std::memory_order_acq_rel);

        this_thread_head() = pos;

        uint64_t caid = pos / CMT_BATCH_CNT;
        uint64_t inner_ca_pos = pos % CMT_BATCH_CNT;
        uint64_t ca_pos = caid & QMASK;
        
        int local_yield_cnt = 0;
        while (*last_tail + QSIZE <= pos) {
            // printf("Producer %d yield at %lu last_tail = %lu, last_head = %lu, head = %lu\n", producer_id, nv_this_thread_head(), *last_tail, *last_head, head->load());
            producer_yield_thread(local_yield_cnt++);

            update_last_tail();
        }

        memcpy(&data[ca_pos].data[inner_ca_pos], new_data, sizeof(T));

        // printf("Pid = %d, DataId = %lu, DataIndex = %lu, DataPos = %lu\n", producer_id, *((uint64_t *)new_data), pos, pos & QMASK);

        std::atomic_thread_fence(std::memory_order_release);
        this_thread_head() = UINT64_MAX;

        if (pos % (QSIZE >> 5) == 0) {
            try_wake_consumer();
        }

        return pos + 1;
    }

    bool pop() {
        this_thread_tail() = tail->load(std::memory_order_consume);

        uint64_t pos = tail->fetch_add(CMT_BATCH_CNT, std::memory_order_acq_rel);
        
        this_thread_tail() = pos;

        uint64_t caid = pos / CMT_BATCH_CNT;
        uint64_t ca_pos = caid & QMASK;
        
        while (nv_this_thread_tail() + CMT_BATCH_CNT - 1 >= *last_head) {

            // exit condition
            if (unlikely(producers_exit)) {
                return false;
            }

            // printf("Consumer %d yield at %lu last_tail = %lu, last_head = %lu, head = %lu\n", consumer_id, nv_this_thread_tail(), *last_tail, *last_head, head->load());
            consumer_yield_thread();

            auto min = head->load(std::memory_order_consume);

            for (int i = 0; i < MAX_NR_PRODUCER; i++) {
                __builtin_prefetch((cache_aligned_uint64 *)(thread_heads + i + 1));
                auto tmp_head = thread_heads[i].value;

                std::atomic_thread_fence(std::memory_order_consume);

                if (tmp_head < min) {
                    min = tmp_head;
                }
            }

            /*
                多个consumer可能会同时更新，导致last_head并不是最新的，但无妨，
                因为只要能通过while条件就不影响写入，而通不过last_head条件也只是再来一次
            */
            /*
                可能发生多个线程同时更新，应当取它们每个人眼中min的最大值，否则可能导致
                较小的min被最后写入，导致有人的while判断从能通过变成不能通过，发生不必要的yield
            */
            do {
                *last_head = min;
            } while(*last_head < min);
        }
        
        do_commit(&data[ca_pos], caid);    

        std::atomic_thread_fence(std::memory_order_release);

        this_thread_tail() = UINT64_MAX;

        return true;
    }

    void do_commit(const UserArray *src, uint64_t ca_index) {
        pmem_memcpy_persist(&pmem_data[ca_index], src, QCMT_ALIGN);
    }

    void do_unaligned_commit(const UserArray *src, uint64_t ca_index, uint64_t pop_cnt) {
        pmem_memcpy_persist(&pmem_data[ca_index], src, pop_cnt * sizeof(User));
    }

    void notify_producers_exit() {
        producers_exit = true;
        pthread_mutex_lock(&mtx);
        pthread_cond_signal(&cond_var);
        pthread_mutex_unlock(&mtx);
    }

    void tail_commit() {

        printf("Before tail commit, last_tail = %lu, head = %lu, tail = %lu\n", *last_tail, head->load(), tail->load());

        update_last_tail();
        
        uint64_t tail_value = *last_tail;
        uint64_t head_value = head->load(std::memory_order_relaxed);

        printf("Start tail commit, last_tail = %lu, head = %lu, tail = %lu\n", tail_value, head_value, tail->load());

        if (tail_value == head_value) {
            tail->store(tail_value);
            printf("No thing to commit, last_tail > head: last_tail = %lu, tail = %lu, head: %lu\n", tail_value, tail->load(), head_value);
            return;
        } else if (tail_value > head_value) {
            printf("Wierd, last_tail > head: last_tail = %lu, tail = %lu, head: %lu\n", tail_value, tail->load(), head_value);
            return;
        }

        if (tail_value % CMT_BATCH_CNT != 0) {
            printf("Wierd, last_tail not aligned to CMT_CNT: last_tail = %lu\n", tail_value);
        }

        uint64_t start_caid = tail_value / CMT_BATCH_CNT;
        uint64_t end_caid = head_value / CMT_BATCH_CNT;
        uint64_t end_ca_cnt = head_value % CMT_BATCH_CNT;

        uint64_t start_ca_pos = start_caid & QMASK;
        uint64_t end_ca_pos = end_caid & QMASK;

        printf("Start pos: %lu, End pos: %lu, nonfull_cnt = %lu, QSIZE = %lu\n", start_ca_pos, end_ca_pos, end_ca_cnt, QSIZE);

        if (start_ca_pos <= end_ca_pos) {
            printf("No wind\n");
            for (auto i = 0; i < end_ca_pos - start_ca_pos; i++) {
                do_commit(&data[start_ca_pos + i], start_caid + i);
            }
        } else {
            printf("Wind\n");
            for (auto i = 0; i < QSIZE - start_ca_pos; i++) {
                do_commit(&data[start_ca_pos + i], start_caid + i);
            }
            for (auto i = 0; i < end_ca_pos; i++) {
                do_commit(&data[i], start_caid + i + QSIZE - start_ca_pos);
            }
        }
        
        if (end_ca_cnt != 0) {
            do_unaligned_commit(&data[end_ca_pos], end_caid, end_ca_cnt);
        }

        std::atomic_thread_fence(std::memory_order_release);
        *last_tail = head_value;
        tail->store(head_value);

        printf("End tail commit\n");
    }

    uint64_t min_uncommitted_data_index() {
        return *last_tail;
    }

    void pop_at(T *dst, uint64_t index) {
        memcpy(dst, &data[index & QMASK], sizeof(T));
    }

    void reset_thread_states() {
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            thread_heads[i].value = UINT64_MAX;
        }
        for (int i = 0; i < PER_QUEUE_CONSUMER; i++) {
            thread_tails[i].value = UINT64_MAX;
        }
    }

    bool need_rollback() {
        return *last_tail != head->load();
    }

    void statistics(uint32_t qid) {
        uint64_t total_producer_yield_cnts = 0,
            total_consumer_yield_cnts = 0;
        
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            total_producer_yield_cnts += producer_yield_cnts[i].value;
        }
        for (int i = 0; i < MAX_NR_CONSUMER; i++) {
            total_consumer_yield_cnts += consumer_yield_cnts[i].value;
        }

        printf( "Queue %u:\n"
                "\tTotal yield:\n"
                "\t\tproducer: %lu\n"
                "\t\tconsumer: %lu\n"
                "\tAvg yield:\n"
                "\t\tproducer: %lu\n"
                "\t\tconsumer: %lu\n\n",
                qid,
                total_producer_yield_cnts, 
                total_consumer_yield_cnts,
                total_producer_yield_cnts / MAX_NR_PRODUCER,
                total_consumer_yield_cnts / MAX_NR_CONSUMER);
    }

private:
    volatile uint64_t &this_thread_head() {
        return thread_heads[producer_id].value;
    }

    uint64_t nv_this_thread_head() {
        return ((cache_aligned_uint64 *)thread_heads)[producer_id].value;
    }

    volatile uint64_t &this_thread_tail() {
        return thread_tails[0].value;
    }

    uint64_t nv_this_thread_tail() {
        return ((cache_aligned_uint64 *)thread_tails)[0].value;
    }

    void producer_yield_thread(uint32_t cnt) {
        pthread_mutex_lock(&mtx);
	
        if (!first) {
            first = true;
                pthread_cond_signal(&cond_var);
            first = false;
        } else {
            // usleep(10 + cnt * 10);
            sched_yield();
        }

        pthread_mutex_unlock(&mtx);

        producer_yield_cnts[producer_id].value++;
    }

    void try_wake_consumer() {
        if (consumer_maybe_waiting) {
            pthread_mutex_lock(&mtx);
            pthread_cond_signal(&cond_var);
            pthread_mutex_unlock(&mtx);
        }
    }

    void consumer_yield_thread() {
        pthread_mutex_lock(&mtx);
        consumer_maybe_waiting = true;
        pthread_cond_wait(&cond_var, &mtx);
        consumer_maybe_waiting = false;
        pthread_mutex_unlock(&mtx);
        consumer_yield_cnts[consumer_id].value++;
    }

    void update_last_tail() {

        auto min = tail->load(std::memory_order_consume);
        
        for (int i = 0; i < PER_QUEUE_CONSUMER; i++) {

            auto tmp_tail = thread_tails[i].value;

            std::atomic_thread_fence(std::memory_order_consume);

            if (tmp_tail < min) {
                min = tmp_tail;
            }
        }

        /*
            多个producer可能会同时更新，导致last_tail并不是最新的，但无妨，
            因为只要能通过while条件就不影响写入，而通不过last_tail条件也只是再来一次
        */
        /*
            可能发生多个线程同时更新，应当取它们每个人眼中min的最大值，否则可能导致
            较小的min被最后写入，导致有人的while判断从能通过变成不能通过，发生不必要的yield
        */
        do {
            *last_tail = min;
        } while(*last_tail < min);
    }

    std::atomic<uint64_t> *head;
    volatile cache_aligned_uint64 *thread_heads;
    volatile uint64_t *last_tail;

    std::atomic<uint64_t> *tail;
    volatile cache_aligned_uint64 *thread_tails;
    volatile uint64_t *last_head;

    DataArray *data;
    DataArray *pmem_data;

    volatile bool producers_exit;

    char pad[64];
    std::atomic<uint64_t> yield_producer_cnt;

    cache_aligned_uint64 producer_yield_cnts[MAX_NR_PRODUCER];
    cache_aligned_uint64 consumer_yield_cnts[MAX_NR_CONSUMER];

    pthread_mutex_t mtx;
    pthread_cond_t cond_var;
    volatile bool first;
    volatile bool consumer_maybe_waiting;
    uint32_t id;
};