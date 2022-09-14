#include <memory.h>
#include <sched.h>
#include <unistd.h>
#include <cstdio>

#include "thread_id.hpp"
#include "util.hpp"

using T = int;

constexpr uint64_t MAX_NR_PRODUCER = 50;
// 测试性能，需要调参
constexpr uint64_t QBITS = 18;
// 测试正确性
// constexpr uint64_t QBITS = 4;
constexpr uint64_t QSIZE = 1 << QBITS;
constexpr uint64_t QMASK = QSIZE - 1;

// OPT8
struct alignas(64) cache_aligned_uint64 {
    uint64_t value;
};

template<typename T>
class MPSCQueue {
public:
    MPSCQueue() 
        : head(0), last_head(0), tail(0) {
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            // thread_heads[i].value = UINT64_MAX;
            thread_heads[i]= UINT64_MAX;
            producer_yield_cnts[i].value = 0;
        }
        consumer_yield_cnt.value = 0;
    }

    void push(const T *new_data) {
        this_thread_head() = head.load(std::memory_order_consume);
        this_thread_head() = head.fetch_add(1, std::memory_order_acq_rel);
        
        while (unlikely(tail + QSIZE <= nv_this_thread_head())) {
            // printf("Producer %d yield at %lu tail = %lu, last_head = %lu\n", producer_id, nv_this_thread_head(), tail, last_head);
            yield_producer_thread();
        }

        std::atomic_thread_fence(std::memory_order_acquire);
        
        memcpy(&data[nv_this_thread_head() & QMASK], new_data, sizeof(T));

        std::atomic_thread_fence(std::memory_order_release);

        this_thread_head() = UINT64_MAX;
    }

    void pop(void (*read_func)(const T *data_ptr, uint64_t pop_cnt), uint64_t max_pop_cnt) {
        /* 
            因为last_head只有consumer会更新，而在发生tail >= last_head前consumer也不会尝试去更新，
            因此第一次发生tail >= last_head时不应该直接yield，而应该尝试更新
        */
        if (unlikely(tail >= last_head)) {
            update_last_head();
        }
        
        /*
            如果更新后仍然不满足，直接yield并进入loop
        */
        while (unlikely(tail >= last_head)) {
            // printf("Consumer %d yield at tail = %lu, last_head = %lu\n", consumer_id, tail, last_head);
            yield_consumer_thread();

            update_last_head();
        }

        std::atomic_thread_fence(std::memory_order_acquire);
        
        uint64_t ready_cnt = last_head - tail;
        uint64_t pop_cnt = ready_cnt > max_pop_cnt ? max_pop_cnt : ready_cnt;

        uint64_t read_start = tail & QMASK;
        uint64_t first_batch_cnt = QSIZE - read_start;
        
        if (first_batch_cnt > pop_cnt) {
            read_func(&data[read_start], pop_cnt);    
        } else {
            read_func(&data[read_start], first_batch_cnt);
            read_func(&data[0], pop_cnt - first_batch_cnt);
        }

        std::atomic_thread_fence(std::memory_order_release);

        tail += pop_cnt;        
    }

    cache_aligned_uint64 producer_yield_cnts[MAX_NR_PRODUCER];
    cache_aligned_uint64 consumer_yield_cnt;

private:
    volatile uint64_t &this_thread_head() {
        // return thread_heads[producer_id].value;
        return thread_heads[producer_id];
    }

    uint64_t nv_this_thread_head() {
        // return ((cache_aligned_uint64 const *)thread_heads)[producer_id].value;
        return ((uint64_t const *)thread_heads)[producer_id];
    }
    
    void update_last_head() {
        auto min = head.load(std::memory_order_consume);
        // auto min = head.load();
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            // auto tmp_head = thread_heads[i].value;
            auto tmp_head = thread_heads[i];

            // OPT4
            std::atomic_thread_fence(std::memory_order_consume);
            // std::atomic_thread_fence(std::memory_order_seq_cst);

            if (tmp_head < min) {
                min = tmp_head;
            }
        }

        /*
            多个consumer可能会同时更新，导致last_head并不是最新的，但无妨，
            因为只要能通过while条件就不影响写入，而通不过last_head条件也只是再来一次
        */
        last_head = min;
    }

    void yield_producer_thread() {
        // usleep(1);
        producer_yield_cnts[producer_id].value++;
        // 需要调参
        nice(1);
        sched_yield();
        // usleep(1);
    }

    void yield_consumer_thread() {
        consumer_yield_cnt.value++;
        nice(2);
        sched_yield();
        // usleep(2);
    }

    std::atomic<uint64_t> head;
    // volatile cache_aligned_uint64 thread_heads[MAX_NR_PRODUCER];
    volatile uint64_t thread_heads[MAX_NR_PRODUCER];

    volatile uint64_t tail;

    // volatile cache_aligned_uint64 thread_tails[MAX_NR_CONSUMER];
    uint64_t last_head;

    T data[QSIZE];
};