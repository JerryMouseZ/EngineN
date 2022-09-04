#include <memory.h>
#include <sched.h>
#include <unistd.h>

#include "thread_id.hpp"

// using T = int;

constexpr uint64_t MAX_NR_PRODUCER = 50;
constexpr uint64_t MAX_NR_CONSUMER = 50;
// 测试性能，需要调参
// constexpr uint64_t QBITS = 18;
// 测试正确性
constexpr uint64_t QBITS = 4;
constexpr uint64_t QZIEE = 1 << QBITS;
constexpr uint64_t QMASK = QZIEE - 1;

// OPT7
struct alignas(64) cache_aligned_uint64 {
    uint64_t value;
};

template<typename T>
class LocklessQueue {
public:
    LocklessQueue() 
        : head(0), tail(0), last_head(0), last_tail(0) {
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            // thread_heads[i].value = UINT64_MAX;
            thread_heads[i]= UINT64_MAX;
        }
        for (int i = 0; i < MAX_NR_CONSUMER; i++) {
            // thread_tails[i].value = UINT64_MAX;
            thread_tails[i] = UINT64_MAX;
        }
    }

    void push(const T *new_data) {
        // OPT1
        this_thread_head() = head.load(std::memory_order_consume);
        this_thread_head() = head.fetch_add(1, std::memory_order_acq_rel);
        // this_thread_head() = head.load();
        // this_thread_head() = head.fetch_add(1);
        
        // OPT2
        // while (last_tail + QZIEE <= nv_this_thread_head()) {
        while (last_tail + QZIEE <= this_thread_head()) {
            printf("Producer %d yield at %lu last_tail = %lu, last_head = %lu\n", producer_id, nv_this_thread_head(), last_tail, last_head);
            yield_thread();

            // OPT3
            auto min = tail.load(std::memory_order_consume);
            // auto min = tail.load();
            for (int i = 0; i < MAX_NR_CONSUMER; i++) {
                // auto tmp_tail = thread_tails[i].value;
                auto tmp_tail = thread_tails[i];

                // OPT4
                std::atomic_thread_fence(std::memory_order_consume);
                // std::atomic_thread_fence(std::memory_order_seq_cst);

                if (tmp_tail < min) {
                    min = tmp_tail;
                }
            }

            /*
                多个producer可能会同时更新，导致last_tail并不是最新的，但无妨，
                因为只要能通过while条件就不影响写入，而通不过last_tail条件也只是再来一次
            */
            // last_tail = min;

            // OPT6
            /*
                可能发生多个线程同时更新，应当取它们每个人眼中min的最大值，否则可能导致
                较小的min被最后写入，导致有人的while判断从能通过变成不能通过，发生不必要的yield
            */
            do {
                last_tail = min;
            } while(last_tail < min);
        }
        
        
        // OPT2
        memcpy(&data[nv_this_thread_head() & QMASK], new_data, sizeof(T));
        // memcpy(&data[this_thread_head() & QMASK], new_data, sizeof(T));

        // OPT5
        std::atomic_thread_fence(std::memory_order_release);
        // std::atomic_thread_fence(std::memory_order_seq_cst);
        this_thread_head() = UINT64_MAX;
    }

    void pop(T *data_ptr) {
        // OPT1
        this_thread_tail() = tail.load(std::memory_order_consume);
        this_thread_tail() = tail.fetch_add(1, std::memory_order_acq_rel);
        // this_thread_tail() = tail.load();
        // this_thread_tail() = tail.fetch_add(1); 
        
        // OPT2
        while (nv_this_thread_tail() >= last_head) {
        // while (this_thread_tail() >= last_head) {
            printf("Consumer %d yield at %lu last_tail = %lu, last_head = %lu\n", consumer_id, nv_this_thread_tail(), last_tail, last_head);
            yield_thread();

            // OPT3
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
            // last_head = min;

            // OPT6
            /*
                可能发生多个线程同时更新，应当取它们每个人眼中min的最大值，否则可能导致
                较小的min被最后写入，导致有人的while判断从能通过变成不能通过，发生不必要的yield
            */
            do {
                last_head = min;
            } while(last_head < min);
        }
        
        
        // OPT2
        memcpy(data_ptr, &data[nv_this_thread_tail() & QMASK], sizeof(T));
        // memcpy(data_ptr, &data[this_thread_tail() & QMASK], sizeof(T));

        // OPT5
        std::atomic_thread_fence(std::memory_order_release);
        // std::atomic_thread_fence(std::memory_order_seq_cst);
        this_thread_tail() = UINT64_MAX;
    }

private:
    volatile uint64_t &this_thread_head() {
        // return thread_heads[producer_id].value;
        return thread_heads[producer_id];
    }

    uint64_t nv_this_thread_head() {
        // return ((cache_aligned_uint64 const *)thread_heads)[producer_id].value;
        return ((uint64_t const *)thread_heads)[producer_id];
    }

    volatile uint64_t &this_thread_tail() {
        // return thread_tails[consumer_id].value;
        return thread_tails[consumer_id];
    }

    uint64_t nv_this_thread_tail() {
        // return ((cache_aligned_uint64 const *)thread_tails)[consumer_id].value;
        return ((uint64_t const *)thread_tails)[consumer_id];
    }

    void yield_thread() {
        // sched_yield();
        // usleep(1);
        // 需要调参
        usleep(10);
    }

    std::atomic<uint64_t> head;
    // volatile cache_aligned_uint64 thread_heads[MAX_NR_PRODUCER];
    volatile uint64_t thread_heads[MAX_NR_PRODUCER];
    volatile uint64_t last_tail;

    std::atomic<uint64_t> tail;
    // volatile cache_aligned_uint64 thread_tails[MAX_NR_CONSUMER];
    volatile uint64_t thread_tails[MAX_NR_CONSUMER];
    volatile uint64_t last_head;

    T data[QZIEE];
};