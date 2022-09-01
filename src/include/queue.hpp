#include <memory.h>
#include <sched.h>

#include "thread_id.hpp"

// using T = int;

constexpr uint64_t MAX_NR_PRODUCER = 8;
constexpr uint64_t MAX_NR_CONSUMER = 8;
constexpr uint64_t QBITS = 8;
constexpr uint64_t QZIEE = 1 << QBITS;
constexpr uint64_t QMASK = QZIEE - 1;

template<typename T>
class LocklessQueue {
public:
    void push(const T *new_data) {
        this_thread_head() = head.load(std::memory_order_consume);
        this_thread_head() = head.fetch_add(1, std::memory_order_acq_rel);
        
        while (last_tail + QZIEE <= nv_this_thread_head()) {
            sched_yield();

            auto min = tail.load(std::memory_order_consume);
            for (int i = 0; i < MAX_NR_CONSUMER; i++) {
                auto tmp_tail = thread_tails[i];

                std::atomic_thread_fence(std::memory_order_consume);

                if (tmp_tail < min) {
                    min = tmp_tail;
                }
            }

            /*
                多个producer可能会同时更新，导致last_tail并不是最新的，但无妨，
                因为只要能通过while条件就不影响写入，而通不过last_tail条件也只是再来一次
            */
            last_tail = min;

            // do {
            //     last_tail = min;
            // } while(last_tail > min);
        }
        
        
        memcpy(&data[nv_this_thread_head() & QMASK], new_data, sizeof(T));

        std::atomic_thread_fence(std::memory_order_release);
        this_thread_head() = UINT64_MAX;
    }

    void pop(T *data_ptr) {
        this_thread_tail() = tail.load(std::memory_order_consume);
        this_thread_tail() = tail.fetch_add(1, std::memory_order_acq_rel);
        
        while (nv_this_thread_tail() >= last_head) {
            sched_yield();

            auto min = head.load(std::memory_order_consume);
            for (int i = 0; i < MAX_NR_PRODUCER; i++) {
                auto tmp_head = thread_heads[i];

                std::atomic_thread_fence(std::memory_order_consume);

                if (tmp_head < min) {
                    min = tmp_head;
                }
            }

            /*
                多个consumer可能会同时更新，导致last_head并不是最新的，但无妨，
                因为只要能通过while条件就不影响写入，而通不过last_head条件也只是再来一次
            */
            last_head = min;

            // do {
            //     last_head = min;
            // } while(last_head > min);
        }
        
        
        memcpy(data_ptr, &data[nv_this_thread_tail() & QMASK], sizeof(T));

        std::atomic_thread_fence(std::memory_order_release);
        this_thread_tail() = UINT64_MAX;
    }

private:
    volatile uint64_t &this_thread_head() {
        return thread_heads[producer_id];
    }

    uint64_t nv_this_thread_head() {
        return ((uint64_t const *)thread_heads)[producer_id];
    }

    volatile uint64_t &this_thread_tail() {
        return thread_tails[consumer_id];
    }

    uint64_t nv_this_thread_tail() {
        return ((uint64_t const *)thread_tails)[consumer_id];
    }

    std::atomic<uint64_t> head;
    volatile uint64_t thread_heads[MAX_NR_PRODUCER];
    volatile uint64_t last_tail;

    std::atomic<uint64_t> tail;
    volatile uint64_t thread_tails[MAX_NR_CONSUMER];
    volatile uint64_t last_head;

    T data[QZIEE];
};