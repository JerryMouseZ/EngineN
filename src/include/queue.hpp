#pragma once

#include <memory.h>
#include <sched.h>
#include <unistd.h>
#include <libpmem.h>

#include "thread_id.hpp"
#include "commit_array.hpp"
#include "util.hpp"
#include "time.hpp"

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
    static constexpr uint64_t UNALIGNED_META_SIZE = 3 * 64 + MAX_NR_PRODUCER * 64;
    static constexpr uint64_t QMETA_SIZE = ROUND_UP(UNALIGNED_META_SIZE, CMT_ALIGN);

    LocklessQueue() 
        : producers_exit(false), consumer_sleep_elapse(new duration_t(0)) {

        pthread_mutex_init(&mtx, NULL);
        pthread_cond_init(&cond_var, NULL);

        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            producer_yield_cnts[i].value = 0;
        }
        consumer_yield_cnt.value = 0;

	    first = false;
        consumer_maybe_waiting = false;
    }

    int open(std::string fname, bool *is_new_create, DataArray *pmem_data, uint32_t id) {

        char *map_ptr = reinterpret_cast<char *>(map_file(fname.c_str(), QMETA_SIZE + QSIZE * sizeof(DataArray), is_new_create));
        
        if (map_ptr == nullptr) {
            return 1;
        }

        head = reinterpret_cast<std::atomic<uint64_t> *>(map_ptr);
        thread_heads = reinterpret_cast<volatile cache_aligned_uint64 *>(map_ptr + 64);
        tail = (volatile uint64_t *)(thread_heads + MAX_NR_PRODUCER); // cast away volatile qualifier
        last_head = reinterpret_cast<volatile uint64_t *>(((char *)tail) + 64);

        data = (DataArray *)(map_ptr + QMETA_SIZE);
        this->pmem_data = pmem_data;
        this->id = id;

        DEBUG_PRINTF(QINFO, "Open queue %u: tail = %lu, last_head = %lu, head = %lu\n", id, *tail, *last_head, head->load());

        return 0;
    }

    uint64_t push(const T *new_data) {
        this_thread_head() = head->load(std::memory_order_consume);

        uint64_t pos = head->fetch_add(1, std::memory_order_acq_rel);

        this_thread_head() = pos;

        uint64_t caid = pos / CMT_BATCH_CNT;
        uint64_t inner_ca_pos = pos % CMT_BATCH_CNT;
        uint64_t ca_pos = caid & QMASK;
        
        while (unlikely(*tail + QSIZE <= pos)) {
            // printf("Producer %d yield at %lu tail = %lu, last_head = %lu, head = %lu\n", producer_id, nv_this_thread_head(), *tail, *last_head, head->load());
            producer_yield_thread();
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

        uint64_t pos = *tail;

        uint64_t caid = pos / CMT_BATCH_CNT;
        uint64_t ca_pos = caid & QMASK;
        
        while (pos + CMT_BATCH_CNT - 1 >= *last_head) {

            // exit condition
            if (unlikely(producers_exit)) {
                return false;
            }

            // printf("Consumer %d yield at tail = %lu, last_head = %lu, head = %lu\n", consumer_id, *tail, *last_head, head->load());
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

        *tail = pos + CMT_BATCH_CNT;

        return true;
    }

    void do_commit(const DataArray *src, uint64_t ca_index) {
        pmem_memcpy_persist(&pmem_data[ca_index], src, CMT_ALIGN);
    }

    void do_unaligned_commit(const DataArray *src, uint64_t ca_index, uint64_t pop_cnt) {
        pmem_memcpy_persist(&pmem_data[ca_index], src, pop_cnt * sizeof(T));
    }

    void notify_producers_exit() {
        producers_exit = true;
        pthread_mutex_lock(&mtx);
        pthread_cond_signal(&cond_var);
        pthread_mutex_unlock(&mtx);
    }

    void tail_commit() {
        uint64_t tail_value = *tail;
        uint64_t head_value = head->load(std::memory_order_relaxed);

        DEBUG_PRINTF(QDEBUG, "Queue %u tail_commit: Start at tail = %lu, head = %lu\n", id, tail_value, head_value);

        if (tail_value == head_value) {
            DEBUG_PRINTF(QINFO, "Queue %u tail_commit: No thing to commit, tail > head: tail = %lu, head: %lu\n", id, tail_value, head_value);
            return;
        } else if (tail_value > head_value) {
            DEBUG_PRINTF(QINFO, "Queue %u tail_commit: Wierd, last_tail > head: tail = %lu, head: %lu\n", id, tail_value, head_value);
            return;
        }

        if (tail_value % CMT_BATCH_CNT != 0) {
            DEBUG_PRINTF(QINFO, "Queue %u tail_commit: Wierd, tail not aligned to CMT_CNT: tail = %lu\n", id, tail_value);
        }

        uint64_t start_caid = tail_value / CMT_BATCH_CNT;
        uint64_t end_caid = head_value / CMT_BATCH_CNT;
        uint64_t end_ca_cnt = head_value % CMT_BATCH_CNT;

        uint64_t start_ca_pos = start_caid & QMASK;
        uint64_t end_ca_pos = end_caid & QMASK;

        DEBUG_PRINTF(QINFO, "Queue %u tail_commit: Start qpos: %lu, End qpos: %lu, nonfull_cnt = %lu, QSIZE = %lu\n", id, start_ca_pos, end_ca_pos, end_ca_cnt, QSIZE);

        if (start_ca_pos <= end_ca_pos) {
            DEBUG_PRINTF(QDEBUG, "No wind\n");
            for (auto i = 0; i < end_ca_pos - start_ca_pos; i++) {
                do_commit(&data[start_ca_pos + i], start_caid + i);
            }
        } else {
            DEBUG_PRINTF(QDEBUG, "Wind\n");
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
        *tail = head_value;

        DEBUG_PRINTF(QDEBUG, "Queue %u: End tail commit\n", id);
    }

    uint64_t min_uncommitted_data_index() {
        return *tail;
    }

    void pop_at(T *dst, uint64_t index) {
        memcpy(dst, &data[index & QMASK], sizeof(T));
    }

    void reset_thread_states() {
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            thread_heads[i].value = UINT64_MAX;
        }
    }

    bool need_rollback() {
        return *tail != head->load();
    }

    void statistics(uint32_t qid) {
        uint64_t total_producer_yield_cnts = 0;
        
        for (int i = 0; i < MAX_NR_PRODUCER; i++) {
            total_producer_yield_cnts += producer_yield_cnts[i].value;
        }

        DEBUG_PRINTF(QINFO,
                "Queue %u:\n"
                "\tTotal yield:\n"
                "\t\tproducer: %lu\n"
                "\tAvg yield:\n"
                "\t\tproducer: %lu\n"
                "\t\tconsumer: %lu\n"
                "\tConsumer sleep total elapse: %lfms\n\n",
                qid,
                total_producer_yield_cnts, 
                total_producer_yield_cnts / MAX_NR_PRODUCER,
                consumer_yield_cnt.value,
                consumer_sleep_elapse->count() * 1000);
    }

    ~LocklessQueue() {
        delete consumer_sleep_elapse;
    }

private:
    volatile uint64_t &this_thread_head() {
        return thread_heads[producer_id].value;
    }

    uint64_t nv_this_thread_head() {
        return ((cache_aligned_uint64 *)thread_heads)[producer_id].value;
    }

    void producer_yield_thread() {
        pthread_mutex_lock(&mtx);
	
        if (!first) {
            first = true;
                pthread_cond_signal(&cond_var);
            first = false;
        } else {
            sched_yield();
        }

        pthread_mutex_unlock(&mtx);

        producer_yield_cnts[producer_id].value++;
    }

    void try_wake_consumer() {
        if (unlikely(consumer_maybe_waiting)) {
            pthread_mutex_lock(&mtx);
            pthread_cond_signal(&cond_var);
            pthread_mutex_unlock(&mtx);
        }
    }

    void consumer_yield_thread() {
        pthread_mutex_lock(&mtx);

        start_time_record();

        consumer_maybe_waiting = true;
        pthread_cond_wait(&cond_var, &mtx);
        consumer_maybe_waiting = false;

        end_time_record(consumer_sleep_elapse);

        pthread_mutex_unlock(&mtx);
        consumer_yield_cnt.value++;
    }

public:
    /* first cacheline, almost read-only data */
    std::atomic<uint64_t> *head;
    volatile cache_aligned_uint64 *thread_heads;
    volatile uint64_t *tail;
    volatile uint64_t *last_head;
    DataArray *data;
    DataArray *pmem_data;
    duration_t *consumer_sleep_elapse;    
    uint32_t id;
    volatile bool producers_exit;
    char pad1[3];
    /* cacheline end */

    /* per-thread write data, cache-aligned */
    cache_aligned_uint64 producer_yield_cnts[MAX_NR_PRODUCER];
    cache_aligned_uint64 consumer_yield_cnt;

    /* cacheline start */
    pthread_mutex_t mtx;
    pthread_cond_t cond_var;
    volatile bool first;
    volatile bool consumer_maybe_waiting;
} __attribute__((packed)) ;