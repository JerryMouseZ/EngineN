#pragma once

#include <cstring>
#include <memory.h>
#include <sched.h>
#include <unistd.h>
#include <libpmem.h>
#include <assert.h>
#include <pthread.h>
#include <string>
#include <set>

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

    return pos;
  }

  bool pop() {

    uint64_t pos = *tail;

    uint64_t caid = pos / CMT_BATCH_CNT;
    uint64_t inner_ca_pos = pos % CMT_BATCH_CNT;
    uint64_t ca_pos = caid & QMASK;

    uint64_t waiting_cnt = CMT_BATCH_CNT;

    if (unlikely(pos % CMT_BATCH_CNT != 0)) {
      waiting_cnt -= (pos % CMT_BATCH_CNT);
    }

    while (pos + waiting_cnt - 1 >= *last_head) {

      // exit condition
      if (unlikely(producers_exit)) {
        return false;
      }

      // printf("Consumer %d yield at tail = %lu, last_head = %lu, head = %lu\n", consumer_id, *tail, *last_head, head->load());
      consumer_yield_thread();

      update_last_head();
    }

    if (likely(waiting_cnt == CMT_BATCH_CNT)) {
      do_commit(&data[ca_pos], caid);    
    } else {
      pmem_memcpy_persist(&pmem_data[caid].data[inner_ca_pos], &data[ca_pos].data[inner_ca_pos], waiting_cnt * sizeof(T));
      /* do_unaligned_rest_commit(&data[ca_pos].data[inner_ca_pos], caid, DataArray::N_DATA - waiting_cnt, waiting_cnt); */
    }

    std::atomic_thread_fence(std::memory_order_release);

    *tail = pos + waiting_cnt;

    return true;
  }

  bool pop_nonexit() {
    uint64_t pos = *tail;
    uint64_t caid = pos / CMT_BATCH_CNT;
    uint64_t inner_ca_pos = pos % CMT_BATCH_CNT;
    uint64_t ca_pos = caid & QMASK;

    uint64_t waiting_cnt = CMT_BATCH_CNT;
    if (unlikely(pos % CMT_BATCH_CNT != 0)) {
      waiting_cnt -= (pos % CMT_BATCH_CNT);
    }

    if (likely(waiting_cnt == CMT_BATCH_CNT)) {
      do_commit(&data[ca_pos], caid);    
    } else {
      pmem_memcpy_persist(&pmem_data[caid].data[inner_ca_pos], &data[ca_pos].data[inner_ca_pos], waiting_cnt * sizeof(T));
    }
    std::atomic_thread_fence(std::memory_order_release);
    *tail = pos + waiting_cnt;
    return true;
  }


  void do_commit(const DataArray *src, uint64_t ca_index) {
    pmem_memcpy_persist(&pmem_data[ca_index], src, CMT_ALIGN);
  }

  void do_unaligned_commit(const DataArray *src, uint64_t ca_index, uint64_t pop_cnt) {
    pmem_memcpy_persist(&pmem_data[ca_index], src, pop_cnt * sizeof(T));
  }

  void do_unaligned_rest_commit(const DataArray *src, uint64_t ca_index, uint64_t start_pos, uint64_t pop_cnt) {
    pmem_memcpy_persist(&pmem_data[ca_index].data[start_pos], src, pop_cnt * sizeof(T));
  }

  void notify_producers_exit() {
    producers_exit = true;
    pthread_mutex_lock(&mtx);
    pthread_cond_signal(&cond_var);
    pthread_mutex_unlock(&mtx);
  }

  void tail_commit() {
    DEBUG_PRINTF(*last_head == head->load(), "Queue %u last_head(%lu) != head(%lu) before tail commit\n", id, *last_head, head->load());
    uint64_t tail_value = *tail;
    uint64_t head_value = head->load(std::memory_order_relaxed);
    if (head_value == tail_value)
      return;
    int can_pop_cnt = (head_value - tail_value) / CMT_BATCH_CNT;
    // 先把不对齐的部分给pop掉
    if (can_pop_cnt) {
      pop_nonexit();
    }
    
    // 更新一下tail，can_pop_cnt有可能会增加，因为第一次pop的数量不是一个batch，然后pop对齐的部分
    tail_value = *tail;
    can_pop_cnt = (head_value - tail_value) / CMT_BATCH_CNT;
    while (can_pop_cnt) {
      pop_nonexit();
      --can_pop_cnt;
    }
    
    // tail还是有可能不对齐，因为一次都没有pop，但是现在可以保证head和tail在同一个chunk了
    tail_value = *tail;
    uint64_t caid = tail_value / CMT_BATCH_CNT;
    uint64_t ca_pos = caid & QMASK;
    uint64_t inner_ca_pos = tail_value % CMT_BATCH_CNT;
    int count = (head_value - tail_value);
    if (count == 0) {
      assert(*tail == head_value);
      return;
    }
    pmem_memcpy_persist(&pmem_data[caid].data[inner_ca_pos], &data[ca_pos].data[inner_ca_pos], count * sizeof(T));
    std::atomic_thread_fence(std::memory_order_release);
    *tail = head_value;
    DEBUG_PRINTF(QDEBUG, "Queue %u: End tail commit\n", id);
  }

  void compact_head() {
    update_last_head();
    auto last_value = *last_head;
    auto head_value = head->load();

    std::set<int> hole_index;
    for (int tid = 0; tid < MAX_NR_PRODUCER; tid++) {
      uint64_t the_head = thread_heads[tid].value;
      if (the_head != UINT64_MAX) {
        hole_index.insert(the_head);
      }
    }

    if (hole_index.empty()) {
      return;
    }

    // 双指针，左边指当前的数据，右边指向可以填充的数据，因为第一个数据肯定是要被填充了，所以不用去找第一个left
    int hole_cnt = hole_index.size();
    int left = last_value, right = last_value;
    for (; left < head_value && hole_cnt > 0; ++left) { // 小于就行了，因为等于的时候也没有数据可以填充了
                                                        // 找到第一个不是洞的数据
      while (right <= head_value && hole_index.find(right) != hole_index.end()) {
        ++right;
      }
      if (right > head_value)
        break;
      user_copy(left, right);
      right++; // 把right设置为下一个数据，因为当前已经填充过了
    }
    head_value -= hole_cnt;
    head->store(head_value);

    reset_thread_states();
    update_last_head();
    // 确实可能不相等，比如最后一个数据就是hole
    DEBUG_PRINTF(*last_head >= head->load(), "Queue %u last_head(%lu) < head(%lu) after compact head\n", id, *last_head, head->load());
  }

  void user_copy(uint64_t vdst, uint64_t vsrc) {
    memcpy(user_at(vdst), user_at(vsrc), sizeof(T));
  }

  T *user_at(uint64_t v) {
    auto caid = v / CMT_BATCH_CNT;
    auto in_ca_pos = v % CMT_BATCH_CNT;
    auto ca_pos = caid & QMASK;
    return &data[ca_pos].data[in_ca_pos];
  }

  void update_last_head() {
    // 等于min也会有问题，有可能是最后一个没有写
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

  uint64_t min_uncommitted_data_index() {
    return *tail;
  }

  void pop_at(T *dst, uint64_t index) {
    uint64_t caid = index / CMT_BATCH_CNT;
    uint64_t in_ca_pos = index % CMT_BATCH_CNT;
    uint64_t ca_pos = caid & QMASK;
    memcpy(dst, &data[ca_pos].data[in_ca_pos], sizeof(T));
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
