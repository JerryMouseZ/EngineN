#pragma once
#include "util.hpp"
#include "data.hpp"
#include "thread_id.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>

// 测试性能，需要调参
constexpr uint64_t SQBITS = 14;
// 测试正确性
// constexpr uint64_t QBITS = 10;
constexpr uint64_t SQSIZE = 1 << SQBITS;
constexpr uint64_t SQMASK = SQSIZE - 1;

struct sync_send {
  int64_t cnt;
};

struct sync_resp {
  int64_t cnt;
};


struct RemoteUser {
  int64_t id;
  int64_t salary;
};

class SyncQueue {
public:
  SyncQueue() 
    : head(0), last_head(0), tail(0), send_head(0), neighbor_local_cnt{0} {
      data = reinterpret_cast<RemoteUser *>(map_anonymouse(SQSIZE * sizeof(RemoteUser)));
      exited = false;
      pthread_mutex_init(&mutex, NULL);
      pthread_cond_init(&cond, NULL);
      for (int i = 0; i < MAX_NR_PRODUCER; i++) {
        thread_heads[i].value = UINT64_MAX;
      }
    }
  ~SyncQueue() {
    munmap(data, SQSIZE * sizeof(RemoteUser));
  }

  int open(uint32_t id, int send_fd, volatile bool *alive) {

    data = reinterpret_cast<RemoteUser *>(map_anonymouse(SQSIZE * sizeof(RemoteUser)));

    if (data == nullptr) {
      return 1;
    }

    this->id = id;
    this->send_fd = send_fd;
    this->alive = alive;
    return 0;
  }

  uint64_t push(const User *user) {
    this_thread_head() = head.load(std::memory_order_consume);

    size_t pos = head.fetch_add(1, std::memory_order_acquire);

    this_thread_head() = pos;
    
    while (tail + SQSIZE <= pos) {
      sched_yield();
    }

    data[pos % SQSIZE].id = user->id;
    data[pos % SQSIZE].salary = user->salary;

    std::atomic_thread_fence(std::memory_order_release);
    this_thread_head() = UINT64_MAX;

    DEBUG_PRINTF(VLOG, "push to send queue\n");
    try_wake_consumer();
    return pos;
  }

  // 或许可以用uvsend，不然用的线程好像太多了
  int pop(RemoteUser **begin) {
    size_t pos = tail;

    update_last_head();
    
    size_t pop_cnt = last_head - pos;
    pop_cnt = std::min(pop_cnt, (size_t)2048); // 64k is the best package size
    pop_cnt = std::min(pop_cnt, SQSIZE - pos % SQSIZE); // 不要超过末尾了
    *begin = &data[pos % SQSIZE];
    tail += pop_cnt;
    return pop_cnt;
  }


  void consumer_yield() {
    pthread_mutex_lock(&mutex);
    consumer_maybe_waiting = true;
    pthread_cond_wait(&cond, &mutex);
  }


  void try_wake_consumer() {
    if (unlikely(consumer_maybe_waiting)) {
      pthread_mutex_lock(&mutex);
      if (consumer_maybe_waiting)
        pthread_cond_signal(&cond);
      pthread_mutex_unlock(&mutex);
    }
  }

  void notify_consumer_exit() {
    exited = true;
    pthread_mutex_lock(&mutex);
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
  }


  volatile uint64_t &this_thread_head() {
    return thread_heads[producer_id].value;
  }

  void update_last_head() {
    // 等于min也会有问题，有可能是最后一个没有写
    auto min = head.load(std::memory_order_consume);

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
      last_head = min;
    } while(last_head < min);
  }

  uint64_t get_free_cnt() {
    return SQSIZE - (head - tail);
  }

  uint64_t get_head() {
    return head;
  }

  // 即便是在rollback，tail也可以由resp负责更新
  void sync_to(uint64_t pos) {
    head = send_head = pos;
    for (int i = 0; i < 4; i++) {
      neighbor_local_cnt[i] = pos;
    }
  }

  void copy_to(const User *user, uint64_t pos) {
    uint64_t real_pos = pos & SQMASK;
    data[real_pos].id = user->id;
    data[real_pos].salary = user->salary;
  }

  void update_head(uint64_t new_head) {
    ((std::atomic<uint64_t> *)(&head))->store(new_head, std::memory_order_release);
  }

  /* void do_blocking_sync(uint64_t cnt) { */
  /*   if (unlikely(!*alive)) { */
  /*     return; */
  /*   } */ 

  /*   int ret; */
  /*   sync_send msg; */
  /*   msg.cnt = cnt; */

  /*   ret = send_all(send_fd, &msg, sizeof(msg), 0); */
  /*   if (ret < 0) { */
  /*     *alive = false; */
  /*   } */
  /*   assert(ret == sizeof(msg)); */

  /*   ret = send_all(send_fd, &data[send_head], cnt * sizeof(RemoteUser), 0); */
  /*   if (ret < 0) { */
  /*     *alive = false; */
  /*   } */
  /*   assert(ret == cnt * sizeof(RemoteUser)); */

  /*   send_head += cnt; */
  /* } */

  void advance_tail(uint64_t pos) {
    assert(pos >= tail);
    if (pos > tail) {
      std::atomic_thread_fence(std::memory_order_release);
      tail = pos;
    }
  }

  /* void on_recv_resp(const sync_resp *msg) { */
  /*   tail += msg->cnt; */
  /* } */

public:
  volatile cache_aligned_uint64 thread_heads[MAX_NR_PRODUCER];
  volatile uint64_t last_head;
  std::atomic<uint64_t> head;
  volatile uint64_t tail;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  // 单线程处理resp所以不用volatile
  uint32_t neighbor_local_cnt[4];
  uint64_t send_head;
  RemoteUser *data;
  uint32_t id;
  volatile bool *alive;
  volatile bool consumer_maybe_waiting;

  int send_fd;
  bool exited;
};
