#pragma once
#include "util.hpp"
#include "data.hpp"
#include <cstddef>
#include <cstdint>
#include <pthread.h>
#include <sched.h>

// 测试性能，需要调参
constexpr uint64_t SQBITS = 14;
// 测试正确性
// constexpr uint64_t QBITS = 10;
constexpr uint64_t SQSIZE = 1 << SQBITS;
constexpr uint64_t SQMASK = SQSIZE - 1;

struct sync_send {
  size_t cnt;
};

struct sync_resp {
  size_t cnt;
};


struct RemoteUser {
  int64_t id;
  int64_t salary;
};

class SyncQueue {
public:
  SyncQueue() 
    : head(0), tail(0), send_head(0), neighbor_local_cnt{0} {
      data = reinterpret_cast<RemoteUser *>(map_anonymouse(SQSIZE * sizeof(RemoteUser)));
      exited = false;
      pthread_mutex_init(&mutex, NULL);
      pthread_cond_init(&cond, NULL);
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
    size_t pos = head;
    while (tail + SQSIZE <= pos) {
      sched_yield();
    }

    data[pos].id = user->id;
    data[pos].salary = user->salary;
    ++head;
    DEBUG_PRINTF(VLOG, "push to send queue\n");
    try_wake_consumer();
    return pos;
  }

  // 或许可以用uvsend，不然用的线程好像太多了
  int pop(RemoteUser **begin) {
    size_t pos = tail;
    int pop_cnt = head - pos;
    pop_cnt = std::min(pop_cnt, 2048); // 64k is the best package size
    *begin = &data[pos];
    tail += pop_cnt;
    return pop_cnt;
  }


  void consumer_yield() {
    pthread_mutex_lock(&mutex);
    consumer_maybe_waiting = true;
    pthread_cond_wait(&cond, &mutex);
    consumer_maybe_waiting = false;
    pthread_mutex_unlock(&mutex);
  }


  void try_wake_consumer() {
    if (unlikely(consumer_maybe_waiting)) {
      pthread_mutex_lock(&mutex);
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
  volatile uint64_t head;
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
