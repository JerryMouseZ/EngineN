#ifndef SR_H
#define SR_H
#include "include/data.hpp"
#include "include/util.hpp"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <libpmem.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <thread>

template<class T, int Capacity>
class CircularFifo{
public:
  CircularFifo() : _tail(0), _head(0) {
    char *map_ptr = reinterpret_cast<char *>(map_anonymouse(Capacity * sizeof(T) + 64 + Capacity));
    _head = reinterpret_cast<std::atomic<uint64_t> *>(map_ptr);
    _tail = reinterpret_cast<std::atomic<uint64_t> *>(map_ptr + 8);
    _send = reinterpret_cast<std::atomic<uint64_t> *>(map_ptr + 16);
    is_readable = reinterpret_cast<volatile uint8_t *>(map_ptr + 64);
    _array = reinterpret_cast<T *>(map_ptr + 64 + Capacity);
    pthread_mutex_init(&mtx, NULL);
    pthread_cond_init(&cond_var, NULL);
    exited = false;
  }

  ~CircularFifo() {
    munmap((void *)_head, Capacity * sizeof(T) + 64 + Capacity);
  }

  size_t push(const T& item)
  {
    size_t current_tail = _tail->fetch_add(1, std::memory_order_acquire);
    while (current_tail - _head->load(std::memory_order_acquire) >= Capacity) {
      producer_yield_thread();
    }

    _array[current_tail % Capacity] = item;
    std::atomic_thread_fence(std::memory_order_release);
    is_readable[current_tail % Capacity] = 1;
    if (current_tail % (Capacity >> 5) == 0) {
      try_wake_consumer();
    }

    return current_tail + 1;
  }

  bool check_readable(int index, int num) {
    static const uint64_t value = 0x0101010101010101;
    uint64_t *base = (uint64_t *)(is_readable + (index % Capacity));
    // align部分用8比较
    for (int i = 0; i < num / 8; ++i) {
      if (base[i] != value)
        return true;
    }
    // unalign部分用1比较
    uint8_t *res = (uint8_t *)base;
    for (int i = (num / 8) * 8; i < num; ++i) {
      if (res[i])
        return false;
    }
    return true;
  }
  
  void exit() {
    exited = true;
    try_wake_consumer();
  }


  // 在pop就不要任何检查了
  T* prepare_send(int num)
  {
    size_t index = _send->load(std::memory_order_relaxed);
    while (index + num > _tail->load(std::memory_order_acquire)) {
      consumer_yield_thread();
      if (exited)
        return nullptr;
    }
    
    // 这里有边界条件，就是可能最后几个请求会等特别久，但是又没有tail_commit
    int sched_count = 0;
    // 等num个全部写完
    while (check_readable(index)) {
      sched_yield();
      sched_count++;
      if (sched_count > 100) {
        return nullptr;
      }
    }
    
    // 现在还不能invalid，要等到收到了数据以后
    /* memset((void *)&is_readable[index % Capacity], 0, num); */
    _send->store(index + num, std::memory_order_release);
    return _array[index % Capacity];
  }

  void invalidate(int index) {
    int real_index = (index - 1) % Capacity;
    is_readable[real_index] = 0;
  }

  bool check_pop() {
    bool res = false;
    size_t index = _head->load(std::memory_order_relaxed);
    while (!check_readable(index, 64)) {
      index += 64;
      res = true;
    }
    if (res)
      _head->store(index, std::memory_order_release);
    return res;
  }

  void producer_yield_thread() {
    // 如果找到空闲位置了就返回
    if (check_pop())
      return;
    pthread_mutex_lock(&mtx);
    if (!first) {
      first = true;
      pthread_cond_signal(&cond_var);
      first = false;
    } else {
      sched_yield();
    }
    pthread_mutex_unlock(&mtx);
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
    consumer_maybe_waiting = true;
    pthread_cond_wait(&cond_var, &mtx);
    consumer_maybe_waiting = false;
    pthread_mutex_unlock(&mtx);
  }


private:
  std::atomic<uint64_t>  *_tail; // 当next_location用就好了
  std::atomic<uint64_t> *_head;
  std::atomic<uint64_t> *_send;
  volatile uint8_t *is_readable;
  T *_array;
  volatile bool exited;

  pthread_mutex_t mtx;
  pthread_cond_t cond_var;
  volatile bool first;
  volatile bool consumer_maybe_waiting;
};
#endif
