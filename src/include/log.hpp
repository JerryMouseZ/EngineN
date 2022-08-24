#ifndef LOG_H
#define LOG_H
#include <atomic>
#include <libpmem.h>
#include "data.hpp"

class CircularFifo{
public:
  enum {Capacity = 30 * 64};

  CircularFifo() : _tail(0), _head(0), _tmp_tail(0), pop_count(0){
    /* fprintf(stderr, "%d\n", Capacity); */
  }
  virtual ~CircularFifo() {}

  void push(const User& item)
  {
    const auto current_tail = _tail.fetch_add(1, std::memory_order_acquire);
    while(current_tail - _head.load(std::memory_order_acquire) >= Capacity);
    if ((current_tail + 1) % 30 == 0)
      pop_count.fetch_add(1);
    _array[current_tail % Capacity] = item;
  }

  // 在pop就不要任何检查了
  void pop(int num, void *res)
  {
    pmem_memcpy_persist(res, _array + _head % Capacity, sizeof(User) * num);
    _head += num;
    pop_count.fetch_sub(1);
  }

private:
  std::atomic<size_t>  _tail;
  std::atomic<size_t> _tmp_tail;
  std::atomic<size_t>  _head;
  User              _array[Capacity];
public:
  std::atomic<size_t> pop_count;
};

#endif

