#ifndef LOG_H
#define LOG_H
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <libpmem.h>
#include <sys/mman.h>
#include <thread>
#include "data.hpp"

class CircularFifo{
public:
  enum {Capacity = 30 * 128 * 1024};

  void tail_commit() {
    int count = (_tail->load(std::memory_order_acquire) - _head->load(std::memory_order_acquire)) / 30;
    for (int i = 0; i < count; ++i)
      pop();
    for (size_t i = *_head; i < _tail->load(std::memory_order_acquire); ++i) {
      pmem_memcpy_persist(pmem_users + i, _array + i % Capacity, sizeof(User));
    }
    *_head = _tail->load(std::memory_order_acquire);
  }

  CircularFifo(const std::string &filename, Data *data) : _tail(0), _head(0), pop_count(0){
    char *map_ptr = reinterpret_cast<char *>(map_file(filename.c_str(), Capacity * sizeof(User) + 64));
    _head = reinterpret_cast<std::atomic<size_t> *>(map_ptr);
    _tail = reinterpret_cast<std::atomic<size_t> *>(map_ptr + 8);
    _array = reinterpret_cast<User *>(map_ptr + 64);
    this->data = data;
    this->pmem_users = data->get_pmem_users();

    tail_commit(); // 把上一次退出没提交完的提交完
    
    exited = false;
    auto write_task = [&] {
      while (!exited) {
        while (pop_count == 0) {
          std::this_thread::yield();
          if (exited)
            break;
        }
        pop();
      }
    };
    writer_thread = new std::thread(write_task);
  }
  
  ~CircularFifo() {
    exited = true;
    writer_thread->join();
    // tail commit
    tail_commit();
    munmap((void *)_head, Capacity * sizeof(User) + 64);
  }

  // 如果index > *_head，说明还没有刷下去，这个时候可以从索引里面读
  // TODO: 读着读者被刷走了怎么办呢，再加一个volatile表示不要被刷走
  // 两个offset转换好像有原子性的问题，需要自己给定一个唯一的转化函数,要给索引一个index
  size_t push(const User& item)
  {
    const auto current_tail = _tail->fetch_add(1, std::memory_order_acquire);
    while(current_tail - _head->load(std::memory_order_acquire) >= Capacity);
    if ((current_tail + 1) % 30 == 0)
      pop_count.fetch_add(1);
    _array[current_tail % Capacity] = item;
    return current_tail + 1;
  }

  // 在pop就不要任何检查了
  void pop()
  {
    size_t index = _head->load(std::memory_order_acquire);
    pmem_memcpy_persist(pmem_users + index, _array + index % Capacity, sizeof(User) * 30);
    // pmem的next_free好像没用了，但是还是要考虑一下中断之后写的情况
    _head->fetch_add(30);
    pop_count.fetch_sub(1);
  }

  const User *read(uint32_t index) {
    if (!data->get_flag(index))
      return nullptr;
    index -= 1;
    if (index >= *_head) 
      return _array + index % Capacity;
    return data->data_read(index + 1);
  }

private:
  std::atomic<size_t>  *_tail; // 当next_location用就好了
  std::atomic<size_t> *_head;
  User *_array;
  std::atomic<size_t> pop_count; // 这个需要放文件里面吗，感觉好像有问题
  Data *data;
  User *pmem_users;
  volatile bool exited;
  std::thread *writer_thread;
public:
};
#endif

