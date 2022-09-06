#ifndef LOG_H
#define LOG_H
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <libpmem.h>
#include <sched.h>
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

  CircularFifo(const std::string &filename, Data *data) : _tail(0), _head(0), done_count(0){
    char *map_ptr = reinterpret_cast<char *>(map_file(filename.c_str(), Capacity * sizeof(User) + 64 + Capacity));
    _head = reinterpret_cast<std::atomic<size_t> *>(map_ptr);
    _tail = reinterpret_cast<std::atomic<size_t> *>(map_ptr + 8);
    _count = reinterpret_cast<std::atomic<size_t> *>(map_ptr + 16);
    is_readable = reinterpret_cast<std::atomic<uint8_t> *>(map_ptr + 64);
    _array = reinterpret_cast<User *>(map_ptr + 64 + Capacity);
    this->data = data;
    this->pmem_users = data->get_pmem_users();
    memset(cmpa, 1, 30);
    tail_commit(); // 把上一次退出没提交完的提交完
    
    exited = false;
    auto write_task = [&] {
      while (!exited) {
        while (done_count == 0 && !exited) {
          std::this_thread::yield();
        }
        if (exited)
          break;
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
    munmap((void *)_head, Capacity * sizeof(User) + 64 + Capacity);
  }

  // 如果index > *_head，说明还没有刷下去，这个时候可以从索引里面读
  // TODO: 读着读者被刷走了怎么办呢，再加一个volatile表示不要被刷走
  // 两个offset转换好像有原子性的问题，需要自己给定一个唯一的转化函数,要给索引一个index
  size_t push(const User& item)
  {
    size_t current_tail = _tail->fetch_add(1, std::memory_order_acquire);
    while (current_tail - _head->load(std::memory_order_acquire) >= Capacity) {
      sched_yield();
    }

    // 这里可以不用原子指令吗，不希望is_readable设置期间发生调度
    _array[current_tail % Capacity] = item;
    is_readable[current_tail % Capacity] = 1;
    // 有什么办法能确保这30个全部写完了呢
    if ((current_tail + 1) % 30 == 0)
      done_count.fetch_add(1);
    return current_tail + 1;
  }

  // 在pop就不要任何检查了
  void pop()
  {
    size_t index = _head->load(std::memory_order_acquire);
    // 等30个全部写完
    while (memcmp((void *)&is_readable[index % Capacity], cmpa, 30)) {
      sched_yield();
    }

    pmem_memcpy_persist(pmem_users + index, _array + index % Capacity, sizeof(User) * 30);
    memset((void *)&is_readable[index % Capacity], 0, 30);
    // pmem的next_free好像没用了，但是还是要考虑一下中断之后写的情况
    _head->fetch_add(30);
    done_count.fetch_sub(1);
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
  std::atomic<size_t> *_count;
  std::atomic<uint8_t> *is_readable;
  uint8_t cmpa[30];
  User *_array;
  std::atomic<size_t> done_count; // 这个需要放文件里面吗，感觉好像有问题
  Data *data;
  User *pmem_users;
  volatile bool exited;
  std::thread *writer_thread;
public:
};
#endif

