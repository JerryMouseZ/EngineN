#include "include/engine.hpp"
#include "include/wb_cache.hpp"
#include "include/global.hpp"

cpu_set_t all_core_cpu_set;

bool operator==(const UserString &l, const UserString &r)
{
  return std::string(l.ptr) == std::string(r.ptr);
}

void *res_copy(const User *user, void *res, int32_t select_column) {
  switch(select_column) {
  case UserColumn::Id: 
    memcpy(res, &user->id, 8); 
    res = (char *)res + 8; 
    break;
  case UserColumn::Userid: 
    memcpy(res, user->user_id, 128); 
    res = (char *)res + 128; 
    break;
  case UserColumn::Name: 
    memcpy(res, user->name, 128); 
    res = (char *)res + 128; 
    break; 
  case UserColumn::Salary: 
    memcpy(res, &user->salary, 8); 
    res = (char *)res + 8; 
    break;
  default: break; // wrong
  }
  return res;
}

void Engine::write(const User *user) {
  if (unlikely(!wbc_dirty.load(std::memory_order_acquire))) {
    wbc_dirty.store(true, std::memory_order_release);
  }

  if (unlikely(writer_id < 0)) {
    writer_id = next_writer_id++;
    writer_cpu_id = writer_id % NR_WRITER_CPU;
    committer_id = get_commiter_id(writer_id);
    fprintf(stderr, "Writer %ld is bound to cpu %ld\n", writer_id, writer_cpu_id);
    set_thread_affinity(writer_cpu_id);
  }
  
  wbc.write(user);
  // fprintf(stderr, "write %ld %ld %ld %ld done\n", user->id, std::hash<std::string>{}(user->user_id), std::hash<std::string>{}(user->name), user->salary);
}