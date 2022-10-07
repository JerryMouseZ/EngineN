#include "include/data.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>

bool UserString::operator==(const UserString &other) {
  return memcmp(ptr, other.ptr, 128) == 0;
}

size_t std::hash<UserString>::operator()(const UserString& k) const
{
  return (hash<string>()(string(k.ptr, 128)));
}

Data::Data() {}
Data::~Data() {
  pmem_unmap(pmem_ptr, EACH_DATA_FILE_LEN);
}

void Data::open(const std::string &fdata) {
  size_t map_len;
  int is_pmem;
  bool new_create = false;

  if (access(fdata.c_str(), F_OK)) {
    new_create = true;
  }

  pmem_ptr = reinterpret_cast<char *>(pmem_map_file(fdata.c_str(), EACH_DATA_FILE_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
  DEBUG_PRINTF(pmem_ptr, "%s open mmaped failed", fdata.c_str());
  pmem_users = (UserArray *)pmem_ptr;

  prefault(pmem_ptr, EACH_DATA_FILE_LEN, false);
}

// data read and data write
const User* Data::data_read(uint32_t index) {
  // 让log来检查，这里就不重复检查了
  uint64_t ca_pos = index / UserArray::N_DATA;
  uint64_t inner_ca_pos = index % UserArray::N_DATA;
  return &pmem_users[ca_pos].data[inner_ca_pos];
}

UserArray* Data::get_pmem_users() {
  return pmem_users;
}


DataMap::DataMap() {
    value_map = (uint8_t *)map_anonymouse(map_size);
}


void DataMap::put(int64_t value) {
  if (value > INT32_MAX || value < INT32_MIN)
    return;
  uint32_t val = value;
  int index = val >> 3;
  uint8_t iner_bit = 1 << (index & 7);
  uint8_t old = value_map[index];
  uint8_t update = old | iner_bit;
  while (!__sync_bool_compare_and_swap(&value_map[index], old, update)) {
    old = value_map[index];
    update = old | iner_bit;
  }
}

uint8_t DataMap::get(int64_t value) {
  if (value > INT32_MAX || value < INT32_MIN)
    return 1;
  uint32_t val = value;
  int index = val >> 3;
  uint8_t iner_bit = 1 << (index & 7);
  uint8_t old = value_map[index];
  return old & iner_bit;
}
