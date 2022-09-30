#include "include/data.hpp"

bool UserString::operator==(const UserString &other) {
  return memcmp(ptr, other.ptr, 128) == 0;
}

size_t std::hash<UserString>::operator()(const UserString& k) const
{
  return (hash<string>()(string(k.ptr, 128)));
}

DataFlag::DataFlag() : ptr(nullptr) {}
DataFlag::~DataFlag() {
  if (ptr) {
    munmap((void *)ptr, EACH_NR_USER);
  }
}

void DataFlag::Open(const std::string &filename) {
  ptr = reinterpret_cast<volatile uint8_t *>(map_file(filename.c_str(), EACH_NR_USER, nullptr));
}

void DataFlag::set_flag(uint32_t index) {
  ptr[index] = 1;
}

bool DataFlag::get_flag(size_t index) {
  return ptr[index];
}

Data::Data() {}
Data::~Data() {
  pmem_unmap(pmem_ptr, EACH_DATA_FILE_LEN);
}

void Data::open(const std::string &fdata, const std::string &fflag) {
  size_t map_len;
  int is_pmem;
  bool new_create = false;

  if (access(fdata.c_str(), F_OK)) {
    new_create = true;
  }

  pmem_ptr = reinterpret_cast<char *>(pmem_map_file(fdata.c_str(), EACH_DATA_FILE_LEN, PMEM_FILE_CREATE, 0666, &map_len, &is_pmem));
  DEBUG_PRINTF(pmem_ptr, "%s open mmaped failed", fdata.c_str());
  pmem_users = (UserArray *)pmem_ptr;


  if (new_create) {
    // 其实会自动置零的，这里相当于是一个populate
    pmem_memset_nodrain(pmem_ptr, 0, EACH_DATA_FILE_LEN);
  } else {
    prefault(pmem_ptr, EACH_DATA_FILE_LEN);
  }

  flags = new DataFlag();
  flags->Open(fflag);
}

// data read and data write
const User* Data::data_read(uint32_t index) {
  // 让log来检查，这里就不重复检查了
  uint64_t ca_pos = index / UserArray::N_DATA;
  uint64_t inner_ca_pos = index % UserArray::N_DATA;
  return &pmem_users[ca_pos].data[inner_ca_pos];
}

void Data::put_flag(uint32_t index) {
  flags->set_flag(index);
}

bool Data::get_flag(uint32_t index) {
  return flags->get_flag(index);
}

UserArray* Data::get_pmem_users() {
  return pmem_users;
}
