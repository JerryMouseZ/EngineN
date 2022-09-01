#include "include/thread_id.hpp"

std::atomic<int> nr_producer = {0};
std::atomic<int> nr_cosumer = {0};
thread_local int producer_id = -1;
thread_local int consumer_id = -1;