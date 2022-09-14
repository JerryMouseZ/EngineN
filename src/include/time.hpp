#pragma once

#include <ctime>
#include <ratio>
#include <chrono>

using time_point_t = std::chrono::_V2::system_clock::time_point;
using duration_t = std::chrono::duration<double>;

extern thread_local time_point_t time_record_start_point;

#ifdef TIME_RECORD

static inline void start_time_record() {
    time_record_start_point = std::chrono::high_resolution_clock::now();
}

static inline void end_time_record(duration_t *acc_target) {
    auto end_point = std::chrono::high_resolution_clock::now();
    *acc_target += std::chrono::duration_cast<duration_t>(end_point - time_record_start_point);
}

#else

static inline void start_time_record() {
}

static inline void end_time_record(duration_t *acc_target) {
}

#endif