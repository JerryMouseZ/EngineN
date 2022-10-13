#pragma once
#include <stdint.h>
constexpr uint64_t MAX_NR_PRODUCER = 50;
constexpr uint64_t MAX_NR_CONSUMER = 4;

constexpr uint64_t QCMT_ALIGN = 1 << 15;
constexpr uint64_t CACHELINE_SIZE = 64;

constexpr int32_t NR_FD_EACH_SYNC_HANDLER = 4;
constexpr int32_t NR_SYNC_HANDLER_EACH_NB = MAX_NR_CONSUMER / NR_FD_EACH_SYNC_HANDLER;
