#pragma once
#include <stdint.h>

template <typename T, uint64_t ALIGN>
struct CommitArray {
    static constexpr uint64_t DALIGN = ALIGN;
    static constexpr uint64_t N_DATA = ALIGN / sizeof(T);
    static constexpr uint64_t N_PADDING = ALIGN - N_DATA * sizeof(T);

    T data[N_DATA];
    char padding[N_PADDING];
};
