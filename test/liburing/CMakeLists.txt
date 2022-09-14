cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-O3 -Wall -Wextra -fno-stack-protector -Wno-unused-paramter -Wno-sign-compare -Isrc/include)
add_definitions("-DLIBURING_INTERNAL -D_GNU_SOURCE")

include_directories(src)

add_library(uring
        src/setup.c
        src/queue.c
        src/register.c
        src/syscall.c
)

message(${CMAKE_SOURCE_DIR})
