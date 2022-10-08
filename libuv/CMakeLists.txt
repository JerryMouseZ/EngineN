cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-O3 -Wall -Wextra -fno-stack-protector -Wno-unused-parameter -Isrc/include)
add_definitions("-DLIBURING_INTERNAL -D_GNU_SOURCE")

include_directories(src)

add_library(uv
        src/unix/async.c 
        src/unix/core.c 
        src/unix/dl.c 
        src/unix/fs.c 
        src/unix/getaddrinfo.c 
        src/unix/getnameinfo.c 
        src/unix/internal.h 
        src/unix/loop-watcher.c 
        src/unix/loop.c 
        src/unix/pipe.c 
        src/unix/poll.c 
        src/unix/process.c 
        src/unix/random-devurandom.c 
        src/unix/signal.c 
        src/unix/stream.c 
        src/unix/tcp.c 
        src/unix/thread.c 
        src/unix/tty.c 
        src/unix/udp.c
)
message(${CMAKE_SOURCE_DIR})
