cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(OpenMP)

add_compile_options(-g -O3 -fPIC)

add_subdirectory(libuv)

include_directories(src)
include_directories(libuv/include)

add_library(interface
SHARED
        src/interface.cpp
        src/comm.cpp
        src/thread_data.cpp
        src/sync_data.cpp
        src/data.cpp
        src/engine.cpp
        src/remote.cpp
        src/sync_queue.cpp
)

target_link_libraries(interface OpenMP::OpenMP_CXX uv_a)

message(${CMAKE_SOURCE_DIR})
