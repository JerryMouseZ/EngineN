cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(OpenMP)

add_compile_options(-g -O3)

include_directories(src)

add_library(interface
SHARED
        src/interface.cpp
        src/comm.cpp
        src/thread_data.cpp
        src/sync_data.cpp
        src/data.cpp
        src/engine.cpp
        src/remote.cpp
)

target_link_libraries(interface OpenMP::OpenMP_CXX uv)

message(${CMAKE_SOURCE_DIR})
