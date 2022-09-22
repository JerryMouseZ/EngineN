cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-O3)

include_directories(src)
include_directories(liburing/src/include)

add_subdirectory(liburing)

add_library(interface
SHARED
        src/interface.cpp
        src/comm.cpp
        src/thread_data.cpp
        src/data.cpp
        src/engine.cpp
        src/remote.cpp
)

target_link_libraries(interface uring)

message(${CMAKE_SOURCE_DIR})
