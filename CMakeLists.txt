cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-O3)

include_directories(src)

add_library(interface
SHARED
        src/interface.cpp
        src/thread_data.cpp
)

message(${CMAKE_SOURCE_DIR})
