cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(OpenMP)

include_directories(src)

add_library(interface
SHARED
        src/interface.cpp
        # src/engine.cpp
)

message(${CMAKE_SOURCE_DIR})
