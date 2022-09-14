cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(OpenMP)
# add_compile_options(-O3)
add_subdirectory(liburing)

include_directories(../src)
include_directories(../liburing/src/include)

add_executable(test_multi ./test_multi.cpp ../src/interface.cpp ../src/comm.cpp)
target_link_libraries(test_multi pmem pthread uring)

add_executable(test_engine test_engine.cpp ../src/interface.cpp ../src/comm.cpp)
target_link_libraries(test_engine pmem pthread uring)

add_executable(init init.cpp ../src/interface.cpp ../src/comm.cpp)
target_link_libraries(init pmem pthread uring)

add_executable(test_connect test_connect.cpp ../src/interface.cpp ../src/comm.cpp)
target_link_libraries(test_connect pmem pthread uring)

add_executable(test_data test_data.cpp ../src/comm.cpp)
target_link_libraries(test_data pmem pthread uring)

add_executable(test_log test_log.cpp ../src/comm.cpp)
target_link_libraries(test_log pmem pthread uring)

add_executable(test_engine_read test_engine_read.cpp ../src/interface.cpp ../src/comm.cpp)
target_link_libraries(test_engine_read pmem uring)

# add_executable(test_entry test/test_entry.cpp)
# target_link_libraries(test_entry pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_userstring test/test_userstring.cpp)
# target_link_libraries(test_userstring pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_ftruncate test/test_ftruncate.cpp)
# target_link_libraries(test_ftruncate pmem pmemobj pthread OpenMP::OpenMP_CXX interface)

message(${CMAKE_SOURCE_DIR})
