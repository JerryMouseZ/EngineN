cmake_minimum_required(VERSION 3.1)
project("interface")
add_definitions(-DLOCAL)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(OpenMP)
add_definitions(-DLOCAL)
# add_compile_options(-O3)
add_subdirectory(liburing)
# add_compile_options(-g -O3 -Wno-address-of-packed-member)

include_directories(../src)
include_directories(../liburing/src/include)

# add_executable(test_multi ./test_multi.cpp ../src/interface.cpp ../src/comm.cpp)
# target_link_libraries(test_multi pmem pthread uring)

add_executable(test_local test_local.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/remote.cpp ../src/engine.cpp ../src/sync_data.cpp)
target_link_libraries(test_local pmem pthread uring OpenMP::OpenMP_CXX)

add_executable(test_crash test_crash.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/remote.cpp ../src/engine.cpp ../src/sync_data.cpp)
target_link_libraries(test_crash pmem pthread uring OpenMP::OpenMP_CXX)
# add_executable(init init.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/engine.cpp)
# target_link_libraries(init pmem pthread uring)

add_executable(test_connect test_connect.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/remote.cpp ../src/engine.cpp ../src/sync_data.cpp)
target_link_libraries(test_connect pmem pthread uring OpenMP::OpenMP_CXX)

add_executable(test_request test_request.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/remote.cpp ../src/engine.cpp ../src/sync_data.cpp)
target_link_libraries(test_request pmem pthread uring OpenMP::OpenMP_CXX)

add_executable(test_all test_all.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/remote.cpp ../src/engine.cpp ../src/sync_data.cpp)
target_link_libraries(test_all pmem pthread uring OpenMP::OpenMP_CXX)

add_executable(test_backup test_backup.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp ../src/data.cpp ../src/remote.cpp ../src/engine.cpp ../src/sync_data.cpp)
target_link_libraries(test_backup pmem pthread uring OpenMP::OpenMP_CXX)
# add_executable(test_data test_data.cpp ../src/comm.cpp)
# target_link_libraries(test_data pmem pthread uring)

# add_executable(test_log test_log.cpp ../src/comm.cpp)
# target_link_libraries(test_log pmem pthread uring)

# add_executable(test_engine_read test_engine_read.cpp ../src/interface.cpp ../src/comm.cpp ../src/thread_data.cpp)
# target_link_libraries(test_engine_read pmem uring)

# add_executable(test_multi ./test_multi.cpp ../src/interface.cpp)
# target_link_libraries(test_multi pmem pthread)


# add_executable(test_data test_data.cpp)
# target_link_libraries(test_data pmem pthread)

# add_executable(test_log test_log.cpp)
# target_link_libraries(test_log pmem pthread)

# add_executable(test_engine_read test_engine_read.cpp ../src/interface.cpp)
# target_link_libraries(test_engine_read pmem)

# add_executable(test_multiqueue test_multiqueue.cpp ../src/thread_data.cpp)
# target_link_libraries(test_multiqueue pthread pmem)

# add_executable(test_kill test_kill.cpp ../src/interface.cpp ../src/thread_data.cpp)
# target_link_libraries(test_kill pmem pthread)

# add_executable(test_read_write_interleave test_read_write_interleave.cpp ../src/interface.cpp ../src/thread_data.cpp)
# target_link_libraries(test_read_write_interleave pmem pthread)

# add_executable(test_entry test/test_entry.cpp)
# target_link_libraries(test_entry pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_userstring test/test_userstring.cpp)
# target_link_libraries(test_userstring pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_ftruncate test/test_ftruncate.cpp)
# target_link_libraries(test_ftruncate pmem pmemobj pthread OpenMP::OpenMP_CXX interface)

message(${CMAKE_SOURCE_DIR})
