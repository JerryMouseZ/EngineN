cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
find_package(OpenMP)

include_directories(../src)

add_executable(test_data test_data.cpp ../src/interface.cpp)
target_link_libraries(test_data pmem OpenMP::OpenMP_CXX)

# add_executable(test_engine test_engine.cpp)
# target_link_libraries(test_engine pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_engine_read test/test_engine_read.cpp)
# target_link_libraries(test_engine_read pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_entry test/test_entry.cpp)
# target_link_libraries(test_entry pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_userstring test/test_userstring.cpp)
# target_link_libraries(test_userstring pmem pmemobj pthread OpenMP::OpenMP_CXX)

# add_executable(test_ftruncate test/test_ftruncate.cpp)
# target_link_libraries(test_ftruncate pmem pmemobj pthread OpenMP::OpenMP_CXX interface)

message(${CMAKE_SOURCE_DIR})
