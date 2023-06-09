# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.19

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Disable VCS-based implicit rules.
% : %,v


# Disable VCS-based implicit rules.
% : RCS/%


# Disable VCS-based implicit rules.
% : RCS/%,v


# Disable VCS-based implicit rules.
% : SCCS/s.%


# Disable VCS-based implicit rules.
% : s.%


.SUFFIXES: .hpux_make_needs_suffix_list


# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /hpi/fs00/home/lars.bollmeier/hyrise

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /hpi/fs00/home/lars.bollmeier/hyrise

# Include any dependencies generated for this target.
include src/bin/CMakeFiles/hyriseServer.dir/depend.make

# Include the progress variables for this target.
include src/bin/CMakeFiles/hyriseServer.dir/progress.make

# Include the compile flags for this target's objects.
include src/bin/CMakeFiles/hyriseServer.dir/flags.make

src/bin/CMakeFiles/hyriseServer.dir/server.cpp.o: src/bin/CMakeFiles/hyriseServer.dir/flags.make
src/bin/CMakeFiles/hyriseServer.dir/server.cpp.o: src/bin/server.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/hpi/fs00/home/lars.bollmeier/hyrise/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/bin/CMakeFiles/hyriseServer.dir/server.cpp.o"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/bin && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/hyriseServer.dir/server.cpp.o -c /hpi/fs00/home/lars.bollmeier/hyrise/src/bin/server.cpp

src/bin/CMakeFiles/hyriseServer.dir/server.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/hyriseServer.dir/server.cpp.i"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/bin && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /hpi/fs00/home/lars.bollmeier/hyrise/src/bin/server.cpp > CMakeFiles/hyriseServer.dir/server.cpp.i

src/bin/CMakeFiles/hyriseServer.dir/server.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/hyriseServer.dir/server.cpp.s"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/bin && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /hpi/fs00/home/lars.bollmeier/hyrise/src/bin/server.cpp -o CMakeFiles/hyriseServer.dir/server.cpp.s

# Object files for target hyriseServer
hyriseServer_OBJECTS = \
"CMakeFiles/hyriseServer.dir/server.cpp.o"

# External object files for target hyriseServer
hyriseServer_EXTERNAL_OBJECTS =

hyriseServer: src/bin/CMakeFiles/hyriseServer.dir/server.cpp.o
hyriseServer: src/bin/CMakeFiles/hyriseServer.dir/build.make
hyriseServer: lib/libhyrise_impl.so
hyriseServer: third_party/liblz4.a
hyriseServer: third_party/libsqlparser.a
hyriseServer: third_party/libzstd.a
hyriseServer: /usr/lib/libboost_container.so
hyriseServer: /usr/lib/libboost_system.so
hyriseServer: /mnt/nvrams1/epic/build/oneTBB/build/linux_intel64_gcc_cc9.1.1_libc2.17_kernel3.10.0_release/libtbb.so
hyriseServer: /usr/lib/x86_64-linux-gnu/libsqlite3.so
hyriseServer: /usr/lib/x86_64-linux-gnu/libnuma.so
hyriseServer: /usr/local/lib/pmdk_debug/libpmem2.so
hyriseServer: /usr/local/lib/pmdk_debug/libpmem.so
hyriseServer: third_party/jemalloc/lib/libjemalloc.so
hyriseServer: src/bin/CMakeFiles/hyriseServer.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/hpi/fs00/home/lars.bollmeier/hyrise/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../../hyriseServer"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/bin && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/hyriseServer.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/bin/CMakeFiles/hyriseServer.dir/build: hyriseServer

.PHONY : src/bin/CMakeFiles/hyriseServer.dir/build

src/bin/CMakeFiles/hyriseServer.dir/clean:
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/bin && $(CMAKE_COMMAND) -P CMakeFiles/hyriseServer.dir/cmake_clean.cmake
.PHONY : src/bin/CMakeFiles/hyriseServer.dir/clean

src/bin/CMakeFiles/hyriseServer.dir/depend:
	cd /hpi/fs00/home/lars.bollmeier/hyrise && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /hpi/fs00/home/lars.bollmeier/hyrise /hpi/fs00/home/lars.bollmeier/hyrise/src/bin /hpi/fs00/home/lars.bollmeier/hyrise /hpi/fs00/home/lars.bollmeier/hyrise/src/bin /hpi/fs00/home/lars.bollmeier/hyrise/src/bin/CMakeFiles/hyriseServer.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/bin/CMakeFiles/hyriseServer.dir/depend

