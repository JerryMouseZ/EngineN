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
include src/plugins/CMakeFiles/hyriseTieringPlugin.dir/depend.make

# Include the progress variables for this target.
include src/plugins/CMakeFiles/hyriseTieringPlugin.dir/progress.make

# Include the compile flags for this target's objects.
include src/plugins/CMakeFiles/hyriseTieringPlugin.dir/flags.make

src/plugins/CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.o: src/plugins/CMakeFiles/hyriseTieringPlugin.dir/flags.make
src/plugins/CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.o: src/plugins/tiering_plugin.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/hpi/fs00/home/lars.bollmeier/hyrise/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/plugins/CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.o"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.o -c /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/tiering_plugin.cpp

src/plugins/CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.i"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/tiering_plugin.cpp > CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.i

src/plugins/CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.s"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/tiering_plugin.cpp -o CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.s

# Object files for target hyriseTieringPlugin
hyriseTieringPlugin_OBJECTS = \
"CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.o"

# External object files for target hyriseTieringPlugin
hyriseTieringPlugin_EXTERNAL_OBJECTS =

lib/libhyriseTieringPlugin.so: src/plugins/CMakeFiles/hyriseTieringPlugin.dir/tiering_plugin.cpp.o
lib/libhyriseTieringPlugin.so: src/plugins/CMakeFiles/hyriseTieringPlugin.dir/build.make
lib/libhyriseTieringPlugin.so: src/plugins/CMakeFiles/hyriseTieringPlugin.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/hpi/fs00/home/lars.bollmeier/hyrise/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX shared library ../../lib/libhyriseTieringPlugin.so"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/hyriseTieringPlugin.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/plugins/CMakeFiles/hyriseTieringPlugin.dir/build: lib/libhyriseTieringPlugin.so

.PHONY : src/plugins/CMakeFiles/hyriseTieringPlugin.dir/build

src/plugins/CMakeFiles/hyriseTieringPlugin.dir/clean:
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && $(CMAKE_COMMAND) -P CMakeFiles/hyriseTieringPlugin.dir/cmake_clean.cmake
.PHONY : src/plugins/CMakeFiles/hyriseTieringPlugin.dir/clean

src/plugins/CMakeFiles/hyriseTieringPlugin.dir/depend:
	cd /hpi/fs00/home/lars.bollmeier/hyrise && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /hpi/fs00/home/lars.bollmeier/hyrise /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins /hpi/fs00/home/lars.bollmeier/hyrise /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/CMakeFiles/hyriseTieringPlugin.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/plugins/CMakeFiles/hyriseTieringPlugin.dir/depend

