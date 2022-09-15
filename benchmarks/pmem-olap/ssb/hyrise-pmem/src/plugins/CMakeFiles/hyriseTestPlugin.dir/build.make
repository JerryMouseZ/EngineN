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
include src/plugins/CMakeFiles/hyriseTestPlugin.dir/depend.make

# Include the progress variables for this target.
include src/plugins/CMakeFiles/hyriseTestPlugin.dir/progress.make

# Include the compile flags for this target's objects.
include src/plugins/CMakeFiles/hyriseTestPlugin.dir/flags.make

src/plugins/CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.o: src/plugins/CMakeFiles/hyriseTestPlugin.dir/flags.make
src/plugins/CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.o: src/plugins/test_plugin.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/hpi/fs00/home/lars.bollmeier/hyrise/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/plugins/CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.o"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.o -c /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/test_plugin.cpp

src/plugins/CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.i"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/test_plugin.cpp > CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.i

src/plugins/CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.s"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/test_plugin.cpp -o CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.s

# Object files for target hyriseTestPlugin
hyriseTestPlugin_OBJECTS = \
"CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.o"

# External object files for target hyriseTestPlugin
hyriseTestPlugin_EXTERNAL_OBJECTS =

lib/libhyriseTestPlugin.so: src/plugins/CMakeFiles/hyriseTestPlugin.dir/test_plugin.cpp.o
lib/libhyriseTestPlugin.so: src/plugins/CMakeFiles/hyriseTestPlugin.dir/build.make
lib/libhyriseTestPlugin.so: src/plugins/CMakeFiles/hyriseTestPlugin.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/hpi/fs00/home/lars.bollmeier/hyrise/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX shared library ../../lib/libhyriseTestPlugin.so"
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/hyriseTestPlugin.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/plugins/CMakeFiles/hyriseTestPlugin.dir/build: lib/libhyriseTestPlugin.so

.PHONY : src/plugins/CMakeFiles/hyriseTestPlugin.dir/build

src/plugins/CMakeFiles/hyriseTestPlugin.dir/clean:
	cd /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins && $(CMAKE_COMMAND) -P CMakeFiles/hyriseTestPlugin.dir/cmake_clean.cmake
.PHONY : src/plugins/CMakeFiles/hyriseTestPlugin.dir/clean

src/plugins/CMakeFiles/hyriseTestPlugin.dir/depend:
	cd /hpi/fs00/home/lars.bollmeier/hyrise && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /hpi/fs00/home/lars.bollmeier/hyrise /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins /hpi/fs00/home/lars.bollmeier/hyrise /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins /hpi/fs00/home/lars.bollmeier/hyrise/src/plugins/CMakeFiles/hyriseTestPlugin.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/plugins/CMakeFiles/hyriseTestPlugin.dir/depend
