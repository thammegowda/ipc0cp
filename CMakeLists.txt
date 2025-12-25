cmake_minimum_required(VERSION 3.18)
project(ipc0cp VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall")

# Library sources
set(IPC0CP_SOURCES
    csrc/ipc0cp/ipc.cpp
)

set(IPC0CP_HEADERS
    csrc/ipc0cp/ipc.hpp
)

# Create static library
add_library(ipc0cp_static STATIC ${IPC0CP_SOURCES})
target_include_directories(ipc0cp_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/csrc>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(ipc0cp_static PROPERTIES
    OUTPUT_NAME ipc0cp
    VERSION ${PROJECT_VERSION}
)

# Create shared library
add_library(ipc0cp_shared SHARED ${IPC0CP_SOURCES})
target_include_directories(ipc0cp_shared PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/csrc>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(ipc0cp_shared PROPERTIES
    OUTPUT_NAME ipc0cp
    VERSION ${PROJECT_VERSION}
    SOVERSION 0
)

# Installation rules
include(GNUInstallDirs)

install(TARGETS ipc0cp_static ipc0cp_shared
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(FILES ${IPC0CP_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ipc0cp
)

# Optional: Build examples and tests
option(BUILD_EXAMPLES "Build example programs" OFF)
option(BUILD_TESTS "Build tests" OFF)

if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
