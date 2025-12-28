cmake_minimum_required(VERSION 3.18)
project(ipc0cp VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall")

# Fetch nlohmann_json
include(FetchContent)
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

# Fetch Google Test
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Library sources
set(IPC0CP_SOURCES
    cpp/ipc0cp/serialize.cpp
    cpp/ipc0cp/type_registry.cpp
    cpp/ipc0cp/ring_buffer.cpp
    cpp/ipc0cp/stdio.cpp
)

set(IPC0CP_HEADERS
    cpp/ipc0cp/ipc.hpp
    cpp/ipc0cp/serialize.hpp
    cpp/ipc0cp/type_registry.hpp
    cpp/ipc0cp/ring_buffer.hpp
    cpp/ipc0cp/stdio.hpp
)

# Create static library
add_library(ipc0cp_static STATIC ${IPC0CP_SOURCES})
target_include_directories(ipc0cp_static PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(ipc0cp_static PUBLIC nlohmann_json::nlohmann_json)
set_target_properties(ipc0cp_static PROPERTIES
    OUTPUT_NAME ipc0cp
    VERSION ${PROJECT_VERSION}
)

# Create shared library
add_library(ipc0cp_shared SHARED ${IPC0CP_SOURCES})
target_include_directories(ipc0cp_shared PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(ipc0cp_shared PUBLIC nlohmann_json::nlohmann_json)
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
option(BUILD_TESTS "Build tests" ON)

if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(BUILD_TESTS)
    enable_testing()
    
    # Test: Serialization
    add_executable(test_serialization cpp/tests/test_serialization.cpp)
    target_link_libraries(test_serialization PRIVATE 
        ipc0cp_static 
        GTest::gtest 
        GTest::gtest_main
        rt
    )
    target_include_directories(test_serialization PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cpp)
    add_test(NAME SerializationTest COMMAND test_serialization)
    set_tests_properties(SerializationTest PROPERTIES TIMEOUT 10)
    
    # Test: Ring Buffer
    add_executable(test_ring_buffer cpp/tests/test_ring_buffer.cpp)
    target_link_libraries(test_ring_buffer PRIVATE 
        ipc0cp_static 
        GTest::gtest 
        GTest::gtest_main
        rt
    )
    target_include_directories(test_ring_buffer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cpp)
    add_test(NAME RingBufferTest COMMAND test_ring_buffer)
    set_tests_properties(RingBufferTest PROPERTIES TIMEOUT 30)
    
    # Legacy test consumer (for cross-language testing)
    add_executable(test_consumer cpp/tests/test_consumer.cpp)
    target_link_libraries(test_consumer PRIVATE ipc0cp_static rt)
    target_include_directories(test_consumer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cpp)
    
    # Cross-language integration test (Python producer -> C++ consumer)
    find_program(PYTHON3 python3)
    if(PYTHON3)
        add_test(NAME CrossLanguageTest 
            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cpp/tests/run_cross_language_test.sh
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        set_tests_properties(CrossLanguageTest PROPERTIES 
            TIMEOUT 30
            ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python;CMAKE_BINARY_DIR=${CMAKE_BINARY_DIR}"
        )
    endif()
endif()
