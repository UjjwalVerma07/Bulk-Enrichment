cmake_minimum_required(VERSION 3.14)
project(bulk_enrichment LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

# Add GoogleTest
# Prefer local submodule at extern/googletest; otherwise fetch it
set(GTEST_SOURCE_DIR ${CMAKE_SOURCE_DIR}/extern/googletest)
if(EXISTS ${GTEST_SOURCE_DIR}/CMakeLists.txt)
  add_subdirectory(extern/googletest)
else()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  )
  # For Windows: Prevent overriding the parent project's compiler/linker settings
  set(gtest_force_shared_crt OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

# Add email validation logic + CLI
add_subdirectory(email_validation/cpp)

# Add tests
add_subdirectory(email_validation/cpp/tests)
