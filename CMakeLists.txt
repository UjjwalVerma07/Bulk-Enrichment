cmake_minimum_required(VERSION 3.14)
project(bulk_enrichment LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add GoogleTest (from extern/googletest)
add_subdirectory(extern/googletest)

# Add email validation logic + CLI
add_subdirectory(email_validation/cpp)

# Add tests
add_subdirectory(email_validation/cpp/tests)
