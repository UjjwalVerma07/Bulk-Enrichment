add_library(validator validator.cpp)
target_include_directories(validator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(validator_cli main.cpp)
target_link_libraries(validator_cli PRIVATE validator)
