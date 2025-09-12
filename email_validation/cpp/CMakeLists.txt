add_library(validator validator.cpp)
target_include_directories(validator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(validator PUBLIC cxx_std_17)

add_executable(validator_cli main.cpp)
target_compile_features(validator_cli PRIVATE cxx_std_17)
target_link_libraries(validator_cli PRIVATE validator)
