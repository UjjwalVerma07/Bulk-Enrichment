enable_testing()

add_executable(runTests test_validator.cpp)
target_compile_features(runTests PRIVATE cxx_std_17)
target_link_libraries(runTests PRIVATE validator gtest_main)

include(GoogleTest)
gtest_discover_tests(runTests)

