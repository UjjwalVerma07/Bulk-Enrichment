enable_testing()

add_executable(runTests test_validator.cpp)
target_link_libraries(runTests PRIVATE validator gtest_main)

add_test(NAME ValidatorTests COMMAND runTests)

