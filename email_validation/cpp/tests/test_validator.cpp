#include "../validator.h"
#include <gtest/gtest.h>
# include <iostream>
using namespace std;

//TESTCASES FOR THE ISCHAR

TEST(CharCheckTest,Letters){
    EXPECT_TRUE(isChar('a'));
    EXPECT_TRUE(isChar('F'));
    EXPECT_FALSE(isChar('@'));
    EXPECT_FALSE(isChar('_'));
    EXPECT_FALSE(isChar('1'));
}

//TESTCASES FOR THE ISDIGIT

TEST(DigitCheckTest,Digits){
    EXPECT_TRUE(isDigit('0'));
    EXPECT_TRUE(isDigit('9'));
    EXPECT_FALSE(isDigit('@'));
    EXPECT_FALSE(isDigit('_'));
    EXPECT_FALSE(isDigit(8));
}

//TESTCASES FOR THE TRIM FUNCTION

TEST(TrimCheckTest,TrimmedStrings){
    EXPECT_EQ(trim(" ujjwal"),"ujjwal");
    EXPECT_EQ(trim("verma   "),"verma");
    EXPECT_EQ(trim("ujjwal verma"),"ujjwal verma");
    EXPECT_EQ(trim("  ujjwal verma.    "),"ujjwal verma.");
    EXPECT_EQ(trim("   ujjwal verma"),"ujjwal verma");
    EXPECT_EQ(trim("ujjwal verma     "),"ujjwal verma");
}