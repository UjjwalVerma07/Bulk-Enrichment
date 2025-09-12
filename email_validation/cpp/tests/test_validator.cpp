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

struct TrimTestCase{
    string input;
    string expected;
};
class TrimTestCheck:public testing::TestWithParam<TrimTestCase>{};
TEST_P(TrimTestCheck,HandledTrimmedStrings){
    TrimTestCase param=GetParam();
    EXPECT_EQ(trim(param.input),param.expected);
}
INSTANTIATE_TEST_SUITE_P(Trimtest,TrimTestCheck,testing::Values(
    TrimTestCase{" ujjwal","ujjwal"},
    TrimTestCase{"verma   ","verma"},
    TrimTestCase{"ujjwal verma","ujjwal verma"},
    TrimTestCase{"  ujjwal verma.    ","ujjwal verma."},
    TrimTestCase{"   ujjwal verma","ujjwal verma"},
    TrimTestCase{"ujjwal verma     ","ujjwal verma"}
));




//TESTCASES FOR THE isValid Functions with Different Scenario;
TEST(EMAIL_VALIDATION_TEST,EmptyEmail){
    EXPECT_FALSE(isvalid(""));
}

TEST(EMAIL_VALIDATION_TEST,TooLongEmail){
    string long_email=string(255,'a');
    long_email+="@gmail.com";
    EXPECT_FALSE(isvalid(long_email));
}

TEST(EMAIL_VALIDATION_TEST,MissingAtSymbol){
    string email="ujjwalvarma6948.gmail.com";
    EXPECT_FALSE(isvalid(email));
}

TEST(EMAIL_VALIDATION_TEST,DuplicateAtSymbol){
    string email1="ujjwalvarma6948@@gmail.com";
    string email2="ujjwal.verma@data-axle.com";
    EXPECT_FALSE(isvalid(email1));
    EXPECT_TRUE(isvalid(email2));
}

TEST(EMAIL_VALIDATION_TEST,EmptyLocalOrDomain){
    string empty_local="@gmail.com";
    string empty_domain="ujjwalvarma6948@";
    EXPECT_FALSE(isvalid(empty_local));
    EXPECT_FALSE(isvalid(empty_domain));
}

TEST(EMAIL_VALIDATION_TEST,DuplicateDots){
    string email1="ujjwalvarma6948@gmail..com";
    string email2="ujjwal..varma6948@gmail.com";
    string email3="ujjwal.verma@data-axle.com";
    EXPECT_FALSE(isvalid(email1));
    EXPECT_FALSE(isvalid(email2));
    EXPECT_TRUE(isvalid(email3));
}

TEST(EMAIL_VALIDATION_TEST,TooLongLocal){
    string too_long_local_email=string(65,'a');
    too_long_local_email+="@gmail.com";  
    EXPECT_FALSE(isvalid(too_long_local_email));
}

TEST(EMAIL_VALIDATION_TEST,TooLongDomain){
    string too_long_domain=string(255,'g');
    string too_long_domain_email="ujjwalvarma@"+too_long_domain;
    EXPECT_FALSE(isvalid(too_long_domain_email));

}

TEST(EMAIL_VALIDATION_TEST,DotBeforeAt){
    string email1="ujjwal@verma.gmail.com";
    string email2="ujjwal@verma.com";
    EXPECT_FALSE(isvalid(email1));
    EXPECT_FALSE(isvalid(email2));
}

class Email_Validation_NoDot:public testing::TestWithParam<string>{};
TEST_P(Email_Validation_NoDot,NoDotNoAt){
    string email=GetParam();
    EXPECT_FALSE(isvalid(email));
};

string NoDotNoAtTestName(const testing::TestParamInfo<string>& info) {
    std::string name = info.param;
    for (auto& c : name) {
        if (!isalnum(c)) {
            c = '_';
        }
    }
    return "Case_" + std::to_string(info.index) + "_" + name;
}

INSTANTIATE_TEST_SUITE_P(NoDotNoAtTest,Email_Validation_NoDot,testing::Values(
          "ujjwalverma",
          "ujjwalvermadata-axle.com",
          "ujjwalverma@com"
),NoDotNoAtTestName);


TEST(EMAIL_VALIDATION_TEST,DotAtLast){
    string email1="ujjwalverma@gmailcom.";
    string email2="ujjwalverma@gmailcom";
    EXPECT_FALSE(isvalid(email1));
    EXPECT_FALSE(isvalid(email2));
}

class Email_Validation_InvalidCharacters:public testing::TestWithParam<std::string>{};
TEST_P(Email_Validation_InvalidCharacters,InvalidEmail){
    string email=GetParam();
    EXPECT_FALSE(isvalid(email));
}
string InvalidEmailTestNameGenerator(const testing::TestParamInfo<string>&info){
    string name=info.param;
    for(char &c:name){
        if(!isalnum(c)){
            c='_';
        }
    }
    return name;
}
INSTANTIATE_TEST_SUITE_P(InvalidEmailTest,Email_Validation_InvalidCharacters,testing::Values(
    "ujjwal$varma@gmail.com",
    "user!name@yahoo.com",
    "test%email@outlook.com",
    "hello world@gmail.com"
),InvalidEmailTestNameGenerator);



class Email_Validation_TLD_Short:public testing::TestWithParam<string>{};
TEST_P(Email_Validation_TLD_Short,Email_Validation_Short_TLD){
    string email=GetParam();
    EXPECT_FALSE(isvalid(email));
}

INSTANTIATE_TEST_SUITE_P(Email_Validation_TLD_Test,Email_Validation_TLD_Short,testing::Values(
    "ujjwal@gmail.c",
    "user@domain.a",
    "name@sub.domain.x"
));


//SHOULD USE THE PARAMETRIZED HERE ALSO
class EMAIL_VALIDATION_FOR_DOMAIN:public testing::TestWithParam<string>{};
TEST_P(EMAIL_VALIDATION_FOR_DOMAIN,VALID_DOMAIN_TEST){
    string email=GetParam();
    EXPECT_TRUE(isvalid(email));
}
string ValidTestNameGenerator(const testing::TestParamInfo<string>&info){
          string name=info.param;
    for(char &c:name){
        if(!isalnum(c)){
            c='_';
        }
    }
    return name;
}
INSTANTIATE_TEST_SUITE_P(ValidDomainTest,EMAIL_VALIDATION_FOR_DOMAIN,testing::Values(
    "ujjwalvarma6948@gmail.com",
    "user_name-123@outlook.com",
    "first.last@data-axle.com",
    "simple@example.com",
    "user.name_with-dots@gmail.com",
   "firstname.lastname@outlook.com"
),ValidTestNameGenerator);




// Test edge cases for valid characters and sizes
TEST(EMAIL_VALIDATION_TEST, EdgeCaseValidEmails) {
    // Local size exactly 64
    std::string local64(64, 'a');
    EXPECT_TRUE(isvalid(local64 + "@gmail.com"));
    
    // Domain size exactly 255
    std::string domain255(247, 'b'); // 247 + "@" + 8 ("gmail.com") = 255
    EXPECT_FALSE(isvalid("user@" + domain255));
    
    // Mixed valid characters in local
    EXPECT_TRUE(isvalid("a_b.c-d123@outlook.com"));
}