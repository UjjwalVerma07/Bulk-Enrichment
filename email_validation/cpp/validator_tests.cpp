#include <gtest/gtest.h>
#include <string>

// We keep current structure, so include directly from the same folder
#include "validator_lib.h"

TEST(EmailValidator, Accepts_AllowedDomains_And_ValidSyntax){
	EXPECT_TRUE(isvalid("abc@gmail.com"));
	EXPECT_TRUE(isvalid("a@example.com"));
	EXPECT_TRUE(isvalid("a@company.org"));
	EXPECT_TRUE(isvalid("abc@GMAIL.COM")); // case-insensitive domain
}

TEST(EmailValidator, Rejects_DisallowedDomains){
	EXPECT_FALSE(isvalid("a@unknown.org"));
	EXPECT_FALSE(isvalid("a@company.com"));
}

TEST(EmailValidator, Rejects_SyntaxIssues){
	EXPECT_FALSE(isvalid("abc..xyz@gmail.com"));
	EXPECT_FALSE(isvalid(".abc@gmail.com"));
	EXPECT_FALSE(isvalid("abc.@gmail.com"));
	EXPECT_FALSE(isvalid("ab@c@gmail.com")); // more than one '@'
	EXPECT_FALSE(isvalid("abgmail.com")); // missing '@'
	EXPECT_FALSE(isvalid("abc@gmail")); // no dot in domain / no TLD
	EXPECT_FALSE(isvalid("ab$@gmail.com")); // invalid local character
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

