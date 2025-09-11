#ifndef VALIDATOR_H //This is called as header guard if multiple times we include the header than it make sures the definations dont get duplicated while compilation
#define VALIDATOR_H

#include <string>
using namespace std;
bool isChar(char c);
bool isDigit(char c);
bool isvalid(const string &email);
string trim(const string &str);

#endif