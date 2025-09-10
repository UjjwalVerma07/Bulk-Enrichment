#include <bits/stdc++.h>
using namespace std;

static bool isChar(char c){
	if((c>='A' and c<='Z') || (c>='a' and c<='z')){
		return true;
	}
	return false;
}

static bool isDigit(char c){
	if(c>='0' && c<='9'){
		return true;
	}
	return false;
}

bool isvalid(string email){
	if(email.length()==0){
		return false;
	}
	if(email.length()>254){
		return false;
	}
	// must contain exactly one '@'
	size_t atPos=email.find('@');
	if(atPos==string::npos){
		return false;
	}
	if(email.find('@',atPos+1)!=string::npos){
		return false;
	}
	string local=email.substr(0,atPos);
	string domain=email.substr(atPos+1);
	if(local.size()==0 || domain.size()==0){
		return false;
	}
	// no consecutive dots anywhere
	if(email.find("..")!=string::npos){
		return false;
	}
	// avoid leading/trailing dot around @ and in local start
	if(local.front()=='.' || local.back()=='.' || domain.front()=='.'){
		return false;
	}
	// length constraints
	if(local.size()>64){
		return false;
	}
	if(domain.size()>255){
		return false;
	}
	// valid characters in local part: letters, digits, '.', '_', '-'
	for(char c: local){
		bool isLetter=isChar(c);
		bool isNumber=isDigit(c);
		bool isAllowedPunct=(c=='.' || c=='_' || c=='-');
		if(!(isLetter || isNumber || isAllowedPunct)){
			return false;
		}
	}
	// domain format: at least one dot and TLD length >= 2
	size_t lastDot=domain.rfind('.');
	if(lastDot==string::npos){
		return false;
	}
	if(lastDot==domain.size()-1){
		return false;
	}
	string tld=domain.substr(lastDot+1);
	if(tld.size()<2){
		return false;
	}
	// domain allowed characters: letters, digits, '-', '.' and no invalid positions
	if(domain.front()=='.' || domain.back()=='.'){
		return false;
	}
	for(char c: domain){
		bool isLetter=isChar(c);
		bool isNumber=isDigit(c);
		bool isAllowedPunct=(c=='-' || c=='.');
		if(!(isLetter || isNumber || isAllowedPunct)){
			return false;
		}
	}
	// lowercase domain for whitelist check
	string normDomain=domain;
	for(char &c: normDomain){ c=tolower(c); }
	static const unordered_set<string> allowedDomains={
		"gmail.com","yahoo.com","outlook.com","example.com","company.org"
	};
	if(allowedDomains.find(normDomain)==allowedDomains.end()){
		return false;
	}
	return true;
}

string trim(const string &s) {
	size_t start = s.find_first_not_of(" \t\r\n");
	size_t end = s.find_last_not_of(" \t\r\n");
	if(start == string::npos || end == string::npos) return "";
	return s.substr(start, end - start + 1);
}

