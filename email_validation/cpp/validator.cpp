// #include<bits/stdc++.h>
// using namespace std;

// bool isChar(char c){
//     if((c>='A' and c<='Z') || (c>='a' and c<='z')){
//         return true;
//     }
//     return false;
// }

// bool isDigit(char c){
//     if(c>='0' && c<='9'){
//         return true;
//     }
//     return false;
// }

// bool isvalid(string email){
//     //Check if first character is alphabet or not
//     if(email.length()==0){
//         return false;
//     }
//     if(!isChar(email[0])){
//         return false;
//     }
//     int at=-1;
//     int dot=-1;
//     for(int i=0;i<email.length();i++){
//         if(email[i]=='@'){
//             at=i;
//         }
//         else if(email[i]=='.'){
//             dot=i;
//         }
//     }
//     if(at==-1 or dot==-1){
//         return false;
//     }
//      if(at>dot){
//         return false;
//     }
//     if(dot>=email.length()-1){
//         return false;
//     }
//     return true;
// }
// string trim(const string &s) {
//     size_t start = s.find_first_not_of(" \t\r\n");
//     size_t end = s.find_last_not_of(" \t\r\n");
//     if(start == string::npos || end == string::npos) return "";
//     return s.substr(start, end - start + 1);
// }
// int main(int argc,char*argv[]){
//     if(argc!=3){
//         cerr<<"Usage: validator <input_csv> output_csv>";
//         return 1;
//     }
//     string inputFile=argv[1];
//     string outputFile=argv[2];
    
//     ifstream input(inputFile);
//     ofstream output(outputFile);

//     if(!input.is_open()){
//         cout<<"Error opening input file";
//         return 1;
//     }
//     if(!output.is_open()){
//         cout<<"Error opening output file";
//         return 1;
//     }

//     string line;
//     bool headerProcessed=false;

//     while(getline(input,line)){
//         stringstream ss(line);
//         vector<string>row;
//         string cell;
//         while(getline(ss,cell,',')){
//             row.push_back(cell);
//         }
//         if(!headerProcessed){
//             output<<line<<",email_valid\n";
//             headerProcessed=true;
//         }else{
//             string email=row.size()>2 ? row[2]:"";
//            bool valid=isvalid(trim(email));
//            output<<line<<","<<(valid ? "True":"False")<<"\n";
//         }
//     }

//     input.close();
//     output.close();

//     cout<<"Validation Complete. Output Written to "<<outputFile<<endl;
//     return 0;
// }



//Email Should be Valid from the domains names that are given

//Single @ Symbol Rule

//No Consecutive Dot Rule

//Valid Charaters in the Local Part

//Length Constraints:-
//email<=254 Characters
//localpart<=64 Characetrs
//Domain_part<=255 Characters

//Domain Format Check:-
//domain should Contain atlease one dot
//Top Level Domain should be atleas 2 characters






#include "validator.h"
#include <iostream>
#include <string>
#include <unordered_set>
#include <fstream>
#include <sstream>
#include <vector>
using namespace std;

//Need to write the functions for the isChar also
bool isChar(char c){
    if((c>='A' and c<='Z') || (c>='a' and c<='z')){
        return true;
    }
    return false;
}


//Need to write the test cases for the isDigit also;
bool isDigit(char c){
    if(c>='0' && c<='9'){
        return true;
    }
    return false;
}

bool isvalid(const string &email){
    //Checks the length of the email;
    if(email.length()==0){
        return false;
    }

    //To check if the first character is alphabet or not 
    if(email.length()>254)return false;

    size_t atPos=email.find('@');
    //means @ is not present in the email;
    if(atPos==string::npos)return false;

    //meand duplicate @ are present;
    if(email.find('@',atPos+1)!=string::npos)return false;

    string local=email.substr(0,atPos);
    string domain=email.substr(atPos+1);

    if(local.size()==0 or domain.size()==0)return false;


     //No Consecutive Dot anywhere
    if(email.find("..")!=string::npos){
        return false;
    }
    

    //This is For the Size Check
    if(local.size()>64)return false;
    if(domain.size()>255)return false;


    //this is to chekck whether the . comes befores @
    int at=-1;
    int dot=-1;
    for(int i=0;i<email.length();i++){
        if(email[i]=='@'){
            at=i;
        }
        else if(email[i]=='.'){
            dot=i;
        }
    }
    if(at==-1 or dot==-1){
        return false;
    }

    //This means . is coming before the @ //like ujjwalvarma6948.gmail@com
     if(at>dot){
        return false;
    }

    //This means the . is coming at last or is not coming at all;
    if(dot>=email.length()-1){
        return false;
    }

   //valid characters in local part 
    for(char c :local){
        bool isLetter=isChar(c);
        bool isNumber=isDigit(c);
        bool isAllowedPunct=(c=='.' || c=='_' || c=='-');
        if(!(isLetter || isNumber || isAllowedPunct))return false;
    }

    //top level domain should be atleast of 2 in size
    size_t lastDot=domain.rfind('.');

    //means the last dot is not present;
    if(lastDot==string::npos)return false;

    if(lastDot==domain.size()-1)return false;
     

    //To Check that the top level Domain should be atleast has a size greater than 2;
    string tld=domain.substr(lastDot+1);
    if(tld.size()<2)return false;
    if(domain.front()=='.' || domain.back()=='.')return false;
    

    //Suppose Their exist ujjwalvarma6948@gmail.com
    //local would me Local=ujjwalvarma6948
    //Domain would be gmail.com
    //Top Level Domain would be "com,in,org" 


    //This tell that the domain should be from the this list of allowedDomains;
    static const unordered_set<string>allowedDomains={
        "gmail.com","yahoo.com","outlook.com","data-axle.com","example.com"
    };
    if(allowedDomains.find(domain)==allowedDomains.end()){
        return false;
    }
   return true;
}

//Need to write the test cases for the trim function also
string trim(const string &s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end = s.find_last_not_of(" \t\r\n");
    if(start == string::npos || end == string::npos) return "";
    return s.substr(start, end - start + 1);
}


// int main(int argc,char*argv[]){
//     if(argc!=3){
//         cerr<<"Usage: validator <input_csv> output_csv>";
//         return 1;
//     }
//     string inputFile=argv[1];
//     string outputFile=argv[2];
    
//     ifstream input(inputFile);
//     ofstream output(outputFile);

//     if(!input.is_open()){
//         cout<<"Error opening input file";
//         return 1;
//     }
//     if(!output.is_open()){
//         cout<<"Error opening output file";
//         return 1;
//     }

//     string line;
//     bool headerProcessed=false;

//     while(getline(input,line)){
//         stringstream ss(line);
//         vector<string>row;
//         string cell;
//         while(getline(ss,cell,',')){
//             row.push_back(cell);
//         }
//         if(!headerProcessed){
//             output<<line<<",email_valid\n";
//             headerProcessed=true;
//         }else{
//             string email=row.size()>2 ? row[2]:"";
//            bool valid=isvalid(trim(email));
//            output<<line<<","<<(valid ? "True":"False")<<"\n";
//         }
//     }

//     input.close();
//     output.close();

//     cout<<"Validation Complete. Output Written to "<<outputFile<<endl;
//     return 0;
// }