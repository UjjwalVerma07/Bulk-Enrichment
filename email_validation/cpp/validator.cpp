#include<bits/stdc++.h>
using namespace std;

bool isChar(char c){
    if((c>='A' and c<='Z') || (c>='a' and c<='z')){
        return true;
    }
    return false;
}

bool isDigit(char c){
    if(c>='0' && c<='9'){
        return true;
    }
    return false;
}

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
string trim(const string &s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end = s.find_last_not_of(" \t\r\n");
    if(start == string::npos || end == string::npos) return "";
    return s.substr(start, end - start + 1);
}


bool isvalid(const string &email){
    if(email.empty()) return false;
    if(!isChar(email[0])) return false;

    int atPos = email.find('@');
    int dotPos = email.rfind('.'); // last dot in the string

    if(atPos == string::npos || dotPos == string::npos) return false;
    if(atPos > dotPos) return false; // @ must be before dot
    if(atPos == 0 || dotPos == email.length()-1) return false; // nothing before @ or after dot
    if(dotPos - atPos < 2) return false; // at least one char between @ and .

    return true;
}
int main(int argc,char*argv[]){
    // string email="ujjwalvarma6948@gmail.com";
    // if(isvalid(email)){
    //     cout<<"Valid Email";
    // }
    // else{
    //     cout<<"Invalid Email";
    // }
    if(argc!=3){
        cerr<<"Usage: validator <input_csv> output_csv>";
        return 1;
    }
    string inputFile=argv[1];
    string outputFile=argv[2];
    
    ifstream input(inputFile);
    ofstream output(outputFile);

    if(!input.is_open()){
        cout<<"Error opening input file";
        return 1;
    }
    if(!output.is_open()){
        cout<<"Error opening output file";
        return 1;
    }

    string line;
    bool headerProcessed=false;

    while(getline(input,line)){
        stringstream ss(line);
        vector<string>row;
        string cell;
        while(getline(ss,cell,',')){
            row.push_back(cell);
        }
        if(!headerProcessed){
            output<<line<<",is_valid\n";
            headerProcessed=true;
        }else{
            string email=row.size()>2 ? row[2]:"";
           bool valid=isvalid(trim(email));
           output<<line<<","<<(valid ? "True":"False")<<"\n";
        }
    }

    input.close();
    output.close();

    cout<<"Validation Complete. Output Written to "<<outputFile<<endl;
    return 0;
}