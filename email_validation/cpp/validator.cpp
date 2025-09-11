#include <iostream>
#include <fstream>
#include <string>
#include <string_view>
using namespace std;

static inline bool isChar(char c){
    return (c>='A' && c<='Z') || (c>='a' && c<='z');
}

static inline string_view trim_view(string_view s){
    size_t start = 0;
    size_t end = s.size();
    while(start < end){
        char ch = s[start];
        if(ch!=' ' && ch!='\t' && ch!='\r' && ch!='\n') break;
        ++start;
    }
    while(end > start){
        char ch = s[end-1];
        if(ch!=' ' && ch!='\t' && ch!='\r' && ch!='\n') break;
        --end;
    }
    return s.substr(start, end - start);
}

static inline bool isvalid(string_view email){
    if(email.empty()){
        return false;
    }
    if(!isChar(email.front())){
        return false;
    }
    int at = -1;
    int dot = -1;
    for(size_t i=0;i<email.size();i++){
        char ch = email[i];
        if(ch=='@'){
            at = static_cast<int>(i);
        } else if(ch=='.'){
            dot = static_cast<int>(i);
        }
    }
    if(at==-1 || dot==-1){
        return false;
    }
    if(at>dot){
        return false;
    }
    if(static_cast<size_t>(dot) >= email.size()-1){
        return false;
    }
    return true;
}

int main(int argc,char*argv[]){
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
        if(!headerProcessed){
            output<<line<<",email_valid\n";
            headerProcessed=true;
            continue;
        }

        size_t first = line.find(',');
        size_t second = (first==string::npos) ? string::npos : line.find(',', first+1);
        string_view email_field;
        if(second != string::npos){
            size_t third_start = second + 1;
            size_t third_end = line.find(',', third_start);
            if(third_end == string::npos) third_end = line.size();
            email_field = string_view(line).substr(third_start, third_end - third_start);
        } else {
            email_field = string_view();
        }

        string_view email_trimmed = trim_view(email_field);
        bool valid = isvalid(email_trimmed);
        output<<line<<","<<(valid ? "True":"False")<<"\n";
    }

    input.close();
    output.close();

    cout<<"Validation Complete. Output Written to "<<outputFile<<endl;
    return 0;
}