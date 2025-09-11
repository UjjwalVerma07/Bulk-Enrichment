#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include "validator.h"
using namespace std;



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
        stringstream ss(line);
        vector<string>row;
        string cell;
        while(getline(ss,cell,',')){
            row.push_back(cell);
        }
        if(!headerProcessed){
            output<<line<<",email_valid\n";
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