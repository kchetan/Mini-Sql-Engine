#include<iostream>
#include<stdlib.h>
#include<string>
#include<fstream>
using namespace std;
int main()
{
	ofstream outfile;
	outfile.open("test.txt");
	for(int i=0;i<100;i++)
		outfile << "dbs.getRecord(\"transactions\"," << rand()%30 << ");"<< "\n";
	outfile.close();


}
