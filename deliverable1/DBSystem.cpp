#include<bits/stdc++.h>
#define F first
#define S second
#define PB push_back
#define MP make_pair
#define SZ(x) x.size()
#define DEL '|'
#define DB(x) cout<<__LINE__<<" :: "<<#x<< ": "<<x<<endl;
using namespace std;

class DBSystem {

	private :
		typedef struct attr{
			char name[20];
			char type[20];
		}attr;
		typedef struct page{
			int st_id;
			int en_id;
			char * data;
			int in_db;
		}page;
		typedef struct table{
			char name[20];
			vector <attr *> metadata;
			vector <page *> list_pages;
		}table;
		int page_size;
		int num_pages;
		string path_data;
		vector < char *> db;//lru main memory
		int lru_page;//current editing page
		map < string , int > dict;
		vector < table * > list_table;
		vector < pair < int , int > > page_owner;

	public :
		void readConfig(string configFilePath) {
			lru_page=0;
			string path = configFilePath + "config.txt";
			string line;
			ifstream myfile (path.c_str());
			vector < attr *> temp1;
			bool fir = false;
			int co = 0;
			if(myfile.is_open()) {
				while(getline(myfile , line)) {
					string buf;
					stringstream ss(line);
					while(ss >> buf) 
					{
						//	cout << buf  << endl;
						if(buf == "PAGE_SIZE") {
							ss >> buf;
							page_size = atoi(buf.c_str());
						}
						else if(buf == "NUM_PAGES") {
							ss >> buf;
							num_pages = atoi(buf.c_str());
						}
						else if(buf == "PATH_FOR_DATA") {
							ss >> buf;
							path_data = buf;
						}
						else if(buf == "BEGIN") {
							fir = false;
						}
						else if(buf == "END") {
							list_table[list_table.size()-1]->metadata=temp1;
							temp1.clear();
						}
						else {
							if(!fir) 
							{
								fir = true;
								table * temp=(table *)malloc(sizeof(table));
								strcpy(temp->name,buf.c_str());
								dict[buf] = co++;
								list_table.push_back(temp);

							}
							else {
								istringstream sss(buf);
								vector < string > buf1;
								string word;
								while(getline(sss , word , ',')) 
								{
									buf1.PB(word);
								}
								attr * temp=(attr *)malloc(sizeof(attr));
								strcpy(temp->name,buf1[0].c_str());
								strcpy(temp->type,buf1[1].c_str());
								temp1.push_back(temp);
							}
						}
					}
				}

			}
			myfile.close();
			
			/*
			   map < string , int > :: iterator it;
			   for(it=dict.begin();it!=dict.end();it++) {
			   cout << it->first << ' ' << it->second << '\n';
			   }
			   for(int i=0;i<dict.size();i++) {
			   cout << dict[list_table[i]->name] << ' ' << list_table[i]->name << '\n';
			   }
			   for(int i=0;i<list_table.size();i++) 
			   {
			   cout << list_table[i]->name << ' '<< '\n';
			   for(int j=0;j<list_table[i]->metadata.size();j++)
			   cout << list_table[i]->metadata[j]->name << ' ' << list_table[i]->metadata[j]->type << endl;
			   }*/

		}

		void populateDBInfo() {

			//	cur_page = 0;
			int records=1;

			for(int i=0;i<list_table.size();i++) 
			{	
				records=1;
				string path = path_data , line,qt_rem;
				path+=string(list_table[i]->name);
				path += ".csv";
				page * cur_page =(page *)malloc(sizeof(page));
				cur_page->data = (char *)malloc(page_size*sizeof(char));
				cur_page->data[0]='\0';
				cur_page->st_id=1;
				ifstream myfile (path.c_str());
				if(myfile.is_open()) {
					while(getline(myfile , line)) 
					{
						if(line.size() < page_size - strlen(cur_page->data)) 
						{
							qt_rem="";
							for(int j=0;j<line.size();j++)
								if(line[j]!='"')
									qt_rem+=line[j];
							strcat(cur_page->data,(qt_rem+DEL).c_str());
							records++;
						}
						else {
							if(strlen(cur_page->data) <= page_size-1) 
								cur_page->data[strlen(cur_page->data)] = '\0';
							cur_page->en_id=records-1;
							cur_page->in_db=-1;
							list_table[i]->list_pages.push_back(cur_page);
							cur_page =(page *)malloc(sizeof(page));
							cur_page->data = (char *)malloc(page_size*sizeof(char));
							cur_page->data[0]='\0';
							cur_page->st_id=records;
							qt_rem="";
							for(int j=0;j<line.size();j++)
								if(line[j]!='"')
									qt_rem+=line[j];
							strcat(cur_page->data,(qt_rem+DEL).c_str());
							records++;	
						}
					}
					if(strlen(cur_page->data) <= page_size-1) 
						cur_page->data[strlen(cur_page->data)] = '\0';
					if(cur_page->st_id==records)
						        cur_page->en_id=records;
					else
						        cur_page->en_id=records-1;
					cur_page->in_db=-1;
					list_table[i]->list_pages.push_back(cur_page);
					myfile.close();
				}	
			} 

			/*for(int i=0;i<list_table.size();i++) 
			{
				cout << list_table[i]->name << "-------- "<< '\n';
				for(int j=0;j<list_table[i]->list_pages.size();j++)
					cout  << list_table[i]->list_pages[j]->data<<' '<< list_table[i]->list_pages[j]->st_id << ' ' << list_table[i]->list_pages[j]->en_id  << endl;
			}*/
		}

		string getRecord(string tableName, int recordId) {
			recordId++;
	//			DB(list_table[0]->list_pages[12]->in_db);
			int val=dict[tableName.c_str()],page_no=0;
			for(int i=0;i<list_table[val]->list_pages.size();i++)
			{
				if(recordId >= list_table[val]->list_pages[i]->st_id && recordId <=  list_table[val]->list_pages[i]->en_id )
				{
					page_no=i;
					break;
				}
			}
			if ( list_table[val]->list_pages[page_no]->in_db == -1 )
			{
		cout << "MISS " ;
				if(db.size() == num_pages)
				{
		cout << lru_page << endl;
					strcpy(list_table[page_owner[lru_page].F]->list_pages[page_owner[lru_page].S]->data,db[lru_page]);
					strcpy(db[lru_page],list_table[val]->list_pages[page_no]->data);
					//DB(list_table[page_owner[lru_page].F]->list_pages[page_owner[lru_page].S]->data);
					list_table[page_owner[lru_page].F]->list_pages[page_owner[lru_page].S]->in_db=-1;
					page_owner[lru_page].F=val;
					page_owner[lru_page].S=page_no;
					list_table[val]->list_pages[page_no]->in_db=lru_page;
					lru_page=(lru_page+1)%num_pages;

				}
				else
				{
		cout << db.size() << "" << endl;
					page_owner.push_back(make_pair(val,page_no));
					char *temp=(char *)malloc(page_size*sizeof(char));
					strcpy(temp,list_table[val]->list_pages[page_no]->data);
					list_table[val]->list_pages[page_no]->in_db=db.size();
					db.push_back(temp);
					//				lru_page=(lru_page+1)%(num_pages);
				}
			}
			else 
			{
		cout << "HIT\n" ;
	/*			for(int i=0;i<db.size();i++)
				{
					cout << db[i] << " before-insert   " << page_owner[i].F << " " << page_owner[i].S << endl;
				}
				cout  << "lru page number - " << lru_page << endl << endl;*/
				if (list_table[val]->list_pages[page_no]->in_db <lru_page)
				{
					vector < pair < int , int > > temp;
					temp.push_back(make_pair(val,page_no));
					for(int ll=lru_page-1;ll>list_table[val]->list_pages[page_no]->in_db;ll--)
						list_table[page_owner[ll].F]->list_pages[page_owner[ll].S]->in_db-=1;
					db.insert(db.begin()+lru_page,db[list_table[val]->list_pages[page_no]->in_db]);
					db.erase(db.begin()+list_table[val]->list_pages[page_no]->in_db);
					page_owner.insert(page_owner.begin()+lru_page,temp[0]);
					page_owner.erase(page_owner.begin()+list_table[val]->list_pages[page_no]->in_db);
					list_table[val]->list_pages[page_no]->in_db=(lru_page+num_pages-1)%num_pages;
				}
				else if (list_table[val]->list_pages[page_no]->in_db > lru_page)
				{
					vector < pair < int , int > > temp;
					temp.push_back(make_pair(val,page_no));
					for(int ll=lru_page;ll<list_table[val]->list_pages[page_no]->in_db;ll++)
						list_table[page_owner[ll].F]->list_pages[page_owner[ll].S]->in_db+=1;

					db.insert(db.begin()+lru_page,db[list_table[val]->list_pages[page_no]->in_db]);
					/*for(int i=0;i<db.size();i++)
					{
						cout << db[i] << " lru page < in db   " << page_owner[i].F << " " << page_owner[i].S << endl;
					}
					cout  << "lru page number - " << lru_page << endl << endl;*/
					db.erase(db.begin()+list_table[val]->list_pages[page_no]->in_db+1);
					page_owner.insert(page_owner.begin()+lru_page,temp[0]);
					page_owner.erase(page_owner.begin()+(list_table[val]->list_pages[page_no]->in_db+1));
					list_table[val]->list_pages[page_no]->in_db=lru_page;
					//DB(page_no);
					//DB(list_table[val]->list_pages[page_no]->in_db);
					if (db.size()==num_pages)
						lru_page=(lru_page+1)%num_pages;
				}
				else
				{
					lru_page=(lru_page+1)%num_pages;
				}
				/*for(int i=0;i<db.size();i++)
				{
					cout << db[i] << " after-delete   " << page_owner[i].F << " " << page_owner[i].S << endl;
				}
				cout  << "lru page number - " << lru_page << endl << endl;*/

				//swap(page_owner[(lru_page-1+num_pages)%num_pages],page_owner[list_table[val]->list_pages[page_no]->in_db]);
			}
			string ret="";
			int co=recordId - list_table[val]->list_pages[page_no]->st_id; 
			for(int i=0;i<strlen(list_table[val]->list_pages[page_no]->data);i++)
			{
				if(list_table[val]->list_pages[page_no]->data[i]==DEL)
				{
					if(co==0)
						break;
					else
					{
						co--;
						ret="";
					}
				}
				else
					ret+=list_table[val]->list_pages[page_no]->data[i];
			}
			//cout << ret << endl ;
	/*		cout  << endl;
			for(int i=0;i<db.size();i++)
			{
				cout << db[i] << "    " << page_owner[i].F << " " << page_owner[i].S << endl;
			}
			cout  << "lru page number - " << lru_page << endl << endl;
	*/		
			return ret;
		}

		void insertRecord(string tableName, string record)
		{
			int val=dict[tableName.c_str()],page_no=list_table[val]->list_pages.size()-1,x;
			//	DB(list_table[val]->list_pages[page_no]->data);
			if(list_table[val]->list_pages[page_no]->in_db!=-1)
			{
				x=strlen(db[list_table[val]->list_pages[page_no]->in_db]);
			}
			else
			{
				x=strlen(list_table[val]->list_pages[page_no]->data);
			}
			if (record.size() < page_size - x)
			{
				string temp;
				//	DB(temp);
				if ( list_table[val]->list_pages[page_no]->in_db == -1 )
				{
					temp=string(list_table[val]->list_pages[page_no]->data) + record + DEL;
					//				DB(temp);
					if(db.size() == num_pages)
					{
						strcpy(db[lru_page],temp.c_str());
						list_table[page_owner[lru_page].F]->list_pages[page_owner[lru_page].S]->in_db=-1;
						page_owner[lru_page].F=val;
						page_owner[lru_page].S=page_no;
						list_table[val]->list_pages[page_no]->in_db=lru_page;
						lru_page=(lru_page+1)%num_pages;

					}
					else
					{
						page_owner.push_back(make_pair(val,page_no));
						list_table[val]->list_pages[page_no]->in_db=lru_page;
						string temp=string(list_table[val]->list_pages[page_no]->data) + record + DEL;
						char *t1=(char *)malloc(page_size*sizeof(char));
						strcpy(t1,temp.c_str());
						db.push_back(t1);
						//	lru_page=(lru_page+1)%(num_pages);
					}

				}
				else
				{
					temp=string(db[list_table[val]->list_pages[page_no]->in_db]) + record + DEL;
					if (list_table[val]->list_pages[page_no]->in_db == (lru_page-1+num_pages)%num_pages)
					{
						strcpy(db[(lru_page-1+num_pages)%num_pages],temp.c_str());
					}
					else if (list_table[val]->list_pages[page_no]->in_db < lru_page)
					{
						vector < pair < int , int > > temp1;
						temp1.push_back(make_pair(val,page_no));
						char *t1=(char *)malloc(page_size*sizeof(char));
						strcpy(t1,(string(db[list_table[val]->list_pages[page_no]->in_db])+record+DEL).c_str());
						for(int ll=lru_page-1;ll>list_table[val]->list_pages[page_no]->in_db;ll--)
							list_table[page_owner[ll].F]->list_pages[page_owner[ll].S]->in_db-=1;
						db.insert(db.begin()+lru_page,t1);
						db.erase(db.begin()+list_table[val]->list_pages[page_no]->in_db);
						page_owner.insert(page_owner.begin()+lru_page,temp1[0]);
						page_owner.erase(page_owner.begin()+list_table[val]->list_pages[page_no]->in_db);
						list_table[val]->list_pages[page_no]->in_db=(lru_page+num_pages-1)%num_pages;
					}
					else if (list_table[val]->list_pages[page_no]->in_db > lru_page)
					{
						vector < pair < int , int > > temp1;
						temp1.push_back(make_pair(val,page_no));
						char *t1=(char *)malloc(page_size*sizeof(char));
						strcpy(t1,(string(db[list_table[val]->list_pages[page_no]->in_db])+record+DEL).c_str());
						//		DB((lru_page-1+num_pages)%num_pages);
					for(int ll=lru_page;ll<list_table[val]->list_pages[page_no]->in_db;ll++)
						list_table[page_owner[ll].F]->list_pages[page_owner[ll].S]->in_db+=1;
						db.insert(db.begin()+lru_page,t1);
						db.erase(db.begin()+list_table[val]->list_pages[page_no]->in_db+1);
						page_owner.insert(page_owner.begin()+lru_page,temp1[0]);
						page_owner.erase(page_owner.begin()+(list_table[val]->list_pages[page_no]->in_db+1));
						list_table[val]->list_pages[page_no]->in_db=lru_page;
					}

				}
			}
			else
			{
				page * cur_page =(page *)malloc(sizeof(page));
				cur_page->data = (char *)malloc(page_size*sizeof(char));
				cur_page->data[0]='\0';
				cur_page->st_id=list_table[val]->list_pages[page_no]->en_id+1;
				cur_page->en_id=list_table[val]->list_pages[page_no]->en_id+1;
				strcpy(cur_page->data,(record+DEL).c_str());
				string temp=record + DEL;
				if(db.size() == num_pages)
				{
					strcpy(list_table[page_owner[lru_page].F]->list_pages[page_owner[lru_page].S]->data,db[lru_page]);
					strcpy(db[lru_page],temp.c_str());
					list_table[page_owner[lru_page].F]->list_pages[page_owner[lru_page].S]->in_db=-1;
					page_owner[lru_page].F=val;
					page_owner[lru_page].S=page_no+1;
					list_table[val]->list_pages[page_no]->in_db=lru_page;

				}
				else
				{
					page_owner.push_back(make_pair(val,page_no+1));
					string temp=record + DEL;
					char *t1=(char *)malloc(page_size*sizeof(char));
					strcpy(t1,temp.c_str());
					db.push_back(t1);
				}
				cur_page->in_db=lru_page;
				list_table[val]->list_pages.push_back(cur_page);
				list_table[val]->list_pages[++page_no]->in_db=lru_page;
				lru_page=(lru_page+1)%(num_pages);

			}

			/*			for(int i=0;i<list_table.size();i++) 
						{
						cout << list_table[i]->name << "----------- "<< '\n';
						for(int j=0;j<list_table[i]->list_pages.size();j++)
						cout << list_table[i]->list_pages[j]->data << ' ' << list_table[i]->list_pages[j]->st_id << ' ' << list_table[i]->list_pages[j]->en_id  << endl;
						}*/
		}

		void flushPages()
		{
			for(int i=0;i<db.size();i++)
			{
				strcpy(list_table[page_owner[i].F]->list_pages[page_owner[i].S]->data,db[i]);
			}
			for(int i=0;i<list_table.size();i++) 
			{
				string p = path_data + string(list_table[i]->name) + ".csv";
				ofstream myfile;
				myfile.open(p.c_str());
				for(int j=0;j<list_table[i]->list_pages.size();j++) 
				{
					string buf = list_table[i]->list_pages[j]->data;
					istringstream sss(buf);
					string word;
					while(getline(sss , word , DEL)) 
					{
						if (word!="")
						{
							string qt_rem="\"";
							for(int j=0;j<word.size();j++)
							{
								if(word[j]==',')
								{
									qt_rem=qt_rem+"\""+word[j]+"\"";
								}
								else
								{
									qt_rem+=word[j];
								}
							}
							qt_rem+="\"";
							//cout << qt_rem << endl;
							myfile << qt_rem;
							myfile << '\n';
						}
					}
				}
				myfile.close();
			}
		}

		

		string Tolower(string x)
		{
			string ret;
			for(int i=0;i<x.size();i++) ret += tolower(x[i]);
			return ret;
		}

		


};

int main(int argc , char *argv[]) {



	//DBSystem dbs;
	//dbs.Main(argc , argv);
	DBSystem dbs ;
	dbs.readConfig(argv[1]);
	dbs.populateDBInfo();
dbs.getRecord("countries",0);
dbs.getRecord("countries",1);
dbs.getRecord("countries",2);
dbs.insertRecord("countries", "record");
dbs.getRecord("countries",2);
dbs.getRecord("countries",2);
dbs.getRecord("countries",3);
dbs.getRecord("countries",41);
dbs.getRecord("countries",9);
dbs.getRecord("countries",39);
dbs.getRecord("countries",28);
dbs.getRecord("countries",1);
dbs.getRecord("countries",30);
dbs.getRecord("countries",38);
dbs.getRecord("countries",39);
dbs.getRecord("countries",31);
dbs.insertRecord("countries", "record");
dbs.getRecord("countries",42);
dbs.getRecord("countries",28);
	//dbs.flushPages();

	return 0;
}

