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
		char ** db_cache;
		vector < char *> db;//lru main memory
		int lru_page;//current editing page
		map < string , int > dict;
		vector < table * > list_table;
		int * db_cache_flag;
		vector < pair < int , int > > page_owner;
		map < string , set < string > > map_v;

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
			db_cache_flag = (int *)malloc(sizeof(int)*num_pages);
			db_cache=(char **)malloc(sizeof(char *)*num_pages);
			for(int i=0;i<num_pages;i++)
			{
				db_cache_flag[i] = 0;
				db_cache[i]=(char *)malloc(sizeof(char)*page_size);
			}


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

							vector < string > rec;
							istringstream ss(qt_rem);
							vector < string > buf;
							string word;
							while(getline(ss , word , ','))
							{
								rec.PB(word);
							}

							string hash, tableName = list_table[i]->name;
							for(int i=0;i<list_table[dict[tableName]]->metadata.size();i++)
							{
								hash = tableName + ' ' + list_table[dict[tableName]]->metadata[i]->name;
								map_v[hash].insert(rec[i]);
							}

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


							vector < string > rec;
							istringstream ss(qt_rem);
							vector < string > buf;
							string word;
							while(getline(ss , word , ','))
							{
								rec.PB(word);
							}

							string hash, tableName = list_table[i]->name;
							for(int i=0;i<list_table[dict[tableName]]->metadata.size();i++)
							{
								hash = tableName + ' ' + list_table[dict[tableName]]->metadata[i]->name;
								map_v[hash].insert(rec[i]);
							}



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
				//	cout << "MISS " ;
				if(db.size() == num_pages)
				{
					//	cout << lru_page << endl;
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
					//	cout << db.size() << "" << endl;
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
				//	cout << "HIT\n" ;
				/*			for(int i=0;i<db.size();i++)
							{
							cout << db[i] << " before- map_vinsert   " << page_owner[i].F << " " << page_owner[i].S << endl;
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
			vector < string > rec;
			istringstream ss(record);
			vector < string > buf;
			string word;
			while(getline(ss , word , ','))
			{
				rec.PB(word);
			}

			string hash;
			for(int i=0;i<list_table[dict[tableName]]->metadata.size();i++)
			{
				hash = tableName + ' ' + list_table[dict[tableName]]->metadata[i]->name;
				map_v[hash].insert(rec[i]);
			}




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

		void queryType(string query) 
		{
			//	free(temp1);
			for(int i=query.size()-1;i>0;i--)
			{
				if(query[i]==' ')
					query.resize(i);
				else
					break;
			}
			if(query[query.size()-1]==';')
			{
				query.resize(query.size()-1);
			}
			else
			{
				//cout << "semicolon missing\n";
				//return;
			}
			for(int i=query.size()-1;i>0;i--)
				if(query[i]==' ')
					query.resize(i);
				else
					break;
			stringstream ssss(query);
			string buf;
			int cs = 0 , cc = 0;
			vector < string > word;
			while(ssss >> buf) {
				if(buf == "select") cs++;
				if(buf == "create") cc++;
				word.push_back(buf);
			}
			if(cs + cc != 1) cout << "invalid\n";
			//		if(word[SZ(word)-1] != ";") cout << "semicolon missing\n";
			if(cs == 1) {
				if(word[0] == "select") selectCommand(query);
				else cout << "invalid select\n";
			}
			if(cc == 1) {
				if(word[0] == "create") createCommand(query);
				else cout << "invalid create\n";
			}

		}

		void selectCommand(string query) 
		{
			stringstream ss(query);
			string buf;
			vector < string > word , cols , tables , orderby , groupby , having , condition , distinct , cond[3] , have[3];

			int p1 , p2 , pgr , pha , por;
			bool cwh = false , cgr = false , cha = false , cor = false,valid=true;
			while(ss >> buf) 
				word.PB(buf);
			for(int i=1;i<SZ(word);i++) {
				if(word[i] == "from") {
					p1 = i+1;
					break;
				}
				else if(word[i] == ",") 
					continue;
				else
				{
					stringstream sss(word[i]);
					string temp;
					while(getline(sss,temp,','))
					{
						if(temp!="")
						{
							//		cout << temp << endl;
							cols.PB(temp);
						}
					}
				}
			}

			for(int i=p1;i<SZ(word);i++) {
				if(word[i] == "where") 
				{
					cwh = true;
					p2 = i+1;
					break;
				}
				else if(word[i] == "groupby") {
					cgr = true;
					pgr = i+1;
					break;
				}
				else if(word[i] == "having") {
					cha = true;
					pha = i+1;
					break;
				}
				else if(word[i] == "orderby") {
					cor = true;
					por = i+1;
					break;
				}

				else if(word[i] == ";") break;
				else if(word[i] == ",") continue;
				else
				{
					stringstream sss(word[i]);
					string temp;
					while(getline(sss,temp,','))
					{
						if(temp!="")
						{
							//		cout << temp << endl;
							tables.PB(temp);
						}
					}
				}
			}

			if(cwh) 
			{
				for(int i=p2;i<SZ(word);i++) {
					if(word[i] == "groupby") {
						cgr = true;
						pgr = i+1;
						break;
					}
					else if(word[i] == "having") {
						cha = true;
						pha = i+1;
						break;
					}
					else if(word[i] == "orderby") {
						cor = true;
						por = i+1;
						break;
					}
					else if(word[i] == ";") 
						break;
					else
					{
						//	cout << word[i] << endl;
						condition.PB(word[i]);
					}

				}
			}

			if(cgr) 
			{
				for(int i=pgr;i<SZ(word);i++) {
					if(word[i] == "having") {
						cha = true;
						pha = i+1;
						break;
					}
					else if(word[i] == "orderby") {
						cor = true;
						por = i+1;
						break;
					}
					else if(word[i] == ";") 
						break;
					else
					{
						stringstream sss(word[i]);
						string temp;
						while(getline(sss,temp,','))
						{
							if(temp!="")
							{
								//		cout << temp << endl;
								groupby.PB(temp);
							}
						}
					}

				}
			}

			if(cha)
			{
				for(int i=pha;i<SZ(word);i++) {
					if(word[i] == "orderby") {
						cor = true;
						por = i+1;
						break;
					}
					else if(word[i] == ";") 
						break;
					else
					{
						//	cout << word[i] << endl;
						having.PB(word[i]);
					}

				}
			}

			bool orderby_asc=true;
			if(cor)
			{
				for(int i=por;i<SZ(word);i++) 
				{
					stringstream sss(word[i]);
					string temp;
					while(getline(sss,temp,','))
					{
						if(temp!="" || temp!="\0")
						{
							orderby.PB(temp);
						}
					}
					if(orderby.size())
					{
						string temp="";
						for(int k=0;k < orderby[orderby.size()-1].size() ;k++)
						{
							if(orderby[orderby.size()-1][k]=='(')
							{
								if(orderby[orderby.size()-1][k+1]=='D')
									orderby_asc=false;		
								break;
							}
							else
							{
								temp+=orderby[orderby.size()-1][k];
							}
						}
						if(temp!="")
							orderby[orderby.size()-1]=temp;
						else
							orderby.erase(orderby.begin()+orderby.size()-1);
					}
				}
				//		for(int k=0;k<orderby.size();k++)
				//			cout << "@" << orderby[k] << "@" << endl;
			}

			int di = 0 , at;
			string che;
			for(int i=0;i<cols.size();i++)
			{
				for(int j=0;j<cols[i].size();j++) che += cols[i][j];
				che += ',';
			}
			for(int i=0;i<che.size();i++) 
			{
				if(che[i] == '(') di++;
				if(che[i] == ')' && di == 1) 
				{
					di++;
					at = i;
				}
			}
			if(di == 2) 
			{
				string add = "";
				bool diok = false;
				int dien = 0;
				cols.resize(0);
				for(int i=0;i<che.size();i++)
				{
					if(che[i] == ' ') continue;
					if(diok)
					{
						if(che[i] == ',' || i == che.size()-1 || che[i]==')')
						{
							//cout << add << '\n';
							if(add!="" && add!=",") distinct.PB(add);
							if(add!="" && add!=",") cols.PB(add);
							add = "";
						}
						else add += che[i];
					}
					if(che[i] == '(') diok = true;
					if(che[i] == ')') 
					{
						diok = false;
						dien = i+1;
					}
				}
				for(int i=dien;i<che.size();i++)
				{
					if(che[i] == ' ') continue;
					if(che[i] == ',' || i == che.size()-1 || che[i] == ')')
					{
						//cout << add << '\n';
						if(add!="" && add!=")" && add!=",") cols.PB(add);
						add = "";
					}
					else add += che[i];
				}
			}
			//Where verify
			string coad = "";
			for(int i=0;i<condition.size();i++)
			{
				if(Tolower(condition[i]) == "and" || Tolower(condition[i]) == "or")
				{
					cond[0].PB(coad);
					cond[1].PB("");
					cond[2].PB("");
					coad = "";
					cond[0].PB(Tolower(condition[i]));
					cond[1].PB("");
					cond[2].PB("");
				}
				else coad += condition[i]+' ';
			}
			if(coad != "") 
			{
				cond[0].PB(coad);
				cond[1].PB("");
				cond[2].PB("");
			}
			for(int i=0;i<cond[0].size();i++) 
			{
				if(cond[0][i] == "and" || cond[0][i] == "or") continue;
				else 
				{
					string at1 , op , at2;
					int neco = 0;
					for(int j=0;j<cond[0][i].size();j++)
					{
						if(cond[0][i][j] == ' ') {
							neco++;
							continue;
						}
						/*if(neco == 1)
						  {
						  if(cond[0][i][j] == '>' || cond[0][i][j] == '<' || cond[0][i][j] == '=') neco = 1;
						  else neco = 2;
						  }
						  if(cond[0][i][j] == '>' || cond[0][i][j] == '<' || cond[0][i][j] == '=' || cond[0][i][j] == '!') neco = 1;*/
						if(neco == 0) at1 += cond[0][i][j];
						else if(neco == 1) op += cond[0][i][j];
						else at2 += cond[0][i][j];
					}
					cond[0][i] = at1;
					cond[1][i] = op;
					cond[2][i] = at2;
					if(!verifyAttr(tables , at1 , at2 , op)) 
					{
						cout << "Invalid query\n";
						return;
					}
				}
			}
			//Having verify
			string haad = "";
			for(int i=0;i<having.size();i++)
			{
				if(Tolower(having[i]) == "and" || Tolower(having[i]) == "or")
				{
					have[0].PB(haad);
					have[1].PB("");
					have[2].PB("");
					haad = "";
					have[0].PB(Tolower(having[i]));
					have[1].PB("");
					have[2].PB("");
				}
				else haad += having[i] + ' ';
			}
			if(haad != "")
			{
				have[0].PB(haad);
				have[1].PB("");
				have[2].PB("");
			}
			for(int i=0;i<have[0].size();i++)
			{
				if(have[0][i] == "and" || have[0][i] == "or") continue;
				else
				{
					string at1 , op , at2;
					int neco = 0;
					for(int j=0;j<have[0][i].size();j++)
					{
						if(have[0][i][j] == ' ') {
							neco++;
							continue;
						}
						/*if(neco == 1)
						  {
						  if(have[0][i][j] == '>' || have[0][i][j] == '<' || have[0][i][j] == '=') neco = 1;
						  else neco = 2;
						  }
						  if(have[0][i][j] == '>' || have[0][i][j] == '<' || have[0][i][j] == '=' || have[0][i][j] == '!') neco = 1;*/
						if(neco == 0) at1 += have[0][i][j];
						else if(neco == 1) op += have[0][i][j];
						else at2 += have[0][i][j];
					}
					have[0][i] = at1;
					have[1][i] = op;
					have[2][i] = at2;
					if(!verifyAttr(tables , at1 , at2 , op))
					{
						cout << "Invalid query\n";
						return;
					}
				}
			}
			//Tables verify
			int j,k;
			for(int i=0;i<tables.size();i++)
			{
				for(j=0;j<list_table.size();j++)
				{
					if(list_table[j]->name==tables[i])
						break;
				}
				if(j==list_table.size())
				{
					valid=false;
					cout << "Query Invalid\n";
					return ;
				}
			}
			if(cols.size() == 1)
			{
				if(cols[0] == "*")
				{
					cols.resize(0);
					for(int i=0;i<list_table.size();i++)
					{
						for(int j=0;j<tables.size();j++)
						{
							if(list_table[i]->name == tables[j])
							{
								for(int k=0;k<list_table[i]->metadata.size();k++) 
									cols.PB(list_table[i]->metadata[k]->name);
							}
						}
					}
				}
			}
			//Verify Columns
			bool flag=true;
			for(int i=0;i<cols.size();i++)
			{
				for(j=0;j<list_table.size();j++)
				{
					for(int l=0;l<tables.size();l++)
					{
						if(list_table[j]->name==tables[l])
						{
							for(k=0;k<list_table[j]->metadata.size();k++)
							{
								if(list_table[j]->metadata[k]->name==cols[i])
								{
									break;
								}
							}
							if(k == list_table[j]->metadata.size()) {
								flag = false; 
								break;
							}

						}
					}
				}
				if(flag==false)
				{
					cout << "Query Invalid\n";
					//return ;
				}
			}
			//Verfify groupby
			bool flag1=true;
			for(int i=0;i<groupby.size();i++)
			{
				for(j=0;j<list_table.size();j++)
				{
					for(int l=0;l<tables.size();l++)
					{
						if(list_table[j]->name==tables[l])
						{
							for(k=0;k<list_table[j]->metadata.size();k++)
							{
								if(list_table[j]->metadata[k]->name==groupby[i])
								{
									break;
								}
							}
							if(k == list_table[j]->metadata.size()) {
								flag1 = false; 
								break;
							}
						}
					}
				}
				if(flag1==false)
				{
					cout << "Query Invalid\n";
					return ;
				}
			}
			//Verify orderby
			vector < string > orderby_col_type;
			map <  string,int > orderby_col_to_num;
			bool flag2=true;
			for(int i=0;i<orderby.size();i++)
			{
				for(j=0;j<list_table.size();j++)
				{
					for(int l=0;l<tables.size();l++)
					{
						if(list_table[j]->name==tables[l])
						{
							for(k=0;k<list_table[j]->metadata.size();k++)
							{
								if(list_table[j]->metadata[k]->name==orderby[i])
								{
									orderby_col_type.push_back(list_table[j]->metadata[k]->type);
									orderby_col_to_num[list_table[j]->metadata[k]->name]=orderby_col_to_num.size();
									break;
								}
							}
							if(k == list_table[j]->metadata.size()) {
								flag2 = false; 
								break;
							}
						}
					}
				}
				if(flag2==false)
				{
					cout << "Query Invalid\n";
					return ;
				}
			}

			if(valid)
			{
				/*cout << "Querytype : select\n";
				  cout << "Distinct : ";
				  if(distinct.size())
				  {
				  for(int i=0;i<distinct.size()-1;i++)
				  {
				  cout << distinct[i] << ',';
				  }
				  cout << distinct[distinct.size()-1 ] << endl;
				  }
				  else
				  cout << "NA\n";
				  cout << "Columns : ";
				  if(cols.size())
				  {
				  for(int i=0;i<cols.size()-1;i++)
				  {
				  cout << cols[i] << ',';
				  }
				  cout << cols[cols.size()-1 ] << endl;
				  }
				  else
				  cout << "NA\n";
				  cout << "Tablename : ";
				  if(tables.size())
				  {
				  for(int i=0;i<tables.size()-1;i++)
				  {
				  cout << tables[i] << ',';
				  }
				  cout << tables[tables.size()-1 ] << endl;
				  }
				  else
				  cout << "NA\n";
				  cout << "Condition : ";
				  if(cond[0].size())
				  {
				  for(int i=0;i<cond[0].size()-1;i++)
				  {
				  if(cond[0][i] == "and" || cond[0][i] == "or") cout << cond[0][i] << ' ';
				  else cout << cond[0][i] << ' ' << cond[1][i] << ' ' << cond[2][i] << ' ';
				  }
				  if(cond[0][SZ(cond[0])-1] == "and" || cond[0][SZ(cond[0])-1] == "or") cout << cond[0][cond[0].size()-1] << ' ';
				  else cout << cond[0][cond[0].size()-1 ] << ' ' << cond[1][cond[0].size()-1] << ' ' << cond[2][cond[0].size()-1] << endl;
				  }
				  else
				  cout << "NA\n";
				  cout << "Groupby : ";
				  if(groupby.size())
				  {
				  for(int i=0;i<groupby.size()-1;i++)
				  {
				  cout << groupby[i] << ',';
				  }
				  cout << groupby[groupby.size()-1 ] << endl;
				  }
				  else
				  cout << "NA\n";
				  cout << "Having : ";
				  if(have[0].size())
				  {
				  for(int i=0;i<have[0].size()-1;i++)
				  {
				  if(have[0][i] == "and" || have[0][i] == "or") cout << have[0][i] << ' ';
				  else cout << have[0][i] << ' ' << have[1][i] << ' ' << have[2][i] << ' ';
				  }
				  if(have[0][SZ(have[0])-1] == "and" || have[0][SZ(have[0])-1] == "or") cout << have[0][have[0].size()-1] << ' ';
				  else cout << have[0][have[0].size()-1 ] << ' ' << have[1][have[0].size()-1] << ' ' << have[2][have[0].size()-1] << endl;
				  }
				  else
				  cout << "NA\n";
				cout << "Orderby : ";
				if(orderby.size())
				{
					for(int i=0;i<orderby.size()-1;i++)
					{
						cout << orderby[i] << ',';
					}
					cout << orderby[orderby.size()-1 ] << endl;
				}
				else
					cout << "NA\n";*/
						//DB("came");
						int tptr;
				for(int i=0;i<list_table.size();i++)
				{
					if(list_table[i]->name == tables[0]) 
					{
						tptr = i;
						break;
					}
				}

				int pre[102];
				for(int i=0;i<102;i++) pre[i] = 0;

				for(int i=0;i<list_table[tptr]->metadata.size();i++) 
				{
					for(int j=0;j<cols.size();j++) 
					{
						if(list_table[tptr]->metadata[i]->name == cols[j])
						{
							pre[i] = 1;
						}
					}
				}


				vector < vector < string > > fin;
				string tname = list_table[tptr]->name;
				for(int i=0;i<list_table[tptr]->list_pages.size();i++) 
				{
					int st = list_table[tptr]->list_pages[i]->st_id - 1;
					int en = list_table[tptr]->list_pages[i]->en_id - 1;
					for(int j=st;j<=en;j++) 
					{
						string here = getRecord(tname , j);
						vector < string > topr;
						string tmp = "";
						for(int k=0;k<here.size();k++) 
						{
							if(here[k] == ',')
							{
								topr.PB(tmp);
								tmp = "";
								continue;
							}
							tmp += here[k];
						}
						if(tmp != "") topr.PB(tmp);

						if(cwh)
						{
							if(cond[0].size() == 1) 
							{
								if(!validRow(topr , list_table[tptr]->metadata , cond[0][0] , cond[1][0] , cond[2][0])) continue;
								else fin.PB(topr);
							}
							else
							{
								vector < string > vals;
								for(int k=0;k<cond[0].size();k++)
								{
									if(cond[0][k] == "and") vals.PB("and");
									else if(cond[0][k] == "or") vals.PB("or");
									else 
									{
										bool x = validRow(topr , list_table[tptr]->metadata , cond[0][k] , cond[1][k] , cond[2][k]);
										if(x) vals.PB("1");
										else vals.PB("0");
									}
								}
								bool anf = false , orf = false;
								for(int k=0;k<vals.size();k++)
								{
									if(vals[k] == "and") anf = true;
									if(vals[k] == "or") orf = true;
								}
								int got;
								if(anf)
								{
									got = 1;
									for(int k=0;k<vals.size();k++)
									{
										if(vals[k] != "and")
										{
											if(vals[k] == "0") got = 0;
										}
									}
									if(!got) continue;
								}
								if(orf)
								{
									got = 0;
									for(int k=0;k<vals.size();k++)
									{
										if(vals[k] != "or")
										{
											if(vals[k] == "1") got = 1;
										}
									}
									if(!got) continue;
								}
								fin.PB(topr);
							}
						}
						else fin.PB(topr);
					}
				}

				//------------------------ NORMAL SORTING>.....!! 
				if(orderby.size())
				{
					/*for (int c = 0 ; c < fin.size(); c++)
					  {
					  for (int d = 0 ; d < fin.size() - c - 1  ; d++)
					  {
					  bool x=eval(fin[d],fin[d+1],orderby,list_table[tptr]->metadata ,orderby_asc);
					  if ((x && orderby_asc) || (!x && !orderby_asc))
					  {
					  vector < string >swap;
					  swap       = fin[d];
					  fin[d]   = fin[d+1];
					  fin[d+1] = swap;
					  }
					  }
					  }*/
					//------------------------- PHASE MERGE SORT
					vector < string > ans;
					fin=two_phase_merge_sort(pre,fin,orderby_col_type,orderby,orderby_col_to_num,orderby_asc,list_table[tptr]->metadata);
					/*	for(int i=0;i<ans.size();i++) 
						{
						cout << ans[i] << '\n';
						}*/
				}

				for(int i=0;i<fin.size();i++)
				{
					vector < string > now;
					bool prn = false;
					for(int j=0;j<fin[i].size();j++)
					{
						if(pre[j]) 
						{
							prn = true;
							now.PB(fin[i][j]);
						}
					}
					for(int j=0;j<now.size();j++)
					{
						cout << "\"" << now[j] << "\"";
						if(j != SZ(now) - 1) cout << ',';
					}
					if(prn) cout << '\n';

				}

			}
		}


		vector < vector < string > > two_phase_merge_sort( int flag[],vector < vector< string > > output, vector < string > cols_type, vector < string > cols, map<string,int> col_to_num,bool ascflag,vector < attr * > meta)
		{
			vector < vector<string> > phase1;
			for(int i=0;i<output.size();i+=num_pages)
			{
				vector < string > temp;
				if(output.size()-i>=num_pages)
				{
					for(int j=i;j<i+num_pages;j++)
					{
						string s ="";
						for(int k=0;k<output[j].size();k++)
						{
							s += output[j][k];
							if(k!=output[j].size()-1)
								s += ",";	
						}
						cache_insert(s,j-i);
					}
					for(int j=0;j<num_pages;j++)
					{
						int small = db_smallest(0,num_pages,cols,cols_type,col_to_num,meta,ascflag);
						string to_in(db_cache[small]);
						temp.push_back(to_in);
						db_cache_flag[small] = 1;
					}	
				}
				else
				{
					for(int j=i;j<output.size();j++)
					{
						string s="";
						for(int k=0;k<output[j].size();k++)
						{
							s +=output[j][k];
							if(k!=output[j].size()-1)
								s+=",";
						}
						cache_insert(s,j-i);
					}
					//		cout << "came" << endl ;
					for(int j=0;j<output.size()-i;j++)
					{
						int small = db_smallest(0,output.size()-i,cols,cols_type,col_to_num, meta,ascflag);
						//	cout << small << " " <<  db_cache[small] <<  endl;
						string to_in(db_cache[small]);
						temp.push_back(to_in);
						db_cache_flag[small] = 1;
					}
				}
				phase1.push_back(temp);	
			}
			vector < vector<string> > phase2 = phase1;
			while(phase2.size() != 1)
			{
				vector < vector<string> > main_temp;
				for(int i=0;i<phase2.size();i+=num_pages)
				{
					vector <string> temp;
					int store_count[100000] = {0};
					if(phase2.size()-i<num_pages)
					{
						for(int j=i;j<phase2.size();j++)
						{
							cache_insert(phase2[j][store_count[j-i]],j-i);
						}
						int breakflag =0;
						int counter = 0;
						while(1)
						{
							int small = db_smallest(0,phase2.size()-i,cols,cols_type,col_to_num,meta,ascflag);
							string to_in(db_cache[small]);
							temp.push_back(to_in);
							store_count[small]++;
							if(phase2[i+small].size() == store_count[small])
							{
								breakflag++;
								db_cache_flag[small] = 1;
							}
							else
								cache_insert(phase2[i+small][store_count[small]],small);
							if(breakflag == phase2.size()-i)
								break;
						}

					}
					else
					{
						for(int j=i;j<num_pages+i;j++)
						{
							cache_insert(phase2[j][store_count[j-i]],j-i);
						}
						int breakflag =0;
						while(1)
						{
							int small = db_smallest(0,num_pages,cols,cols_type,col_to_num,meta,ascflag);
							string to_in(db_cache[small]);
							temp.push_back(to_in);
							store_count[small]++;
							if(phase2[i+small].size() == store_count[small])
							{
								breakflag++;
								db_cache_flag[small] = 1;
							}
							else
								cache_insert(phase2[i+small][store_count[small]],small);
							if(breakflag == num_pages)
								break;
						}
					}
					main_temp.push_back(temp);
				}
				phase2 = main_temp;

			}	
			vector < vector < string > > rec;
			for(int i=0;i<phase2[0].size();i++)
			{
				vector < string > temp;
				istringstream ss(phase2[0][i]);
				string word;
				while(getline(ss , word , ','))
				{
					temp.PB(word);
				}
				rec.push_back(temp);
			}
			return rec;

		}

		void cache_insert(string st,int pos)
		{
			db_cache_flag[pos] = 0;
			db_cache[pos][0] = '\0';
			strcpy(db_cache[pos],st.c_str());
		}

		int db_smallest(int start,int end,vector <string> cols, vector <string> cols_type, map<string,int> col_to_num,vector < attr * > meta,bool order_asc)
		{
			int s = start;
			while(db_cache_flag[s]!=0)
				s++;
			for(int i=0;i<end;i++)
			{
				if(db_cache_flag[i]==0 && s!=i)
				{
					if(db_compare(i,s,cols,cols_type,col_to_num,meta,order_asc))
					{
						//cout << db_cache[i] << " *** " << db_cache[s] << endl; 
						s = i;
					}
				}
			}
			return s;
		}

		bool db_compare(int n, int m,vector <string> cols, vector <string> cols_type, map<string,int> col_to_num,vector < attr * > meta,bool order_asc)
		{
			vector <string> rec1;
			vector <string> rec2;
			string str1(db_cache[n]);
			string str2(db_cache[m]);
			istringstream ss(str1);
			string word;
			while(getline(ss , word , ',')) 
			{
				rec1.PB(word);
			}
			istringstream sss(str2);
			while(getline(sss , word , ',')) 
			{
				rec2.PB(word);
			}
			bool x=eval(rec1,rec2,cols,meta,0);
			return (x && order_asc) || (!x && !order_asc);
		}

		bool eval(vector< string >s1 ,vector< string >s2 ,vector < string > orderby,vector < attr * > meta ,bool order_asc)
		{
			for(int j=0;j<orderby.size();j++)
			{
				for(int i=0;i<meta.size();i++)
				{
					if(meta[i]->name == orderby[j])
					{
						if(Tolower(meta[i]->type) == "int" || Tolower(meta[i]->type)=="float")
						{
							float c1=atof(s1[i].c_str()),c2=atof(s2[i].c_str());
							if(c1<c2 ) return true;
							else if(c1>c2) return false;
							else if (c1==c2) break;

						}
						else if(Tolower(meta[i]->type) == "string")
						{
							int c=strcmp(s1[i].c_str(),s2[i].c_str());
							if(c==0) break;
							else if(c<0 ) return true;
							else if(c>0) return false;

						}
					}
				}
			}
			//cout << "CAME       ";
			return false;
		}

		bool validRow(vector < string > row , vector < attr * > meta , string at1 , string op , string at2)
		{
			for(int i=0;i<meta.size();i++)
			{
				if(meta[i]->name == at1)
				{
					if(Tolower(meta[i]->type) == "int")
					{
						if(op == "=")
						{
							if(atoi(row[i].c_str()) == atoi(at2.c_str())) return true;
							else return false;
						}
						else if(op == ">")
						{
							if(atoi(row[i].c_str()) > atoi(at2.c_str())) return true;
							else return false;
						}
						else if(op == "<")
						{
							if(atoi(row[i].c_str()) < atoi(at2.c_str())) return true;
							else return false;
						}
						else if(op == ">=")
						{
							if(atoi(row[i].c_str()) >= atoi(at2.c_str())) return true;
							else return false;
						}
						else if(op == "<=")
						{
							if(atoi(row[i].c_str()) <= atoi(at2.c_str())) return true;
							else return false;
						}
						else if(op == "!=") 
						{
							if(atoi(row[i].c_str()) != atoi(at2.c_str())) return true;
							else return false;
						}
					}
					if(Tolower(meta[i]->type) == "float")
					{
						if(op == "=")
						{
							if(atof(row[i].c_str()) == atof(at2.c_str())) return true;
							else return false;
						}
						else if(op == ">")
						{
							if(atof(row[i].c_str()) > atof(at2.c_str())) return true;
							else return false;
						}
						else if(op == "<")
						{
							if(atof(row[i].c_str()) < atof(at2.c_str())) return true;
							else return false;
						}
						else if(op == ">=")
						{
							if(atof(row[i].c_str()) >= atof(at2.c_str())) return true;
							else return false;
						}
						else if(op == "<=")
						{
							if(atof(row[i].c_str()) <= atof(at2.c_str())) return true;
							else return false;
						}
						else if(op == "!=") 
						{
							if(atof(row[i].c_str()) != atof(at2.c_str())) return true;
							else return false;
						}
					}
					else if(Tolower(meta[i]->type) == "string")
					{
						if(op == "=")
						{
							if(row[i] == at2) return true;
							else return false;
						}
						else if(op == "LIKE")
						{
							if(Tolower(row[i]) == Tolower(at2)) return true;
							else return false;
						}
					}	
				}
			}
		}


		void createCommand(string query) 
		{
			stringstream ss(query);
			string buf , add = "";
			vector < string > word;
			while(ss >> buf)
				word.PB(buf);
			//Verify tablename
			for(int i=0;i<list_table.size();i++)
			{
				if(list_table[i]->name == word[1]) 
				{
					cout << "Table with name " << word[1] << " already exists\n";
					return ;
				}
			}

			bool crok = false;
			vector < pair < string , string > > catr;
			pair < string , string > temp;
			for(int i=0;i<query.size();i++) 
			{
				if(query[i] == ')') crok = false;
				if(crok)
				{
					if(query[i] == ',' || query[i] == ' ') 
					{
						if(add != "") 
						{
							if(query[i] == ' ') temp.first = add;
							if(query[i] == ',') 
							{
								temp.second = add;
								catr.PB(temp);
								if(add!="String" && add!="int" && add!="float")
								{
									cout << "Invalid query\n" ;
									return ;
								}

							}
						}
						add = "";
					}
					else 
					{
						add += query[i];
						if(i == query.size() - 2) 
						{
							temp.second = add;
							catr.PB(temp);
							if(add!="String" && add!="int" && add!="float")
							{
								cout << "Invalid query\n" ;
								return ;
							}
						}
					}
				}
				if(query[i] == '(') crok = true;
			}



			table temp1;
			list_table.push_back(&temp1);
			strcpy(temp1.name,word[1].c_str());
			dict[word[1]] = dict.size();
			vector < attr *> tempp;

			for(int i=0;i<catr.size();i++) 
			{
				attr * tem = (attr *)malloc(sizeof(attr));
				strcpy(tem->name,catr[i].first.c_str());
				strcpy(tem->type,catr[i].second.c_str());
				tempp.PB(tem);
			}
			list_table[list_table.size()-1]->metadata=tempp;

			cout << "Querytype : create\n";
			cout << "Tablename : " << word[1] << '\n';
			cout << "Attributes : ";
			for(int i=0;i<catr.size()-1;i++) cout << catr[i].first << ' ' << catr[i].second << ',';
			cout <<  catr[catr.size()-1].first << ' ' << catr[catr.size()-1].second;
			cout << '\n';

			/// file creating 
			string patb = path_data + word[1] + ".csv";
			ofstream myfile1;
			myfile1.open(patb.c_str());
			myfile1.close();


			for(int j=0;j<list_table.size();j++) 
			{
				patb = path_data + list_table[j]->name + ".data";
				myfile1.open(patb.c_str());
				int k;
				for(k=0;k<list_table[j]->metadata.size()-1;k++)
				{
					myfile1 << list_table[j]->metadata[k]->name << ":" << list_table[j]->metadata[k]->type <<  ",";
				}
				if(list_table[j]->metadata.size())
					myfile1 << list_table[j]->metadata[k]->name << ":" << list_table[j]->metadata[k]->type << "\n";	
				myfile1.close();
			}


			string p = "./config.txt";
			ofstream myfile;
			myfile.open(p.c_str());
			myfile << "PAGE_SIZE " << page_size << "\n" ;
			myfile << "NUM_PAGES " << num_pages << "\n" ;
			myfile << "PATH_FOR_DATA " << path_data << "\n" ;
			for(int j=0;j<list_table.size();j++) 
			{
				myfile << "BEGIN" << "\n" ;
				myfile << list_table[j]->name << "\n" ;
				for(int k=0;k<list_table[j]->metadata.size();k++)
				{
					myfile << list_table[j]->metadata[k]->name << "," << list_table[j]->metadata[k]->type << "\n";
				}
				myfile << "END" << "\n" ;

			}
			myfile.close();

		}

		bool verifyAttr(vector < string > tables , string at1 , string at2 , string op)
		{
			if(op == "<" || op == ">" || op == "=" || op == "<=" || op == ">=" || op == "!=" || op == "LIKE") ;
			else {
				DB(op);
				return false;
			}

			string type1 = "" , type2 = "";

			for(int j=0;j<list_table.size();j++)
			{
				for(int l=0;l<tables.size();l++)
				{
					if(list_table[j]->name==tables[l])
					{
						for(int k=0;k<list_table[j]->metadata.size();k++)
						{
							if(list_table[j]->metadata[k]->name==at1)
							{
								type1 = list_table[j]->metadata[k]->type;
							}
							if(list_table[j]->metadata[k]->name == at2)
								type2 = list_table[j]->metadata[k]->type;
						}
					}
				}
			}
			if(type1 == "")
			{
				bool ch = false , dot = false , in = false;
				for(int i=0;i<at1.size();i++)
				{
					if(at1[i] >= '0' && at1[i] <= '9') in = true;
					else if(at1[i] == '.') dot = true;
					else ch = true;
				}
				if(in && !dot && !ch) type1 = "Int";
				if(in && dot && !ch) type1 = "Float";
				if(ch) type1 = "String";
			}
			if(type2 == "")
			{
				bool ch = false , dot = false , in = false;
				for(int i=0;i<at2.size();i++)
				{
					if(at2[i] >= '0' && at2[i] <= '9') in = true;
					else if(at2[i] == '.') dot = true;
					else ch = true;
				}
				if(in && !dot && !ch) type2 = "int";
				if(in && dot && !ch) type2 = "float";
				if(ch) type2 = "String";
			}
			if(type1 != type2) {
				return false;
			}
			if(type1 == type2 && op == "LIKE") {
				if(type1 == "int") return false;
				else if(type2 == "float") return false;
				else return true;
			}
			return true;
		}

		string Tolower(string x)
		{
			string ret;
			for(int i=0;i<x.size();i++) ret += tolower(x[i]);
			return ret;
		}

		

		void Main(int argc , char *argv[]) {
			if(argc <= 1) cout << "Give path to config file as command line argument\n";
			else {

				readConfig(argv[1]);
				populateDBInfo();

				char query[102];
				int t;
				char dum;
				scanf("%d\n",&t);
				while(t--) {
					gets(query);
					queryType(string(query));
				}
			}

		}


};

int main(int argc , char *argv[]) {

	DBSystem dbs;
	dbs.Main(argc , argv);
	//dbs.insertRecord("countries", "\"record\",\"fre\",\"gre\",\"ewq\"");
	//dbs.queryType("V(countries, ID)");
	/*dbs.readConfig("./");
	  dbs.populateDBInfo();
	  dbs.queryType("select DISTINCT ( ID , NAME ) ,   CODE from countries where ID<=3 and NAME != ABC groupby ID having ID>=50 orderby NAME;");
	  dbs.queryType("select * from countries where ID=3 groupby ID having ID>=50 orderby NAME;");
	  dbs.queryType("create state (ID int,NAME String,COUNTRY String);");

	  dbs.getRecord("countries",0);
	  dbs.getRecord("countries",1);
	  dbs.getRecord("countries",2);
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
	  for(int i=0;i<45;i++) cout << i << ' ' << dbs.getRecord("countries",i) << '\n';*/

	//	dbs.flushPages();

	return 0;
}

