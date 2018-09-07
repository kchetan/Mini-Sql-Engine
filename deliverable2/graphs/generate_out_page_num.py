import random
import sys
import os
import subprocess
file = open("config.txt","r")
mystr = file.read()
file.close()
mystr = mystr.split('\n',2)
num=1
for i in range(10):
  mystr2 ="PAGE_SIZE 320 \n" +"NUM_PAGES "+str(num)+"\n"+mystr[2]
  file = open("config.txt","w")
  file.write(mystr2)
  file.close()
  outfile = "out_page_num_"+str(num)
  command2 = "./a.out > ./plot_page_num/out_files/" +outfile
  os.system(command2)
  num+=1

