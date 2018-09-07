import random
import sys
import os
import subprocess
file = open("config.txt","r")
mystr = file.read()
file.close()
mystr = mystr.split('\n',1)
num=150
for i in range(100):
  mystr2 ="PAGE_SIZE " +str(num)+"\n" +mystr[1]
  file = open("config.txt","w")
  file.write(mystr2)
  file.close()
  outfile = "out_page_size_"+str(num)
  command2 = "./a.out > ./plot_page_size/out_files/" +outfile
  os.system(command2)
  num+=20

