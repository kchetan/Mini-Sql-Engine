import random
import sys
import os
import subprocess
import matplotlib.pyplot as plt

num = 150
x=[]
y=[]
for i in range(100):
	x.append(num)
	miss =0
	outfile = "./plot_page_size/out_files/out_page_size_" + str(num)
	outfile = open(outfile,"r")
	for line in outfile:
	  if "MISS" in line:
	    miss+=1;
	outfile.close()
	num+=20
	y.append(miss)
print(y)
plt.plot(x,y)
plt.xlabel("Page Size (Bytes)")
plt.ylabel("Number Of Misses")
plt.title('Page Size vs Number Of Misses')
plt.grid(True)
plt.text(1500,60,"Number Of Pages = 8")
plt.show()
