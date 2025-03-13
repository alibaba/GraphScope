
import sys, os
import math
from datetime import datetime, date, time
import time as _time

if len(sys.argv) < 3:
  print("Incorrect number of parameters")



edgesPerPerson={}
SEPARATOR="|"
numEdges=0;
for i in range(1,len(sys.argv)-1):
  index=0
  print("Reading "+sys.argv[i])
  knowsFile = open(sys.argv[i],'r')
  for line in knowsFile.readlines():
      if index > 0:
          edge = line.split(SEPARATOR)
          if int(edge[0]) in edgesPerPerson:
              edgesPerPerson[int(edge[0])]+=1
          else:
              edgesPerPerson[int(edge[0])]=1
  
          if int(edge[1]) in edgesPerPerson:
              edgesPerPerson[int(edge[1])]+=1
          else:
              edgesPerPerson[int(edge[1])]=1
          numEdges+=2
      index+=1   
  knowsFile.close()

outputFile = open(sys.argv[len(sys.argv)-1],'w')
histogram = {}
for person in edgesPerPerson:
    degree = edgesPerPerson[person]
    outputFile.write(str(degree)+"\n")
    #if degree in histogram:
    #    histogram[degree]+=1
    #else:
    #    histogram[degree]=1

outputFile.close()
    

#print("Average degree: "+str(numEdges/float(len(edgesPerPerson))))
#for degree in histogram:
#    outputFile.write(str(degree)+" "+str(histogram[degree])+" \n")
