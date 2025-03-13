
import sys, os
from sets import Set


if( len(sys.argv) == 1):
    print("Validates if the ids in the union of the first column of the input files are unique.")
    print("Usage: validateIdUniqueness <file0> <file1> ... <filen>")

ids = Set()
for i in range(1,len(sys.argv)):
    print("Reading "+sys.argv[i])
    inputFile = open(sys.argv[i],'r')
    index = 0
    for line in inputFile.readlines():
        if index > 0:
            newId = int((line.split('|'))[0])
            if newId in ids:
                print("ERROR: Id "+str(newId)+" already exists")
                print("Line "+str(index+1)+" of "+sys.argv[i]) 
                exit()
            ids.add(newId)
        index+=1
    inputFile.close()
print("GREAT! All ids are different.")
