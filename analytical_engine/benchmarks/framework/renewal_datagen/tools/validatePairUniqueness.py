
import sys, os


if( len(sys.argv) < 4):
    print("Validates that a pair of columns never appear repeated.")
    print("Usage: validateIdUniqueness <coulmn1> <column2> <file0> <file1> ... <filen>")

column1=int(sys.argv[1])
column2=int(sys.argv[2])

ids = {} 

for i in range(3,len(sys.argv)):
    print("Reading "+sys.argv[i])
    inputFile = open(sys.argv[i],'r')
    index = 0
    for line in inputFile.readlines():
        if index > 0:
            firstId = int((line.split('|'))[column1])
            secondId = int((line.split('|'))[column2])
            if firstId not in ids:
              ids[firstId] = set([])
            s = ids[firstId]
            if secondId in s:
              print("ERROR, Id pair not unique")
              print(str(firstId)+" "+str(secondId))
              exit(1)
            s.add(secondId)
        index+=1
    inputFile.close()
print("GREAT! All ids are different.")
