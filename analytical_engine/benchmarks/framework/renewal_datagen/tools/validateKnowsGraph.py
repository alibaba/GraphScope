
import sys, os
import glob


if( len(sys.argv) == 1):
    print("Validates the correcness of the knows graph.")
    print("Usage: validateKnowsGraph <dir>")

person_files = glob.glob(sys.argv[1]+'/person_?_?.csv')
knows_files = glob.glob(sys.argv[1]+'/person_knows_person_?_?.csv')

persons = set()

for filename in person_files:
    file = open(filename,"r")
    print("reading "+filename)
    count = 0
    for line in file.readlines():
        if count != 0:
            fields = line.split('|')
            persons.add(int(fields[0]))
        count+=1
    file.close()


for filename in knows_files:
    file = open(filename,"r")
    print("reading "+filename)
    count = 0
    for line in file.readlines():
        if count != 0:
            fields = line.split('|')
            if (int(fields[0]) not in persons):
                print("ERROR: missing person "+fields[0])
                exit()
            if (int(fields[1]) not in persons):
                print("ERROR: missing person "+fields[1])
                exit()
        count+=1


    file.close()

print("GREAT: Knows graph is correct!")



