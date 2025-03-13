
import sys, os


if( len(sys.argv) == 1):
    print("Validates the correctness of an update stream regarding the dates of the events.")
    print("Usage: validateIdUniqueness <file>")

fileName = sys.argv[1]

file = open(fileName,"r")

previous_entry = -1
for line in file.readlines():
    fields = line.split("|")
    if previous_entry > int(fields[0]):
        print("ERROR: date is smaller than previous one")
        exit()
    if (int(fields[1])+10000) > int(fields[0]):
        print("ERROR: dependant event is later than the current one")
        print(line)
        exit()
    previous_entry = int(fields[0])

print("GREAT! UPDATE STREAM "+sys.argv[1]+" SEEMS CORRECT")

file.close()
