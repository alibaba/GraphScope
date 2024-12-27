import csv
import os

input = "expected.csv"

#The first column is group_id, the second column is user_id
# sort the file by user_id.
# write the sorted file to sorted.csv
# No header

def sort(input):
    with open(input, 'r') as file:
        lines = file.readlines()
    lines.sort(key=lambda x: int(x.split(",")[1]))
    # swap the first and second column
    with open("sorted.csv", 'w') as file:
        for line in lines:
            parts = line.split(",")
            file.write(parts[1].strip() + "," + parts[0].strip() + "\n")
            
sort(input)