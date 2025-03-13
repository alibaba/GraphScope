
import sys, os

input_file = open(sys.argv[1])

def parse_line( input_file ):
    line = input_file.readline()
    if len(line) > 0:
        if line[0] == '(':
            output = line.replace("\n","").replace(" ","") 
            while output.find(')') == -1:
                line = input_file.readline()
                output = output.replace("\n","").replace(" ","") + line.replace("\n","").replace(" ","") 
            return output 
    return line 

def parse_distribution(input_file, output_file):
    line = parse_line(input_file)
    accum = 0
    entry_labels = []
    entry_freq = []
    while(len(line) > 0 and line[0] == "(" ):
        line = line.replace("(","")
        line = line.replace(")","")
        line = line.split(",")
        freq = int(line[0])
        accum += freq
        prev = 0
        if(len(entry_freq) > 0):
            prev = entry_freq[len(entry_freq)-1]
        entry_freq.append(prev + freq)
        entry_labels.append(line[1])
        line = parse_line(input_file)
    index = 0
    while(index < len(entry_labels)):
        output_file.write(str(entry_freq[index] / float(accum))+" "+entry_labels[index]+"\n")
        index += 1

    return line


words_file = open("words.csv", "w")
hashtags_file = open("hashtags.csv", "w")
sentence_count_file = open("sentence_count.csv", "w")
sentence_lengths_file = open("sentence_lengths.csv", "w") 

line = parse_line(input_file) 
while(len(line) > 0):
    if line.find("Number of unique words",0,len("Number of unique words")) == 0:
        line = parse_distribution(input_file, words_file )
    elif line.find("Number of unique hashtags",0,len("Number of unique hashtags")) == 0:
        line = parse_distribution(input_file, hashtags_file )
    elif line.find("Number of sentence counts",0,len("Number of sentence counts")) == 0:
        line = parse_distribution(input_file, sentence_count_file )
    elif line.find("Words per sentence count",0,len("Words per sentence count")) == 0:
        line = parse_distribution(input_file, sentence_lengths_file )
    else:
        line = parse_line(input_file)
            
input_file.close()
words_file.close()
hashtags_file.close()
sentence_count_file.close()
sentence_lengths_file.close()
