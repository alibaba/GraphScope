
import sys, os
import glob


if( len(sys.argv) == 1):
    print("Validates the correctness of the studyAt graph.")
    print("Usage: validateStudyAt <dir>")

study_at_files = glob.glob(sys.argv[1]+'/person_studyAt_organization_?_?.csv')
organization_files = glob.glob(sys.argv[1]+'/organization_?_?.csv')
update_stream_files = glob.glob(sys.argv[1]+'/updateStream_?_?_person.csv')

universities = set()

for filename in organization_files:
    file = open(filename,"r")
    print("reading "+filename)
    count = 0
    for line in file.readlines():
        if count != 0:
            fields = line.split('|')
            if fields[1] == "university":
                universities.add(int(fields[0]))
        count+=1
    file.close()

print("Number of universities read "+str(len(universities)))


for filename in study_at_files:
    file = open(filename,"r")
    print("reading "+filename)
    count = 0
    for line in file.readlines():
        if count != 0:
            fields = line.split('|')
            if (int(fields[1]) not in universities):
                print("ERROR: missing university "+fields[0])
                exit()
        count+=1


    file.close()

for filename in update_stream_files:
    file = open(filename,"r")
    print("reading "+filename)
    count = 0
    for line in file.readlines():
        study_ats = line.split('|')[15]
        if( study_ats != ''):
            for study_at in study_ats.split(';'):
                uni_id = study_at.split(',')[0]
                if (int(uni_id) not in universities):
                    print("ERROR: missing university "+uni_id)
                    exit()
    file.close()

print("GREAT: studyAt graph is correct!")



