#!/usr/bin/env python2

import discoverparams
import readfactors
import random
import os
import codecs
from datetime import date
from timeparameters import *
from calendar import timegm

SEED = 1

def findNameParameters(names):
	srtd = sorted(names,key=lambda x: -x[1])
	res = []
	hist = {}
	for t in srtd:
		if t[1] not in hist:
			hist[t[1]] = []
		hist[t[1]].append(t[0])
	counts = sorted([i for i in hist.iterkeys()])

	mid = len(counts)/2
	i = mid
	while counts[i] - counts[mid] < 0.1 * counts[mid]:
		res.extend([name for name in hist[counts[i]]])
		i += 1
	i = mid - 1
	while  counts[mid] - counts[i] < 0.1 * counts[mid]:
		res.extend([name for name in hist[counts[i]]])
		i -= 1
	return res

class CSVSerializer:
	def __init__(self):
		self.handlers = []
		self.inputs = []

	def setOutputFile(self, outputFile):
		self.outputFile=outputFile

	def registerHandler(self, handler, inputParams, header):
		handler.header = header
		self.handlers.append(handler)
		self.inputs.append(inputParams)

	def writeCSV(self):
		output = codecs.open( self.outputFile, "w",encoding="utf-8")

		if len(self.inputs) == 0:
			return

		headers = [self.handlers[j].header for j in range(len(self.handlers))]
		output.write("|".join(headers))
		output.write("\n")

		for i in range(len(self.inputs[0])):
			# compile a single CSV line from multiple handlers
			csvLine = []
			for j in range(len(self.handlers)):
				handler = self.handlers[j]
				data = self.inputs[j][i]
				csvLine.append(handler(data))
			output.write('|'.join([s for s in csvLine]))
			output.write("\n")
		output.close()

def handlePersonParam(person):
	return str(person)

def handleMaxTimeParam(timeParam):
	res =  str(timegm(date(year=int(timeParam.year),
		month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000)
	return res

def handleMinTimeParam(timeParam):
	res =  str(timegm(date(year=int(timeParam.year),
		month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000)
	return res

def handleTimeDurationParam(timeParam):
	res =  str(timegm(date(year=int(timeParam.year),
		month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000)
	res += "|"+str(timeParam.duration)
	return res


def handlePairCountryParam((Country1, Country2)):
	return Country1+"|"+Country2

def handleCountryParam(Country):
	return Country

def handleTagParam(tag):
	return tag

def handleTagTypeParam(tagType):
	return tagType

def handleMonthParam(month):
	return str(month)

def handleFirstNameParam(firstName):
	return firstName

def handlePairPersonParam((person1, person2)):
	return str(person1)+"|"+str(person2)

def handleWorkYearParam(timeParam):
	return str(timeParam)

def main(argv=None):
	if argv is None:
		argv = sys.argv

	if len(argv) < 3:
		print "arguments: <input dir> <output>"
		return 1

	indir = argv[1]+"/"
	activityFactorFiles=[]
	personFactorFiles=[]
	friendsFiles = []
	outdir = argv[2]+"/"
	random.seed(SEED)
	

	for file in os.listdir(indir):
		if file.endswith("activityFactors.txt"):
			activityFactorFiles.append(indir+file)
		if file.endswith("personFactors.txt"):
			personFactorFiles.append(indir+file)
		if file.startswith("m0friendList"):
			friendsFiles.append(indir+file)

	# read precomputed counts from files	
	(personFactors, countryFactors, tagFactors, tagClassFactors, nameFactors, givenNames,  ts, postHisto) = readfactors.load(personFactorFiles, activityFactorFiles, friendsFiles)

	# find person parameters
	selectedPersonParams = {}
	for i in range(1, 15):
		factors = readfactors.getFactorsForQuery(i, personFactors)
		selectedPersonParams[i] = discoverparams.generate(factors)

	# Queries 13 and 14 take two person parameters each. Generate pairs
	secondPerson = {}
	for i in [13, 14]:
		secondPerson[i] = []
		for person in selectedPersonParams[i]:
			j = 0
			while True:
				j = random.randint(0, len(selectedPersonParams[i])-1)
				if selectedPersonParams[i][j] != person:
					break
			secondPerson[i].append(selectedPersonParams[i][j])

	# find country parameters for Query 3 and 11
	selectedCountryParams = {}
	for i in [3, 11]:
		factors = readfactors.getCountryFactorsForQuery(i, countryFactors)
		selectedCountryParams[i] = discoverparams.generate(factors, portion=0.1)

		# make sure there are as many country parameters as person parameters
		oldlen = len(selectedCountryParams[i])
		newlen = len(selectedPersonParams[i])
		selectedCountryParams[i].extend([selectedCountryParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	# Query 3 needs two countries as parameters. Generate the second one:
	secondCountry = []
	for c in selectedCountryParams[3]:
		i=0
		while True:
			i = random.randint(0, len(selectedCountryParams[3])-1)
			if selectedCountryParams[3][i]!=c:
				break
		secondCountry.append(selectedCountryParams[3][i])

	(leftTagFactors, rightTagFactors) = discoverparams.divideFactors(tagFactors, 0.7)
	leftSize = len(leftTagFactors)
	rightSize = len(rightTagFactors)
	leftPortion = 0.1*(leftSize+rightSize) / (2.0*leftSize)
	rightPortion = 0.1*(leftSize+rightSize) / (2.0*rightSize)
	selectedTagParams = {}
	for i in [6]:
		selectedTagParams[i] = discoverparams.generate(leftTagFactors, portion=leftPortion)
		selectedTagParams[i].extend(discoverparams.generate(rightTagFactors, portion=rightPortion))
		oldlen = len(selectedTagParams[i])
		newlen = len(selectedPersonParams[i])
		selectedTagParams[i].extend([selectedTagParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	# generate tag type parameters for Query 12
	selectedTagTypeParams = {}
	for i in [12]:
		selectedTagTypeParams[i] = discoverparams.generate(tagClassFactors, portion=0.1)
		# make sure there are as many tag paramters as person parameters
		oldlen = len(selectedTagTypeParams[i])
		newlen = len(selectedPersonParams[i])
		selectedTagTypeParams[i].extend([selectedTagTypeParams[i][random.randint(0, oldlen-1)] for j in range(newlen-oldlen)])

	# find time parameters for Queries 2,3,4,5,9
	selectedPersons = selectedPersonParams[2] + selectedPersonParams[3]+selectedPersonParams[4]
	selectedPersons += selectedPersonParams[5] + selectedPersonParams[9]

	timeSelectionInput = {
		2: (selectedPersonParams[2], "f", getTimeParamsBeforeMedian),
		3: (selectedPersonParams[3], "ff", getTimeParamsWithMedian),
		4: (selectedPersonParams[4], "f", getTimeParamsWithMedian),
		5: (selectedPersonParams[5], "ffg", getTimeParamsAfterMedian),
		9: (selectedPersonParams[9], "ff", getTimeParamsBeforeMedian)
	}

	selectedTimeParams = findTimeParams(timeSelectionInput, personFactorFiles, activityFactorFiles, friendsFiles, ts[1])
	# Query 11 takes WorksFrom timestamp
	selectedTimeParams[11] = [random.randint(ts[2], ts[3]) for j in range(len(selectedPersonParams[11]))]

	# Query 10 additionally needs the month parameter
	months = []
	for person in selectedPersonParams[10]:
		month = random.randint(1, 12)
		months.append(month)

	nameParams = []
	for person in selectedPersonParams[1]:
		n = givenNames.getValue(person)
		nameParams.append(n)

	# serialize all the parameters as CSV
	csvWriters = {}
	# all the queries have Person as parameter
	for i in range(1,15):
		csvWriter = CSVSerializer()
		csvWriter.setOutputFile(outdir+"interactive_%d_param.txt"%(i))
		if i != 13 and i != 14: # these three queries take two Persons as parameters
			csvWriter.registerHandler(handlePersonParam, selectedPersonParams[i], "personId")
		csvWriters[i] = csvWriter

	# add output for Time parameter
	for i in timeSelectionInput:
		if i==3 or i==4:
			csvWriters[i].registerHandler(handleTimeDurationParam, selectedTimeParams[i], "startDate|durationDays")
		elif i==2 or i==9:
			csvWriters[i].registerHandler(handleMaxTimeParam, selectedTimeParams[i], "maxDate")
		elif i==5:
			csvWriters[i].registerHandler(handleMinTimeParam, selectedTimeParams[i], "minDate")

	# other, query-specific parameters
	csvWriters[1].registerHandler(handleFirstNameParam, nameParams, "firstName")
	csvWriters[3].registerHandler(handlePairCountryParam, zip(selectedCountryParams[3],secondCountry), "countryXName|countryYName")
	csvWriters[6].registerHandler(handleTagParam, selectedTagParams[6], "tagName")
	csvWriters[10].registerHandler(handleMonthParam, months, "month")
	csvWriters[11].registerHandler(handleCountryParam, selectedCountryParams[11], "countryName")
	csvWriters[11].registerHandler(handleWorkYearParam, selectedTimeParams[11], "workFromYear")
	csvWriters[12].registerHandler(handleTagTypeParam, selectedTagTypeParams[12], "tagClassName")
	csvWriters[13].registerHandler(handlePairPersonParam, zip(selectedPersonParams[13], secondPerson[13]), "person1Id|person2Id")
	csvWriters[14].registerHandler(handlePairPersonParam, zip(selectedPersonParams[14], secondPerson[14]), "person1Id|person2Id")


	for j in csvWriters:
		csvWriters[j].writeCSV()
	
if __name__ == "__main__":
	sys.exit(main())
