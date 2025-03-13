#!/usr/bin/env python2

import sys
import getopt
import math
from readfactors import FactorCount
from operator import itemgetter
import itertools

LAST_MONTHS = 3 # number of months that we consider for date parameters in the filters of a form timestamp <= Date0
START_YEAR = 2010 # default value that gets over-written from the data generator output

class MonthYearCount:
	def __init__(self, month, year, count):
		self.month=month
		self.year=year
		self.count=count


class TimeParameter:
	def __init__(self, year, month, day, duration):
		self.month=month
		self.year=year
		self.day=day
		self.duration=duration

def findTimeParameters(persons, factors, procedure, timestampSelection):
	if "w" == procedure:
		medians = computeTimeMedians(factors, lastmonthcount = 12)
	else:
		medians = computeTimeMedians(factors)

	timeParams = timestampSelection(factors,medians)

	return timeParams


def getMedian(data, sort_key, getEntireTuple = False):
	if len(data) == 0:
		if getEntireTuple:
			return MonthYearCount(0,0,0)
		return 0

	if len(data) == 1:
		if getEntireTuple:
			return data[0]
		return data[0].count

	srtd = sorted(data,key=sort_key)
	mid = len(data)/2

	if len(data) % 2 == 0:
		if getEntireTuple:
			return srtd[mid]
		return (sort_key(srtd[mid-1]) + sort_key(srtd[mid])) / 2.0

	if getEntireTuple:
		return srtd[mid]
	return sort_key(srtd[mid])


def MonthYearToDate(myc, day):
	return "%d-%d-%d"%(myc.year, myc.month, day)


def getTimeParamsWithMedian(factors, (medianFirstMonth, medianLastMonth, median)):
	# strategy: find the median of the given distribution, then increase the time interval until it matches the given parameter
	res = []
	for values in factors:
		input = sorted(values,key=lambda myc: (myc.year, myc.month))
		currentMedian = getMedian(values,lambda myc: myc.count, True)
		if int(median) == 0 or int(currentMedian.count) == 0 or int(currentMedian.year) == 0:
			res.append(TimeParameter(START_YEAR,1,1,0))
			continue
		if currentMedian.count > median:
			duration = int(28*currentMedian.count/median)
			res.append(TimeParameter(currentMedian.year, currentMedian.month, 1, duration))
		else:
			duration = int(28*median/currentMedian.count)
			res.append(TimeParameter(currentMedian.year, currentMedian.month, 1, duration))
	return res

def getTimeParamsBeforeMedian(factors, (medianFirstMonth, medianLastMonth, median)):
	# strategy: find the interval [0: median] with the sum of counts as close as possible to medianFirstMonth
	res = []
	i = 0
	for values in factors:
		input = sorted(values,key=lambda myc: (myc.year, myc.month))
		localsum = 0
		best = MonthYearCount(0,0,0)
		for myc in input:
			localsum += myc.count
			i+=1
			if localsum >= medianFirstMonth:
				day = max(28 -28*(localsum-medianFirstMonth)/myc.count,1)
				res.append(TimeParameter(myc.year, myc.month, day, None))
				break
			best = myc

		if localsum < medianFirstMonth:
			res.append(TimeParameter(best.year, best.month, 28, None))

	return res

def getTimeParamsAfterMedian(factors, (medianFirstMonth, medianLastMonth, median)):
	# strategy: find the interval [median: end] with the sum of counts as close as possible to medianFirstMonth
	res = []

	for values in factors:
		input = sorted(values,key=lambda myc: (-myc.year, -myc.month)) 
		localsum = 0
		best = MonthYearCount(0,0,0)
		for myc in input:
			localsum += myc.count
			if localsum >= medianLastMonth:
				day = max(28 * (localsum-medianLastMonth)/myc.count,1)
				res.append(TimeParameter(myc.year, myc.month, day, None))
				break
			best = myc

		if localsum < medianLastMonth:
			res.append(TimeParameter(best.year, best.month, 1, None))
	return res

def computeTimeMedians(factors, lastmonthcount = LAST_MONTHS):
	mediantimes = []
	lastmonths = []
	firstmonths = []
	for values in factors:
		values.sort(key=lambda myc: (myc.year, myc.month))

		l = len(values)
		lastmonthsum = sum(myc.count for myc in values[max(l-lastmonthcount,0):l])
		lastmonths.append(lastmonthsum)
		cutoff_max = l-lastmonthcount
		if cutoff_max < 0:
			cutoff_max = l
		firstmonthsum = sum(myc.count for myc in values[0:cutoff_max])
		firstmonths.append(firstmonthsum)
		mediantimes.append(getMedian(values,lambda myc: myc.count))

	median = getMedian(mediantimes, lambda x: x)
	medianLastMonth = getMedian(lastmonths, lambda x: x)
	medianFirstMonth = getMedian(firstmonths, lambda x: x)
	
	return (medianFirstMonth, medianLastMonth, median)	

def readTimeParams(persons, personFactorFiles, activityFactorFiles, friendFiles):

	postCounts = {}
	groupCounts = {}
	offset = 8
	monthcount = 12*3 + 1

	for inputFactorFile in personFactorFiles:
		with open(inputFactorFile, 'r') as f:
			for line in f.readlines():
				line = line.split(",")
				person = int(line[0])
				localPostCounts = map(int,line[offset:offset+monthcount])
				localGroupCounts = map(int, line[offset+monthcount:])
				if not person in postCounts:
					postCounts[person] = localPostCounts
				else:
					postCounts[person] =  [sum(x) for x in zip(postCounts[person], localPostCounts)]

				if not person in groupCounts:
					groupCounts[person] = localGroupCounts
				else:
					groupCounts[person] = [sum(x) for x in zip(groupCounts[person], localGroupCounts)]

	friendsPostsCounts = {}
	fGroupCount = {}
	for inputFriendFile in friendFiles:
		with open(inputFriendFile, 'r') as f:
			for line in f:
				people = map(int, line.split(","))
				person = people[0]
				friendsPostsCounts[person] = [0]*monthcount
				for friend in people[1:]:
					if not friend in postCounts:
						continue
					friendsPostsCounts[person] = [x+y for x,y in zip(friendsPostsCounts[person], postCounts[friend])]				
				fGroupCount[person] = [0]*monthcount
				for friend in people[1:]:
					if not friend in groupCounts:
						continue
					fGroupCount[person] = [x+y for x,y in zip(fGroupCount[person], groupCounts[friend])]


	ffPostCounts = {}
	ffGroupCount = {}
	for inputFriendFile in friendFiles:
		with open(inputFriendFile, 'r') as f:
			for line in f:
				people = map(int, line.split(","))
				person = people[0]
				ffPostCounts[person] = [0]*monthcount
				for friend in people[1:]:
					if not friend in friendsPostsCounts:
						continue
					ffPostCounts[person] = [x+y for x,y in zip(ffPostCounts[person],friendsPostsCounts[friend])]
				ffGroupCount[person] = [0]*monthcount
				for friend in people[1:]:
					if not friend in fGroupCount:
						continue
					ffGroupCount[person] = [x+y for x,y in zip(ffGroupCount[person],fGroupCount[friend])]

	return (friendsPostsCounts, ffPostCounts, ffGroupCount)



def findTimeParams(input, personFactorFiles, activityFactorFiles, friendFiles, startYear):
	START_YEAR = startYear
	fPostCount = {}
	ffPostCount = {}
	persons = []
	for queryId in input:
		persons += input[queryId][0]

	(fPostCount, ffPostCount, ffGroupCount) = readTimeParams(set(persons),personFactorFiles, activityFactorFiles, friendFiles)

	mapParam = {
		"f" : fPostCount,
		"ff": ffPostCount,
		"ffg": ffGroupCount
	}

	output = {}
	for queryId in input:
		factors = mapParam[input[queryId][1]]
		mycFactors  = []
		for person in input[queryId][0]:
			countsPerMonth = factors[person]
			myc = []
			for (month,count) in enumerate(countsPerMonth):
				if count == 0:
					continue
				year = startYear + month / 12
				myc.append(MonthYearCount(month % 12 + 1, int(year), count))
			mycFactors.append(myc)

		output[queryId] = findTimeParameters(input[queryId][0], mycFactors, input[queryId][1], input[queryId][2])

	return output

def main(argv=None):
	if argv is None:
		argv = sys.argv

	if len(argv)< 2:
		print "arguments: <input persons file>"
		return 1

	f = open(argv[1])

	factors = prepareTimedFactors(f, getFriendFriendPostByTime)

	medians = computeTimeMedians(factors)

	timestamps = getTimeParamsWithMedian(factors,medians)

if __name__ == "__main__":
	sys.exit(main())