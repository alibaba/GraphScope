#!/usr/bin/env python2

import sys
import getopt
import codecs

FACTORS = ["f", "ff", "fp", "fpt", "ffg", "p", "pl", "pt", "pr", "g", "w", "ffw", "ffp", "fw", "fg", "ffpt", "fpr", "org"]

FACTOR_MAP = {value: key for (key, value) in enumerate(FACTORS)}


class FactorCount:
	def __init__(self):
		self.values = [0]*len(FACTORS)

	def setValue(self, factor, value):
		self.values[FACTOR_MAP[factor]] = value

	def addValue(self, factor, value):
		self.values[FACTOR_MAP[factor]] += value

	def getValue(self, factor):
		return self.values[FACTOR_MAP[factor]]

class Factors:
	def __init__(self, persons = []):
		self.values = {}
		for p in persons:
			self.values[p] = FactorCount()

	def addNewParam(self, p):
		self.values[p] = FactorCount()

	def existParam(self, p):
		return p in self.values

	def setValue(self, person, factor, value):
		self.values[person].setValue(factor, value)

	def getValue(self, person, factor):
		return self.values[person].getValue(factor)

	def addValue(self, person, factor, value):
		self.values[person].addValue(factor, value)

class NameParameter:
	def __init__(self, persons=[]):
		self.values={}
		for p in persons:
			self.values[p] = 0
	
	def setValue(self, person, value):
		self.values[person] = value

	def getValue(self, person):
		return self.values[person]

def load(personFactorFiles,activityFactorFiles, friendFiles):
	print "loading input for parameter generation"
	results = Factors()
	countries = Factors()
	postsHisto = Factors()
	givenNames = NameParameter()

	tagClasses = {}
	tags = {}
	names = {}
	timestamp = [0,0,0,0]

	for inputfileName in personFactorFiles:
		with codecs.open(inputfileName, "r", "utf-8") as f:
			for line in f.readlines():
				line = line.split(",")
				person = int(line[0])
				if not results.existParam(person):
					results.addNewParam(person)
				name = line[1]
				givenNames.setValue(person, name)
				results.addValue(person, "f", int(line[2]))
				results.addValue(person, "p", int(line[3]))
				results.addValue(person, "pl", int(line[4]))
				results.addValue(person, "pt", int(line[5]))
				results.addValue(person, "g", int(line[6]))
				results.addValue(person, "w", int(line[7]))
				results.addValue(person, "pr", int(line[8]))
				for i in range((len(line)-9)/2):
					if not postsHisto.existParam(i):
						postsHisto.addNewParam(i)
					postsHisto.addValue(i, "p", int(line[9+i]))

	for inputFileName in activityFactorFiles:
		with codecs.open(inputFileName, "r", "utf-8") as f:
			countryCount = int(f.readline())
			for i in range(countryCount):
				line = f.readline().split(",")
				country = line[0]
				if not countries.existParam(country):
					countries.addNewParam(country)
				countries.addValue(country, "p", int(line[1]))

			tagCount = int(f.readline())
			for i in range(tagCount):
				line = f.readline().split(",")
				tag = line[0]
				if not tag in tagClasses:
					tagClasses[tag] = 0
				tagClasses[tag] += int(line[2])

			tagCount = int(f.readline())
			for i in range(tagCount):
				line = f.readline()
				count = line[1+line.rfind(","):]
				name = line[:line.rfind(",")]
				if not name in tags:
					tags[name] = 0
				tags[name] += int(count)

			nameCount = int(f.readline())
			for i in range(nameCount):
				line = f.readline().split(",")
				name = line[0]
				if not name in names:
					names[name] = 0
				names[name] += int(line[1])

			for i in range(4):
				t = f.readline().rstrip()
				if timestamp[i] == 0 and t != 'null':
					timestamp[i] = int(t)

	loadFriends(friendFiles, results)

	return (results, countries, tags.items(), tagClasses.items(), names.items(), givenNames,timestamp, postsHisto)

def loadFriends(friendFiles, factors):

	# scan the friends list and sum up the counts related to friends (number of posts of friends etc)
	for inputFriendsFileName in friendFiles:
		with open(inputFriendsFileName, 'r') as f:
			for line in f:
				people = map(int, line.split(","))
				person = people[0]
				if not factors.existParam(person):
					continue
				for friend in people[1:]:
					if not factors.existParam(friend):
						continue
					factors.addValue(person, "ff", factors.getValue(friend, "f"))
					factors.addValue(person, "fp", factors.getValue(friend, "p"))
					factors.addValue(person, "fpt", factors.getValue(friend, "pt"))
					factors.addValue(person, "fw", factors.getValue(friend, "w"))
					factors.addValue(person, "fg", factors.getValue(friend, "g"))

	# second scan for friends-of-friends counts (groups of friends of friends)
	for inputFriendsFileName in friendFiles:
		with open(inputFriendsFileName, 'r') as f:
			for line in f:
				people = map(int, line.split(","))
				person = people[0]
				if not factors.existParam(person):
					continue
				for friend in people[1:]:
					if not factors.existParam(friend):
						continue
					factors.addValue(person, "ffg", factors.getValue(friend, "fg"))
					factors.addValue(person, "ffw", factors.getValue(friend, "fw"))
					factors.addValue(person, "ffp", factors.getValue(friend, "fp"))
					factors.addValue(person, "ffpt", factors.getValue(friend, "fpt"))

def getColumns(factors, columnNames):
	res = []
	for key in factors.values:
		factor = factors.values[key]
		values = []
		values.append(key)
		values.extend([factor.getValue(column) for column in columnNames])
		res.append(values)
	return res


def getFactorsForQuery(queryId, factors):

	queryFactorDict = {
		1: getColumns(factors, ["f", "ff"]),
		2: getColumns(factors, [ "f", "fp"]),
		3: getColumns(factors, ["ff", "ffp"]),
		4: getColumns(factors, ["fp", "f",  "fpt"]),
		5: getColumns(factors, ["ff", "ffg"]),	
		6: getColumns(factors, ["f","ff", "ffp", "ffpt"]),
		7: getColumns(factors, ["pl", "p"]),
		8: getColumns(factors, ["pr","p"]), ### add "pr"
		9: getColumns(factors, ["f", "ffp", "ff"]),
		10: getColumns(factors, ["f","ff", "ffp", "ffpt"]),
		11: getColumns(factors, ["f","ff", "ffw"]),
		12: getColumns(factors, ["f", "fp"]), ### add "fpr"
		13: getColumns(factors, ["ff"]),
		14: getColumns(factors, ["ff"])
	}

	return queryFactorDict[queryId]

def getCountryFactorsForQuery(queryId, factors):
	queryFactorDict = {
		3: getColumns(factors, ["p"]),
		11: getColumns(factors, ["p"]) ### replace with "org"
	}

	return queryFactorDict[queryId]

def getTagFactorsForQuery(queryId, factors):
	queryFactorDict = {
		6: getColumns(factors, ["p"]),
	}

	return queryFactorDict[queryId]

if __name__ == "__main__":
	argv = sys.argv
	if len(argv)< 3:
		print "arguments: <m0factors.txt> <m0friendsList.txt>"
		sys.exit(1)

	sys.exit(load(argv[1], argv[2]))


