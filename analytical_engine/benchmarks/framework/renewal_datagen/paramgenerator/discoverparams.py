#!/usr/bin/env python2

import sys
import getopt
import math
#import numpy as np
import random
from operator import itemgetter

# amount of parameters to be mined (as a percentage to the domain size, i.e. total number of persons)
SHARE = 0.01

def allclose(a, b, rtol=1e-05, atol=1e-08):
	return abs(a - b) <= (atol + rtol * abs(b))

def readFactors(f):
	res = []
	for line in f.readlines():
		values = [item if index == 0  else int(item) for (index, item)  in enumerate(line.split(","))]
		res.append(values)

	return res

class Window:
	def __init__(self, paramId, start, end):
		self.param = paramId
		self.start = start
		self.end = end
		self.avg = 0.0
		self.stddev = 0.0
		self.size = end-start+1

	def __str__(self):
		res = "[%d, %d] "%(self.start, self.end)
		res += "size: %d, avg: %0.2f, stddev: %0.2f" % (self.size, self.avg, self.stddev) 
		return res

def getAverageCost(rows, key):
	return float(sum([key(r) for r in rows])) / len(rows)

def getCostStdDev(rows, avg, key):
	return math.sqrt(sum([math.pow(key(r)-avg,2) for r in rows]) / len(rows))

def updateAverageCost(avg, oldelem, newelem, samplesize):
	return avg + (newelem - oldelem) / samplesize;



def findWindows(factors, param, amount, bounds):

	data = factors[bounds[0]: bounds[1]]
	allWindows = []
	start = 0

	initWindow = Window(param, start, amount-1)
	initWindow.avg = getAverageCost(data[start:amount], itemgetter(param))
	initWindow.stddev = getCostStdDev(data[start:amount], initWindow.avg, itemgetter(param))

	s1 = sum([x[param] for x in data[start:start+amount]])
	s2 = sum([x[param]*x[param] for x in data[start:start+amount]])	
	start += 1
	allWindows.append(initWindow)

	while start + amount  < len(data):
		end = start + amount 
		if data[end-1][param]<10:
			break

		window = Window(param, bounds[0]+start, bounds[0]+end-1)

		# update the streaming stats about avg and stddev
		s1 -= data[start-1][param]
		s1 += data[end-1][param]
		s2 -= data[start-1][param]*data[start-1][param]
		s2 += data[end-1][param]*data[end-1][param]

		window.avg = float(s1) / amount
		window.stddev = math.sqrt(float(amount*s2 - s1*s1))/amount

		allWindows.append(window)
		start+=1

	allWindows.sort(key=lambda windows: windows.stddev)

	res = []
	first = allWindows[0]
	iter = 0

	while iter < len(allWindows) and allWindows[iter].stddev == first.stddev:
		res.append(allWindows[iter])
		iter+=1
	#print "windows with the minimal std.dev: ",len(res)

	return res

def mergeWindows(windows):
	res = []

	cur = windows[0]

	iter = 1
	constucted = cur

	while iter < len(windows):
		while iter < len(windows) and windows[iter].start == cur.start+1 and allclose(windows[iter].avg, cur.avg):
			cur = windows[iter]
			constucted.end=cur.end
			constucted.size+=1
			iter+=1

		res.append(constucted)
		if iter >= len(windows):
			break

		constucted = windows[iter]
		cur = windows[iter]
		iter += 1

	return res


def generate(factors, portion=SHARE):
	amount = int(len(factors)*portion)
	params = len(factors[0]) -1

	keys = [i for i in range(1,params+1)]
	factors.sort(key=apply(itemgetter, keys), reverse=True)
	result = []
	paramId = 1

	current_windows = findWindows(factors,paramId,amount,(0,len(factors)));

	#print "current windows length: ",len(current_windows)

	while len(current_windows) > 1 and paramId < params:
		paramId += 1
		current_windows = mergeWindows(current_windows)
		#print "Merged windows: ", len(current_windows)
		#print "------------------ Parameter #", paramId

		new_windows = []
		for w in current_windows:
			w2 = findWindows(factors,paramId, amount, (w.start, w.end+1))
			new_windows.extend(w2)

		new_windows.sort(key=lambda w: w.stddev)
		#print "new windows size ", len(new_windows)
		current_windows = []
		first = new_windows[0]
		iter = 0

		while iter < len(new_windows) and new_windows[iter].stddev == first.stddev :
			current_windows.append(new_windows[iter])
			iter+=1
	
	#print "-------------------------"
	#print "Final result: ",len(current_windows)," window(s)"
	#print "Head of the result:"
	#print "Window: ", current_windows[0]
	w = current_windows[0]

	result.extend([factors[w.start+i][0] for i in range(amount)])
	return result

def divideFactors(factors, splitPortion):
  sortedFactors = sorted(factors, key=lambda (k,v): v, reverse=True)
  oSum= 0
  for l in sortedFactors:
    oSum+=l[1]
  splitPoint = splitPortion*oSum
  leftFactors = []
  rightFactors = []
  currentSum = 0
  for l in sortedFactors:
    currentSum+=l[1]
    if currentSum <= splitPoint:
      leftFactors.append(l)
    else:
      rightFactors.append(l)
  return(leftFactors,rightFactors)







