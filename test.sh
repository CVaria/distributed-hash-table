#!/usr/bin/env python

import sys

counter = 0
time = 0

fp = open(sys.argv[1])
line = fp.readline()
while line:
	tokens = line.split(" ")
	counter = counter+1
	time = time + long(tokens[1])
	line = fp.readline()		 
fp.close

print counter
print time

print float(time)/(1000 * counter)
