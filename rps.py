#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys,re


def get_rps(filename, filter):
	pattern = re.compile(filter)
	lst = {}
	with open(filename,'r') as fp:
		for line in fp:
			result = re.search(pattern, line)
			if result:
				tt_re = re.search("\[(\d{2})\/([a-zA-Z]{3})\/(\d{4}):(\d{2}):(\d{2}):(\d{2})\s(\+\d{4})]", line)
				index =  tt_re.group(0)
				if lst.has_key(index):
					lst[index] = lst[index] + 1					
				else: 
					lst[index] = 1
 

 	max = lst.values()[0]
 	vals = []
 	for time in lst:
		print "%s %d" % (time, lst[time])
		vals.append(lst[time])
		if lst[time] > max:
			max = lst[time]

		
	vals.sort()
	if (len(vals) % 2 == 0):
		centr = int((len(vals)/2)-1)
		avg = (vals[centr] + vals[centr+1])/2
	else:
		centr = int(len(vals)/2)
		avg = vals[centr]

	print "Max = %d, avg= %d"  % (max, int(avg)) 
if __name__ == '__main__':
	
	filename = sys.argv[1]
	ipaddr = sys.argv[2]

	get_rps(filename, ipaddr)

