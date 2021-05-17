#!/usr/bin/env python

import re
import sqlite3
from collections import defaultdict

def parse_log(filename):
	pattern = re.compile('NEW CONNECTIONS\s.*SRC=(\S+)\sDST=(\S+)\s.*PROTO=(\S+)\sSPT=(\S+)\sDPT=(\S+)')
	str_list = []
	
	with open(filename, 'r') as fp:
		for line in fp:

			result = re.search(pattern, line)
			if result:
					str_list.append((result.group(1),result.group(2),result.group(3),result.group(4),result.group(5)))

	return str_list

def main():

	parsed_list = parse_log('/home/akolesnikov/syslog.1')

	con = sqlite3.connect('testsyslog.db')

	with con:
		cur = con.cursor()
		#cur.execute("DROP TABLE IF EXISTS traff")
		#cur.execute("CREATE TABLE traff(src_ip VARCHAR(16), dst_ip VARCHAR(16), proto VARCHAR(6), spt INT, dpt INT)")
		cur.executemany("INSERT INTO traff VALUES(?, ?, ?, ?, ?)", parsed_list)


if __name__ == '__main__':
	main()