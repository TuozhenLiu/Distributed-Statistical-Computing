#! /usr/bin/env python3

import sys


line_count = 0
word_count = 0

for line in sys.stdin:
	line = line.strip()
	if not line:
		continue
	word = line.split(' ')
	line_count += 1
	word_count += len(word)

print(f"line_count:{line_count}, word_count:{word_count}")

