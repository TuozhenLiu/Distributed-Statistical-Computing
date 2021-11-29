#! /usr/bin/env python3

import sys

dict_ = dict()
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    city, num = line.split('\t')
    if city in dict_:
        dict_[city].append(int(num))
    else:
        dict_[city] = [int(num)]

for city, num_list in dict_.items():
    avg_num = sum(num_list) / len(num_list)
    # print(f"{city}: {avg_num}")
    print("%s: %.2f" % (city, avg_num))
