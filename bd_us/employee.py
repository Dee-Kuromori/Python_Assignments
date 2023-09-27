#!/usr/bin/env python
import sys
import csv

sum = 0
columns = sys.stdin
next(columns)
# print(columns)
for line in columns:
    # line = line.split()
    line = line.split(',')

    if line[1] == 'Mark Wilson':
        print("The user is exist!!")
        break

# key = fields[0]
# salary = fields[2]
# if not line:
#    continue
# salary = float(salary.strip())
# sum +=  salary
# print(key,salary,sum)
