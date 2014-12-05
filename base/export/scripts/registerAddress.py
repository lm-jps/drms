#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import fileinput
import re
from datetime import datetime
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

# Print stdin
# import fileinput
# fobj = open('/home/jsoc/thefile2.txt', 'a')
# for line in fileinput.input():
#    print(line, file=fobj)

regExpA = re.compile(r'From\s(\S+)\s')
regExpS = re.compile(r'Subject:.+CONFIRM\sEXPORT\sADDRESS\s+\[(\S+)\]')

address = None
confirmation = None

for line in fileinput.input():
    if len(line) == 0:
        continue
    
    if address is None:
        matchObj = regExpA.match(line)
        if matchObj:
            address = matchObj.group(1)

    if confirmation is None:
        matchObj = regExpS.match(line)
        if matchObj:
            confirmation = matchObj.group(1)

    if address and confirmation:
        break

fobj = open('/home/jsoc/thefile3.txt', 'w')
print('address is ' + address + ' conf is ' + confirmation, file=fobj)
