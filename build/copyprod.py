#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import re
from subprocess import check_output, check_call, CalledProcessError
import smtplib
from datetime import datetime

# Hard code a bunch of stuff since I don't have time to do this correctly.
PROD_ROOTDIR = '/home/jsoc/cvs/Development'
WAYSTATION = 'waystation'

RV_SUCCESS = 0

# This class changes the current working directory, and restores the original working directory when
# the context is left.
class Chdir:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = os.path.realpath(newPath)
    
    def __enter__(self):
        self.savedPath = os.path.realpath(os.getcwd())
        os.chdir(self.newPath)
        cdir = os.path.realpath(os.getcwd())
        if cdir == self.newPath:
            return 0
        else:
            return 1
    
    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
        cdir = os.path.realpath(os.getcwd())
        if cdir == self.savedPath:
            return 0
        else:
            return 1

msg = None
rv = RV_SUCCESS

try:
    with Chdir(PROD_ROOTDIR) as ret:
        if ret == 0:
            cmdList = ['rsync', '-alu', WAYSTATION + '/JSOC/', 'JSOC.new/JSOC']
            check_call(cmdList)
            cmdList = ['chgrp', '-R', 'jsoc', 'JSOC.new/JSOC']
            check_call(cmdList)
            cmdList = ['chmod', '-R', 'g-w', 'JSOC.new/JSOC']
            check_call(cmdList)
            cmdList = ['mv', 'JSOC',  'JSOC_' + datetime.now().strftime('%Y%m%d_%S')]
            check_call(cmdList)
            cmdList = ['mv', 'JSOC.new/JSOC', 'JSOC']
            check_call(cmdList)

except CalledProcessError as exc:
    if exc.output:
        print('Error calling rsync: ' + exc.output, file=sys.stderr)
except ValueError:
    print('Bad arguments to rsync: \n' + '\n'.join(cmdList[1:]))
except Exception as exc:
    raise # Re-raise
