#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import pwd
from subprocess import check_output, check_call, call, Popen, CalledProcessError

PROD_ROOTDIR = '/home/jsoc/cvs/Development'
# PROD_ROOTDIR = '/tmp/arta'
WAYSTATION = 'waystation'
WAYSTATION_USER = 'arta'

RV_SUCCESS = 0
RV_ERROR_WRONGUSER = 1
RV_ERROR_ARGS = 2
RV_ERROR_MAKE = 3

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

# Allow only arta to modify the files in the waystation.
if pwd.getpwuid(os.getuid())[0] != WAYSTATION_USER:
    sys.exit(RV_ERROR_WRONGUSER)

# Turn off debug builds.
os.environ['JSOC_DEBUG'] = '0'

# Make sure the JSOCROOT is PROD_ROOTDIR + '/JSOC'
os.environ['JSOCROOT'] = PROD_ROOTDIR + '/JSOC'

# Unset 'GLOBALHSTAGOVERRIDE'
del os.environ['GLOBALHSTAGOVERRIDE']

try:
    with Chdir(PROD_ROOTDIR + '/' + WAYSTATION + '/JSOC') as ret:
        # os.chdir does NOT change the environment variable $PWD. But our make system relies on PWD being the current directory.
        os.environ['PWD'] = os.path.realpath(os.getcwd())

        cmdList = ['./configure', '-d']
        check_call(cmdList)
        
        cmdList = ['make']
        check_call(cmdList)
        cmdList = ['/usr/bin/make', 'dsds']
        check_call(cmdList)

        cmdList = ['chgrp', '-Rh', 'jsoc', '.']
        check_call(cmdList)
        cmdList = ['chmod', '-R', 'g-w', '.']
        check_call(cmdList)

        sys.exit(RV_SUCCESS);
except CalledProcessError as exc:
    if exc.output:
        print('Error calling make: ' + exc.output, file=sys.stderr)
    sys.exit(RV_ERROR_MAKE);
except ValueError:
    print('Bad arguments to make: \n' + '\n'.join(cmdList[1:]))
    sys.exit(RV_ERROR_MAKE);
except Exception as exc:
    raise # Re-raise
