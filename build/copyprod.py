#!/usr/bin/env python

# Use this script to copy all or part of the JSOC waystation tree to the production tree. Without any
# arguments, the existing tree is moved to JSOC_YYYYMMDD_HHMMSS, and the tree in waystation is copied to
# JSOC. 
#
# If an argument is given, then it must either be a file in the make-output directory of the waystation,
# or it must be the string __ALL_MODIFIED__. If the argument is the former, then an rsync is performed for that
# specify file to copy it from the waystation to the production tree. If the argument is __ALL__, then 
# ALL files whose checksums differ on the waystation when they are compared against their counterparts on
# the production tree are copied from the waystation to the production tree.
#
# If any action taken by the script will cause sumsd.py to be updated, then the script first shuts down 
# the production sumsd.py running on k1, then it moves sumsd.py to the JSOC_YYYYMMDD_HHMMSS directory, 
# then it starts the sumsd.py from the JSOC_YYYYMMDD_HHMMSS directory.
#
# IFF SUMSD_LISTENPORT (the port on k1 that sumsd.py listens on) in configsdp.txt in the waystation differs 
# from configsdp.txt in the production tree, then there was a change to sumsd.py that has caused it to become 
# incompatible with the client code in the production tree. In this case, this script will invoke the sumsd.py 
# daemon in the production tree with SUMSD_LISTENPORT.

import sys
import os
import re
import json
from subprocess import check_output, check_call, CalledProcessError, Popen, PIPE
from shlex import quote
import smtplib
from datetime import datetime

# DEBUG = True

# hard code a bunch of stuff since I don't have time to do this correctly
if 'DEBUG' in globals():
    PROD_ROOTDIR = os.path.join('/', 'tmp', 'arta')
    JSOC_ROOTDIR = 'JSOC'
    JSOC_ROOTDIR_NEW = os.path.join('JSOC.new', 'JSOC')
    WAYSTATION = 'waystation'
    JSOC_CONFIG = 'configsdp.txt'
    SUMS_SERVER = 'rumble'
    SUMS_USER = 'arta'
    SUMS_SOURCE_DIR = os.path.join('base', 'sums', 'scripts')
    SUMS_DAEMON = 'sumsd.py'
    START_SUMS_DAEMON = 'start-mt-sums.py'
    STOP_SUMS_DAEMON = 'stop-mt-sums.py'
    INSTANCES_FILE = os.path.join('/', 'tmp', 'arta', 'testinstances.txt')
    SUMS_LOG_FILE = os.path.join('/', 'tmp', 'arta', 'testsumslog.txt')

else:
    PROD_ROOTDIR = os.path.join('/', 'home', 'jsoc', 'cvs', 'Development')
    JSOC_ROOTDIR = 'JSOC'
    JSOC_ROOTDIR_NEW = os.path.join('JSOC.new', 'JSOC')
    WAYSTATION = 'waystation'
    JSOC_CONFIG = 'configsdp.txt'    
    SUMS_SERVER = 'k1'
    SUMS_USER = 'production'
    SUMS_SOURCE_DIR = os.path.join('base', 'sums', 'scripts')
    SUMS_DAEMON = 'sumsd.py'
    START_SUMS_DAEMON = 'start-mt-sums.py'
    STOP_SUMS_DAEMON = 'stop-mt-sums.py'
    # INSTANCES_FILE = 'testinstances.txt'
    # SUMS_LOG_FILE = '/usr/local/logs/SUM/testsumslog.txt'


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

def CheckForFileUpdate(file, prod, waystation):
    cmd = 'rsync -aciluv --dry-run ' + quote(os.path.join(waystation, file)) + ' ' + quote(os.path.join(prod, file)) + ' | egrep ' + "'" + '^>.*c\S*\s' + "'" + ' | cut -d' + "'" + ' ' + "'" + ' -f2'
    print('running ' + cmd)
    proc = Popen(cmd, shell=True, stdin=None, stdout=PIPE, stderr=None)
    outs, errs = proc.communicate(input=None, timeout=30)
    return outs.decode('UTF8').strip() == os.path.basename(file).strip()
    
def CheckForPortNumberUpdate(prod, waystation):
    rv = None
    
    # original port number
    originalPort = None
    cmd = 'diff -w ' + quote(os.path.join(prod, JSOC_CONFIG)) + ' ' + quote(os.path.join(waystation, JSOC_CONFIG)) + ' | egrep -o ' + "'" + '^<\s.+SUMSD_LISTENPORT\s+[0-9]+' + "'" + ' | egrep -o ' + "'" + '[0-9]+' + "'"
    print('running ' + cmd)
    proc = Popen(cmd, shell=True, stdin=None, stdout=PIPE, stderr=None)
    outs, errs = proc.communicate(input=None, timeout=30)
    
    if len(outs) > 0:
        originalPort = int(outs.decode('UTF8'))
    
    if originalPort:
        # new port number (we have to return this so we can start a sumsd.py that listens on this port)
        newPort = None
        cmd = 'diff -w ' + quote(os.path.join(prod, JSOC_CONFIG)) + ' ' + quote(os.path.join(waystation, JSOC_CONFIG)) + ' | egrep -o ' + "'" + '^>\s.+SUMSD_LISTENPORT\s+[0-9]+' + "'" + ' | egrep -o ' + "'" + '[0-9]+' + "'"
        print('running ' + cmd)
        proc = Popen(cmd, shell=True, stdin=None, stdout=PIPE, stderr=None)
        outs, errs = proc.communicate(input=None, timeout=30)

        if len(outs) > 0:
            newPort = int(outs.decode('UTF8'))
    
        if newPort is not None and newPort != originalPort:
            rv = newPort
            
    # return the new port number
    return rv
    
def ConditionalAppend(l, elem):
    if elem and len(elem) > 0:
        l.append(elem)
        
def ConditionalConstruction(p, v, s):
    if v in globals():
        return p + globals()[v] + s
    return ''

msg = None
rv = RV_SUCCESS

try:
    with Chdir(PROD_ROOTDIR) as ret:
        if ret == 0:
            updatingSUMS = False
            updatingSUMSPort = False

            # check to see if we are going to modify the production sumsd.py
            print('checking for a SUMS update...')
            if CheckForFileUpdate(os.path.join(SUMS_SOURCE_DIR, SUMS_DAEMON), JSOC_ROOTDIR, os.path.join(WAYSTATION, JSOC_ROOTDIR)):
                print('updating SUMS')
                updatingSUMS = True
            else:
                print('no SUMS update')
            
            print('checking for a port update')
            newSUMSPort = CheckForPortNumberUpdate(JSOC_ROOTDIR, os.path.join(WAYSTATION, JSOC_ROOTDIR))
            if newSUMSPort is not None:
                print('updating SUMS port to ' + str(newSUMSPort))
                updatingSUMSPort = True
            else:
                print('no port update')

            if updatingSUMSPort and not updatingSUMS:
                raise Exception('if you are updating the sumsd port, then you must also update sumsd')
            
            # if so, then we need to stop the existing use of it on k1; to do that, run the sumsd.py stop
            # script
            if updatingSUMS:
                existingProdSumsd = os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, SUMS_DAEMON)
                # stops all SUMS instances that were started with the prodSumsd script, returning a list of the ports each
                # instance was listening to
                cmdList = [ sys.executable, os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, STOP_SUMS_DAEMON), 'daemon=' + existingProdSumsd ]
                ConditionalAppend(cmdList, ConditionalConstruction('--instancesfile=', 'INSTANCES_FILE', ''))
                ConditionalAppend(cmdList, ConditionalConstruction('--logfile=', 'SUMS_LOG_FILE', ''))
                print('stopping SUMS (if instances are running): ' + ' '.join(cmdList))

                # gotta run this on the SUMS server
                sshCmdList = [ '/usr/bin/ssh', SUMS_USER + '@' + SUMS_SERVER, ' '.join(cmdList) ]
                resp = check_output(cmdList) # raises CalledProcessError if stop-sums-daemon does not return zero
                portsTerminated = json.loads(resp.decode('UTF8'))['terminated']
                if len(portsTerminated) > 0:
                    print('stopped instance(s)s of ' + existingProdSumsd + ' listening on port(s) ' + ','.join([ str(port) for port in portsTerminated ]))
            
            # copy files from the waystation to a temporary directory
            if not os.path.exists(JSOC_ROOTDIR_NEW):
                os.makedirs(JSOC_ROOTDIR_NEW, exist_ok=True)
            
            print('copying from the waystation (' + os.path.join(WAYSTATION, JSOC_ROOTDIR) + ') to a new JSOC directory (' + JSOC_ROOTDIR_NEW + ')')
            cmdList = [ 'rsync', '-alu', os.path.join(WAYSTATION, JSOC_ROOTDIR, ''), JSOC_ROOTDIR_NEW ] # '' is to add trailing slash
            print('running ' + ' '.join(cmdList))
            check_call(cmdList)
            print('recursively setting to user jsoc the group for the new JSOC directory')
            cmdList = [ 'chgrp', '-Rh', 'jsoc', JSOC_ROOTDIR_NEW ]
            print('running ' + ' '.join(cmdList))
            check_call(cmdList)
            print('recursively removing group write on the new JSOC directory')
            cmdList = [ 'chmod', '-R', 'g-w', JSOC_ROOTDIR_NEW ]
            print('running ' + ' '.join(cmdList))
            check_call(cmdList)

            # rename the production tree to save it        
            newProdSumsdDir = JSOC_ROOTDIR + '_' + datetime.now().strftime('%Y%m%d_%H%M%S')
            newProdSumsd = os.path.join(PROD_ROOTDIR, newProdSumsdDir, SUMS_SOURCE_DIR, SUMS_DAEMON)
            print('renaming ' + JSOC_ROOTDIR + ' to ' + newProdSumsdDir)
            cmdList = [ 'mv', JSOC_ROOTDIR, newProdSumsdDir ]
            print('running ' + ' '.join(cmdList))
            check_call(cmdList)
            print('renaming ' + JSOC_ROOTDIR_NEW + ' to ' + JSOC_ROOTDIR)
            cmdList = [ 'mv', JSOC_ROOTDIR_NEW, JSOC_ROOTDIR ]
            print('running ' + ' '.join(cmdList))
            check_call(cmdList)
            
            if updatingSUMS and len(portsTerminated) > 0:
                # restart the prodSumsd SUMS instances that were shutdown earlier
                # start-mt-sums.sh will edit the master file that lists running SUMS processes
                cmdList = [ sys.executable, os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, START_SUMS_DAEMON), 'daemon=' + newProdSumsd, 'ports=' + ','.join([ str(port) for port in portsTerminated ]) ]
                ConditionalAppend(cmdList, ConditionalConstruction('--instancesfile=', 'INSTANCES_FILE', ''))
                ConditionalAppend(cmdList, ConditionalConstruction('--logfile=', 'SUMS_LOG_FILE', ''))
                print('restarting the production SUMS daemon(s) on port(s) ' + ','.join([ str(port) for port in portsTerminated ]))
                
                # gotta run this on the SUMS server
                sshCmdList = [ '/usr/bin/ssh', SUMS_USER + '@' + SUMS_SERVER, ' '.join(cmdList) ]
                print('running ' + ' '.join(sshCmdList))
                check_call(cmdList) # raises CalledProcessError if start-sums-daemon does not return zero
              
            if newSUMSPort is not None:      
                # start a sumsd.py instance using the current production sumsd.py source file
                cmdList = [ sys.executable, os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, START_SUMS_DAEMON), 'daemon=' + os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, SUMS_DAEMON), 'ports=' + str(newSUMSPort) ]
                ConditionalAppend(cmdList, ConditionalConstruction('--instancesfile=', 'INSTANCES_FILE', ''))
                ConditionalAppend(cmdList, ConditionalConstruction('--logfile=', 'SUMS_LOG_FILE', ''))
                
                # gotta run this on the SUMS server
                sshCmdList = [ '/usr/bin/ssh', SUMS_USER + '@' + SUMS_SERVER, ' '.join(cmdList) ]
                print('running ' + ' '.join(sshCmdList))
                check_call(cmdList) # raises CalledProcessError if start-sums-daemon does not return zero
except CalledProcessError as exc:
    if exc.output:
        print('Error calling rsync: ' + exc.output, file=sys.stderr)
except ValueError:
    print('Bad arguments to rsync: \n' + '\n'.join(cmdList[1:]))
except Exception as exc:
    raise # Re-raise
