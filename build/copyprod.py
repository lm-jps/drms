#!/usr/bin/env python3

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
from subprocess import check_call, CalledProcessError, Popen, PIPE
from shlex import quote
import pexpect
import getpass
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../include'))
from drmsparams import DRMSParams

# DEBUG = True
DRY_RUN = False

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
    drmsParams = DRMSParams()

    with Chdir(PROD_ROOTDIR) as ret:
        if ret == 0:
            updatingSUMS = False
            updatingSUMSPort = False
            latestPort = int(drmsParams.get('SUMSD_LISTENPORT'))
            pword = None

            # check to see if we are going to modify the production sumsd.py
            print('checking for a sumsd.py update...')
            if CheckForFileUpdate(os.path.join(SUMS_SOURCE_DIR, SUMS_DAEMON), JSOC_ROOTDIR, os.path.join(WAYSTATION, JSOC_ROOTDIR)):
                print('updating sumsd.py')
                updatingSUMS = True
            else:
                print('no sumsd.py update')

            print('checking for a SUMS port update')
            newSUMSPort = CheckForPortNumberUpdate(JSOC_ROOTDIR, os.path.join(WAYSTATION, JSOC_ROOTDIR))
            if newSUMSPort is not None:
                print('updating SUMS port to ' + str(newSUMSPort))
                updatingSUMSPort = True
            else:
                print('no port update')

            if updatingSUMSPort and not updatingSUMS:
                raise Exception('if you are updating the sumsd port, then you must also update sumsd.py')

            # always stop the current Development/JSOC sumsd.py; assume that MT SUMS is changing, even if
            # sumsd.py itself is not; a dependency may be changing - it does not hurt to assume sumsd.py is changing
            existingProdSumsd = os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, SUMS_DAEMON)
            # stops all SUMS instances that were started with the prodSumsd script, returning a list of the ports each
            # instance was listening to
            cmdList = [ sys.executable, os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, STOP_SUMS_DAEMON), 'daemon=' + existingProdSumsd, '--quiet' ]
            ConditionalAppend(cmdList, ConditionalConstruction('--instancesfile=', 'INSTANCES_FILE', ''))

            if DRY_RUN:
                print(f'[ DRY_RUN] stopping SUMS (if instances are running), running on k1 `{" ".join(cmdList)}`')
            else:
                print(f'stopping SUMS (if instances are running), running on k1 `{" ".join(cmdList)}`')

            # gotta run this on the SUMS server; get production's password

            # could raise
            if pword is None:
                print('please enter password for ' + SUMS_USER + '@' + SUMS_SERVER)
                pword = getpass.getpass()

            sshCmdList = [ '/usr/bin/ssh', SUMS_USER + '@' + SUMS_SERVER, ' '.join(cmdList) ]

            if DRY_RUN:
                print(f'[ DRY RUN ] stopping SUMS: running `{" ".join(sshCmdList)}`')
            else:
                child = pexpect.spawn(' '.join(sshCmdList))
                child.expect('password:')
                child.sendline(pword.encode('UTF8'))
                child.expect(pexpect.EOF)
                resp = child.before

                portsTerminated = json.loads(resp.decode('UTF8'))['terminated']
                print(os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, STOP_SUMS_DAEMON) + ' ran properly')

                if len(portsTerminated) > 0:
                    print('stopped instance(s)s of ' + existingProdSumsd + ' listening on port(s) ' + ','.join([ str(port) for port in portsTerminated ]))

            # copy files from the waystation to a temporary directory
            if not os.path.exists(JSOC_ROOTDIR_NEW):
                if DRY_RUN:
                    print(f'[ DRY RUN ] making temporary JSOC directory `{JSOC_ROOTDIR_NEW}`' )
                else:
                    os.makedirs(JSOC_ROOTDIR_NEW, exist_ok=True)

            cmdList = [ 'rsync', '-alu', os.path.join(WAYSTATION, JSOC_ROOTDIR, ''), JSOC_ROOTDIR_NEW ] # '' is to add trailing slash
            if DRY_RUN:
                print(f'[ DRY RUN ] copying files: from `{os.path.join(WAYSTATION, JSOC_ROOTDIR)}` to `{JSOC_ROOTDIR_NEW}`')
                print(f'[ DRY RUN ] running {" ".join(cmdList)}')
            else:
                print(f'copying from the waystation ({os.path.join(WAYSTATION, JSOC_ROOTDIR)}) to a new JSOC directory ({JSOC_ROOTDIR_NEW})')
                print(f'running {" ".join(cmdList)}')
                check_call(cmdList)

            cmdList = [ 'chgrp', '-Rh', 'jsoc', JSOC_ROOTDIR_NEW ]
            if DRY_RUN:
                print('[ DRY RUN ] setting group owner to `jsoc`')
                print(f'[ DRY RUN ] running {" ".join(cmdList)}')
            else:
                print('recursively setting to user jsoc the group for the new JSOC directory')
                print(f'running {" ".join(cmdList)}')
                check_call(cmdList)

            cmdList = [ 'chmod', '-R', 'g-w', JSOC_ROOTDIR_NEW ]
            if DRY_RUN:
                print('[ DRY RUN ] removing group write')
                print(f'[ DRY RUN] running {" ".join(cmdList)}')
            else:
                print('recursively removing group write on the new JSOC directory')
                print(f'running {" ".join(cmdList)}')
                check_call(cmdList)

            # rename the production tree to save it
            newProdSumsdDir = JSOC_ROOTDIR + '_' + datetime.now().strftime('%Y%m%d_%H%M%S')
            newProdSumsd = os.path.join(PROD_ROOTDIR, newProdSumsdDir, SUMS_SOURCE_DIR, SUMS_DAEMON)

            cmdList = [ 'mv', JSOC_ROOTDIR, newProdSumsdDir ]
            if DRY_RUN:
                print(f'[ DRY RUN ] renaming the production tree: from `{JSOC_ROOTDIR}` to `{newProdSumsdDir}`')
                print(f'[ DRY RUN] running {" ".join(cmdList)}')
            else:
                print(f'renaming `{JSOC_ROOTDIR}` to `{newProdSumsdDir}`')
                print(f'running {" ".join(cmdList)}')
                check_call(cmdList)

            cmdList = [ 'mv', JSOC_ROOTDIR_NEW, JSOC_ROOTDIR ]
            if DRY_RUN:
                print(f'[ DRY RUN ] renaming the new tree: from `{JSOC_ROOTDIR_NEW}` to `{JSOC_ROOTDIR}`')
                print(f'[ DRY RUN ] running {" ".join(cmdList)}')
            else:
                print(f'renaming `{JSOC_ROOTDIR_NEW}` to `{JSOC_ROOTDIR}`')
                print(f'running {" ".join(cmdList)}')
                check_call(cmdList)

            starting_py_bin = drmsParams.get('BIN_PY3')
            if starting_py_bin is None:
                starting_py_bin = sys.executable

            if newSUMSPort is not None and len(portsTerminated) > 0:
                # the Development/JSOC sumsd.py was stopped for at least one port, AND we are changing the SUMS port;
                # restart the SUMS instances that were shutdown earlier using the new date dir for sumsd.py, with the OLD port numbers
                # start-mt-sums.sh will edit the master file that lists running SUMS processes
                cmdList = [ starting_py_bin, os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, START_SUMS_DAEMON), 'daemon=' + newProdSumsd, '--ports=' + ','.join([ str(port) for port in portsTerminated ]), '--quiet' ]
                ConditionalAppend(cmdList, ConditionalConstruction('--instancesfile=', 'INSTANCES_FILE', ''))
                ConditionalAppend(cmdList, ConditionalConstruction('--logfile=', 'SUMS_LOG_FILE', ''))

                if DRY_RUN:
                    print(f'[ DRY RUN ] restarting the production SUMS daemon(s) on port(s) `{",".join([ str(port) for port in portsTerminated ])}`')
                    print(f'[ DRY RUN ] running on k1 `{" ".join(cmdList)}`')
                else:
                    print(f'restarting the production SUMS daemon(s) on port(s) `{",".join([ str(port) for port in portsTerminated ])}`')
                    print(f'running on k1 `{" ".join(cmdList)}`')

                # gotta run this on the SUMS server
                if pword is None:
                    print('please enter password for ' + SUMS_USER + '@' + SUMS_SERVER)
                    pword = getpass.getpass()

                sshCmdList = [ '/usr/bin/ssh', SUMS_USER + '@' + SUMS_SERVER, ' '.join(cmdList) ]
                if DRY_RUN:
                    print(f'[ DRY_RUN] running `{" ".join(sshCmdList)}`')
                else:
                    print(f'running `{" ".join(sshCmdList)}`')
                    child = pexpect.spawn(' '.join(sshCmdList))
                    child.expect('password:')
                    child.sendline(pword.encode('UTF8'))
                    child.expect(pexpect.EOF)
                    resp = child.before

                    pidsStarted = json.loads(resp.decode('UTF8'))['started'] # list of ints
                    print(os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, START_SUMS_DAEMON) + ' ran properly')
                    print('started pids ' + ','.join([ str(pid) for pid in pidsStarted ]))

            # start a sumsd.py instance using the current production sumsd.py source file and latest port number;
            # use the latest port number (if the port number changed, this will be the new port number, if it will be the old port number);
            # the SUMSD_LISTENPORT parameter value will always be the latest port number
            cmdList = [ starting_py_bin, os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, START_SUMS_DAEMON), 'daemon=' + os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, SUMS_DAEMON), '--ports=' + str(latestPort), '--quiet' ]
            ConditionalAppend(cmdList, ConditionalConstruction('--instancesfile=', 'INSTANCES_FILE', ''))
            ConditionalAppend(cmdList, ConditionalConstruction('--logfile=', 'SUMS_LOG_FILE', ''))

            if DRY_RUN:
                print(f'[ DRY_RUN] re-starting SUMS, running on k1 `{" ".join(cmdList)}`')
            else:
                print(f'starting SUMS, running on k1 `{" ".join(cmdList)}`')

            # gotta run this on the SUMS server
            if pword is None:
                print('please enter password for ' + SUMS_USER + '@' + SUMS_SERVER)
                pword = getpass.getpass()

            sshCmdList = [ '/usr/bin/ssh', SUMS_USER + '@' + SUMS_SERVER, ' '.join(cmdList) ]
            if DRY_RUN:
                print(f'[ DRY_RUN] running `{" ".join(sshCmdList)}`')
            else:
                print(f'running `{" ".join(sshCmdList)}`')
                child = pexpect.spawn(' '.join(sshCmdList))
                child.expect('password:')
                child.sendline(pword.encode('UTF8'))
                child.expect(pexpect.EOF)
                resp = child.before

                pidsStarted = json.loads(resp.decode('UTF8'))['started'] # list of ints
                print(os.path.join(PROD_ROOTDIR, JSOC_ROOTDIR, SUMS_SOURCE_DIR, START_SUMS_DAEMON) + ' ran properly')
                print('started pids ' + ','.join([ str(pid) for pid in pidsStarted ]))

except CalledProcessError as exc:
    if exc.output:
        print('Error running: \n  ' + exc.output, file=sys.stderr)
except ValueError:
    print('Bad arguments in command-line: \n  ' + ' '.join(cmdList[1:]))
except Exception as exc:
    raise # Re-raise
