#!/usr/bin/env python

from __future__ import print_function
import sys
import os.path
import argparse
import pwd
import re
import getpass
import socket
import fnmatch
import time
import signal
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

# Return codes
RET_SUCCESS = 0
RET_DRMSPARAMS = 1
RET_INVALIDARG = 2
RET_DBCONNECT = 3
RET_RUNCMD = 4
RET_WRITEPID = 5
RET_USER = 6
RET_HOST = 7
RET_KILLSUMS = 8

PID_FILE = 'sums.pidfile'

def shutdown():
    rv = RET_SUCCESS
    drmsParams = DRMSParams()
    if drmsParams is None:
        print('Cannot open DRMS params file.', file=sys.stderr)
        rv = RET_DRMSPARAMS
    else:
        try:
            sumsLogDir = getDrmsParam(drmsParams, 'SUMLOG_BASEDIR')

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]
            
            if etype == 'drmsParams':
                print('Error reading DRMS parameters: ' + msg, file=sys.stderr)
                rv = RET_DRMSPARAMS

        if rv == RET_SUCCESS:
            actualProcs = readPIDFile(sumsLogDir + '/' + PID_FILE)
            if actualProcs is not None:
                # Kill each SUMS process. It looks like SUMS needs the SIGKILL signal to die.
                for procName in actualProcs:
                    pid = actualProcs[procName]
                    if os.path.exists('/proc/' + str(pid)):
                        os.kill(pid, signal.SIGKILL)
                        killedPid, istat = os.waitpid(pid, os.WNOHANG)
                        if killedPid == 0:
                            print('Unable to kill SUMS process ' + procName + '(pid ' + str(pid) +').')
                            rv = RET_KILLSUMS
                            break

    sys.exit(rv)

def getDrmsParam(drmsParams, name):
    param = drmsParams.get(name)
    if param is None:
        raise Exception('drmsParams', 'Parameter ' + param + ' does not exist.')
    else:
        return param

def readPIDFile(pidFile):
    rv = {}
    try:
        # An exit out of the 'with' scope will cause fin to be closed.
        with open (pidFile, 'r') as fin:
            regExpComment = re.compile(r'\s*[#]')
            regExpNotEmpty = re.compile(r'[^\s\t]')
            regExpValid = re.compile(r'\s*(\S+)\s+(\d+)\s*')

            for line in fin:
                # Skip comment lines
                if regExpComment.match(line):
                    continue
                # Skip empty lines
                if not regExpNotEmpty.search(line):
                    continue
                matchObj = regExpValid.match(line)
                # Skip invalid lines
                if matchObj is None:
                    continue
                
                # We have a valid line
                procName = matchObj.group(1)
                pid = int(matchObj.group(2))
                rv[procName] = pid
                
    except IOError as exc:
        # Do not re-raise. The calling scope should treat this as if there are no SUMS processes running at all.
        type, value, traceback = sys.exc_info()
        print('Unable to open ' + "'" + value.filename + "'.", file=sys.stderr)
        rv = {}

def writePIDFile(pidFile, procData):
    try:
    	with open (pidFile, 'w') as fout:
            for key, val in procData:
            	print(key + '    ' + val, file=fout)

    except IOError as exc:
        type, value, traceback = sys.exc_info()
        raise Exception('writePID', 'Unable to open ' + "'" + value.filename + "' for writing.", file=sys.stderr)


def runCmd(cmdList):
    pid = -1
   
    print('was going to run ' + ' '.join(cmdList))
    sys.exit(1) 
    try:
        sp = subprocess.Popen(cmdList)
        pid = sp.pid
    
    except ValueError:
        raise Exception('runCmd', 'Command ' + "'" + ' '.join(cmdList) + "'" + ' called with bad arguments.')
    except OSError:
        raise Exception('runCmd', 'Command ' + "'" + ' '.join(cmdList) + "'" + ' ran improperly.')

    return pid

rv = RET_SUCCESS

if __name__ == "__main__":
    signal.signal(signal.SIGUSR1, shutdown)
    
    try:
        drmsParams = DRMSParams()
        if drmsParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')
        nSums = int(getDrmsParam(drmsParams, 'SUM_NUMSUM'))
        maxNSums = int(getDrmsParam(drmsParams, 'SUM_MAXNUMSUM'))
        sumsServer = getDrmsParam(drmsParams, 'SUMSERVER')
        drmsDb = getDrmsParam(drmsParams, 'DBNAME')
        sumsLogDir = getDrmsParam(drmsParams, 'SUMLOG_BASEDIR')
        sumsManager = getDrmsParam(drmsParams, 'SUMS_MANAGER')
        sumsDb = drmsDb + '_sums'

        # Who am I running as?
        user = getpass.getuser()
        if sumsManager != user:
            raise Exception('badUser', 'This script must be run as the SUMS manager (user ' + user + ').')
        
        # What host am I running on?
        regExp = re.compile(r'([^\s.]+)([.].+)?')
        host = socket.gethostname()
        matchObj = regExp.match(host)
        if matchObj is None:
            raise Exception('badHost', 'Unable to obtain host name.')
        host = matchObj.group(1)
        if not host == sumsServer[0:len(host)]:
            raise Exception('badHost', 'This script must be run on ' + sumsServer + '.')

        # Come up with a SUMS log name. Use the name of the latest log file, which should exist if this is not a respawn.
        # If this it not a respawn, then create a new log file.
        # that a log will exist. Since this script does not know how to create a log file if one does not exist, this 
        # script must never be invoked directly - it must be invoked by sum_start.NetDRMS. At the very least, we can simply
        # die if there is no existing log file.

        # If there is no PID file, then SUMS isn't running. Create a new log file, and spawn all SUMS processes.
        latest = None
        if not os.path.isfile(sumsLogDir + '/' + PID_FILE):
            # If there is no PID file, then SUMS isn't running. Create a new log file, and spawn all SUMS processes.
            respawn = False
        else:
            # SUMS is running. If all SUMS processes are running, there is nothing to do.
            respawn = True

            # Find last log and append an 'R' to it to identify the log as a "restart".
            regExp = re.compile(r'\s*sum_svc_(\d\d\d\d)[.](\d\d)[.](\d\d)[.](\d\d)(\d\d)(\d\d)[.]log')
            for file in os.listdir(sumsLogDir):
                if fnmatch.fnmatch(file, 'sum_svc_*'):
                    matchObj = regExp.match(file)
                    if matchObj is not None:
                        timeTuple = (matchObj.group(1), matchObj.group(2), matchObj.group(3), matchObj.group(4), matchObj.group(5), matchObj.group(6))
                        if latest is None or  mktime(timeTuple) > mktime(latest):
                            latest = timeTuple

        if latest is None:
            # Create a new log file.
            latest = time.localtime()

        sumsLog = 'sum_svc_' + time.strftime('%Y.%m.%d.%H%M%S', latest) + '.log'
        if respawn:
            # Must suffix an 'R' to the log-file name.
            sumsLog = sumsLog + 'R'
        
        print('Running on SUMS-server host ' + sumsServer + '.')
        print('Connection to SUMS DB ' + sumsDb + '.')
        print('Using SUMS-log file ' + sumsLog + '.')        
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise

        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'drmsParams':
            print('Error reading DRMS parameters: ' + msg, file=sys.stderr)
            rv = RET_DRMSPARAMS
        elif etype == 'badUser':
            print('Error user: ' + msg, file=sys.stderr)
            rv = RET_USER
        elif etype == 'badHost':
            print('Error SUMS server: ' + msg, file=sys.stderr)
            rv = RET_HOST

    # Determine the set of processes that should be running, given nSums. The value of the dictionary key
    # is the PID, which is unknown to begin with. An unknown PID is designated by a -1 value. As the
    # PIDs are gleaned, the expectedProcs values will be updated.
    expectedProcs = {'sum_svc':-1}

    if nSums > 1:
        expectedProcs['Sdelser'] = -1

        expectedProcs['Sinfo'] = -1
        expectedProcs['Sput'] = -1
        expectedProcs['Sget'] = -1
        expectedProcs['Salloc'] = -1
        expectedProcs['Sopen'] = -1
        
        for iProc in range(1, nSums):
            expectedProcs['Sinfo' + str(iProc)] = -1
            expectedProcs['Sput' + str(iProc)] = -1
            expectedProcs['Sget' + str(iProc)] = -1
            expectedProcs['Salloc' + str(iProc)] = -1
            expectedProcs['Sopen' + str(iProc)] = -1
            
    # This PID file contains the list of PIDs of the SUMS processes that were running the last time this
    # script completed. Each line in this file contains a process name, followed by the PID of that process.
    # Ensure that there is a line in this file for each expected process. If such a line exists, then ensure
    # that the process identified by the PID is running. If not, then spawn the missing process. If there
    # is no line in the PID file for the expected process, spawn the missing process.

    # Read the PID file into a dictionary.
    actualProcs = None
    if respawn:
        actualProcs = readPIDFile(PID_FILE)

    # Time to install the signal handler. If this program terminates after this point, then
    # we need to save the state of the actualProcs dictionary (which lists which SUMS
    # processes are running, and the PIDs of those processes). The contents of actualProcs
    # may change over time, in memory, as processes are spawned. If that is the case, then
    # actualProcs will no longer accurately reflect the contents in the pid file. This program
    # regularly writes the current state of actualProcs to the pid file, but an interrupt that
    # kills this program could interfere with this process and prevent it from writing
    # the pid file. The signal handler will catch SIGINT and SIGTERM and flush actualProcs to
    # pid file.
    #
    # WARNING: The SUMS processes will inherit the setting of a signal handler. However, these
    # processes cannot "see" the handler in this program, so they will use the default signal handler.
    # The net result is a no-op.
    def terminationHandler(*args):
        print('Termination signal handler called')
        try:
            writePIDFile(actualProcs)

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'writePID':
                print('Error writing PID file: ' + msg, file=sys.stderr)
        
    signal.signal(signal.SIGINT, terminationHandler)

    while (1):
        try:
            if actualProcs is not None:
                # iterate through the expected procs to see if they are all running
                for procName in expectedProcs.keys():
                    if procName in actualProcs:
                        # gotta see if the process is still running
                        pid = actualProcs[procName]
                        if not os.path.exists('/proc/' + str(pid)):
                            print('Process ' + procName + ' is not running.')
                            print('Starting process ' + procName + '.')
                            pid = runCmd([procName, sumsDb, sumsLog])
                            # Save the PID in actualProcs so we can update the PID file.
                            actualProcs[procName] = pid
                    else:
                        # there is no record of the process in the PID file - spawn the service
                        print('Process ' + procName + ' is not running.')
                        print('Starting process ' + procName + '.')
                        pid = runCmd([procName, sumsDb, sumsLog])
                        # Save the PID in actualProcs so we can update the PID file.
                        actualProcs[procName] = pid
            else:
                # No pid file, or couldn't open existing pid file. Assume there are no SUMS processes running.
                for procName in expectedProcs.keys():
                    pid = runCmd([procName, sumsDb, sumsLog])
                    # Save the PID in actualProcs so we can update the PID file.
                    actualProcs[procName] = pid

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'runCmd':
                print('Error running command: ' + msg, file=sys.stderr)
                rv = RET_RUNCMD
                
        # save the pid file
        try:
            writePIDFile(actualProcs)

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise
        
            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'writePID':
                print('Error writing PID file: ' + msg, file=sys.stderr)
                rv = RET_WRITEPID

        # Pause, before checking again.
        time.sleep(10)

sys.exit(rv)

