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
import subprocess
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
RET_FILEIO = 9
RET_SUMRMPID = 10

PID_FILE = 'sums.pidfile'

def shutdown(*args):
    rv = RET_SUCCESS
    print('Shutting down SUMS...')
    
    drmsParams = DRMSParams()
    if drmsParams is None:
        print('Cannot open DRMS params file.', file=sys.stderr)
        rv = RET_DRMSPARAMS
    else:
        try:
            sumsLogDir = getDrmsParam(drmsParams, 'SUMLOG_BASEDIR')
            pidFile = sumsLogDir + '/' + PID_FILE

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]
            
            if etype == 'drmsParams':
                print('Error reading DRMS parameters: ' + msg, file=sys.stderr)
                rv = RET_DRMSPARAMS

        if rv == RET_SUCCESS:
            actualProcs = readPIDFile(pidFile)
            if len(actualProcs) > 0:
                # Kill each SUMS process. It looks like SUMS needs the SIGKILL signal to die.
                regExp = re.compile(r'\s*[(\S+)]\s*')
                for procName in actualProcs:
                    pid = getPid(procName, actualProcs)
                    if isRunning(procName, actualProcs):
                        # Here's the skinny. It may be the case that sum_rm was started by sums_procck.py, 
                        # or it may be the case that sum_svc started it. And it is most likely the latter.
                        # In that case, what should really happen is that sums_procck.py should kill sums_svc, 
                        # and sum_svc should kill sum_rm, because only a parent can call waitpid. But I 
                        # don't really control sum_svc, and don't have a good idea what would happen if 
                        # I implemented a signal handler to catch SIGUSR1, SIGINT, AND SIGTERM that then
                        # killed sum_rm and waited for it to die (which would be the right thing to do).
                        print('Killing ' + procName + '(pid ' + str(pid) + ').')                    
                        sys.stdout.flush()
                        os.kill(pid, signal.SIGKILL)

                        # Wait for the process to die, but only if sums_procck.py is the parent of
                        # the process. Procnames that are not enclosed by square brackets are children.
                        matchObj = regExp.match(procName)
                        if matchObj is None:
                            killedPid, istat = os.waitpid(pid, 0)

                            # Ensure the process is dead. We cannot determine that from looking at the return value
                            # from waitpid. Instead, use the isRunning() function.
                            if isRunning(procName, actualProcs): 
                                print('Unable to kill SUMS process ' + procName + '(pid ' + str(pid) +').')
                                rv = RET_KILLSUMS
                                sys.stdout.flush()
                                break

    if rv == RET_SUCCESS:
        print('Removing pidfile ' + pidFile)
        sys.stdout.flush()
        try:
            os.remove(pidFile)
        except IOError as exc:
            type, value, traceback = sys.exc_info()
            print('Unable to remove ' + "'" + value.filename + "'.", file=sys.stderr)
            rv = RET_FILEIO

        print('Exiting process via shutdown() with return value ' + rv + '.')
        sys.stdout.flush()
        sys.exit(rv)
    else:
        print('Failed to shutdown properly.')
        sys.stdout.flush()

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
            # The optional [] surrounding the pid means 'started by sum_svc'
            regExpValid = re.compile(r'\s*(\S+)\s+(\d+)\s*')

            for line in fin:
                # Skip comment lines
                if regExpComment.match(line):
                    continue
                # Skip empty lines
                if not regExpNotEmpty.search(line):
                    continue
                matchObj = regExpValid.match(line)
                # Skip invalid lines (and the line that contains the pid of this script - a line with only a pid).
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
    
    return rv

def writePIDFile(pidFile, procData):
    if len(procData) > 0:
        try:
    	    with open (pidFile, 'w') as fout:
                # First write the pid of this process
                pid = os.getpid()
                print(str(pid), file=fout)
                for key, val in procData.items():
                    print(key + '    ' + str(val), file=fout)

        except IOError as exc:
            type, value, traceback = sys.exc_info()
            if os.path.exists(pidFile):
                os.remove(pidFile)
            raise Exception('writePID', 'Unable to open ' + "'" + value.filename + "' for writing.")

def runCmd(cmdList):
    pid = -1
   
    try:
        sp = subprocess.Popen(cmdList)
        pid = sp.pid
    
    except ValueError:
        raise Exception('runCmd', 'Command ' + "'" + ' '.join(cmdList) + "'" + ' called with bad arguments.')
    except OSError:
        raise Exception('runCmd', 'Command ' + "'" + ' '.join(cmdList) + "'" + ' ran improperly.')

    return pid

def getPid(procName, procData):
    rv = -1

    if procName in procData:
        rv = procData[procName]
    elif '[' + procName + ']' in procData:
        rv = procData['[' + procName + ']']

    return rv

def isRunning(procName, procData):
    rv = 0
    
    if procName in procData or '[' + procName + ']' in procData:
        rv = os.path.exists('/proc/' + str(getPid(procName, procData)))

    return rv

def isDefunct(procName, procData):
    rv = 0

    if isRunning(procName, procData):
        procFile = '/proc/' + str(getPid(procName, procData)) + '/status'
        try:
            with open(procFile, 'r') as fin:
                regExp = re.compile(r'State:\s+(\w)\s+')
                for line in fin:
                    matchObj = regExp.match(line)
                    if matchObj is not None:
                        state = matchObj.group(1)
                        if state == 'Z':
                            rv = 1
                        break
        except IOError as exc:
            print(exc.strerror, file=sys.stderr)
            print('Unable to open proc file ' + procFile + '.', file=sys.stderr)

    return rv

def delPid(procName, procData):
    if procName in procData:
        del procData[procName]
    elif '[' + procName + ']' in procData:
        del procData['[' + procName + ']']


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
        pidFile = sumsLogDir + '/' + PID_FILE

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

        # Come up with a SUMS log name. Append an 'R' to latest log file, which should exist if this is a respawn.
        # If this it not a respawn, then create a new log file.

        # The sum_rm log file is in /tmp/sum_rm*. It is created by the sum_rm program itself, unlike the sum_svc
        # log.

        # If there is no PID file, then SUMS isn't running. Create a new log file, and spawn all SUMS processes.
        latest = None
        if not os.path.isfile(pidFile):
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
        # sum_rm's last argument is a time string, not a log-file name.
        sumsRmLog = time.strftime('%Y.%m.%d.%H%M%S', latest) + '.log'
        if respawn:
            # Must suffix an 'R' to the log-file name. I'm not sure what to do with sum_rm - it needs a timestring
            # so don't suffix an 'R'.
            sumsLog = sumsLog + 'R'

        print('Running on SUMS-server host ' + sumsServer + '.')
        print('Connection to SUMS DB ' + sumsDb + '.')
        print('Using SUMS-log file ' + sumsLog + '.')
        print('Using SUMS-rm-log file ' + sumsRmLog + '.')
        sys.stdout.flush()
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
    # is the log file to use when spawning the process.
    expectedProcs = {'sum_svc':sumsLog}

    if nSums > 1:
        expectedProcs['Sdelser'] = sumsLog

        expectedProcs['Sinfo'] = sumsLog
        expectedProcs['Sput'] = sumsLog
        expectedProcs['Sget'] = sumsLog
        expectedProcs['Salloc'] = sumsLog
        expectedProcs['Sopen'] = sumsLog
        
        for iProc in range(1, nSums):
            expectedProcs['Sinfo' + str(iProc)] = sumsLog
            expectedProcs['Sput' + str(iProc)] = sumsLog
            expectedProcs['Sget' + str(iProc)] = sumsLog
            expectedProcs['Salloc' + str(iProc)] = sumsLog
            expectedProcs['Sopen' + str(iProc)] = sumsLog

    # sum_rm gets a special log
    expectedProcs['sum_rm'] = sumsRmLog

    # This PID file contains the list of PIDs of the SUMS processes that were running the last time this
    # script completed. Each line in this file contains a process name, followed by the PID of that process.
    # Ensure that there is a line in this file for each expected process. If such a line exists, then ensure
    # that the process identified by the PID is running. If not, then spawn the missing process. If there
    # is no line in the PID file for the expected process, spawn the missing process.

    # Read the PID file into a dictionary.
    actualProcs = {}
    if respawn:
        actualProcs = readPIDFile(pidFile)

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
            print('Writing pid file ' + pidFile + '.')
            writePIDFile(pidFile, actualProcs)

        except Exception as exc:
            if len(exc.args) == 2:
                etype = exc.args[0]
                msg = exc.args[1]

                if etype == 'writePID':
                    print('Error writing PID file: ' + msg, file=sys.stderr)
            sys.exit(RET_WRITEPID)

        sys.exit(RET_SUCCESS)
        
    signal.signal(signal.SIGINT, terminationHandler)

    while (1):
        try:
            # If sum_rm is not running, or it is defunct, but sum_svc is running, we have to 
            # kill sum_svc. Then in the expected procs loop below, if either sum_rm or sum_svc is
            # expected, sum_svc will be relaunched (which will respawn sum_rm).
            if 'sum_rm' in expectedProcs and (not isRunning('sum_rm', actualProcs) or isDefunct('sum_rm', actualProcs)) and isRunning('sum_svc', actualProcs):
                print('Killing and restarting sum_svc to start missing sum_rm process.')
                pid = getPid('sum_svc', actualProcs)
                os.kill(pid, signal.SIGKILL)
                killedPid, istat = os.waitpid(pid, 0)
                print('killedPid ' + str(killedPid))
                if killedPid == 0:
                    raise Exception('killSVC', 'Unable to kill sum_svc process (pid ' + str(pid) +').')
                delPid('sum_rm', actualProcs)
                delPid('sum_svc', actualProcs)

            # Iterate through the expected procs to see if they are all running.
            for procName in expectedProcs.keys():
                if isRunning(procName, actualProcs) and not isDefunct(procName, actualProcs):   
                    continue
                else:
                    # Delete no-longing-running proc from list of running procs.
                    delPid(procName, actualProcs)

                    # If we are going to launch sum_svc, but sum_rm is still running, kill sum_rm since
                    # sum_svc is going to spawn sum_rm. sums_procck.py is never the parent of sum_rm, do
                    # do not wait for the process to die.
                    if procName == 'sum_svc' and isRunning('sum_rm', actualProcs):
                        pid = getPid('sum_rm', actualProcs)
                        print('Killing orphaned sum_rm process (pid ' + str(pid) + '). When sum_svc restarts, it will spawn a new sum_rm.')
                        os.kill(pid, signal.SIGKILL)
                        delPid('sum_rm', actualProcs)

                # We never spawn sum_rm directly, skip it. If sum_rm is not running, sum_svc will not be running either, so when sum_svc
                # is processed in this loop, it will spawn sum_rm
                if procName == 'sum_rm':
                    continue

                # There is no record of the process in the PID file - spawn the service.
                print('Process ' + procName + ' is not running.')
                print('Starting process ' + procName + '.')
                sys.stdout.flush() 
                pid = runCmd([procName, sumsDb, expectedProcs[procName]])
                # Save the PID in actualProcs so we can update the PID file.
                actualProcs[procName] = pid

                # if we just launched sum_svc, then sum_rm was also spawned, and we need to
                # get its pid and put it into actualProcs. When sum_svc starts, it puts sum_rm's
                # pid in /tmp/pidSupp.<pid>.log where <pid> is the pid of the sum_svc parent.
                if procName == 'sum_svc':
                    maxLoop = 20
                    pidSupp = '/tmp/pidSupp.' + str(pid) + '.log'
                    while maxLoop > 0 and not os.path.exists(pidSupp):
                        time.sleep(1)
                        maxLoop = maxLoop - 1

                    with open(pidSupp, 'r') as fin:
                        regExp = re.compile(r'\s*(\d+)\s*')
                        for line in fin:
                            matchObj = regExp.match(line)
                            if matchObj is not None:
                                sumRmPid = matchObj.group(1)
                                break
                    if sumRmPid is not None:
                        # The brackets surrounding the program name mean that sums_procck.py is
                        # not the parent of the program.
                        actualProcs['[sum_rm]'] = int(sumRmPid)
                    else:
                        raise Exception('sumrmPid', 'Unable to obtain newly spawned sum_rm pid.')

        except IOError as exc:
            print(exc.strerror, file=sys.stderr)
            print('Unable to read sum_rm pid file ' + pidSupp + '.', file=sys.stderr)
            rv = RET_SUMRMPID
        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'runCmd':
                print('Error running command: ' + msg, file=sys.stderr)
                rv = RET_RUNCMD
            elif etype == 'sumrmPid':
                print('Error getting sum_rm pid: ' + msg, file=sys.stderr)
                rv = RET_SUMRMPID
            elif etype == 'killSVC':
               print('Error killing sum_svc: ' + msg, file=sys.stderr)
               rv = RET_KILLSUMS
                
        # save the pid file
        try:
            writePIDFile(pidFile, actualProcs)

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise
        
            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'writePID':
                print('Error writing PID file: ' + msg, file=sys.stderr)
                rv = RET_WRITEPID

        # Pause, before checking again.
        time.sleep(1)

sys.exit(rv)

