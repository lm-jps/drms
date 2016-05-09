#!/usr/bin/env python

# The client sends a 'subscribe' request to request-subs.py cgi. That cgi inserts
# a row into the requests table with status N (new). The client then polls, by sending a 'polldump' 
# request to request-subs.py, for the DUMP file that manage-subs.py creates in response to 
# the N request. Until the DUMP file has been created, request-subs.py finds either an 
# N or P (pending) request status, and in response returns a 'wait' response to the client.
# In the meantime, the main thread of manage-subs.py finds the N request, 
# sets the request status to P, and spawns a Worker thread. The Worker thread 
# creates the DUMP file, and then it sets the request status to D (dump ready). In response
# to the client polldump request, request-subs.py discovers the D status and 
# now returns a 'continue' response to the client.
# In response to this, the client scps the DUMP file to the client machine, 
# and ingests it. When the client is done ingesting the DUMP file, it sends a 'pollcomplete' 
# request to request-subs.py, indicating to the the server it has successfully
# ingested the DUMP file and is awaiting completion of the subscription request. request-subs.py
# sets the status to I (ingested), which causes the manager to start the clean-up process.
# During clean-up, request-subs.py returns 'wait' to the client. When the manager
# finishing cleaning-up, it sets the status to C (complete) or E (error). Then in response to
# the client pollcomplete request, request-subs.py returns 'success' or 'failure', and
# then it deletes the request from the request table altogether. If the
# response is success, then the client script terminates, and the client can use the
# series subscribed to. If the response is failure, then the client must run get_slony_logs.pl
# (to download any logs with the new series in it) and then delete the series whose
# subscription attempt failed before the client script terminates.
#
# [ client submits request ] --> N --> [ server starts worker ] --> P -->
#    [ server creates the dump ] --> D --> [ client downloads dump ] --> A -->
#    [ client ingests dump ] --> I --> [ server cleans up ] --> C -->
#    [ client receives notice ] --> S
#    

# Responses to client polling:
# State       Response to pollDump        Response to pollComplete
# -----       --------------------        ------------------------
#   N           requestQueued               invalidArgument
#   P           requestProcessing           invalidArgument
#   D           dumpReady                   invalidArgument
#   A           invalidArgument             requestFinalizing
#   I           invalidArgument             requestFinalizing
#   C           invalidArgument             requestComplete
#   S           internalError               internalError
#   E           requestFailed               requestFailed

import os
import sys
import time
import logging
import threading
import argparse
import io
import signal
import re
import tarfile
from subprocess import check_call, Popen, CalledProcessError
import psycopg2
from datetime import datetime, timedelta
import shutil
import glob
import fcntl
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsLock import DrmsLock


sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

if sys.version_info < (3, 0):
    raise Exception("You must run the 3.0 release, or a more recent release, of Python.")

RV_TERMINATED = 0
RV_DRMSPARAMS = 1
RV_ARGS = 2
RV_CLIENTCONFIG = 3
RV_REQTABLEWRITE = 4
RV_REQTABLEREAD = 5
RV_INVALIDARGUMENT = 6
RV_UNKNOWNREQUESTID = 7
RV_GETSUBLIST = 8
RV_DBRESPONSE = 9
RV_DBCOMMAND = 10
RV_UKNOWNERROR = 11

LST_TABLE_SERIES = 'series'
LST_TABLE_NODE = 'node'
CFG_TABLE_NODE = 'node'


def terminator(*args):
    # Raise the SystemExit exception (which will be caught by the __exit__() method below).
    sys.exit(0)

class TerminationHandler(object):
    def __new__(cls, thContainer):
        return super(TerminationHandler, cls).__new__(cls)

    def __init__(self, thContainer):
        self.container = thContainer
        self.msLockFile = thContainer[0]
        self.subLockFile = thContainer[1]
        self.pidStr = thContainer[2]
        self.log = thContainer[3]
        self.writeStream = thContainer[4]
        self.readEndPipe = thContainer[5]
        self.writeEndPipe =thContainer[6]
        self.saveState = False
        super(TerminationHandler, self).__init__()
        
    def __enter__(self):
        signal.signal(signal.SIGINT, terminator)
        signal.signal(signal.SIGTERM, terminator)
        signal.signal(signal.SIGHUP, terminator)

        # Acquire locks.
        self.msLock = DrmsLock(self.msLockFile, self.pidStr)

    # Normally, __exit__ is called if an exception occurs inside the with block. And since SIGINT is converted
    # into a KeyboardInterrupt exception, it will be handled by __exit__(). However, SIGTERM will not - 
    # __exit__() will be bypassed if a SIGTERM signal is received. Use the signal handler installed in the
    # __enter__() call to handle SIGTERM.
    def __exit__(self, etype, value, traceback):
        if etype == SystemExit:
            self.log.writeInfo(['Termination signal handler called.'])
            self.container[7] = RV_TERMINATED
            self.saveState = True
        self.finalStuff()
        
        # Flush writeStream, print the stream contents to the log, and close it.
        if self.writeStream:
            self.writeStream.flush()
            self.log.writeDebug([ 'Logging redirected stdout and stderr.' ])
            self.writeStream.logLines() # Flushes write end of pipe.
            self.writeStream.close()
            self.writeStream = None
            
        if self.writeEndPipe:
            self.writeEndPipe.flush()
            self.writeEndPipe.close()
            self.writeEndPipe = None
        
        if self.readEndPipe:
            self.readEndPipe.close()
            self.readEndPipe = None

        # Clean up subscription lock
        if os.path.exists(self.subLockFile):
            os.remove(self.subLockFile)
            
        # Clean up manage-subs lock
        try:        
            self.msLock.close()
            self.msLock = None
        except IOError:
            pass
        
    def finalStuff(self):
        self.log.writeInfo(['Halting threads.'])
        
        # Do not do this! The call of Log::__del__ is deferred until after the program exits. If you end your program by
        # calling sys.exit(), then sys.exit() gets called BEFORE Log::__del__ is called, and this causes a race condition.
        # del self.log
        # self.log.close()
        # On second thought, let's not close the log either. Just flush for now, and close() at the end of the program run.
        self.log.flush()
    
        # Wait for worker threads to complete.
        for worker in Worker.tList:
            worker.stop(self.saveState)
            self.log.writeDebug([ 'Waiting for worker (request ID ' + str(worker.requestID) + ') to halt.' ])
            worker.join() # Cannot interrupt threads at an unsafe point to do so. If there is a long-running thread,
                          # then we block here and we do not process new requests. If we need to shut down for reals, 
                          # then we have to kill -9 manage-subs.py.
            self.log.writeDebug([ 'Worker (request ID ' + str(worker.requestID) + ') halted.' ])

class ManageSubsParams(DRMSParams):

    def __init__(self):
        super(ManageSubsParams, self).__init__()

    def get(self, name):
        val = super(ManageSubsParams, self).get(name)

        if val is None:
            raise Exception('drmsParams', 'Unknown DRMS parameter: ' + name + '.')
        return val

class Arguments(object):

    def __init__(self, parser=None):
        # This could raise in a few places. Let the caller handle these exceptions.
        if parser:
            self.parser = parser
        
        if parser:
            # Parse the arguments.
            self.parse()
        
            # Set all parsed args.
            self.setAllArgs()
        
    def parse(self):
        try:
            if self.parser:
                self.parsedArgs = self.parser.parse_args()      
        except Exception as exc:
            if len(exc.args) == 2:
                type, msg = exc.args
                  
                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise Exception('args', msg)
            else:
                raise # Re-raise
                
    def setParser(self, parser):
        if parser:
            self.parser = parser
            self.parse()
            self.setAllArgs()

    def setArg(self, name, value):
        if not hasattr(self, name):
            # Since Arguments is a new-style class, it has a __dict__, so we can
            # set attributes directly in the Arguments instance.
            setattr(self, name, value)
        else:
            raise Exception('args', 'Attempt to set an argument that already exists: ' + name + '.')
            
    def set(self, name, value):
        # Sets attribute, even if it exists already.
        setattr(self, name, value)

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)
        
    def getArg(self, name):
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise Exception('args', 'Unknown argument: ' + name + '.')
            
    def get(self, name):
        # None is returned if the argument does not exist.
        return getattr(self, name, None)
            
    def addFileArgs(self, file):
        cfileDict = {}
        rv = getCfg(file, cfileDict)
        if rv != 0:
            raise Exception('clientConfig', 'Unable to open or parse client-side configuration file ' + file + '.')
        for key, val in cfileDict.items():
            self.setArg(key, val)
            
    def dump(self, log):
        attrList = []
        for attr in sorted(vars(self)):
            attrList.append('  ' + attr + ':' + str(getattr(self, attr)))
        log.writeDebug([ '\n'.join(attrList) ])


class Log(object):
    """Manage a logfile."""
    def __init__(self, file, level, formatter):
        self.fileName = file
        self.log = logging.getLogger()
        self.log.setLevel(level)
        self.fileHandler = logging.FileHandler(file)
        self.fileHandler.setLevel(level)
        self.fileHandler.setFormatter(formatter)
        self.log.addHandler(self.fileHandler)
        
    def close(self):
        if self.log:
            if self.fileHandler:
                self.log.removeHandler(self.fileHandler)
                self.fileHandler.flush()
                self.fileHandler.close()
                self.fileHandler = None
            self.log = None
            
    def flush(self):
        if self.log and self.fileHandler:
            self.fileHandler.flush()
            
    def getLevel(self):
        # Hacky way to get the level - make a dummy LogRecord
        logRecord = self.log.makeRecord(self.log.name, self.log.getEffectiveLevel(), None, '', '', None, None)
        return logRecord.levelname

    def writeDebug(self, text):
        if self.log:
            for line in text:
                self.log.debug(line)
            self.fileHandler.flush()
            
    def writeInfo(self, text):
        if self.log:
            for line in text:
                self.log.info(line)
        self.fileHandler.flush()
    
    def writeWarning(self, text):
        if self.log:
            for line in text:
                self.log.warning(line)
            self.fileHandler.flush()
    
    def writeError(self, text):
        if self.log:
            for line in text:
                self.log.error(line)
            self.fileHandler.flush()
            
    def writeCritical(self, text):
        if self.log:
            for line in text:
                self.log.critical(line)
            self.fileHandler.flush()

class RedirectStdFileStreams(object):
    def __new__(cls, stdoutFileObj=None, stderrFileObj=None):
        return super(RedirectStdFileStreams, cls).__new__(cls)
    
    # stdoutFileObj and stderrFileObj must be file-type IO Streams.
    def __init__(self, stdoutFileObj=None, stderrFileObj=None):
        self.stdout = stdoutFileObj or sys.stdout
        self.stderr = stderrFileObj or sys.stderr

    def __enter__(self):
        self.stdoutOrig = sys.stdout
        self.stderrOrig = sys.stderr

        # Need to save the pointers to the original sys.stdout and sys.stderr by duplicating their file descriptors.
        self.stdoutFileOrig = os.dup(self.stdoutOrig.fileno())
        self.stderrFileOrig = os.dup(self.stderrOrig.fileno())
        self.stdoutOrig.flush()
        self.stderrOrig.flush()

        # We are redirecting the stdout and stderr file descriptors to a pipe that writes into self.stdout and self.stderr so that
        # child processes can write to stdout and stderr, which then redirects to self.stdout and self.stderr.
        
        # These dup2 calls will cause fd 1 and fd 2 to no longer point to the terminal window. That is why we saved pointers to the 
        # terminal window with self.stdoutFDOrig and self.stderrFDOrig.
        os.dup2(self.stdout.fileno(), self.stdoutOrig.fileno()) # fd 1 now redirects to the write end of the pipe (instead of the terminal window)
        os.dup2(self.stderr.fileno(), self.stderrOrig.fileno()) # fd 2 now redirects to the write end of the pipe (instead of the terminal window)

        sys.stdout = self.stdout
        sys.stderr = self.stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self.stdout.flush()
        self.stderr.flush()

        # Restore original stdout and stderr.
        os.dup2(self.stdoutFileOrig, self.stdoutOrig.fileno())
        os.close(self.stdoutFileOrig)
        self.stdoutFileOrig = None
        os.dup2(self.stderrFileOrig, self.stderrOrig.fileno())
        os.close(self.stderrFileOrig)
        self.stderrFileOrig = None

        sys.stdout = self.stdoutOrig
        sys.stderr = self.stderrOrig

class WriteStream(object):
    def __init__(self, readEndPipe, writeEndPipe, log):
        self.log = log
        self.io = io.StringIO()
        self.pipeReadEnd = readEndPipe
        self.pipeWriteEnd = writeEndPipe
        
    def close(self):
        self.pipeReadEnd = None
        self.pipeWriteEnd = None
        self.io.close()
        
    def readPipe(self):
        if self.pipeReadEnd:
            while True:
                pipeBytes = self.pipeReadEnd.read(4096)
                if len(pipeBytes) > 0:
                    self.io.write(pipeBytes)
                else:
                    break
        
    def flush(self):
        self.io.flush()
        
    def getStream(self):
        return self.io
        
    def getLines(self):
        # First, flush pipe into IO stream.
        self.pipeWriteEnd.flush()

        self.readPipe()
        
        self.io.seek(0)
        lines = [ line.rstrip() for line in self.io.readlines() ]
        self.io.truncate(0)
        return lines
        
    def logLines(self, log=None):
        lines = self.getLines()

        if len(lines) > 0:
            if log is None:
                log = self.log
            if log:
                log.writeInfo(lines)
            return lines
        return None
        
class Worker(threading.Thread):
    """ This kinda does what the old subscription Updater code did. """

    tList = [] # A list of running worker threads.
    maxThreads = 16 # Default. Can be overriden with the Downloader.setMaxThreads() method.
    eventMaxThreads = threading.Event() # Event fired when the number of threads decreases.
    lock = threading.Lock() # Guard tList.
    
    def __init__(self, request, newSite, arguments, connMaster, connSlave, log, writeStream, fclean):
        threading.Thread.__init__(self)
        self.request = request
        self.requestID = request.requestid
        self.newSite = newSite
        self.arguments = arguments
        self.connMaster = connMaster # Master db
        self.connSlave = connSlave # Slave db
        self.log = log
        self.writeStream = writeStream
        self.sdEvent = threading.Event()
        
        self.reqTable = self.request.reqtable
        self.client = self.request.client
        self.series = self.request.series
        self.action = self.request.action
        self.archive = self.request.archive
        self.retention = self.request.retention
        self.tapegroup = self.request.tapegroup
        
        self.siteLogDir = self.arguments.getArg('subscribers_dir')
        self.repDir = self.arguments.getArg('kRepDir')
        self.slonyCfg = self.arguments.getArg('slonyCfg')
        self.clientLstDbTable = self.arguments.getArg('kLstTable')
        self.triggerDir = self.arguments.getArg('dumpDir')
        self.lstDir = self.arguments.getArg('tables_dir')
        self.parserConfig = self.arguments.getArg('parser_config')
        self.parserConfigTmp = self.parserConfig + '.tmp'
        self.subscribeLock = os.path.join(self.arguments.getArg('kServerLockDir'), 'subscribelock.txt')
        self.oldClientLogDir = os.path.join(self.siteLogDir, self.client)
        self.newClientLogDir = os.path.join(self.siteLogDir, self.client + '.new')
        self.oldLstPath = os.path.join(self.lstDir, self.client + '.lst')
        self.newLstPath = os.path.join(self.lstDir, self.client + '.new.lst')
        self.sqlgenArgFile = os.path.join(self.triggerDir, self.client + '-sqlgen.txt.tmp')
        self.saveState = False
        self.cleanTempOnStart = fclean
    
    def run(self):
        # There is at most one thread per client.
        errorMsg = None
        try:
            self.log.writeInfo([ 'Starting worker thread to handle request: (' + self.request.dump(False) + ').'])
            self.writeStream.logLines()
            
            if self.cleanTempOnStart:
                self.cleanTemp()

            # We don't want the main thread modifying this the request part-way through the following operations.
            self.reqTable.acquireLock()
            try:
                self.log.writeDebug([ 'Worker thread acquired req table lock ' + hex(id(self.reqTable.lock)) + '.'])
                reqStatus = self.request.getStatus()[0]
            finally:
                self.reqTable.releaseLock()
                self.log.writeDebug([ 'Worker thread released req table lock ' + hex(id(self.reqTable.lock)) + '.'])

            # Check for download error or completion
            self.log.writeDebug([ 'Request ' + str(self.requestID) + ' status is ' + reqStatus + '.' ])
            if reqStatus.upper() == 'P':
                self.log.writeDebug([ 'Processing status P for request ' + str(self.requestID) + '.' ])
                self.reqTable.acquireLock()
                try:
                    # Don't clean up old files - we could be resuming after a manual shutdown.

                    # Create client.new.lst so that it contains the series that the client will be subscribed to
                    # should the request succeed. This means that client.new.lst should contain all series in
                    # the existing client.lst, minus series being un-subscribed from (if action == unsubscribe), plus
                    # the series being subscribed to (if action == subscribe or resubscribe).
                    #
                    # It is important to note that client.new.lst is not reachable and therefore will not be
                    # used until after the dump file, which contains a dump of the series to which the client is 
                    # subscribing, is created and successfully ingested by the client. After the dump file is
                    # successfully ingested, then client.new.lst is put into slony_parser.cfg and su_production.slonycfg.

                    # Create the client.new.lst file and su_production.slonylst row.
                    # We must add the series to both the lst file and to su_production.slonylst so
                    # that if the parser is modified to use the su_production tables, it will see
                    # series for node.new.
                    subListObj = SubscriptionList(self.clientLstDbTable, self.connMaster, self.client)
                    if self.newSite:
                        subList = [ series.lower() for series in self.series ]
        
                        if self.action.lower() != 'subscribe':
                            raise Exception('invalidRequest', 'As a new client, ' + self.client + ' cannot make a ' + self.action.lower() + ' request.')
                    else:
                        # Read the client's subscription list from su_production.slonylst. There is no need 
                        # for a lock since only one thread can modify the subscription list for a client.
                        subList = subListObj.getSubscriptionList()
        
                        if self.action == 'subscribe':
                            # Append to subList.
                            subList.extend([ series.lower() for series in self.series ])
                        elif self.action == 'resubscribe':
                            # Don't do anything.
                            pass
                        elif self.action == 'unsubscribe':
                            # Remove from subList.
                            for series in self.series:                            
                                subList.remove(series.lower())
                        else:
                            # Error, unknown action.
                            raise Exception('invalidArgument', 'Invalid request action: ' + self.action + '.')
            
                    # If the request was successfully processed, then the cached subscription list must be updated.
                    if self.action == 'subscribe' or self.action == 'unsubscribe':
                        # We must acquire the global subscribe lock before proceeding. Loop until we obtain the lock
                        # (or time-out after some number of failed attempts). The global subscribe lock needs to be available
                        # to shell scripts, as well as Perl and Python programs, and since shell does not support
                        # flock-type of lock commands, we have to simulate an atomic locking shell command:
                        #   (set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null
                
                        # Release reqTable lock to allow other client requests to be processed.
                        self.reqTable.releaseLock()
                
                        maxLoop = 30
                        while True:
                            if self.sdEvent.isSet():
                                raise Exception('shutDown', 'Received shut-down message from main thread.')
                            try:
                                try:
                                    cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                                    check_call(cmd, shell=True)
                                except CalledProcessError as exc:
                                    # Other exceptions will re-raise to the outer-most exception handler.
                                    raise Exception('subLock')
                        
                                # Obtained global subscription lock.

                                self.reqTable.acquireLock()
                        
                                # Modify the client.lst file by creating a temporary file...
                                with open(self.newLstPath, 'w') as fout:
                                    # If we are unsubscribing from the only subscribed series, then len(subList) == 0, and this is OK.
                                    for aseries in subList:
                                        print(aseries.lower(), file=fout)
                
                                # ...and the su_production.slonylst rows for client.new.lst.
                                cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=replace', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new', '--lst=' + self.newLstPath ]
                                # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                                check_call(cmdList)
                                self.writeStream.logLines() # Log all gentables.pl output

                                if self.action == 'subscribe':
                                    # Do not create a temp site-dir log, unless we are subscribing to a new series. During un-subscription,
                                    # client.new.lst is not added slon_parser.cfg, so no logs are ever written to self.newClientLogDir.
                                    # ...and make the temporary client.new directory in the site-logs directory.
                                    os.mkdir(self.newClientLogDir) # umask WILL mask-out bits if it is not 0000; os.chmod() is better.
                                    os.chmod(self.newClientLogDir, 0O2755) # This is the way you specify Octal in Py 3 (not Py 2).
                                break
                            except Exception as exc:
                                if len(exc.args) == 1 and exc.args[0] == 'subLock':
                                    # Could not obtain lock; try again (up to 30 tries)
                                    if maxLoop <= 0:
                                        raise Exception('subLock', 'Could not obtain subscription lock after 30 tries.')
                                    time.sleep(1)
                                    maxLoop -= 1
                                else:
                                    # Failure to run gentables.pl create client.new dir or chmod client.new, re-raise the exception generated.
                                    raise
                            finally:
                                self.reqTable.releaseLock()
                                if os.path.exists(self.subscribeLock):
                                    os.remove(self.subscribeLock)

                        self.reqTable.acquireLock()

                        # Refresh this script's cache FROM DB.
                        subListObj.refreshSubscriptionList()

                    if self.action == 'subscribe' or self.action == 'resubscribe':
                        # Time to create the DUMP. There are two dump files. One contains SQL commands 
                        # that, when ingested on the client site, will create the schema containing the 
                        # series being subscribed to. This dump file is generated only if the site's 
                        # database does not have the series' schema at the time of subscription. 
                        # The other dump file contains a set of SQL commands that depends on the state 
                        # of client subscribing to the series:
                        # COMMAND                                 APPLIES TO
                        # -------------------------------------------------------------------------------
                        # create Slony _jsoc schema               A client subscribing for the first time
                        # create Slony _jsoc.sl* tables           A client subscribing for the first time
                        # create Slony PG functions in _jsoc      A client subscribing for the first time
                        # populate _jsoc.sl_sequence_offline      All client subscriptions
                        # populate _jsoc.sl_archive_tracking      All client subscriptions
                        # copy command containing series data     All client subscriptions
        
                        # Now that the dump file exists and is complete, tell the client that it is ready for
                        # consumption. The client is polling by checking for a request status (via request-subs.py) 
                        # of 'D'.
        
                        # The dump script wrapper is sql_gen. sql_gen calls sdo_slony1_dump.sh, which calls dumpreptables.pl. 
                        # LOCKS: The last program calls parse_slon_logs.pl (which must obtain both the parse lock and 
                        # the subscription lock), and it also modifies subscription files/directories (so it obtains the 
                        # subscription lock). So, we do not need to acquire any locks before calling sql_gen (in fact, if we do that, 
                        # we would deadlock). It could take a little time to 

                        # . $kRepDir/subscribe_manage/sql_gen $node $new_site $archive $retention $tapegroup $input_file
                        # The last argument is file containing two tab-separated columns. The first column is the series, 
                        # and the second column is the subscribe command. This is all legacy stuff - I didn't want to modify
                        # sql_gen so I am using the existing, somewhat-messy interface (instead of passing arguments on the
                        # command line).            
                        if self.newSite:
                            newSiteStr = '1'
                        else:
                            newSiteStr = '0'
                    
                        # 86 sql_gen. Instead do the work directly in manage-subs.py.
                        # 1. We are going to dump a database table. Ensure the table exists in the slave database.
                        regExp = re.compile(r'\s*(\S+)\.(\S+)\s*')
                        matchObj = regExp.match(self.series[0].lower())
                        if matchObj is not None:
                            ns = matchObj.group(1)
                            table = matchObj.group(2)
                        else:
                            raise Exception('invalidArgument', 'Not a valid DRMS series name: ' + self.series[0].lower() + '.')
                    
                        if not dbTableExists(connSlave, ns, table):
                            raise Exception('invalidArgument', 'DB table ' + self.series[0].lower() + ' does not exist.')
                
                        # 2. Run createns for the schema of the series being subscribed to.
                        cmdList = [ os.path.join(self.arguments.getArg('kModDir'), 'createns'), 'JSOC_DBHOST=' + self.arguments.getArg('SLAVEHOSTNAME'), 'ns=' + ns, 'nsgroup=user', 'dbusr=' + self.arguments.getArg('REPUSER') ]
                        if not os.path.exists(self.triggerDir):
                            os.mkdir(self.triggerDir)
                            os.chmod(self.triggerDir, 0O2755)
                        outFile = os.path.join(self.triggerDir, self.client + '.' + ns + '.createns.sql')
                
                        wroteIntMsg = False
                        with open(outFile, 'w') as fout:
                            self.log.writeInfo([ 'Creating createns.sql file:' + ' '.join(cmdList) + '.' ])
                            proc = Popen(cmdList, stdout=fout)
            
                            maxLoop = 60 # 1-minute timeout
                            while True:
                                if maxLoop <= 0:
                                    raise Exception('sqlDump', 'Time-out waiting for dump file to be generated.')
            
                                if not wroteIntMsg and self.sdEvent.isSet():
                                    self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                                    wroteIntMsg = True
                                    # proc.kill()
                                    # raise Exception('shutDown', 'Received shut-down message from main thread.')
            
                                if proc.poll() is not None:
                                    self.writeStream.logLines() # Log all creatns output.
                                    if proc.returncode != 0:
                                        raise Exception('sqlDump', 'Failure generating dump file, createns returned ' + str(proc.returncode) + '.') 
                                    break

                                maxLoop -= 1
                                time.sleep(1)
                
                        # 3. Run createtabstruct for the table of the series being subscribed to.
                        cmdList = [ os.path.join(self.arguments.getArg('kModDir'), 'createtabstructure'), '-u', 'JSOC_DBHOST=' + self.arguments.getArg('SLAVEHOSTNAME'), 'in=' + self.series[0].lower(), 'out=' + self.series[0].lower(), 'archive=' + str(self.archive), 'retention=' + str(self.retention), 'tapegroup=' + str(self.tapegroup), 'owner=' + self.arguments.getArg('REPUSER') ]
                        outFile = os.path.join(self.triggerDir, self.client + '.subscribe_series.sql')

                        wroteIntMsg = False
                        with open(outFile, 'w') as fout:
                            print('BEGIN;', file=fout)
                            print("SET CLIENT_ENCODING TO 'UTF8';", file=fout)
                            fout.flush()
                            self.log.writeInfo([ 'Dumping table structure: ' + ' '.join(cmdList) + '.' ])
                            proc = Popen(cmdList, stdout=fout)
                    
                            self.reqTable.releaseLock()

                            maxLoop = 60 # 1-minute timeout
                            while True:
                                if maxLoop <= 0:
                                    raise Exception('sqlDump', 'Time-out waiting for dump file to be generated.')
        
                                if not wroteIntMsg and self.sdEvent.isSet():
                                    self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                                    wroteIntMsg = True
                                    # proc.kill()
                                    # raise Exception('shutDown', 'Received shut-down message from main thread.')
        
                                if proc.poll() is not None:
                                    self.writeStream.logLines() # Log all createtabstructure output.
                                    if proc.returncode != 0:
                                        raise Exception('sqlDump', 'Failure generating dump file, createtabstructure returned ' + str(proc.returncode) + '.') 
                                    break

                                maxLoop -= 1
                                time.sleep(1)

                            self.reqTable.acquireLock()                            

                        # 4. Run sdo_slony1_dump.sh and ensure it completed fine. APPEND output to outFile. sdo_slony1_dump.sh expects
                        # three shell variables to be set: kJSOCRoot, kSMlogDir, and node. Originally, node was a shell variable set in
                        # sql_gen from a command-line argument. sql_gen was a wrapper around sdo_slony1_dump.sh (it sourced the latter). 
                        # Since sdo_slony1_dump.sh is a bash script, we can set three environment variables, and bash will read them. 
                        # Set these via the Popen env object argument. BUT we can't simply set the env vars we want in the env argument.
                        # If we do that, then the rest of the environment in manage-subs.py is NOT passed to the child process. Instead, 
                        # we have to copy the environment of manage-subs.py and then add the three environment variables needed
                        # by sdo_slony1_dump.sh.
                        cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'sdo_slony1_dump.sh'), self.arguments.getArg('SLAVEDBNAME'), self.arguments.getArg('CLUSTERNAME'), self.arguments.getArg('SLAVEPORT'), newSiteStr, os.path.join(self.arguments.getArg('SMworkDir'), 'slon_counter' + '.' + self.client + '.txt'), self.series[0].lower() ]
                        outFile = os.path.join(self.triggerDir, self.client + '.subscribe_series.sql')
            
                        wroteIntMsg = False
                        with open(outFile, 'a', encoding='UTF8') as fout:
                            pipeReadEndFD, pipeWriteEndFD = os.pipe()

                            # Make these pipes not block on read.
                            flag = fcntl.fcntl(pipeReadEndFD, fcntl.F_GETFL)
                            fcntl.fcntl(pipeReadEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)
                            # flag = fcntl.fcntl(pipeWriteEndFD, fcntl.F_GETFL)
                            # fcntl.fcntl(pipeWriteEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)

                            # The DB is in LATIN-1, so we need to read in bytes, then decode to a string, then encode
                            # to UTF8 bytes. Then we print these UTF8 bytes to the output file.
                            pipeReadEnd = os.fdopen(pipeReadEndFD, 'r', encoding='LATIN1')
                            pipeWriteEnd = os.fdopen(pipeWriteEndFD, 'w', encoding='LATIN1')

                            self.log.writeInfo([ 'Dumping table data: ' + ' '.join(cmdList) + '.' ])
                            # stderr should get written to self.writeStream.
                            proc = Popen(cmdList, stdout=pipeWriteEnd, env=dict(os.environ, kJSOCRoot=self.arguments.getArg('kJSOCRoot'), kSMlogDir=self.arguments.getArg('kSMlogDir'), config_file=self.arguments.getArg('slonyCfg'), node=self.client))

                            self.reqTable.releaseLock()
                            maxLoop = 43200 # 12-hour timeout, aia.lev1 takes many hours (probably 6 hours) to dump
                            while True:
                                if maxLoop <= 0:
                                    raise Exception('sqlDump', 'Time-out waiting for dump file to be generated.')
            
                                if not wroteIntMsg and self.sdEvent.isSet():
                                    self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                                    wroteIntMsg = True
                                    # proc.kill()
                                    # raise Exception('shutDown', 'Received shut-down message from main thread.')
                                    
                                # Read what is available from the pipe, convert to utf8, and write it to fout.
                                while True:
                                    pipeBytes = pipeReadEnd.read(4096)
                                    if len(pipeBytes) > 0:
                                        # print(pipeBytes.decode('LATIN1').encode('UTF8'), file=fout)
                                        print(pipeBytes, file=fout, end='')
                                    else:
                                        break
            
                                if proc.poll() is not None:
                                    self.writeStream.flush()
                                    self.writeStream.logLines() # Log all sdo_slony1_dump.sh output.
                                    if proc.returncode != 0:
                                        raise Exception('sqlDump', 'Failure generating dump file, sdo_slony1_dump.sh returned ' + str(proc.returncode) + '.') 
                                        
                                    pipeWriteEnd.flush()
                                    pipeWriteEnd.close()
                                    
                                    # Read the remaining bytes from the pipe.
                                    while True:
                                        pipeBytes = pipeReadEnd.read(4096)
                                        if len(pipeBytes) > 0:
                                            # print(pipeBytes.decode('LATIN1').encode('UTF8'), file=fout)
                                            print(pipeBytes, file=fout, end='')
                                        else:
                                            break
                                    pipeReadEnd.close()                                        
                                    break

                                maxLoop -= 1
                                time.sleep(1)

                            fout.flush()
                            print('COMMIT;', file=fout)
                            self.reqTable.acquireLock()
                            
                        # 5. tar the two dump files together.
                        with tarfile.open(os.path.join(self.triggerDir, self.client + '.sql.tar.gz'), 'w:gz') as tarOut:
                            outFile = os.path.join(self.triggerDir, self.client + '.' + ns + '.createns.sql')
                            tarOut.add(outFile, arcname=os.path.basename(outFile))
                            os.remove(outFile)
                            outFile = os.path.join(self.triggerDir, self.client + '.subscribe_series.sql')
                            tarOut.add(outFile, arcname=os.path.basename(outFile))
                            os.remove(outFile)
    
                        # Tell client that the dump is ready for use. We do that by setting the request status to D.
                        self.request.setStatus('D')
                        reqStatus = 'D'
                
                        time.sleep(1)
                finally:
                    self.reqTable.releaseLock()

            # No lock held here.
            if reqStatus.upper() == 'D':
                self.log.writeDebug([ 'Processing status D for request ' + str(self.requestID) + '.' ])
                # Poll on the request status waiting for it to be I. This indicates that the client has successfully ingested
                # the dump file. The client could also set the status to E if there was some error, in which case the client
                # provides an error message that the server logs. The loop is interruptable by a shut-down request.
                maxLoop = 172800 # 48-hour timeout
                while True:
                    if maxLoop <= 0:
                        raise Exception('sqlAck', 'Time-out waiting for client to ingest dump file.')
                
                    if self.sdEvent.isSet():
                        # Stop polling if main thread is shutting down.
                        raise Exception('shutDown', 'Received shut-down message from main thread.')
                
                    try:
                        self.reqTable.acquireLock()
                        (code, msg) = self.request.getStatus()
                        if code.upper() == 'D':
                            self.log.writeDebug([ 'Waiting for client ' + self.client + ' to download dump file (request ' + str(self.requestID) + '). Status ' + code.upper() + '.'])
                        elif code.upper() == 'A':
                            self.log.writeDebug([ 'Waiting for client ' + self.client + ' to ingest dump file (request ' + str(self.requestID) + '). Status ' + code.upper() + '.'])
                        elif code.upper() == 'I':
                            # Onto clean-up
                            reqStatus = 'I'
                            break
                        elif code.upper() == 'E':
                            reqStatus = 'E'
                            raise Exception('dumpApplication', self.client + ' failed to properly ingest dump file.')
                        else:
                            reqStatus = 'E'
                            raise Exception('invalidReqStatus', 'Unexpected request status ' + code.upper() + '.')
                    finally:
                        self.reqTable.releaseLock()
                
                    maxLoop -= 1
                    time.sleep(1)
            
            # No lock held here.
            if reqStatus == 'I' or (reqStatus == 'P' and self.action == 'unsubscribe'):
                # In the subscribe-action case, the script sdo_slony1_dump.sh puts an entry for client.new / client.new.lst into
                # the slon_parser.cfg and su_production.slonycfg files. But for the unsubscribe-action case, we do not do that.
                # Instead, simply make client.new.lst and the client.new.lst row in su_production.slonycfg have the post-unsubscribe
                # list of tables, and then in clean-up, we COPY this list over client.lst and the client.lst row in su_production.slonycfg.
                # At no point we do populate site_logs/client.new with all series minus the ones being unsubscribed from. Instead,
                # if there are no errors, we instantaneously update the client.new.lst file and the client's row in su_production.slonycfg.
                self.log.writeDebug([ 'Cleaning up request ' + str(self.requestID) + '.' ])
                # Clean-up, regardless of action.
                # reqTable lock is not held here.
                wroteIntMsg = False
                maxLoop = 30
                while True:
                    if not wroteIntMsg and self.sdEvent.isSet():
                        self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                        wroteIntMsg = True

                    try:
                        try:
                            cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                            check_call(cmd, shell=True)
                        except CalledProcessError as exc:
                            raise Exception('subLock')
                    
                        # Obtained global subscription lock.
                    
                        self.reqTable.acquireLock()
                
                        # LEGACY - Remove client.new.lst from slon_parser.cfg
                        with open(self.parserConfigTmp, 'w') as fout, open(self.parserConfig, 'r') as fin:
                            regExp = re.compile(re.escape(self.client + '.new.lst'))

                            for line in fin:
                                matchObj = regExp.match(line)
                                if matchObj is None:
                                    print(line, file=fout)
                
                        os.rename(self.parserConfigTmp, self.parserConfig)

                        # Remove client.new from su_production.slonylst and su_production.slonycfg. Do not call legacy code in
                        # SubTableMgr::Remove. This would delete $node.new.lst, but it is still being used.
                        # cmd="$kRepDir/subscribe_manage/gentables.pl op=remove config=$config_file --node=$node.new"
                        cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new' ]
                        # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                        check_call(cmdList)
                        self.writeStream.logLines() # Log all gentables.pl output
                
                        if self.newSite:
                            # If we are unsubscribing, we cannot be a new site.
                            if self.action == 'unsubscribe':
                                raise Exception('invalidArgument', self.client + ' is a new subscriber. Cannot unsubscribe from series.')
                        
                            # LEGACY - Add a line for client to slon_parser.cfg.
                            with open(self.parserConfig, 'a') as fout:
                                print(self.oldClientLogDir + '        ' + self.oldLstPath, file=fout)
                        
                            # Add client to su_production.slonycfg. At the same time, insert records for client in
                            # su_production.slonylst. The client's sitedir doesn't exit yet, but it will shortly.
                            # Use client.new.lst to populate su_production.slonylst.
                            # $kRepDir/subscribe_manage/gentables.pl op=add config=$config_file --node=$node --sitedir=$subscribers_dir --lst=$tables_dir/$node.new.lst
                            cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=add', 'conf=' + self.slonyCfg, '--node=' + self.client, '--sitedir=' + self.siteLogDir, '--lst=' + self.newLstPath ]
                            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                            check_call(cmdList)
                            self.writeStream.logLines() # Log all gentables.pl output

                            # Rename the client.new site dir.
                            if os.path.exists(self.oldClientLogDir):
                                shutil.rmtree(self.oldClientLogDir)
                    
                            # We are assuming that perms on newClientLogDir were created correctly and that we
                            # do not want to change them at this point.
                            os.rename(self.newClientLogDir, self.oldClientLogDir)
                        else:
                            # Update su_production.slonylst for the client with the new list of series. 
                            cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=replace', 'conf=' + self.slonyCfg, '--node=' + self.client, '--lst=' + self.newLstPath ]
                            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                            check_call(cmdList)
                            self.writeStream.logLines() # Log all gentables.pl output
                    
                            # Copy all the log files in newClientLogDir to oldClientLogDir, overwriting logs of the same name.
                            # There is a period of time where we allow the log parser to run while the client is ingesting
                            # the dump file. During that time, two parallel universes exist - in one universe, the set of 
                            # logs produced is identical to the one that would have been produced had the client not 
                            # taken any subscription action. In the other universe, the set of logs produced is what you'd expect
                            # had the subscription action succeeded. So, if we have a successful subscription, then we need to
                            # discard the logs generated in the first universe, in the oldClientLogDir, overwriting them 
                            # with the analogous logs in the newClientLogDir.
                            #
                            # self.newClientLogDir is not used for un-subscription, nor is it used for re-subscription.
                            if self.action == 'subscribe':
                                for logFile in os.listdir(self.newClientLogDir):
                                    src = os.path.join(self.newClientLogDir, logFile)
                                    dst = os.path.join(self.oldClientLogDir, logFile)
                                    # copy() copies permissing bits as well, and the destination is overwritten if it exists.
                                    shutil.copy(src, dst)

                                # Remove newClientLogDir. I think this is better than deletion during iteration in the
                                # previous loop. If an error happens part way through in the above loop, then we could
                                # get in a weird, indeterminate state.
                                shutil.rmtree(self.newClientLogDir)

                        # LEGACY - Rename the client.new.lst file. If oldLstPath happens to exist, then rename() will overwrite it.
                        os.rename(self.newLstPath, self.oldLstPath)
                
                        break # out of lock-acquisition loop
                    except Exception as exc:
                        if len(exc.args) == 1 and exc.args[0] == 'subLock':
                            # Could not obtain lock; try again (up to maxLoop tries)
                            if maxLoop <= 0:
                                raise Exception('subLock', 'Could not obtain subscription lock after ' + str(maxLoop) + ' tries.')
                            time.sleep(1)
                            maxLoop -= 1
                        else:
                            # gentables.pl failed or creating client.new, re-raise the exception generated by check_call()
                            # or os.mkdir() or os.chmod().
                            raise
                    finally:
                        self.reqTable.releaseLock()
                        if os.path.exists(self.subscribeLock):
                            os.remove(self.subscribeLock)
                        
            # Outside of lock-acquisition loop.
            # reqTable lock is NOT held.
            # The request has been completely processed without error. Set status to 'C'.
            self.reqTable.acquireLock()
            try:
                self.request.setStatus('C')
            finally:
                self.reqTable.releaseLock()
        except ValueError as exc:
            # Popen() and check_call() will raise this exception if arguments are bad.
            errorMsg = 'Command called with invalid arguments: ' + ' '.join(cmdList) + '.'
            self.log.writeError([ errMsg ])
        except FileNotFoundError as exc:
            import traceback
            
            errorMsg = 'File not found.'
            self.log.writeError([ errorMsg ])
            self.log.writeError([ traceback.format_exc(5) ])
        except IOError as exc:
            import traceback
            
            errorMsg = 'IO error.'
            self.log.writeError([ traceback.format_exc(5) ])
        except OSError as exc:
            # Popen() and check_call() can raise this.
            errorMsg = exc.diag.message_primary
            self.log.writeError([ errorMsg ])
        except CalledProcessError as exc:
            # check_call() can raise this.
            errorMsg = 'Command returned non-zero status code ' + str(exc.returncode) + ': '+ ' '.join(cmdList) + '.'
            self.log.writeError([ errorMsg ])
        except Exception as exc:
            if len(exc.args) == 2:
                eType, eMsg = exc.args
                
                if eType == 'shutDown':
                    msg = 'Worker (requed ID ' + str(self.requestID) + ') received shutdown signal.'
                    self.saveState = True # Do not clean up temp files; we shut down, perhaps in the middle of a request that was being processed.
                                          # Resume on restart.
                else:
                    msg = 'Problem in worker thread (request ID ' + str(self.requestID) + '): ' + eMsg + ' ('+ eType + ').'                    
                    errorMsg = msg

                if self.log:
                    self.log.writeError([ msg ])
            else:
                import traceback
                
                errorMsg = 'Unknown error in worker thread (request ID ' + str(self.requestID) + '); thread terminating.'
                self.log.writeError([ errorMsg ])
                self.log.writeError([ traceback.format_exc(5) ])
        finally:
            self.reqTable.acquireLock()
            self.log.writeInfo([ 'Worker thread (request ID ' + str(self.requestID) + ') terminating.' ])
                
            if errorMsg:
                # Set request status to error.
                self.log.writeDebug([ 'Setting status for request ' + str(self.requestID) + ' to E.' ])
                self.request.setStatus('E', errorMsg)
                time.sleep(1)
                
            # Detach this worker from the request that it is doing work for.
            self.request.removeWorker()
            
            # Always release reqTable lock.
            self.reqTable.releaseLock()
            maxLoop = 30
            while True:
                try:
                    try:
                        cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                        check_call(cmd, shell=True)
                    except CalledProcessError as exc:
                        raise Exception('subLock')
                        
                    # Obtained global subscription lock.
                    if errorMsg:
                        # There was some kind of error. We need to clean up several temporary things, and we need to remove the client
                        # entry from the configuration table IFF the client was a new subscriber.
                        self.cleanOnError()
                    elif not self.saveState:
                        self.cleanTemp()
                    break
                except Exception as exc:
                    if len(exc.args) == 1 and exc.args[0] == 'subLock':
                        # Could not obtain lock; try again (up to maxLoop tries)
                        if maxLoop <= 0:
                            raise Exception('subLock', 'Could not obtain subscription lock after ' + str(maxLoop) + ' tries.')
                        time.sleep(1)
                        maxLoop -= 1
                    else:
                        # gentables.pl failed or creating client.new, re-raise the exception generated by check_call()
                        # or os.mkdir() or os.chmod().
                        raise
                finally:
                    if os.path.exists(self.subscribeLock):
                        os.remove(self.subscribeLock)
                        
            # This thread is about to terminate. 
            # We need to check the class tList variable to update it, so we need to acquire the lock.
            try:
                Worker.lock.acquire()
                Worker.tList.remove(self) # This thread is no longer one of the running threads.
                if len(Worker.tList) == Worker.maxThreads - 1:
                    # Fire event so that main thread can process additional requests.
                    Worker.eventMaxThreads.set()
                    # Clear event so that main will block the next time it calls wait.
                    Worker.eventMaxThreads.clear()
            finally:
                Worker.lock.release()

    def stop(self, saveState=False):
        self.log.writeInfo([ 'Stopping worker (request ID ' + str(self.requestID) + ').'])
        self.saveState = saveState
        self.sdEvent.set()
            
    def cleanTemp(self):
        try:
            # LEGACY - Remove the client.lst.new file entry from slon_parser.cfg.
            (head, tail) = os.path.split(self.parserConfig)
            if not os.path.exists(head):
                os.mkdir(head)
            if not os.path.exists(self.parserConfig):
                open(self.parserConfig, 'w').close()
            if os.path.exists(self.parserConfigTmp):
                os.remove(self.parserConfigTmp)

            self.log.writeDebug([ 'Removing ' + self.client + '.new.lst from ' + self.parserConfig + '.' ])
            with open(self.parserConfigTmp, 'w') as fout, open(self.parserConfig, 'r') as fin:
                regExp = re.compile(r'' + self.client + '\.new\.lst')
                regExpSp = re.compile(r'\s*$')

                for line in fin:
                    line = line.rstrip()
                    matchObj = regExpSp.match(line)
                    if matchObj is not None:
                        continue
                
                    matchObj = regExp.search(line)
                    if matchObj is None:
                        self.log.writeDebug([ 'Added ' + line + ' to ' + self.parserConfigTmp + '.'])
                        print(line, file=fout)

            os.rename(self.parserConfigTmp, self.parserConfig)
            self.log.writeDebug([ 'Renamed ' + self.parserConfigTmp + ' to ' + self.parserConfig + '.' ])

            # LEGACY - Delete the client.lst.new file.
            if os.path.exists(self.newLstPath):
                os.remove(self.newLstPath)
                self.log.writeDebug([ 'Removed ' + self.newLstPath + '.' ])
            
            # Remove client.new from su_production.slonylst and su_production.slonycfg. 
            cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new' ]
            self.log.writeDebug( [ 'Calling check_call(): ' + ' '.join(cmdList) + '.' ])
            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
            check_call(cmdList)
            self.writeStream.logLines() # Log all gentables.pl output
            self.log.writeDebug( [ 'Success calling check_call().' ])
        
            # Remove client.new site-log directory.
            if os.path.exists(self.newClientLogDir):
                self.log.writeInfo(['Removing temporary site-log directory ' + self.newClientLogDir + '.'])
                shutil.rmtree(self.newClientLogDir)
        
            # LEGACY - Remove slon_parser.cfg.tmp from tables dir.
            if os.path.exists(self.parserConfigTmp):
                os.remove(self.parserConfigTmp)

            # Remove client-sqlgen.txt.tmp temporary argument file from triggers directory.
            if os.path.exists(self.sqlgenArgFile):
                os.remove(self.sqlgenArgFile)
        
            # LEGACY - Other crap from the initial implementation. The newer implementation does not
            # use files for passing information back and forth to/from the client.
        
            # Remove client's dump files. Remove untar'd version.
            createNSFiles = glob.glob(os.path.join(self.triggerDir, self.client + '.*.createns.sql'))
            for afile in createNSFiles:
                os.remove(afile)
            
            dumpFile = os.path.join(self.triggerDir, self.client + '.subscribe_series.sql')
            if os.path.exists(dumpFile):
                os.remove(dumpFile)
                        
            # Remove client's dump file. Remove tar file.
            if os.path.exists(os.path.join(self.triggerDir, self.client + '.sql.tar')):
                os.remove(os.path.join(self.triggerDir, self.client + '.sql.tar'))

            # Remove client's dump file. Remove tarball.
            if os.path.exists(os.path.join(self.triggerDir, self.client + '.sql.tar.gz')):
                os.remove(os.path.join(self.triggerDir, self.client + '.sql.tar.gz'))
        except OSError as exc:
            raise Exception('fileIO', exc.diag.message_primary)
        except CalledProcessError as exc:
            raise Exception('cleanTemp', 'Command returned non-zero status code ' + str(exc.returncode) + ': '+ ' '.join(cmdList) + '.')

    def cleanOnError(self):
        # Stuff to remove if an error happens anywhere during the subscription process. Remove site from
        # set of subscribers completely, if the site was a new subscriber.
        try:
            # If dump files exist, rename them for debugging purposes.
            createNSFiles = glob.glob(os.path.join(self.triggerDir, self.client + '.*.createns.sql'))
            dumpFile = os.path.join(self.triggerDir, self.client + '.subscribe_series.sql')
            tarFile = os.path.join(self.triggerDir, self.client + '.sql.tar.gz')

            for afile in createNSFiles:
                savedFile = afile + '.' + datetime.now().strftime('%Y-%m-%d-%H%M%S')
                self.log.writeDebug([ 'Saving dump file ' + afile + ' to ' + savedFile + '.' ])
                os.rename(afile, savedFile)
            if os.path.exists(dumpFile):
                savedFile = dumpFile + '.' + datetime.now().strftime('%Y-%m-%d-%H%M%S')
                self.log.writeDebug([ 'Saving dump file ' + dumpFile + ' to ' + savedFile + '.' ])
                os.rename(dumpFile, savedFile)
            if os.path.exists(tarFile):
                savedFile = tarFile + '.' + datetime.now().strftime('%Y-%m-%d-%H%M%S')
                self.log.writeDebug([ 'Saving tar file ' + tarFile + ' to ' + savedFile + '.' ])
                os.rename(tarFile, savedFile)
        
            self.cleanTemp()

            # LEGACY - If this was a newsite subscription, then we need to remove the client entry
            # from slon_parser.cfg and remove client.lst from the table dir.
            if self.newSite:
                self.log.writeDebug([ 'Removing ' + self.client + '.lst from ' + self.parserConfig + '.' ])
                with open(self.parserConfigTmp, 'w') as fout, open(self.parserConfig, 'r') as fin:
                    regExp = re.compile(r'' + self.client + '\.lst')
                    regExpSp = re.compile(r'\s*$')

                    for line in fin:
                        line = line.rstrip()

                        matchObj = regExpSp.match(line)
                        if matchObj is not None:
                            continue
                        
                        matchObj = regExp.search(line)
                        if matchObj is None:
                            self.log.writeDebug([ 'Added ' + line + ' to ' + self.parserConfigTmp + '.'])
                            print(line, file=fout)                            

                os.rename(self.parserConfigTmp, self.parserConfig)
                
                if os.path.exists(self.oldLstPath):
                    os.remove(self.oldLstPath)
                
                if os.path.exists(self.newLstPath):
                    os.remove(self.newLstPath)
    
            # If this was a newsite subscription, then we need to remove the client entry
            # from su_production.slonycfg and remove the client's rows from su_production.slonylst.
            if self.newSite:            
                cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client ]
                # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                check_call(cmdList)
                self.writeStream.logLines() # Log all gentables.pl output
        except CalledProcessError as exc:
            self.log.writeError([ 'Error running gentables.pl, status ' +  str(exc.returncode) + '.'])

    @staticmethod
    def newThread(request, newSite, arguments, connMaster, connSlave, log, writeStream, fclean):
        worker = Worker(request, newSite, arguments, connMaster, connSlave, log, writeStream, fclean)
        worker.tList.append(worker)
        log.writeDebug([ 'Calling worker.start().' ])
        worker.start()
        log.writeDebug([ 'Successfully called worker.start().' ])
        return worker
        
class Request(object):
    def __init__(self, conn, log, reqtable, dbtable, requestid, client, starttime, action, series, archive, retention, tapegroup, status, errmsg):
        self.conn = conn
        self.log = log
        self.reqtable = reqtable
        self.dbtable = dbtable
        self.requestid = requestid
        self.client = client
        self.starttime = starttime
        self.action = action
        self.series = series
        self.archive = archive
        self.retention = retention
        self.tapegroup = tapegroup
        self.status = status
        self.errmsg = errmsg
        
    def copy(self, source):
        self.conn = source.conn
        self.log = source.log
        self.reqtable = source.reqtable
        self.dbtable = source.dbtable
        self.requestid = source.requestid
        self.client = source.client
        self.starttime = source.starttime
        self.action = source.action
        self.series = source.series
        self.archive = source.archive
        self.retention = source.retention
        self.tapegroup = source.tapegroup
        self.status = source.status
        self.errmsg = source.errmsg
        
    def setStatus(self, code, msg=None):
        # ART - Validate code
        self.status = code
        if msg is not None:
            self.errmsg = msg

        try:            
            # Write to the DB.
            with self.conn.cursor() as cursor:
                # The requestid column is an integer.
                if self.errmsg:
                    cmd = 'UPDATE ' + self.dbtable + " SET status='" + self.status + "', errmsg='" + self.errmsg + "' WHERE requestid=" + str(self.requestid)
                else:
                    cmd = 'UPDATE ' + self.dbtable + " SET status='" + self.status + "' WHERE requestid=" + str(self.requestid)

                cursor.execute(cmd)
                self.conn.commit()
        except psycopg2.Error as exc:
            self.conn.rollback()
            raise Exception('reqtableWrite', exc.diag.message_primary)
        except:
            self.conn.rollback()
            raise
            
    def getStatus(self):
        try:
            with self.conn.cursor() as cursor:
                # The requestid column is an integer.
                cmd = 'SELECT status, errmsg FROM ' + self.dbtable + " WHERE requestid=" + str(self.requestid)

                cursor.execute(cmd)
                records = cursor.fetchall()
                if len(records) != 1:
                    raise Exception('reqtableRead', 'Unexpected number of database rows returned from query: ' + cmd + '.')

                self.status = records[0][0]    # text
                self.errmsg = records[0][1]    # text

                return (self.status, self.errmsg)
        except psycopg2.Error as exc:
            raise Exception('reqtableRead', exc.diag.message_primary)
        finally:
            self.conn.rollback()
    
    def hasWorker(self):
        return hasattr(self, 'worker') and self.worker
        
    def getWorker(self):
        if self.hasWorker():
            return self.worker
        return None
     
    def setWorker(self, worker):
        if not isinstance(worker, Worker):
            raise Exception('invalidArgument', 'Type of argument to setWorker argument must be Worker; ' + type(worker) + ' was provided.')
        self.worker = worker
        
    def stopWorker(self, **kwargs):
        if 'saveState' in kwargs:
            saveState = kwargs['saveState']
        else:
            saveState = False
        if 'wait' in kwargs:
            wait = kwargs['wait']
        else:
            wait = False
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']
        else:
            timeout = 30;
            
        if hasattr(self, 'worker') and self.worker:
            self.worker.stop(saveState)
            if wait and self.worker.isAlive():
                 self.worker.join(timeout)
            self.worker = None
                 
    def removeWorker(self):
        if hasattr(self, 'worker') and self.worker:
            self.worker = None

    def dump(self, toLog=True):
        text = 'requestID=' + str(self.requestid) + ', client=' + self.client + ', starttime=' + self.starttime.strftime('%Y-%m-%d %T') + ', series=' + ','.join(self.series) + ', action=' + self.action + ', archive=' + str(self.archive) + ', retention=' + str(self.retention) + ', tapegroup=' + str(self.tapegroup) + ', status=' + self.status + ', errmsg=' + str(self.errmsg if (self.errmsg and len(self.errmsg) > 0) else "''")
        if toLog:
            self.log.writeInfo([ text ])
        return text

class ReqTable(object):    
    def __init__(self, tableName, timeOut, conn, log):
        self.tableName = tableName
        self.timeOut = timeOut
        self.conn = conn
        self.log = log
        self.lock = threading.Lock()
        self.reqDict = {}
        
        self.tryRead()
    
    def read(self):
        # requests(client, requestid, starttime, action, series, status, errmsg)
        cmd = 'SELECT requestid, client, starttime, action, series, archive, retention, tapegroup, status, errmsg FROM ' + self.tableName

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(cmd)        
        
                for record in cursor:
                    requestidStr = str(record[0])

                    requestid = record[0] # bigint
                    client = record[1]    # text
                    # Whoa! For starttime, pyscopg2 returns timestamps as datetime.datetime objects already!
                    starttime = record[2] # datetime.datetime
                    action = record[3]    # text            
                    series = [ aseries for aseries in record[4].split(',') ] # text (originally)
                    archive = record[5]   # int
                    retention = record[6] # int
                    tapegroup = record[7] # int
                    status = record[8]    # text
                    errmsg = record[9]    # text
                    req = Request(self.conn, self.log, self, self.tableName, requestid, client, starttime, action, series, archive, retention, tapegroup, status, errmsg)
                    self.reqDict[requestidStr] = req
        except psycopg2.Error as exc:
            raise Exception('reqtableRead', exc.diag.message_primary + ': ' + cmd + '.')
        finally:
            self.conn.rollback()        

    def tryRead(self):
        nAtts = 0
        while True:
            try:
                self.read()
                break
            except Exception as exc:
                if len(exc.args) != 2:
                    raise # Re-raise

                etype = exc.args[0]

                if etype == 'reqtableRead':
                    if nAtts > 10:
                        raise # Re-raise
                else:
                    raise

            nAtts += 1
            time.sleep(1)

    # This method finds new requests ('N' records) inserted since the last time it was run (or since the table was first read). It ignores
    # all other changes to the database table (made from outside this program) that have happened. To read those changes,
    # shut down this program, then make the changes, then start this program again.
    def refresh(self):
        # Get a fresh copy of the requests from the DB.
        latestReqTable = ReqTable(self.tableName, self.timeOut, self.conn, self.log)
        latestReqs = latestReqTable.get()
        oldReqs = self.get()
        
        latestReqs.sort(key=lambda req : req.requestid)
        oldReqs.sort(key=lambda req : req.requestid)

        if len(oldReqs) == 0:
            # Copy all from latestReqs.
            for latestReq in latestReqs:
                req = Request(self.conn, self.log, self, self.tableName, latestReq.requestid, latestReq.client, latestReq.starttime, latestReq.action, latestReq.series, latestReq.archive, latestReq.retention, latestReq.tapegroup, latestReq.status, latestReq.errmsg)
                # This new request has a pointer to the old request table, which is correct.
                self.reqDict[str(latestReq.requestid)] = req
                
            return
        
        if len(latestReqs) == 0:
            # Delete all requests.
            for oldReq in oldReqs:
                # Stop any associated worker.
                oldReq.stopWorker(wait=True, timeout=120)
                
            self.reqDict.clear()
            
            return

        # Step through all reqs in both lists. If the current requestIDs match, then copy from latest to old, and 
        # increment both pointers. If they do not match, then if the smaller requestID is from the latest, copy over.
        # Increment the iLatest pointer. If the smaller requestID is from the old, delete and increment iOld.
        toDel = []
        toAdd = []
        iLatest = 0
        iOld = 0
        
        while iLatest < len(latestReqs) or iOld < len(oldReqs):
            if iLatest < len(latestReqs):
                latestReq = latestReqs[iLatest]
                
                # We don't want the new request table being referenced from any new request.
                latestReq.conn = self.conn
                latestReq.log = self.log
                latestReq.reqtable = self
                latestReq.tableName = self.tableName
            else:
                latestReq = None
                        
            if iOld < len(oldReqs):
                oldReq = oldReqs[iOld]                
            else:
                oldReq = None
        
            if latestReq and oldReq and (latestReq.requestid == oldReq.requestid):
                # Don't delete the oldReq - just update its attributes. There could be a worker currently processing
                # the request. The only thing that should change is the status (request-subs.py could change it).
                self.reqDict[str(oldReq.requestid)].copy(latestReq)
                
                iLatest += 1
                iOld += 1
            elif latestReq and oldReq:
                if latestReq.requestid < oldReq.requestid:
                    toAdd.append(latestReq)
                    iLatest += 1
                else:
                    toDel.append(oldReq)
                    iOld += 1
            elif latestReq:
                toAdd.append(latestReq)
                iLatest += 1
            else:
                toDel.append(oldReq)
                iOld += 1
                
        for req in toDel:
            req.stopWorker(wait=True, timeout=120)
            del self.reqDict[str(req.requestid)]
            
        for req in toAdd:
            self.reqDict[str(req.requestid)] = req
        
    def acquireLock(self):
        self.lock.acquire()
        self.locked = True
    
    def releaseLock(self):
        if self.locked:
            self.lock.release()
            self.locked = False
        
    def setStatus(self, requestids, code, msg=None):
        for arequestid in requestids:
            requestidStr = str(arequestid)
    
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')

            if msg:
                self.reqDict[requestidStr].setStatus(code, msg)
                self.reqDict[requestidStr]['errmsg'] = msg
            else:
                self.reqDict[requestidStr].setStatus(code)

    def getStatus(self, requestids):
        statuses = []
        for arequestid in requestids:
            requestidStr = str(arequestid)
            
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')
            
            # A 2-tuple: code, msg
            status = self.reqDict[requestidStr].getStatus()[0]
            
            statuses.append(status)       
            
        return statuses

    def get(self, requestids=None):
        toRet = []
    
        if not requestids:
            return [ self.reqDict[key] for (key, val) in self.reqDict.items() ]
        
        for arequestid in requestids:
            requestidStr = str(arequestid)
            
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')
    
            toRet.append(self.reqDict[requestidStr])
    
        return toRet
    
    def getPending(self, client=None):
        pendLst = []
    
        for requestidStr in self.reqDict.keys():
            if (self.reqDict[requestidStr].status == 'P' or self.reqDict[requestidStr].status == 'D' or self.reqDict[requestidStr].status == 'I') and (client is None or self.reqDict[requestidStr].client == client):
                self.log.writeDebug([ 'Adding request (ID ' + requestidStr + ') to pending list.' ])
                pendLst.append(self.reqDict[requestidStr])

        if len(pendLst) > 0:    
            # Sort by start time. Sorts in place - and returns None.
            pendLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return pendLst
    
    def getNew(self, client=None):
        newLst = []

        for requestidStr in self.reqDict.keys():
            if self.reqDict[requestidStr].status == 'N' and (client is None or self.reqDict[requestidStr].client == client):
                self.log.writeDebug([ 'Adding request (ID ' + requestidStr + ') to new list.' ])
                newLst.append(self.reqDict[requestidStr])            
        
        if len(newLst) > 0:
            # Sort by start time. Sorts in place - and returns None.
            newLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return newLst

    def getProcessing(self, client=None):
        procLst = []

        # Return results for client only.
        for requestidStr in self.reqDict.keys():
            if (self.reqDict[requestidStr].status == 'P' or self.reqDict[requestidStr].status == 'D' or self.reqDict[requestidStr].status == 'I') and (client is None or self.reqDict[requestidStr].client == client):
                self.log.writeDebug([ 'Adding request (ID ' + requestidStr + ') to processing list.' ])
                procLst.append(self.reqDict[requestidStr])

        if len(procLst) > 0:    
            # Sort by start time. Sorts in place - and returns None.
            procLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return procLst
        
    def getInError(self, client=None):
        errLst = []
    
        for requestidStr in self.reqDict.keys():
            if self.reqDict[requestidStr].status == 'E' and (client is None or self.reqDict[requestidStr].client == client):
                self.log.writeDebug([ 'Adding request (ID ' + requestidStr + ') to in-error list.' ])
                errLst.append(self.reqDict[requestidStr])

        if len(errLst) > 0:
            # Sort by start time. Sorts in place - and returns None.
            errLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return errLst

    def deleteRequests(self, requestids):
        for arequestid in requestids:
            requestidStr = str(arequestid)
    
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')

            try:
                cmd = 'DELETE FROM ' + self.tableName + ' WHERE requestid=' + str(self.reqDict[requestidStr].requestid)
                with self.conn.cursor() as cursor:
                    cursor.execute(cmd)
                    self.conn.commit()
            except psycopg2.Error as exc:
                self.conn.rollback()
                raise Exception('reqtableWrite', exc.diag.message_primary + ': ' + cmd + '.')
            except:
                self.conn.rollback()
                raise

            self.reqDict[requestidStr].stopWorker(wait=True) # Will stop worker and clean-up temp files, if a worker exists. Waits for worker to terminate.
            del self.reqDict[requestidStr]

    def getTimeout(self):
        return self.timeOut

class SubscriptionList(object):
    def __init__(self, sublistDbTable, conn, client):
        self.sublistDbTable = sublistDbTable
        self.conn = conn
        self.client = client
        self.list = []
        self.getSubscriptionList()

    def getSubscriptionList(self):
        if len(self.list) == 0:
            cmd = 'SELECT series FROM ' + self.sublistDbTable + " WHERE node = '" + self.client + "' ORDER BY series"

            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(cmd)
                    records = cursor.fetchall()
                    self.list = [ rec[0] for rec in records ]            
            except psycopg2.Error as exc:
                raise Exception('getSubList', exc.diag.message_primary)
            finally:
                self.conn.rollback()

        return self.list

    def refreshSubscriptionList(self):
        if len(self.list) > 0:
            del self.list[:]
        self.getSubscriptionList()
        
class CfgAction(argparse.Action):
    def __init__(self, option_strings, dest, arguments, *args, **kwargs):
        self.arguments = arguments
        super(CfgAction, self).__init__(option_strings, dest, *args, **kwargs)
        
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        # Put all arguments inside the Slony configuration file into self.arguments
        self.arguments.addFileArgs(values)

class BooleanAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values.lower() == 'true':
            setattr(namespace, self.dest, True)
        elif values.lower() == 'false':
            setattr(namespace, self.dest, False)
        else:
            raise Exception('args', "Argument value must be 'True' or 'False'.")

class ListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))

class LogLevelAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        valueLower = value.lower()
        if valueLower == 'critical':
            level = logging.CRITICAL
        elif valueLower == 'error':
            level = logging.ERROR
        elif valueLower == 'warning':
            level = logging.WARNING
        elif valueLower == 'info':
            level = logging.INFO
        elif valueLower == 'debug':
            level = logging.DEBUG
        else:
            level = logging.ERROR

        setattr(namespace, self.dest, level)
        
def clientIsNew(arguments, conn, client):
    # Master database.
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + arguments.getArg('kCfgTable') + ' WHERE ' + CFG_TABLE_NODE + " = '" + client + "'"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        conn.rollback() # closes the cursor
    
    if len(records) == 0:
        return True
    else:
        return False
        
def dbTableExists(conn, schema, table):
    cmd = "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '" + schema + "' AND c.relname = '" + table + "'"

    try:
        cursor = conn.cursor()
        cursor.execute(cmd)
        records = cursor.fetchall()
        if len(records) > 1:
            raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')

    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        conn.rollback() # closes the cursor
        
    if len(records) == 1:
        return True
    else:
        return False

def cleanAllSavedFiles(triggerDir, log):
    regExpNs = re.compile(r'.+\.createns\.sql\.(.+)$')
    regExpDump = re.compile(r'.+\.subscribe_series\.sql\.(.+)$')
    regExpTar = re.compile(r'.+\.sql.tar.gz\.(.+)$')
    
    for file in os.listdir(triggerDir):
        date = None
        matchNs = regExpNs.match(file)
        
        if matchNs:
            date = matchNs.group(1)
        else:
            matchDump = regExpDump.match(file)
            if matchDump:
                date = matchDump.group(1)
            else:
                matchTar = regExpTar.match(file)
                if matchTar:
                    date = matchTar.group(1)

        if date:
            try:
                # This was difficult to get right - you cannot use %T for strptime(), although you can use it for strftime().
                datetime.strptime(date, '%Y-%m-%d-%H%M%S')

                # OK to remove the file.
                log.writeInfo([ 'Removing saved dump file: ' + os.path.join(triggerDir, file) + '.' ])
                os.remove(os.path.join(triggerDir, file))
            except ValueError:
                pass

# Main Program
if __name__ == "__main__":
    rv = RV_TERMINATED
    
    try:
        msLog = None
        manageSubsParams = ManageSubsParams()
        arguments = Arguments()

        parser = CmdlParser(usage='%(prog)s [ slonycfg=<configuration file> ] [ loglevel=<critical, error, warning, info, or debug > ]')    
        parser.add_argument('cfg', '-c', '--cfg', help='The configuration file that contains information needed to locate database information.', metavar='<slony configuration file>', dest='slonyCfg', action=CfgAction, arguments=arguments, default=manageSubsParams.get('SLONY_CONFIG'))
        parser.add_argument('loglevel', '-l', '--loglevel', help='Specifies the amount of logging to perform. In increasing order: critical, error, warning, info, debug', dest='loglevel', action=LogLevelAction, default=logging.ERROR)
        parser.add_argument('fclean', '-f', '--fclean', help='Force clean up temporary files from an earlier run.', dest='fclean', action='store_true', default=False)

        arguments.setParser(parser)
    
        pid = os.getpid()
        
        # Create/Initialize the log file.
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        msLog = Log(os.path.join(arguments.getArg('kSMlogDir'), 'manage-subs-log.txt'), arguments.getArg('loglevel'), formatter)
        pipeReadEndFD, pipeWriteEndFD = os.pipe()

        # Make these pipes not block on read.
        flag = fcntl.fcntl(pipeReadEndFD, fcntl.F_GETFL)
        fcntl.fcntl(pipeReadEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)
        flag = fcntl.fcntl(pipeWriteEndFD, fcntl.F_GETFL)
        fcntl.fcntl(pipeWriteEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)

        pipeReadEnd = os.fdopen(pipeReadEndFD, 'r')
        pipeWriteEnd = os.fdopen(pipeWriteEndFD, 'w')
        writeStream = WriteStream(pipeReadEnd, pipeWriteEnd, msLog)

        arguments.dump(msLog)
        msLog.writeCritical(['Logging threshold level is ' + msLog.getLevel() + '.']) # Critical - always write the log level to the log.
            
        thContainer = [ os.path.join(arguments.getArg('kServerLockDir'), 'manage-subs-lock.txt'), os.path.join(arguments.getArg('kServerLockDir'), arguments.getArg('kSubLockFile')), str(pid), msLog, writeStream, pipeReadEnd, pipeWriteEnd, None ]
        with TerminationHandler(thContainer) as th:
            # Redirect all the output from subprocesses to writeStream.
            with RedirectStdFileStreams(stdoutFileObj=pipeWriteEnd, stderrFileObj=pipeWriteEnd) as stdStreams:
                with psycopg2.connect(database=arguments.getArg('SLAVEDBNAME'), user=arguments.getArg('REPUSER'), host=arguments.getArg('SLAVEHOSTNAME'), port=str(arguments.getArg('SLAVEPORT'))) as connSlave, psycopg2.connect(database=arguments.getArg('MASTERDBNAME'), user=arguments.getArg('REPUSER'), host=arguments.getArg('MASTERHOSTNAME'), port=str(arguments.getArg('MASTERPORT'))) as connMaster:
                    msLog.writeInfo([ 'Connected to database ' + arguments.getArg('SLAVEDBNAME') + ' on ' + arguments.getArg('SLAVEHOSTNAME') + ':' + str(arguments.getArg('SLAVEPORT')) + ' as user ' + arguments.getArg('REPUSER') ])
                    msLog.writeInfo([ 'Connected to database ' + arguments.getArg('MASTERDBNAME') + ' on ' + arguments.getArg('MASTERHOSTNAME') + ':' + str(arguments.getArg('MASTERPORT')) + ' as user ' + arguments.getArg('REPUSER') ])

                    # We hang on to old dump files only until we restart manage-subs.py, then we deleted them.
                    cleanAllSavedFiles(arguments.getArg('dumpDir'), msLog)

                    # Read the requests table into memory.
                    reqTable = ReqTable(arguments.getArg('kSMreqTable'), timedelta(seconds=int(arguments.getArg('kSMreqTableTimeout'))), connSlave, msLog)
                    msLog.writeInfo([ 'Requests table ' + arguments.getArg('kSMreqTable') + ' loaded from ' + arguments.getArg('SLAVEHOSTNAME') + '.'])

                    # Main dispatch loop. When a SIGINT (ctrl-c), SIGTERM, or SIGHUP is received, the 
                    # terminator context manager will be exited.
                    firstIter = True
                    while True:
                        # Deal with pending requests first. If a request has been pending too long, then delete the request now
                        # and send an error message back to the requestor. This will clean-up requests that died somewhere
                        # while being processed.
                        try:
                            reqTable.acquireLock()
                            reqsPending = reqTable.getPending()
                            
                            for areq in reqsPending:
                                msLog.writeInfo([ 'Found a pending request: ' + areq.dump(False) + '.'])
                                timeNow = datetime.now(areq.starttime.tzinfo)
                                if timeNow > areq.starttime + reqTable.getTimeout():
                                    msLog.writeError(['The processing of request ' + str(areq.requestid) + ' timed-out.'])
                
                                    # Kill the Worker thread (if it exists). The Worker will clean-up subscription-management files, 
                                    # like client.new.lst.
                                    areq.setStatus('E', 'Processing timed-out.')
                                    areq.stopWorker(wait=False)
                                else:
                                    if not areq.hasWorker() or not areq.getWorker().isAlive():
                                        # Spawn a worker to handle pending request that has no worker.
                                        newSite = clientIsNew(arguments, connMaster, areq.client)
                                    
                                        countDown = 5
                                        while countDown > 0:
                                            # Must have request-table lock. The worker thread is going to check the request status to
                                            # ensure it is pending. But we don't want to do that until the main thread has set it
                                            # to pending.
                                            Worker.lock.acquire()
                                            try:
                                                if len(Worker.tList) < Worker.maxThreads:
                                                    msLog.writeInfo([ 'Instantiating a worker thread for request ' + str(areq.requestid) + ' for client ' + areq.client + '.' ])
                                                    if not areq.hasWorker():
                                                        doClean = arguments.getArg('fclean')
                                                    else:
                                                        # The worker thread died. Remove old Worker object.
                                                        areq.stopWorker(wait=False)
                                                        doClean = False
                                                        
                                                    areq.setWorker(Worker.newThread(areq, newSite, arguments, connMaster, connSlave, msLog, writeStream, doClean))
                                                    if doClean:
                                                        reqTable.setStatus([ areq.requestid ], 'P') # The new request is now being processed. If there was an issue creating the new thread, an exception will have been raised, and we will not execute this line.
                                                    break # The finally clause will ensure the Worker lock is released.
                                            finally:
                                                Worker.lock.release()
                    
                                            # Worker.eventMaxThreads.wait() # Wakes up when a worker thread completes. Don't use.
                                            # Poll instead so that we can get other things done while we are waiting for a thread 
                                            # to become available.
                                            time.sleep(1)
                                            countDown -= 1

                            # Completed requests, success and failures alike are handled by request-subs.py. There is nothing to do here.
                        finally:
                            reqTable.releaseLock()
                    
                            
                        # Remove old errored-out requests. The records of these errored-out requests should be
                        # available for a while so that request-subs.py can pass the error along to the requestor,
                        # but after a time-out, the errored-out request record should be deleted.
                        try:
                            reqTable.acquireLock()
                            reqsInError = reqTable.getInError()
                            expiredAndInError = []

                            for areq in reqsInError:
                                timeNow = datetime.now(areq.starttime.tzinfo)
                                if timeNow > areq.starttime + reqTable.getTimeout():
                                    msLog.writeInfo(['Deleting the record for errored-out request: (' + areq.dump(False) + ').'])
                                    expiredAndInError.append(areq.requestid)
                                    
                            if len(expiredAndInError) > 0:
                                reqTable.deleteRequests(expiredAndInError)
                                
                        finally:
                            reqTable.releaseLock()
                        
                        # If the requestor is making a subscription request, but there is already a request
                        # pending for that requestor, then send an error message back to the requestor 
                        # that will cause the client code to exit without modifying any client state. This is 
                        # actually already handled in request-subs.py, but handle it here too (don't send a
                        # message to the client - manage-subs.py is not communicating directly to the client)
                        # by killing the request (that should not have been allowed to get to the manager
                        # in the first place).
                        try:
                            reqTable.acquireLock()
                            reqsNew = reqTable.getNew()

                            for areq in reqsNew:
                                msLog.writeInfo([ 'Found a new request: (' + areq.dump(False) + ').' ])
                            
                                timeNow = datetime.now(areq.starttime.tzinfo)
                                if timeNow > areq.starttime + reqTable.getTimeout():
                                    msLog.writeError(['The processing of request ' + str(areq.requestid) + ' timed-out.'])                
                                    areq.setStatus('E', 'Processing timed-out.')
                                else:
                                    # Start a Worker thread to handle the subscription request.
                                    if reqTable.getProcessing([ areq.client ]):                        
                                        # Set the status of this pending request to an error code.
                                        areq.setStatus('E', 'Server busy processing request for client ' + areq.client + ' already.')
                                    else:
                                        # At this point, the wrapper CGI, request-subs.py, has ensured that all requests in 
                                        # the request table are valid. 
                                        newSite = clientIsNew(arguments, connMaster, areq.client)
            
                                        # Process the request, but time-out after five seconds so we don't get stuck here
                                        # waiting for a slot to open up. If all slots are full and could take a long time to process,
                                        # then we could get stuck in this loop for a long time if not for a time-out. It
                                        # would be better do another iteration of the outer, main loop, wherein
                                        # lies code that can finalize pending request.
                                        countDown = 5
                                        while countDown > 0:
                                            # Must have request-table lock. The worker thread is going to check the request status to
                                            # ensure it is pending. But we don't want to do that until the main thread has set it
                                            # to pending.
                                            Worker.lock.acquire()
                                            try:
                                                if len(Worker.tList) < Worker.maxThreads:
                                                    msLog.writeInfo([ 'Instantiating a worker thread for request ' + str(areq.requestid) + ' for client ' + areq.client + '.' ])
                                                    areq.setWorker(Worker.newThread(areq, newSite, arguments, connMaster, connSlave, msLog, writeStream, arguments.getArg('fclean')))
                                                    reqTable.setStatus([ areq.requestid ], 'P') # The new request is now being processed. If there was an issue creating the new thread, an exception will have been raised, and we will not execute this line.
                                                    msLog.writeDebug([ 'Main thread set request ' + str(areq.requestid) + ' status to P.' ])
                                                    break # The finally clause will ensure the Worker lock is released.
                                            finally:
                                                Worker.lock.release()
                        
                                            # Worker.eventMaxThreads.wait() # Wakes up when a worker thread completes. Don't use.
                                            # Poll instead so that we can get other things done while we are waiting for a thread 
                                            # to become available.
                                            time.sleep(1)
                                            countDown -= 1
                        finally:
                            reqTable.releaseLock()

                        # Refresh the requests table. This could result in new requests being added to the table, or completed requests being
                        # deleted. 
                        try:
                            reqTable.acquireLock()
                            reqTable.refresh()
                        finally:
                            reqTable.releaseLock()
                
                        firstIter = False            
                        time.sleep(1)                
                        
        if thContainer[7] == RV_TERMINATED:
            pass
    except FileNotFoundError as exc:
        type, value, traceback = sys.exc_info()
        if msLog:
            msLog.writeError([ value ])
        else:
            print(value, file=sys.stderr)
    except Exception as exc:
        if len(exc.args) == 2:
            eType, eMsg = exc.args
            
            if eType == 'drmsParams':
                rv = RV_DRMSPARAMS
            elif eType == 'args':
                rv = RV_ARGS
            elif eType == 'clientConfig':
                rv = RV_CLIENTCONFIG
            elif eType == 'reqtableWrite':
                rv = RV_REQTABLEWRITE
            elif eType == 'reqtableRead':
                rv = RV_REQTABLEREAD
            elif eType == 'invalidArgument':
                rv = RV_INVALIDARGUMENT
            elif eType == 'unknownRequestid':
                rv = RV_UNKNOWNREQUESTID
            elif eType == 'getSubList':
                rv = RV_GETSUBLIST
            elif eType == 'dbResponse':
                rv = RV_DBRESPONSE
            elif eType == 'dbCmd':
                rv = RV_DBCOMMAND
            else:
                raise
            
            if msLog:
                msLog.writeError([ eMsg ])
            else:
                print(eMsg, file=sys.stderr)
        else:
            rv = RV_UKNOWNERROR
            import traceback
            if msLog:
                msLog.writeError([ traceback.format_exc(5) ])
                
    if msLog:
        if rv == RV_TERMINATED:
            msLog.writeInfo([ 'Normal shut-down complete.' ])
        msLog.close()
    logging.shutdown()

    sys.exit(rv)
