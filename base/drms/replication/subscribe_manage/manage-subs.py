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


import sys
import time
import contextlib

from subprocess import check_call, Popen

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

if sys.version_info < (3, 0):
    raise Exception("You must run the 3.0 release, or a more recent release, of Python.")

# Response codes
RESP_COMPLETE
RESP_PENDING
RESP_BUSY

LST_TABLE_SERIES = 'series'
LST_TABLE_NODE = 'node'

# 
def finalStuff(log, lockFile):
    log.write(['Termination signal handler called. Halting threads.'])
    
    # Stop threads
    for worker in Worker.tList:
        worker.stop() # Set a flag to halt any polling
        worker.join(60)
    
    # Clean up subscription lock
    try:
        os.remove(lockFile)
    except IOError:
        pass

def terminator(*args):
    # Raise the SystemExit exception.
    sys.exit(0)

# BTW, the contextmanager decorator is equivalent to the with context manager.
# To handle signal interruptions properly, you have to install a signal handler
# inside the __enter__() of the context manager (or before the try of the 
# contextmanage decorated function). Although handling SIGINT can be done without
# a signal handler, SIGTERM will cause the process to quit and bypass the __exit__()
# or contextmanager decorator finally clause.
@contextlib.contextmanager
def terminationHandler(log, lockFile):
    signal.signal(signal.SIGINT, terminator)
    signal.signal(signal.SIGTERM, terminator)
    signal.signal(signal.SIGHUP, terminator)

    try:
        yield
    except KeyboardInterrupt:
        pass # Ignore
    except SystemError:
        pass # Ignore
    finally:
        finalStuff(log, lockFile)

class ManageSubsParams(DRMSParams):

    def __init__(self):
        super(ManageSubsParams, self).__init__()

    def get(self, name):
        val = super(ManageSubsParams, self).get(name)

        if val is None:
            raise Exception('drmsParams', 'Unknown DRMS parameter: ' + name + '.')
        return val

class Arguments(object):

    def __init__(self, parser):
        # This could raise in a few places. Let the caller handle these exceptions.
        self.parser = parser
        
        # Parse the arguments.
        self.parse()
        
        # Set all args.
        self.setAllArgs()
        
    def parse(self):
        try:
            self.parsedArgs = self.parser.parse_args()      
        except Exception as exc:
            if len(exc.args) == 2:
                type, msg = exc
                  
                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise Exception('args', msg)
            else:
                raise # Re-raise

    def setArg(self, name, value):
        if not hasattr(self, name):
            # Since Arguments is a new-style class, it has a __dict__, so we can
            # set attributes directly in the Arguments instance.
            setattr(self, name, value)
        else:
            raise Exception('args', 'Attempt to set an argument that already exists: ' + name + '.')

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)
        
    def getArg(self, name):
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise Exception('args', 'Unknown argument: ' + name + '.')
            
    def addFileArgs(self, file):
        cfileDict = {}
        rv = getCfg(file, cfileDict)
        if rv != 0:
            raise Exception('clientConfig', 'Unable to open or parse client-side configuration file ' + file + '.')
        for key, val in cfileDict.items():
            self.setArg(key, val)

class Log(object):
    """Manage a logfile."""

    def __init__(self, file):
        self.fileName = file
        self.fobj = None
        
        try:
            head, tail = os.path.split(file)

            if not os.path.isdir(head):
                os.mkdir(head)
            fobj = open(self.fileName, 'a')
        except OSError as exc:
            type, value, traceback = sys.exc_info()
            raise Exception('badLogfile', 'Unable to access ' + "'" + value.filename + "'.")
        except IOError as exc:
            type, value, traceback = sys.exc_info()
            raise Exception('badLogfile', 'Unable to open ' + "'" + value.filename + "'.")
        
        self.fobj = fobj

    def __del__(self):
        if self.fobj:
            self.fobj.close()

    def write(self, text):
        try:
            lines = ['[' + datetime.now().strftime('%Y-%m-%d %T') + '] ' + line + '\n' for line in text]
            self.fobj.writelines(lines)
            self.fobj.flush()
        except IOError as exc:
            type, value, traceback = sys.exc_info()
            raise Exception('badLogwrite', 'Unable to write to ' + value.filename + '.')
            
class RedirectStdFileStreams(object):
    def __new__(cls, stdout=None, stderr=None):
        return super(RedirectStdFileStreams, cls).__new__(cls)
    
    def __init__(self, stdout=None, stderr=None):
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr

    def __enter__(self):
        self.stdoutOrig = sys.stdout
        self.stderrOrig = sys.stderr
        self.stdoutOrig.flush()
        self.stderrOrig.flush()
        sys.stdout = self.stdout
        sys.stderr = self.stderr


    def __exit__(self, exc_type, exc_value, traceback):
        self.stdout.flush()
        self.stderr.flush()
        # Restore original stdout and stderr.
        sys.stdout = self.stdoutOrig
        sys.stderr = self.stderrOrig
        
class WriteStream(object):
    def __init__(self):
        self.io = StringIO()
        
    def __del__(self):
        self.io.close()
        
    def getStream(self):
        return self.io
        
    def getLines(self):
        self.io.seek(0)
        lines = [line.rstrip() for line in self.io.readlines()]
        self.io.truncate(0)
        return lines
        
class Worker(threading.Thread):
    """ This kinda does what the old subscription Updater code did. """

    tList = [] # A list of running worker threads.
    maxThreads = 16 # Default. Can be overriden with the Downloader.setMaxThreads() method.
    eventMaxThreads = threading.Event() # Event fired when the number of threads decreases.
    lock = threading.Lock() # Guard tList.
    
    def __init__(self, request, newSite, arguments, cursor, log):
        self.request = request
        self.requestID = request.requestID
        self.newSite = newSite
        self.arguments = arguments
        self.cursor = cursor
        self.log = log
        self.sdEvent = threading.Event()
        
        self.reqTable = self.request.reqTable
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
        self.triggerDir = self.arguments.getArg('triggerdir')
        self.lstDir = self.arguments.getArg('tables_dir')
        self.parserConfig = self.arguments.getArg('parser_config')
        self.parserConfigTmp = self.parserConfig + '.tmp'
        self.subscribeLock = join(self.arguments.getArg('kServerLockDir'), 'subscribelock.txt')
        self.oldClientLogDir = os.path.join(self.siteLogDir, self.client)
        self.newClientLogDir = os.path.join(self.siteLogDir, self.client + '.new')
        self.oldLstPath = os.path.join(lstDir, self.client + '.lst')
        self.newLstPath = os.path.join(lstDir, self.client + '.new.lst')
        self.sqlgenArgFile = os.path.join(self.triggerDir, self.client + '-sqlgen.txt.tmp')
    
    def __run__(self):
        # There is at most one thread per client.
        try:
            try:
                # We don't want the main thread modifying this the request part-way through the following operations.
                self.reqTable.acquireLock()
                reqStatus = self.request.getStatus()[0]

                # Check for download error or completion
                if reqStatus != 'P':
                    raise Exception('invalidRequestState', 'Request ' + str(self.requestID) + ' not pending.')

                # Just in case, clean any old cruft.
                self.cleanTemp()

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
                if self.newSite:
                    subList = [ self.series.lower() ]
            
                    if self.action.lower() != 'subscribe':
                        raise Exception('invalidRequest', 'As a new client, ' + self.client + ' cannot make a ' + self.action.lower() + ' request.')
                else:
                    # Read the client's subscription list from su_production.slonylst. There is no need 
                    # for a lock since only one thread can modify the subscription list for a client.
                    subList = SubscriptionList(self.clientLstDbTable, self.cursor, self.client)
            
                    if self.action == 'subscribe':
                        # Append to subList.
                        subList.append(self.series.lower())
                    elif self.action == 'resubscribe':
                        # Don't do anything.
                        pass
                    elif self.action == 'unsubscribe':
                        # Remove from subList.
                        subList.remove(self.series.lower())
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
                    
                            # ...and make the temporary client.new directory in the site-logs directory.
                            os.mkdir(self.newClientLogDir) # umask WILL mask-out bits if it is not 0000; os.chmod() is better.
                            os.chmod(self.newClientLogDir, 2755)
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
                            if os.path.exists(self.subscribeLock):
                                os.remove(self.subcribeLock)

                    # Refresh this script's cache FROM DB.
                    subList.refreshSubscriptionList()

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
                        newSiteStr = 'true'
                    else:
                        newSiteStr = 'false'
                    
                    # Create sqlgenArgFile file. It must contains two columns: 1. the series (lower-case), 2. the exact string "subscribe". The 
                    # two columns must be white-space delimited.
                    with open(self.sqlgenArgFile, 'w') as fout;
                        print(self.series.lower() + '        subscribe', file=fout)
                
                    # The dump could take a while to run. We should start the process asynchronously, and poll for completion, while
                    # checking for a shutdown condition.
                    cmdList = [ os.path.join(repDir, 'subscribe_manage', 'sql_gen'), self.client, self.newSiteStr, str(self.archive), str(self.retention), str(self.tapegroup), self.sqlgenArgFile ]
                    proc = Popen(cmdList)
                
                    # Release reqTable lock to allow other client requests to be processed.
                    self.reqTable.releaseLock()
                
                    maxLoop = 3600 # 1-hour timeout
                    while True:
                        if maxLoop <= 0:
                            raise Exception('sqlDump', 'Time-out waiting for dump file to be generated.')
                
                        if self.sdEvent.isSet():
                            proc.kill()
                            raise Exception('shutDown', 'Received shut-down message from main thread.')
                
                        if proc.returncode is not None:
                            if proc.returncode != 0:
                                raise Exception('sqlDump', 'Failure generating dump file, sql_gen returned ' + str(proc.returncode) + '.') 
                            break

                        maxLoop -= 1
                        time.sleep(1)
                        
                    self.reqTable.acquireLock()
                
                    if os.path.exists(self.sqlgenArgFile):
                        os.remove(self.sqlgenArgFile)

                    # Tell client that the dump is ready for use. We do that by setting the request status to D.
                    self.request.setStatus('D')

                    # Release the lock again while the client downloads and applies the SQL file.
                    self.reqTable.releaseLock()

                    # Poll on the request status waiting for it to be I. This indicates that the client has successfully ingested
                    # the dump file. The client could also set the status to E if there was some error, in which case the client
                    # provides an error message that the server logs.
                    maxLoop = 86400 # 24-hour timeout
                    while True:
                        if maxLoop <= 0:
                            raise Exception('sqlAck', 'Time-out waiting for client to ingest dump file.')
                    
                        if self.sdEvent.isSet():
                            # Stop polling if main thread is shutting down.
                            raise Exception('shutDown', 'Received shut-down message from main thread.')
                    
                        try:
                            self.reqTable.acquireLock()
                            (code, msg) = self.request.getStatus()
                            if code.upper() == 'I':
                                # Onto clean-up
                                break
                            elif code.upper() == 'E':
                                raise Exception('dumpApplication', self.client + ' failed to properly ingest dump file.')
                            else:
                                raise Exception('invalidReqStatus', 'Unexpected request status ' + code.upper() + '.')
                        finally:
                            self.reqTable.releaseLock()
                    
                        maxLoop -= 1
                        time.sleep(1)
                
                # Clean-up, regardless of action.
                # reqTable lock is not held here.
                maxLoop = 30
                while True:
                    if self.sdEvent.isSet():
                        raise Exception('shutDown', 'Received shut-down message from main thread.')

                    try:
                        try:
                            cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                            check_call(cmd, shell=True)
                        except CalledProcessError as exc:
                            raise Exception('subLock')
                        
                        # Obtained global subscription lock.
                        
                        self.reqTable.acquireLock()
                    
                        # LEGACY - Remove client.new.lst from slon_parser.cfg
                        with open(self.parserConfigTmp, 'w') as fout and open(self.parserConfig, 'r') as fin:
                            regExp = re.compile(re.escape(self.client + '.new.lst'))

                            for line in fin:
                                matchObj = regExp.match(line)
                                if matchObj is None:
                                    print(line, file=fout)
                    
                        os.rename(self.parserConfigTmp, self.parserConfig)

                        # Remove client.new from su_production.slonylst and su_production.slonycfg. Do not call legacy code in
                        # SubTableMgr::Remove. This would delete $node.new.lst, but it is still being used.
                        # cmd="$kRepDir/subscribe_manage/gentables.pl op=remove config=$config_file --node=$node.new"
                        cmdList = [ os.path.join(repDir, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new' ]
                        # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                        check_call(cmdList)
                    
                        if self.newSite:
                            # LEGACY - Add a line for client to slon_parser.cfg.
                            with open(self.parserConfig, 'a') as fout:
                                print(self.oldClientLogDir + '        ' + self.oldLstPath, file=fout)
                            
                            # Add client to su_production.slonycfg. At the same time, insert records for client in
                            # su_production.slonylst. The client's sitedir doesn't exit yet, but it will shortly.
                            # Use client.new.lst to populate su_production.slonylst.
                            # $kRepDir/subscribe_manage/gentables.pl op=add config=$config_file --node=$node --sitedir=$subscribers_dir --lst=$tables_dir/$node.new.lst
                            cmdList = [ os.path.join(repDir, 'subscribe_manage', 'gentables.pl'), 'op=add', 'conf=' + self.slonyCfg, '--node=' + self.client, '--sitedir=' + self.siteLogDir, '--lst=' + self.newLstPath ]
                            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                            check_call(cmdList)

                            # Rename the client.new site dir.
                            if os.path.exists(self.oldClientLogDir):
                                shutil.rmtree(self.oldClientLogDir)
                        
                            # We are assuming that perms on newClientLogDir were created correctly and that we
                            # do not want to change them at this point.
                            os.rename(self.newClientLogDir, self.oldClientLogDir)
                        else:
                            # Update su_production.slonylst for the client with the new list of series. 
                            cmdList = [ os.path.join(repDir, 'subscribe_manage', 'gentables.pl'), 'op=replace', 'conf=' + self.slonyCfg, '--node=' + self.client, '--lst=' + self.newLstPath ]
                            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                            check_call(cmdList)
                        
                            # Copy all the log files in newClientLogDir to oldClientLogDir, overwriting logs of the same name.
                            # There is a period of time where we allow the log parser to run while the client is ingesting
                            # the dump file. During that time, two parallel universes exist - in one universe, the set of 
                            # logs produced is identical to the one that would have been produced had the client not 
                            # taken any subscription action. In the other universe, the set of logs produced is what you'd expect
                            # had the subscription action succeeded. So, if we have a successful subscription, then we need to
                            # discard the logs generated in the first universe, in the oldClientLogDir, overwriting them 
                            # with the analogous logs in the newClientLogDir.
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
                            # Could not obtain lock; try again (up to 30 tries)
                            if maxLoop <= 0:
                                raise Exception('subLock', 'Could not obtain subscription lock after 30 tries.')
                            time.sleep(1)
                            maxLoop -= 1
                        else:
                            # gentables.pl failed or creating client.new, re-raise the exception generated by check_call()
                            # or os.mkdir() or os.chmod().
                            raise
                    finally:
                        if os.path.exists(self.subscribeLock):
                            os.remove(self.subcribeLock)
                            
                # Outside of lock-acquisition loop.
                # reqTable lock is held.
                # The request has been completely processed without error. Set status to 'C'.
                self.request.setStatus('C')
                            
            finally:
                # Always release reqTable lock.
                self.reqTable.releaseLock()
                
            return
            
        except ValueError as exc:
            # Popen() and check_call() will raise this exception if arguments are bad.
            print('Command called with invalid arguments: ' + ' '.join(cmdList) + '.')
        except IOError as exc:
            print(exc.diag.message_primary)
        except OSError as exc:
            # Popen() and check_call() can raise this.
            print(exc.diag.message_primary)
        except CalledProcessError as exc:
            # check_call() can raise this.
            print('Command returned non-zero status code ' + str(exc.returncode) + ': '+ ' '.join(cmdList) + '.')
        finally:
            maxLoop = 15
            while True:
                try:
                    try:
                        cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                        check_call(cmd, shell=True)
                    except CalledProcessError as exc:
                        raise Exception('subLock')
                        
                    # Obtained global subscription lock.
                    
                    # There was some kind of error. We need to clean up several things.
                    self.cleanOnError()
                except Exception as exc:
                    if len(exc.args) == 1 and exc.args[0] == 'subLock':
                        # Could not obtain lock; try again (up to 30 tries)
                        if maxLoop <= 0:
                            raise Exception('subLock', 'Could not obtain subscription lock after 30 tries.')
                        time.sleep(1)
                        maxLoop -= 1
                    else:
                        # gentables.pl failed or creating client.new, re-raise the exception generated by check_call()
                        # or os.mkdir() or os.chmod().
                        raise
                finally:
                    if os.path.exists(self.subscribeLock):
                        os.remove(self.subcribeLock)

    def stop(self):
        self.sdEvent.set()
            
    def cleanTemp(self):
        try:
            # LEGACY - Remove the client.lst.new file entry from slon_parser.cfg.
            with open(self.parserConfigTmp, 'w') as fout and open(self.parserConfig, 'r') as fin:
                regExp = re.compile(re.escape(self.client + '.new.lst'))

                for line in fin:
                    matchObj = regExp.match(line)
                    if matchObj is None:
                        print(line, file=fout)

            os.rename(self.parserConfigTmp, self.parserConfig)

            # LEGACY - Delete the client.lst.new file.
            if os.path.exists(self.newLstPath):
                os.remove(self.newLstPath)
            
            # Remove client.new from su_production.slonylst and su_production.slonycfg. 
            cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new' ]
            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
            check_call(cmdList)
        
            # Remove client.new site-log directory.
            if os.path.exists(tempLogDir):
                self.log.write(['Removing temporary site-log directory ' + self.siteLogDir + '.'])
                shutil.rmtree(self.siteLogDir)
        
            # LEGACY - Remove slon_parser.cfg.tmp from tables dir.
            if os.path.exists(self.parserConfigTmp):
                os.remove(elf.parserConfigTmp)

            # Remove client-sqlgen.txt.tmp temporary argument file from triggers directory.
            if os.path.exists(self.sqlgenArgFile):
                os.remove(self.sqlgenArgFile)
        
            # LEGACY - Other crap from the initial implementation. The newer implementation does not
            # use files for passing information back and forth to/from the client.
        
            # Remove client's dump file. Remove untar'd version.
            if os.path.exists(os.path.join(self.triggerDir, self.client + '.sql')):
                os.remove(os.path.join(self.triggerDir, self.client + '.sql'))
            
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
        # Stuff to remove if an error happens anywhere during the subscription process.
        try:
            self.cleanTemp()

            # LEGACY - If this was a newsite subscription, then we need to remove the client entry
            # from slon_parser.cfg and remove client.lst from the table dir.
            if self.newSite:
                with open(self.parserConfigTmp, 'w') as fout and open(self.parserConfig, 'r') as fin:
                regExp = re.compile(re.escape(self.client + '.lst'))

                for line in fin:
                    matchObj = regExp.match(line)
                    if matchObj is None:
                        print(line, file=fout)

                os.rename(self.parserConfigTmp, self.parserConfig)
                
                if os.path.exists(self.lstPath):
                    os.remove(self.lstPath)
            
            # If this was a newsite subscription, then we need to remove the client entry
            # from su_production.slonycfg and remove the client's rows from su_production.slonylst.
            if self.newSite:            
                cmdList = [ os.path.join(self.repDir, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client ]
                # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                check_call(cmdList)
        except:
            pass

    @staticmethod
    def newThread(request, newSite, cursor, log):
        worker = Worker(request, newSite, cursor, log)
        worker.tList.append(worker)
        worker.start()
        return worker
        
class Request(object):
    def __init__(self, reqtable, dbtable, requestid, client, starttime, action, series, status, errmsg):
        self.reqtable = reqtable
        self.dbtable = dbtable
        self.requestid = requestid
        self.client = client
        self.starttime = starttime
        self.action = action
        self.series = series
        self.status = status
        self.errmsg = errmsg
        
    def setStatus(self, code, msg=None):
        # ART - Validate code
        self.status = code
        if msg is not None:
            self.errmsg = msg

        try:            
            # Write to the DB.
            # The requestid column is an integer.
            if self.errmsg:
                cmd = 'UPDATE ' + self.dbtable + " SET status='" + self.status + "', errmsg='" + self.errmsg + "' WHERE requestid=" + str(self.requestid)
            else:
                cmd = 'UPDATE ' + self.dbtable + " SET status='" + self.status + "' WHERE requestid=" + str(self.requestid)

            cursor.execute(cmd)
            cursor.commit()
        except psycopg2.Error as exc:
            raise Exception('reqtableWrite', exc.diag.message_primary)
            
    def getStatus(self):
        try:
            # The requestid column is an integer.
            cmd = 'SELECT status, errmsg FROM ' + self.dbtable + " WHERE requestid=" + str(requestid)

            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) != 1:
                raise Exception('reqtableRead', 'Unexpected number of database rows returned from query: ' + cmd + '.')

            self.status = records[0][0]    # text
            self.errmsg = records[0][1]    # text

            cursor.rollback()

            return (self.status, self.errmsg)

        except psycopg2.Error as exc:
            raise Exception('reqtableRead', exc.diag.message_primary)
        except ProgrammingError as exc:
            raise Exception('reqtableRead', exc.diag.message_primary)
            
    def setWorker(self, worker):
        if not isinstance(worker, Worker):
            raise Exception('invalidArgument', 'Type of argument to setWorker argument must be Worker; ' + type(worker) + ' was provided.')
        self.worker = worker

class ReqTable(object):    
    def __init__(self, tableName, timeOut, cursor, log):
        self.tableName = tableName
        self.timeOut = timeOut
        self.cursor = cursor
        self.log = log
        self.lock = thread.allocate_lock()
        self.reqDict = {}
    
    def read(self):
        # requests(client, requestid, starttime, action, series, status, errmsg)
        cmd = 'SELECT client, requestid, starttime, action, series, archive, retention, tapegroup, status, errmsg FROM ' + self.tableName
        
        try:
            self.cursor.execute(cmd)
        
        except psycopg2.Error as exc:
            raise Exception('reqtableRead', exc.diag.message_primary, cmd)
        
        for record in self.cursor:
            requestidStr = str(record[0])

            client = record[0]    # text
            requestid = record[1] # integer            
            # Whoa! For starttime, pyscopg returns timestamps as datetime.datetime objects already!
            starttime = record[2] # datetime.datetime
            action = record[3]    # text            
            series = [ aseries for aseries in record[4].split(',') ] # text (originally)
            archive = record[5]   # int
            retention = record[6] # int
            tapegroup = record[7] #int
            status = record[8]    # text
            errmsg = record[9]    # text
            
            self.reqDict[requestidStr] = Request(self, requestid, client, starttime, action, series, status, errmsg)
            
        self.cursor.rollback()

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
        # Delete existing items from self.
        self.reqDict.clear()
        
        # Read the table from the database anew.
        self.tryRead()
        
    def acquireLock(self):
        self.lock.acquire()
        self.locked = True
    
    def releaseLock(self):
        if self.locked:
            self.lock.release()
            self.locked = False
        
    def setStatus(self, requestids, code, msg=None):
        try:
            for arequestid in requestids:
                requestidStr = str(arequestid)
        
                if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                    raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')

                if msg:
                    self.reqDict[requestidStr].setStatus(code, msg)
                    self.reqDict[requestidStr]['errmsg'] = msg
                else:
                    self.reqDict[requestidStr].setStatus(code)
            
        except psycopg2.Error as exc:
            raise Exception('reqtableWrite', exc.diag.message_primary)
            
    def getStatus(self, requestids):
        try:
            statuses = []
            for arequestid in requestids:
                requestidStr = str(arequestid)
                
                if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                    raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')
                
                # A 2-tuple: code, msg
                status = self.reqDict[requestidStr].getStatus()[0]
                
                statuses.append(status)
        except psycopg2.Error as exc:
            raise Exception('reqtableRead', exc.diag.message_primary)
        except ProgrammingError as exc:
            raise Exception('reqtableRead', exc.diag.message_primary)
            
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
    
        if client:
            # Return results for client only.
            for requestidStr in self.reqDict.iterkeys():
                if self.reqDict[requestidStr].status == 'P' and self.reqDict[requestidStr].client == client:
                    pendLst.append(self.reqDict[requestidStr])
        else:
            for requestidStr in self.reqDict.iterkeys():
                if self.reqDict[requestidStr].status == 'P':
                    pendLst.append(self.reqDict[requestidStr])
    
            # Sort by start time. Sorts in place - and returns None.
            pendLst.sort(key=lambda(req): req['starttime'].strftime('%Y-%m-%d %T'))

        return pendLst
    
    def getNew(self, client=None):
        newLst = []
        
        if client:
            # Return results for client only.
            for requestidStr in self.reqDict.iterkeys():
                if self.reqDict[requestidStr].status == 'N' and self.reqDict[requestidStr].client == client:
                    newLst.append(self.reqDict[requestidStr])            
        else:
            for requestidStr in self.reqDict.iterkeys():
                if self.reqDict[requestidStr].status == 'N':
                    newLst.append(self.reqDict[requestidStr])

        # Sort by start time. Sorts in place - and returns None.
        newLst.sort(key=lambda(req): req['starttime'].strftime('%Y-%m-%d %T'))

        return newLst

    def getProcessing(self, client=None):
        procLst = []
    
        if client:
            # Return results for client only.
            for requestidStr in self.reqDict.iterkeys():
                if (self.reqDict[requestidStr].status == 'P' or self.reqDict[requestidStr].status == 'D' or self.reqDict[requestidStr].status == 'I') and self.reqDict[requestidStr].client == client:
                    procLst.append(self.reqDict[requestidStr])
        else:
            for requestidStr in self.reqDict.iterkeys():
                if self.reqDict[requestidStr].status == 'P' or self.reqDict[requestidStr].status == 'D' or self.reqDict[requestidStr].status == 'I':
                    procLst.append(self.reqDict[requestidStr])
    
            # Sort by start time. Sorts in place - and returns None.
            procLst.sort(key=lambda(req): req['starttime'].strftime('%Y-%m-%d %T'))

        return procLst

    def getTimeout(self):
        return self.timeOut


class SubscriptionList(object):
    def __init__(self, sublistDbTable, cursor, client):
        self.sublistDbTable = sublistDbTable
        self.cursor = cursor
        self.client = client
        self.list = []
        self.getSubscriptionList()

    def getSubscriptionList(self):
        if len(self.list) == 0:
            cmd = 'SELECT series FROM ' + self.sublistDbTable + " WHERE node = '" + self.client + "' ORDER BY series"

            try:
                self.cursor.execute(cmd)
            except psycopg2.Error as exc:
                raise Exception('getSubList', exc.diag.message_primary)

            records = self.cursor.fetchall()

            self.list = [ rec[0] for rec in records ]
            
            self.cursor.rollback()

        return self.list

    def refreshSubscriptionList(self):
        if len(self.list) > 0:
            del self.list[:]
        self.getSubscriptionList()

# Main Program
if __name__ == "__main__":
    rv = RV_SUCCESS
    
    manageSubsParams = ManageSubsParams()

    parser = CmdlParser(usage='%(prog)s [ slonycfg=<configuration file> ]')

    arguments = Arguments(parser)
    arguments.setArg('slonyCfg', manageSubsParams.get('SLONY_CONFIG'))
    arguments.setArg('dbname', manageSubsParams.get('DBNAME'))
    arguments.setArg('dbuser', manageSubsParams.get('WEB_DBUSER'))
    arguments.setArg('dbhost', manageSubsParams.get('SERVER'))
    arguments.setArg('dbport', int(manageSubsParams.get('DRMSPGPORT')))
    
    # Add the server-side subscription arguments to arguments.
    arguments.addFileArgs(arguments.getArg('slonyCfg'))
    
    pid = os.getpid()
    with DrmsLock(join(arguments.getArg('kServerLockDir'), 'manage-subs-lock.txt'), str(pid)) as lock:
        log = Log(join(arguments.getArg('kSMlogDir'), 'manage-subs-log.txt'))
        writeStream = WriteStream()

        # Redirect all the output from subprocesses to writeStream.
        with RedirectStdFileStreams(stdout=writeStream.getStream(), stderr=writeStream.getStream()) as stdStreams:
            subscribeLock = join(arguments.getArg('kServerLockDir'), arguments.getArg('sublock'))
            with terminationHandler(log, subscribeLock):
                with psycopg2.connect(database=arguments.getArg('dbname'), user=arguments.getArg('dbuser'), host=arguments.getArg('dbhost'), port=str(arguments.getArg(dbport))) as conn:
                    log.write([ 'Connected to database ' + arguments.getArg('dbname') + ' on ' + arguments.getArg('dbhost') + ':' + str(arguments.getArg(dbport)) + ' as user ' + arguments.getArg('dbuser') ])
                    with conn.cursor() as cursor:
                        # Read the requests table into memory.
                        reqTable = ReqTable(arguments.getArg('kReqTable'), timedelta(minutes=arguments.reqtimeout), cursor, log)

                        # Main dispatch loop. When a SIGINT (ctrl-c), SIGTERM, or SIGHUP is received, the 
                        # terminator context manager will be exited.
                        while True:
                            # Deal with pending requests first. If a request has been pending too long, then delete the request now
                            # and send an error message back to the requestor. This will clean-up requests that died somewhere
                            # while being processed.
                            try:
                                reqTable.acquireLock()
                                reqsPending = reqTable.getPending()
                                for areq in reqsPending:
                                    timeNow = datetime.now(areq['starttime'].tzinfo)
                                    if timeNow > areq['starttime'] + reqTable.getTimeout():
                                        log.write(['The processing of request ' + str(areq['requestid']) + ' timed-out.'])
                        
                                        # Kill the Worker thread (if it exists). The Worker will clean-up subscription-management files, 
                                        # like client.new.lst.
                                        areq.setStatus('E', 'Processing timed-out.')
                                        areq.worker.stop()

                                # Completed requests, success and failures alike, are handled by request-subs.py. There is nothing to do here.

                            finally:
                                reqTable.releaseLock()

                            # If the requestor is making a subscription request, but there is already a request
                            # pending for that requestor, then send an error message back to the requestor 
                            # that will cause the client code to exit without modifying any client state. This is 
                            # actually already handled in request-subs.py, but handle it here too (don't send a
                            # message to the client - manage-subs.py is not communicating directly to the client)
                            # by killing the request (that should not have been allowed to get to the manager
                            # in the first place).
                            reqTable.acquireLock()
                            reqsNew = reqTable.getNew()
                            for areq in reqsNew:
                                if reqTable.getProcessing([ areq.client ]):                        
                                    # Set the status of this pending request to an error code.
                                    areq.setStatus('E', 'Server busy processing request for client ' + areq.client + ' already.')
                                else:

                                    # At this point, the wrapper CGI, request-subs.py, has ensured that all requests in 
                                    # the request table are valid. 
                    
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
                                                log.write(['Instantiating a worker thread for request ' + str(areq.requestid) + 'for client ' + areq.client + '.'])
                                    
                                                areq.setWorker(Worker.newThread(areq, binPath, cursor, log))
                                                requests.setStatus([areq], 'P') # The new request is now being processed. If there was an issue creating the new thread, an exception will have been raised, and we will not execute this line.
                                                break # The finally clause will ensure the Worker lock is released.
                                        finally:
                                            Worker.lock.release()
                                
                                        # Worker.eventMaxThreads.wait() # Wakes up when a worker thread completes. Don't use.
                                        # Poll instead so that we can get other things done while we are waiting for a thread 
                                        # to become available.
                                        time.sleep(1)
                                        countDown -= 1
                        
                            # Refresh the requests table. This could result in new requests being added to the table, or completed requests being
                            # deleted. 
                            reqTable.refresh()
