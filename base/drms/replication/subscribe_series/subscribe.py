#!/usr/bin/env python

# subscribe.py is the subscription-service client. With subscribe.py, the client can subscribe to data series 
# published at the providing site (the server). To do this, the client sends HTTP requests to the server via the
# subscription-service CGI, whose URL is identified by the kSubService parameter in repclient.cfg. There are three
# different services provided by the subscription server: series subscription, series subscription cancellation, 
# and series re-subscription. The workflow for each is as follows:
#
# subscription:
#   The client sends a 'subscribe' request to the server. The server inserts a record into a requests database table, 
#   and returns to the client a 'requestQueued' response, along with a request ID number. The subscription manager 
#   daemon then processes the subscription request.
#
#   Subscription proceeds asynchronously, creating one or more 'dump files' (sql files that contain statements
#   to set-up the subscription on the client) that the client must ingest. The client must poll the server
#   so that the client knows when the dump file(s) are ready for ingestion. The client polls by repeatedly
#   making a 'polldump' request to the server, sending the request ID of the original subscription request.
#   The server then responds to this polldump request with either a 'requestQueued' or 'requestProcessing' or 
#   a 'dumpReady' status. The first status means that the server has not yet started processing the subscription
#   request. If it has started processing the request, but has not yet finished creating the first dump file, 
#   then the status returned by the server is requestProcessing. In both cases, the client must continue to poll
#   awaiting the third response status - dumpReady.
#
#   When the client receives the dumpReady status, it then downloads and ingests the prepared dump file(s) sitting on the
#   server. Depending on the current DB configuration, there may be one or two dump files to download at ingest at this time.
#   The client does this, and then notifies the server when it has completely ingested these files. The client does this
#   by sending a 'pollcomplete' to the server. At this point, the server may then either create an additional dump file
#   (which happens if the total payload to be downloaded to the client is large - the payload is broken-up into multiple
#   additional dump files) or it may start finalizing the subscription if the entire payload has been transfered to the client.
#   In either case, the client must poll the server with additional pollcomplete requests. If there are no more dump files
#   to be ingested by the client, and the server is finalizing the subscription request, the server responds with a 
#   'requestFinalizing' status. As soon as the server has finished finalizing the request, it will send a 'requestComplete'
#   status in response to the client's pollcomplete request. However, if an additional dump file is forthcoming, then 
#   the server will continue to send the requestFinalizing status to the client. When the next dump file is ready at 
#   the server, it will send a dumpReady response to the client. And then the client ingests this dump file and 
#   sends a pollcomplete request to the server when it has completed ingesting this next dump file. The interaction continues
#   as described above until the last dump file has been ingested. After that happens, the server sends the 'requestComplete'
#   response to the client in response to the client's pollcomplete request. At this point, the client is successfully
#   subscribed to the series.
#   
# subscription cancellation:
#   The client sends an 'unsubscribe' request to the server. The server inserts a record into a requests database table, 
#   and returns to the client a 'requestQueued' response, along with a request ID number. The subscription manager 
#   daemon then processes the subscription-cancellation request.
#
#   subscription cancellation proceeds asynchronously at the server. The server services this request by 
#   removing the series from the list of subscribed-to series maintained for the client. The client polls the server 
#   for completion with a pollcomplete request. The server then responds to this pollcomplete request with either a 
#   requestQueued or a requestProcessing or a requestComplete status. As with the subscription request, until the client 
#   receives a requestComplete response, it continues to poll with a pollcomplete request. After the client receives this
#   requestComplete response, the client is no longer subscribed to the series, but it must clean-up on the client side
#   (it must fetch all pending Slony logs, ingest them all, and then delete the series). The client cleans-up independently 
#   of the server - there is no need to communicate clean-up completion status to the server.
#
# re-subscription:
#   Due to the complexities and potential for race conditions to occur on the server, the safest way to implement this
#   this feature is for the server to first process a subscription-cancellation request, followed by a 
#   subscription request for the same series. However, a modification to each of these processes is required.
#   First, during the subscription-cancellation phase, after the server has stopped the subscription and during the
#   client clean-up, the client does not delete the DRMS data series. Instead, it deletes the rows from the data-
#   series database table. Second, during the subscription-phase, the server does include in any dump file any SQL 
#   that would otherwise cause any data series database objects to be created or modified.
#
#   The client, therefore, first sends an unsubscribe request to the server, with the workflow proceeding as described 
#   in the subscription section above. When the client receives the requestComplete status in response to the
#   pollcomplete request, it fetches and ingest all pending Slony logs. However, as mentioned above, it does not
#   delete the series. Instead it deletes all rows from the data series database table. To do this, the client
#   makes an unsubscribe request as described in the subscription cancellation section above.
#
#   After the rows have been deleted, the client then issues a 'resubscribe' request to the server. This request
#   results in a near-identical set of procedures to be performed on the server, except that the SQL that would normally create
#   the DRMS data series at the client are omitted. To do this, the client makes a resubscribe request instead of a subscribe
#   request, but it follows exactly the same steps it followed for the subscribe request as described above.


import sys

if sys.version_info < (3, 2):
    raise Exception("You must run the 3.2 release, or a more recent release, of Python.")

import os
import re
from datetime import datetime, timedelta
from io import StringIO
from pySmartDL import SmartDL
import time
import tarfile
import argparse
import threading
import logging
import signal
import urllib
import json
from subprocess import Popen, CalledProcessError, PIPE, check_call
import fcntl
import psycopg2
import gzip
import tty
import termios
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsLock import DrmsLock
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

RV_SUCCESS = 0
RV_DRMS = 1
RV_DBCONNECTION = 2
RV_DRMSPARAMS = 3
RV_ARGS = 4
RV_CLIENTCONFIG = 5
RV_LOCK = 6
RV_DBCMD = 7
RV_SUBSERVICE = 8
RV_GETLOGS = 9
RV_DELSERIES = 10
RV_INGESTSQL = 11

STATUS_REQUEST_RESUMING = 'requestResuming'
STATUS_REQUEST_QUEUED = 'requestQueued'
STATUS_REQUEST_PROCESSING = 'requestProcessing'
STATUS_REQUEST_DUMP_READY = 'dumpReady'
STATUS_REQUEST_FINALIZING = 'requestFinalizing'
STATUS_REQUEST_COMPLETE = 'requestComplete'
STATUS_ERR_TERMINATED = 'terminated'
STATUS_ERR_INTERNAL = 'internalError'
STATUS_ERR_INVALID_ARG = 'invalidArgument'
STATUS_ERR_INVALID_REQUEST = 'invalidRequest'


LOCKFILE = 'clientsublock.txt'


def terminator(*args):
    # Raise the SystemExit exception (which will be caught by the __exit__() method below.
    sys.exit(0)

class TerminationHandler(DrmsLock):
    def __new__(cls, lockFile, dieFile, strPid, log):
        return super(TerminationHandler, cls).__new__(cls, lockFile, strPid)

    def __init__(self, lockFile, dieFile, strPid, log):
        self.lockFile = lockFile
        self.dieFile = dieFile
        self.log = log
        log.writeInfo([ 'Lock file is ' + lockFile + '.', 'Die file is ' + dieFile + '.' ])
        super(TerminationHandler, self).__init__(lockFile, strPid)
        
    def __enter__(self):
        signal.signal(signal.SIGINT, terminator)
        signal.signal(signal.SIGTERM, terminator)
        signal.signal(signal.SIGHUP, terminator)

    # Normally, __exit__ is called if an exception occurs inside the with block. And since SIGINT is converted
    # into a KeyboardInterrupt exception, it will be handled by __exit__(). However, SIGTERM will not - 
    # __exit__() will be bypassed if a SIGTERM signal is received. Use the signal handler installed in the
    # __enter__() call to handle SIGTERM.
    def __exit__(self, etype, value, traceback):
        if etype == SystemExit:
            self.log.writeInfo(['Termination signal handler called.'])
        
        self.hastaLaVistaBaby()
        
        # Remove subscription lock file by calling parent __exit__().
        super(TerminationHandler, self).__exit__(etype, value, traceback)

    def hastaLaVistaBaby(self):        
        # Do not do this! The call of Log::__del__ is deferred until after the program exits. If you end your program by
        # calling sys.exit(), then sys.exit() gets called BEFORE Log::__del__ is called, and this causes a race condition.
        # del self.log
        # self.log.close()
        # On second thought, let's not close the log either. Just flush for now, and close() at the end of the program run.
        self.log.flush()
        if os.path.exists(self.dieFile):
            os.remove(self.dieFile)
    
class SubscribeParams(DRMSParams):
    def __init__(self):
        super(SubscribeParams, self).__init__()

    def get(self, name):
        val = super(SubscribeParams, self).get(name)

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
            raise Exception('args', 'Attempt to set an argument that already exists: ' + name + ':' + str(value) + '.')
            
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
            
    def addFileArgs(self, file):
        cfileDict = {}
        rv = getCfg(file, cfileDict)
        if rv != 0:
            raise Exception('argFile', 'Unable to open or parse client-side argument file ' + file + '.')
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
        for line in text:
            self.log.debug(line)
            
    def writeInfo(self, text):
        for line in text:
            self.log.info(line)
    
    def writeWarning(self, text):
        for line in text:
            self.log.warning(line)
    
    def writeError(self, text):
        for line in text:
            self.log.error(line)
            
    def writeCritical(self, text):
        for line in text:
            self.log.critical(line)
        
class CfgAction(argparse.Action):
    def __init__(self, option_strings, dest, arguments, *args, **kwargs):
        self.arguments = arguments
        super(CfgAction, self).__init__(option_strings, dest, *args, **kwargs)
        
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        # Put all arguments inside the Slony configuration file into self.arguments
        self.arguments.addFileArgs(values)
        
class ListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))
        
class OverrideAction(argparse.Action):
    '''
    This class sets the arguments directly in the Arguments object passed in
    the arguments parameter. When the argument parser is called, the arguments
    will get set in the arguments::parsedArgs property, which then get copied
    as properties into arguments, the Arguments object. If the user were to not
    provide a value for an argument on the command line, then the parser would
    set the argument's value to None (the default) in arguments::parsedArgs, 
    which would then overwrite the value set in arguments, the Arguments
    object, by the CfgAction class. To allow a default to be set by the
    CfgAction and not have it overwritten by the default value of None set by the
    parser when the user does not supply a value on the command line, suppress the
    setting of a default value in the add_argument() call for the argument.
    '''
    def __init__(self, option_strings, dest, arguments, *args, **kwargs):
        self.arguments = arguments
        super(OverrideAction, self).__init__(option_strings, dest, *args, **kwargs)
        
    def __call__(self, parser, namespace, values, option_string=None):
        self.arguments.set(self.dest, values)

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

class SqlCopy(threading.Thread):
    def __init__(self, readPipe, cursor, dbtable, columns, log):
        self.readPipe = readPipe
        self.cursor = cursor
        self.dbtable = dbtable
        self.columns = columns
        self.log = log
        self.sdEvent = threading.Event()
        
    # Executes in a child thread.
    def run(self):
        if not self.sdEvent.isSet():
            try:
                with os.fdopen(self.readPipe, encoding='LATIN1') as readFd:
                    self.cursor.copy_from(readFd, self.dbtable, columns=self.columns)
            except Exception as exc:
                import traceback
                if self.log:
                    self.log.write(['There was a problem copying data into database table ' + self.dbtable + '.'])
                    self.log.write([traceback.format_exc(0)])

    # Executes in the main thread.
    def stop(self):
        self.sdEvent.set()
        self.join(30)

def dbSchemaExists(conn, schema):
    cmd = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = '" + schema + "'"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        conn.rollback()
        
    if len(records) == 1:
        return True
    else:
        return False

def dbTableExists(conn, schema, table):
    cmd = "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '" + schema + "' AND c.relname = '" + table + "'"

    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        conn.rollback()
        
    if len(records) == 1:
        return True
    else:
        return False
        
def dbFunctionExists(conn, schema, function):
    cmd = "SELECT n.nspname, c.proname FROM pg_proc c JOIN pg_namespace n ON n.oid = c.pronamespace WHERE n.nspname = '" + schema + "' AND c.proname = '" + function + "'"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        conn.rollback()
        
    if len(records) == 1:
        return True
    else:
        return False

def seriesExists(conn, series):
    # Well, this is a complicated question. A remote site can subscribe to a series only if it is on the external website (i.e., jsoc.stanford.edu).
    # However, certain programs that provide lists of series put whitelisted series in these list too. However, remote sites cannot
    # subscribe to white-listed series. The best way to get the list of subscribe-able series is to query the DB directly, looking at 
    # the <ns>.drms_series table.
    regExp = re.compile(r'\s*(\S+)\.(\S+)\s*')
    matchObj = regExp.match(series)
    if matchObj is not None:
        ns = matchObj.group(1)
        table = matchObj.group(2)
    else:
        raise Exception('invalidArgument', 'Not a valid DRMS series name: ' + series + '.')
        
    if dbSchemaExists(conn, ns):
        cmd = 'SELECT seriesname FROM ' + ns + ".drms_series WHERE lower(seriesname) = '" + series.lower() + "'"
 
        try:
            with conn.cursor() as cursor:
                cursor.execute(cmd)
                records = cursor.fetchall()
                if len(records) > 1:
                    raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')
        except psycopg2.Error as exc:
            raise Exception('dbCmd', exc.diag.message_primary)
        finally:
            conn.rollback()
    
        if len(records) == 1:
            return True
        else:
            return False
    else:
        return False
        
def truncateDBTable(conn, table):
    cmd = 'TRUNCATE TABLE ONLY ' + table
    commit = False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
        commit = True
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        if commit:
            conn.commit()
        else:
            conn.rollback()
        
def seriesIsPublished(serviceURL, series):
    cgiArgs = { 'qseries' : series }
    arguments = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
    ans = False
    
    with urllib.request.urlopen(serviceURL + '?' + arguments) as response:
        # siteInfoStr is a string, that happens to be json.
        info = json.loads(response.read().decode('UTF-8'))
        ans = info[series]
        
    return ans
    
def clientIsSubscribed(client, serviceURL, series):
    cgiArgs = { 'series' : series }
    arguments = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
    ans = False
    
    with urllib.request.urlopen(serviceURL + '?' + arguments) as response:
        # siteInfoStr is a string, that happens to be json.
        info = json.loads(response.read().decode('UTF-8'))
        if client in info['nodelist'][series]: # A list of all clients subscribed to this series.
            ans = True
            
    return ans

def ingestSQLFile(sqlIn, psqlBin, dbhost, dbport, dbname, dbuser, log):
    # The SQL file has a UTF8 encoding, but the DRMS databse has a Latin-1 encoding. However,
    # the SQL file has a SET CLIENT_ENCODING TO 'UTF8'; statement, yay! So, we can run psql, 
    # which starts the underlying transaction with the server encoding (Latin-1), but when the 
    # sql file is ingested, the client encoding will be changed to UTF8, and then when the text data 
    # are sent to the server, they are converted to Latin-1 automagically.
    
    # sqlIn is actually a io.TextIOWrapper, a file object which I believe is incompatible with
    # Popen(). Filter this through a stdin PIPE to Popen().
    
    # Make a pipe for capturing psql stderr as well.
    pipeReadEndFDErr, pipeWriteEndFDErr = os.pipe()

    # Make these pipes not block on read (since we are going to be reading from the pipe here).
    flag = fcntl.fcntl(pipeReadEndFDErr, fcntl.F_GETFL)
    fcntl.fcntl(pipeReadEndFDErr, fcntl.F_SETFL, flag | os.O_NONBLOCK)

    pipeReadEndErr = os.fdopen(pipeReadEndFDErr, 'r', encoding='UTF8')
    pipeWriteEndErr = os.fdopen(pipeWriteEndFDErr, 'w', encoding='UTF8')
    
    try:
        cmdList = [ psqlBin, '-h', dbhost, '-p', dbport, '-d', dbname, '-vON_ERROR_STOP=1', '-U', dbuser, '-f', '-']
        proc = Popen(cmdList, stdin=PIPE, stderr=pipeWriteEndErr, stdout=PIPE)
    except OSError as exc:
        raise Exception('sqlDump', "Cannot run command '" + ' '.join(cmdList) + "' ")
    except ValueError as exc:
        raise Exception('sqlDump', "psql command '" + ' '.join(cmdList) + "' called with invalid arguments.")            
    
    
    # It turns out the psql will NOT close any of the pipes or files opened in this script. So, before we write
    # to any pipe in a loop, we need to check if psql has died, and if so, bail out of the loop.
    doneWriting = False
    while True:
        # It appears that psql can exit before all data are written to the write-end of the pipe. And when
        # psql exits, it does NOT close the read-end of the pipe, nor does it close the write-end of the pipe.
        # It does not close any of the pipes or fds opened in this script.
        if not doneWriting:
            sqlInBytes = sqlIn.read(8192) # Returns str.
            log.writeDebug([ 'Read ' + str(len(sqlInBytes)) + ' bytes from dump file.'])
            if len(sqlInBytes) > 0:
                try:
                    # If there is a problem writing to the proc's stdin (like psql has died), this will raise.
                    # The only exception to handle is BrokenPipeError. proc.stdin is a blocking pipe.
                    # proc.stdin.write() needs a bytes object, not a str.
                    bytesWritten = proc.stdin.write(bytes(sqlInBytes, 'UTF8'))
                    proc.stdin.flush() 
                    log.writeDebug([ 'Wrote ' + str(bytesWritten) + ' bytes to psql pipe.' ])                    
                except BrokenPipeError as exc:
                    # psql terminated. Time to error out. Let the proc.poll() block of code handle this.
                    log.writeError([ 'The psql child process terminated - cannot send data to psql.' ])
                    doneWriting = True
            else:
                log.writeDebug([ 'Done sending data to psql.' ])
                doneWriting = True
        else:
            proc.stdin.close()
                
        # Log any stderr messages from psql. Don't worry about stdout - psql will print an insert line
        # for every line ingested, and those don't provide any useful information
        if not pipeReadEndErr.closed:
            while True:
                pipeBytes = pipeReadEndErr.read(4096)
                if len(pipeBytes) > 0:
                    log.writeDebug([ 'Read ' + str(len(pipeBytes)) + ' bytes from psql stderr.' ])
                    log.writeInfo([ pipeBytes ])
                else:
                    break

        proc.poll()
        if proc.returncode is not None:
            log.writeDebug([ 'psql finished.' ])
            if not pipeWriteEndErr.closed:
                pipeWriteEndErr.flush()

            # When the write end of this pipe is closed, the read end closes too.            
            if not pipeReadEndErr.closed:
                while True:
                    pipeBytes = pipeReadEndErr.read(4096)
                    if len(pipeBytes) > 0:
                        log.writeInfo([ pipeBytes.decode('UTF8') ])
                    else:
                        break
            
            # These can be called, even if the pipe is already closed.
            pipeWriteEndErr.close()
            pipeReadEndErr.close()
        
            if proc.returncode != 0:
                # Log error.
                msg = 'Command "' + ' '.join(cmdList) + '" returned non-zero status code ' + str(proc.returncode) + '.'
                log.writeError([ msg ])
                raise Exception('ingestSQL', 'psql failed to ingest SQL file.')
            log.writeInfo([ 'psql terminated - ingestion of SQL file complete.' ])
            break
        # Do not sleep! We want to pipe the dump-file data bytes to psql as quickly as possible. We don't want
        # to sleep 1 second every 8192 bytes.

def extractSchema(series):
    regExp = re.compile(r'\s*(\S+)\.(\S+)\s*')
    matchObj = regExp.match(series)
    if matchObj is not None:
        schema = matchObj.group(1)
        table = matchObj.group(2)
    else:
        raise Exception('invalidArgument', 'Not a valid DRMS series name: ' + series + '.')
        
    return schema

# Main Program
if __name__ == "__main__":
    rv = RV_SUCCESS
    log = None
    client = None
    reqId = None
    serviceURL = None
    
    try:
        subscribeParams = SubscribeParams()
        if subscribeParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')

        arguments = Arguments()

        parser = CmdlParser(usage='%(prog)s [ -hjpl ] cfg=<client configuration file> reqtype=<subscribe, resubscribe, unsubscribe> series=<comma-separated list of series> [ --archive=<0, 1>] [ --retention=<number of days>] [ --tapegroup=<tape-group number> ] [ --logfile=<log-file name> ]')

        parser.add_argument('cfg', '--config', help='The client-side configuration file used by the subscription service.', metavar='<client configuration file>', dest='slonyCfg', action=CfgAction, arguments=arguments, required=True)
        parser.add_argument('reqtype', '--reqtype', help='The type of request (subscribe, resubscribe, or unsubscribe).', metavar='<request type>', dest='reqtype', required=True)
        parser.add_argument('series', '--series', help='A comma-separated list of DRMS series to subscribe/resubscribe to, or to unsubscribe from.', dest='series', action=ListAction, required=True)
        parser.add_argument('archive', '--archive', help='The tape archive flag for the series - either 0 (do not archive) or 1 (archive).', metavar='<series archive flag>', dest='archive', type=int, action=OverrideAction, arguments=arguments, default=argparse.SUPPRESS)
        parser.add_argument('retention', '--retention', help='The number of days the series SUs remain on disk before becoming subject to deletion.', metavar='<series SU disk retention>', dest='retention', type=int, action=OverrideAction, arguments=arguments, default=argparse.SUPPRESS)
        parser.add_argument('tapegroup', '--tapegroup', help='If the archive flag is 1, the number identifying the group of series that share tape files.', metavar='<series SU tape group>', dest='tapegroup', type=int, action=OverrideAction, arguments=arguments, default=argparse.SUPPRESS)
        parser.add_argument('-p', '--pause', help='Pause and ask for user confirmation before applying the downloaded SQL dump file.', dest='pause', action='store_true', default=False)
        parser.add_argument('-t', '--loglevel', help='Specifies the amount of logging to perform. In increasing order: critical, error, warning, info, debug', dest='loglevel', action=LogLevelAction, default=logging.ERROR)
        parser.add_argument('-l', '--logfile', help='The file to which logging is written.', metavar='<file name>', dest='logfile', default=os.path.join('.', 'subscribe_' + datetime.now().strftime('%Y%m%d') + '.log'))
        
        arguments.setParser(parser)

        # Create/Initialize the log file.
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        log = Log(arguments.getArg('logfile'), arguments.getArg('loglevel'), formatter)
        
        log.writeCritical(['Logging threshold level is ' + log.getLevel() + '.']) # Critical - always write the log level to the log.

        arguments.dump(log)
        
        client = arguments.getArg('node')
        lockFile = os.path.join(arguments.getArg('kLocalLogDir'), LOCKFILE)
        dieFile = os.path.join(arguments.getArg('ingestion_path'), 'get_slony_logs.' + client + '.die')
        strPid = str(os.getpid())
        
        log.writeCritical([ 'Client is ' + client + '.' ])

        # In addition to handling termination signals, TerminationHandler also prevents multiple, simultaneous runs of this script.
        with TerminationHandler(lockFile, dieFile, strPid, log) as lock:
            cluster = arguments.getArg('slony_cluster')
        
            # Connect to database.
            try:
                with psycopg2.connect(database=arguments.getArg('pg_dbname'), user=arguments.getArg('pg_user'), host=arguments.getArg('pg_host'), port=str(arguments.getArg('pg_port'))) as conn:
                    log.writeInfo([ 'Connected to database ' +  arguments.getArg('pg_dbname') + ' on ' + arguments.getArg('pg_host') + ':' + str(arguments.getArg('pg_port')) + ' as user ' + arguments.getArg('pg_user') + '.' ])
                    if dbSchemaExists(conn, '_' + cluster):
                        # Check existing installation to make sure there aren't any existing issues.
                        # If the _jsoc schema exists, then the following should also exist:
                        #   _jsoc.sl_sequence_offline (relation)
                        #   _jsoc.sl_archive_tracking (relation)
                        #   _jsoc.sequenceSetValue_offline (function)
                        #   _jsoc.finishTableAfterCopy (function)
                        #   _jsoc.archiveTracking_offline (function)
                        if not dbTableExists(conn, '_' + cluster, 'sl_sequence_offline'):
                            raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: _' + cluster + '.sl_sequence_offline.')
                        if not dbTableExists(conn, '_' + cluster, 'sl_archive_tracking'):
                            raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: _' + cluster + '.sl_archive_tracking.')
                        if not dbFunctionExists(conn, '_' + cluster, 'sequencesetvalue_offline'):
                            raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database function: _' + cluster + '.sequencesetvalue_offline.')
                        if not dbFunctionExists(conn, '_' + cluster, 'finishtableaftercopy'):
                            raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database function: _' + cluster + '.finishtableaftercopy.')
                        if not dbFunctionExists(conn, '_' + cluster, 'archivetracking_offline'):
                            raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database function: _' + cluster + '.archivetracking_offline.')
    
                        newSite = False
                        log.writeInfo([ 'Client ' + client + ' is NOT a first-time subscriber.' ])
                    else:
                        newSite = True
                        log.writeInfo([ 'Client ' + client + ' is a first-time subscriber.' ])
                        
                    # Create the 'die file', if it does not already exist, which prevents get_slony_logs.pl from running.
                    with open(dieFile, 'a') as fout:
                        pass
    
                    serviceURL = arguments.getArg('kSubService')
                    pubServiceURL = arguments.getArg('kPubListService')
                    
                    # Check for pending request. Fetch any existing request from the server for this client. Then fill-in the 
                    # various arguments to this script with the values from that request. The client can get into this state if
                    # they kill this script before it completes. If they re-run this script, we want their existing request to 
                    # complete. To be really safe about this, I could save a UUID on both the server and client, and make sure they
                    # are equal before proceeding, but maybe some other time.
                    cgiArgs = { 'action' : 'continue', 'client' : client }
                    
                    msg = 'Checking for existing request at server.'
                    print(msg)
                    log.writeInfo([ msg ])
                    log.writeInfo([ 'Calling cgi with URL: ' + serviceURL ])
                    log.writeInfo([ 'cgi args: ' + json.dumps(cgiArgs) ])
                    urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
                
                    with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                        info = json.loads(response.read().decode('UTF-8'))

                    if 'status' not in info or 'msg' not in info:
                        raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')
                        
                    if info['status'] == STATUS_REQUEST_RESUMING:
                        # The request is 'pending'. Set the arguments to the proper values.
                        reqId = info['reqid']
                        reqType = info['reqtype']
                        seriesList = info['series']
                        archive = info['archive']
                        retention = info['retention']
                        tapeGroup = info['tapegroup']

                        resuming = True
                        resumeAction = info['resumeaction']
                        resumeStatus = info['resumestatus'].upper()
                        log.writeInfo([ 'Found existing request at server: reqid=' + str(reqId) + ', reqtype=' + reqType + ', series=' + ','.join(seriesList) + ', archive=' + str(archive) + ', retention=' + str(retention) + ', tapegroup=' + str(tapeGroup) ])
                    else:
                        reqType = arguments.getArg('reqtype').lower()
                        archive = arguments.getArg('archive')
                        retention = arguments.getArg('retention')
                        tapeGroup = arguments.getArg('tapegroup')
                        seriesList = arguments.getArg('series')

                        resuming = False
                        log.writeInfo([ 'No existing request at server.' ])

                    series = None 
                    schema = None
                    
                    if reqType != 'subscribe' and reqType != 'unsubscribe' and reqType != 'resubscribe':
                        raise Exception('invalidArgument', 'Unknown subscription request type: ' + reqType + '.')

                    if reqType == 'unsubscribe' or reqType == 'resubscribe':
                        if reqType == 'unsubscribe':
                            # Make sure that newSite is False.
                            if newSite:
                                raise Exception('invalidArgument', 'Cannot cancel subscription to series. Client ' + client + ' has never subscribed to any series')
                                
                            # Make sure there is at least one series and make sure that the series exist locally and 
                            # make sure that the client is currently subscribed to these series.
                            if len(seriesList) < 1:
                                raise Exception('invalidArgument', 'Please provide one or more series whose subscriptions are to be cancelled.')
                                
                            for series in seriesList:
                                if not seriesExists(conn, series):
                                    raise Exception('invalidArgument', series + ' does not exist locally. Please select a different series whose subscription is to be cancelled.')
    
                                if not clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('invalidArgument', 'You are not subscribed to ' + series + '. Please select a different series whose subscription is to be cancelled.')
                                    
                            print('Cancelling subscription to series ' + ','.join(seriesList) + '.')
                        else:
                            # Make sure that newSite is False.
                            if newSite:
                                raise Exception('invalidArgument', 'Cannot re-subscribe to series. Client ' + client + ' has never subscribed to any series')
                                
                            # Make sure there is exactly one series and make sure that the series exist locally and 
                            # make sure that the client is currently subscribed to it.
                            if len(seriesList) != 1:
                                raise Exception('invalidArgument', 'Please provide one series whose subscription is to be reset.')

                            series = seriesList[0]

                            if not seriesExists(conn, series):
                                raise Exception('invalidArgument', series + ' does not exist locally. Please select a different series whose subscription is to be reset.')

                            if not clientIsSubscribed(client, pubServiceURL, series):
                                raise Exception('invalidArgument', 'You are not subscribed to ' + series + '. Please select a different series whose subscription is to be reset.')

                            print('Resetting subscription to series ' + series + '.')
    
                        # To un-subscribe from one or more series, provide client, series.
                        cgiArgs = { 'action' : 'unsubscribe', 'client' : client, 'newsite' : False, 'series' : ','.join(seriesList) }

                        # make the request to the subscription service
                        log.writeInfo([ 'Calling cgi with URL: ' + serviceURL ])
                        log.writeInfo([ 'cgi args: ' + json.dumps(cgiArgs) ])
                        urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
            
                        with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                            info = json.loads(response.read().decode('UTF-8'))

                        # Should receive a STATUS_REQUEST_QUEUED response.
                        if 'status' not in info or 'msg' not in info:
                            raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')

                        if info['status'] != STATUS_REQUEST_QUEUED:
                            raise Exception('subService', 'Request failure: ' + info['msg'] + ' (status ' + info['status'] + ').')

                        if 'reqid' not in info:
                            raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')

                        reqId = info['reqid']
                        log.writeInfo([ 'cgi response: ' + info['msg'] + ' Response status: ' + info['status'] + '.'])
                        time.sleep(1)
                        
                        # poll for completion
                        cgiArgs = { 'action' : 'pollcomplete', 'client' : client, 'reqid' : reqId}
                        urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).

                        natt = 0
                        while info['status'] == STATUS_REQUEST_QUEUED or info['status'] == STATUS_REQUEST_PROCESSING:
                            # Send pollcomplete requests. These tells the server to clean up and finalize, so 
                            # the request is in the STATUS_REQUEST_PROCESSING state.                        
                            try:
                                with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                    info = json.loads(response.read().decode('UTF-8'))
                                    natt = 0
                                    time.sleep(2)
                            except urllib.error.URLError as exc:
                                # we want to try again, until we time-out
                                natt += 1
                                if natt > 10:
                                    raise
                                if type(exc.response) is str:
                                    msg = exc.response
                                else:
                                    msg = ''
                                log.writeWarning([ 'Unable to send subscription server pollcomplete status (' + msg + '). Trying again.' ])
                                time.sleep(1)
                                
                        if info['status'] != STATUS_REQUEST_COMPLETE:
                            raise Exception('subService', info['status'], 'Unexpected response from subscription service: ' + info['status'])
                            
                        # Remove get_slony_logs die file.
                        if os.path.exists(dieFile):
                            os.remove(dieFile)
                            log.writeInfo([ 'Deleted die file ' + dieFile + '.' ])
                        
                        # Run get_slony_logs.pl
                        cmdList = [ arguments.getArg('kRSPerl'), arguments.getArg('kSQLIngestionProgram'), arguments.getArg('slonyCfg') ]
                        log.writeInfo([ 'Running ' + ' '.join(cmdList) + '.' ])
                        print('Getting latest Slony logs.')

                        try:
                            check_call(cmdList)
                        except CalledProcessError as exc:
                            raise Exception('getLogs', 'Unable to run ' + arguments.getArg('kSQLIngestionProgram') + ' properly.')
                            
                        log.writeInfo([ 'Successfully downloaded and ingested Slony log files (' + ' '.join(cmdList) + ' ran successfully).' ])

                        if reqType == 'unsubscribe':
                            # Delete series. The user can skip deleting this series - however, after this script completes, the series
                            # will no longer be under replication. Before the user can re-subscribe to the series, the user would
                            # have to delete the series, or move it.
                            print('Before re-subscribing to these series, you will need to either delete them, or move them out of the way.')
                            deleteSeriesBin = arguments.getArg('kDeleteSeriesProgram')
                            stdinFD = sys.stdin.fileno()
                            oldAttr = termios.tcgetattr(stdinFD)
                            for series in seriesList:
                                try:
                                    tty.setraw(sys.stdin.fileno())
                                    print('Would you like to delete ' + series + ' now (y/n)?\r')
                                    ans = sys.stdin.read(1)
                                    if ans.lower() != 'y':
                                        print('  ...skipping ' + series + '\r')
                                        continue
                                    else:
                                        print('  ...deleting ' + series + '\r')

                                    print('Would you like to keep the SUs (and not delete them) for ' + series + ' (y/n)?\r')
                                    ans = sys.stdin.read(1)
                                finally:
                                    termios.tcsetattr(stdinFD, termios.TCSADRAIN, oldAttr)
                            
                                if ans.lower() != 'y':
                                    cmdList = [ deleteSeriesBin, series, 'JSOC_DBUSER=slony']
                                    print('  ...deleting SUs of series ' + series + '.')
                                else:
                                    cmdList = [ deleteSeriesBin, '-k', series, 'JSOC_DBUSER=slony']
                                    print('  ...NOT deleting SUs of series ' + series + '.')
                                
                                log.writeInfo([ 'Running ' + ' '.join(cmdList) + '.' ])
                            
                                # Pipe stdout and stderr, but do not print or log their content (which is akin to 
                                # You are about to permanently erase all metadata for the series 'mdi.fdv_avg120').
                                proc = Popen(cmdList, stdin=PIPE, stdout=PIPE, stderr=PIPE)
                                proc.communicate(input=b'yes\nyes\n')
                                if proc.returncode != 0:
                                    raise Exception('delSeries', 'Failure to delete series ' + series + '.')
                                msg = 'Successfully deleted series ' + series + '.'
                                print(msg)
                                log.writeInfo([ msg ])
                            
                            for series in seriesList:
                                msg = 'Successfully cancelled subscription to ' + series + '.'
                                print(msg)
                                log.writeInfo([ msg ])
                        else:
                            # resubscribe
                            # delete rows in series database table
                            truncateDBTable(conn, series.lower())
                            
                            msg = 'Successfully cancelled subscription to ' + series + ' (first part of a re-subscription request).'
                            log.writeInfo([ msg ])

                    if reqType == 'subscribe' or reqType == 'resubscribe':
                        if resuming:
                            # Need to re-initialize some variables (series, schema).
                            series = seriesList[0]
                            schema = extractSchema(series)
                        else:
                            # Make sure that there is only a single series in the series list and make sure that
                            # the series is one that does not exist locally and make sure that the client is not
                            # already subscribed to the series and make sure that the series published.
                            if len(seriesList) > 1:
                                raise Exception('invalidArgument', 'Please ' + reqType + ' to a single series at a time.')
                            elif len(seriesList) == 0:
                                raise Exception('invalidArgument', 'Please provide a series to which you would like to ' + reqType + '.')
        
                            series = seriesList[0]
                            schema = extractSchema(series)
    
                            if reqType == 'subscribe':
                                if seriesExists(conn, series):
                                    raise Exception('invalidArgument', series + ' already exists locally. Please select a series that does not exist.')
                                
                                if clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('invalidArgument', 'You are already subscribed to ' + series + '. Please select a series to which you are not subscribed.')
                                
                                if not seriesIsPublished(pubServiceURL, series):
                                    raise Exception('invalidArgument', 'Series ' + series + ' is not published and not available for subscription.')
                                
                                if newSite:
                                    # Make sure the admin.ns table exists, because we are going to insert into it. And if it is missing,
                                    # then the NetDRMS is bad.
                                    if not dbTableExists(conn, 'admin', 'ns'):
                                        raise Exception('drms', 'Your DRMS is missing a required database relation (or the containing schema): admin.ns.')
                                        
                                # to subscribe to any series, the client DRMS has to have the 'drms' namespace (this is a required component
                                # created by the NetDRMS.sql script), and it has to have the capturesunum() function in the drms namespace,
                                # and it has to have the drms.ingested_sunums database table
                                if not dbSchemaExists(conn, 'drms'):
                                    raise Exception('drms', 'Your DRMS is missing a required namespace: drms.')                                    
                            else:
                                # resubscribe
                                if not seriesExists(conn, series):
                                    raise Exception('invalidArgument', series + ' does not exist locally. Please select a an existing series.')
                                
                                if clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('subService', 'Failure cancelling subscription to ' + series + '.')

                                if not seriesIsPublished(pubServiceURL, series):
                                    raise Exception('invalidArgument', 'Series ' + series + ' is not published and not available for resetting.')

                                if newSite:
                                    raise Exception('invalidArgument', 'Cannot reset subscription to series. Client ' + client + ' has never subscribed to any series')
                                
                            if dbSchemaExists(conn, schema):
                                # We are going to insert into several database tables in this schema. Make sure they exist.
                                if not dbTableExists(conn, schema, 'drms_series'):
                                    raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: ' + schema + '.drms_series')
                                if not dbTableExists(conn, schema, 'drms_keyword'):
                                    raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: ' + schema + '.drms_keyword')
                                if not dbTableExists(conn, schema, 'drms_link'):
                                    raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: ' + schema + '.drms_link')
                                if not dbTableExists(conn, schema, 'drms_segment'):
                                    raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: ' + schema + '.drms_segment')
                                if not dbTableExists(conn, schema, 'drms_session'):
                                    raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: ' + schema + '.drms_session')
                                if not dbTableExists(conn, schema, 'drms_sessionid_seq'):
                                    raise Exception('drms', 'Invalid DRMS subscription set-up. Missing database table: ' + schema + '.drms_sessionid_seq')

                            if reqType == 'subscribe':
                                # To subscribe to a series, provide client, series, archive, retention, tapegroup, newSite.
                                cgiArgs = { 'action' : 'subscribe', 'client' : client, 'newsite' : newSite, 'series' : series, 'archive' : archive, 'retention' : retention, 'tapegroup' : tapeGroup }
                                print('Subscribing to series ' + series + '.')
                            else:
                                # To re-subscribe to a series, provide client, series.
                                cgiArgs = { 'action' : 'resubscribe', 'client' : client, 'newsite' : False, 'series' : series, 'archive' : archive, 'retention' : retention, 'tapegroup' : tapeGroup }
                                print('Resetting series ' + series + '.')

                            # make the request to the subscription service
                            log.writeInfo([ 'Calling cgi with URL: ' + serviceURL ])
                            log.writeInfo([ 'cgi args: ' + json.dumps(cgiArgs) ])
                            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
            
                            with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                info = json.loads(response.read().decode('UTF-8'))

                            # Should receive a STATUS_REQUEST_QUEUED response.
                            if 'status' not in info or 'msg' not in info:
                                raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')
                
                            if info['status'] != STATUS_REQUEST_QUEUED:
                                raise Exception('subService', 'Request failure: ' + info['msg'] + ' (status ' + info['status'] + ').')
    
                            if 'reqid' not in info:
                                raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')
                        
                            reqId = info['reqid']
                    
                            log.writeInfo([ 'cgi response: ' + info['msg'] + ' Response status: ' + info['status'] + '.'])
    
                            time.sleep(1)

                        # poll server for dump files
                        if not resuming or resumeAction.lower() == 'polldump':                            
                            # poll for dump file.
                            log.writeInfo([ 'Polling for dump file.' ])
                            cgiArgs = { 'action' : 'polldump', 'client' : client, 'reqid' : reqId}
                            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
                            print('Waiting for server to create dump file.')
                            time.sleep(2)

                            natt = 0
                            while (resuming and info['status'] == STATUS_REQUEST_RESUMING) or info['status'] == STATUS_REQUEST_QUEUED or info['status'] == STATUS_REQUEST_PROCESSING:
                                log.writeInfo([ 'Calling cgi with URL: ' + serviceURL ])
                                log.writeInfo([ 'cgi args: ' + json.dumps(cgiArgs) ])
                                try:
                                    with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                        info = json.loads(response.read().decode('UTF-8'))
                                        if 'status' not in info or 'msg' not in info:
                                            raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')
                                        log.writeInfo([ 'cgi response: ' + info['msg'] + ' Response status: ' + info['status'] + '.'])
                                        print('.', end='')
                                        sys.stdout.flush()
                                        natt = 0
                                        time.sleep(2)
                                except urllib.error.URLError as exc:
                                    # we want to try again, until we time-out
                                    natt += 1
                                    if natt > 10:
                                        raise
                                    if type(exc.reason) is str:
                                        msg = exc.reason
                                    else:
                                        msg = ''
                                    log.writeWarning([ 'Unable to send subscription server pollcomplete status (' + msg + '). Trying again.' ])
                                    time.sleep(1)
                                    
                            # Print a newline.
                            print('dump file is ready.')

                            if info['status'] != STATUS_REQUEST_DUMP_READY:
                                raise Exception('subService', 'Unexpected response from subscription service: ' + info['status'] + '.')

                        if not resuming or resumeStatus == 'A':                            
                            # Download and apply create-ns file.
                            if reqType == 'subscribe' and not dbSchemaExists(conn, schema):
                                scheme, netloc, path, query, frag = urllib.parse.urlsplit(arguments.getArg('kSubXfer'))
                                xferURL = urllib.parse.urlunsplit((scheme, netloc, os.path.join(path, client + '.' + schema + '.createns.sql.gz'), None, None))
                                dest = os.path.join(arguments.getArg('kLocalWorkingDir'), client + '.' + schema + '.createns.sql.gz')

                                # SmartDL downloads the file via multiple streams into temporary files. It combines the multiple
                                # files into a single destination file when the download completes. So, if the destination file
                                # exists, the download completed and was successful.
                                if not resuming or not os.path.exists(dest):
                                    if os.path.exists(dest):
                                        log.writeInfo([ 'Removing stale dump ' + dest + '.' ])
                                        os.remove(dest)
                                    log.writeInfo([ 'Downloading ' + dest + '.' ])
                                    print('Downloading dump file (to ' + dest + ').')
                                    dl = SmartDL(xferURL, dest)
                                    dl.start()
                                    log.writeInfo([ 'Successfully downloaded ' + dest + '.' ])
                                    print('  ...download complete.')
                                    
                                with gzip.open(dest, mode='rt', encoding='UTF8') as fin:
                                    # Ingest createns.sql. Will raise if a problem occurs. When that happens, the cursor is rolled back.
                                    # The sql in fin is piped to a psql process.
                                    print('Ingesting dump file (' + dest + ').')
                                    ingestSQLFile(fin, arguments.getArg('PSQL').strip(" '" + '"'), arguments.getArg('pg_host'), str(arguments.getArg('pg_port')), arguments.getArg('pg_dbname'), arguments.getArg('pg_user'), log)
                                    print('  ...ingestion complete.')
                                    
                                # Remove the create-ns file.
                                os.remove(dest)
                                log.writeInfo([ 'Successfully ingested createNs file: ' + dest + '.' ])

                            # download and apply the sequence of dump files (if reqType == 'resubscribe', there will be no
                            # series database object creation SQL and no newsite SQL)
                            scheme, netloc, path, query, frag = urllib.parse.urlsplit(arguments.getArg('kSubXfer'))
                            xferURL = urllib.parse.urlunsplit((scheme, netloc, os.path.join(path, client + '.subscribe_series.sql.gz'), None, None))
                            dest = os.path.join(arguments.getArg('kLocalWorkingDir'), client + '.subscribe_series.sql.gz')

                            # start of dump-file loop
                            fileNo = 0
                            while True:
                                # SmartDL downloads the file via multiple streams into temporary files. It combines the multiple
                                # files into a single destination file when the download completes. So, if the destination file
                                # exists, the download completed and was successful.
                                if not resuming or not os.path.exists(dest):
                                    if os.path.exists(dest):
                                        log.writeInfo([ 'Removing stale dump ' + dest + '.' ])
                                        os.remove(dest)
                                    log.writeInfo([ 'Downloading ' + dest + '.' ])
                                    print('Downloading dump file (to ' + dest + ').')
                                    dl = SmartDL(xferURL, dest)
                                    dl.start()
                                    log.writeInfo([ 'Successfully downloaded ' + dest + '.' ])
                                    print('  ...download complete.')

                                with gzip.open(dest, mode='rt', encoding='UTF8') as fin:
                                    # Will raise if a problem occurs. When that happens, the cursor is rolled back.

                                    # If reqType == 'subscribe', then the sql will create a new series and populate it. 
                                    # If reqType == 'resubscribe', then the sql will truncate the 'series table' and reset the series-table sequence
                                    # only.
                                    print('Ingesting dump file (' + dest + ').')
                                    ingestSQLFile(fin, arguments.getArg('PSQL').strip(" '" + '"'), arguments.getArg('pg_host'), str(arguments.getArg('pg_port')), arguments.getArg('pg_dbname'), arguments.getArg('pg_user'), log)
                                    log.writeInfo([ 'Successfully ingested dump file: ' + dest + ' (number ' +  str(fileNo) + ')' ])
                                    print('  ...ingestion complete.')
                                    
                                # Remove the dump file.
                                os.remove(dest)

                                # We want to issue the pollcomplete request regardless of our resuming status (if we are resuming
                                # we already issued the polldump request above, if we needed to).

                                # Send a pollcomplete request to the subscription service. This will set status to 'I' to tell the server to
                                # clean up and finalize the request. After submitting this request, the Slony logs
                                # we receive could have insert statements for newly subscribed-to series.
                                cgiArgs = { 'action' : 'pollcomplete', 'client' : client, 'reqid' : reqId}
                                urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
                                print('Waiting for server to finalize request.')

                                # We just sent the first pollcomplete request. This tells the server to clean up and finalize, so 
                                # the request is in the STATUS_REQUEST_FINALIZING state.
                                info = {}
                                info['status'] = STATUS_REQUEST_FINALIZING

                                natt = 0
                                while info['status'] == STATUS_REQUEST_FINALIZING:
                                    # due to normal network activity, urlopen() could fail; I have
                                    # seen 'connection timed-out' errors.
                                    try:
                                        with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                            info = json.loads(response.read().decode('UTF-8'))
                                            print('.', end='')
                                            sys.stdout.flush()
                                            natt = 0
                                            time.sleep(1)
                                    except urllib.error.URLError as exc:
                                        # we want to try again, until we time-out
                                        natt += 1
                                        if natt > 10:
                                            raise
                                        if type(exc.response) is str:
                                            msg = exc.response
                                        else:
                                            msg = ''
                                        log.writeWarning([ 'Unable to send subscription server pollcomplete status (' + msg + '). Trying again.' ])
                                        time.sleep(1)

                                # Print a newline.
                                print('server has finalized request.')

                                if info['status'] != STATUS_REQUEST_COMPLETE:
                                    # The only other acceptable status is STATUS_REQUEST_DUMP_READY. This happens when there is
                                    # more than one dump file to ingest. The server will see a request status of 'I', and then 
                                    # it will set status to either 'D' (if there is another dump file to ingest) or it
                                    # will set status to 'C' (if there are no more dump files.)
                                    if info['status'] != STATUS_REQUEST_DUMP_READY:
                                        # Must undo (If newsite, remove _jsoc. If new schema, remove schema and remove schema's entry from admin.ns. If
                                        # subscribing, remove database table.)
                                        raise Exception('subService', info['status'], 'Unexpected response from subscription service: ' + info['status'])
                                    # back to the beginning of the dump-file loop
                                    fileNo += 1
                                else:
                                    # Yay, we are done ingesting dump files. Break out of dump-file loop.
                                    break
                                
                        if reqType == 'subscribe':
                            msg = 'Successfully subscribed to series ' + series + '.'
                        else:
                            msg = 'Successfully re-subscribed to series ' + series + '.'
                            
                        print(msg)
                        log.writeInfo([ msg ])
            except (psycopg2.DatabaseError, psycopg2.OperationalError) as exc:
                # Closes the cursor and connection
                if hasattr(exc, 'diag') and hasattr(exc.diag, 'message_primary') and exc.diag.message_primary:
                    msg = exc.diag.message_primary
                else:
                    import traceback
                
                    msg = traceback.format_exc(0)
                raise Exception('dbConnection', msg)

        # We are leaving the termination handler.
    except Exception as exc:
        if len(exc.args) == 2:
            type = exc.args[0]
            msg = exc.args[1]
            
            if type == 'drms':
                rv = RV_DRMS
            elif type == 'dbConnection':
                rv = RV_DBCONNECTION
            elif type == 'drmsParams':
                rv = RV_DRMSPARAMS
            elif type == 'invalidArgument' or type == 'args':
                rv = RV_ARGS
            elif type == 'argsFile':
                rv = RV_CLIENTCONFIG
            elif type == 'dbCmd':
                    rv = RV_DBCMD
            elif type == 'subService':
                rv = RV_SUBSERVICE
            elif type == 'drmsLock':
                rv = RV_LOCK
            elif type == 'getLogs':
                rv = RV_GETLOGS
            elif type == 'delSeries':
                rv = RV_DELSERIES
            elif type == 'ingestSQL':
                rv = RV_INGESTSQL

            if log:
                log.writeError([ msg ])

            print(msg)
        else:
            import traceback
            if log:
                log.writeError([ traceback.format_exc(5) ])

            print(traceback.format_exc(5))
                
        # Send a request to tell the server that an error occurred. The server will rollback in response.
        if client and reqId and serviceURL:
            cgiArgs = { 'action' : 'error', 'client' : client, 'reqid' : reqId}
            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).
            urllib.request.urlopen(serviceURL + '?' + urlArgs)
            
    log.close()
    logging.shutdown()

    sys.exit(rv)
