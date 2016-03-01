#!/usr/bin/env python

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
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsLock import DrmsLock
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

RV_SUCCESS = 0
RV_DBCONNECTION = 1
RV_DRMSPARAMS = 2
RV_ARGS = 3
RV_CLIENTCONFIG = 4
RV_LOCK = 5
RV_DBCMD = 6
RV_SUBSERVICE = 7

STATUS_REQUEST_RESUMING = 'requestResuming'
STATUS_REQUEST_QUEUED = 'requestQueued'
STATUS_REQUEST_PROCESSING = 'requestProcessing'
STATUS_DUMP_READY = 'dumpReady'
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

    def hastaLaVistaBaby(self):        
        # Do not do this! The call of Log::__del__ is deferred until after the program exits. If you end your program by
        # calling sys.exit(), then sys.exit() gets called BEFORE Log::__del__ is called, and this causes a race condition.
        # del self.log
        # self.log.close()
        # On second thought, let's not close the log either. Just flush for now, and close() at the end of the program run.
        self.log.flush()        
    
        # Remove die file (if present).
        if os.path.exists(self.dieFile):
            os.remove(self.dieFile)
    
        # Remove subscription lock file.
        os.remove(self.lockFile)

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
                with os.fdopen(self.readPipe) as readFd:
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
        
def dbFunctionExists(conn, schema, function):
    cmd = "SELECT n.nspname, c.proname FROM pg_proc c JOIN pg_namespace n ON n.oid = c.pronamespace WHERE n.nspname = '" + schema + "' AND c.proname = '" + function + "'"
    
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
    else:
        return False
        
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

def ingestCreateNSFile(client, schema, cursor, log):
    # We are going to have to rely on a specific structure to the SQL file. Therefore, this code will not work with an arbitrary SQL file.
    # The SQL file has multiple commands, each terminated by a newline followed by a semicolon. However, there could be 
    # line-breaks within commands. So, we have to read lines until a semicolon is encountered, combine those lines into a 
    # single SQL command, then execute that command.
    regExpEmpty = re.compile(r'\s+$')
    regExpComment = re.compile(r'--')
    regExpEndCommand = re.compile(r'[;]\s*$')
    command = ''

    with open(client + '.' + schema + '.' + 'createns.sql') as sqlIn:
        for line in sqlIn:
            if regExpEmpty.match(line) or regExpComment.match(line):
                continue
            command = command + line.rstrip()
            if regExpEndCommand.search(line):
                # End of command, execute it.
                try:
                    cursor.execute(command)
                except psycopg2.Error as exc:
                    raise Exception('sqlDump', exc.diag.message_primary)
                    
                command = ''

def ingestDumpFile(client, series, cursor, log):
    # We are going to have to rely on a specific structure to the SQL file. Therefore, this code will not work with an arbitrary SQL file.
    # The SQL file has two types of commands: non-COPY commands, and COPY commands. The non-COPY commands all occupy a single line in the file.
    # The COPY commands all start on one line (COPY table ... FROM stdin;). Each following line is a row of tab-delimited data. Following the last
    # line of data, there is a line with two characters - "\.". The COPY commands must follow the non-COPY commands, since the latter 
    # make database relations used by the former.
    # 
    # Do a line-buffered read of the SQL file. For each line read that contains a non-COPY command (except BEGIN - skip that), call cursor.execute(). 
    # As soon as the first COPY-command line is encountered, commit the cursor. Parse the first line of the COPY command to extract the table and column names.
    # These will be used as arguments to the cursor.copy_from() function. Make a pipe and spawn a thread to handle the cursor.copy_from()
    # function call. Use the read-end of the pipe as the input-file argument to the cursor.copy_from() call. The main thread continues to
    # read from the open SQL file, writing each data line to the write-end of the pipe. When the main thread encounters the "\." EOF delimiter, 
    # it closes the write-end of the pipe and joins the spawned thread. The main thread processes the remainder of the SQL file in this manner,
    # spawning a thread to handle each COPY command. When then main thread encounters the COMMIT command in the SQL file, it drops that command, 
    # and it closes the SQL file handle.
    regExpEmpty = re.compile(r'\s+$')
    regExpComment = re.compile(r'--')
    regExpBegin = re.compile(r'\s*begin\s*$', re.IGNORECASE)
    regExpCommit = re.compile(r'\s*commit\s*$', re.IGNORECASE)
    regExpCopy = re.compile(r'\s*copy$', re.IGNORECASE)
    regExpEOF = re.compile(r'\\\.')
    regExpExtractFromCopy = re.compile(r'\s*COPY\s+(\S+)\s*\(([^)]+)\)')
    firstLine = True
    copying = False
    committed = False

    writeFd = None
    writePipe = None
    readPipe = None
    copier = None
    
    try:
        with open(client + '.subscribe_series.sql') as sqlIn:
            for line in sqlIn:
                if committed:
                    raise Exception('sqlDump', 'Unexpected lines after COMMIT statement.')
        
                if regExpEmpty.match(line) or regExpComment.match(line):
                    continue
                
                if regExpBegin.match(line):
                    if firstLine:
                        firstLine = False
                    else:
                        raise Exception('sqlDump', 'BEGIN statement unexpected in SQL dump file.')

                    # Drop the BEGIN command.                                    
                    continue
                    
                if regExpCommit.match(line):
                    if firstLine:
                        raise Exception('sqlDump', 'COMMIT statement unexpected in SQL dump file.')
                    else:
                        # Done with SQL file.
                        if copying:
                            raise Exception('sqlDump', 'Unexpected format of SQL dump file. COMMIT encountered while copying data to ' + series + '.')
                        
                        committed = True
                        
                    # Drop the COMMIT command.
                    continue
                    
                if regExpCopy.match(line):
                    # The previous command could have been the last non-COPY command.
                    cursor.commit()
                
                    # Make sure any previous COPY command completed.
                    if writeFd or copier or writePipe or readPipe:
                        raise Exception('sqlDump', 'Missing EOF terminator for COPY command.')
                
                    # Extract the series and the column list from the copy command. The column names must be enclosed in
                    # quotes when they are provided in the columns argument, but they are already in quotes in the 
                    # dump file.
                    if not regExpExtractFromCopy.match(line):
                        raise Exception('sqlDump', 'Unexpected format of COPY-command line: ' + line + '.')
                    
                    dumpSeries = regExpExtractFromCopy.group(1)
                    if dumpSeries.lower() != series.lower():
                        raise Exception('sqlDump', 'Series in dump file (' + dumpSeries + ') does not match series in subscription request (' + series + ').')

                    columns = regExpExtractFromCopy.group(2).lower()
                    columnList = columns.split(',')
                
                    readPipe, writePipe = os.pipe()
                    copier = SqlCopy(readPipe, cursor, series.lower(), columnList, log)
                    copier.start()
                    writeFd = fdopen(writePipe, 'w')
                    copying = True
                
                    # Drop the COPY command.
                    continue
                if copying:
                    if regExpEOF.match(line):
                        writeFd.close() # Will also close the read fd and cause the copy thread to terminate.
                        writeFd = None
                        copier.join(60)
                        if copier.is_alive():
                            raise Exception('threadTO', 'Unable to terminate copier thread.')
                        copier = None
                        os.close(writePipe)
                        writePipe = None
                        os.close(readPipe)
                        readPipe = None
                        copying = False
                        cursor.commit()
                    else:
                        print(line, file=writeFd)
                    continue
                    
                if firstLine:
                    raise Exception('sqlDump', 'Unexpected format of SQL dump file. The first line must be BEGIN.')
            
                # A non-COPY command line.
                try:
                    cursor.execute(line)
                except psycopg2.Error as exc:
                    raise Exception('sqlDump', exc.diag.message_primary)
                    
        # Make sure any previous COPY command completed.
        if writeFd or copier or writePipe or readPipe:
            raise Exception('sqlDump', 'Missing EOF terminator for COPY command.')

    finally:
        if writeFd:
            writeFd.close()
            writeFd = None
        if copier:
            copier.join(60)
            copier = None
        if writePipe:
            os.close(writePipe)
            writePipe = None
        if readPipe:
            os.close(readPipe)
            readPipe = None

    if not committed:
        raise Exception('sqlDump', 'Missing COMMIT statement.')
    
            
# Main Program
if __name__ == "__main__":
    rv = RV_SUCCESS
    log = None
    
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
        parser.add_argument('-j', '--jmd', help="When receiving a dump file, if set then set-up the JMD to pre-fetch the series' Storage Units.", dest='jmd', action='store_true', default=False)
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
            try:
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
        
                        serviceURL = arguments.getArg('kSubService')
                        pubServiceURL = arguments.getArg('kPubListService')
                        xferURL = arguments.getArg('kSubXfer')
                        
                        # Check for pending request. Fetch any existing request from the server for this client. Then fill-in the 
                        # various arguments to this script with the values from that request. The client can get into this state if
                        # they kill this script before it completes. If they re-run this script, we want their existing request to 
                        # complete. To be really safe about this, I could save a UUID on both the server and client, and make sure they
                        # are equal before proceeding, but maybe some other time.
                        cgiArgs = { 'action' : 'continue', 'client' : client }
                        
                        log.writeInfo([ 'Checking for existing request at server.' ])
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
                            log.writeInfo([ 'Found existing request at server: reqid=' + str(reqId) + ', reqtype=' + reqType + ', series=' + ','.join(seriesList) + ', archive=' + str(archive) + ', retention=' + str(retention) + ', tapegroup=' + str(tapeGroup) ])
                        else:
                            reqType = arguments.getArg('reqtype').lower()
                            archive = arguments.getArg('archive')
                            retention = arguments.getArg('retention')
                            tapeGroup = arguments.getArg('tapegroup')
                            seriesList = arguments.getArg('series')

                            resuming = False
                            log.writeInfo([ 'No existing request at server.' ])

                        # JMD integration is a client-side feature.
                        jmdIntegration = arguments.getArg('jmd')

                        if not resuming:
                            if reqType == 'subscribe':
                                # Make sure that there is only a single series in the series list and make sure that
                                # the series is one that does not exist locally and make sure that the client is not
                                # already subscribed to the series and make sure that the series published.
                                if len(seriesList) > 1:
                                    raise Exception('invalidArgument', 'Please subscribe to a single series at a time.')
                                elif len(seriesList) == 0:
                                    raise Exception('invalidArgument', 'Please provide a series to which you would like to subscribe.')
                
                                series = seriesList[0]
            
                                if seriesExists(conn, series):
                                    raise Exception('invalidArgument', series + ' already exists locally. Please select a different series.')

                                if clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('invalidArgument', 'You are already subscribed to ' + series + '. Please select a different series.')

                                if not seriesIsPublished(pubServiceURL, series):
                                    raise Exception('invalidArgument', 'Series ' + series + ' is not published and not available for subscription.')

                                if newSite:
                                    # Make sure the admin.ns table exists, because we are going to insert into it. And if it is missing,
                                    # then the NetDRMS is bad.
                                    if not dbTableExists(cursor, 'admin.ns'):
                                        raise Exception('drms', 'Your DRMS is missing a required database relation (or the containing schema): admin.ns')
                    
                                    # We are also going to insert into several database tables. Make sure they exist.
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

                                # To subscribe to a series, provide client, series, archive, retention, tapegroup, newSite.
                                cgiArgs = { 'action' : 'subscribe', 'client' : client, 'newsite' : True, 'series' : series, 'archive' : archive, 'retention' : retention, 'tapegroup' : tapeGroup, 'newsite' : newSite }
                            elif reqType == 'resubscribe':
                                # Make sure that newSite is False.
                                if newSite:
                                    raise Exception('invalidArgument', 'Cannot re-subscribe to series. Client ' + client + ' has never subscribed to any series')
            
                                # Make sure that there is only a single series in the series list and make sure that
                                # the series exists locally and make sure that the client is currently subscribed to this series and
                                # make sure the series is still published.
                                if len(series) > 1:
                                    raise Exception('invalidArgument', 'Please re-subscribe to a single series at a time.')
                                elif len(series) == 0:
                                    raise Exception('invalidArgument', 'Please provide a series to which you would like to re-subscribe.')

                                series = seriesList[0]

                                if not seriesExists(cursor, series):
                                    raise Exception('invalidArgument', series + ' does not exist locally. Please select a different series to which you would like to re-subscribe.')
            
                                if not clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('invalidArgument', 'You are not subscribed to ' + series + '. Please select a different series to which you would like to re-subscribe.')
                
                                if not seriesIsPubished(series):
                                    raise Exception('invalidArgument', 'Series ' + series + ' is not published and not available for re-subscription.')

                                # To re-subscribe to a series, provide client, series.
                                cgiArgs = { 'action' : 'resubscribe', 'client' : client, 'newsite' : False, 'series' : series }
                            elif reqType == 'unsubscribe':
                                # Make sure that newSite is False.
                                if newSite:
                                    raise Exception('invalidArgument', 'Cannot un-subscribe from series. Client ' + client + ' has never subscribed to any series')
                
                                # Make sure there is at least one series and make sure that the series exist locally and 
                                # make sure that the client is currently subscribed to these series.
                                if len(series) < 1:
                                    raise Exception('invalidArgument', 'Please provide one or more series from which you would like to un-subscribe.')

                                for series in seriesList:
                                    if not seriesExists(cursor, series):
                                        raise Exception('invalidArgument', series + ' does not exist locally. Please select a different series to which you would like to re-subscribe.')
            
                                    if not clientIsSubscribed(client, pubServiceURL, series):
                                        raise Exception('invalidArgument', 'You are not subscribed to ' + series + '. Please select a different series to which you would like to re-subscribe.')
            
                                # To un-subscribe from one or more series, provide client, series.
                                cgiArgs = { 'action' : 'unsubscribe', 'client' : client, 'newsite' : False, 'series' : ','.join(seriesList) }
                            else:
                                raise Exception('invalidArgument', 'Unknown subscription request type: ' + reqType + '.')
        
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

                        # Back to the case statements.
                        if reqType == 'subscribe' or reqType == 'resubscribe':
                            # poll for dump file.
                            log.writeInfo([ 'Polling for dump file.' ])
                            cgiArgs = { 'action' : 'polldump', 'client' : client, 'reqid' : reqId}
                            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).

                            while (resuming and info['status'] == STATUS_REQUEST_RESUMING) or info['status'] == STATUS_REQUEST_QUEUED or info['status'] == STATUS_REQUEST_PROCESSING:
                                log.writeInfo([ 'Calling cgi with URL: ' + serviceURL ])
                                log.writeInfo([ 'cgi args: ' + json.dumps(cgiArgs) ])
                                with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                    info = json.loads(response.read().decode('UTF-8'))
                                    if 'status' not in info or 'msg' not in info:
                                        raise Exception('subService', 'Request failure: ' + STATUS_ERR_INTERNAL + '.')
                                    log.writeInfo([ 'cgi response: ' + info['msg'] + ' Response status: ' + info['status'] + '.'])
                                    time.sleep(5)

                            if info['status'] != STATUS_DUMP_READY:
                                raise Exception('subService', 'Unexpected response from subscription service: ' + info['status'] + '.')
                                
                            # del log
                            raise Exception('blah', 'trying to get out')

                            # Download create-ns/dump-file tarball.
                            dest = os.path.join(arguments.getArg('kLocalWorkingDir'), client + '.sql.tar.gz')
                            dl = SmarlDL(xferURL)
                            dl.start()
            
                            # Extract files from tarball.
                            with tarfile.open(dl.get_dest()) as tar:
                                dest = arguments.getArg('kLocalWorkingDir')

                                if reqType == 'subscribe' and not dbSchemaExists(conn, schema):
                                    # create-ns    
                                    with dest.getmember(client + '.' + schema + '.' + 'createns.sql') as createNsMember:
                                        tar.extract(createNsMember, dest)

                                with dest.getmember(client + '.subscribe_series.sql') as dumpMember:
                                    tar.extract(dumpMember, dest)
            
                            # Apply the series-schema-creation SQL (new subscriptions only).
                            if reqType == 'subscribe':
                                # Check for the existence of the schema. 
                                if not dbSchemaExists(conn, schema):
                                    # Ingest createns.sql. Will raise if a problem occurs. When that happens, the cursor is rolled back.
                                    ingestCreateNSFile(client, schema, cursor, log)

                            # Apply the series-creation (new subscriptions only) / _jsoc-creation (new site only) / series-population SQL. This is a bit tricky.
                            # We can apply each SQL command as we read it from the file. In theory, these commands could span multiple lines. Commands are separated
                            # by semicolons which are not necessarily followed by newlines. But there could be semicolons in the strings of commands, and various forms
                            # of escaping to deal with. Yuck! We'd need a heavy-weight parser to do this in a general way. However, the dump file has a 
                            # specific format which we will exploit.
                            # 
                            # psycopg2 does not provide a means for piping an SQL file to the database - end of story. If you read a file into memory to use the 
                            # cursor.execute() command, it reads the WHOLE file into memory before executing cursor.execute(). So, we HAVE TO parse the SQL file
                            # in some way. 
                            #
                            # If reqType == 'subscribe', then the sql will create a new series and populate it. 
                            # If reqType == 'resubscribe', then the sql will truncate the 'series table' and reset the series-table sequence
                            # only.
                            #
                            # Will raise if a problem occurs. When that happens, the cursor is rolled back.
                            ingestDumpFile(client, series, cursor, log)

                            # Send a pollcomplete request to the subscription service. After submitting this request, the Slony logs
                            # we receive could have insert statements for newly subscribed-to series.
                            cgiArgs = { 'action' : 'pollcomplete', 'reqid' : reqId}
                            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).

                            while info['status'] == STATUS_REQUEST_FINALIZING:
                                with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                    info = json.loads(response.read().decode('UTF-8'))
                                    time.sleep(5)
                    
                            if info['status'] != STATUS_REQUEST_COMPLETE:
                                raise Exception('subService', info['status'], 'Unexpected response from subscription service: ' + info['status'])
                
                            # We have successfully subscribed/resubscribed to a series. For a subscribe request, if the 'jmd' flag is set, 
                            # then populate the JMD's sunum queue table and install the trigger that auto-populates this table as new series 
                            # rows are ingested. We need to stop the Slony-log ingestion script first so that no new records are ingested 
                            # while the copy to sunum_queue is happening and the trigger is being installed.
                            if reqType == 'subscribe' and jmdIntegration:
                                dieFile = os.path.join(arguments.getArg('ingestion_path'), 'get_slony_logs.' + client + '.die')
                
                                pass
                        elif reqType == 'unsubscribe':
                            pass
                except (psycopg2.DatabaseError, psycopg2.OperationalError) as exc:
                    # Closes the cursor and connection
                    if hasattr(exc, 'diag') and hasattr(exc.diag, 'message_primary') and exc.diag.message_primary:
                        msg = exc.diag.message_primary
                    else:
                        import traceback
                    
                        msg = traceback.format_exc(0)
                    raise Exception('dbConnection', msg)

            except Exception as exc:
                if len(exc.args) == 2:
                    type = exc.args[0]
                    msg = exc.args[1]
            
                    if type == 'drms':
                        rv = RV_DRMS
                    if type == 'dbConnection':
                        rv = RV_DBCONNECTION
                    elif type == 'invalidArgument' or type == 'args':
                        rv = RV_ARGS
                    elif type == 'dbCmd':
                        rv = RV_DBCMD
                    elif type == 'subService':
                        rv = RV_SUBSERVICE

                    log.writeError([ msg ])
                else:
                    raise
        # We are leaving the termination handler.
    except Exception as exc:
        if len(exc.args) == 2:
            type = exc.args[0]
            msg = exc.args[1]
            
            if type == 'drmsParams':
                rv = RV_DRMSPARAMS
            elif type == 'args':
                rv = RV_ARGS
            elif type == 'argsFile':
                rv = RV_CLIENTCONFIG
            elif type == 'drmsLock':
                rv = RV_LOCK

            if log:
                log.writeError([ msg ])
            else:
                print(msg)
        else:
            raise
            
    log.close()
    logging.shutdown()

    sys.exit(rv)
