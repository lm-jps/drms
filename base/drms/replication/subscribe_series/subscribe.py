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
from subprocess import Popen, CalledProcessError, PIPE, check_call
import fcntl
import psycopg2
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
        
        # Remove subscription lock file by calling parent __exit__().
        super(TerminationHandler, self).__exit__(etype, value, traceback)

    def hastaLaVistaBaby(self):        
        # Do not do this! The call of Log::__del__ is deferred until after the program exits. If you end your program by
        # calling sys.exit(), then sys.exit() gets called BEFORE Log::__del__ is called, and this causes a race condition.
        # del self.log
        # self.log.close()
        # On second thought, let's not close the log either. Just flush for now, and close() at the end of the program run.
        self.log.flush()        
    
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

def ingestCreateNsFile(createNsFile, psqlBin, dbhost, dbport, dbname, dbuser, log):
    # Make a pipe to communicate with psql process. Open the sql file in with UTF8 encoding, 
    # open the write-end of the pipe with UTF8 encoding, and open the read-end of the pipe with
    # LATIN-1 encoding (the db is in LATIN-1).
    with open(createNsFile, encoding='UTF8') as sqlIn:
        pipeReadEndFD, pipeWriteEndFD = os.pipe()
        
        # Make the read-end non-blocking.
        flag = fcntl.fcntl(pipeReadEndFD, fcntl.F_GETFL)
        fcntl.fcntl(pipeReadEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)
        
        pipeReadEnd = os.fdopen(pipeReadEndFD, 'r', encoding='LATIN1')
        pipeWriteEnd = os.fdopen(pipeWriteEndFD, 'w', encoding='UTF8')

        try:
            cmdList = [ psqlBin, '-h', dbhost, '-p', dbport, '-d', dbname, '-U', dbuser, '-f', '-']
            proc = Popen(cmdList, stdin=pipeReadEnd, stdout=PIPE, stderr=PIPE)
        except OSError as exc:
            raise Exception('sqlDump', "Cannot run command '" + ' '.join(cmdList) + "' ")
        except ValueError as exc:
            raise Exception('sqlDump', "psql command '" + ' '.join(cmdList) + "' called with invalid arguments.")            

        for line in sqlIn:
            strippedLine = line.rstrip()
            print(strippedLine, file=pipeWriteEnd)
            
        pipeWriteEnd.flush()
        pipeWriteEnd.close()
            
        out, err = proc.communicate()
        pipeReadEnd.close()

# I give up. You cannot pipe the dump file into psql this way. For some reason, the COPY command will not work.
# But the dump file is not in the correct encoding. You have to pass an actual file to the psql -f command.
# I guess the way around this is to modify the server to print this line at the top of the dump file:
#   SET CLIENT_ENCODING TO 'UTF8';
# Then we simply call check_call() to cal psql with the -f flag that contains the path to the dump file.
def ingestDumpFile(dumpFile, psqlBin, dbhost, dbport, dbname, dbuser, log):
    try:
        cmdList = [ psqlBin, '-h', dbhost, '-p', dbport, '-d', dbname, '-U', dbuser, '-f', dumpFile ]
        check_call(cmdList)
    except CalledProcessError as exc:
        raise Exception('sqlDump', ' '.join(cmdList) + ' returned non-zero code ' + exc.returncode + '.')
    
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

                        # JMD integration is a client-side feature.
                        jmdIntegration = arguments.getArg('jmd')
                        # For subscribe/unsubscribe only.
                        series = None 
                        schema = None
                        
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
                                schema = extractSchema(series)
            
                                if seriesExists(conn, series):
                                    raise Exception('invalidArgument', series + ' already exists locally. Please select a different series.')

                                if clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('invalidArgument', 'You are already subscribed to ' + series + '. Please select a different series.')

                                if not seriesIsPublished(pubServiceURL, series):
                                    raise Exception('invalidArgument', 'Series ' + series + ' is not published and not available for subscription.')

                                if newSite:
                                    # Make sure the admin.ns table exists, because we are going to insert into it. And if it is missing,
                                    # then the NetDRMS is bad.
                                    if not dbTableExists(conn, 'admin', 'ns'):
                                        raise Exception('drms', 'Your DRMS is missing a required database relation (or the containing schema): admin.ns')
                                        
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
                                schema = extractSchema(series)

                                if not seriesExists(conn, series):
                                    raise Exception('invalidArgument', series + ' does not exist locally. Please select a different series to which you would like to re-subscribe.')
            
                                if not clientIsSubscribed(client, pubServiceURL, series):
                                    raise Exception('invalidArgument', 'You are not subscribed to ' + series + '. Please select a different series to which you would like to re-subscribe.')
                
                                if not seriesIsPubished(series):
                                    raise Exception('invalidArgument', 'Series ' + series + ' is not published and not available for re-subscription.')
                                    
                                if dbSchemaExists(conn, schema):
                                    # The site should have all these tables, since the series exists locally.
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

                                # To re-subscribe to a series, provide client, series.
                                cgiArgs = { 'action' : 'resubscribe', 'client' : client, 'newsite' : False, 'series' : series }
                            elif reqType == 'unsubscribe':
                                # Make sure that newSite is False.
                                if newSite:
                                    raise Exception('invalidArgument', 'Cannot un-subscribe from series. Client ' + client + ' has never subscribed to any series')
                
                                # Make sure there is at least one series and make sure that the series exist locally and 
                                # make sure that the client is currently subscribed to these series.
                                if len(seriesList) < 1:
                                    raise Exception('invalidArgument', 'Please provide one or more series from which you would like to un-subscribe.')

                                for series in seriesList:
                                    if not seriesExists(conn, series):
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
                        else:
                            # Resuming. 
                            if reqType == 'subscribe' or reqType == 'resubscribe':
                                # Need to re-initialize some variables (series, schema).
                                series = seriesList[0]
                                schema = extractSchema(series)

                        # Back to the case statements.
                        if reqType == 'subscribe' or reqType == 'resubscribe':
                            if !resuming or resumeAction.lower() == 'polldump':
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

                            # Download create-ns/dump-file tarball.
                            if !resuming or resumeStatus == 'A':
                                scheme, netloc, path, query, frag = urllib.parse.urlsplit(arguments.getArg('kSubXfer'))
                                xferURL = urllib.parse.urlunsplit((scheme, netloc, os.path.join(path, client + '.sql.tar.gz'), None, None))
                                dest = os.path.join(arguments.getArg('kLocalWorkingDir'), client + '.sql.tar.gz')

                                # SmartDL downloads the file via multiple streams into temporary files. It combines the multiple
                                # files into a single destination file when the download completes. So, if the destination file
                                # exists, the download completed and was successful.
                                if not os.path.exists(dest):
                                    dl = SmartDL(xferURL, dest)
                                    dl.start()
            
                                # Extract files from tarball.
                                with tarfile.open(dl.get_dest()) as tar:
                                    dest = arguments.getArg('kLocalWorkingDir')

                                    if reqType == 'subscribe' and not dbSchemaExists(conn, schema):
                                        # create-ns    
                                        createNsMember = tar.getmember(client + '.' + schema + '.' + 'createns.sql')
                                        tar.extract(createNsMember, dest)

                                    dumpMember = tar.getmember(client + '.subscribe_series.sql')
                                    tar.extract(dumpMember, dest)

                                createNsFile = os.path.join(arguments.getArg('kLocalWorkingDir'), client + '.' + schema + '.' + 'createns.sql')
                                dumpFile = os.path.join(arguments.getArg('kLocalWorkingDir'), client + '.subscribe_series.sql')
                            
                                # Apply the series-schema-creation SQL.
                                if reqType == 'subscribe':
                                    # Check for the existence of the schema. 
                                    if not dbSchemaExists(conn, schema):
                                        # Ingest createns.sql. Will raise if a problem occurs. When that happens, the cursor is rolled back.
                                        ingestCreateNsFile(createNsFile, arguments.getArg('PSQL').strip(" '" + '"'), arguments.getArg('pg_host'), str(arguments.getArg('pg_port')), arguments.getArg('pg_dbname'), arguments.getArg('pg_user'), log)
                                        log.writeInfo([ 'Successfully ingested createNs file: ' + createNsFile + '.' ])

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
                                ingestDumpFile(dumpFile, arguments.getArg('PSQL').strip(" '" + '"'), arguments.getArg('pg_host'), str(arguments.getArg('pg_port')), arguments.getArg('pg_dbname'), arguments.getArg('pg_user'), log)
                                log.writeInfo([ 'Successfully ingested dump file: ' + dumpFile + '.' ])

                            # We want to issue the pollcomplete request regardless of our resuming status (if we are resuming
                            # we already issued the polldump request above, if we needed to).

                            # Send a pollcomplete request to the subscription service. This will set status to 'I' to tell the server to
                            # clean up and finalize the request. After submitting this request, the Slony logs
                            # we receive could have insert statements for newly subscribed-to series.
                            cgiArgs = { 'action' : 'pollcomplete', 'client' : client, 'reqid' : reqId}
                            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).

                            # We just sent the first pollcomplete request. This tells the server to clean up and finalize, so 
                            # the request is in the STATUS_REQUEST_FINALIZING state.
                            info = {}
                            info['status'] = STATUS_REQUEST_FINALIZING

                            while info['status'] == STATUS_REQUEST_FINALIZING:
                                with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                    info = json.loads(response.read().decode('UTF-8'))
                                    time.sleep(2)
                    
                            if info['status'] != STATUS_REQUEST_COMPLETE:
                                # Must undo (If newsite, remove _jsoc. If new schema, remove schema and remove schema's entry from admin.ns. If
                                # subscribing, remove database table.)
                                raise Exception('subService', info['status'], 'Unexpected response from subscription service: ' + info['status'])
                
                            # We have successfully subscribed/resubscribed to a series. For a subscribe request, if the 'jmd' flag is set, 
                            # then populate the JMD's sunum queue table and install the trigger that auto-populates this table as new series 
                            # rows are ingested. We need to stop the Slony-log ingestion script first so that no new records are ingested 
                            # while the copy to sunum_queue is happening and the trigger is being installed.
                            if reqType == 'subscribe' and jmdIntegration:
                                dieFile = os.path.join(arguments.getArg('ingestion_path'), 'get_slony_logs.' + client + '.die')
                
                                pass
                        elif reqType == 'unsubscribe':
                            cgiArgs = { 'action' : 'pollcomplete', 'client' : client, 'reqid' : reqId}
                            urlArgs = urllib.parse.urlencode(cgiArgs) # For use with HTTP GET requests (not POST).

                            # We just sent the first pollcomplete request. This tells the server to clean up and finalize, so 
                            # the request is in the STATUS_REQUEST_FINALIZING state.
                            info = {}
                            info['status'] = STATUS_REQUEST_PROCESSING

                            while info['status'] == STATUS_REQUEST_QUEUED or info['status'] == STATUS_REQUEST_PROCESSING:
                                with urllib.request.urlopen(serviceURL + '?' + urlArgs) as response:
                                    info = json.loads(response.read().decode('UTF-8'))
                                    time.sleep(2)
                                    
                            if info['status'] != STATUS_REQUEST_COMPLETE:
                                raise Exception('subService', info['status'], 'Unexpected response from subscription service: ' + info['status'])

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
            import traceback
            if log:
                log.writeError([ traceback.format_exc(5) ])
            else:
                print(traceback.format_exc(5))
            
    log.close()
    logging.shutdown()

    sys.exit(rv)
