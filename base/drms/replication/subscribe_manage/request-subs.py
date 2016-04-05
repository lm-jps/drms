#!/usr/bin/env python


# This is the back-end CGI implementation that accepts new subscription requests via HTTP.
# For each request, this code creates a record in a database table that exists during the
# subscription-request process. The wrapper CGI script must call this script with 
# a command-line, so it must handle both HTTP POST and GET requests, extracting
# the CGI arguments and then placing them on a command-line invocation of this script.

# This program supports three kinds of subscription requests:
# 1. subscribe - Create a new subscription for ONE series for the requestor.
# 2. unsubscribe - Remove from subscription one or more series.
# 3. resubscribe - Like "subscribe", except that the server does not put any DDL into the dump file.
#    The net effect is to truncate the series table at the requesting node, and the re-populate 
#    it. There is no table or schema creation performed.
#


import sys
import os
import json
import re
import logging
import argparse
import signal
import psycopg2
import time
from datetime import datetime, timedelta
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsCgi import CgiParser
from drmsLock import DrmsLock
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

if sys.version_info < (3, 0):
    raise Exception("You must run the 3.0 release, or a more recent release, of Python.")

# Constants
DEBUG_CGI = False
LST_TABLE_SERIES = 'series'
LST_TABLE_NODE = 'node'
CFG_TABLE_NODE = 'node'
SLONY_TABLE = 'sl_table'
SLONY_TABLE_NSP = 'tab_nspname'
SLONY_TABLE_REL = 'tab_relname'

# Request statuses
STATUS_REQUEST_RESUMING = 'requestResuming'
STATUS_REQUEST_QUEUED = 'requestQueued'
STATUS_REQUEST_PROCESSING = 'requestProcessing'
STATUS_DUMP_READY = 'dumpReady'
STATUS_REQUEST_FINALIZING = 'requestFinalizing'
STATUS_REQUEST_COMPLETE = 'requestComplete'
STATUS_ERR_TERMINATED = 'terminated'
STATUS_ERR_INTERNAL = 'internalError'
STATUS_ERR_INVALID_ARGUMENT = 'invalidArgument'
STATUS_ERR_INVALID_REQUEST = 'invalidRequest'
STATUS_ERR_FAILURE = 'requestFailed'

        
def terminator(*args):
    # Raise the SystemExit exception (which will be caught by the __exit__() method below).
    sys.exit(0)

class TerminationHandler(object):
    def __new__(cls, thContainer):
        return super(TerminationHandler, cls).__new__(cls)

    def __init__(self, thContainer):
        self.log = thContainer[1]

        self.container = thContainer
        super(TerminationHandler, self).__init__()
        
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
            self.container[1] = STATUS_ERR_TERMINATED
        self.hastaLaVistaBaby()

    def hastaLaVistaBaby(self):
        # Do not do this! The call of Log::__del__ is deferred until after the program exits. If you end your program by
        # calling sys.exit(), then sys.exit() gets called BEFORE Log::__del__ is called, and this causes a race condition.
        # del self.log
        # self.log.close()
        # On second thought, let's not close the log either. Just flush for now, and close() at the end of the program run.
        self.log.flush()
                
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
            
    def writeInfo(self, text):
        if self.log:
            for line in text:
                self.log.info(line)
    
    def writeWarning(self, text):
        if self.log:
            for line in text:
                self.log.warning(line)
    
    def writeError(self, text):
        if self.log:
            for line in text:
                self.log.error(line)
            
    def writeCritical(self, text):
        if self.log:
            for line in text:
                self.log.critical(line)

class RequestSubsParams(DRMSParams):

    def __init__(self):
        super(RequestSubsParams, self).__init__()

    def get(self, name):
        val = super(RequestSubsParams, self).get(name)

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
            
    def get(self, name):
        # None is returned if the argument does not exist.
        return getattr(self, name, None)
            
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
  
class Response(object):
    def __init__(self, **kwargs):
        if not self.status:
            raise Exception('invalidArgument', 'Derived Response class must set status property.')
        if not 'msg' in kwargs:
            raise Exception('invalidArgument', 'Message must be provided to Response constructor.')
        self.msg = kwargs['msg']
        if not 'client' in kwargs:
            raise Exception('invalidArgument', 'Client name must be provided to Response constructor.')
        self.client = kwargs['client']
        if 'log' in kwargs:
            self.log = kwargs['log']
        else:
            self.log = None
            
        self.jsonRoot = None
    
    def createHeader(self):
        self.header = 'Content-type: application/json'
        
    def createContent(self):
        if self.jsonRoot:
            self.jsonRoot['status'] = self.status
            self.jsonRoot['msg'] = self.msg
        else:
            self.jsonRoot = { 'status' : self.status, 'msg' : self.msg }
            
        self.content = json.dumps(self.jsonRoot)

    def setStatus(self, status):
        self.status = status
    
    def send(self):
        self.createHeader()
        self.createContent()
        
        # To send a response, you simply print to stdout.
        print(self.header)
        print('\n')
        print(self.content)
        
    def logMsg(self):
        pass

class ResumeResponse(Response):
    def __init__(self, **kwargs):
        if 'status' in kwargs:
            self.status = kwargs['status']
        else:
            self.status = STATUS_REQUEST_RESUMING
            
        if 'reqid' in kwargs:
            self.reqid = kwargs['reqid']
        else:
            raise Exception('invalidArgument', 'reqid is required for ResumeResponse constructor.')
            
        if 'reqtype' in kwargs:
            self.reqtype = kwargs['reqtype']
        else:
            raise Exception('invalidArgument', 'reqtype is required for ResumeResponse constructor.')
            
        if 'series' in kwargs:
            self.series = kwargs['series']
        else:
            raise Exception('invalidArgument', 'series is required for ResumeResponse constructor.')
        
        if 'archive' in kwargs:
            self.archive = kwargs['archive']
        else:
            raise Exception('invalidArgument', 'archive is required for ResumeResponse constructor.')
        
        if 'retention' in kwargs:
            self.retention = kwargs['retention']
        else:
            raise Exception('invalidArgument', 'retention is required for ResumeResponse constructor.')
            
        if 'tapegroup' in kwargs:
            self.tapegroup = kwargs['tapegroup']
        else:
            raise Exception('invalidArgument', 'tapegroup is required for ResumeResponse constructor.')
            
        if 'resumeaction' in kwargs:
            self.resumeaction = kwargs['resumeaction']
        else:
            raise Exception('invalidArgument', 'resumeaction is required for ResumeResponse constructor.')
            
        super(ResumeResponse, self).__init__(**kwargs)
        
    def createContent(self):
        if self.jsonRoot:
            self.jsonRoot['reqid'] = self.reqid
            self.jsonRoot['reqtype'] = self.reqtype
            self.jsonRoot['series'] = self.series
            self.jsonRoot['archive'] = self.archive
            self.jsonRoot['retention'] = self.retention
            self.jsonRoot['tapegroup'] = self.tapegroup
            self.jsonRoot['resumeaction'] = self.resumeaction
        else:
            self.jsonRoot = { 'reqid' : self.reqid, 'reqtype' : self.reqtype, 'series' : self.series, 'archive' : self.archive, 'retention' : self.retention, 'tapegroup' : self.tapegroup, 'resumeaction' : self.resumeaction }
        super(ResumeResponse, self).createContent()
        
    def logMsg(self):
        if self.log:
            msg = 'Sent resume response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.writeInfo([ msg ])

class WaitResponse(Response):
    def __init__(self, **kwargs):
        if 'status' in kwargs:
            self.status = kwargs['status']
        else:
            self.status = STATUS_REQUEST_PROCESSING
            
        if 'reqid' in kwargs:
            self.reqid = kwargs['reqid']
        else:
            raise Exception('invalidArgument', 'reqid is required for WaitResponse constructor.')
        super(WaitResponse, self).__init__(**kwargs)
        
    def createContent(self):
        if self.jsonRoot:
            self.jsonRoot['reqid'] = self.reqid
        else:
            self.jsonRoot = { 'reqid' : self.reqid }
        super(WaitResponse, self).createContent()
        
    def logMsg(self):
        if self.log:
            msg = 'Sent wait response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.writeInfo([ msg ])

class ContinueResponse(Response):
    def __init__(self, **kwargs):
        if 'status' in kwargs:
            self.status = kwargs['status']
        else:
            self.status = STATUS_REQUEST_COMPLETE
        super(ContinueResponse, self).__init__(**kwargs)
        
    def logMsg(self):
        if self.log:
            msg = 'Sent continue response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.writeInfo([ msg ])
        
class ErrorResponse(Response):
    def __init__(self, **kwargs):
        if 'status' in kwargs:
            self.status = kwargs['status']
        else:
            self.status = STATUS_ERR_INTERNAL
        super(ErrorResponse, self).__init__(**kwargs)    
    
    def logMsg(self):
        if self.log:
            msg = 'Sent error response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.writeError([ msg ])

def clientIsNew(arguments, conn, client, log):
    # Master database.
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + arguments.getArg('kCfgTable') + ' WHERE ' + CFG_TABLE_NODE + " = '" + client + "'"
    log.writeDebug([ 'Checking client-new status on master: ' + cmd + '.' ])
    
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

def getPendingRequest(conn, reqTable, client):
    pendingRequest = False
    pendAction = None
    pendSeriesList = None
    pendArchive = None
    pendRetention = None
    pendTapegroup = None
    pendStatus = None
    pendErrMsg = None
    
    # Slave database.
    cmd = 'SELECT requestid, action, series, archive, retention, tapegroup, status, errmsg FROM ' + reqTable + " WHERE lower(client) = '" + client + "'"

    numPending = 0        
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd) # Opens a transaction.
            records = cursor.fetchall()

            for record in records:
                if record[6].upper() != 'E' and record[6].upper() != 'S':
                    numPending += 1
                    pendingRequestID = record[0]
                    pendAction = record[1]
                    pendSeriesList = record[2]
                    pendArchive = record[3]
                    pendRetention = record[4]
                    pendTapegroup = record[5]
                    pendStatus = record[6]
                    pendErrMsg = record[7]

            if numPending > 1:
                raise Exception('dbResponse', 'There is more than one pending request for client ' + client + ' (at most there should be one).')
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        conn.rollback() # Closes the transaction.
    
    if numPending > 0:
        return (True, pendingRequestID, pendAction, pendSeriesList, pendArchive, pendRetention, pendTapegroup, pendStatus, pendErrMsg)
    else:
        return (False, None, None, None, None, None, None, None, None)

def clientIsSubscribed(arguments, conn, client, series):
    # Master database.
    cmd = 'SELECT ' + LST_TABLE_SERIES + ' FROM ' + arguments.getArg('kLstTable') + ' WHERE ' + LST_TABLE_NODE + " = '" + client + "' AND " + LST_TABLE_SERIES + " = '" + series + "'"
    
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
    
    # Slave database.
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

def seriesIsPublished(arguments, conn, series):
    regExp = re.compile(r'\s*(\S+)\.(\S+)\s*')
    matchObj = regExp.match(series)
    if matchObj is not None:
        nsp = matchObj.group(1)
        table = matchObj.group(2)
    else:
        raise Exception('args', 'Not a valid DRMS series name: ' + series + '.')

    # Slave database.
    cmd = 'SELECT ' + SLONY_TABLE_NSP + "||'.'||" + SLONY_TABLE_REL + ' AS series FROM _' + arguments.getArg('CLUSTERNAME') + '.' + SLONY_TABLE + ' WHERE ' + SLONY_TABLE_NSP + " = '" + nsp + "' AND " + SLONY_TABLE_REL + " = '" + table + "'"
    
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

def insertRequest(conn, log, dbTable, **kwargs):
    # Insert a pending row into the su_production.slonyreq table. Ensure that the series name comprises lower-case letters.
    client = kwargs['client']
    action = kwargs['action'].lower()
    series = kwargs['series']
    
    if 'archive' in kwargs:
        archive = str(kwargs['archive'])
    else:
        archive = 'null'
    if 'retention' in kwargs:
        retention = str(kwargs['retention'])
    else:
        retention = 'null'
    if 'tapegroup' in kwargs:
        tapegroup = str(kwargs['tapegroup'])
    else:
        tapegroup = 'null'
        
    reqid = -1
    
    try:
        with conn.cursor() as cursor:
             # Slave database.
            cmd = "SELECT nextval('" + dbTable + '_seq' + "')"
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) != 1 or len(records[0]) != 1:
                raise Exception('dbCmd', 'Unexpected db-query results.')
        
            reqid = records[0][0] # integer

            guts = str(reqid) + ", '" + client + "', '" + datetime.now().strftime('%Y-%m-%d %T') + "', '" + action + "', '" + series + "', " + archive + ", " + retention + ", " + tapegroup + ", 'N'"
            log.writeInfo([ 'Inserting new request into db table ' + dbTable + ': (' + guts + ')' ])
    
            # Slave database.
            cmd = 'INSERT INTO ' + dbTable + '(requestid, client, starttime, action, series, archive, retention, tapegroup, status) VALUES(' + guts + ')'
            cursor.execute(cmd)
        conn.commit() # commit only if there are no errors.
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
        
    return reqid

# Main Program
if __name__ == "__main__":
    client = 'unknown'
    
    try:
        requestSubsParams = RequestSubsParams()        
        arguments = Arguments()
    
        # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
        if os.getenv('REQUEST_URI') or DEBUG_CGI:
            parser = CgiParser(usage='%(prog)s action=<action string> [ client=<client> ] [ requestid=<id> ] [ series=<series list> ] [ archive=<archive code> ] [ retention=<number of days> ] [ tapegroup=<group id> ] [ cfg=<configuration file> ]')
        else:
            parser = CmdlParser(usage='%(prog)s action=<action string> [ client=<client> ] [ requestid=<id> ] [ series=<series list> ] [ archive=<archive code> ] [ retention=<number of days> ] [ tapegroup=<group id> ] [ cfg=<configuration file> ]')
            
        # Required (name does not start with a dash).
        parser.add_argument('a', 'action', '--action', help='The request action (subscribe, unsubscribe, resubscribe, polldump, pollcomplete).', metavar='<action>', required=True, dest='action')
        parser.add_argument('c', 'client', '--client', help='The client making the subscription request.', metavar='<client>', required=True, dest='client')

        # Optional (name starts with a dash).
        if not os.getenv('REQUEST_URI'):
            # Do not allow external users to choose the slony configuration file.
            parser.add_argument('-p', '--cfg', help='The configuration file that contains information needed to locate database information.', metavar='<slony configuration file>', dest='slonyCfg', action=CfgAction, arguments=arguments, default=argparse.SUPPRESS)
        
        parser.add_argument('-n', '--newsite', 'newsite', help='If True, from the client perspective, the client has never previously subscribed (True or False).', metavar='<True or False>', dest='newsite', action=BooleanAction, default=argparse.SUPPRESS)
        parser.add_argument('-i', '--reqid', 'reqid', help='The id of a request previously submitted.', metavar='<request id>', dest='reqid', type=int, default=argparse.SUPPRESS)
        parser.add_argument('-s', '--series', 'series', help='A comma-separated list of series.', metavar='<series list>', dest='series', action=ListAction, default=argparse.SUPPRESS)
        parser.add_argument('-b', '--archive', 'archive', help='A comma-separated list of series.', metavar='<archive action>', dest='archive', type=int, default=argparse.SUPPRESS)
        parser.add_argument('-r', '--retention', 'retention', help='The number of days to archive the data files.', metavar='<retention>', dest='retention', type=int, default=argparse.SUPPRESS)
        parser.add_argument('-t', '--tapegroup', 'tapegroup', help='The group id of the archive tapes.', metavar='<tape group>', dest='tapegroup', type=int, default=argparse.SUPPRESS)
        parser.add_argument('-l', '--loglevel', 'loglevel', help='Specifies the amount of logging to perform. In increasing order: critical, error, warning, info, debug', dest='loglevel', action=LogLevelAction, default=logging.ERROR)
    
        arguments.setParser(parser)
        
        # Add the server-side subscription arguments to arguments.
        if arguments.get('slonyCfg') is None:
            arguments.setArg('slonyCfg', requestSubsParams.get('SLONY_CONFIG'))
            arguments.addFileArgs(arguments.getArg('slonyCfg'))

        # Create/Initialize the log file.
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        rsLog = Log(os.path.join(arguments.getArg('kSMcgiLogDir'), 'request-subs-log.txt'), arguments.getArg('loglevel'), formatter)

        # Check for the two required arguments. Do not use required=True as an argument for these two arguments, otherwise if they are 
        # missing, then argparse will ERROR-OUT, instead of raising an exception. We need an exception to be raised so that we
        # can send an appropriate response to the client in the exception handler.
        if not arguments.get('action') or not arguments.get('client'):
            raise Exception('invalidArgument', 'To run this program, you must supply two required arguments: ' + "'action' and 'client'.")
            
        action = arguments.getArg('action')
        client = arguments.getArg('client')
        
        # Copy DRMSParams arguments to arguments object.
        arguments.setArg('dbuser', requestSubsParams.get('WEB_DBUSER'))
        
        pid = os.getpid()
        strPid = str(os.getpid())
        
        rsLog.writeCritical(['Logging threshold level is ' + rsLog.getLevel() + '.']) # Critical - always write the log level to the log.

        arguments.dump(rsLog)
        
        thContainer = [ None, rsLog ]
        
        # We need to handle signals so we can ensure that the requestor receives a decent response in the event of an error.
        with TerminationHandler(thContainer) as th:
            # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called. The first
            # db statement will open a transaction, and it will remain open until the conn.commit() or conn.rollback() call is made.
            # Open a connection to both databases. On the master database, apache has read-only permissions.
            with psycopg2.connect(database=arguments.getArg('SLAVEDBNAME'), user=arguments.getArg('dbuser'), host=arguments.getArg('SLAVEHOSTNAME'), port=str(arguments.getArg('SLAVEPORT'))) as connSlave, psycopg2.connect(database=arguments.getArg('MASTERDBNAME'), user=arguments.getArg('dbuser'), host=arguments.getArg('MASTERHOSTNAME'), port=str(arguments.getArg('MASTERPORT'))) as connMaster:
                rsLog.writeInfo([ 'Connected to database ' + arguments.getArg('SLAVEDBNAME') + ' on ' + arguments.getArg('SLAVEHOSTNAME') + ':' + str(arguments.getArg('SLAVEPORT')) + ' as user ' + arguments.getArg('dbuser') ])
                rsLog.writeInfo([ 'Connected to database ' + arguments.getArg('MASTERDBNAME') + ' on ' + arguments.getArg('MASTERHOSTNAME') + ':' + str(arguments.getArg('MASTERPORT')) + ' as user ' + arguments.getArg('dbuser') ])

                # Set to None if the argument does not exist.                
                newSite = arguments.get('newsite')
                reqid = arguments.get('reqid')
                seriesList = arguments.get('series')
                archive = arguments.get('archive')
                retention = arguments.get('retention')
                tapegroup = arguments.get('tapegroup')
            
                pendingRequest = False    
                # The server should always send the create schema command. Put logic in the client side that decided whether it should be 
                # run or not.
                rsLog.writeInfo([ 'client is ' + client + '.' ])

                newSiteServer = clientIsNew(arguments, connMaster, client, rsLog)
                if newSite is not None and newSiteServer!= newSite:
                    raise Exception('invalidArgument', 'The newsite status at the client does not match the newsite status at the server.')
                newSite = newSiteServer
                rsLog.writeInfo([ 'newSite is ' + str(newSite) + '.' ])
                            
                # Check for an existing request. If there is such a request, return a status code telling the user to poll on the request to
                # await completion.
                pendingRequest, pendRequestID, pendAction, pendSeriesList, pendArchive, pendRetention, pendTapegroup, pendStatus, pendErrMsg = getPendingRequest(connSlave, arguments.getArg('kSMreqTable'), client)

                if pendingRequest:
                    rsLog.writeInfo([ 'client ' + client + ' has a pending request:' + ' id - ' + str(pendRequestID) + ', action - ' + pendAction + ', series - ' + pendSeriesList + ', status - ' + pendStatus + ', errMsg - ' + str(pendErrMsg if pendErrMsg else "''") ])
                else:
                    rsLog.writeInfo([ 'client ' + client + ' does NOT have a pending request.' ])

                if action.lower() == 'continue':
                    if not pendingRequest:
                        raise Exception('invalidArgument', 'Cannot resume an existing request. There is no pending request for client ' + client + '.')
                        
                    # Figure out if client should make a pollDump or pollComplete request.
                    if pendStatus.upper() == 'N' or pendStatus.upper() == 'P' or pendStatus.upper() == 'D':
                        respAction = 'polldump'
                    elif pendStatus.upper() == 'I' or pendStatus.upper() == 'C':
                        respAction = 'pollcomplete'
                    else:
                        # Error response.
                        raise Exception('requestFailed', 'There was an error processing your request. Please try again or contact the JSOC.')
                    
                    resp = ResumeResponse(log=rsLog, status=STATUS_REQUEST_RESUMING, msg='To continue, make a ' + respAction.lower() + ' request.', reqid=pendRequestID, reqtype=pendAction, series=pendSeriesList.split(','), archive=pendArchive, retention=pendRetention, tapegroup=pendTapegroup, resumeaction=respAction, client=client)
                    resp.logMsg()
                    resp.send()
                elif action.lower() == 'subscribe' or action.lower() == 'resubscribe':
                    if pendingRequest:
                        raise Exception('invalidRequest', 'You cannot ' + action.lower() + ' to a series at this time - a ' + pendAction + ' request for ' + pendSeriesList + ' is pending. Please wait for that request to complete.')
                    if len(client) < 1:
                        raise Exception('invalidArgument', 'You must provide a valid client name.')
                    if newSite and action.lower() != 'subscribe':
                        raise Exception('invalidRequest', 'You have never subscribed to a series before. You cannot make a ' + action.lower() + ' request.')

                    # Allow the user to subscribe/resubscribe to a single series at a time.
                    if seriesList is None or len(seriesList) != 1:
                        raise Exception('invalidArgument', 'You must specify a single series to which you would like to ' + action.lower() + '.')
                    
                    series = seriesList[0]
                    
                    if action.lower() == 'subscribe':
                        rsLog.writeInfo([ 'client ' + client + ' is requesting a subscription to series ' + series + '.' ])
                        if archive is None or (archive != 0 and archive != -1 and archive != 1):
                            raise Exception('invalidArgument', 'You must provide an integer value of -1, 0, or 1 for the archive argument.')
                        if retention is None or retention < 0:
                            raise Exception('invalidArgument', 'You must provide an integer value greater than or equal to 0 for the retention argument.')
                        if tapegroup is None or tapegroup < 0:
                            raise Exception('invalidArgument', 'You must provide an integer value greater than or equal to 0 for the tapegroup argument.')
                    else:
                        rsLog.writeInfo([ 'client ' + client + ' is requesting a RE-subscription to series ' + series + '.' ])

                    # Check for existing subscription to series.
                    subscribed = clientIsSubscribed(arguments, connMaster, client, series)
                    if subscribed:
                        rsLog.writeInfo([ 'client ' + client + ' is currently subscribed to series ' + series + '.' ])
                    else:
                        rsLog.writeInfo([ 'client ' + client + ' is NOT currently subscribed to series ' + series + '.' ])
                    
                    if action.lower() == 'subscribe':
                        if subscribed:
                            raise Exception('invalidArgument', 'Cannot subscribe to ' + series + '; client ' + client + ' is already subscribed to this series.')
                    else:
                        if not subscribed:
                            raise Exception('invalidArgument', 'Cannot re-subscribe to ' + series + '; client ' + client + ' is not already subscribed to this series.')

                    if not seriesExists(connSlave, series):
                        raise Exception('invalidArgument', 'Cannot ' + action.lower() + ' to ' + series + '; it does not exist.')
                    if not seriesIsPublished(arguments, connSlave, series):
                        raise Exception('invalidArgument', 'Cannot ' + action.lower() + ' to ' + series + '; it is not published.')
                        
                    rsLog.writeInfo([ 'series ' + series + ' exists on the server and is published.' ])

                    # Insert a pending row into the su_production.slonyreq table. Ensure that the series name comprises lower-case letters.
                    # Slave database.
                    reqid = insertRequest(connSlave, rsLog, arguments.getArg('kSMreqTable'), client=client, action=action, series=','.join(seriesList), archive=archive, retention=retention, tapegroup=tapegroup)
                
                    if action.lower() == 'subscribe':
                        respMsg = 'Request for subscription to series ' + series + ' is queued. Poll for completion with a polldump request. Please sleep between iterations when looping over this request.'
                    else:
                        respMsg = 'Request for re-subscription to series ' + series + ' is queued. Poll for completion with a polldump request. Please sleep between iterations when looping over this request.'
                    
                    # Send a 'wait' response.
                    resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg, reqid=reqid, client=client)
                    resp.logMsg()
                    resp.send()
                elif action.lower() == 'unsubscribe':
                    if pendingRequest:
                        raise Exception('invalidRequest', 'You cannot un-subscribe from series at this time - a ' + pendAction + ' request for ' + pendSeriesList + ' is pending. Please wait for that request to complete.')
                    if len(client) < 1:
                        raise Exception('invalidArgument', 'You must provide a valid client name.')
                    if newSite:
                        raise Exception('invalidArgument', 'You have never subscribed to a series before. You cannot make a ' + action.lower() + ' request.')

                    # The user can unsubscribe from multiple series.
                    if seriesList is None or len(seriesList) < 1:
                        raise Exception('invalidArgument', 'Please provide a list of series from which you would like to unsubscribe.')
                
                    reqid = insertRequest(connSlave, rsLog, arguments.getArg('kSMreqTable'), client=client, action=action, series=','.join(seriesList))
                    
                    respMsg = 'Request for un-subscription from series ' + ','.join(seriesList) + ' is queued. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.'
                    resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg, reqid=reqid, client=client)
                    resp.logMsg()
                    resp.send()
                elif action.lower() == 'polldump' or action.lower() == 'pollcomplete':
                    if reqid is None:
                        raise Exception('invalidArgument', 'Please provide a request ID.')
                    if not pendRequestID or pendRequestID != reqid:
                        raise Exception('invalidArgument', 'The request ID provided (' + str(reqid) + ') does not match the ID of the request currently pending (' + str(pendRequestID) + ').')
                    if not pendingRequest:
                        if action.lower() == 'polldump':
                            raise Exception('invalidRequest', 'You cannot poll for dump-completion of request ' + str(reqid) + '. That request is not pending.')
                        else:
                            raise Exception('invalidRequest', 'You cannot poll for completion of request ' + str(reqid) + '. That request is not pending.')
                    

                    # Now, we have to poll on the pending request, waiting for manage-subs.py to reply with:
                    #   D - dump complete
                    #   C - complete (clean-up is done)
                    #   E - error
                    if pendStatus.upper() == 'N' or pendStatus.upper() == 'P' or pendStatus.upper() == 'D':
                        if action.lower() != 'polldump':
                            raise Exception('invalidArgument', 'You must send a polldump request to continue with the subscription process.')
                        if pendStatus.upper() == 'D':
                        
                            # The client acknowledges that the dump is ready to be downloaded and applied.
                            try:
                                # Slave database.
                                with connSlave.cursor() as cursor:
                                    cmd = 'UPDATE ' + arguments.getArg('kSMreqTable') + " SET status = 'A' WHERE requestid = " + str(reqid)
                                    cursor.execute(cmd)
                                    pendStatus = 'A'
                            except psycopg2.Error as exc:
                                raise Exception('dbCmd', exc.diag.message_primary)
                            finally:                        
                                connSlave.commit()
                        
                            # Send a 'continue' response.
                            if pendAction.lower() == 'subscribe' or pendAction.lower() == 'resubscribe':
                                resp = ContinueResponse(log=rsLog, status=STATUS_DUMP_READY, msg='The SQL dump file is ready for ingestion.', client=client)
                            elif pendAction.lower() == 'unsubscribe':
                                raise Exception('manage-subs', 'Unexpected request status(D)/action(unsubscribe) combination for request ' + str(reqid) + '.')
                            else:
                                raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')
                        elif pendStatus.upper() == 'N':
                            # Send a 'wait' response.
                            if pendAction.lower() == 'subscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='Request for subscription to series ' + pendSeriesList + ' is queued. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            elif pendAction.lower() == 'resubscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='Request for re-subscription to series ' + pendSeriesList + ' is queued. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            elif pendAction.lower() == 'unsubscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='Request for un-subscription from series ' + pendSeriesList + ' is queued. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            else:
                                raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')
                        else:
                            # Send a 'wait' response.
                            if pendAction.lower() == 'subscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='Request for subscription to series ' + pendSeriesList + ' is being processed. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            elif pendAction.lower() == 'resubscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='Request for re-subscription to series ' + pendSeriesList + ' is being processed. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            elif pendAction.lower() == 'unsubscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='Request for un-subscription from series ' + pendSeriesList + ' is being processed. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            else:
                                raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')

                        resp.logMsg() 
                        resp.send()
                    elif pendStatus.upper() == 'A' or pendStatus.upper() == 'I' or pendStatus.upper() == 'C':
                        if action.lower() != 'pollcomplete':
                            raise Exception('invalidArgument', 'You must send a pollcomplete request to continue with the subscription process.')
                        if pendStatus.upper() == 'A':
                            # If this is a pollcomplete request, then we need to tell manage-subs.py that the client
                            # has ingested the dump file and is awaiting server clean-up. Update the request status to 'I'.
                            try:
                                # Slave database.
                                with connSlave.cursor() as cursor:
                                    cmd = 'UPDATE ' + arguments.getArg('kSMreqTable') + " SET status = 'I' WHERE requestid = " + str(reqid)
                                    cursor.execute(cmd)
                                    pendStatus = 'I'
                            except psycopg2.Error as exc:
                                raise Exception('dbCmd', exc.diag.message_primary)
                            finally:                        
                                connSlave.commit()
                                
                            if pendAction.lower() == 'unsubscribe':
                                # Status I is not valid for an unsubscribe request.
                                raise Exception('manage-subs', 'Unexpected request status(A)/action(unsubscribe) combination for request ' + str(reqid) + '.')
                            elif pendAction.lower() == 'subscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='Requesting finalization for subscription to series ' + pendSeriesList + '. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            elif pendAction.lower() == 'resubscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='Requesting finalization for re-subscription to series ' + pendSeriesList + '. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            else:
                                raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')
                        elif pendStatus.upper() == 'I':
                            # Send a 'wait' response.
                            if pendAction.lower() == 'unsubscribe':
                                # Status I is not valid for an unsubscribe request.
                                raise Exception('manage-subs', 'Unexpected request status(I)/action(unsubscribe) combination for request ' + str(reqid) + '.')
                            elif pendAction.lower() == 'subscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='Request for subscription to series ' + pendSeriesList + ' is being finalized. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            elif pendAction.lower() == 'resubscribe':
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='Request for re-subscription to series ' + pendSeriesList + ' is being finalized. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.', client=client, reqid=reqid)
                            else:
                                raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')
                        elif pendStatus.upper() == 'C':
                            # Send a 'continue' response.
                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_COMPLETE, msg='Your ' + pendAction.lower() + ' request has successfully completed.', client=client)
                            
                            try:
                                # Slave database.
                                with connSlave.cursor() as cursor:
                                    cmd = 'UPDATE ' + arguments.getArg('kSMreqTable') + " SET status = 'S' WHERE requestid = " + str(reqid)
                                    cursor.execute(cmd)
                            except psycopg2.Error as exc:
                                raise Exception('dbCmd', exc.diag.message_primary)
                            finally:                        
                                connSlave.commit()
                            
                        resp.logMsg()
                        resp.send()
                    elif pendStatus.upper() == 'E':
                        raise Exception('requestFailed', pendErrMsg)
                    else:                                
                        raise Exception('manage-subs', 'Unexcepted status code: ' + pendStatus.upper() + '.')

                    
                    
                        
                        
                else:
                    # Unrecognized action.
                    raise Exception('invalidArgument', 'Action ' + "'" + action + "'" + ' is not recognized.')
                            
        # Check for SIGINT.
        if thContainer[0] == STATUS_ERR_TERMINATED:
            resp = ErrorResponse(log=rsLog, msg='request-subs.py was terminated.', status=STATUS_ERR_TERMINATED, client=client)
            resp.log()
            resp.send()
            
    except Exception as exc:
        if len(exc.args) == 2:
            eType, eMsg = exc.args
            resp = ErrorResponse(log=rsLog, msg=eMsg, client=client)
            
            if eType == 'drmsParams':
                resp.setStatus(STATUS_ERR_INTERNAL)
            elif eType == 'args':
                resp.setStatus(STATUS_ERR_INTERNAL)
            elif eType == 'serverConfig':
                resp.setStatus(STATUS_ERR_INTERNAL)
            elif eType == 'invalidArgument':
                resp.setStatus(STATUS_ERR_INVALID_ARGUMENT)
            elif eType == 'dbResponse':
                resp.setStatus(STATUS_ERR_INTERNAL)
            elif eType == 'dbCmd':
                resp.setStatus(STATUS_ERR_INTERNAL)
            elif eType == 'invalidRequest':
                resp.setStatus(STATUS_ERR_INVALID_REQUEST)
            elif eType == 'manage-subs':
                resp.setStatus(STATUS_ERR_INTERNAL)
            elif eType == 'requestFailed':
                resp.setStatus(STATUS_ERR_FAILURE)
            else:
                if rsLog:
                    rsLog.writeError([ traceback.format_exc(5) ])
                
            resp.logMsg()
            resp.send()
        else:
            import traceback
            
            if rsLog:
                rsLog.writeError([ traceback.format_exc(5) ])
                
            resp = ErrorResponse(log=rsLog, msg='Unknown error in subscription CGI.', client=client)
            resp.send()
            
    rsLog.close()
    logging.shutdown()

    sys.exit(0)
