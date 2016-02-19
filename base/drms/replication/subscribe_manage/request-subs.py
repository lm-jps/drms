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
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsCgi import CgiParser
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

if sys.version_info < (3, 0):
    raise Exception("You must run the 3.0 release, or a more recent release, of Python.")

# Constants
LST_TABLE_SERIES = 'series'
LST_TABLE_NODE = 'node'
CFG_TABLE_NODE = 'node'
SLONY_TABLE = 'sl_table'
SLONY_TABLE_NSP = 'tab_nspname'
SLONY_TABLE_REL = 'tab_relname'

# Request statuses
STATUS_REQUEST_QUEUED = 'requestQueued'
STATUS_REQUEST_PROCESSING = 'requestProcessing'
STATUS_DUMP_READY = 'dumpReady'
STATUS_REQUEST_COMPLETE = 'requestComplete'
STATUS_ERR_TERMINATED = 'terminated'
STATUS_ERR_INTERNAL = 'internalError'
STATUS_ERR_INVALID_ARG = 'invalidArgument'
STATUS_ERR_INVALID_REQUEST = 'invalidRequest'

def finalStuff(log):
    pass

def terminator(*args):
    raise Exception('terminated')

# BTW, the contextmanager decorator is equivalent to the with context manager.
@contextlib.contextmanager
def terminationHandler(log):
    signal.signal(signal.SIGINT, terminator)
    signal.signal(signal.SIGTERM, terminator)
    signal.signal(signal.SIGHUP, terminator)

    try:
        yield
    except KeyboardInterrupt:
        pass # Ignore
    except SystemError:
        pass # Ignore
    except Exception as exc:
        if exc.args[0] == 'terminated':
            log.write(['Termination signal handler called. Halting threads.'])
    finally:
        finalStuff(log)
        
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

class RequestSubsParams(DRMSParams):

    def __init__(self):
        super(RequestSubsParams, self).__init__()

    def get(self, name):
        val = super(RequestSubsParams, self).get(name)

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
            if self.parser:
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
        # An exception is raised if the argument does not exist.
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
            raise Exception('serverConfig', 'Unable to open or parse server-side configuration file ' + file + '.')
        for key, val in cfileDict.items():
            self.setArg(key, val)
            
class Response
    def __init__(self, **kwargs):
        if not self.status:
            raise Exception('invalidArgument', 'Derived Response class must set status property.')
        if not 'msg' in kwargs:
            raise Exception('invalidArgument', 'Message must be provided to Response constructor.')
        self.msg = msg
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
    
    def send(self):
        self.createHeader()
        self.createContent()
        
        # To send a response, you simply print to stdout.
        print(self.header)
        print('\n')
        print(self.content)
        
    def log(self):
        pass
            
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
        super(WaitResponse, self).__init__(kwargs)
        
    def createContent(self):
        if self.jsonRoot:
            self.jsonRoot['reqid'] = self.reqid
        else:
            self.jsonRoot = { 'reqid' : self.reqid }
        super(WaitResponse, self).createContent()
        
    def log(self):
        if self.log:
            msg = 'Sent wait response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.write([ msg ])

class ContinueResponse(Response):
    def __init__(self, **kwargs):
        if 'status' in kwargs:
            self.status = kwargs['status']
        else:
            self.status = STATUS_REQUEST_COMPLETE
        super(ContinueResponse, self).__init__(kwargs)
        
    def log(self):
        if self.log:
            msg = 'Sent continue response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.write([ msg ])
        
class ErrorResponse(Response):
    def __init__(self, **kwargs):
        if 'status' in kwargs:
            self.status = kwargs['status']
        else:
            self.status = STATUS_ERR_INTERNAL
        super(ErrorResponse, self).__init__(kwargs)    
    
    def log(self):
        if self.log:
            msg = 'Sent error response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
            self.log.write([ msg ])

class ListAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))

def clientIsNew(arguments, cursor, client):
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + arguments.getArg('kCfgTable') + ' WHERE ' + CFG_TABLE_NODE + " = '" + client + "'"
    
    try:
        cursor.execute(cmd)
        records = cursor.fetchall()
        if len(records) > 1:
            raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')

    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        cursor.rollback()
    
    if len(records) == 0:
        return True
    else:
        return False

def clientIsSubscribed(arguments, cursor, client, series):
    cmd = 'SELECT ' + LST_TABLE_SERIES + ' FROM ' + arguments.getArg('kLstTable') + ' WHERE ' + LST_TABLE_NODE + " = '" + client + "' AND " + LST_TABLE_SERIES + " = '" + series + "'"
    
    try:
        cursor.execute(cmd)
        records = cursor.fetchall()
        if len(records) > 1:
            raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')

    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        cursor.rollback()
    
    if len(records) == 1:
        return True
    else:
        return False

def seriesExists(cursor, series):
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
    
    cmd = 'SELECT seriesname FROM ' + ns + ".drms_series WHERE lower(seriesname) = '" + series.lower() + "'"
 
    try:
        cursor.execute(cmd)
        records = cursor.fetchall()
        if len(records) > 1:
            raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')

    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        cursor.rollback()
    
    if len(records) == 1:
        return True
    else:
        return False

def seriesIsPublished(arguments, cursor, series):
    cmd = 'SELECT ' + SLONY_TABLE_NSP + "||'.'||" + SLONY_TABLE_REL + ' AS series FROM _' + arguments.getArg('CLUSTERNAME') + '.' + SLONY_TABLE + ' WHERE ' + SLONY_TABLE_NSP + " = '" + nsp + "' AND " + SLONY_TABLE_REL + " = '" + table + "'"
    
    try:
        cursor.execute(cmd)
        records = cursor.fetchall()
        if len(records) > 1:
            raise Exception('dbResponse', 'Unexpected number of database rows returned from query: ' + cmd + '.')
    
    except psycopg2.Error as exc:
        raise Exception('dbCmd', exc.diag.message_primary)
    finally:
        cursor.rollback()
        
    if len(records) == 1:
        return True
    else:
        return False

# Main Program
if __name__ == "__main__":
    rv = RV_SUCCESS

    try:
        requestSubsParams = RequestSubsParams()
    
        # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
        if os.getenv('REQUEST_URI') or DEBUG_CGI:
            parser = CgiParser(usage='%(prog)s action=<action string> [ client=<client> ] [ requestid=<id> ] [ series=<series list> ] [ archive=<archive code> ] [ retention=<number of days> ] [ tapegroup=<group id> ] [ cfg=<configuration file> ]')
        else:
            parser = CmdlParser(usage='%(prog)s action=<action string> [ client=<client> ] [ requestid=<id> ] [ series=<series list> ] [ archive=<archive code> ] [ retention=<number of days> ] [ tapegroup=<group id> ] [ cfg=<configuration file> ]')
            
        # Required (name does not start with a dash).
        parser.add_argument('a', 'action', help='The request action (subscribe, unsubscribe, resubscribe, polldump, pollcomplete).', metavar='<action>', dest='action', required=True)

        # Optional (name starts with a dash).
        parser.add_argument('-c', '--client', help='A comma-separated list of series.', metavar='<series list>', dest='series', default='')
        parser.add_argument('-i', '--reqid', help='The id of a request previously submitted.', metavar='<request id>', dest='reqid', type=int, default=-1)
        parser.add_argument('-s', '--series', help='A comma-separated list of series.', metavar='<series list>', dest='series', action=ListAction, default=[])
        parser.add_argument('-b', '--archive', help='A comma-separated list of series.', metavar='<archive action>', dest='archive', type=int, default=0)
        parser.add_argument('-r', '--retention', help='The number of days to archive the data files.', metavar='<retention>', dest='retention', type=int, default=-1)
        parser.add_argument('-t', '--tapegroup', help='The group id of the archive tapes.', metavar='<tape group>', dest='tapegroup', type=int, default=0)
        if os.getenv('REQUEST_URI') or DEBUG_CGI:
            # Do not allow external users to choose the slony configuration file.
            parser.add_argument('-p', '--cfg', help='The configuration file that contains information needed to locate database information.', metavar='<slony configuration file>', dest='cfg', default=publistDrmsParams.get('SLONY_CONFIG'))
    
        arguments = Arguments(parser)
        
        # Copy DRMSParams arguments to arguments object.
        arguments.setArg('dbname', requestSubsParams.get('DBNAME'))
        arguments.setArg('dbuser', requestSubsParams.get('WEB_DBUSER'))
        arguments.setArg('dbhost', requestSubsParams.get('SERVER'))
        arguments.setArg('dbport', int(requestSubsParams.get('DRMSPGPORT')))
        
        # Add the server-side subscription arguments to arguments.
        arguments.setArg('slonyCfg', requestSubsParams.get('SLONY_CONFIG'))
        arguments.addFileArgs(arguments.getArg('slonyCfg'))
        
        pid = os.getpid()
        
        rsLog = Log(join(arguments.getArg('kSMcgiLogDir'), 'request-subs-log.txt'))
        
        # We need to handle signals so we can ensure that the requestor receives a decent response in the event of an error.
        with terminationHandler(rsLog):
            # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called. The first
            # db statement will open a transaction, and it will remain open until the conn.commit() or conn.rollback() call is made.
            with psycopg2.connect(database=arguments.getArg('dbname'), user=arguments.getArg('dbuser'), host=arguments.getArg('dbhost'), port=str(arguments.getArg(dbport))) as conn:
                rsLog.write([ 'Connected to database ' + arguments.getArg('dbname') + ' on ' + arguments.getArg('dbhost') + ':' + str(arguments.getArg(dbport)) + ' as user ' + arguments.getArg('dbuser') ])

                # Only one required argument.
                action = arguments.getArg('action')

                # Set to None if the argument does not exist.                
                client = arguments.get('client')
                reqid = arguments.get('reqid')
                seriesList = arguments.get('series')
                archive = arguments.get('archive')
                retention = arguments.get('retention')
                tapegroup = arguments.get('tapegroup')
                
                pendingRequest = False
    
                # The server should always send the create schema command. Put logic in the client side that decided whether it should be 
                # run or not.
                with conn.cursor() as cursor:
                    if client:
                        newSite = clientIsNew(arguments, cursor, client)
                                        
                        # Check for an existing request. If there is such a request, return a status code telling the user to poll on the request to
                        # await completion.
                        cmd = 'SELECT requestid, action, series, status, errmsg FROM ' + arguments.getArg('reqtable') + " WHERE lower(client) = '" + client + "'"
            
                        try:
                            cursor.execute(cmd) # Opens a transaction.
                            records = cursor.fetchall()
                
                            if len(records) == 1:
                                record = records[0]                
                                pendRequestID = record[0]
                                pendAction = record[1]
                                pendSeriesList = record[2]
                                pendStatus = records[3]
                                pendErrMsg = records[4]
                                pendingRequest = True
                            elif len(records) > 1
                                raise Exception('dbResponse', 'There is more than one pending request for client ' + client + ' (at most there should be one).')
                        except psycopg2.Error as exc:
                            raise Exception('dbCmd', exc.diag.message_primary)
                        finally:
                            cursor.rollback() # Closes the transaction.
                    else:
                        newSite = False
                        pendRequestID = None
                        pendAction = None
                        pendSeriesList = None
                        pendStatus = None
                        pendErrMsg = None
                        pendingRequest = False

                    if action.lower() == 'subscribe' or action.lower() == 'resubscribe':
                        if pendingRequest:
                            raise Exception('invalidRequest', 'You cannot ' + action.lower() + ' to a series at this time - a ' + pendAction + ' request for ' + pendSeriesList.join(',') + ' is pending. Please wait for that request to complete.')
                        if client is None or len(client) < 1:
                            raise Exception('invalidArgument', 'You must provide a client name.')
                        if newSite and action.lower() != 'subscribe':
                            raise Exception('invalidArgument', 'You have never subscribed to a series before. You cannot make a ' + action.lower() + ' request.')

                        # Allow the user to subscribe/resubscribe to a single series at a time.
                        if seriesList is None or len(seriesList) != 1:
                            raise Exception('invalidArgument', 'You must specify a single series to which you would like to ' + action.lower() + '.')
                            
                        series = seriesList[0]
                            
                        if action.lower() == 'subscribe':
                            if archive is None or (archive != 0 and archive != -1 and archive != 1):
                                raise Exception('invalidArgument', 'You must provide an integer value of -1, 0, or 1 for the archive argument.')
                            if retention is None or retention < 0:
                                raise Exception('invalidArgument', 'You must provide an integer value greater than or equal to 0 for the retention argument.')
                            if tapegroup is None or tapegroup < 0:
                                raise Exception('invalidArgument', 'You must provide an integer value greater than or equal to 0 for the tapegroup argument.')

                        # Check for existing subscription to series.
                        subscribed = clientIsSubscribed(arguments, cursor, client, series)
                        if action.lower() == 'subscribe':
                            if subscribed:
                                raise Exception('invalidArgument', 'Cannot subscribe to ' + series + '; client ' + client ' is already subscribed to this series.')
                        else:
                            if not subscribed:
                                raise Exception('invalidArgument', 'Cannot re-subscribe to ' + series + '; client ' + client ' is not already subscribed to this series.')

                        if not seriesExists(cursor, series):
                            raise Exception('invalidArgument', 'Cannot ' + action.lower() + ' to ' + series + '; it does not exist.')
                        if not seriesIsublished(arguments, cursor, series):
                            raise Exception('invalidArgument', 'Cannot ' + action.lower() + ' to ' + series + '; it is not published.')

                        try:
                            # Insert a pending row into the su_production.requests table. Ensure that the series name comprises lower-case letters.
                            cmd = "SELECT nextval('" + arguments.getArg('kReqTable') + '_seq' + "')"
                            cursor.execute(cmd)
                            records = cursor.fetchall()
                            if len(records) != 1 or len(records[0]) != 1:
                                raise Exception('dbCmd', 'Unexpected db-query results.')
                                
                            reqid = records[0][0] # integer
                            
                            cmd = 'INSERT INTO ' + arguments.getArg('kReqTable') + "(client, requestid, starttime, action, series, archive, retention, tapegroup, status) VALUES('" + client + "', " + str(reqid) + ", '" + datetime.now().strftime('%Y-%m-%d %T') + "', '" + action.lower() + "', '" + seriesList.join(',') + "', " + str(archive) + ", " + str(retention) + ", " + str(tapegroup) + ", 'N'"
                            cursor.execute(cmd)
                            
                        except psycopg2.Error as exc:
                            raise Exception('dbCmd', exc.diag.message_primary)
                        finally:                        
                            cursor.commit()
                        
                        if action.lower() == 'subscribe':
                            respMsg = 'Request for subscription to series ' + series + ' is queued. Poll for completion with a polldump request. Please sleep between iterations when looping over this request.'
                        else:
                            respMsg = 'Request for re-subscription to series ' + series + ' is queued. Poll for completion with a polldump request. Please sleep between iterations when looping over this request.'
                            
                        # Send a 'wait' response.
                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg, reqid=reqid)
                        resp.log()
                        resp.send()
                    elif action.lower() = 'unsubscribe':
                        if pendingRequest:
                            raise Exception('invalidRequest', 'You cannot un-subscribe from series at this time - a ' + pendAction + ' request for ' + pendSeriesList.join(',') + ' is pending. Please wait for that request to complete.')
                        if client is None or len(client) < 1:
                            raise Exception('invalidArgument', 'You must provide a client name.')
                        if newSite:
                            raise Exception('invalidArgument', 'You have never subscribed to a series before. You cannot make a ' + action.lower() + ' request.')

                        # The user can unsubscribe from multiple series.
                        if seriesList is None or len(seriesList) < 1:
                            raise Exception('invalidArgument', 'Please provide a list of series from which you would like to unsubscribe.')
                        
                        try:
                            cmd = "SELECT nextval('" + arguments.getArg('kReqTable') + '_seq' + "')"
                            cursor.execute(cmd)
                            records = cursor.fetchall()
                            if len(records) != 1 or len(records[0]) != 1:
                                raise Exception('dbCmd', 'Unexpected db-query results.')
                                
                            reqid = records[0][0] # integer
                            
                            cmd = 'INSERT INTO ' + arguments.getArg('kReqTable') + "(client, requestid, starttime, action, series, status) VALUES('" + client + "', " + str(reqid) + ", '" + datetime.now().strftime('%Y-%m-%d %T') + "', '" + action.lower() + "', '" + seriesList.join(',') +  "', 'N'"
                            cursor.execute(cmd)
                        except psycopg2.Error as exc:
                            raise Exception('dbCmd', exc.diag.message_primary)
                        finally:                        
                            cursor.commit()
                            
                        respMsg = 'Request for un-subscription from series ' + seriesList.join(',') + ' is queued. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.'
                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg)
                        resp.log()
                        resp.send()
                        
                    elif action.lower() = 'polldump' or action.lower() == 'pollcomplete':
                        if reqid is None:
                            raise Exception('invalidArgument', 'Please provide a request ID.')
                        if not pendRequestID or pendRequestID != reqid:
                            raise Exception('invalidArgument', 'The request ID provided (' + str(reqid) + ') does not match the ID of the request currently pending (' + str(pendRequestID) + ').')
                        if not pendingRequest:
                            if action.lower() = 'polldump':
                                raise Exception('invalidRequest', 'You cannot poll for dump-completion of request ' + str(reqid) + '. That request is not pending.')
                            else:
                                raise Exception('invalidRequest', 'You cannot poll for completion of request ' + str(reqid) + '. That request is not pending.')
     
                        if action.lower() == 'pollcomplete':
                            # If this is a pollcomplete request, then we need to tell manage-subs.py that the client
                            # has ingested the dump file and is awaiting server clean-up. Update the request status to 'I'.
                            try:
                                cmd = 'UPDATE ' + arguments.getArg('kReqTable') + " SET status = 'I' WHERE requestid = " + str(reqid)
                                cursor.execute(cmd)
                            except psycopg2.Error as exc:
                                raise Exception('dbCmd', exc.diag.message_primary)
                            finally:                        
                                cursor.commit()                        

                        # Now, we have to poll on the pending request, waiting for manage-subs.py to reply with:
                        #   D - dump complete
                        #   C - complete (clean-up is done)
                        #   E - error

                        try:                            
                            cmd = 'SELECT status, errmsg FROM ' + arguments.getArg('kReqTable') + ' WHERE requestid = ' + reqid
                            cursor.execute(cmd)
                            records = cursor.fetchall()
                
                            if len(records) == 1:
                                record = records[0]                
                                pendStatus = record[0]
                                pendErrMsg = record[1]
                            elif len(records) > 1
                                raise Exception('dbResponse', 'There is more than one pending request with a request ID of ' + str(reqid) + '.')
                                
                            if pendStatus.upper() == 'N' or pendStatus.upper() == 'P' or pendStatus.upper() == 'D':
                                if action.lower() != 'polldump':
                                    raise Exception('invalidArgument', 'You must send a polldump request to continue with the subscription process.')
                                if pendStatus.upper() == 'D':
                                    # Send a 'continue' response.
                                    if pendAction.lower() == 'subscribe' or pendAction.lower() == 'resubscribe':
                                        resp = ContinueResponse(log=rsLog, status=STATUS_DUMP_READY, msg='The SQL dump file is ready for ingestion.')
                                    elif pendAction.lower() == 'unsubscribe':
                                        raise Exception('manage-subs', 'Unexpected request status(D)/action(unsubscribe) combination for request ' + str(reqid) + '.')
                                    else:
                                        raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')
                                elif pendStatus.upper() == 'N':
                                    # Send a 'wait' response.
                                    if pendAction.lower() == 'subscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='Request for subscription to series ' + pendSeriesList.join(',') + ' is queued. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.')
                                    elif pendAction.lower() == 'resubscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='Request for re-subscription to series ' + pendSeriesList.join(',') + ' is queued. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.')
                                    elif pendAction.lower() == 'unsubscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='Request for un-subscription from series ' + pendSeriesList.join(',') + ' is queued. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.')
                                    else:
                                        raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')
                                else:
                                    # Send a 'wait' response.
                                    if pendAction.lower() == 'subscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='Request for subscription to series ' + pendSeriesList.join(',') + ' is being processed. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.')
                                    elif pendAction.lower() == 'resubscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='Request for re-subscription to series ' + pendSeriesList.join(',') + ' is being processed. Poll for dump file with a polldump request. Please sleep between iterations when looping over this request.')
                                    elif pendAction.lower() == 'unsubscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='Request for un-subscription from series ' + pendSeriesList.join(',') + ' is being processed. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.'
                                    else:
                                        raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')

                                resp.log() 
                                resp.send()
                            elif pendStatus.upper() == 'I' or pendStatus.upper() == 'C':
                                if action.lower() != 'pollcomplete':
                                    raise Exception('invalidArgument', 'You must send a pollcomplete request to continue with the subscription process.')
                                if pendStatus.upper() == 'C':
                                    # Send a 'continue' response.
                                    resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_COMPLETE, msg='Your ' + pendAction.lower() + ' request has successfully completed.')
                                elif pendStatus.upper() == 'I':
                                    # Send a 'wait' response.
                                    if pendAction.lower() == 'unsubscribe':
                                        # Status I is not valid for an unsubscribe request.
                                        raise Exception('manage-subs', 'Unexpected request status(I)/action(unsubscribe) combination for request ' + str(reqid) + '.')
                                    elif pendAction.lower() == 'subscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='Request for subscription to series ' + pendSeriesList.join(',') + ' is being finalized. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.')
                                    elif pendAction.lower() == 'resubscribe':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='Request for re-subscription to series ' + pendSeriesList.join(',') + ' is being finalized. Poll for completion with a pollcomplete request. Please sleep between iterations when looping over this request.')
                                    else:
                                        raise Exception('manage-subs', "Unknown request action '" + pendAction.lower() + "' for request " + str(reqid) + '.')

                                resp.log()
                                resp.send()
                            elif pendStatus.upper() == 'E':
                                raise Exception('manage-subs', pendErrMsg)
                            else:                                
                                raise Exception('manage-subs', 'Unexcepted status code: ' + pendStatus.upper() + '.')

                        except psycopg2.Error as exc:
                            raise Exception('dbCmd', exc.diag.message_primary)
                        finally:                        
                            cursor.rollback()
                    else:
                        # Unrecognized action.
                        raise Exception('invalidArgument', 'Action ' + "'" + action + "'" + ' is not recognized.')
    except Exception as exc:
        if len(exc.args) == 2:
            eType, eMsg = exc.args
            resp = ErrorResponse(log=rsLog, msg=Emsg)
            if eType == 'terminated':
                resp.setStatus(STATUS_ERR_TERMINATED)
            elif eType == 'drmsParams':
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
            else:
                raise
            
            resp.log()
            resp.send()
        else:
            raise
            
    sys.exit(0)
