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
import socket
import select
import inspect
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
STATUS_REQUEST_DUMP_READY = 'dumpReady'
STATUS_REQUEST_FINALIZING = 'requestFinalizing'
STATUS_REQUEST_COMPLETE = 'requestComplete'
STATUS_ERR_TERMINATED = 'terminated'
STATUS_ERR_INTERNAL = 'internalError'
STATUS_ERR_INVALID_ARGUMENT = 'invalidArgument'
STATUS_ERR_INVALID_REQUEST = 'invalidRequest'
STATUS_ERR_FAILURE = 'requestFailed'
STATUS_ERR_SERVER_TIMEOUT = 'requestTimedout'
STATUS_ERR_SERVER_ERROR = 'serverError'
STATUS_ERR_UNEXPECTED_DB_RESPONSE = 'unexpectedDbResponse'
STATUS_ERR_DB_ERROR = 'dbError'
STATUS_ERR_CANNOT_SEND_MESSAGE = 'cannotSendMessage'
STATUS_ERR_CANNOT_RECEIVE_MESSAGE = 'cannotReceiveMessage'
STATUS_ERR_INVALID_SERVER_RESPONSE = 'invalidServerResponse'
STATUS_ERR_CANNOT_CONNECT_TO_SERVER = 'cannotConnectToServer'

REQTYPE_GET_PENDING = 'getPending'
REQTYPE_ERROR = 'error'
REQTYPE_SET_STATUS = 'setStatus'

        
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
        
    def __prependFrameInfo(self, msg):
        frame, fileName, lineNo, fxn, context, index = inspect.stack()[2]
        return os.path.basename(fileName) + ':' + str(lineNo) + ': ' + msg

    def writeDebug(self, text):
        if self.log:
            for line in text:
                self.log.debug(self.__prependFrameInfo(line))
            self.fileHandler.flush()
            
    def writeInfo(self, text):
        if self.log:
            for line in text:
                self.log.info(self.__prependFrameInfo(line))
            self.fileHandler.flush()
    
    def writeWarning(self, text):
        if self.log:
            for line in text:
                self.log.warning(self.__prependFrameInfo(line))
            self.fileHandler.flush()
    
    def writeError(self, text):
        if self.log:
            for line in text:
                self.log.error(self.__prependFrameInfo(line))
            self.fileHandler.flush()
            
    def writeCritical(self, text):
        if self.log:
            for line in text:
                self.log.critical(self.__prependFrameInfo(line))
            self.fileHandler.flush()

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


class RSException(Exception):
    def __init__(self, msg):
        super(RSException, self).__init__(msg)

class InvalidArgument(RSException):
    status = STATUS_ERR_INVALID_ARGUMENT
    def __init__(self, msg):
        super(InvalidArgument, self).__init__(msg)

class ServerSideTimeout(RSException):
    status = STATUS_ERR_SERVER_TIMEOUT
    def __init__(self, msg):
        super(ServerSideTimeout, self).__init__(msg)

class ServerSideError(RSException):
    status = STATUS_ERR_SERVER_ERROR
    def __init__(self, msg):
        super(ServerSideError, self).__init__(msg)

class UnexpectedDbResponse(RSException):
    status = STATUS_ERR_UNEXPECTED_DB_RESPONSE
    def __init__(self, msg):
        super(UnexpectedDbResponse, self).__init__(msg)

class DbError(RSException):
    status = STATUS_ERR_DB_ERROR
    def __init__(self, msg):
        super(DbError, self).__init__(msg)

class CannotSendMessage(RSException):
    status = STATUS_ERR_CANNOT_SEND_MESSAGE
    def __init__(self, msg):
        super(CannotSendMessage, self).__init__(msg)

class CannotReceiveMsg(RSException):
    status = STATUS_ERR_CANNOT_RECEIVE_MESSAGE
    def __init__(self, msg):
        super(CannotReceiveMsg, self).__init__(msg)

class InvalidRequest(RSException):
    status = STATUS_ERR_INVALID_REQUEST
    def __init__(self, msg):
        super(InvalidRequest, self).__init__(msg)

class InvalidServerResponse(RSException):
    status = STATUS_ERR_INVALID_SERVER_RESPONSE
    def __init__(self, msg):
        super(InvalidServerResponse, self).__init__(msg)
        
class CannotConnectToServer(RSException):
    status = STATUS_ERR_CANNOT_CONNECT_TO_SERVER
    def __init__(self, msg):
        super(CannotConnectToServer, self).__init__(msg)


class ServerRequest(object):
    MSGLEN_NUMBYTES = 8 # this is the hex-text version of the number of bytes in the response message
    MAX_MSG_BUFSIZE = 4096 # don't receive more than this in one call!
    
    def __init__(self, **kwargs):
        # these assignments will validate that arguments exist
        self.client = kwargs['client']
        self.timeout = kwargs['timeout']
        self.acquirereqlock = kwargs['acquirereqlock']
        self.log = kwargs['log']
        self.kwargs = kwargs
        
    def getReqDict(self):
        return { 'client': self.client, 'timeout': self.timeout, 'acquirereqlock': self.acquirereqlock }
    
    # msg is a string
    def jsonizeMsg(self, msg):
        return json.dumps(msg)
    
    # jsonContent is a string
    def unjsonizeJson(self, jsonContent):
        return json.loads(jsonContent)
        
    def send(self):
        reqdict = self.getReqDict() # dictionary
        self.log.writeDebug([ 'sending request to server: ' + str(reqdict) ])
        jsonContent = self.jsonizeMsg(reqdict) # json
        self.connection.sendMsg(jsonContent)
    
    def receiveResponse(self):
        bytesReceived = self.connection.receiveMsg() # bytes
        strReceived = bytesReceived.decode() # json
        response = self.unjsonizeJson(strReceived) # dict
        self.log.writeDebug([ 'response from server: ' + str(response) ])
        responseObj = self.getRspObj(response)
        responseObj.validate(self) # pass request to response validate method


class GetPendingRequest(ServerRequest):
    def __init__(self, **kwargs):
        super(GetPendingRequest, self).__init__(**kwargs)
        self.reqidCGI = kwargs['reqid'] # the CGI arg requestid, for validation

    # request is:
    #   { 'reqtype': 'getPending',
    #     'timeout': 20, # the acquire-locks timeout in seconds
    #     'client': 'nso',
    #     'acquirereqlock': true
    #   }
    def getReqDict(self):
        reqDict = super(GetPendingRequest, self).getReqDict()
        reqDict['reqtype'] = REQTYPE_GET_PENDING
        return reqDict
    
    # response is:
    #   { 'serverstatus': { 'code': 'ok', 'errmsg': '' } # ok, error, timeout (acquiring locks)
    #     'requests': # if no pending requests, then the list is empty
    #        [
    #           { 'requestid': 230,
    #             'action': 'subscribe',
    #             'series': [ 'hmi.M_45s', 'aia.lev1' ],
    #             'archive': 1,
    #             'retention': 90,
    #             'tapegroup': 7,
    #             'subuser': 'slony',
    #             'status': 'I',
    #             'errmsg': 'blah'
    #           }
    #        ]
    #   }
    def getRspObj(self, responseDict):
        return GetPendingServerResponse(elements=responseDict)


# request that server sets the request status to E
class ErrorRequest(ServerRequest):
    def __init__(self, **kwargs):
        self.requestid = kwargs['reqid']
        self.errmsg = kwargs['errmsg']
        super(ErrorRequest, self).__init__(**kwargs)
    
    # request is:
    #   { 'reqtype': 'error',
    #     'timeout': 20, # the acquire-locks timeout in seconds
    #     'requestid': 23,
    #     'client': 'nso',
    #     'acquirereqlock': true,
    #     'ermsg': 'generic problem in the subscription client'
    #   }
    def getReqDict(self):
        reqDict = super(ErrorRequest, self).getReqDict()
        reqDict['reqtype'] = REQTYPE_ERROR
        reqDict['requestid'] = self.requestid
        reqDict['errmsg'] = self.errmsg
        return reqDict
        
    # response is:
    #  { 'serverstatus': { 'code': 'ok', 'errmsg': '' }, # ok, error, timeout (acquiring locks)
    #  }
    def getRspObj(self, responseDict):
        return ErrorServerResponse(elements=responseDict)


class SetStatusRequest(ServerRequest):
    def __init__(self, **kwargs):
        self.status = kwargs['status']
        if 'errmsg' in kwargs:
            self.errmsg = kwargs['errmsg']
        super(SetStatusRequest, self).__init__(**kwargs)
    
    # request is:
    #   { 'reqtype': 'setStatus',
    #     'timeout': 20, # the acquire-locks timeout in seconds
    #     'requestid': 25,
    #     'client': 'nso',
    #     'acquirereqlock': true,
    #     'status': 'A'
    #     'errmsg': 'whatever' # optional
    #   }
    def getReqDict(self):        
        reqDict = super(SetStatusRequest, self).getReqDict()
        reqDict['reqtype'] = REQTYPE_SET_STATUS
        reqDict['requestid'] = self.requestid
        reqDict['status'] = self.status
        if hasattr(self, 'errmsg'):
            reqDict['errmsg'] = self.errmsg
        return reqDict

    # response is:
    #  { 'serverstatus': { 'code': 'ok', 'errmsg': '' }, # ok, error, timeout (acquiring locks)
    #    'requestid': 252 # or -1 if the request cannot be found
    #  }
    def getRspObj(self, responseDict):
        return SetStatusServerResponse(elements=responseDict)


class ServerResponse(object):
    def __init__(self, **kwargs):
        elements = kwargs['elements']
        for key,val in elements.items():
            if type(val) is dict:
                nested = ServerResponse(elements=val)
                self.__dict__.update({ key: nested })
            else:
                self.__dict__.update({ key: val })
                
    def validate(self):
        # check for server-side timeout
        if self.serverstatus.code.lower() == 'timeout':
            raise ServerSideTimeout(self.serverstatus.errmsg)
            
        # check for error
        if self.serverstatus.code.lower() == 'error':
            raise ServerSideError(self.serverstatus.errmsg)


class GetPendingServerResponse(ServerResponse):
    def __init__(self, **kwargs):
        super(GetPendingServerResponse, self).__init__(**kwargs)

    def validate(self, request):
        super(GetPendingServerResponse, self).validate()
        
        if len(self.requests) == 0:
            request.log.writeInfo([ 'client ' + request.connection.client + ' does NOT have a pending request' ])
        elif len(self.requests) > 1:
            raise UnexpectedDbResponse('client ' + request.connection.client + ' has multiple pending requests (there should be one at most)')
        else:
            request.log.writeInfo([ 'client ' + request.connection.client + ' has a pending request:' + ' id - ' + str(self.requests[0].requestid) + ', action - ' + self.requests[0].action + ', series - ' + ','.join(self.requests[0].series) + ', status - ' + self.requests[0].status + ', errMsg - ' + str(self.requests[0].errmsg if self.requests[0].errmsg else "''") ])            
    
            if request.reqidCGI is not None:
                if self.requests[0].requestid != request.reqidCGI:
                    raise InvalidArgument("the ID of the client's pending request (" + str(self.requestid) + ") does not match the ID specified for the current action (" + str(request.reqid) + ")")


class ErrorServerResponse(ServerResponse):
    def __init__(self, **kwargs):
        super(ErrorServerResponse, self).__init__(**kwargs)

    def validate(self, request):
        super(ErrorServerResponse, self).validate()


class SetStatusServerResponse(ServerResponse):
    def __init__(self, **kwargs):
        super(SetStatusServerResponse, self).__init__(**kwargs)

    def validate(self, request):
        super(SetStatusServerResponse, self).validate()

class Response(object):
    def __init__(self, **kwargs):
        if not self.status:
            raise InvalidArgument('derived Response class must set status property')
        if not 'msg' in kwargs:
            raise InvalidArgument('message must be provided to Response constructor')
        self.msg = kwargs['msg']
        if not 'client' in kwargs:
            raise IinvalidArgument('client name must be provided to Response constructor')
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
            raise InvalidArgument('reqid is required for ResumeResponse constructor')
            
        if 'reqtype' in kwargs:
            self.reqtype = kwargs['reqtype']
        else:
            raise InvalidArgument('reqtype is required for ResumeResponse constructor')
            
        if 'series' in kwargs:
            self.series = kwargs['series']
        else:
            raise InvalidArgument('series is required for ResumeResponse constructor')
        
        if 'archive' in kwargs:
            self.archive = kwargs['archive']
        else:
            raise InvalidArgument('archive is required for ResumeResponse constructor')
        
        if 'retention' in kwargs:
            self.retention = kwargs['retention']
        else:
            raise InvalidArgument('retention is required for ResumeResponse constructor')
            
        if 'tapegroup' in kwargs:
            self.tapegroup = kwargs['tapegroup']
        else:
            raise InvalidArgument('tapegroup is required for ResumeResponse constructor')
            
        if 'subuser' in kwargs:
            self.subuser = kwargs['subuser']
        else:
            raise InvalidArgument('subuser is required for ResumeResponse constructor')
            
        if 'resumeaction' in kwargs:
            self.resumeaction = kwargs['resumeaction']
        else:
            raise InvalidArgument('resumeaction is required for ResumeResponse constructor')
        
        if 'resumestatus' in kwargs:
            self.resumestatus = kwargs['resumestatus']
        else:
            raise InvalidArgument('resumestatus is required for ResumeResponse constructor')
        
        super(ResumeResponse, self).__init__(**kwargs)
        
    def createContent(self):
        if self.jsonRoot:
            self.jsonRoot['reqid'] = self.reqid
            self.jsonRoot['reqtype'] = self.reqtype
            self.jsonRoot['series'] = self.series
            self.jsonRoot['archive'] = self.archive
            self.jsonRoot['retention'] = self.retention
            self.jsonRoot['tapegroup'] = self.tapegroup
            self.jsonRoot['subuser'] = self.subuser
            self.jsonRoot['resumeaction'] = self.resumeaction
            self.jsonRoot['resumestatus'] = self.resumestatus
        else:
            self.jsonRoot = { 'reqid' : self.reqid, 'reqtype' : self.reqtype, 'series' : self.series, 'archive' : self.archive, 'retention' : self.retention, 'tapegroup' : self.tapegroup, 'subuser' : self.subuser, 'resumeaction' : self.resumeaction, 'resumestatus' : self.resumestatus }
        super(ResumeResponse, self).createContent()
        
    def logMsg(self):
        if self.log:
            msg = 'sent resume response to ' + self.client + ' (' + self.status + ', ' + self.msg + ').'
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
            raise InvalidArgument('reqid is required for WaitResponse constructor')
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
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + arguments.getArg('kCfgTable') + ' WHERE lower(' + CFG_TABLE_NODE + ") = '" + client.lower() + "'"
    log.writeDebug([ 'Checking client-new status on master: ' + cmd + '.' ])

    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise UnexpectedDbResponse('unexpected number of database rows returned from query: ' + cmd)
    except psycopg2.Error as exc:
        raise DbError(exc.diag.message_primary)
    finally:
        conn.rollback() # closes the cursor
    
    if len(records) == 0:
        return True
    else:
        return False
        
class Connection(object):
    '''
        client connection
    '''
    def __init__(self, **kwargs):
        self.client = kwargs['client']
        self.serverhost = kwargs['host']
        self.serverport = kwargs['port']
        self.sock = None
        self.log = kwargs['log']

        if self.client is None or len(self.client) == 0:
            raise InvalidArgument('attempting to retrieve pending request information, but client was not provided')
        
        self.connect()

    def connect(self):
        #for info in socket.getaddrinfo(self.serverhost, self.serverport, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, flags=0):
        for info in socket.getaddrinfo(self.serverhost, self.serverport, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, flags=0):
            af, socktype, proto, canonname, sa = info
            self.log.writeDebug([ 'attempting to connect to subscription manager: host ' + sa[0] + ', port ' + str(sa[1]) ])
            try:
                self.sock = socket.socket(af, socktype, proto)
            except OSError:
                self.sock = None
                self.log.writeInfo([ 'could not create client socket' ])
                continue
            
            try:
                self.sock.connect(sa)
                self.log.writeInfo([ 'successfully connected to subscription manager' ])
                break
            except OSError:
                import traceback
                
                self.sock.close()
                self.sock = None
                self.log.writeInfo([ 'could not connect to subscription manager: ' + traceback.format_exc(1) ])
                continue
                
        if self.sock is None:
            raise CannotConnectToServer('unable to connect to subscription-manager service')
            
    def getID(self):
        if hasattr(self, 'peerName') and self.peerName and len(self.peerName) > 0:
            return self.peerName
        else:
            # will raise if the socket has been closed by client
            self.peerName = str(self.sock.getpeername())
        
        return self.peerName
        
    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        
    def sendRequest(self, request):
        # send request over socket
        setattr(request, 'connection', self)
        request.send()
        return request.receiveResponse()

    # msg is a string
    def sendMsg(self, msg):
        msgBytes = bytes(msg, 'UTF-8')
    
        # First send the length of the message.
        bytesSentTotal = 0
        numBytesMessage = '{:08x}'.format(len(msgBytes))
        
        # send the size of the message
        while bytesSentTotal < ServerRequest.MSGLEN_NUMBYTES:
            bytesSent = self.sock.send(bytearray(numBytesMessage[bytesSentTotal:], 'UTF-8'))
            if not bytesSent:
                raise CannotSendMessage('socket broken - cannot send message-length data to server')
            bytesSentTotal += bytesSent
        
        # then send the actual message
        bytesSentTotal = 0
        while bytesSentTotal < len(msgBytes):
            bytesSent = self.sock.send(msgBytes[bytesSentTotal:])
            if not bytesSent:
                raise CannotSendMessage('socket broken - cannot send message data to server')
            bytesSentTotal += bytesSent

        self.log.writeDebug([ self.getID() + ' - sent ' + str(bytesSentTotal) + ' bytes request to server' ])

    # returns a bytes object
    def receiveMsg(self):
        # first, receive length of message
        allTextReceived = b''
        bytesReceivedTotal = 0

        # time-out time
        timeStart = datetime.now()
        timeOutTime = timeStart + timedelta(seconds=15) # the entire request bails out

        while bytesReceivedTotal < ServerRequest.MSGLEN_NUMBYTES:
            if datetime.now() > timeOutTime:
                raise CannotReceiveMsg('timeout waiting for response from server')

            toRead = [ self.sock ]
            readable, writeable, problematic = select.select(toRead, [], [], 0) # does not block so we can check for a timeout

            if len(readable) > 0:
                textReceived = self.sock.recv(min(ServerRequest.MSGLEN_NUMBYTES - bytesReceivedTotal, ServerRequest.MAX_MSG_BUFSIZE))
                if textReceived == b'':
                    raise CannotReceiveMsg('socket broken - cannot receive message-length data from server')
                allTextReceived += textReceived
                bytesReceivedTotal += len(textReceived)
            else:
                # a recv() would block
                self.log.writeDebug([ 'waiting for server to send response' ])
                time.sleep(0.2)
            
        # Convert hex string to number.
        numBytesMessage = int(allTextReceived.decode('UTF-8'), 16)
        
        # Then receive the message.
        allTextReceived = b''
        bytesReceivedTotal = 0

        while bytesReceivedTotal < numBytesMessage:
            if datetime.now() > timeOutTime:
                raise CannotReceiveMsg('timeout waiting for response from server')
                
            toRead = [ self.sock ]
            readable, writeable, problematic = select.select(toRead, [], [], 0) # does not block

            if len(readable) > 0:            
                textReceived = self.sock.recv(min(numBytesMessage - bytesReceivedTotal, ServerRequest.MAX_MSG_BUFSIZE))
                if textReceived == b'':
                    raise CannotReceiveMsg('socket broken - cannot receive message data from server')
                allTextReceived += textReceived
                bytesReceivedTotal += len(textReceived)
            else:
                # a recv() would block
                self.log.writeDebug([ 'waiting for server to send response' ])
                time.sleep(0.2)

        self.log.writeDebug([ self.getID() + ' - received ' + str(bytesReceivedTotal) + ' bytes from server' ])
        
        # Return a bytes object (not a string). The unjsonize function will need a str object for input.
        return allTextReceived

def clientIsSubscribed(arguments, conn, client, series):
    # Master database.
    cmd = 'SELECT ' + LST_TABLE_SERIES + ' FROM ' + arguments.getArg('kLstTable') + ' WHERE lower(' + LST_TABLE_NODE + ") = '" + client.lower() + "' AND lower(" + LST_TABLE_SERIES + ") = '" + series.lower() + "'"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise UnexpectedDbResponse('unexpected number of database rows returned from query: ' + cmd)
    except psycopg2.Error as exc:
        raise DbError(exc.diag.message_primary)
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
        raise InvalidArgument('not a valid DRMS series name: ' + series)
    
    # Slave database.
    cmd = 'SELECT seriesname FROM ' + ns + ".drms_series WHERE lower(seriesname) = '" + series.lower() + "'"
 
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise UnexpectedDbResponse('unexpected number of database rows returned from query: ' + cmd)
    except psycopg2.Error as exc:
        raise DbError(exc.diag.message_primary)
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
        nsp = matchObj.group(1).lower() # make the following comparisons case-insensitive
        table = matchObj.group(2).lower() # make the following comparisons case-insensitive
    else:
        raise InvalidArgument('not a valid DRMS series name: ' + series)

    # Slave database.
    cmd = 'SELECT ' + SLONY_TABLE_NSP + "||'.'||" + SLONY_TABLE_REL + ' AS series FROM _' + arguments.getArg('CLUSTERNAME') + '.' + SLONY_TABLE + ' WHERE ' + SLONY_TABLE_NSP + " = '" + nsp + "' AND " + SLONY_TABLE_REL + " = '" + table + "'"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) > 1:
                raise UnexpectedDbResponse('unexpected number of database rows returned from query: ' + cmd)
    except psycopg2.Error as exc:
        raise DbError(exc.diag.message_primary)
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
            raise InvalidArgument("Argument value must be 'True' or 'False'")

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

# no need to use the subscription manager to add a new request since the manager cannot possibly be working on a request
# that has not been created
def insertRequest(conn, log, dbTable, **kwargs):
    # Insert a pending row into the su_production.slonyreq table. Ensure that the series name comprises lower-case letters.
    client = kwargs['client']
    action = kwargs['action'].lower()
    seriesStr = kwargs['series']
    
    if 'archive' in kwargs and kwargs['archive'] and kwargs['archive']:
        archive = str(kwargs['archive'])
    else:
        archive = '0'
    if 'retention' in kwargs and kwargs['retention'] and kwargs['retention']:
        retention = str(kwargs['retention'])
    else:
        retention = '7'
    if 'tapegroup' in kwargs and kwargs['tapegroup'] and kwargs['tapegroup']:
        tapegroup = str(kwargs['tapegroup'])
    else:
        tapegroup = '0'
    if 'subuser' in kwargs and kwargs['subuser'] and len(kwargs['subuser']) > 0:
        subuser = kwargs['subuser']
    else:
        subuser = 'slony'
        
    reqid = -1
    
    try:
        with conn.cursor() as cursor:
             # Slave database.
            cmd = "SELECT nextval('" + dbTable + '_seq' + "')"
            cursor.execute(cmd)
            records = cursor.fetchall()
            if len(records) != 1 or len(records[0]) != 1:
                raise UnexpectedDbResponse('unexpected db-query results')
        
            reqid = records[0][0] # integer

            guts = str(reqid) + ", '" + client + "', '" + datetime.now().strftime('%Y-%m-%d %T') + "', '" + action + "', '" + seriesStr + "', " + archive + ", " + retention + ", " + tapegroup + ", '" + subuser + "', 'N'"
            log.writeInfo([ 'Inserting new request into db table ' + dbTable + ': (' + guts + ')' ])
    
            # slave database
            cmd = 'INSERT INTO ' + dbTable + '(requestid, client, starttime, action, series, archive, retention, tapegroup, subuser, status) VALUES(' + guts + ')'
            cursor.execute(cmd)
        conn.commit() # commit only if there are no errors.
    except psycopg2.Error as exc:
        raise DbError(exc.diag.message_primary)
        
    return reqid

# Main Program
if __name__ == "__main__":
    client = 'unknown'
    
    try:
        requestSubsParams = RequestSubsParams()
        arguments = Arguments()
    
        # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
        if os.getenv('REQUEST_URI') or DEBUG_CGI:
            parser = CgiParser(usage='%(prog)s action=<action string> [ client=<client> ] [ requestid=<id> ] [ series=<series list> ] [ archive=<archive code> ] [ retention=<number of days> ] [ tapegroup=<group id> ] [ subuser=<subscription client DB account>] [ cfg=<configuration file> ]')
        else:
            parser = CmdlParser(usage='%(prog)s action=<action string> [ client=<client> ] [ requestid=<id> ] [ series=<series list> ] [ archive=<archive code> ] [ retention=<number of days> ] [ tapegroup=<group id> ] [ subuser=<subscription client DB account>] [ cfg=<configuration file> ]')
            
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
        parser.add_argument('-u', '--subuser', 'subuser', help='The DB account the subscription client uses.', metavar='<subscripton client DB user>', dest='subuser', default=argparse.SUPPRESS)
        parser.add_argument('-l', '--loglevel', 'loglevel', help='Specifies the amount of logging to perform. In increasing order: critical, error, warning, info, debug', dest='loglevel', action=LogLevelAction, default=logging.DEBUG)
    
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
            raise InvalidArgument('to run this program, you must supply two required arguments: ' + "'action' and 'client'")
            
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

                try:
                    rsLog.writeInfo([ 'received a ' + action + ' request from client ' + client ])
                
                    # Set to None if the argument does not exist.                
                    newSite = arguments.get('newsite')
                    reqid = arguments.get('reqid')
                    seriesList = arguments.get('series')
                    archive = arguments.get('archive')
                    retention = arguments.get('retention')
                    tapegroup = arguments.get('tapegroup')
                    subuser = arguments.get('subuser')
                    serverhost = arguments.getArg('SM_SERVER')
                    serverport = arguments.getArg('SM_SERVER_PORT')
            
                    pendingRequest = False    
                    # The server should always send the create schema command. Put logic in the client side that decided whether it should be 
                    # run or not.
                    newSiteServer = clientIsNew(arguments, connMaster, client, rsLog)
                    if newSite is not None and newSiteServer!= newSite:
                        raise InvalidArgument('the newsite status at the client does not match the newsite status at the server')
                    newSite = newSiteServer
                    rsLog.writeInfo([ 'newSite is ' + str(newSite) ])

                    if action.lower() == 'continue':
                        # make socket connection to manage-subs.py
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rslog)
                        try:
                            # acquire the request-table lock, and the request lock too, if one exists - we will not be
                            # modifying the request, but we will read it then take action based upon what we read 
                            request = GetPendingRequest(client=client, reqid=None, acquirereqlock=True, timeout=5, log=rsLog)
                            pendingRequest = connection.sendRequest(request)
                            
                            if pendingRequest is None:
                                raise InvalidRequest('cannot resume an existing request; there is no pending request for client ' + client)
                                
                            if pendingRequest.status.upper() == 'E':
                                raise FailureAtServer(pendingRequest.errmsg)

                            # figure out if client should make a pollDump or pollComplete request.
                            if pendingRequest.status.upper() == 'N' or pendingRequest.status.upper() == 'P' or pendingRequest.status.upper() == 'D':
                                respAction = 'polldump'
                            elif pendingRequest.status.upper() == 'I' or penpendingRequest.status.upper() == 'C' or pendingRequest.status.upper() == 'A':
                                respAction = 'pollcomplete'
                            else:
                                # Error response.
                                raise InvalidServerResponse('invalid request status of ' + pendingRequest.status.upper() + ' at server')
                    
                            resp = ResumeResponse(log=rsLog, status=STATUS_REQUEST_RESUMING, msg='to continue, submit a ' + respAction.lower() + ' request', reqid=pendingRequest.requestid, reqtype=pendingRequest.action, series=pendingRequest.series, archive=pendingRequest.archive, retention=pendingRequest.retention, tapegroup=pendingRequest.tapegroup, subuser=pendingRequest.subuser, resumeaction=respAction, resumestatus=pendingRequest.status.upper(), client=client)
                        finally:
                            connection.close()
                    elif action.lower() == 'subscribe':
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rsLog)
                        try:
                            request = GetPendingRequest(client=client, reqid=None, acquirereqlock=False, timeout=5, log=rsLog)
                            pendingRequest = connection.sendRequest(request)
                            
                            if pendingRequest is not None:
                                raise InvalidRequest('you cannot subscribe to a series at this time - a ' + pendingRequest.action + ' request for ' + ','.join(pendingRequest.series) + ' is pending; please wait for that request to complete')

                            # allow the user to subscribe/resubscribe to a single series at a time.
                            if seriesList is None or len(seriesList) != 1:
                                raise InvalidArgument('you can subscribe to a single series only')
                                
                            series = seriesList[0]

                            rsLog.writeInfo([ 'client ' + client + ' is requesting a subscription to series ' + series ])
                            
                            if archive is None or (archive != 0 and archive != -1 and archive != 1):
                                raise InvalidArgument('you must provide an integer value of -1, 0, or 1 for the archive argument')
                            if retention is None or retention < 0:
                                raise InvalidArgument('you must provide an integer value greater than or equal to 0 for the retention argument')
                            if tapegroup is None or tapegroup < 0:
                                raise InvalidArgument('you must provide an integer value greater than or equal to 0 for the tapegroup argument')                                

                            subscribed = clientIsSubscribed(arguments, connMaster, client, series)
                            if subscribed:
                                rsLog.writeInfo([ 'client ' + client + ' is currently subscribed to series ' + series ])
                                raise InvalidArgument('cannot subscribe to ' + series + '; client ' + client + ' is already subscribed to this series')
                            else:
                                rsLog.writeInfo([ 'client ' + client + ' is NOT currently subscribed to series ' + series ])

                            if not seriesExists(connSlave, series):
                                raise InvalidArgument('cannot subscribe to ' + series + '; it does not exist')
                            if not seriesIsPublished(arguments, connSlave, series):
                                raise InvalidArgument('cannot subscribe to ' + series + '; it is not published')
                        
                            rsLog.writeInfo([ 'series ' + series + ' exists on the server and is published' ])
                                                        
                            # insert a pending row into the su_production.slonyreq table; ensure that the series name comprises lower-case letters;
                            reqid = insertRequest(connSlave, rsLog, arguments.getArg('kSMreqTable'), client=client, action=action, series=series, archive=archive, retention=retention, tapegroup=tapegroup, subuser=subuser)
                
                            # send a 'wait' response
                            respMsg = 'request for subscription to series ' + series + ' is queued; poll for completion with a polldump request; please sleep between iterations when looping over this request'
                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg, reqid=reqid, client=client)
                        finally:
                            connection.close()
                    elif action.lower() == 'resubscribe':
                        if newSite :
                            raise InvalidRequest('you have never subscribed to a series before; you cannot make a ' + action.lower() + ' request')
                    
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rsLog)
                        try:
                            # acquire the request-table lock, but not the request lock; we are not modifying an existing request
                            # (it does not exist yet since the client is the entity that creates it); blocks until 
                            # req table lock is acquired (or time-out occurs)
                            request = GetPendingRequest(client=client, reqid=None, acquirereqlock=False, timeout=5, log=rsLog)
                            pendingRequest = connection.sendRequest(request)
                            
                            if pendingRequest is not None:
                                raise InvalidRequest('you cannot re-subscribe to a series at this time - a ' + pendingRequest.action + ' request for ' + ','.join(pendingRequest.series) + ' is pending; please wait for that request to complete')
                                
                            # allow the user to subscribe/resubscribe to a single series at a time
                            if seriesList is None or len(seriesList) != 1:
                                raise InvalidArgument('you can re-subscribe to a single series only')

                            series = seriesList[0]

                            rsLog.writeInfo([ 'client ' + client + ' is requesting a RE-subscription to series ' + series ])
                            
                            # Check for existing subscription to series.
                            subscribed = clientIsSubscribed(arguments, connMaster, client, series)
                            if subscribed:
                                rsLog.writeInfo([ 'client ' + client + ' is currently subscribed to series ' + series ])
                                raise InvalidArgument('cannot re-subscribe to ' + series + '; client ' + client + ' must first cancel the subscription')
                            else:
                                rsLog.writeInfo([ 'client ' + client + ' is NOT currently subscribed to series ' + series ])

                            if not seriesExists(connSlave, series):
                                raise InvalidArgument('cannot subscribe to ' + series + '; it does not exist')
                            if not seriesIsPublished(arguments, connSlave, series):
                                raise InvalidArgument('cannot subscribe to ' + series + '; it is not published')
                        
                            rsLog.writeInfo([ 'series ' + series + ' exists on the server and is published' ])

                            # insert a pending row into the su_production.slonyreq table; ensure that the series name comprises lower-case letters;
                            reqid = insertRequest(connSlave, rsLog, arguments.getArg('kSMreqTable'), client=client, action=action, series=series, archive=archive, retention=retention, tapegroup=tapegroup, subuser=subuser)

                            # send a 'wait' response
                            respMsg = 'request for re-subscription to series ' + series + ' is queued; poll for completion with a polldump request; please sleep between iterations when looping over this request'
                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg, reqid=reqid, client=client)
                        finally:
                            connection.close()
                    elif action.lower() == 'unsubscribe':
                        if newSite:
                            raise InvalidRequest('you have never subscribed to a series before; you cannot make a ' + action.lower() + ' request')
                            
                        # the user can drop subscriptions from multiple series
                        if seriesList is None or len(seriesList) < 1:
                            raise InvalidArgument('please provide a list of series from which you would like to unsubscribe')

                        # make socket connection to manage-subs.py
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rsLog)
                        try:
                            # acquire the request-table lock, but not the request lock; we are not modifying an existing request
                            # (it does not exist yet since the client is the entity that creates it); blocks until 
                            # req table lock is acquired (or time-out occurs)
                            request = GetPendingRequest(client=client, reqid=None, acquirereqlock=False, timeout=5, log=rsLog)
                            pendingRequest = connection.sendRequest(request)
                            
                            if pendingRequest is not None:
                                # a request is pending for this client - that must be resolved first
                                raise InvalidRequest('you cannot un-subscribe from series at this time - a ' + pendingRequest.action.lower() + ' request for ' + pendSeriesList + ' is pending; please wait for that request to complete')

                            reqid = insertRequest(connSlave, rsLog, arguments.getArg('kSMreqTable'), client=client, action=action, series=','.join(seriesList))
                            
                            respMsg = 'request for un-subscription from series ' + ','.join(seriesList) + ' is queued; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request'
                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg=respMsg, reqid=reqid, client=client)                            
                        finally:
                            connection.close()
                    elif action.lower() == 'polldump':
                        # make socket connection to manage-subs.py
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rsLog)
                        try:
                            try:
                                request = GetPendingRequest(client=client, reqid=reqid, acquirereqlock=True, timeout=5, log=rsLog)
                                pendingRequest = connection.sendRequest(request)

                                if pendingRequest is None:
                                    raise InvalidRequest('you cannot make a polldump request; no subscription request is pending')
                                    
                                rsLog.writeInfo([ action.lower() + ' request for a ' + pendingRequest.action.lower() + ' pending request' ])
                            
                                if pendingRequest.status.upper() == 'E':
                                    raise FailureAtServer(pendingRequest.errmsg)

                                if pendingRequest.action.lower() == 'unsubscribe':
                                    # polldump not valid for unsubscribe
                                    raise InvalidRequest('an action of polldump is not valid for an un-subscription')
                                elif pendingRequest.action.lower() == 'subscribe':
                                    # valid statuses: N, P, D
                                    if pendingRequest.status.upper() == 'N':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='request for subscription to series ' + ','.join(pendingRequest.series) + ' is queued; poll for dump file with a polldump request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'P':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='request for subscription to series ' + ','.join(pendingRequest.series) + ' is being processed; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'D':
                                        try:
                                            # set status to A to indicate to the server that the client is downloading the dump file
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='A')
                                            connection.sendRequest(request)
                                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_DUMP_READY, msg='the SQL dump file is ready for ingestion', client=client)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still P so that we try to set status to A on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='request for subscription to series ' + ','.join(pendingRequest.series) + ' is being processed; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    else:
                                        raise InvalidServerResponse('pending-request status of ' + pendingRequest.status.upper() + ' is not valid for an action of ' + pendingRequest.action.lower())
                                elif pendingRequest.action.lower() == 'resubscribe':
                                    # valid statuses: N, P, D
                                    if pendingRequest.status.upper() == 'N':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='request for re-subscription to series ' + ','.join(pendingRequest.series) + ' is queued; poll for dump file with a polldump request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'P':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='request for re-subscription to series ' + ','.join(pendingRequest.series) + ' is being processed; poll for completion with a polldump request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'D':
                                        try:
                                            # set status to A to indicate to the server that the client is downloading the dump file
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='A')
                                            connection.sendRequest(request)
                                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_DUMP_READY, msg='the SQL dump file is ready for ingestion', client=client)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still P so that we try to set status to A on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='request for re-subscription to series ' + ','.join(pendingRequest.series) + ' is being processed; poll for completion with a polldump request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    else:
                                        raise InvalidServerResponse('pending-request status of ' + pendingRequest.status.upper() + ' is not valid for an action of ' + pendingRequest.action.lower())
                                else:
                                    raise InvalidServerResponse('unknown pending request type ' + pendingRequest.action.lower())
                            except ServerSideTimeout as exc:
                                # if we cannot acquire the lock, then take same action as if manage-subs.py has not changed the status from N to P (or from P to D)
                                resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='request for subscription to series ' + ','.join(pendingRequest.series) + ' is queued; poll for dump file with a polldump request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                        finally:
                            connection.close()
                    elif action.lower() == 'pollcomplete':
                        # make socket connection to manage-subs.py
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rsLog)
                        try:
                            try:
                                request = GetPendingRequest(client=client, reqid=reqid, acquirereqlock=True, timeout=5, log=rsLog)
                                pendingRequest = connection.sendRequest(request)
                            
                                if pendingRequest is None:
                                    raise InvalidRequest('cannot make a pollcomplete request; no subscription request is pending')
                                    
                                rsLog.writeInfo([ action.lower() + ' request for a ' + pendingRequest.action.lower() + ' pending request' ])

                                if pendingRequest.status.upper() == 'E':
                                    raise FailureAtServer(pendingRequest.errmsg)

                                if pendingRequest.action.lower() == 'unsubscribe':
                                    # valid statuses: N, P, C
                                    if pendingRequest.status.upper() == 'N':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_QUEUED, msg='request for un-subscription from series ' + ','.join(pendingRequest.series) + ' is queued; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'P':
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_PROCESSING, msg='request for un-subscription from series ' + ','.join(pendingRequest.series) + ' is being processed; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'C':
                                        resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_COMPLETE, msg='your ' + pendingRequest.action.lower() + ' request has successfully completed', client=client)
                                    else:
                                        raise InvalidServerResponse('pending-request status of ' + pendingRequest.status.upper() + ' is not valid for an action of ' + pendingRequest.action.lower())
                                elif pendingRequest.action.lower() == 'subscribe':
                                    # valid statuses: D, A, I, C
                                    if pendingRequest.status.upper() == 'D':
                                        try:
                                            # set status to A to indicate to the server that the client is downloading the dump file
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='A')
                                            connection.sendRequest(request)                                            
                                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_DUMP_READY, msg='the SQL dump file is ready for ingestion', client=client)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still I so that we try to set status to A on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='time-out requesting finalization for subscription to series ' + ','.join(pendingRequest.series) + '; try again then poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'A':
                                        try:
                                            # set status to I to indicate to the server that the client has ingested the dump file
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='I')
                                            connection.sendRequest(request)
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='requesting finalization for subscription to series ' + ','.join(pendingRequest.series) + '; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still A so that we try to set status to I on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='time-out notifying server that dump file (for series ' + ','.join(pendingRequest.series) + ') has been ingested; try again then poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'I':
                                        # do not change status                                    
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='request for subscription to series ' + ','.join(pendingRequest.series) + ' is being finalized; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'C':
                                        try:
                                            # set status to S to indicate to the server that the client has seen that the request is complete
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='S')
                                            connection.sendRequest(request)
                                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_COMPLETE, msg='your subscription request has successfully completed', client=client)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still C so that we try to set status to S on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='time-out acknowledging subscription to series ' + ','.join(pendingRequest.series) + ' is complete; try again then poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    else:
                                        raise InvalidServerResponse('pending-request status of ' + pendingRequest.status.upper() + ' is not valid for an action of ' + pendingRequest.action.lower())
                                elif pendingRequest.action.lower() == 'resubscribe':
                                    # valid statuses: D, A, I, C
                                    if pendingRequest.status.upper() == 'D':
                                        try:
                                            # set status to A to indicate to the server that the client is downloading the dump file
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='A')
                                            connection.sendRequest(request)
                                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_DUMP_READY, msg='the SQL dump file is ready for ingestion', client=client)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still I so that we try to set status to A on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='time-out requesting finalization for subscription to series ' + ','.join(pendingRequest.series) + '; try again then poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'A':
                                        try:
                                            # set status to I to indicate to the server that the client has ingested the dump file
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='I')
                                            connection.sendRequest(request)
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='requesting finalization for re-subscription to series ' + ','.join(pendingRequest.series) + '; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still A so that we try to set status to I on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='time-out notifying server that dump file (for series ' + ','.join(pendingRequest.series) + ') has been ingested; try again then poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'I':
                                        # do not change status                                    
                                        resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='request for re-subscription to series ' + ','.join(pendingRequest.series) + ' is being finalized; poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    elif pendingRequest.status.upper() == 'C':
                                        try:
                                            # set status to S to indicate to the server that the client has seen that the request is complete
                                            request = SetStatusRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, status='S')
                                            connection.sendRequest(request)
                                            resp = ContinueResponse(log=rsLog, status=STATUS_REQUEST_COMPLETE, msg='your re-susbscription request has successfully completed', client=client)
                                        except ServerSideTimeout as exc:
                                            # pretend that the status was still C so that we try to set status to S on the next attempt
                                            resp = WaitResponse(log=rsLog, status=STATUS_REQUEST_FINALIZING, msg='time-out acknowledging subscription to series ' + ','.join(pendingRequest.series) + ' is complete; try again then poll for completion with a pollcomplete request; please sleep between iterations when looping over this request', client=client, reqid=reqid)
                                    else:
                                        raise InvalidServerResponse('pending-request status of ' + pendingRequest.status.upper() + ' is not valid for an action of ' + pendingRequest.action.lower())
                                else:
                                    raise InvalidServerResponse('unknown pending request type ' + pendingRequest.action.lower())
                            except ServerSideTimeout as exc:
                                # we do not know what the status of the request is (it could be N, P, D, A, I, or C), so we cannot take any 
                                # appropriate action; just let the outer exception handler take over
                                raise
                        finally:
                            connection.close()
                    elif action.lower() == 'error':
                        # the client (subscribe.py) encountered a fatal error - notify the server
                        connection = Connection(client=client, host=serverhost, port=serverport, log=rsLog)
                        try:
                            # set request status to E
                            # XXX need to modify subscribe.py to pass an error message
                            request = ErrorRequest(client=client, reqid=reqid, acquirereqlock=True, log=rsLog, timeout=5, errmsg='generic problem at client ' + client)
                            connection.sendRequest(request)
                        except ServerSideTimeout as exc:
                            # do not handle a server time-out here; the client has done all it can do to tell the server that
                            # it is terminating with an error; do not re-raise because the client is not going to change its
                            # behavior if the server times-out
                            pass
                        finally:
                            connection.close()
                    else:
                        # Unrecognized action.
                        raise InvalidRequest('request of type ' + "'" + action + "'" + ' is not recognized')
                        
                    resp.logMsg()
                    resp.send()
                except RSException as exc:
                    # notify client that something went wrong in request-subs.py, but let client decide what to do - 
                    # it will probably send an error request (which will be handled in request-subs.py) and then terminate
                    resp = ErrorResponse(log=rsLog, msg=exc.args[0], status=exc.status, client=client)
                    resp.logMsg()
                    resp.send()
                                                
        # check for SIGINT
        if thContainer[0] == STATUS_ERR_TERMINATED:
            resp = ErrorResponse(log=rsLog, msg='request-subs.py was terminated', status=STATUS_ERR_TERMINATED, client=client)
            resp.log()
            resp.send()
    except RSException as exc:
        resp = ErrorResponse(log=rsLog, msg=exc.args[0], status=exc.status, client=client)
        resp.logMsg()
        resp.send()
    except Exception as exc:  
        if rsLog:
            import traceback
            rsLog.writeError([ traceback.format_exc(8) ])
            
        resp = ErrorResponse(log=rsLog, msg='unknown error in subscription CGI', client=client)
        resp.send()
    if rsLog:
        rsLog.close()
    logging.shutdown()

    sys.exit(0)
