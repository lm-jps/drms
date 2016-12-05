#!/usr/bin/env python

from __future__ import print_function
import sys
import re
import os
import threading
import socket
import signal
import select
from datetime import datetime, timedelta, timezone
import re
import random
import json
import uuid
from subprocess import check_call, CalledProcessError
from pathlib import Path
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

if sys.version_info < (3, 2):
    raise Exception('You must run the 3.2 release, or a more recent release, of Python.')


SUMSD = 'sumsd'
SUM_MAIN = 'public.sum_main'
SUM_PARTN_ALLOC = 'public.sum_partn_alloc'
SUM_ARCH_GROUP = 'public.sum_arch_group'
SUM_PARTN_AVAIL = 'public.sum_partn_avail'

DARW = 1
DADP = 2
DAAP = 4
DAAEDDP = 32
DAAPERM = 64
DAADP = 128

# Return code
RV_SUCCESS = 0
RV_DRMSPARAMS = 1
RV_ARGS = 2
RV_BADLOGFILE = 3
RV_BADLOGWRITE = 4
RV_DBCOMMAND = 5
RV_DBCONNECTION = 6
RV_DBCURSOR = 7
RV_SOCKETCONN = 8
RV_REQUESTTYPE = 9
RV_POLL = 10

# Exception status codes
RESPSTATUS_OK = 'ok'
RESPSTATUS_JSON = 'bad-json'
RESPSTATUS_CLIENTINFO = 'bad-clientinfo'
RESPSTATUS_REQ = 'bad-request'
RESPSTATUS_REQTYPE = 'bad-request-type'
RESPSTATUS_GENRESPONSE = 'cant-generate-response'

# Maximum number of DB rows returned
MAX_MTSUMS_NSUS = 32768

class SumsDrmsParams(DRMSParams):

    def __init__(self):
        super(SumsDrmsParams, self).__init__()

    def get(self, name):
        val = super(SumsDrmsParams, self).get(name)

        if val is None:
            raise ParamsException('Unknown DRMS parameter: ' + name + '.')
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

                raise ArgsException(msg)
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


class ParamsException(Exception):

    def __init__(self, msg):
        super(ParamsException, self).__init__(msg)


class ArgsException(Exception):

    def __init__(self, msg):
        super(ArgsException, self).__init__(msg)


class PollException(Exception):

    def __init__(self, msg):
        super(PollException, self).__init__(msg)


class SocketConnectionException(Exception):

    def __init__(self, msg):
        super(SocketConnectionException, self).__init__(msg)


class DBConnectionException(Exception):

    def __init__(self, msg):
        super(DBConnectionException, self).__init__(msg)


class DBCommandException(Exception):

    def __init__(self, msg):
        super(DBCommandException, self).__init__(msg)


class ReceiveJsonException(Exception):

    def __init__(self, msg):
        super(ReceiveJsonException, self).__init__(msg)
        
        
class ClientInfoException(Exception):

    def __init__(self, msg):
        super(ClientInfoException, self).__init__(msg)


class ExtractRequestException(Exception):

    def __init__(self, msg):
        super(ExtractRequestException, self).__init__(msg)


class RequestTypeException(Exception):

    def __init__(self, msg):
        super(RequestTypeException, self).__init__(msg)


class GenerateResponseException(Exception):

    def __init__(self, msg):
        super(GenerateResponseException, self).__init__(msg)

class ImplementationException(Exception):

    def __init__(self, msg):
        super(ImplementationException, self).__init__(msg)


class TaperequestException(Exception):

    def __init__(self, msg):
        super(TaperequestException, self).__init__(msg)


class Dbconnection(object):

    def __init__(self, host, port, database, user):
        self.conn = None
        
        # Connect to the db. If things succeed, then save the db-connection information.
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.openConnection()
        
    def commit(self):
        # Does not close DB connection. It can be used after the commit() call.
        if not self.conn:
            raise DBCommandException('Cannot commit - no database connection exists.')
            
        if self.conn:
            self.conn.commit()

    def rollback(self):
        # Does not close DB connection. It can be used after the rollback() call.
        if not self.conn:
            raise DBCommandException('Cannot rollback - no database connection exists.')

        if self.conn:
            self.conn.rollback()

    def close(self):
        # Does a rollback, then closes DB connection so that it can no longer be used.
        self.closeConnection()
            
    def openConnection(self):
        if self.conn:
            raise DBConnectionException('Already connected to the database.')
            
        try:
            self.conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user)
        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection
            if hasattr(exc, 'diag') and hasattr(exc.diag, 'message_primary'):
                msg = exc.diag.message_primary
            else:
                msg = 'Unable to connect to the database (no, I do not know why).'
            raise DBConnectionException(msg)
    
    def closeConnection(self):    
        if not self.conn:
            raise DBConnectionException('There is no database connection.')
        
        if self.conn:
            self.conn.close()

    def exeCmd(self, cmd, results, result=True):
        if not self.conn:
            raise DBCommandException('Cannot execute database command ' + cmd + ' - no database connection exists.')
        
        if result:
            try:
                with self.conn.cursor('namedCursor') as cursor:
                    cursor.itersize = 4096

                    try:
                        cursor.execute(cmd)
                        for row in cursor:
                            results.append(row) # results is a list of lists
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise DBCommandException(exc.diag.message_primary)
            except psycopg2.Error as exc:
                raise DBCommandException(exc.diag.message_primary)
        else:
            try:
                with self.conn.cursor() as cursor:
                    try:
                        cursor.execute(cmd)
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise DBCommandException(exc.diag.message_primary)
            except psycopg2.Error as exc:
                raise DBCommandException(exc.diag.message_primary)

class DataObj(object):
    pass
    
class Jsonizer(object):
    def __init__(self, dataObj):
        self.data = dataObj
        self.json = json.dumps(dataObj)
        
    def getJSON(self):
        return self.json
        
class Unjsonizer(object):
    def __init__(self, jsonStr):
        self.json = jsonStr
        self.unjsonized = json.loads(jsonStr) # JSON objects are converted to Python dictionaries!
        
class Request(object):
    def __init__(self, reqType, unjsonized, collector):
        self.reqType = reqType
        self.unjsonized = unjsonized.unjsonized # a request-specific dictionary
        self.collector = collector
        self.data = DataObj()
        if 'sessionid' in self.unjsonized:
            # Only the OpenRequest will not have a sessionid.
            self.data.sessionid = self.unjsonized['sessionid']

    def __str__(self):
        return str(self.unjsonized)
        
    def generateResponse(self, dest=None):
        # Commit changes to the DB. The DB connection is closed when the Collector thread terminates, but the transaction
        # is rolled back at that time.
        self.collector.dbconn.commit()

    def generateErrorResponse(self, status, errMsg):
        return ErrorResponse(self, status, errMsg)
            
    @staticmethod
    def hexToInt(hexStr):
        return int(hexStr, 16)


class OpenRequest(Request):
    """
    unjsonized is:
    {
        'reqtype' : 'open'
    }
    """
    def __init__(self, unjsonized, collector):
        super(OpenRequest, self).__init__('open', unjsonized, collector)
        # No data for this request.
        
    def generateResponse(self, dest=None):
        resp = OpenResponse(self, dest)
        super(OpenRequest, self).generateResponse(dest)
        return resp


class CloseRequest(Request):
    """
    unjsonized is:
    {
        'reqtype' : 'close',
        'sessionid' : 2895025
    }
    """
    def __init__(self, unjsonized, collector):
        super(CloseRequest, self).__init__('close', unjsonized, collector)
        
    def generateResponse(self, dest=None):
        resp = CloseResponse(self, dest)
        super(CloseRequest, self).generateResponse(dest)
        return resp


class InfoRequest(Request):
    """
    unjsonized is:
    {
       'reqtype' : 'info',
       'sessionid' : 2895025,
       'sus' : [ '3039', '5BA0' ]
    }
    """
    def __init__(self, unjsonized, collector):
        super(InfoRequest, self).__init__('info', unjsonized, collector)
        
        if len(self.unjsonized['sus']) > MAX_MTSUMS_NSUS:
            raise ExtractRequestException('Too many SUs in request (maximum of ' + str(MAX_MTSUMS_NSUS) + ' allowed).')
        
        self.data.sus = [ Request.hexToInt(hexStr) for hexStr in self.unjsonized['sus'] ]

        processed = set()
        self.data.sulist = []
         
        # sus may contain duplicates. They must be removed.
        for su in self.data.sus:
            if str(su) not in processed:        
                self.data.sulist.append(str(su)) # Make a list of strings - we'll need to concatenate the elements into a comma-separated list for the DB query.
                processed.add(str(su))

    def generateResponse(self, dest=None):
        resp = InfoResponse(self, dest)
        super(InfoRequest, self).generateResponse(dest)
        return resp

class GetRequest(Request):
    """
    unjsonized is:
    {
       'reqtype' : 'get',
       'sessionid' : 2895025,
       'touch' : True,
       'retrieve' : False,
       'retention' : 60,
       'sus' : ['1DE2D412', '1AA72414']
    }
    """
    def __init__(self, unjsonized, collector):
        super(GetRequest, self).__init__('get', unjsonized, collector)

        if len(self.unjsonized['sus']) > MAX_MTSUMS_NSUS:
            raise ExtractRequestException('Too many SUs in request (maximum of ' + str(MAX_MTSUMS_NSUS) + ' allowed).')
        
        self.data.touch = self.unjsonized['touch']
        self.data.retrieve = self.unjsonized['retrieve']
        self.data.retention = self.unjsonized['retention']
        self.data.sus = [ Request.hexToInt(hexStr) for hexStr in self.unjsonized['sus'] ]

        processed = set()
        self.data.susNoDupes = []
         
        # sus may contain duplicates. They must be removed.
        for su in self.data.sus:
            if str(su) not in processed:        
                self.data.susNoDupes.append(str(su)) # Make a list of strings - we'll need to concatenate the elements into a comma-separated list for the DB query.
                processed.add(str(su))
                
    def generateResponse(self, dest=None):
        resp = GetResponse(self, dest)
        super(GetRequest, self).generateResponse(dest)
        return resp


class AllocRequest(Request):
    """
    unjsonized is:
    {
        'reqtype' : 'alloc',
        'sessionid' : 7035235,
        'sunum' : '82C5E02A',
        'sugroup' : 22,
        'numbytes' : 1024
    }
    
    or
    
    unjsonized is:
    {
        'reqtype' : 'alloc',
        'sessionid' : 7035235,
        'sunum' : null,
        'sugroup' : 22,
        'numbytes' : 1024
    }
    """
    def __init__(self, unjsonized, collector):
        super(AllocRequest, self).__init__('alloc', unjsonized, collector)

        if self.unjsonized['sunum']:
            self.data.sunum = Request.hexToInt(self.unjsonized['sunum'])
        else:
            # Do not create the sunum attribute. A check later looks for sunum existence.
            pass
        self.data.sugroup = self.unjsonized['sugroup']
        self.data.numbytes = self.unjsonized['numbytes']
        
    def generateResponse(self, dest=None):
        resp = AllocResponse(self, dest)
        super(AllocRequest, self).generateResponse(dest)
        return resp
        

class PutRequest(Request):
    """
    unjsonized is:
    {
        'reqtype' : 'put',
        'sessionid' : 7035235,
        'sudirs' : [ {'2B13493A' : '/SUM19/D722684218'}, {'2B15A227' : '/SUM12/D722838055'} ],
        'series' : 'hmi.M_720s',
        'retention' : 14,
        'archivetype' : 'temporary+archive'
    }
    """
    def __init__(self, unjsonized, collector):
        super(PutRequest, self).__init__('put', unjsonized, collector)
        
        if len(self.unjsonized['sudirs']) > MAX_MTSUMS_NSUS:
            raise ExtractRequestException('Too many SUs in request (maximum of ' + str(MAX_MTSUMS_NSUS) + ' allowed).')
        
        sudirsNoDupes = []
        processed = set()

        # self.unjsonized['sudirs'] may contain duplicates. They must be removed. We do not need to keep track of the 
        # original list with duplicates, however, since we won't be returning any information back to caller.
        for elem in sorted(self.unjsonized['sudirs'], key=self.suSort):
            [(hexStr, path)] = elem.items()
            suStr = str(Request.hexToInt(hexStr))
            if suStr not in processed:
                sudirsNoDupes.append({ suStr : path}) # Make a list of strings - we'll need to concatenate the elements into a comma-separated list for the DB query.
                processed.add(suStr)
                
        self.data.sudirsNoDupes = sudirsNoDupes
        self.data.series = self.unjsonized['series']
        if 'retention' in self.unjsonized:
            self.data.retention = self.unjsonized['retention']
        else:
            # Don't know why the RPC SUMS has a default for this parameter, but not most others.
            self.data.retention = 2
        self.data.archivetype = self.unjsonized['archivetype']
        
    def generateResponse(self, dest=None):
        resp = PutResponse(self, dest)
        super(PutRequest, self).generateResponse(dest)
        return resp
        
    @classmethod
    def suSort(cls, elem):
        [(hexStr, path)] = elem.items()
        return Request.hexToInt(hexStr)
        

class DeleteseriesRequest(Request):
    """
    unjsonized is:
    {
    'reqtype' : 'deleteseries',
    'sessionid' : 7035235,
    'series' : 'hmi.M_720s'
    }
    """
    def __init__(self, unjsonized, collector):
        super(DeleteseriesRequest, self).__init__('deleteseries', unjsonized, collector)
        
        self.data.series = self.unjsonized['series']
        
    def generateResponse(self, dest=None):
        resp = DeleteseriesResponse(self, dest)
        super(DeleteseriesRequest, self).generateResponse(dest)
        return resp


class PingRequest(Request):
    def __init__(self, unjsonized, collector):
        super(PingRequest, self).__init__('ping', unjsonized, collector)
        
    def generateResponse(self, dest=None):
        resp = PingResponse(self, dest)
        super(PingRequest, self).generateResponse(dest)
        return resp


class PollRequest(Request):
    def __init__(self, unjsonized, collector):
        super(PollRequest, self).__init__('poll', unjsonized, collector)

        self.data.requestid = self.unjsonized['requestid']
        
    def generateResponse(self, dest=None):
        resp = PollResponse(self, dest)
        super(PollRequest, self).generateResponse(dest)


class RequestFactory(object):
    def __init__(self, collector):
        self.collector = collector

    def getRequest(self, jsonStr):
        unjsonized = Unjsonizer(jsonStr)
        
        reqType = unjsonized.unjsonized['reqtype'].lower()
        if reqType == 'open':
            return OpenRequest(unjsonized, self.collector)
        elif reqType == 'close':
            return CloseRequest(unjsonized, self.collector)
        elif reqType == 'info':
            return InfoRequest(unjsonized, self.collector)
        elif reqType == 'get':
            return GetRequest(unjsonized, self.collector)
        elif reqType == 'alloc':
            return AllocRequest(unjsonized, self.collector)
        elif reqType == 'put':
            return PutRequest(unjsonized, self.collector)
        elif reqType == 'deleteseries':
            return DeleteseriesRequest(unjsonized, self.collector)
        elif reqType == 'ping':
            return PingRequest(unjsonized, self.collector)
        elif reqType == 'poll':
            return PollRequest(unjsonized, self.collector)
        else:
            raise RequestTypeException('The request type ' + reqType + ' is not supported.')
            

class Response(object):
    def __init__(self, request):
        self.request = request
        self.cmd = None
        self.dbRes = None
        self.data = {} # A Py dictionary containing the response to the request. Will be JSONized before being sent to client.
        self.jsonizer = None
        
    def exeDbCmd(self):
        if hasattr(self.request.collector, 'debugLog') and self.request.collector.debugLog:
            self.request.collector.debugLog.write(['db command is: ' + self.cmd])

        self.request.collector.dbconn.exeCmd(self.cmd, self.dbRes, True)
        
    def exeDbCmdNoResult(self):
        if hasattr(self.request.collector, 'debugLog') and self.request.collector.debugLog:
            self.request.collector.debugLog.write(['db command is: ' + self.cmd])

        self.request.collector.dbconn.exeCmd(self.cmd, None, False)

        
    def getJSON(self, error=False, errMsg=None):
        self.jsonizer = Jsonizer(self.data)
        return self.jsonizer.getJSON()
        
    def setStatus(self, status):
        self.data['status'] = status
        
    @staticmethod    
    def stripHexPrefix(hexadecimal):
        regexp = re.compile(r'^\s*0x(\S+)', re.IGNORECASE)
        match = regexp.match(hexadecimal)
        if match:
            return match.group(1)
        else:
            return hexadecimal
            
    @staticmethod
    def intToHex(bigint):
        return Response.stripHexPrefix(hex(bigint))
            
class ErrorResponse(Response):
    def __init__(self, request, status, errMsg):
        super(ErrorResponse, self).__init__(request)
        msg = 'Unable to create ' + request.reqType + ' response: ' + errMsg
        
        if request.collector.debugLog:
            request.collector.debugLog.write([ 'Error: status (' + str(status) + '), msg (' + msg + ')' ])
        
        self.data['status'] = status
        self.data['errmsg'] = msg


class OpenResponse(Response):
    def __init__(self, request, dest=None):
        super(OpenResponse, self).__init__(request)

        self.dbRes = []
        self.cmd = "SELECT nextval('public.sum_seq')"
        self.exeDbCmd()
        
        if len(self.dbRes) != 1 or len(self.dbRes[0]) != 1:
            raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)
            
        sessionid = self.dbRes[0][0] # self.dbRes is a list of lists (or a 'table')
        
        self.cmd = 'INSERT INTO public.sum_open(sumid, open_date) VALUES (' + str(sessionid) + ', localtimestamp)'
        self.exeDbCmdNoResult()
        
        self.data['sessionid'] = sessionid


class CloseResponse(Response):
    def __init__(self, request, dest=None):
        super(CloseResponse, self).__init__(request)

        self.cmd = 'DELETE FROM public.sum_partn_alloc WHERE sumid = ' + str(self.request.data.sessionid) + ' AND (status = 8 OR status = 1)'
        self.exeDbCmdNoResult()
    
        self.cmd = 'DELETE FROM public.sum_open WHERE sumid = ' + str(self.request.data.sessionid)
        self.exeDbCmdNoResult()
    

class InfoResponse(Response):
    def __init__(self, request, dest=None):
        super(InfoResponse, self).__init__(request)
        
        # Extract response data from the DB.
        dbInfo = [] # In theory there could be multiple DB requests.
        self.dbRes = []
        # Get DB info for unique SUs only (the sulist list does not contain duplicates).
        self.cmd = "SELECT T1.ds_index, T1.online_loc, T1.online_status, T1.archive_status, T1.offsite_ack, T1.history_comment, T1.owning_series, T1.storage_group, T1.bytes, T1.create_sumid, T1.creat_date, T1.username, COALESCE(T1.arch_tape, 'N/A'), COALESCE(T1.arch_tape_fn, 0), COALESCE(T1.arch_tape_date, '1958-01-01 00:00:00'), COALESCE(T1.safe_tape, 'N/A'), COALESCE(T1.safe_tape_fn, 0), COALESCE(T1.safe_tape_date, '1958-01-01 00:00:00'), COALESCE(T2.effective_date, '195801010000'), coalesce(T2.status, 0), coalesce(T2.archive_substatus, 0) FROM " + SUM_MAIN + " AS T1 LEFT OUTER JOIN " + SUM_PARTN_ALLOC + " AS T2 ON (T1.ds_index = T2.ds_index) WHERE T1.ds_index IN (" + ','.join(self.request.data.sulist) + ')'
        self.exeDbCmd()
        dbInfo.append(self.dbRes)
        self.parse(dbInfo)
        
    def parse(self, dbInfo):
        infoList = []
        processed = {}
        
        # Make an object from the lists returned by the database. dbResponse is a list of lists.
        for row in dbInfo[0]:
            rowIter = iter(row)
            infoDict = {}
            sunum = next(rowIter)
            infoDict['sunum'] = Response.intToHex(sunum) # Convert to hex string since some parsers do not support 64-bit integers.
            infoDict['onlineLoc'] = next(rowIter)
            infoDict['onlineStatus'] = next(rowIter)
            infoDict['archiveStatus'] = next(rowIter)
            infoDict['offsiteAck'] = next(rowIter)
            infoDict['historyComment'] = next(rowIter)
            infoDict['owningSeries'] = next(rowIter)
            infoDict['storageGroup'] = next(rowIter)
            infoDict['bytes'] = Response.intToHex(next(rowIter)) # Convert to hex string since some parsers do not support 64-bit integers.
            infoDict['createSumid'] = next(rowIter)
            # The db returns a datetime object. Convert the datetime to a str object.
            infoDict['creatDate'] = next(rowIter).strftime('%Y-%m-%d %T')
            infoDict['username'] = next(rowIter)
            infoDict['archTape'] = next(rowIter)
            infoDict['archTapeFn'] = next(rowIter)
            # The db returns a datetime object. Convert the datetime to a str object.
            infoDict['archTapeDate'] = next(rowIter).strftime('%Y-%m-%d %T')
            infoDict['safeTape'] = next(rowIter)
            infoDict['safeTapeFn'] = next(rowIter)
            # The db returns a datetime object. Convert the datetime to a str object.
            infoDict['safeTapeDate'] = next(rowIter).strftime('%Y-%m-%d %T')
            infoDict['effectiveDate'] = next(rowIter)
            infoDict['paStatus'] = next(rowIter)
            infoDict['paSubstatus'] = next(rowIter)
            
            # Put SU in hash of processed SUs.
            suStr = str(sunum) # Convert hexadecimal string to decimal string.
            processed[suStr] = infoDict
        
        # Loop through ALL SUs, even duplicates (the sus list may contain duplicates).
        for su in self.request.data.sus:
            if str(su) in processed:
                infoList.append(processed[str(su)])
            else:
                # Must check for an invalid SU and set some appropriate values if the SU is indeed invalid:
                #   sunum --> sunum
                #   paStatus --> 0
                #   paSubstatus --> 0
                #   onlineLoc --> ''
                #   effectiveDate --> 'N/A'
                # The other attributes do not matter.
                # If the SUNUM was invalid, then there was no row in the response for that SU. So, we
                # have to create dummy rows for those SUs.
                infoDict = {}
                infoDict['sunum'] = Response.intToHex(su) # Convert to hex string since some parsers do not support 64-bit integers.
                infoDict['onlineLoc'] = ''
                infoDict['onlineStatus'] = ''
                infoDict['archiveStatus'] = ''
                infoDict['offsiteAck'] = ''
                infoDict['historyComment'] = ''
                infoDict['owningSeries'] = ''
                infoDict['storageGroup'] = -1
                infoDict['bytes'] = Response.intToHex(0) # In sum_main, bytes is a 64-bit integer. In SUM_info, it is a double. sum_open.c converts the integer (long) to a floating-point number.
                infoDict['createSumid'] = -1
                infoDict['creatDate'] = '1966-12-25 00:54'
                infoDict['username'] = ''
                infoDict['archTape'] = ''
                infoDict['archTapeFn'] = -1
                infoDict['archTapeDate'] = '1966-12-25 00:54'
                infoDict['safeTape'] = ''
                infoDict['safeTapeFn'] = -1
                infoDict['safeTapeDate'] = '1966-12-25 00:54'
                infoDict['effectiveDate'] = 'N/A'
                infoDict['paStatus'] = 0
                infoDict['paSubstatus'] = 0

                infoList.append(infoDict)
                
        self.data['suinfo'] = infoList

class GetResponse(Response):
    def __init__(self, request, dest=None):
        super(GetResponse, self).__init__(request)
        
        # Extract response data from the DB.
        dbInfo = []
        self.dbRes = []
        # sum_main query first. The DB response will be used to generate the SUM_get() response.
        self.cmd = 'SELECT T1.ds_index, T1.online_loc, T1.online_status, T1.archive_status, T1.arch_tape, T1.arch_tape_fn FROM ' + SUM_MAIN + ' AS T1 WHERE ds_index IN (' + ','.join(self.request.data.susNoDupes) + ')'
        self.exeDbCmd()
        dbInfo.append(self.dbRes)
        
        self.parse(dbInfo)
        
        if dest:
            dest.data['supaths'] = self.data['supaths']

        # SUM_get() has a side effect: if the SU is online, then we update the retention, otherwise, we read the SU from tape (if the
        # DRMS has a tape system).

        # sum_partn_alloc UPDATE query second. This is one side-effect of the SUM_get(). It potentially modifies the effective_date of the
        # SUs.
        if self.request.data.touch:
            if self.request.data.retention < 0:
                # If the retention value is negative, then set the effective_date to max(today + -retention, current effective date).
                # WTAF! effective_date is a DB string! A string! It has the format YYYYMMDDHHMM - no time zone. Use DB's timestamp
                # functions to use math on effective_date.
                # 
                # A status of 8 implies a read-only SU. Add "3 days grace".
                # susNoDupes does not contain duplicates.
                self.cmd = 'UPDATE ' + SUM_PARTN_ALLOC + " AS T1 SET effective_date = to_char(CURRENT_TIMESTAMP + interval '" + str(-self.request.data.retention + 3) + " days', 'YYYYMMDDHH24MI') FROM " + SUM_MAIN + " AS T2 WHERE T1.status != 8 AND (T1.effective_date = '0' OR CURRENT_TIMESTAMP + interval '" + str(-self.request.data.retention) + " days' >  to_timestamp(T1.effective_date, 'YYYYMMDDHH24MI')) AND T1.ds_index IN (" + ','.join(self.request.data.susNoDupes) + ") AND T1.ds_index = T2.ds_index AND T2.online_status = 'Y'"
            else:
                # Set the effective date to today + retention.
                self.cmd = 'UPDATE ' + SUM_PARTN_ALLOC + " AS T1 SET effective_date = to_char(CURRENT_TIMESTAMP + interval '" + str(self.request.data.retention + 3) + " days', 'YYYYMMDDHH24MI') FROM " + SUM_MAIN + " AS T2 WHERE T1.status != 8 AND T1.ds_index IN (" + ','.join(self.request.data.susNoDupes) + ") AND T1.ds_index = T2.ds_index AND T2.online_status = 'Y'"
                
            self.exeDbCmdNoResult()
            
        # Tape read. Send a request to the tape system for all SUs that have the readfromtape attribute.
        tapeRequest = {}
        for sunum in self.info:
            if self.info[str(sunum)]['readfromtape']:
                # Insert the SUNUM, the tape ID, and the tape file number into a contain to be passed to the tape service.
                tapeRequest[str(sunum)] = { 'tapeid' : self.info[str(sunum)]['tapeid'], 'tapefn' : self.info[str(sunum)]['tapefn'] }
        
        if len(tapeRequest) > 0:
            self.data['taperead-requestid'] = uuid.uuid1()
            if dest:
                dest.data['taperead-requestid'] = self.data['taperead-requestid']
            
            # Make tape-service request. NOT IMPLEMENTED!
            raise ImplementationException('SUMS is configured to provide tape service, but the tape service is not implemented.')
            
            # Spawn a thread to process the tape-read request. Store the thread ID and a status, initially 'pending', in a hash array 
            # in a class variable of the TapeRequestClient class. The key for this hash-array entry is the taperead-requestid value. 
            # When the TapeRequest thread completes successfully, the thread ID of the entry is set to None. The status is
            # either set to success or failure. The PollRequest looks for the entry in the hash array. If it does not find it, 
            # the PollRequest errors out. If it finds it, it then it looks at the status. If it is 'pending', then the PollRequest
            # code returns the taperead-requestid back to the client. If the status is 'complete', then the PollRequest
            # code returns a valid GetResponse formed from the information returned from the tape service.
            # tapeRequestClient = TapeRequestClient(self.data['taperead-requestid'], self.request)

    def parse(self, dbInfo):
        supaths = []
        processed = {}
        self.info = {}
        
        for row in dbInfo[0]:
            rowIter = iter(row)
            suPathDict = {}
            
            sunum = next(rowIter)
            self.info[str(sunum)] = {}
            self.info[str(sunum)]['path'] = next(rowIter)
            
            # Save the online and archive status for side-effect changes.
            self.info[str(sunum)]['online'] = (next(rowIter).lower() == 'y')
            self.info[str(sunum)]['archived'] = (next(rowIter).lower() == 'y')
            self.info[str(sunum)]['tapeid'] = next(rowIter) # String
            self.info[str(sunum)]['tapefn'] = next(rowIter) # Integer
            
            suPathDict['sunum'] = Response.intToHex(sunum) # Convert to hex string since some parsers do not support 64-bit integers.
            # Gotta deal with offline SUs. Despite the fact these are offline, SUM_MAIN::online_loc has a path. We need to remove that path.
            if self.info[str(sunum)]['online']:
                suPathDict['path'] = self.info[str(sunum)]['path']
                self.info[str(sunum)]['readfromtape'] = False
            else:
                suPathDict['path'] = None
                if not self.request.data.retrieve:
                    self.info[str(sunum)]['readfromtape'] = False
                else:
                    # If the DRMS does not have a tape system, then archive_status should never be anything other than 'N'. But
                    # just to be sure, check the tape-system attribute of SUMS.
                    if self.request.collector.hasTapeSys and self.info[str(sunum)]['archived']:
                        self.info[str(sunum)]['readfromtape'] = True
                    else:
                        self.info[str(sunum)]['readfromtape'] = False
            
            if str(sunum) not in processed:
                processed[str(sunum)] = suPathDict
        
        # sus may contain duplicates.
        for su in self.request.data.sus:
            if str(su) in processed:
                supaths.append(processed[str(su)])
            else:
                # Set the path to the None for all invalid/unknown SUs.
                suPathDict = {}
                suPathDict['sunum'] = Response.intToHex(su)
                suPathDict['path'] = None
                
                supaths.append(suPathDict)
        
        # To send to client.
        self.data['supaths'] = supaths
        
class AllocResponse(Response):
    def __init__(self, request, dest=None):
        super(AllocResponse, self).__init__(request)
        
        partSet = 0
        
        if self.request.collector.hasMultPartSets:
            if sugroup in self.request.data:
                self.dbRes = []
                self.cmd = 'SELECT sum_set FROM ' + SUM_ARCH_GROUP + ' WHERE group_id = ' + self.request.data.sugroup
                self.exeDbCmd()

                if len(self.dbRes) != 1 or len(self.dbRes[0]) != 1:
                    raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)
            
                partSet = self.dbRes[0][0]
                
        self.dbRes = []
        self.cmd = 'SELECT partn_name FROM ' + SUM_PARTN_AVAIL + ' WHERE avail_bytes >= ' + str(self.request.data.numbytes) + ' AND pds_set_num = ' + str(partSet)
        self.exeDbCmd()
        
        if len(self.dbRes) < 1:
            raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)
    
        partitions = []
        for row in self.dbRes:
            if len(row) != 1:
                raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)
        
            partitions.append(row[0])
            
        # Create sunum, if needed.
        if hasattr(self.request.data, 'sunum'):
            sunum = self.request.data.sunum
        else:
            self.dbRes = []
            self.cmd = "SELECT nextval('public.sum_ds_index_seq')"
            self.exeDbCmd()
            
            if len(self.dbRes) != 1 or len(self.dbRes[0]) != 1:
                raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)

            sunum = self.dbRes[0][0]

        # Randomly choose one of the partitions to put the new SU into. We want to spread the write load over available 
        # partitions.
        randIndex = random.randint(0, len(partitions) - 1)
        partition = partitions[randIndex]
        sudir = os.path.join(partition, 'D' + str(sunum))
        os.mkdir(sudir)
        os.chmod(sudir, 0O2755)
        
        # Insert a record into the sum_partn_alloc table for this SU. status is DARW, which is 1. effective_date is "0". arch_sub is 0. group_id is 0. safe_id is 0. ds_index is 0.
        self.cmd = 'INSERT INTO ' + SUM_PARTN_ALLOC + "(wd, sumid, status, bytes, effective_date, archive_substatus, group_id, safe_id, ds_index) VALUES ('" + sudir + "', '" + str(self.request.data.sessionid) + "', " + str(DARW) + ", " + str(self.request.data.numbytes) + ", '0', 0, 0, 0, 0)"
        self.exeDbCmdNoResult()
        
        # To send to client.
        self.data['sunum'] = Response.intToHex(sunum)
        self.data['sudir'] = sudir
    
class PutResponse(Response):
    def __init__(self, request, dest=None):
        super(PutResponse, self).__init__(request)

        try:    
            # We have to change ownership of the SU files to the production user - ACK! This is really bad design. It seems like
            # the only solution without a better design is to call an external program that runs as setuid root. This program calls
            # chown recursively. It also make files read-only by calling chmod on all regular files. 
            
            # Save a mapping from SUDIR to SUNUM.
            sunums = {}
            
            partitionsNoDupes = set()
            for elem in self.request.data.sudirsNoDupes:
                [(suStr, path)] = elem.items()
                parts = Path(path).parts
                partition = os.path.join(parts[0], parts[1])
                partitionsNoDupes.add(partition)
                sunums[path] = suStr

            # sum_chmown does not do a good job of preventing the caller from changing ownership of an arbitrary
            # directory, so add a little more checking here. Make sure that all partitions containing the SUs being committed 
            # are valid SUMS partitions.
            self.dbRes = []
            self.cmd = 'SELECT count(*) FROM ' + SUM_PARTN_AVAIL + ' WHERE partn_name IN (' + ','.join([ "'" + partition + "'" for partition in partitionsNoDupes] ) + ')'
            self.exeDbCmd()
        
            if len(self.dbRes) != 1 or len(self.dbRes[0]) != 1:
                raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)
        
            if self.dbRes[0][0] != len(partitionsNoDupes):
                raise 'One or more invalid paritition paths.'

            if self.request.collector.hasTapeSys:
                apStatus = DAAP
            else:
                apStatus = DADP

            # This horrible program operates on a single SU at a time, so we have to call it in a loop.
            sudirs = []
            sus = []
            for elem in self.request.data.sudirsNoDupes:
                [(suStr, path)] = elem.items()
                sudirs.append("'" + path + "'")
                sus.append(suStr)

                cmdList = [ os.path.join(self.request.collector.sumsBinDir, 'sum_chmown'), path ]

                try:
                    check_call(cmdList)
                except CalledProcessError as exc:
                    raise Exception()

            # If all file permission and ownership changes succeed, then commit the SUs to the SUMS database.

            # The tape group was determined during the SUM_alloc() call and is now stored in SUM_PARTN_ALLOC (keyed by wd NOT ds_index).
            storageGroup = {} # Map SUNUM to storage group.
            allStorageGroups = set()
            self.dbRes = []
            # Ugh. SUMS does not insert the SUNUM during the SUM_alloc() call. It sets ds_index to 0. Use wd as the key.
            self.cmd = 'SELECT wd, group_id FROM ' + SUM_PARTN_ALLOC + ' WHERE wd IN (' +  ','.join(sudirs) + ')'
            self.exeDbCmd()
        
            if len(self.dbRes) != len(sudirs):
                raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd + '. Rows returned: ' + str(len(self.dbRes)))

            for row in self.dbRes:
                # map sunum to group
                storageGroup[sunums[row[0]]] = row[1]
                if str(row[1]) not in allStorageGroups:
                    allStorageGroups.add(str(row[1]))
            
            storageSet = {} # Map storage group to storage set.
            for group in allStorageGroups:
                # default to storage set 0 for all groups
                storageSet[group] = 0

            self.dbRes = []
            self.cmd = 'SELECT group_id, sum_set FROM ' + SUM_ARCH_GROUP + ' WHERE group_id IN (' + ','.join(allStorageGroups) + ')'
            self.exeDbCmd()
    
            if len(self.dbRes) > 0:
                if len(self.dbRes) != len(allStorageGroups):
                    raise DBCommandException('Unexpected DB response to cmd: ' + self.cmd)

                for row in self.dbRes:
                    # map group to storage set
                    storageSet[str(row[0])] = row[1]
        
            # Update SUMS sum_main database table - Calculate SU dir number of bytes, set online status to 'Y', set archstatus to 'N', 
            # set offsiteack to 'N', set dsname to seriesname, set storagegroup to tapegroup (determined in SUM_alloc()), set storageset 
            # to set determined in SUM_alloc(), set username to getenv('USER') or nouser if no USER env. Insert all of this into sum_main.
            suSize = {}
            for elem in self.request.data.sudirsNoDupes:
                [(suStr, path)] = elem.items()
                resolved = os.path.realpath(path)
                numBytes = os.path.getsize(resolved) + sum([ os.path.getsize(fullPath) for fullPath in [ os.path.join(root, afile) for root, dirs, files in os.walk(resolved) for afile in files ] ]) + sum([ os.path.getsize(fullPath) for fullPath in [ os.path.join(root, adir) for root, dirs, files in os.walk(resolved) for adir in dirs ] ])
                # Need to use to save this number into SUM_PARTN_ALLOC too.
                suSize[suStr] = numBytes

                self.cmd = 'INSERT INTO ' + SUM_MAIN + "(online_loc, online_status, archive_status, offsite_ack, history_comment, owning_series, storage_group, storage_set, bytes, ds_index, create_sumid, creat_date, access_date, username) VALUES ('" + path + "', 'Y', 'N', 'N', '', '" + self.request.data.series + "', " + str(storageGroup[suStr]) + ', ' + str(storageSet[str(storageGroup[suStr])]) + ', ' + str(numBytes) + ', ' + suStr + ', ' + str(self.request.data.sessionid) + ", localtimestamp, localtimestamp, '" + os.getenv('USER', 'nouser') + "')"
                self.exeDbCmdNoResult()
            
            if apStatus == DADP:
                # We do this simply to ensure that we do not have two sum_partn_alloc records with status DADP (delete pending).
                self.cmd = 'DELETE FROM ' + SUM_PARTN_ALLOC + ' WHERE ds_index IN (' + ','.join(sus) + ') AND STATUS = ' + str(DADP)
                self.exeDbCmdNoResult()

            # Set apstatus: if SUMS_TAPE_AVAILABLE ==> DAAP (4), else DADP (2), set archsub to one of DAAPERM, DAAEDDP, or DAADP, 
            # depending on flags, set effective_date to tdays in the future (with format "%04d%02d%02d%02d%02d"). safe_id is 0
            # (it looks obsolete). Insert all of this into sum_partn_alloc.
            if self.request.data.archivetype == 'permanent+archive' and self.request.collector.hasTapeSys:
                archsub = DAAPERM
            elif self.request.data.archivetype == 'temporary+noarchive':
                archsub = DAAEDDP
            elif self.request.data.archivetype == 'temporary+archive' and self.request.collector.hasTapeSys:
                archsub = DAADP
            else:
                archsub = DAAEDDP
        
            for elem in self.request.data.sudirsNoDupes:
                [(suStr, path)] = elem.items()
                self.cmd = 'INSERT INTO ' + SUM_PARTN_ALLOC + "(wd, sumid, status, bytes, effective_date, archive_substatus, group_id, safe_id, ds_index) VALUES ('" + path + "', " + str(self.request.data.sessionid) + ', ' + str(apStatus) + ', ' + str(suSize[suStr]) + ", to_char(CURRENT_TIMESTAMP + interval '" + str(abs(self.request.data.retention)) + " days', 'YYYYMMDDHH24MI'), " + str(archsub) + ', ' + str(storageGroup[suStr]) + ', 0, ' + suStr + ')'
                self.exeDbCmdNoResult()
            
            # To send to client.
            # Just 'ok'.
        except:
            # We have to clean up db rows that were created in SUM_alloc() and SUM_put(). The SUM_alloc() request was processed in 
            # a previous DB transaction, so it will have been committed by this point. Then re-raise so an error response is generated.
            
            # Undo SUM_alloc() insertions.
            self.cmd = 'DELETE FROM ' + SUM_PARTN_ALLOC + ' WHERE ds_index IN (' + ','.join(sus) + ')'
            self.exeDbCmdNoResult()
            
            self.cmd = 'DELETE FROM ' + SUM_MAIN + ' WHERE ds_index IN (' + ','.join(sus) + ')'
            self.exeDbCmdNoResult()
            
            raise


class DeleteseriesResponse(Response):
    def __init__(self, request, dest=None):
        super(DeleteseriesResponse, self).__init__(request)
        
        series = lower(self.request.data.series)

        # This update/join is a very quick operation. And if the series has no records, it is a quick noop.
        self.cmd = 'UPDATE ' + SUM_PARTN_ALLOC + ' AS T1 SET status = ' + str(DADP) + ", effective_date = '0', archive_substatus = " + str(DAADP) + ' FROM ' + SUM_MAIN + " AS T2 WHERE lower(T2.owning_series) = '" + series + "' AND T1.ds_index = T2.ds_index"
        self.exeDbCmdNoResult()
        
        # To send to client.
        # Just 'ok'.


class PingResponse(Response):
    """
    As long as we can respond with an 'ok', then SUMS is up and running.
    """
    def __init__(self, request, dest=None):
        super(PingResponse, self).__init__(request)

        # To send to client.
        # Just 'ok'.


class PollResponse(Response):
    def __init__(self, request, dest=None):
        super(PollResponse, self).__init__(request)
        
        tapeRequestID = self.request.data.requestID
        
        # Check with TapeRequestClient class to see if request has completed.
        reqStatus = TapeRequestClient.getTapeRequestStatus(tapeRequestID)
        if reqStatus == 'pending':
            self.data['taperead-requestid'] = tapeRequestID
        elif reqStatus == 'complete':
            origRequest = TapeRequestClient.getOrigRequest(tapeRequestID)
            origRequest.generateResponse(self) # This will make a GetResponse (so far, you can poll for GetResponse only).
        else:
            raise TaperequestException('Unexpected status returned by tape system: ' + reqStatus + '.')
        

class TapeRequestClient(threading.Thread):
    tMap = {} # Map taperead-requestid to (status, TapeRequestClient object)
    tMapLock = threading.Lock() # Guard tList access.
    maxThreads = 16
    
    def __init__(self, origRequest):
        self.origRequest = origRequest
        
    def run(self):
        # When the tape-request has completed, update the SUMS db tables, and set the request's status to 'complete'.
        pass
    
    @classmethod 
    def getTapeRequestStatus(cls, requestID):
        status = None
        try:
            TapeRequestClient.tMapLock.acquire()
            status = cls.tMap[requestID].status
        finally:
            TapeRequestClient.tMapLock.release()
                
        return status
        
    @classmethod
    def getOrigRequest(cls, requestID):
        try:
            TapeRequestClient.tMapLock.acquire()
            origRequest = cls.tMap[requestID].tapeRequest.origRequest
        finally:
            TapeRequestClient.tMapLock.release()


class Collector(threading.Thread):

    tList = [] # A list of running thread IDs.
    tListLock = threading.Lock() # Guard tList access.
    maxThreads = 32 # Default. Can be overriden with the Collector.setMaxThreads() method.
    eventMaxThreads = threading.Event() # Event fired when the number of threads decreases below threshold.
    
    MSGLEN_NUMBYTES = 8 # This is the hex-text version of the number of bytes in the response message.
                        # So, we can send back 4 GB of response!
    MAX_MSG_BUFSIZE = 4096 # Don't receive more than this in one call!
    
    def __init__(self, sock, host, port, database, user, hasTapeSys, hasMultPartSets, sumsBinDir, log, debugLog):
        threading.Thread.__init__(self)
        # Could raise. Handle in the code that creates the thread.
        self.dbconn = Dbconnection(host, port, database, user)            
        self.sock = sock
        self.hasTapeSys = hasTapeSys
        self.hasMultPartSets = hasMultPartSets
        self.sumsBinDir = sumsBinDir
        self.log = log
        self.debugLog = debugLog
        self.reqFactory = None
                
    def run(self):
        try:
            # The client must pass in some identifying information (other than their IP address).
            # Receive that information now.
            try:
                msgStr = self.receiveJson() # msgStr is a string object.
                self.extractClientInfo(msgStr)

                # First, obtain request.
                msgStr = self.receiveJson() # msgStr is a string object.
                
                if self.debugLog:
                    self.debugLog.write([ 'Request:\n' + msgStr ])
                
                self.extractRequest(msgStr) # Will raise if reqtype is not supported.

                if self.log:
                    self.log.write(['New ' + self.request.reqType + ' request from process ' + str(self.clientInfo.data.pid) + ' by user ' + self.clientInfo.data.user + ' at ' + str(self.sock.getpeername()) + ':\n' + str(self.request) ])

                msgStr = self.generateResponse() # A str object.
            except ReceiveJsonException as exc:
                msgStr = self.generateErrorResponse(RESPSTATUS_JSON, exc.args[0])
            except ClientInfoException as exc:
                msgStr = self.generateErrorResponse(RESPSTATUS_CLIENTINFO, exc.args[0])
            except ExtractRequestException as exc:
                msgStr = self.generateErrorResponse(RESPSTATUS_REQ, exc.args[0])
            except RequestTypeException as exc:
                msgStr = self.generateErrorResponse(RESPSTATUS_REQTYPE, exc.args[0])
            except GenerateResponseException as exc:
                log.write([ 'Failure creating response.' ])
                log.write([ exc.args[0] ])
                import traceback
                log.write([ traceback.format_exc(3) ])
                msgStr = self.generateErrorResponse(RESPSTATUS_GENRESPONSE, exc.args[0])


            if self.debugLog:
                self.debugLog.write([ 'Response:\n' + msgStr ])
            # Send results back on the socket, which is connected to a single DRMS module. By sending the results
            # back, the client request is completed. We want to construct a list of "SUM_info" objects. Each object
            # { sunum:12592029, onlineloc:'/SUM52/D12592029', ...}
            self.sendJson(msgStr) # Expects a str object.
            
            # This thread is about to terminate. We don't want to end this thread before
            # the client closes the socket though. Otherwise, our socket will get stuck in 
            # the TIME_WAIT state. So, perform another read, and end the thread after the client
            # has broken the connection. recv() will block till the client kills the connection
            # (or it inappropriately sends more data over the connection).
            textReceived = self.sock.recv(Collector.MAX_MSG_BUFSIZE)
            if textReceived == b'':
                # The client closed their end of the socket.
                if self.debugLog:
                    self.debugLog.write(['Client at ' + str(self.sock.getpeername()) + ' properly terminated connection.'])
            else:
                raise SocketConnectionException('Client sent extraneous data over socket connection.')

        except SocketConnectionException as exc:
            # Don't send message back - we can't communicate with the client properly, so only log a message on the server side.
            log.write(['There was a problem communicating with client ' + str(self.sock.getpeername()) + '.'])
            log.write([ exc.args[0] ])
        except Exception as exc:
            import traceback
            log.write([ traceback.format_exc(5) ])

        # We need to check the class tList variable to update it, so we need to acquire the lock.
        try:
            Collector.tListLock.acquire()
            if self.debugLog:
                self.debugLog.write(['Class Collector acquired Collector lock for client ' + str(self.sock.getpeername()) + '.'])
            Collector.tList.remove(self) # This thread is no longer one of the running threads.
            if len(Collector.tList) == Collector.maxThreads - 1:
                # Fire event so that main thread can add new SUs to the download queue.
                Collector.eventMaxThreads.set()
                # Clear event so that main will block the next time it calls wait.
                Collector.eventMaxThreads.clear()
        except Exception as exc:
            import traceback
            log.write(['There was a problem closing the Collector thread for client ' + str(self.sock.getpeername()) + '.'])
            log.write([ traceback.format_exc(0) ])
        finally:
            Collector.tListLock.release()
            if self.debugLog:
                self.debugLog.write(['Class Collector released Collector lock for client ' + str(self.sock.getpeername()) + '.'])
                
            # Close DB connection.
            self.dbconn.close()

            # Always shut-down server-side of client socket pair.
            if self.debugLog:
                self.debugLog.write(['Shutting down server side of client socket ' + str(self.sock.getpeername()) + '.'])
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            
    def extractClientInfo(self, msg):
        # msg is JSON:
        # {
        #    "pid" : 1946,
        #    "user" : "TheDonald"
        # }
        # 
        # The pid is a JSON number, which could be a double string. But the client
        # will make sure that the number is a 32-bit integer.
        if self.debugLog:
            self.debugLog.write([str(self.sock.getpeername()) + ' extracting client info.'])
        
        clientInfo = Unjsonizer(msg)
        
        self.clientInfo = DataObj()
        self.clientInfo.data = DataObj()
        self.clientInfo.data.pid = clientInfo.unjsonized['pid']
        self.clientInfo.data.user = clientInfo.unjsonized['user']

    def extractRequest(self, msg):
        if not self.reqFactory:
            self.reqFactory = RequestFactory(self)
            
        self.request = self.reqFactory.getRequest(msg)
        
    def generateResponse(self):
        try:
            self.response = self.request.generateResponse()
            self.response.setStatus(RESPSTATUS_OK)
        except Exception as exc:
            # Create a response with a non-OK status and an error message.
            raise GenerateResponseException(exc.args[0])
        return self.response.getJSON()
        
    def generateErrorResponse(self, status, errMsg):
        self.response = self.request.generateErrorResponse(status, errMsg)
        return self.response.getJSON()
        
    # msg is a bytes object.
    def sendMsg(self, msg):
        # First send the length of the message.
        bytesSentTotal = 0
        numBytesMessage = '{:08x}'.format(len(msg))
        
        while bytesSentTotal < Collector.MSGLEN_NUMBYTES:
            bytesSent = self.sock.send(bytearray(numBytesMessage[bytesSentTotal:], 'UTF-8'))
            if not bytesSent:
                raise SocketConnectionException('Socket broken - cannot send message-length data to client.')
            bytesSentTotal += bytesSent
        
        # Then send the message.
        bytesSentTotal = 0
        while bytesSentTotal < len(msg):
            bytesSent = self.sock.send(msg[bytesSentTotal:])
            if not bytesSent:
                raise SocketConnectionException('Socket broken - cannot send message data to client.')
            bytesSentTotal += bytesSent
            
        if self.debugLog:
            self.debugLog.write([str(self.sock.getpeername()) + ' - sent ' + str(bytesSentTotal) + ' bytes response.'])
    
    # Returns a bytes object.
    def receiveMsg(self):
        # First, receive length of message.
        allTextReceived = b''
        bytesReceivedTotal = 0
        
        while bytesReceivedTotal < Collector.MSGLEN_NUMBYTES:
            textReceived = self.sock.recv(min(Collector.MSGLEN_NUMBYTES - bytesReceivedTotal, Collector.MAX_MSG_BUFSIZE))
            if textReceived == b'':
                raise SocketConnectionException('Socket broken - cannot receive message-length data from client.')
            allTextReceived += textReceived
            bytesReceivedTotal += len(textReceived)
            
        # Convert hex string to number.
        numBytesMessage = int(allTextReceived.decode('UTF-8'), 16)
        
        # Then receive the message.
        allTextReceived = b''
        bytesReceivedTotal = 0
        
        while bytesReceivedTotal < numBytesMessage:
            textReceived = self.sock.recv(min(numBytesMessage - bytesReceivedTotal, Collector.MAX_MSG_BUFSIZE))
            if textReceived == b'':
                raise SocketConnectionException('Socket broken - cannot receive message data from client.')
            allTextReceived += textReceived
            bytesReceivedTotal += len(textReceived)
        # Return a bytes object (not a string). The unjsonize function will need a str object for input.
        return allTextReceived
        
    # msg is a str object.
    def sendJson(self, msgStr):
        msgBytes = bytes(msgStr, 'UTF-8')
        self.sendMsg(msgBytes)

    def receiveJson(self):
        msgBytes = self.receiveMsg() # A bytes object, not a str object. json.loads requires a str object.
        return msgBytes.decode('UTF-8') # Convert bytes to str.
            
    # Must acquire Collector lock BEFORE calling newThread() since newThread() will append to tList (the Collector threads will be deleted from tList as they complete).
    @staticmethod
    def newThread(sock, host, port, database, user, hasTapeSys, hasMultPartSets, sumsBinDir, log, debugLog):
        coll = Collector(sock, host, port, database, user, hasTapeSys, hasMultPartSets, sumsBinDir, log, debugLog)
        coll.tList.append(coll)
        coll.start()
        
    @staticmethod
    def dumpBytes(msg):
        print(str(["{0:0>2X}".format(b) for b in msg]))

    @classmethod
    def lockTList(cls):
        cls.tListLock.acquire()
        
    @classmethod
    def unlockTList(cls):
        cls.tListLock.release()
        
    @classmethod
    def getNumThreads(cls):
        return len(cls.tList)
        
    @classmethod
    def freeThreadExists(cls):
        return len(cls.tList) < cls.maxThreads
        
    @classmethod
    def waitForFreeThread(cls):
        cls.eventMaxThreads.wait()

    @classmethod
    def removeThreadFromList(cls, thread):
        cls.tList.remove(thread)

    @classmethod
    def setMaxThreads(cls, maxThreads):
        cls.maxThreads = maxThreads


class TestClient(threading.Thread):

    MSGLEN_NUMBYTES = 8
    MAX_MSG_BUFSIZE = 4096

    def __init__(self, sock, serverPort, log, debugLog):
        threading.Thread.__init__(self)
        self.sock = sock
        self.serverPort = serverPort
        self.log = log
        self.debugLog = debugLog
    
    def run(self):
        # First, connect to the server.
        try:
            self.sock.connect((socket.gethostname(), self.serverPort))
        
            # Send some random SUNUMs to the server thread (one is invalid - 123456789).
            request = {[650547410, 650547419, 650547430, 650551748, 123456789, 650551852, 650551942, 650555939, 650556333]}
            msgStr = self.jsonizeRequest(request)
            msgBytes = bytes(msgStr, 'UTF-8')
            self.sendRequest(msgBytes)
            msgBytes = self.receiveResponse()
            msgStr = msgBytes.decode('UTF-8')
            response = self.unjsonizeResponse(msgStr)
            
            self.dumpsInfoList(response)
        except Exception as exc:
            import traceback
            log.write(['Client ' + str(self.sock.getsockname()) + ' had a problem communicating with the server.'])
            log.write([traceback.format_exc(0)])
        finally:
            self.debugLog.write(['Closing test client socket.'])
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        
    def jsonizeRequest(self, request):
        return json.dumps(request)
        
    def unjsonizeResponse(self, msg):
        infoDict = json.loads(msg)
        infoList = infoDict['suinfolist']
        # We now have a list of Info objects.
        return infoList
    
    # msg is a bytes object.
    def sendRequest(self, msg):
        # First send the length of the message.
        bytesSentTotal = 0
        numBytesMessage = '{:08x}'.format(len(msg))
        
        while bytesSentTotal < TestClient.MSGLEN_NUMBYTES:
            bytesSent = self.sock.send(bytearray(numBytesMessage[bytesSentTotal:], 'UTF-8'))
            if not bytesSent:
                raise SocketConnectionException('Socket broken.')
            bytesSentTotal += bytesSent
        
        # Then send the message.
        bytesSentTotal = 0
        while bytesSentTotal < len(msg):
            bytesSent = self.sock.send(msg[bytesSentTotal:])
            if not bytesSent:
                raise SocketConnectionException('Socket broken.')
            bytesSentTotal += bytesSent
    
    # Returns a bytes object.
    def receiveResponse(self):
        # First, receive length of message.
        allTextReceived = b''
        bytesReceivedTotal = 0
        
        while bytesReceivedTotal < TestClient.MSGLEN_NUMBYTES:
            textReceived = self.sock.recv(min(TestClient.MSGLEN_NUMBYTES - bytesReceivedTotal, TestClient.MAX_MSG_BUFSIZE))
            if textReceived == b'':
                raise SocketConnectionException('Socket broken.')
            allTextReceived += textReceived
            bytesReceivedTotal += len(textReceived)
            
        # Convert hex string to number.
        numBytesMessage = int(allTextReceived.decode('UTF-8'), 16)
        
        # Then receive the message.
        allTextReceived = b''
        bytesReceivedTotal = 0
        
        while bytesReceivedTotal < numBytesMessage:
            textReceived = self.sock.recv(min(numBytesMessage - bytesReceivedTotal, TestClient.MAX_MSG_BUFSIZE))
            if textReceived == b'':
                raise SocketConnectionException('Socket broken.')
            allTextReceived += textReceived
            bytesReceivedTotal += len(textReceived)
        return allTextReceived
        
    def dumpsInfoList(self, infoList):
        for infoDict in infoList:
            self.debugLog.write(['sunum=' + str(infoDict['sunum'])])
            self.debugLog.write(['path=' + infoDict['onlineLoc']])
            self.debugLog.write(['status=' + infoDict['onlineStatus']])
            self.debugLog.write(['archstatus=' + infoDict['archiveStatus']])
            self.debugLog.write(['ack=' + infoDict['offsiteAck']])
            self.debugLog.write(['comment=' + infoDict['historyComment']])
            self.debugLog.write(['series=' + infoDict['owningSeries']])
            self.debugLog.write(['group=' + str(infoDict['storageGroup'])])
            self.debugLog.write(['size=' + str(infoDict['bytes'])])
            self.debugLog.write(['create=' + infoDict['creatDate']])
            self.debugLog.write(['user=' + infoDict['username']])
            self.debugLog.write(['tape=' + infoDict['archTape']])
            self.debugLog.write(['tapefn=' + str(infoDict['archTapeFn'])])
            self.debugLog.write(['tapedate=' + infoDict['archTapeDate']])
            self.debugLog.write(['safetape=' + infoDict['safeTape']])
            self.debugLog.write(['safetapefn=' + str(infoDict['safeTapeFn'])])
            self.debugLog.write(['safetapedate=' + infoDict['safeTapeDate']])
            self.debugLog.write(['pastatus=' + str(infoDict['paStatus'])])
            self.debugLog.write(['pasubstatus=' + str(infoDict['paSubstatus'])])
            self.debugLog.write(['effdate=' + infoDict['effectiveDate']])

class SignalThread(threading.Thread):

    sdLock = threading.Lock() # Guard self.shutDown
    
    def __init__(self, sigset, log):
        threading.Thread.__init__(self)
    
        # Block the signals in the main thread.
        signal.pthread_sigmask(signal.SIG_SETMASK, sigset)
        self.mask = sigset
        self.shutDown = False
        
    def run(self):
        while True:
            print('point A - mask is ' + str(self.mask))
            signo = signal.sigwait(self.mask)
            print('point B')
            log.write(['Signal thread received signal ' + str(signo) + '.'])
            
            if signo == signal.SIGINT:
                try:
                    SignalThread.sdLock.acquire()
                    self.shutDown = True
                finally:
                    SignalThread.sdLock.release()
            else:
                # This handler does not handle this signal.
                log.write(['SUMS server received unrecognized signal ' + str(signo) + '.'])

            # Kill signal thread            
            break
                    
    def isShuttingDown(self):
        try:
            SignalThread.sdLock.acquire()
            rv = self.shutDown
        finally:
            SignalThread.sdLock.release()
        return rv

if __name__ == "__main__":
    rv = RV_SUCCESS
    
    try:
        sumsDrmsParams = SumsDrmsParams()
        if sumsDrmsParams is None:
            raise ParamsException('Unable to locate DRMS parameters file (drmsparams.py).')
            
        parser = CmdlParser(usage='%(prog)s [ -dht ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user>] [ --logfile=<log-file name> ]')
        parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains the series table from which records are to be deleted.', metavar='<db host machine>', dest='dbhost', default=sumsDrmsParams.get('SUMS_DB_HOST'))
        parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database that contains the series table from which records are to be deleted.', metavar='<db host port>', dest='dbport', default=sumsDrmsParams.get('SUMPGPORT'))
        parser.add_argument('-N', '--dbname', help='The name of the database that contains the series table from which records are to be deleted.', metavar='<db name>', dest='database', default=sumsDrmsParams.get('DBNAME') + '_sums')
        parser.add_argument('-U', '--dbuser', help='The name of the database user account.', metavar='<db user>', dest='dbuser', default=sumsDrmsParams.get('SUMS_MANAGER'))
        parser.add_argument('-l', '--logfile', help='The file to which logging is written.', metavar='<file name>', dest='logfile', default=os.path.join(sumsDrmsParams.get('SUMLOG_BASEDIR'), SUMSD + '_' + datetime.now().strftime('%Y%m%d') + '.txt'))
        parser.add_argument('-m', '--maxconn' , help='The maximum number of simultaneous SUMS connections.', metavar='<max connections>', dest='maxconn', default=sumsDrmsParams.get('SUMSD_MAX_THREADS'))
        parser.add_argument('-d', '--debug', help='Print debug statements into a debug log.', dest='debug', action='store_true', default=False)
        parser.add_argument('-t', '--test', help='Create a client thread to test the server.', dest='test', action='store_true', default=False)
        
        arguments = Arguments(parser)
        
        Collector.setMaxThreads(int(arguments.getArg('maxconn')))
        pid = os.getpid()
        
        # The main log file - it is sparse.
        log = Log(arguments.getArg('logfile'))
        
        log.write(['Starting sumsd.py server.'])
        
        # The debug log file - it is a little more verbose than the main log file.
        debugLog = None
        if arguments.getArg('debug'):
            if arguments.getArg('logfile').find('.txt') != -1:
                debugLogFile = arguments.getArg('logfile').replace('.txt', '.dbg.txt')
            else:
                debugLogFile = arguments.getArg('logfile') + '.dbg.txt'
            
            debugLog = Log(debugLogFile)
                            
        serverSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind to any IP address that this server is running on - the empty string
        # represent INADDR_ANY. I DON'T KNOW HOW TO MAKE THIS WORK!!
        # serverSock.bind(('', int(sumsDrmsParams.get('SUMSD_LISTENPORT'))))
        serverSock.bind((socket.gethostname(), int(sumsDrmsParams.get('SUMSD_LISTENPORT'))))
        serverSock.listen(5)
        log.write(['Listening for client requests on ' + str(serverSock.getsockname()) + '.'])

        # Something cool. If the test flag is set, then create another thread that sends a SUM_info request to the main thread.
        # At this point, the server is listening, so it is OK to try to connect to it.
        if arguments.getArg('test'):
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client = TestClient(clientSocket, int(sumsDrmsParams.get('SUMSD_LISTENPORT')), log, debugLog)
            client.start()
            
        # Make a signal-handler thread so we can shut-down cleanly.
        # sigThread = SignalThread({signal.SIGINT}, log)
        # sigThread.start()

        pollObj = select.poll()
        pollObj.register(serverSock, select.POLLIN | select.POLLPRI)

        # while not sigThread.isShuttingDown():
        try:
            while True:
                try:
                    fdList = pollObj.poll(500)
                except IOError as exc:
                    raise PollException('A failure occurred while checking for new client connections.')
            
                if len(fdList) == 0:
                    # Nobody knocking on the door.
                    continue
                else:    
                    (clientSock, address) = serverSock.accept()
                    if debugLog:
                        debugLog.write(['Accepting a client request from ' + address[0] + ' on port ' + str(address[1]) + '.'])
            
                    while True:
                        Collector.lockTList()
                        try:
                            if Collector.freeThreadExists():
                                if debugLog:
                                    debugLog.write(['Instantiating a Collector for client ' + str(address) + '.'])
                                Collector.newThread(clientSock, arguments.getArg('dbhost'), arguments.getArg('dbport'), arguments.getArg('database'), arguments.getArg('dbuser'), int(sumsDrmsParams.get('SUMS_TAPE_AVAILABLE')) == 1, int(sumsDrmsParams.get('SUMS_MULTIPLE_PARTNSETS')) == 1, sumsDrmsParams.get('SUMBIN_BASEDIR'), log, debugLog)
                                break # The finally clause will ensure the Collector lock is released.
                        finally:
                            Collector.unlockTList()
                    
                        # There were no free threads. Wait until there is a free thread.
                        Collector.waitForFreeThread()
                
                        # We woke up, because a free thread became available. However, that thread could 
                        # now be in use. Loop and check again.
        except KeyboardInterrupt:
            # Shut down things if the user hits ctrl-c.
            pass
        
        pollObj.unregister(serverSock)
        
        # Kill server socket.
        log.write(['Closing server socket.'])
        serverSock.shutdown(socket.SHUT_RDWR)
        serverSock.close()
        
    except Exception as exc:
        if len(exc.args) == 2:
            type = exc.args[0]
            
            if type == 'drmsParams':
                rv = RV_DRMSPARAMS
            elif type == 'args':
                rv = RV_ARGS
            elif type == 'badLogfile':
                rv = RV_BADLOGFILE
            elif type == 'badLogwrite':
                rv = RV_BADLOGWRITE
            elif type == 'dbCommand':
                rv = RV_DBCOMMAND
            elif type == 'dbConnection':
                rv = RV_DBCONNECTION
            elif type == 'dbCursor':
                rv = RV_DBCURSOR
            elif type == 'socketConnection':
                rv = RV_SOCKETCONN
            elif type == 'unknownRequestType':
                rv = RV_REQUESTTYPE
            elif type == 'poll':
                rv = RV_POLL
            else:
                raise
                
            msg = exc.args[1]
                
            print(msg, file=sys.stderr)
        else:
            raise

sys.exit(rv)
