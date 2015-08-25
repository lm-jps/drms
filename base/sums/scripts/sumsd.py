#!/usr/bin/env python

from __future__ import print_function
import sys
import re
import os
import threading
import socket
import signal
import select
from datetime import datetime, timedelta
import re
import json
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser


SUMSD = 'sumsd'
SUM_MAIN = 'public.sum_main'
SUM_PARTN_ALLOC = 'public.sum_partn_alloc'
            
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

class SumsDrmsParams(DRMSParams):

    def __init__(self):
        super(SumsDrmsParams, self).__init__()

    def get(self, name):
        val = super(SumsDrmsParams, self).get(name)

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


class RowGenerator(object):

    def __init__(self, listOfRows):
        self.rows = listOfRows
    
    def __iter__(self):
        return self.iterate()
    
    # Iterate through rows.
    def iterate(self):
        i = 0
        while i < len(self.rows):
            yield self.rows[i]
            i += 1


class Dbconnection(object):

    def __init__(self, host, port, database, user):
        self.conn = None
        self.cursor = None
        self.destroy = False # If True, then the destructor has been called.
        
        # Connect to the db. If things succeed, then save the db-connection information.
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.openConnection()
        self.openCursor()
        
    def __del__(self):
        self.destroy = True
        self.close()
        
    def commit(self):
        if not self.conn:
            raise Exception('dbCommand', 'Cannot commit - no database connection exists.')
            
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if not self.conn:
            raise Exception('dbCommand', 'Cannot rollback - no database connection exists.')

        if self.conn:
            self.conn.rollback()

    def close(self):
        self.closeCursor()
        self.closeConnection()
            
    def openConnection(self):
        if self.conn:
            raise Exception('dbConnection', 'Already connected to the database.')
            
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user)
        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection
            if hasattr(exc, 'diag') and hasattr(exc.diag, 'message_primary'):
                msg = exc.diag.message_primary
            else:
                msg = 'Unable to connect to the database (no, I do not know why).'
            raise Exception('dbConnection', msg)
        
        self.conn = conn
    
    def closeConnection(self):    
        if not self.destroy and not self.conn:
            raise Exception('dbConnection', 'There is no database connection.')
        
        if self.conn:
            self.conn.close()
            
    def openCursor(self):
        if self.cursor:
            raise Exception('dbCursor', 'Cursor already exists.')

        if not self.conn:
            raise Exception('dbCursor', 'Cannot create cursor - no database connection exists.')

        try:
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError as exc:
            raise Exception('dbCursor', exc.diag.message_primary)
            
    def closeCursor(self):
        if not self.destroy and not self.cursor:
            raise Exception('dbCursor', 'Cursor does not exist.')
            
        if self.cursor:
            self.cursor.close()

    def exeCmd(self, cmd):
        if not self.conn or not self.cursor:
            raise Exception('dbCommand', 'Cannot execute database command ' + cmd + ' - no database connection exists.')

        try:
            self.cursor.execute(cmd)
            rows = self.cursor.fetchall()
        except psycopg2.Error as exc:
            # Handle database-command errors.
            raise Exception('dbCommand', exc.diag.message_primary)

        return RowGenerator(rows)    

class DataObj(object):
    pass
    
class Jsonizer(object):
    def __init__(self, response, sus):
        self.response = response
        self.sus = sus
        self.json = None
        self.data = None
    
    def jsonize(self):
        self.json = json.dumps(self.data)
        
    def getJSON(self):
        return self.json
    
    @staticmethod    
    def stripHexPrefix(hexadecimal):
        regexp = re.compile(r'^\s*0x(\S+)', re.IGNORECASE)
        match = regexp.match(hexadecimal)
        if match:
            return match.group(1)
        else:
            return hexadecimal
    
class SuminfoJsonizer(Jsonizer):
    # response is a SuminfoResponse object. The db response text is in response.response.
    def __init__(self, response, sus):
        super(SuminfoJsonizer, self).__init__(response, sus)
        infoList = []
        processed = {}
        
        # Make an object from the arrays returned by the database.
        for row in response.getDbResponse():
            rowIter = iter(row)
            infoDict = {}
            infoDict['sunum'] = Jsonizer.stripHexPrefix(hex(rowIter.__next__())) # Convert to hex string since some parsers do not support 64-bit integers.
            infoDict['onlineLoc'] = rowIter.__next__()
            infoDict['onlineStatus'] = rowIter.__next__()
            infoDict['archiveStatus'] = rowIter.__next__()
            infoDict['offsiteAck'] = rowIter.__next__()
            infoDict['historyComment'] = rowIter.__next__()
            infoDict['owningSeries'] = rowIter.__next__()
            infoDict['storageGroup'] = rowIter.__next__()
            infoDict['bytes'] = Jsonizer.stripHexPrefix(hex(rowIter.__next__())) # Convert to hex string since some parsers do not support 64-bit integers.
            infoDict['createSumid'] = rowIter.__next__()
            # The db returns a datetime object. Convert the datetime to a str object.
            infoDict['creatDate'] = rowIter.__next__().strftime('%Y-%m-%d %T')
            infoDict['username'] = rowIter.__next__()
            infoDict['archTape'] = rowIter.__next__()
            infoDict['archTapeFn'] = rowIter.__next__()
            # The db returns a datetime object. Convert the datetime to a str object.
            infoDict['archTapeDate'] = rowIter.__next__().strftime('%Y-%m-%d %T')
            infoDict['safeTape'] = rowIter.__next__()
            infoDict['safeTapeFn'] = rowIter.__next__()
            # The db returns a datetime object. Convert the datetime to a str object.
            infoDict['safeTapeDate'] = rowIter.__next__().strftime('%Y-%m-%d %T')
            infoDict['effectiveDate'] = rowIter.__next__()
            infoDict['paStatus'] = rowIter.__next__()
            infoDict['paSubstatus'] = rowIter.__next__()
            
            # Put SU in hash of processed SUs.
            suStr = str(int(infoDict['sunum'], 16)) # Convert hexadecimal string to decimal string.
            processed[suStr] = infoDict
        
        for su in sus:
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
                infoDict['sunum'] = Jsonizer.stripHexPrefix(hex(su)) # Convert to hex string since some parsers do not support 64-bit integers.
                infoDict['onlineLoc'] = ''
                infoDict['onlineStatus'] = ''
                infoDict['archiveStatus'] = ''
                infoDict['offsiteAck'] = ''
                infoDict['historyComment'] = ''
                infoDict['owningSeries'] = ''
                infoDict['storageGroup'] = -1
                infoDict['bytes'] = Jsonizer.stripHexPrefix(hex(0)) # In sum_main, bytes is a 64-bit integer. In SUM_info, it is a double. sum_open.c converts the integer (long) to a floating-point number.
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
        self.data = { 'suinfolist' : infoList }
        self.jsonize()
        
class Unjsonizer(object):
    def __init__(self, jsonStr):
        self.json = jsonStr
        self.unjsonized = json.loads(jsonStr) # JSON objects are converted to Python dictionaries!
        self.data = DataObj()
        
class ClientinfoUnjsonizer(Unjsonizer):
    # msg is JSON:
    # {
    #    "pid" : 1946,
    #    "user" : "TheDonald"
    # }
    # 
    # The pid is a JSON number, which could be a double string. But the client
    # will make sure that the number is a 32-bit integer.
    def __init__(self, jsonStr):
        super(ClientinfoUnjsonizer, self).__init__(jsonStr)
        self.data.pid = self.unjsonized['pid']
        self.data.user = self.unjsonized['user']
        
class SuminfoUnjsonizer(Unjsonizer):
    # msg is JSON:
    # {
    #    "reqtype" : "infoArray",
    #    "sulist" : [ "3039", "5BA0" ]
    # }
    def __init__(self, jsonStr=None, unjsonizer=None):
        if jsonStr:
            super(SuminfoUnjsonizer, self).__init__(jsonStr)
        elif unjsonizer:
            self.json = unjsonizer.json
            self.unjsonized = unjsonizer.unjsonized
            self.data = DataObj()
        else:
            raise Exception('invalidArgument', 'Must supply either json or Unjsonizer to SuminfoUnjsonizer().')
            
        self.data.reqType = self.unjsonized['reqtype']
        # Convert array of hexadecimal strings to array of integers.
        self.data.sus = [ int(suStr, 16) for suStr in self.unjsonized['sulist'] ]
    
    @classmethod
    def fromJson(cls, msg):
        # Check that obj is an instance of cls.
        return cls(jsonStr=msg)
            
    @classmethod
    def fromObj(cls, obj):
        # Check that obj is an instance of cls.
        return cls(unjsonizer=obj)
        
class Response(object):
    def __init__(self, debugLog, dbconn, suList):
        self.debugLog = debugLog
        self.dbconn = dbconn
        self.suList = suList
        self.response = None

    def exeDbCmd(self):
        if self.debugLog:
            self.debugLog.write(['db command is: ' + self.cmd])
        return self.dbconn.exeCmd(self.cmd)
        
    def getDbResponse(self):
        return self.response
        
class SuminfoResponse(Response):
    def __init__(self, debugLog, dbconn, suList):
        super(SuminfoResponse, self).__init__(debugLog, dbconn, suList)
        self.exeDbCmd()
        
    def exeDbCmd(self):
        self.cmd = "SELECT T1.ds_index, T1.online_loc, T1.online_status, T1.archive_status, T1.offsite_ack, T1.history_comment, T1.owning_series, T1.storage_group, T1.bytes, T1.create_sumid, T1.creat_date, T1.username, COALESCE(T1.arch_tape, 'N/A'), COALESCE(T1.arch_tape_fn, 0), COALESCE(T1.arch_tape_date, '1958-01-01 00:00:00'), COALESCE(T1.safe_tape, 'N/A'), COALESCE(T1.safe_tape_fn, 0), COALESCE(T1.safe_tape_date, '1958-01-01 00:00:00'), COALESCE(T2.effective_date, '195801010000'), coalesce(T2.status, 0), coalesce(T2.archive_substatus, 0) FROM " + SUM_MAIN + " AS T1 LEFT OUTER JOIN " + SUM_PARTN_ALLOC + " AS T2 ON (T1.ds_index = T2.ds_index) WHERE T1.ds_index IN (" + ','.join(self.suList) + ')'
        self.response = super(SuminfoResponse, self).exeDbCmd()

class Collector(threading.Thread):

    tList = [] # A list of running thread IDs.
    tListLock = threading.Lock() # Guard tList access.
    maxThreads = 32 # Default. Can be overriden with the Collector.setMaxThreads() method.
    eventMaxThreads = threading.Event() # Event fired when the number of threads decreases below threshold.
    
    MSGLEN_NUMBYTES = 8 # This is the hex-text version of the number of bytes in the response message.
                        # So, we can send back 4 GB of response!
    MAX_MSG_BUFSIZE = 4096 # Don't receive more than this in one call!
    REQUEST_TYPE = { '0':'none', '1': 'infoArray'}
    
    def __init__(self, sock, host, port, database, user, log, debugLog):
        threading.Thread.__init__(self)
        # Could raise. Handle in the code that creates the thread.
        self.dbconn = Dbconnection(host, port, database, user)            
        self.sock = sock
        self.log = log
        self.debugLog = debugLog
                
    def run(self):
        try:
            # The client must pass in some identifying information (other than their IP address).
            # Receive that information now.
            msgStr = self.receiveJson() # msgStr is a string object.
            self.extractClientInfo(msgStr)

            # First, obtain request.
            msgStr = self.receiveJson() # msgStr is a string object.
            self.extractRequest(msgStr) # Will raise if reqtype is not supported.

            if self.log:
                self.log.write(['New ' + self.request.data.reqType + ' request from process ' + str(self.clientInfo.data.pid) + ' by user ' + self.clientInfo.data.user + ' at ' + str(self.sock.getpeername()) + ': ' + ','.join(self.suList) + '.'])

            msgStr = self.generateResponse() # A str object.

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
                    self.debugLog.write(['Client at ' + str(self.sock.getpeername()) + ' terminated connection.'])
            else:
                raise Exception('socketConnection', 'Client sent extraneous data over socket connection.')

        except Exception as exc:
            import traceback
            log.write(['There was a problem communicating with client ' + str(self.sock.getpeername()) + '.'])
            log.write([traceback.format_exc(0)])

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
            log.write([traceback.format_exc(0)])
        finally:
            Collector.tListLock.release()
            if self.debugLog:
                self.debugLog.write(['Class Collector released Collector lock for client ' + str(self.sock.getpeername()) + '.'])

            # Always shut-down server-side of client socket pair.
            if self.debugLog:
                self.debugLog.write(['Shutting down server side of client socket ' + str(self.sock.getpeername()) + '.'])
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            
    def extractRequest(self, msg):
        request = Unjsonizer(msg)

        if request.unjsonized['reqtype'] == Collector.REQUEST_TYPE['1']:
            self.request = SuminfoUnjsonizer.fromObj(request)
        else:
            raise Exception('unknownRequestType', 'The request type ' + request.unjsonized['reqtype'] + ' is not supported.')
            
        processed = {}
        self.suList = []
        
        if self.debugLog:
            self.debugLog.write([str(self.sock.getpeername()) + ' - requested SUs: ' + ','.join([str(item) for item in self.request.data.sus])])
         
        # suList may contain duplicates. They must be removed.
        for su in self.request.data.sus:
            if str(su) not in processed:        
                self.suList.append(str(su)) # Make a list of strings - we'll need to concatenate the elements into a comma-separated list for the DB query.
                
    def extractClientInfo(self, msg):
        if self.debugLog:
            self.debugLog.write([str(self.sock.getpeername()) + ' extracting client info.'])
        self.clientInfo = ClientinfoUnjsonizer(msg);
        
    def generateResponse(self):
        if self.request.data.reqType == Collector.REQUEST_TYPE['1']:
            # response contains the database-command Pythonized response, unjsonized.
            response = SuminfoResponse(self.debugLog, self.dbconn, self.suList)
            # Jsonize response.
            jsonizer = SuminfoJsonizer(response, self.request.data.sus)
        else:
            raise Exception('unknownRequestType', 'The request type ' + self.request.data.reqType + ' is not supported.')
            
        return jsonizer.getJSON()
        
    # msg is a bytes object.
    def sendMsg(self, msg):
        # First send the length of the message.
        bytesSentTotal = 0
        numBytesMessage = '{:08x}'.format(len(msg))
        
        while bytesSentTotal < Collector.MSGLEN_NUMBYTES:
            bytesSent = self.sock.send(bytearray(numBytesMessage[bytesSentTotal:], 'UTF-8'))
            if not bytesSent:
                raise Exception('socketConnection', 'Socket broken.')
            bytesSentTotal += bytesSent
        
        # Then send the message.
        bytesSentTotal = 0
        while bytesSentTotal < len(msg):
            bytesSent = self.sock.send(msg[bytesSentTotal:])
            if not bytesSent:
                raise Exception('socketConnection', 'Socket broken.')
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
                raise Exception('socketConnection', 'Socket broken.')
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
                raise Exception('socketConnection', 'Socket broken.')
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
    def newThread(sock, host, port, database, user, log, debugLog):
        coll = Collector(sock, host, port, database, user, log, debugLog)
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
                raise Exception('socketConnection', 'Socket broken.')
            bytesSentTotal += bytesSent
        
        # Then send the message.
        bytesSentTotal = 0
        while bytesSentTotal < len(msg):
            bytesSent = self.sock.send(msg[bytesSentTotal:])
            if not bytesSent:
                raise Exception('socketConnection', 'Socket broken.')
            bytesSentTotal += bytesSent
    
    # Returns a bytes object.
    def receiveResponse(self):
        # First, receive length of message.
        allTextReceived = b''
        bytesReceivedTotal = 0
        
        while bytesReceivedTotal < TestClient.MSGLEN_NUMBYTES:
            textReceived = self.sock.recv(min(TestClient.MSGLEN_NUMBYTES - bytesReceivedTotal, TestClient.MAX_MSG_BUFSIZE))
            if textReceived == b'':
                raise Exception('socketConnection', 'Socket broken.')
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
                raise Exception('socketConnection', 'Socket broken.')
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
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')
            
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
                    raise Exception('poll', 'A failure occurred while checking for new client connections.')
            
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
                                Collector.newThread(clientSock, arguments.getArg('dbhost'), arguments.getArg('dbport'), arguments.getArg('database'), arguments.getArg('dbuser'), log, debugLog)
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
