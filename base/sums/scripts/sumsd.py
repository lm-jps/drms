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
import pickle
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
RV_POLL = 9

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

class Info(object):
    pass

class Collector(threading.Thread):

    tList = [] # A list of running thread IDs.
    tListLock = threading.Lock() # Guard tList access.
    maxThreads = 32 # Default. Can be overriden with the Collector.setMaxThreads() method.
    eventMaxThreads = threading.Event() # Event fired when the number of threads decreases below threshold.
    
    MSGLEN_NUMBYTES = 8 # This is the hex-text version of the number of bytes in the response message.
                        # So, we can send back 4 GB of response!
    MAX_MSG_BUFSIZE = 4096 # Don't receive more than this in one call!
    
    def __init__(self, sock, host, port, database, user, log, debugLog):
        threading.Thread.__init__(self)
        # Could raise. Handle in the code that creates the thread.
        self.dbconn = Dbconnection(host, port, database, user)            
        self.sock = sock
        self.log = log
        self.debugLog = debugLog
        
    def run(self):
        try:
            # First, obtain request.
            msg = self.receiveRequest()
            if self.debugLog:
                self.debugLog.write(['Received a request from ' + str(self.sock.getpeername()) + '.'])

            self.unpickleRequest(msg)

            cmd = "SELECT T1.ds_index, T1.online_loc, T1.online_status, T1.archive_status, T1.offsite_ack, T1.history_comment, T1.owning_series, T1.storage_group, T1.bytes, T1.create_sumid, T1.creat_date, T1.username, COALESCE(T1.arch_tape, 'N/A'), COALESCE(T1.arch_tape_fn, 0), COALESCE(T1.arch_tape_date, '1958-01-01 00:00:00'), COALESCE(T1.safe_tape, 'N/A'), COALESCE(T1.safe_tape_fn, 0), COALESCE(T1.safe_tape_date, '1958-01-01 00:00:00'), COALESCE(T2.effective_date, '195801010000'), coalesce(T2.status, 0), coalesce(T2.archive_substatus, 0) FROM " + SUM_MAIN + " AS T1 LEFT OUTER JOIN " + SUM_PARTN_ALLOC + " AS T2 ON (T1.ds_index = T2.ds_index) WHERE T1.ds_index IN (" + ','.join(self.suList) + ')'
            if self.debugLog:
                self.debugLog.write(['db command is: ' + cmd])
                
            response = self.dbconn.exeCmd(cmd)
            
            # Send results back on the socket, which is connected to a single DRMS module. By sending the results
            # back, the client request is completed. We want to construct a list of "SUM_info" objects. Each object
            # { sunum:12592029, onlineloc:'/SUM52/D12592029', ...}
            
            msg = self.pickleResponse(response)            
            print('pickled response on server')
            self.sendResponse(msg)
            print('sent response on server')
        except Exception as exc:
            import traceback
            log.write(['There was a problem communicating with client ' + str(self.sock.getpeername())])
            log.write([traceback.format_exc(0)])
            
        # This thread is about to terminate. 
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
            
    def unpickleRequest(self, msg):
        self.suList = pickle.loads(msg)
        
    def pickleResponse(self, response):
        infoList = []
        for row in response:
            rowIter = iter(row)
            infoObj = Info()
            infoObj.sunum = rowIter.__next__()
            infoObj.onlineLoc = rowIter.__next__()
            infoObj.onlineStatus = rowIter.__next__()
            infoObj.archiveStatus = rowIter.__next__()
            infoObj.offsiteAck = rowIter.__next__()
            infoObj.historyComment = rowIter.__next__()
            infoObj.owningSeries = rowIter.__next__()
            infoObj.storageGroup = rowIter.__next__()
            infoObj.bytes = rowIter.__next__()
            infoObj.createSumid = rowIter.__next__()
            infoObj.creatDate = rowIter.__next__()
            infoObj.username = rowIter.__next__()
            infoObj.archTape = rowIter.__next__()
            infoObj.archTapeFn = rowIter.__next__()
            infoObj.archTapeDate = rowIter.__next__()
            infoObj.safeTape = rowIter.__next__()
            infoObj.safeTapeFn = rowIter.__next__()
            infoObj.safeTapeDate = rowIter.__next__()
            infoObj.effectiveDate = rowIter.__next__()
            infoObj.paStatus = rowIter.__next__()
            infoObj.paSubstatus = rowIter.__next__()

            infoList.append(infoObj)
        
        return pickle.dumps(infoList, pickle.HIGHEST_PROTOCOL)
        
    def sendResponse(self, msg):
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
            
    def receiveRequest(self):
        # First, receive length of message.
        allTextReceived = b''
        bytesReceivedTotal = 0
        
        while bytesReceivedTotal < Collector.MSGLEN_NUMBYTES:
            textReceived = self.sock.recv(min(Collector.MSGLEN_NUMBYTES - bytesReceivedTotal, Collector.MAX_MSG_BUFSIZE))
            if textReceived == b'':
                raise Exception('socketConnection', 'Socket broken.')
            print('how about here')
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
        # Return a bytes object (not a string). The unpickle function will need a bytes object for input.
        return allTextReceived
    
    # Must acquire Collector lock BEFORE calling newThread() since newThread() will append to tList (the Collector threads will be deleted from tList as they complete).
    @staticmethod
    def newThread(sock, host, port, database, user, log, debugLog):
        coll = Collector(sock, host, port, database, user, log, debugLog)
        coll.tList.append(coll)
        coll.start()

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
        
            # Send some random SUNUMs to the server thread.
            request = '650547410, 650547419, 650547430, 650551748, 650551852, 650551942, 650555939, 650556333'
            msg = self.pickleRequest(request)
            print('pickled msg is ' + str(msg))
            print('before sending, unpickled is ' + str(pickle.loads(msg)))
        
            self.sendRequest(msg)
            print('sent the request')
            msg = self.receiveResponse()
            print('got the response in clinent')
            response = self.unpickleResponse(msg)
            print('unpickled the response in client')
            
            self.dumpsInfoList(response)
        except Exception as exc:
            import traceback
            log.write(['Client ' + str(self.sock.getsockname()) + ' had a problem communicating with the server.'])
            log.write([traceback.format_exc(0)])
        finally:
            self.debugLog.write(['Closing test client socket.'])
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        
    def pickleRequest(self, request):
        # Split into a list.
        requestList = request.split(',')
        return pickle.dumps(requestList, pickle.HIGHEST_PROTOCOL)
        
    def unpickleResponse(self, msg):
        infoList = pickle.loads(msg)
        # We now have a list of Info objects.
        return infoList
    
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
        for infoObj in infoList:
            print('come on!!!')
            self.debugLog.write(['sunum=' + str(infoObj.sunum)])
            print('place X1')
            self.debugLog.write(['path=' + infoObj.onlineLoc])
            print('place X2')
            self.debugLog.write(['status=' + infoObj.onlineStatus])
            print('place X3')
            self.debugLog.write(['archstatus=' + infoObj.archiveStatus])
            print('place C')
            self.debugLog.write(['ack=' + infoObj.offsiteAck])
            self.debugLog.write(['comment=' + infoObj.historyComment])
            self.debugLog.write(['series=' + infoObj.owningSeries])
            self.debugLog.write(['group=' + str(infoObj.storageGroup)])
            self.debugLog.write(['size=' + str(infoObj.bytes)])
            self.debugLog.write(['create=' + infoObj.creatDate.strftime('%Y-%m-%d %T')])
            print('place A')
            self.debugLog.write(['user=' + infoObj.username])
            self.debugLog.write(['tape=' + infoObj.archTape])
            self.debugLog.write(['tapefn=' + str(infoObj.archTapeFn)])
            self.debugLog.write(['tapedate=' + infoObj.archTapeDate.strftime('%Y-%m-%d %T')])
            self.debugLog.write(['safetape=' + infoObj.safeTape])
            print('place B')
            self.debugLog.write(['safetapefn=' + str(infoObj.safeTapeFn)])
            self.debugLog.write(['safetapedate=' + infoObj.safeTapeDate.strftime('%Y-%m-%d %T')])
            self.debugLog.write(['pastatus' + str(infoObj.paStatus)])
            self.debugLog.write(['pasubstatus' + str(infoObj.paSubstatus)])
            self.debugLog.write(['effdate=' + infoObj.effectiveDate])

class SignalThread(threading.Thread):

    sdLock = threading.Lock() # Guard self.shutDown
    
    def __init__(self, sigset, log):
        threading.Thread.__init__(self)
    
        # Block the signals in the main thread.
        signal.pthread_sigmask(signal.SIG_SETMASK, sigset)
        print('hopefully this worked')
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
        
        # The debug log file - it is a little more verbose than the main log file.
        debugLog = None
        if arguments.getArg('debug'):
            if arguments.getArg('logfile').find('.txt') != -1:
                debugLogFile = arguments.getArg('logfile').replace('.txt', '.dbg.txt')
            else:
                debugLogFile = arguments.getArg('logfile') + '.dbg.txt'
            
            debugLog = Log(debugLogFile)
                            
        serverSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSock.bind((socket.gethostname(), int(sumsDrmsParams.get('SUMSD_LISTENPORT'))))
        serverSock.listen(5)

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
            elif type == 'poll':
                rv = RV_POLL
            else:
                raise
                
            msg = exc.args[1]
                
            print(msg, file=sys.stderr)
        else:
            raise

sys.exit(rv)
