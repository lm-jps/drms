#!/usr/bin/env python3
import sys

if sys.version_info < (3, 4):
    raise Exception('you must run the 3.4 release, or a more recent release, of Python')

import re
import os
import shutil
import threading
import signal
import copy
from datetime import datetime, timedelta, timezone
import random
import json
import logging
import argparse
from subprocess import check_call, CalledProcessError
from multiprocessing import Process, Lock
from multiprocessing.sharedctypes import Value
from collections import OrderedDict
import queue
import psycopg2
from multiprocessing import Process, Lock
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

# SUSTEWIE GLOBALS
LOG_FILE_BASE_NAME = 'sslog'
SUB_CHUNK_SIZE = 128

# SUMS GLOBALS
SUM_MAIN = 'public.sum_main'
SUM_PARTN_ALLOC = 'public.sum_partn_alloc'
DADP = 2

# EXCEPTIONS
class SSException(Exception):
    '''
    '''
    def __init__(self, msg):
        super(SSException, self).__init__(msg)

class TerminationException(SSException):
    '''
    '''
    retcode = SS_TERMINATED
    def __init__(self, msg):
        super(TerminationException, self).__init__(msg)
        
class ParamsException(SSException):
    '''
    '''
    retcode = SS_DRMSPARAMS
    def __init__(self, msg):
        super(ParamsException, self).__init__(msg)

class ArgsException(SSException):
    '''
    '''
    retcode = SS_ARGS
    def __init__(self, msg):
        super(ArgsException, self).__init__(msg)

class KWArgumentException(SSException):
    '''
    '''
    retcode = SS_KWARGS
    def __init__(self, msg):
        super(SS_KWARGS, self).__init__(msg)
        
class DBConnectionException(SSException):
    '''
    '''
    retcode = SS_DBCONNECTION
    def __init__(self, msg):
        super(DBConnectionException, self).__init__(msg)
        
class DBCommandException(SSException):
    '''
    '''
    retcode = SS_DBCOMMAND
    def __init__(self, msg):
        super(DBCommandException, self).__init__(msg)

class SUFinderException(SSException):
    '''
    '''
    retcode = SS_SUFINDER
    def __init__(self, msg):
        super(SUFinderException, self).__init__(msg)
         

class TerminationHandler(object):
    def __new__(cls, **kwargs):
        return super(TerminationHandler, cls).__new__(cls)

    def __init__(self, **kwargs):
        self.__lockfile = Arguments.checkArg('lockFile', str, KWArgumentException(), **kwargs)
        self.__log = Arguments.checkArg('log', Log, KWArgumentException(), **kwargs)
        self.__pid = Arguments.checkArg('pid', int, KWArgumentException(), **kwargs)
        self.__dbhost = Arguments.checkArg('dbHost', str, KWArgumentException(), **kwargs)
        self.__dbname = Arguments.checkArg('dbName', str, KWArgumentException(), **kwargs)
        self.__dbport = Arguments.checkArg('dbPort', str, KWArgumentException(), **kwargs)
        self.__dbuser = Arguments.checkArg('dbUser', str, KWArgumentException(), **kwargs)

        self.__ssLock = None
        self.__savedSignals = None

        super(TerminationHandler, self).__init__(**kwargs)
        
    def __enter__(self):
        self.enableInterrupts()

        # Acquire locks.
        self.__ssLock = DrmsLock(self.__lockFile, str(self.__pid))
        self.__ssLock.acquireLock()
        
        # open SUMS DB connection(s)
        for nConn in range(0, DBConnection.maxConn):
            DBConnection.connList.append(DBConnection(self.dbhost, self.dbport, self.database, self.dbuser, self.log)

        return self

    # Normally, __exit__ is called if an exception occurs inside the with block. And since SIGINT is converted
    # into a KeyboardInterrupt exception, it will be handled by __exit__(). However, SIGTERM will not - 
    # __exit__() will be bypassed if a SIGTERM signal is received. Use the signal handler installed in the
    # __enter__() call to handle SIGTERM.
    def __exit__(self, etype, value, traceback):
        self.__log.writeDebug([ 'TerminationHandler.__exit__() called' ])
        if etype is not None:
            # if the context manager was exited without an exception, then etype is None
            import traceback
            self.__log.writeDebug([ traceback.format_exc(5) ])

        print('\nSU Steward is shutting down...', end='')
        self.__finalStuff()
        
        # Clean up lock
        try:     
            self.__ssLock.releaseLock()   
            self.__ssLock.close()
            self.__ssLock = None
        except IOError:
            pass
            
        self.__log.writeDebug([ 'exiting TerminationHandler' ])
        
        if etype == SystemExit:
            print('and done')
            raise TerminationException('termination signal handler called')
            
    def __terminator(signo, frame):
        # raise the SystemExit exception (which will be handled by the __exit__() method)
        sys.exit(0)
            
    def __saveSignal(self, signo, frame):
        if self.__savedSignals == None:
            self.__savedSignals = []

        self.__savedSignals.append((signo, frame))
        self.__log.writeDebug([ 'saved signal ' +  signo ])

    def disableInterrupts(self):
        signal.signal(signal.SIGINT, self.__saveSignal)
        signal.signal(signal.SIGTERM, self.__saveSignal)
        signal.signal(signal.SIGHUP, self.__saveSignal)
        
    def enableInterrupts(self):
        signal.signal(signal.SIGINT, self.__terminator)
        signal.signal(signal.SIGTERM, self.__terminator)
        signal.signal(signal.SIGHUP, self.__terminator)
        
        if type(self.__savedSignals) is list:
            for signalReceived in self.__savedSignals:
                self.__terminator(*signalReceived)
        
        self.__savedSignals = None

    def __finalStuff(self):
        self.__log.writeInfo([ 'stop-up the SUFinder queues' ])
        
        finders = SUFinder.getFinders()
        
        # halt SUFinder thread(s); do not halt any other threads;
        # the main thread will wait for SUChunk threads to complete, and SUChunk threads will
        # wait for Worker threads to complete
        for finder in finders:
            self.__log.writeInfo([ 'putting a stopper in the ' + SUFinder.__name__ + ' (ID ' + finder.getID() + ') queue' ])
            # this stops-up the finder queue and it also kills the finder thread
            finder.stop()
            
        self.__log.writeInfo([ 'waiting for the tasks in the SUFinder queues to complete' ])
        
        for finder in finders:
            self.__log.writeInfo([ 'waiting for the ' + SUFinder.__name__ + ' (ID ' + finder.getID() + ') queue to become empty'])
            finder.getQueue().join()

        while True:
            tList = SUFinder.getThreadList()        
            th = None

            if len(tList) > 0:
                    th = tList[0]
                else:
                    break

            if th and isinstance(th, (SUFinder)) and th.is_alive():
                # can't hold lock here - when the thread terminates, it acquires the same lock
                self.__log.writeInfo([ 'waiting for the ' + SUFinder.__name__ + ' (ID ' + finder.getID() + ') thread to terminate' ])
                th.join() # will block, possibly for ever

        # clean up DB connections
        DBConnection.closeAll()
        self.__log.writeInfo([ 'closed all DB connections' ])


class SSDrmsParams(DRMSParams):

    def __init__(self):
        super(SSDrmsParams, self).__init__()

    def get(self, name):
        val = super(SSDrmsParams, self).get(name)

        if val is None:
            raise ParamsException('unknown DRMS parameter: ' + name)
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
                type, msg = exc.args
                  
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
            raise ArgsException('attempt to set an argument that already exists: ' + name)
            
    def replArg(self, name, newValue):
        if hasattr(self, name):
            setattr(self, name, newValue)
        else:
            raise ArgsException('attempt to replace an argument value for an argument that does not already exist: ' + name)

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)
        
    def getArg(self, name):
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise ArgsException('unknown argument: ' + name)
            
    @classmethod
    def checkArg(cls, argName, exc, **kwargs):
        val = None
        if argName in kwargs:
            val = kwargs[argName]
        elif exc is not None:
            raise exc
        return val


# we're going to need one for finding SUs to delete (one thread), and one for each thread deleting SUs and updating 
# the SUMS tables (one thread per chunk of SUs we are deleting)
class DBConnection(object):
    connList = [] # list of existing DB connections
    connListFree = [] # list of currently unused DB connections
    connListLock = threading.RLock() # guard list access - the thread that has the lock can call acquire(), and it will not block
    maxConn = 8
    eventConnFree = threading.Event() # event fired when a connection gets freed up
    nextIDseq = 0 # the id of the next connection

    def __init__(self, **kwargs):
        self.host = Arguments.checkArg('host', KWArgumentException('host argument required'), **kwargs)
        self.port = Arguments.checkArg('port', KWArgumentException('port argument required'), **kwargs)
        self.database = Arguments.checkArg('database', KWArgumentException('database argument required'), **kwargs)
        self.user = Arguments.checkArg('user', KWArgumentException('user argument required'), **kwargs)
        self.log = Arguments.checkArg('log', KWArgumentException('log argument required'), **kwargs)
    
        self.conn = None        
        self.id = str(DBConnection.nextIDseq) # do not call the constructor from more than one thread!

        DBConnection.nextIDseq += 1

        # connect to the db; if things succeed, then save the db-connection information
        self.openConnection()
        
        self.__connLock = threading.Lock() # to ensure that different cursor's commands do not get interrupted
        
    def getID(self):
        return self.id
        
    def acquireLock(self):
        return self.__connLock.acquire()
        
    def releaseLock(self):
        return self.__connLock.release()
        
    def commit(self):
        # Does not close DB connection. It can be used after the commit() call.
        if not self.conn:
            raise DBCommandException('cannot commit - no database connection exists')
            
        if self.conn:
            self.conn.commit()

    def rollback(self):
        # Does not close DB connection. It can be used after the rollback() call.
        if not self.conn:
            raise DBCommandException('cannot rollback - no database connection exists')

        if self.conn:
            self.conn.rollback()

    def close(self):
        # Does a rollback, then closes DB connection so that it can no longer be used.
        self.closeConnection()
            
    def openConnection(self):
        if self.conn:
            raise DBConnectionException('already connected to the database')
            
        try:
            self.conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user)
            self.log.writeInfo([ 'user ' + self.user + ' successfully connected to ' + self.database + ' database: ' + self.host + ':' + str(self.port) + ' - id ' + self.id ])
        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection
            if hasattr(exc, 'diag') and hasattr(exc.diag, 'message_primary'):
                msg = exc.diag.message_primary
            else:
                msg = 'Unable to connect to the database (no, I do not know why).'
            raise DBConnectionException(msg)
            
        # must add to the list of connections and free connections
        DBConnection.connListLock.acquire()
        try:
            DBConnection.connList.append(self)
            DBConnection.connListFree.append(self)
        finally:
            DBConnection.connListLock.release()

        self.log.writeDebug([ 'added connection ' + self.id + ' to connection list and free connection list' ])

    def closeConnection(self):    
        if not self.conn:
            raise DBConnectionException('there is no database connection')
        
        if self.conn:
            DBConnection.connListLock.acquire()
            try:
                self.conn.close()
                self.log.writeInfo([ 'closed DB connection ' + self.id ])

                if self in DBConnection.connListFree:
                    DBConnection.connListFree.remove(self)
                    self.log.writeDebug([ 'removed DB connection ' + self.id + ' from free connection list'])
                DBConnection.connList.remove(self)
                self.log.writeDebug([ 'removed DB connection ' + self.id + ' from connection list'])

            finally:
                DBConnection.connListLock.release()
            
    def release(self):
        DBConnection.connListLock.acquire()
        try:
            # add this connection to the free list
            DBConnection.connListFree.append(self)
            
            # signal a thread waiting for an open connection (if there were previously no slots open)
            if len(DBConnection.connListFree) == 1:
                # fire event so that worker can obtain a DB slot
                DBConnection.eventConnFree.set()
                # clear event so that a worker will block the next time it calls wait()
                DBConnection.eventConnFree.clear()            
        finally:
            DBConnection.connListLock.release()

    def exeCmd(self, cmd, results, result=True):
        if not self.conn:
            raise DBCommandException('cannot execute database command ' + cmd + ' - no database connection exists')
        
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
                
    @classmethod
    def nextOpenConnection(cls):
        conn = None
        while True:
            cls.connListLock.acquire()
            try:
                if len(cls.connListFree) > 0:
                    conn = cls.connListFree.pop(0)                
                    break # the finally clause will ensure the connList lock is released
            finally:
                cls.connListLock.release()
    
            # There were no free threads. Wait until there is a free thread.
            cls.eventConnFree.wait()
            # We woke up, because a free DB connection became available. However, that DB connection could 
            # now be in use. Loop and check again.
            
        return conn


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


class Worker(threading):
    '''
    rm an SU from its SUMS partition; this is a short-lived thread
    '''
    __tList = []
    __tLock = threading.Lock() # guard tList
    __maxThreads = 256
    __idInt = 0

    def __init__(self, su, path, log):
        self.__su = su
        self.__path = path
        self.__log = log
        
        self.__id = str(Worker.__idInt)
        Worker.__idInt += 1

    def run(self):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except:
            import traceback
            self.__log.writeWarning([ 'unable to delete SU ' + path + '; ' + traceback.format_exc(5)])
            # swallow exception so that stewie keeps running
        finally:
            # This thread is about to terminate. 
            # We need to check the class tList variable to update it, so we need to acquire the lock.
            Worker.__tLock.acquire()
            try:
                Worker.__tList.remove(self) # This thread is no longer one of the running threads.
                if len(Worker.__tList) == Worker.__maxThreads - 1:
                    Worker.fireMaxThreadsEvent()
                self.__log.writeInfo([ 'Worker (ID ' +  self.__id + ') halted.' ])            
            finally:
                Worker.__tLock.release()
                
    @classmethod
    def acquireLock(cls):
        cls.__tLock.acquire()
        
    @classmethod
    def releaseLock(cls):
        cls.__tLock.release()
        
    @classmethod
    def __newThread(cls, worker):
        cls.__tList.append(worker)
        worker.start()
    
    # acquire lock first
    @classmethod
    def newWorker(cls, **kwargs):
        worker = None
        
        try:
            su = Arguments.checkArg('su', KWArgumentException(), **kwargs)
            path = Arguments.checkArg('path', KWArgumentException(), **kwargs)
            log = Arguments.checkArg('log', KWArgumentException(), **kwargs)
            
            if len(cls.__tList) == cls.__maxThreads:
                raise KWArgumentException('all Worker slots are occupied')

            worker = cls(su=su, path=path log=log)

            # spawn thread
            cls.__newThread(worker)
        except:
            worker = None
            raise
    
    # acquire lock first
    @classmethod
    def newWorkers(cls, **kwargs):
        workers = []
        
        try:
            sus = Arguments.checkArg('sus', KWArgumentException(), **kwargs)
            paths = Arguments.checkArg('paths', KWArgumentException(), **kwargs)
            log = Arguments.checkArg('log', KWArgumentException(), **kwargs)

            if len(sus) + len(cls.__tList) > cls.__maxThreads:
                raise KWArgumentException('the number of workers ' + str(len(sus)) + ' must be smaller than the number of open Worker threads ' + str(cls.__maxThreads))

            for su in sus:
                workers.append(cls(su=su, path=path log=log))
                
            # spawn threads
            for worker in Workers:
                cls.__newThread(worker)
        except:
            workers = None
            raise

    @classmethod
    def getWorkers(cls):
        workers = ()
        cls.acquireLock()
        try:
            for worker in cls.__tList:
                workers = workers + (worker,)        
        finally:
            cls.releaseLock()
            
        return workers
            
    @classmethod
    def getThreadList(cls):
        return cls.getWorkers()
 
    @classmethod
    def waitMaxThreadsEvent(cls):
        cls.__maxThreadsEvent.wait()
    
    @classmethod
    def fireMaxThreadsEvent(cls):
        cls.__maxThreadsEvent.set()
        cls.__maxThreadsEvent.clear()


class Chunker(object):
    def __init__(self, list, chSize):
        self.chunks = []
        iChunk = -1
        nElem = 1
        
        for elem in list:
            if iChunk == -1 or nElem % chSize == 0:
                iChunk += 1
                self.chunks.append([])
    
            self.chunks[iChunk].append(elem)
            nElem += 1
    
    def __iter__(self):
        return self.iterate()
    
    # iterate through chunks
    def iterate(self):
        i = 0
        while i < len(self.chunks):
            yield self.chunks[i]
            i += 1


class SUChunk(threading):
    __tList = []
    __tLock = threading.Lock() # coarse thread lock
    __maxThreads = 32
    __maxThreadsEvent = threading.Event() # event fired when the number of threads decreases below threshold
    __idInt = 0

    def __init__(self, **kwargs):
        self.__sudirs = Arguments.checkArg('sudirs', KWArgumentException(), **kwargs) # a list of SUDIRs
        self.__dbconn = Arguments.checkArg('dbconn', KWArgumentException(), **kwargs)
        self.__log = Arguments.checkArg('log', KWArgumentException(), **kwargs)
        self.__suFinder = Arguments.checkArg('finder', KWArgumentException(), **kwargs)

        self.__sudirsStr = [ str(sudir) for sudir in self.__sudirs ]
        self.__id = str(SUChunk.__idInt)
        SUChunk.__idInt += 1
        self.__deleteEvent = threading.Event()

    def run(self):
        try:
            # wait until main tells this thread to delete its SUDIRs
            self.__deleteEvent.wait()
        
            workers = []

            # start a worker for each SUDIR rm
            for sudir in self.__sudirs:
                while True:
                    Worker.acquireLock()
                    try:
                        if len(Worker.getThreadList()) < Worker.getMaxThreads():
                            self.log.writeInfo([ 'instantiating a Worker for SUDIR ' + str(sudir) ])
                            # start a worker to handle the deletion of su-chunk-size SUDIRs
                            workers.append(Worker.newWorker(sudir=sudir, path=path, log=self.__log))
                            break # The finally clause will ensure the Downloader lock is released.
                    except StartThreadException:
                        # Ran out of system resources - could not start new thread. Just wait for a thread slot to become free.
                        self.__log.writeWarning([ 'unable to start a new worker thread; trying again later' ])
                        pass
                    finally:
                        Worker.releaseLock()

                    Worker.waitMaxThreadsEvent()
        
            # update the DB (regardless of failure to delete SUDIRs or not)
            # DELETE FROM SUM_PARTN_ALLOC WHERE ds_index IN (self.sudirsStr); DELETE FROM SUM_MAIN WHERE ds_index IN (self.sudirsStr); 
            sql = ''
            sql += 'DELETE FROM ' + SUM_PARTN_ALLOC + ' WHERE ds_index IN (' + self.__sudirsStr + ')' + ';'
            sql += 'DELETE FROM ' + SUM_MAIN + ' WHERE ds_index IN (' + self.__sudirsStr + ')'
        
            self.__dbconn.acquireLock()
            try:
                self.__dbconn.exeCmd(sql, None, False)
                self.__dbconn.commit()
            except DBCommandException as exc:
                self.__logWriteError([ exc.args[0] ])
                self.__dbconn.rollback()
                # do not re-raise - we need to wait for the Worker threads to complete, and we need to remove SUDIRs from the finder
            except:
                import traceback
                
                self.__logWriteError([ traceback.format_exc(5) ])
                # do not re-raise - we need to wait for the Worker threads to complete, and we need to remove SUDIRs from the finder
            finally:
                self.__dbconn.releaseLock()
        
            # wait for all workers to complete (so SUDIR deletions and DB deletions run in parallel) so we can remove
            # this SUChunk from its tList
            for worker in workers:
                if worker and isinstance(worker, (Worker)) and worker.is_alive():
                    self.__log.writeInfo([ 'waiting for Worker (ID ' + worker.getID() + ') to halt' ])
                    worker.join(10) # will block for up to N seconds; rm should run quickly
                    if worker.is_alive():
                        # timeout occurred
                        self.__log.writeWarning([ 'SUChunk ' + self.__id + ' timed-out waiting for worker ' + worker.getID() + ' to terminate' ])
            
            SUFinder.acquireLock()
            try:
                # removeSUDIRs() will fire the waitForChunk event so that the SUFinder knows it can add more SUs to its
                # global pool of SU that will be deleted
                self.__suFinder.removeSUDIRs(self.__sudirs)
            finally:
                SUFinder.releaseLock()
            
        finally:
            SUChunk.__tLock.acquire()
            try:

                SUChunk.__tList.remove(self) # This thread is no longer one of the running threads.                
                if len(SUChunk.__tList) == SUChunk.__maxThreads - 1:
                    SUChunk.fireMaxThreadsEvent()
                
                self.__log.writeInfo([ 'SU Chunk (ID ' +  self.__id + ') terminated' ])
            finally:
                SUChunk.__tLock.release()
                
    def getID(self):
        return self.__id

    # called from main        
    def delete(self):
        self.fireDeleteEvent()
    
    def fireDeleteEvent(self):
        self.__deleteEvent.set()
        self.__deleteEvent.clear()

    # acquire lock first
    @classmethod
    def __newThread(cls, suChunk):
        cls.__tList.append(suChunk)
        suChunk.start()
    
    # acquire lock first
    @classmethod
    def newSUChunk(cls, **kwargs):
        suChunk = None
        
        try:
            sus = Arguments.checkArg('sus', KWArgumentException(), **kwargs)
            suFinder = Arguments.checkArg('finder', KWArgumentException(), **kwargs)
            dbconn = Arguments.checkArg('dbconn', KWArgumentException(), **kwargs)
            log = Arguments.checkArg('log', KWArgumentException(), **kwargs)
            
            if len(cls.__tList) == cls.__maxThreads:
                raise KWArgumentException('the number of chunks ' + str(len(sus)) + ' must be smaller than the number of free SUChunk threads ' + str(cls.__maxThreads))

            suChunk = cls(sus=sus, finder=suFinder, dbconn=dbconn, log=log)

            # spawn thread
            cls.__newThread(suChunk)
        except:
            suFinder = None
            raise
    
    # acquire lock first
    @classmethod
    def newSUChunks(cls, **kwargs):
        suChunks = []
        
        try:
            sus = Arguments.checkArg('sus', KWArgumentException(), **kwargs) # list of list of SUs
            suFinder = Arguments.checkArg('finder', KWArgumentException(), **kwargs)
            dbconn = Arguments.checkArg('dbconn', KWArgumentException(), **kwargs)
            log = Arguments.checkArg('log', KWArgumentException(), **kwargs)

            if len(sus) + len(cls.__tList) > cls.__maxThreads:
                raise KWArgumentException('the number of chunks ' + str(len(sus)) + ' must be smaller than the number of free SUChunk threads ' + str(cls.__maxThreads))

            for suList in sus:
                suChunks.append(cls(sus=suList, finder=suFinder, dbconn=dbconn, log=log))
                
            # spawn threads
            for suChunk in suChunks:
                cls.__newThread(suChunk)
        except:
            suChunks = None
            raise

    @classmethod
    def acquireLock(cls):
        cls.__tLock.acquire()
        
    @classmethod
    def releaseLock(cls):
        cls.__tLock.release()
                
    # called from SUFinder threads
    @classmethod
    def waitMaxThreadsEvent(cls):
        cls.__maxThreadsEvent.wait()
    
    @classmethod
    def fireMaxThreadsEvent(cls):
        cls.__maxThreadsEvent.set()
        cls.__maxThreadsEvent.clear()
        
    @classmethod
    def getSUChunks(cls):
        # do not allow calling thread to modify our threads!
        suChunks = () # immutable

        cls.acquireLock()
        try:
            for suChunk in cls.__tList:
                suChunks = suChunks + (suChunk,)
        finally:
            cls.releaseLock()
        
        return suChunks
        
    @classmethod
    def getThreadList(cls):
        return cls.getSUChunks()


class SUFinder(threading):
    '''
    queries the SUMS DB to obtain expired SUs;
    runs os.statvfs(), which runs the statfs() system call; this call could time-out;
    both of these factors could make these threads run slowly;    
    '''
    __tList = []
    __tLock = threading.Lock() # coarse thread lock
    __idInt = 0
    __maxThreads = 128
    __stopEvent = threading.Event() # event fired when shutting down

    def __init__(self, **kwargs):
        self.__partition = Arguments.checkArg('partition', KWArgumentException(), **kwargs)
        self.__log = Arguments.checkArg('log', KWArgumentException(), **kwargs)
        # the maximum number of SUs in the pool available for deletion
        self.__maxInPool = Arguments.checkArg('maxSUs', KWArgumentException(), **kwargs)

        self.__dbConn = DBConnection.nextOpenConnection()

        # __suPool : a list where the first element is a dict where key is sudir (str) and val is a queue element 
        # (sudir (str), expDate (datetime), series); and the second element is a priority queue
        # of queue elements (sudir (str), expDate (datetime), series)
        # 
        # [ { sudir => queueElement, ... }, [ queueElement, ...] ]
        # 
        # where queueElement is ( sudir, expDate, series ) and the queueElements are sorted by expDate, then series
        self.__suPool = [ {}, queue.Queue(self.__maxInPool) ]
        
        # no need to acquire lock since SUFinder objects are created in the main thread and all SUFinder objects
        # are created before any of their threads are run
        self.__id = str(SUFinder.__idInt)
        SUFinder.__idInt += 1
            
        self.rehydrate() # initialize the sustewie table for the parition
                    
        self.__stewieTable = 'stewie' + self.__id
        self.__queueEmptyEvent = threading.Event() # event fired when a the queue becomes empty

    def run(self):
        try:
            sudirSet = set() # ensure no duplicate SUDIRs are added to the queue

            while not self.__stopEvent.is_set():
                # find more SUs for deletion
                needMoreSUs = True
                sudirs = []
                rows = []

                self.__dbConn.acquireLock()
                try:
                    sql = 'SELECT ' + SUM_MAIN + '.owning_series AS series, ' + self.__stewieTable + '.sudir AS sudir, ' + self.__stewieTable + '.enjoyby AS enjoyby FROM ' + self.__stewieTable + ' LEFT OUTER JOIN ' + SUM_MAIN + ' ON ' + self.__stewieTable + '.sunum = ' + SUM_MAIN + '.ds_index WHERE ' + self.__stewieTable + '.sudir LIKE ' + "'" + self.__partition + '/%' + "'" + ' ORDER BY enjoyby, series'
                    self.__dbConn.exeCmd(sql, rows, True)
                    self.__dbConn.rollback() # end the transaction, even though the transaction did not modify the DB
                    
                    for row in rows:
                        if row[1] not in sudirSet:
                            sudirs.append((row[1], row[2], row[0]))
                            sudirSet.add(row[1])
                
                    if len(sudirs) > 0:
                        # will block if all sudir slots are occupied
                        self.addSUDIRS(sudirs)
                except DBCommandException as exc:    
                    self.__logWriteError([ exc.args[0] ])
                    # do not raise - go to next iteration
                except:
                    import traceback
                    
                    msg = 'SUFinder ' + self.__id + 'died; ' + traceback.format_exc(5)
                    self.__log.writeError([ msg ])
                    self.__dbConn.rollback() # end the transaction, even though the transaction did not modify the DB
                    # do not raise - go to next iteration
                finally:
                    self.__dbConn.releaseLock()
                    
                # at this point, there are pending SUs in the pool; if we were to re-run the sql, we'd end up with self.__maxInPool
                # duplicates - so wait until the pool is empty before re-running the sql
                self.__queueEmptyEvent.wait()
        finally:
            SUFinder.__tLock.acquire()
            try:
                SUFinder.__tList.remove(self) # This thread is no longer one of the running threads.
                if len(SUFinder.__tList) == SUFinder.__maxThreads - 1:
                    SUFinder.fireMaxThreadsEvent()
            finally:
                SUFinder.__tLock.release()
            
            self.__dbConn.release()
            self.log.writeDebug([ 'SUFinder released DB connection ' + self.__dbConn.getID() ])
    
    # called from main thread
    def stop(self):
        # fire stop event so no more SUDIRs are added to the queue
        self.__stopEvent.set()
        
        # put a stopper in the queue so no more suQueue.get() calls are made
        SUFinder.__tLock.acquire()
        try:
            suQueue = SUFinder.__maxInPool[1]
            suQueue.put(None)
        finally:
            SUFinder.__tLock.release()
            
    def getID(self):
        return self.__id
        
    def getPartition(self):
        return self.__partition
        
    def getQueue(self):
        return self.__suPool[1]
    
    # called from main (during SUFinder construction and when looping through finders)
    def rehydrate(self):
        '''
        this is a stupid term people use for re-loading up a cache (like when it becomes stale and you want to make it current)
        '''
        self.__dbConn.acquireLock()
        try:
            # Need to convert today's date into an 'effective date' (a time string of the form YYYYMMDD)
            nowTimeStr = datetime.now().strftime('%Y%m%d')
            sql = ''
            sql += 'DROP TABLE IF EXISTS ' + self.__stewieTable + ';'
            sql += 'CREATE TEMPORARY TABLE ' + self.__stewieTable + ' AS SELECT ds_index AS sunum, wd AS sudir, effective_date AS enjoyby FROM ' + SUM_PARTN_ALLOC + ' WHERE ' + SUM_PARTN_ALLOC + '.status = ' + str(DADP) + ' AND ' + SUM_PARTN_ALLOC + '.wd LIKE ' + "'" + self.__partition + '/%' + "'" + ' AND effective_date < ' + "'" + nowTimeStr + "'";
            sql += 'CREATE INDEX ' + self.__stewieTable + '_pkey ON ' + self.__stewieTable + ' (sunum);'
            
            self.__dbConn.exeCmd(sql, None, False)
            self.__dbConn.commit()
            self.__log.writeInfo([ 'successfully rehydrated steward table ' + self.__stewieTable ])
        except DBCommandException as exc:
            self.__log.writeError([ exc.args[0] ])
            self.__dbConn.rollback()
        except:
            import traceback
            
            self.__log.writeError([ traceback.format_exc(5) ])
            self.__dbConn.rollback()
        finally:
            self.__dbConn.releaseLock()
            
    def waitQueueEmptyEvent(self):
        self.__queueEmptyEvent.wait()
    
    def fireQueueEmptyEvent(self):
        self.__queueEmptyEvent.set()
        self.__queueEmptyEvent.clear()

    # in main thread (get from queue)
    def getSUChunks(self, totalNum):
        # we want to chunk-up totalNum SUs; totalNum is the chunk-size on which the user wants to operarate during 
        # each iteration; we want to break this chunk into sub-chunks so we can parallelize
        chunker = None
        suChunks = []
        
        try:
            # remove a max of totalNum SUs from head of __maxInPool (head is the 'most expired' SU, tail is the least)
            queueElements = []
            metaChunk = None

            try:
                suQueue = self.__maxInPool[1]
                for item in range(0, totalNum):
                    elem = suQueue.get(False) # will not block
                    if elem is not None:
                        queueElements.append(elem)
                    else:
                        # this thread is shutting down
                        break
            except queue.Empty:
                # tried to get a queueElement, but none are left; this is OK because we want to get AT MOST seriesNum
                # elements; on to the next series; 
                # we want to notify the SUFinder thread that the queue was empty; we'll simply skip this iteration
                # of checking for deleteable SUs, even though there might be some new ones marked for deletion in the
                # DB
                self.__queueEmptyEvent.fire()
        
            metaChunk = [ elem[0] for elem in queueElements ] 
        
            if metaChunk:
                chunker = Chunker(metaChunk, chSize=SUB_CHUNK_SIZE)
        except:
            chunker = None
            import traceback

            self.__log.writeError([ traceback.format_exc(5) ])
        
        if chunker:
            for chunk in chunker:
                while True:
                    SUChunk.__tLock.acquire()
                    try:
                        if len(SUChunk.getNumThreads()) < SUChunk.getMaxThreads():
                            self__log.writeInfo([ 'instantiating an SU chunk for SUs ' + str(chunk[0]) + '...' ])
                            # start a worker to handle the deletion of arguments.su-chunk-size SUs
                            suChunk = suChunks.append(SUChunk.newSUChunk(sus=chunk, finder=self, dbconn=self.__dbConn, log=self.__log))
                            SUChunk.__newThread(suChunk)
                            break # The finally clause will ensure the SUChunk lock is released.
                    except StartThreadException:
                        # Ran out of system resources - could not start new thread. Just wait for a thread slot to become free.
                        self.__log.writeWarning([ 'unable to start a new SU chunk thread; trying again later' ])
                        suChunks = None
                    finally:
                        SUChunk.__tLock.release()

                    SUChunk.waitMaxThreadsEvent()

        return suChunks

    # in SUFinder thread (put into queue)
    def addSUDIRS(self, sudirTuples):
        '''
        sudirTuples : a list of 3-tuples (sudir, expDate, series), sorted by expiration date, series
        '''
        finder = SUFinder.__maxInPool[0] # map sudir to heapElement

        for sudirTuple in sudirTuples:
            sudir, expDate, series = sudirTuple 

            suQueue = SUFinder.__suPool[1]

            if sudir in finder:
                raise SUFinderException('attempt to add a duplicate SUDIR to the queue: ' + sudir)
            
            queueTuple = ( sudir, expDate, series )
            finder[sudir] = queueTuple
            # will block if the suQueue is full
            suQueue.put(queueTuple)

    # in SUChunk thread (remove queueElement from sudir --> queueElement map)
    def removeSUDIRs(self, sudirs):
        finder = SUFinder.__suPool[0]
        queue = SUFinder.__suPool[1]
        
        for sudir in sudirs:
            if sudir not in finder:
                raise SUFinderException('attempt to remove an unknown SUDIR (' + sudir + ')')

            # the sudir is known to the SUFinder (the queueElement has already been removed from the series dictionaries)
            queue.task_done()
            del finder[sudir]
                
    @classmethod
    def acquireLock(cls):
        return cls.__tLock.acquire()
        
    @classmethod
    def releaseLock(cls):
        return cls.__tLock.release()        
    
    @classmethod
    def getFinders(cls):
        finders = ()
        cls.acquireLock()
        try:
            for finder in cls.__tList:
                finders = finders + (finder,)        
        finally:
            cls.releaseLock()
            
        return finders
            
    @classmethod
    def getThreadList(cls):
        return cls.getFinders()
        
    @classmethod
    def getMaxThreads(cls):
        return cls.__maxThreads
        
    @classmethod
    def waitMaxThreadsEvent(cls):
        cls.__maxThreadsEvent.wait()
    
    @classmethod
    def fireMaxThreadsEvent(cls):
        cls.__maxThreadsEvent.set()
        cls.__maxThreadsEvent.clear()
    
    # acquire lock first
    @classmethod
    def __newThread(cls, suFinder):
        cls.__tList.append(suFinder)
        suFinder.start()
    
    # acquire lock first
    @classmethod
    def newFinder(cls, **kwargs):
        suFinder = None
        
        try:
            partition = Arguments.checkArg('partition', KWArgumentException(), **kwargs)
            maxSUs = Arguments.checkArg('maxSUs', KWArgumentException(), **kwargs)
            log = Arguments.checkArg('log', KWArgumentException(), **kwargs)
            
            if len(cls.__tList) == cls.__maxThreads:
                raise KWArgumentException('all SUFinder slots are occupied')

            suFinder = cls(partition=partition, maxSUs=maxSUs, log=log)

            # spawn thread
            cls.__newThread(suFinder)
        except:
            suFinder = None
            raise
    
    # acquire lock first
    @classmethod
    def newFinders(cls, **kwargs):
        suFinders = []
        
        try:
            partitions = Arguments.checkArg('partitions', KWArgumentException(), **kwargs)
            maxSUs = Arguments.checkArg('maxSUs', KWArgumentException(), **kwargs)
            log = Arguments.checkArg('log', KWArgumentException(), **kwargs)

            if len(partitions) + len(cls.__tList) > cls.__maxThreads:
                raise KWArgumentException('the number of partitions ' + str(len(partitions)) + ' must be smaller than the number of SUFinder threads ' + str(SUFinder.__maxThreads))

            for partition in partitions:
                suFinders.append(cls(partition=partition, maxSUs=maxSUs, log=log))
                
            # spawn threads
            for suFinder in suFinders:
                cls.__newThread(suFinder)
        except:
            suFinders = None
            raise


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
        
        
class ListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, [ int(setNumStr) for setNumStr in values.split(',') ])


# called from main
def callStatvfs(suPartitionPath, rv):
    fsStats = os.statvfs(suPartitionPath)
    if fsStats is not None and hasattr(fsStats, 'f_bsize') and hasattr(fsStats, 'f_bavail'):
        rv.value = 1 - fsStats.f_bavail / fsStats.f_blocks

def getPartitions(partitionSets):
    # get all partitions that belong to the partition sets passed in
    partitions = []

    dbConn = DBConnection.nextOpenConnection()
    if dbConn:
        try:
            partitionSetsStr = ','.join(partitionSets)
            response = []
            
            sql = 'SELECT partn_name FROM public.sum_partn_avail WHERE pds_set_prime IN (' + partitionSetsStr + ')'
            dbConn.exeCmd(sql, response, True)

            for row in response:
                partitions.append(row[0])
                
            dbConn.rollback()
        except:
            partitions = None
            dbConn.rollback()
            raise 
        finally:
            dbConn.release()
            
def getPartitionUsage(partition):
    lock = Lock()
    availBytes = Value(c_double, 0, lock=lock)
    proc = Process(target=AllocResponse.__callStatvfs, args=(partition, availBytes))
    proc.start()
    proc.join(5) # timeout after 2 seconds

    if proc.exitcode is None:
        raise FileStatException('os.statvfs(' + partition + ') did not terminate')

# called from main
def disablePartition(partition):
    dbConn = DBConnection.nextOpenConnection()
    if dbConn:
        try:
            sql = 'UPDATE sum_partn_avail SET pds_set_num = -1 WHERE partn_name = ' + "'" + partition + "'"
            dbConn.exeCmd(sql, None, False)
            dbConn.commit()
        except:
            dbConn.rollback()
            raise
        finally:
            dbConn.release()

if __name__ == "__main__":
    rv = SS_SUCCESS
    ssLog = None

    try:
        ssDrmsParams = SSDrmsParams()

        parser = CmdlParser(usage='%(prog)s [ -h ] [ sutable=<storage unit table> ] [ reqtable=<request table> ] [ --dbname=<db name> ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --binpath=<executable path> ] [ --logfile=<base log-file name> ]')
    
        # optional parameters
        parser.add_argument('-M', '--dbhost', help='the host machine of the SUMS database', metavar='<db host machine>', dest='dbHost', default=ssDrmsParams.get('SUMS_DB_HOST'))
        parser.add_argument('-P', '--dbport', help='the port on the host machine that is accepting connections to the database', metavar='<db host port>', dest='dbPort', type=int, default=int(ssDrmsParams.get('SUMPGPORT')))
        parser.add_argument('-N', '--dbname', help='the name of the SUMS database', metavar='<db name>', dest='database', default=ssDrmsParams.get('DBNAME') + '_sums')
        parser.add_argument('-U', '--dbuser', help='the name of the SUMS database user account that has write privileges', metavar='<db user>', dest='dbUser', default=ssDrmsParams.get('SUMS_MANAGER'))
        parser.add_argument('-l', '--loglevel', help='specifies the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', dest='logLevel', action=LogLevelAction, default=logging.ERROR)
        parser.add_argument('-c', '--chunksize', help='the number of SUs to delete each main-thread iteration', metavar='<SU chunk size>', dest='suChunkSize', type=int, default=ssDrmsParams.get('SS_SU_CHUNK'))
        parser.add_argument('-s', '--setlist', help='a list of sets (pds_set_num) of partitions on which to operate', metavar='<list of partition sets>', dest='partitionSets', action=ListAction) # defaults to None, which means all partitions
        parser.add_argument('-L', '--lowater', help='the low water mark (percentage)', metavar='<low water mark>', dest='lowWater', type=int, default=int(ssDrmsParams.get('SS_LOW_WATER')))
        parser.add_argument('-H', '--hiwater', help='the high water mark (percentage)', metavar='<high water mark>', dest='hiWater', type=int, default=int(ssDrmsParams.get('SS_HIGH_WATER')))        
        parser.add_argument('-p', '--printsus', help='print a list of SUs to delete - do not delete SUs and do not remove SUs from the SUMS DB', dest='printSus', action='store_true', default=False)
                        
        arguments = Arguments(parser)
        arguments.setArg('lockFile', os.path.join(ssDrmsParams.get('DRMS_LOCK_DIR'), ssDrmsParams.get('SS_LOCKFILE')))
        arguments.setArg('rehydrateInterval', ssDrmsParams.get('SS_REHYDRATE_INTERVAL'))

        pid = os.getpid()

        # Create/Initialize the log file.
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ssLog = Log(os.path.join(ssDrmsParams.get('DRMS_LOG_DIR'), LOG_FILE_BASE_NAME + '_' + datetime.now().strftime('%Y%m%d') + '.txt'), arguments.loglevel, formatter)
        ssLog.writeCritical([ 'starting up SUMS Steward daemon' ])
        ssLog.writeCritical([ 'logging threshold level is ' + ssLog.getLevel() + '.' ]) # critical - always write the log level to the log
        arguments.dump(ssLog)
    
        # TerminationHandler opens a DB connection to the RS database (which is the same as the DRMS database, most likely).
        with TerminationHandler(lockFile=arguments.getArg('lockFile'), log=ssLog, pid=pid, dbHost=arguments.getArg('dbHost'), dbName=arguments.getArg('dbName'), dbPort=arguments.getArg('dbPort'), dbUser=arguments.getArg('dbUser')) as th:
            # start the finder thread - it will run for the entire duration of the steward
            maxSUs = arguments.getArg('suChunkSize') * 10 # keep maxSUs in each partition's for-deletion pool
            
            # find SUs to delete in all specified partitions
            # on error, this raises (terminating stewie)
            partitions = getPartitions(arguments.getArg('partitionSets'))
            
            if len(partitions) == 0:
                raise ArgsException('no valid partitions were specified in the setlist argument') 

            SUFinder.acquireLock()
            try:
                # does not block
                suFinders = SUFinder.newFinders(partitions=partitions, maxSUs=maxSUs, log=ssLog)
            except:
                suFinders = None
                raise
            
            finally:
                SUFinder.releaseLock()

            timeHydrated = datetime.now()
            
            # main
            while True:
                # main loop
                startTime = datetime.now()

                activeFindersQ = queue.Queue()

                # find out which partitions are eligible for cleaning (usage is about the hi-water mark)
                for suFinder in suFinders:
                    # check for rehydration time
                    if datetime.now() > timeHydrated + arguments.getArg('rehydrateInterval'):
                        suFinder.rehydrate()
                        timeHydrated = datetime.now()                
                
                    try:
                        # check for insufficient storage available
                        if getPartitionUsage(suFinder.getPartition()) > arguments.getArg('hiwater'):
                            activeFindersQ.put(suFinder)
                    except FileStatException as exc:
                        ssLog.writeWarning([ exc.args[0] + '; skipping finder ' + suFinder.getID() ])
                        pass

                # collect a chunk of SUs to delete; add them to the to-del set;
                # getSUChunks() is in main thread; the suFinder thread periodically updates its list of SUs
                # ready for deletion; when a chunk's worth of SUs are available, getSUChunks() atomically
                # removes the SUs from its list of delete-ready SUs, and returns the containing chunk to 
                # the main thread; if removal of SUs from its list of delete-ready SUs causes enough
                # slots to open up, the suFinder queries the SUMS DB for more SUs to delete;
                # 
                # chunk-up an suChunkSize chunk of SUs (arguments.suChunkSize could be too large to efficiently deal with)
                while not activeFinders.empty():
                    # continue to delete SUDIRs from each partition until the usage in all of them is 
                    # below the low-water mark
                    try:
                        th.disableInterrupts()

                        try:
                            # one suFinder per partition
                            suFinder = activeFindersQ.get(False)
                            partition = suFinder.getPartition()

                            # manageableChunk is an SUChunk for a single partition
                            manageableChunks = suFinder.getSUChunks(arguments.getArg('suChunkSize')) # blocks until a sub-chunk is available
                            for manageableChunk in manageableChunks:
                                manageableChunk.delete()
                            
                            # wait for chunks to complete
                            while True:
                                suChunks = SUChunk.getSUChunks()
                                if len(suChunks) > 0:
                                    suChunk = suChunks[0]
                    
                                    if suChunk and isinstance(suChunk, (SUChunk)) and suChunk.is_alive():
                                        # can't hold lock here - when the thread terminates, it acquires the same lock
                                        self.log.writeInfo([ 'waiting for the processing of SUChunk (ID ' + suChunk.getID() + ') to complete' ])
                                        suChunk.join(60) # wait up to one minute
                                else:
                                    break
                                
                            # only delete chunks for this partition until the partition usage is less than the low-water mark
                            try:
                                usagePercent = getPartitionUsage(partition) * 100
                                if usagePercent > arguments.getArg('hiWater'):
                                    # we were not able to remove a sufficient amount of storage - disable partition writing
                                    try:
                                        disablePartition(partition)
                                    except DBCommandException as exc:
                                        ssLog.writeWarning([ exc.args[0] ])
                                        ssLog.writeWarning([ 'unable to disable writing to partition ' + partition ])
                                        # do not terminate stewie (swallow exception)
                                    except:
                                        import traceback

                                        ssLog.writeWarning([ traceback.format_exc(5) ])
                                        ssLog.writeWarning([ 'unable to disable writing to partition ' + partition ])
                                        # do not terminate stewie (swallow exception)
                                elif usagePercent > arguments.getArg('lowWater'):
                                    # put back in the Q for next iteration
                                    activeFindersQ.put(suFinder)
                            except FileStatException as exc:
                                ssLog.writeWarning([ exc.args[0] + '; removing finder ' + suFinder.getID() + ' from active set of finders that get scrubbed' ])
                                pass
                        except queue.Empty:
                            # no items in Q
                            pass
                    finally:
                        th.enableInterrupts()

                # sleep until next iteration
                sleepInterval = datetime.timedelta(seconds=arguments.getArg('mainSleep')) - (datetime.now() - startTime)
                if sleepInterval > 0:
                    sys.sleep(sleepInterval)
    except TerminationException as exc:
        msg = exc.args[0]
        if ssLog:
            ssLog.writeCritical([ msg ])
        else:
            print(msg, file=sys.stderr)
        # rv is SS_SUCCESS
    except SSException as exc:
        msg = exc.args[0]
        if ssLog:
            ssLog.writeError([ msg ])
        else:
            print(msg, file=sys.stderr)

        rv = exc.retcode
    except:
        import traceback
        msg = traceback.format_exc(5)
        if ssLog:
            ssLog.writeError([ msg ])
        else:
            print(msg, file=sys.stderr)
        rv = SS_UNKNOWNERROR

    msg = 'exiting with return code ' + str(rv)
    if ssLog:
        ssLog.writeCritical([ msg ])
    else:
        print(msg, file=sys.stderr)
    
    logging.shutdown()

    sys.exit(rv)
