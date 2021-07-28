#!/usr/bin/env python3

# for each partition, a finite pool of eligible-to-delete SUDIRs is maintained by a SUFinder  
# partition loop:
# 1. identify all partitions that have SUDIRs that are eligible for cleaning (partition usage is above a high-water mark)
# 2. 

# each iteration through the SUMS partitions, a chunk of SUDIRs is deleted from each partition; 

import sys

if sys.version_info < (3, 4):
    raise Exception('you must run the 3.4 release, or a more recent release, of Python')

import re
import os
import pathlib
import shutil
import threading
import signal
import copy
import inspect
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
from ctypes import c_double
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsLock import DrmsLock

# SUSTEWIE GLOBALS
LOG_FILE_BASE_NAME = 'sslog'
SUB_CHUNK_SIZE = 128

# EXIT CODES
SS_SUCCESS = 0
SS_UNKNOWNERROR = 1
SS_TERMINATED = 2
SS_DRMSPARAMS = 3
SS_ARGS = 4
SS_KWARGS = 5
SS_DBCONNECTION = 6
SS_DBCOMMAND = 7
SS_SUFINDER = 8

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
        
class DisplayHelpException(SSException):
    '''
    '''
    def __init__(self):
        super(DisplayHelpException, self).__init__('')

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
        super(KWArgumentException, self).__init__(msg)
        
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
        self._lockFile = Arguments.checkArg('lockFile', KWArgumentException, **kwargs)
        self.__log = Arguments.checkArg('log', KWArgumentException, **kwargs)
        self.__pid = Arguments.checkArg('pid', KWArgumentException, **kwargs)
        self.__dbhost = Arguments.checkArg('dbHost', KWArgumentException, **kwargs)
        self.__dbname = Arguments.checkArg('dbName', KWArgumentException, **kwargs)
        self.__dbport = Arguments.checkArg('dbPort', KWArgumentException, **kwargs)
        self.__dbuser = Arguments.checkArg('dbUser', KWArgumentException, **kwargs)

        self.__ssLock = None
        self.__savedSignals = None

        super(TerminationHandler, self).__init__()
        
    def __enter__(self):
        self.enableInterrupts()

        # Acquire locks.
        self.__ssLock = DrmsLock(self._lockFile, str(self.__pid))
        self.__ssLock.acquireLock()
        
        # open SUMS DB connection(s)
        for nConn in range(0, DBConnection.getMaxConn()):
            DBConnection.getThreadList().append(DBConnection(host=self.__dbhost, port=self.__dbport, database=self.__dbname, user=self.__dbuser, log=self._log))

    # __exit__() is called if an EXCEPTION occurs inside the with block; since SIGINT is converted
    # into a KeyboardInterrupt exception, it would be handled by __exit__(); however, SIGTERM and SIGHUP are not converted
    # into an exceptions, so __exit__() would normally not execute; to ensure that __exit__() is called
    # for all three signals, we need to install signal handlers for SIGTERM and SIGHUP
    def __exit__(self, etype, value, traceback):
        self._log.write_debug([ 'TerminationHandler.__exit__() called' ])
        if etype is not None:
            # if the context manager was exited without an exception, then etype is None;
            # the context manager was interrupted by an exception
            import traceback
            self._log.write_debug([ traceback.format_exc(5) ])

        print('\nSU Steward is shutting down...')
        self.__finalStuff()
        print('...and done')
        
        # clean up lock
        try:     
            self.__ssLock.releaseLock()   
            self.__ssLock.close()
            self.__ssLock = None
        except IOError:
            pass
            
        self._log.write_debug([ 'exiting TerminationHandler' ])
        
        if etype == SystemExit:
            # the context manager was interrupted by either SIGINT, SIGTERM, or SIGHUP (or a call to sys.exit(), but that
            # should not be called anywhere in the context
            return True # do not propagate SystemExit
        else:
            raise TerminationException('termination context manager interrupted abnormally')
            # exception will propagate

    def __terminator(signo, frame):
        # raise the SystemExit exception (which will be handled by the __exit__() method)
        sys.exit(0)

    def __saveSignal(self, signo, frame):
        if self.__savedSignals == None:
            self.__savedSignals = []

        self.__savedSignals.append((signo, frame))
        self._log.write_debug([ 'saved signal ' +  signo ])

    def disableInterrupts(self):
        signal.signal(signal.SIGTERM, self.__saveSignal)
        signal.signal(signal.SIGHUP, self.__saveSignal)

    def enableInterrupts(self):
        signal.signal(signal.SIGTERM, self.__terminator)
        signal.signal(signal.SIGHUP, self.__terminator)

        if type(self.__savedSignals) is list:
            for signalReceived in self.__savedSignals:
                self.__terminator(*signalReceived)

        self.__savedSignals = None

    def __finalStuff(self):
        msg = 'stop-up the SUFinder queues'
        self._log.write_info([ msg ])
        print(msg)
        
        finders = SUFinder.getFinders()
        
        # halt SUFinder thread(s); do not halt any other threads;
        # the main thread will wait for SUChunk threads to complete, and SUChunk threads will
        # wait for Worker threads to complete
        for finder in finders:
            msg = 'putting a stopper in the ' + SUFinder.__name__ + ' (ID ' + finder.getID() + ') queue'
            self._log.write_info([ msg ])
            print(msg)
            # this stops-up the finder queue and it also kills the finder thread
            finder.stop()
        
        msg = 'waiting for the tasks in the SUFinder queues to complete'
        self._log.write_info([ msg ])
        
        for finder in finders:
            msg = 'waiting for the ' + SUFinder.__name__ + ' (ID ' + finder.getID() + ') queue to become empty'
            self._log.write_info([ msg ])
            print(msg)
            finder.getQueue().join()
            msg = 'finder (ID ' + finder.getID() + ') queue is empty'
            self._log.write_info([ msg ])
            print(msg)

        while True:
            tList = SUFinder.getThreadList()        
            th = None

            if len(tList) > 0:
                th = tList[0]
            else:
                break

            if th and isinstance(th, (SUFinder)) and th.is_alive():
                # can't hold lock here - when the thread terminates, it acquires the same lock
                msg = 'waiting for the ' + SUFinder.__name__ + ' (ID ' + finder.getID() + ') thread to terminate'
                self._log.write_info([ msg ])
                print(msg)
                th.join() # will block, possibly for ever

        # clean up DB connections
        DBConnection.closeAll()
        msg = 'closed all DB connections'
        self._log.write_info([ msg ])
        print(msg)


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
            
    def dump(self, log):
        attrList = []
        for attr in sorted(vars(self)):
            attrList.append('  ' + attr + ':' + str(getattr(self, attr)))
        log.write_webug([ '\n'.join(attrList) ])
            
    @classmethod
    def checkArg(cls, argName, excCls, **kwargs):
        val = None
        if argName in kwargs:
            val = kwargs[argName]
        elif excCls is not None:
            raise excCls('missing argument ' + argName)
        return val


# we're going to need one for finding SUs to delete (one thread), and one for each thread deleting SUs and updating 
# the SUMS tables (one thread per chunk of SUs we are deleting)
class DBConnection(object):
    __tList = [] # list of existing DBConnection threads
    __connListFree = [] # list of currently unused DB connections
    __tListLock = threading.RLock() # guard list access - the thread that has the lock can call acquire(), and it will not block;
                                    # even though a DBConnection object is not a thread, these objects will be used in multiple threads
    __maxConn = 8
    __eventConnFree = threading.Event() # event fired when a connection gets freed up
    __nextIDseq = 0 # the id of the next connection

    def __init__(self, **kwargs):
        self.__host = Arguments.checkArg('host', KWArgumentException, **kwargs)
        self.__port = Arguments.checkArg('port', KWArgumentException, **kwargs)
        self.__database = Arguments.checkArg('database', KWArgumentException, **kwargs)
        self.__user = Arguments.checkArg('user', KWArgumentException, **kwargs)
        self._log = Arguments.checkArg('log', KWArgumentException, **kwargs)
    
        self.__conn = None        
        self.__id = str(DBConnection.__nextIDseq) # do not call the constructor from more than one thread!

        DBConnection.__nextIDseq += 1

        # connect to the db; if things succeed, then save the db-connection information
        self.openConnection()
        
        self.__connLock = threading.Lock() # to ensure that different cursor's commands do not get interrupted
        
    def getID(self):
        return self.__id
        
    def acquireLock(self):
        return self.__connLock.acquire()
        
    def releaseLock(self):
        return self.__connLock.release()
        
    def commit(self):
        # Does not close DB connection. It can be used after the commit() call.
        if not self.__conn:
            raise DBCommandException('cannot commit - no database connection exists')
            
        if self.__conn:
            self.__conn.commit()

    def rollback(self):
        # Does not close DB connection. It can be used after the rollback() call.
        if not self.__conn:
            raise DBCommandException('cannot rollback - no database connection exists')

        if self.__conn:
            self.__conn.rollback()

    def close(self):
        # Does a rollback, then closes DB connection so that it can no longer be used.
        self.closeConnection()
            
    def openConnection(self):
        if self.__conn:
            raise DBConnectionException('already connected to the database')
            
        try:
            self.__conn = psycopg2.connect(host=self.__host, port=self.__port, database=self.__database, user=self.__user)
            self._log.write_info([ 'user ' + self.__user + ' successfully connected to ' + self.__database + ' database: ' + self.__host + ':' + str(self.__port) + ' - id ' + self.__id ])
        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection
            if hasattr(exc, 'diag') and hasattr(exc.diag, 'message_primary'):
                msg = exc.diag.message_primary
            else:
                msg = 'Unable to connect to the database (no, I do not know why).'
            raise DBConnectionException(msg)
            
        # must add to the list of connections and free connections
        DBConnection.__tListLock.acquire()
        try:
            DBConnection.__tList.append(self)
            DBConnection.__connListFree.append(self)
        finally:
            DBConnection.__tListLock.release()

        self._log.write_debug([ 'added connection ' + self.__id + ' to connection list and free connection list' ])

    def closeConnection(self):    
        if not self.__conn:
            raise DBConnectionException('there is no database connection')
        
        if self.__conn:
            DBConnection.__tListLock.acquire()
            try:
                self.__conn.close()
                self._log.write_info([ 'closed DB connection ' + self.id ])

                if self in DBConnection.__connListFree:
                    DBConnection.__connListFree.remove(self)
                    self._log.write_debug([ 'removed DB connection ' + self.id + ' from free connection list'])
                DBConnection.__tList.remove(self)
                self._log.write_debug([ 'removed DB connection ' + self.id + ' from connection list'])

            finally:
                DBConnection.__tListLock.release()
            
    def release(self):
        DBConnection.__tListLock.acquire()
        try:
            # add this connection to the free list
            DBConnection.__connListFree.append(self)
            
            # signal a thread waiting for an open connection (if there were previously no slots open)
            if len(DBConnection.__connListFree) == 1:
                # fire event so that worker can obtain a DB slot
                DBConnection.__eventConnFree.set()
                # clear event so that a worker will block the next time it calls wait()
                DBConnection.__eventConnFree.clear()            
        finally:
            DBConnection.__tListLock.release()

    def exeCmd(self, cmd, results, result=True):
        if not self.__conn:
            raise DBCommandException('cannot execute database command ' + cmd + ' - no database connection exists')
        
        if result:
            try:
                with self.__conn.cursor('namedCursor') as cursor:
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
                with self.__conn.cursor() as cursor:
                    try:
                        cursor.execute(cmd)
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise DBCommandException(exc.diag.message_primary)
            except psycopg2.Error as exc:
                raise DBCommandException(exc.diag.message_primary)
                
    @classmethod
    def getThreadList(cls):
        return cls.__tList
        
    @classmethod
    def getMaxConn(cls):
        return cls.__maxConn
                
    @classmethod
    def nextOpenConnection(cls):
        conn = None
        while True:
            cls.__tListLock.acquire()
            try:
                if len(cls.__connListFree) > 0:
                    conn = cls.__connListFree.pop(0)                
                    break # the finally clause will ensure the connList lock is released
            finally:
                cls.__tListLock.release()
    
            # There were no free threads. Wait until there is a free thread.
            cls.__eventConnFree.wait()
            # We woke up, because a free DB connection became available. However, that DB connection could 
            # now be in use. Loop and check again.
            
        return conn
        
    @classmethod
    def closeAll(cls):
        cls.__tListLock.acquire()
        try:
            for conn in cls.__tList:
                conn.closeConnection()
        finally:
            cls.__tListLock.release()


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
            
    def get_level(self):
        # Hacky way to get the level - make a dummy LogRecord
        logRecord = self.log.makeRecord(self.log.name, self.log.getEffectiveLevel(), None, '', '', None, None)
        return logRecord.levelname
        
    def __prependFrameInfo(self, msg):
        frame, fileName, lineNo, fxn, context, index = inspect.stack()[2]
        return os.path.basename(fileName) + ':' + str(lineNo) + ': ' + msg

    def write_debug(self, text):
        if self.log:
            for line in text:                
                self.log.debug(self.__prependFrameInfo(line))
            self.fileHandler.flush()
            
    def write_info(self, text):
        if self.log:
            for line in text:
                self.log.info(self.__prependFrameInfo(line))
        self.fileHandler.flush()
    
    def write_warning(self, text):
        if self.log:
            for line in text:
                self.log.warning(self.__prependFrameInfo(line))
            self.fileHandler.flush()
    
    def write_error(self, text):
        if self.log:
            for line in text:
                self.log.error(self.__prependFrameInfo(line))
            self.fileHandler.flush()
            
    def write_critical(self, text):
        if self.log:
            for line in text:
                self.log.critical(self.__prependFrameInfo(line))
            self.fileHandler.flush()


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


class SUChunk(threading.Thread):
    __tList = []
    __tLock = threading.Lock() # coarse thread lock
    __max_threads = 32
    __max_threadsEvent = threading.Event() # event fired when the number of threads decreases below threshold
    __idInt = 0

    def __init__(self, **kwargs):
        self.__sudirs = Arguments.checkArg('sudirs', KWArgumentException, **kwargs) # a list of SUDIRs
        self.__dbconn = Arguments.checkArg('dbconn', KWArgumentException, **kwargs)
        self._log = Arguments.checkArg('log', KWArgumentException, **kwargs)
        self.__suFinder = Arguments.checkArg('finder', KWArgumentException, **kwargs)

        self.__sudirsStr = [ str(sudir) for sudir in self.__sudirs ]
        self.__id = str(SUChunk.__idInt)
        SUChunk.__idInt += 1
        self.__deleteEvent = threading.Event()

        super(SUChunk, self).__init__()
    def run(self):
        try:
            # start a worker for each SUDIR rm
            for sudir in self.__sudirs:
                try:
                    if os.path.exists(sudir):
                        shutil.rmtree(sudir)
                        
                    # update the DB (if rmdir succeeded)
                    # DELETE FROM SUM_PARTN_ALLOC WHERE ds_index IN (self.sudirsStr); DELETE FROM SUM_MAIN WHERE ds_index IN (self.sudirsStr); 
                    sql = ''
                    sql += 'DELETE FROM ' + SUM_PARTN_ALLOC + ' WHERE ds_index IN (' + self.__sudirsStr + ')' + ';'
                    sql += 'DELETE FROM ' + SUM_MAIN + ' WHERE ds_index IN (' + self.__sudirsStr + ')'
        
                    self.__dbconn.acquireLock()
                    try:
                        self.__dbconn.exeCmd(sql, None, False)
                        self.__dbconn.commit()
                    except DBCommandException as exc:
                        self._logWriteError([ exc.args[0] ])
                        self.__dbconn.rollback()
                        # do not re-raise - we need to wait for the Worker threads to complete, and we need to remove SUDIRs from the finder
                    except:
                        import traceback
                
                        self._logWriteError([ traceback.format_exc(5) ])
                        # do not re-raise - we need to wait for the Worker threads to complete, and we need to remove SUDIRs from the finder
                    finally:
                        self.__dbconn.releaseLock()
                except:
                    import traceback
                    self._log.write_warning([ 'unable to delete SU ' + path + '; ' + traceback.format_exc(5)])
                    # swallow exception so that stewie keeps running            
            
            SUFinder.acquireLock()
            try:
                # removeSUDIRs() will fire the waitForChunk event so that the SUFinder knows it can add more SUs to its
                # global pool of SUs that will be deleted
                self.__suFinder.removeSUDIRs(self.__sudirs)
            finally:
                SUFinder.releaseLock()
            
        finally:
            # all SUDIRs have been deleted (or a timeout occurred); will not acquire active chunk CV until main calls wait()
            with Partition.activeChunkCV:
                self.__suFinder.getPartition().removeActiveChunk(self)
                Partition.activeChunkCV.notify()
                # release the cv lock
        
            SUChunk.__tLock.acquire()
            try:

                SUChunk.__tList.remove(self) # This thread is no longer one of the running threads.                
                if len(SUChunk.__tList) == SUChunk.__max_threads - 1:
                    SUChunk.fireMaxThreadsEvent()
                
                self._log.write_info([ 'SU Chunk (ID ' +  self.__id + ') terminated' ])
            finally:
                SUChunk.__tLock.release()
                
    def getID(self):
        return self.__id
        
    def getSUDIRs(self):
        sudirs = () # immutable

        cls.acquireLock()
        try:
            for sudir in self.__sudirs:
                sudirs = sudirs + (sudir,)
        finally:
            cls.releaseLock()
        
        return sudirs

    # called from main
    def delete(self, chunkCompleteEventA, chunkCompleteEventB):
        self.fireDeleteEvent()
        self.__chunkCompleteEventA = chunkCompleteEventA
        self.__chunkCompleteEventB = chunkCompleteEventB
    
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
            sus = Arguments.checkArg('sus', KWArgumentException, **kwargs)
            suFinder = Arguments.checkArg('finder', KWArgumentException, **kwargs)
            dbconn = Arguments.checkArg('dbconn', KWArgumentException, **kwargs)
            log = Arguments.checkArg('log', KWArgumentException, **kwargs)
            
            if len(cls.__tList) == cls.__max_threads:
                raise KWArgumentException('the number of chunks ' + str(len(sus)) + ' must be smaller than the number of free SUChunk threads ' + str(cls.__max_threads))

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
            sus = Arguments.checkArg('sus', KWArgumentException, **kwargs) # list of list of SUs
            suFinder = Arguments.checkArg('finder', KWArgumentException, **kwargs)
            dbconn = Arguments.checkArg('dbconn', KWArgumentException, **kwargs)
            log = Arguments.checkArg('log', KWArgumentException, **kwargs)

            if len(sus) + len(cls.__tList) > cls.__max_threads:
                raise KWArgumentException('the number of chunks ' + str(len(sus)) + ' must be smaller than the number of free SUChunk threads ' + str(cls.__max_threads))

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
        cls.__max_threadsEvent.wait()
    
    @classmethod
    def fireMaxThreadsEvent(cls):
        cls.__max_threadsEvent.set()
        cls.__max_threadsEvent.clear()
        
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


class SUInfoItem(object):
    def __init__(self, **kwargs):
        self.dir = kwargs['dir']
        self.series = kwargs['series']
        self.enjoyby = kwargs['enjoyby']
        
    def __eq__(self, other):
        if isinstance(other, type(self).__name__):
            return pathlib.PurePath(self.dir) == pathlib.PurePath(other.dir) and lower(self.series) == lower(other.series) and self.enjoyby == other.enjoyby
        else:
            return False
    
    def __hash__(self):
        return hash((self.dir, self.series, self.enjoyby))


class SUFinder(threading.Thread):
    '''
    queries the SUMS DB to obtain expired SUs;
    runs os.statvfs(), which runs the statfs() system call; this call could time-out;
    both of these factors could make these threads run slowly; 
    one SUFinder instance per partition   
    '''
    __threads = []
    __tLock = threading.Lock() # coarse thread lock
    __idInt = 0
    __max_threads = 128
    __stopEvent = threading.Event() # event fired when shutting down

    def __init__(self, **kwargs):
        self.__partition = Arguments.checkArg('partition', KWArgumentException, **kwargs)
        # the maximum number of SUs in the pool available for deletion
        self.__maxInPool = Arguments.checkArg('maxsudirs', KWArgumentException, **kwargs)
        self._log = Arguments.checkArg('log', KWArgumentException, **kwargs)
        self.__dbConn = DBConnection.nextOpenConnection()
        
        # no need to acquire lock since SUFinder objects are created in the main thread and all SUFinder objects
        # are created before any of their threads are run
        self.__id = str(SUFinder.__idInt) # although this is the definitive ID, use the partition when identifying an SUFinder
        SUFinder.__idInt += 1

        # __suPool : a list where the first element is a dict where key is sudir (str) and val is a queue element 
        # (SUInfoItem(object)); and the second element is a priority queue of queue elements (SUInfoItem(object))
        # 
        # [ { sudir => SUInfoItem, ... }, [ queueElement, ...] ]
        # 
        # where queueElement is a SUInfoItem and the queueElements are sorted by expDate, then series
        self.__suPool = [ {}, queue.Queue(self.__maxInPool) ]
        self.__stewieTable = 'stewie' + self.__id
        self.__stewieTableIndex = self.__stewieTable + '_pkey'
        self.rehydrate() # initialize the sustewie table (expired SUs from SUM_PARTN_ALLOC) for the parition
        
        super(SUFinder, self).__init__()

    def run(self):
        try:
            suQueue = self.__suPool[1]

            while not self.__stopEvent.is_set():
                # find more SUs for deletion
                needMoreSUs = True
                suInfo = set() # ensures no 
                rows = []

                self.__dbConn.acquireLock()
                try:
                    # select ALL expired SUs from a partition and push them onto the suQueue
                    sql = 'SELECT ' + SUM_MAIN + '.owning_series AS series, ' + self.__stewieTable + '.sudir AS sudir, ' + self.__stewieTable + '.enjoyby AS enjoyby FROM ' + self.__stewieTable + ' LEFT OUTER JOIN ' + SUM_MAIN + ' ON ' + self.__stewieTable + '.sunum = ' + SUM_MAIN + '.ds_index WHERE ' + self.__stewieTable + '.sudir LIKE ' + "'" + self.__partition + '/%' + "'"
                    self.__dbConn.exeCmd(sql, rows, True)
                    self.__dbConn.rollback() # rollback the transaction, even though the transaction did not modify the DB
                    
                    self._log.write_debug([ 'SUFinder found ' + str(len(rows)) + ' expired SUs in partition ' + self.__partition ])
                    for row in rows:
                        suInfo.add(SUInfoItem(dir=row[1], series=row[0], enjoyby=row[2]))
                
                    if len(suInfo) > 0:
                        # will block if all sudir slots are occupied
                        self.addSUDIRS(suInfo) # pushes items onto suQueue
                except DBCommandException as exc:    
                    self._log.Write_error([ exc.args[0] ])
                    # do not raise - go to next iteration
                except:
                    import traceback
                    
                    msg = 'SUFinder (partition ' + self.__partition + ') died; ' + traceback.format_exc(5)
                    self._log.write_error([ msg ])
                    self.__dbConn.rollback() # end the transaction, even though the transaction did not modify the DB
                    # do not raise - go to next iteration
                finally:
                    self.__dbConn.releaseLock()
                    
                # at this point, there are pending SUs in the pool; if we were to re-run the sql, we'd end up with self.__maxInPool
                # duplicates - so wait until the pool is empty before re-running the sql
                suQueue.join()
                self._log.write_debug([ 'SUFinder for ' + self.__partition + ' is waking' ])
                
        finally:
            # the stop event was fired (or an exception occurred) - push a None on the queue so that the main thread 
            # knows not to make any more suQueue.get() calls
            suQueue.put(None)
        
            SUFinder.__tLock.acquire()
            try:
                SUFinder.__threads.remove(self) # This thread is no longer one of the running threads.
                if len(SUFinder.__threads) == SUFinder.__max_threads - 1:
                    SUFinder.fireMaxThreadsEvent()
            finally:
                SUFinder.__tLock.release()
            
            self.__dbConn.release()
            self._log.write_debug([ 'SUFinder released DB connection ' + self.__dbConn.getID() ])
    
    # in the main thread
    def stop(self):
        # fire stop event so no more SUDIRs are added to the queue
        self.__stopEvent.set()
            
    def getID(self):
        return self.__id
        
    def getPartition(self):
        return self.__partition
        
    def getQueue(self):
        return self.__suPool[1]
        
    def setPartition(self, partition):
        self.__partition = partition
    
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
            sql += 'CREATE TEMPORARY TABLE ' + self.__stewieTable + ' AS SELECT ds_index AS sunum, wd AS sudir, effective_date AS enjoyby FROM ' + SUM_PARTN_ALLOC + ' WHERE ' + SUM_PARTN_ALLOC + '.status = ' + str(DADP) + ' AND ' + SUM_PARTN_ALLOC + '.wd LIKE ' + "'" + self.__partition + '/%' + "'" + ' AND effective_date < ' + "'" + nowTimeStr + "';";
            sql += 'CREATE INDEX ' + self.__stewieTableIndex + ' ON ' + self.__stewieTable + ' (sunum);'
            
            self.__dbConn.exeCmd(sql, None, False)
            self.__dbConn.commit()
            self._log.write_info([ 'successfully rehydrated steward table ' + self.__stewieTable ])
        except DBCommandException as exc:
            self._log.write_error([ exc.args[0] + ': ' + sql ])
            self.__dbConn.rollback()
        except:
            import traceback
            
            self._log.write_error([ traceback.format_exc(5) ])
            self.__dbConn.rollback()
        finally:
            self.__dbConn.releaseLock()

    # in main thread (get from queue); interrupts are disabled, so no suQueue.get() loop can get interrupted by 
    # a SIGINT signal
    def deleteSUChunks(self, totalNum):
        # we want to chunk-up totalNum SUs; totalNum is the chunk-size on which the user wants to operarate during 
        # each iteration; we want to break this chunk into sub-chunks so we can parallelize
        chunker = None
        suChunks = []
        suQueue = self.__suPool[1]

        try:
            # remove a max of totalNum SUs from head of __maxInPool (head is the 'most expired' SU, tail is the least)
            queueElements = []
            metaChunk = None
            numRemoved = 0

            try:
                for item in range(0, totalNum):
                    elem = suQueue.get(False) # will not block

                    if elem is not None:
                        queueElements.append(elem)
                        numRemoved += 1
                    else:
                        # this thread is shutting down
                        self._log.write_debug([ 'main thread found cork in suQueue' ])
                        break
                        
                self._log.write_debug([ 'main thread removed ' + str(numRemoved) + ' SUDIRs to the queue for partition ' + self.__partition ])
            except queue.Empty:
                # tried to get a queueElement, but none are left; this is OK because we want to get AT MOST seriesNum
                # elements; on to the next series; 
                # we want to notify the SUFinder thread that the queue was empty; we'll simply skip this iteration
                # of checking for deleteable SUs, even though there might be some new ones marked for deletion in the
                # DB
                pass
        
            metaChunk = [ item.dir for item in queueElements ] 
        
            if metaChunk:
                chunker = Chunker(metaChunk, chSize=SUB_CHUNK_SIZE)
        except:
            chunker = None
            # we need to mark the queue items complete - we will not be operating on any of them, so they essentially get
            # flushed from the queue
            if metaChunk:
                for sudir in metaChunk:
                    suQueue.task_done()

            import traceback
            self._log.write_error([ traceback.format_exc(5) ])
            
        # wait until main is ready for SUChunk deletions to start occurring
        scrubEvent.wait()

        if chunker:
            for chunk in chunker:
                # chunk is a list of SUDIRs
                suChunk = None
                try:
                    while True:
                        SUChunk.__tLock.acquire()
                        try:
                            if len(SUChunk.getNumThreads()) < SUChunk.getMaxThreads():
                                self_log.write_info([ 'instantiating an SU chunk for SUs ' + chunk[0] + '...' ])
                                # start a worker to handle the deletion of arguments.su-chunk-size SUs
                                suChunk = SUChunk.newSUChunk(sus=chunk, finder=self, dbconn=self.__dbConn, log=self._log)
                                SUChunk.__newThread(suChunk)
                                suChunks.append(suChunk)
                                break # the finally clause will ensure the SUChunk lock is released
                        except StartThreadException:
                            # Ran out of system resources - could not start new thread. Just wait for a thread slot to become free.
                            self._log.write_warning([ 'unable to start a new SU chunk thread; trying again later' ])
                            suChunk = None
                        finally:
                            SUChunk.__tLock.release()

                        SUChunk.waitMaxThreadsEvent()
                finally:
                    # if something goes haywire for a chunk, call task_done() on the chunk
                    if suChunk not in suChunks:
                        for sudir in chunk:
                            suQueue.task_done()

        return suChunks

    # in SUFinder thread (put into queue)
    def addSUDIRS(self, suInfo):
        '''
        suInfo : a list of SUInfoItem objects (sudir, expDate, series), sorted by expiration date, series
        '''
        numAdded = 0
        finder = self.__suPool[0] # map sudir to heapElement
        suQueue = self.__suPool[1]
        
        sortedSuInfo = sorted(suInfo, key=lambda info: (info.enjoyby, info.series))

        for info in sortedSuInfo:
            if info.dir in finder:
                raise SUFinderException('attempt to add a duplicate SUDIR to the queue: ' + info.dir)

            finder[sudir] = info
            # will block if the suQueue is full
            suQueue.put(info)
            self._log.write_debug([ 'added ' + info.dir + ' to the queue for partition ' + self.__partition ])

    # in SUChunk thread (remove queueElement from sudir --> queueElement map)
    def removeSUDIRs(self, sudirs):
        finder = self.__suPool[0] # map
        suQueue = self.__suPool[1] # queue
        
        for sudir in sudirs:
            if sudir not in finder:
                raise SUFinderException('attempt to remove an unknown SUDIR (' + sudir + ')')

            # the sudir is known to the SUFinder (the queueElement has already been removed from the series dictionaries)
            suQueue.task_done()
            del finder[sudir]
            self._log.write_debug([ 'removed ' + sudir + ' from the ' + self.__partition + ' queue map' ])
    
    @property
    def free_thread_exists(self):
        return len(__threads) < __max_threads
                
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
            for finder in cls.__threads:
                finders = finders + (finder,)        
        finally:
            cls.releaseLock()
            
        return finders
        
    @classmethod
    def rehydrateFinders(cls):
        finders = cls.getFinders()
        for finder in finders:
            finder.rehydrate()
            finder.log.write_info([ 'rehydrated finder ' + finder.getID() ])
            
    @classmethod
    def getThreadList(cls):
        return cls.getFinders()
        
    @classmethod
    def getMaxThreads(cls):
        return cls.__max_threads
        
    @classmethod
    def waitMaxThreadsEvent(cls):
        cls.__max_threadsEvent.wait()
    
    @classmethod
    def fireMaxThreadsEvent(cls):
        cls.__max_threadsEvent.set()
        cls.__max_threadsEvent.clear()
    
    # acquire lock first
    @classmethod
    def __newThread(cls, suFinder):
        cls.__threads.append(suFinder)
        suFinder.start()
    
    # acquire lock first
    @classmethod
    def new_finder(cls, **kwargs):
        suFinder = None
        
        try:
            partitionName = Arguments.checkArg('partitionName', KWArgumentException, **kwargs)
            maxSUDIRs = Arguments.checkArg('maxSUDIRs', KWArgumentException, **kwargs)
            log = Arguments.checkArg('log', KWArgumentException, **kwargs)
            
            if len(cls.__threads) == cls.__max_threads:
                raise KWArgumentException('all SUFinder slots are occupied')

            suFinder = cls(partitionName=partitionName, maxSUDIRs=maxSUDIRs, log=log)

            # spawn thread
            cls.__newThread(suFinder)
        except:
            suFinder = None
            raise
            
        return suFinder
    
    # acquire lock first
    @classmethod
    def new_finders(cls, **kwargs):
        suFinders = []
        
        try:
            partitionNames = Arguments.checkArg('partitionNames', KWArgumentException, **kwargs)
            maxSUDIRs = Arguments.checkArg('maxSUDIRs', KWArgumentException, **kwargs)
            log = Arguments.checkArg('log', KWArgumentException, **kwargs)

            if len(partitions) + len(cls.__threads) > cls.__max_threads:
                raise KWArgumentException('the number of partitions ' + str(len(partitions)) + ' must be smaller than the number of SUFinder threads ' + str(SUFinder.__max_threads))

            for partitionName in partitionNames:
                suFinders.append(cls(partitionName=partitionName, maxSUDIRs=maxSUDIRs, log=log))

            # spawn threads
            for suFinder in suFinders:
                cls.__newThread(suFinder)
        except:
            suFinders = None
            raise
            
        return suFinders


class PartitionScrubber(object):
    # abstract class
    __activeChunkLock = threading.RLock()
    __activeChunkCV = threading.Condition(__activeChunkLock)

    def __init__(self, *, partition, finder, su_chunk_size=4096, lo_water=90, hi_water=95, log=None):
        self._partition = partition
        self._finder = finder
        self._su_chunk_size = su_chunk_size
        self._lo_water = lo_water
        self._hi_water = hi_water
        self._log = log

        self._active_chunks = []
        self._bytes_deleted = 0
        
    @property
    def name(self):
        return self._partition
    
    # called by main thread, inside __activeChunkLock    
    @property
    def active_chunks(self):
        # do not allow calling thread to modify our threads!
        su_chunks = () # immutable

        for su_chunk in self._active_chunks:
            su_chunks = su_chunks + (su_chunk,)
        
        return su_chunks
        
    def get_status(self):
        status = None
        
        # only delete chunks for this partition until the partition usage is less than the low-water mark
        try:
            usage = self.usage
            usage_percent = usage * 100
            if usage_percent > self._lo_water:
                if self._pre_scrub_usage - usage == 0:
                    # we were not able to remove a sufficient amount of storage - disable partition writing
                    try:
                        self.disable()
                        self._log.write_debug([ 'disabled writing to partition ' + self._partition + '; unable to lower usage below hi-water mark' ])
                    except DBCommandException as exc:
                        self._log.write_warning([ exc.args[0] ])
                        self._log.write_warning([ 'unable to disable writing to partition ' + self._partition ])
                        # do not terminate stewie (swallow exception)
                    except:
                        import traceback

                        self._log.write_warning([ traceback.format_exc(5) ])
                        self._log.write_warning([ 'unable to disable writing to partition ' + self._partition ])
                        # do not terminate stewie (swallow exception)

                    status = 'disabled'
                elif usage_percent <= self._hi_water:
                    # put back in the Q for next iteration
                    self._log.write_debug([ 'usage for partition ' + self._partition + ' is still above low-water mark; scheduling for another round of scrubbing' ])
                    status = 'reschedule'
                else:
                    status = 'continue'
            else:
                # done scrubbing
                status = 'done'
        except FileStatException as exc:
            ssLog.write_warning([ exc.args[0] + '; unable to get usage for partition ' + self._partition ])
            self.disable()
            status = 'cantstat'
            
        return status
    
    # called by SUChunk thread, inside __activeChunkLock
    def remove_active_chunk(self, chunk):
        self._active_chunks.remove(chunk)

    def scrub(self, usage):
        self._pre_scrub_usage = usage
        # chunk-up an suChunkSize chunk of SUs (arguments.suChunkSize could be too large to efficiently deal with)
        self._active_chunks = self._finder.delete_su_chunks(arguments.getArg('suChunkSize')) # blocks if max number SUChunks are active
            
    # called from main
    def disable(partition):
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
        
    @classmethod
    def getActiveChunkCV(cls):
        return cls.__activeChunkCV


class DirectoryPartitionScrubber(PartitionScrubber):
    @property
    def usage(self):
        lock = Lock()
        percentUsage = Value(c_double, 0, lock=lock)
        proc = Process(target=callStatvfs, args=(self.__partition, percentUsage))
        proc.start()
        proc.join(5) # timeout after 5 seconds

        if proc.exitcode is None:
            raise FileStatException('os.statvfs(' + partition + ') did not terminate')
        
        return percentUsage.value


class ScrubberFactory(object):
    '''
    '''
    def __init__(self, *, log=None):
        self._log = log

    def create_scrubber(self, *, cls, partition, max_sudirs=40960, lo_water=90, hi_water=95):
        '''
        instantiate a scrubber for a single SUMS partition; the scrubber object has a SUFinder instance associated with it
        '''
        scrubber = None

        SUFinder.acquireLock()
        try:
            finder = SUFinder.new_finder(partition=partition, max_sudirs=max_sudirs, log=self._log)

            if not finder.free_thread_exists:
                raise KWArgumentException('the maximum number of finder threads ' + str(finder.max_threads) + ' has been reached - cannot create a new scrubber')
        
            scrubber = cls(partition=partition, finder=finder, lo_water=lo_water, hi_water=hi_water, log=self._log)
        finally:
            SUFinder.releaseLock()
            
        return scrubber


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

def get_partitions(partitionSets):
    # get all partitions that belong to the partition sets passed in (if None is passed in, or the empty list is passed in, then return all partitions)
    partitions = []

    dbConn = DBConnection.nextOpenConnection()
    if dbConn:
        try:
            response = []
            
            if partitionSets and len(partitionSets) > 0:
                partitionSetsStr = ','.join(partitionSets)
                sql = 'SELECT partn_name FROM public.sum_partn_avail WHERE pds_set_prime IN (' + partitionSetsStr + ')'
            else:
                sql = 'SELECT partn_name FROM public.sum_partn_avail'

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
            
    return partitions
            

def set_main_loop_event(mlevent):
    mlevent.set()
    mlevent.clear()
    
    # clear timer too
    

if __name__ == "__main__":
    rv = SS_SUCCESS
    ss_log = None

    try:
        ssDrmsParams = SSDrmsParams()

        parser = CmdlParser(usage='%(prog)s [ OPTION ] | --help', add_help=False)
    
        # optional parameters
        parser.add_argument('-c', '--chunksize', help='the number of SUDIRs to delete during each iteration', metavar='<SUDIR chunk size>', dest='suChunkSize', type=int, default=int(ssDrmsParams.get('SS_SU_CHUNK')))
        parser.add_argument('-D', '--dbdbase', help='the SUMS database (e.g., drms_sums)', metavar='<db database>', dest='database', default=ssDrmsParams.get('DBNAME') + '_sums')
        parser.add_argument('-h', '--hiwater', help='the high water mark (percentage) above which scrubbing is initiated', metavar='<high water mark>', dest='hiWater', type=int, default=int(ssDrmsParams.get('SS_HIGH_WATER')))
        parser.add_argument('--help', help='a flag to display this help message', dest='displayHelp', action='store_true', default=False)
        parser.add_argument('-H', '--dbhost', help='the host machine of the SUMS database', metavar='<db host machine>', dest='dbHost', default=ssDrmsParams.get('SUMS_DB_HOST'))
        parser.add_argument('-l', '--lowater', help='the low water mark (percentage) below which scrubbing is halted', metavar='<low water mark>', dest='loWater', type=int, default=int(ssDrmsParams.get('SS_LOW_WATER')))
        parser.add_argument('-L', '--loglevel', help='the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', dest='logLevel', action=LogLevelAction, default=logging.ERROR)
        parser.add_argument('-p', '--printsudirs', help='a flag to print a list of SUDIRs that would otherwise be deleted  (no SUDIRs will be removed)', dest='printSudirs', action='store_true', default=False)        
        parser.add_argument('-P', '--dbport', help='the port on the host machine that is accepting connections to the SUMS database', metavar='<db host port>', dest='dbPort', type=int, default=int(ssDrmsParams.get('SUMPGPORT')))
        parser.add_argument('-r', '--rehydrateinterval', help='the interval, in seconds, between the caching of expired SUDIR lists', metavar='<rehydrate interval>', dest='rehydrateInterval', type=int, default=int(ssDrmsParams.get('SS_REHYDRATE_INTERVAL')))
        parser.add_argument('-s', '--setlist', help='a list of sets (pds_set_num) of partitions to scrub (e.g., /SUM2,/SUM8, ...)', metavar='<list of partition sets>', dest='partitionSets', action=ListAction) # defaults to None, which means all partitions
        parser.add_argument('-S', '--sleepinterval', help='the interval, in seconds, between each iteration', metavar='<sleep interval>', dest='sleepInterval', type=int, default=int(ssDrmsParams.get('SS_SLEEP_INTERVAL')))        
        parser.add_argument('-U', '--dbuser', help='the SUMS-database user that will connect to the SUMS database (must have write privileges for SUMS objects)', metavar='<db user>', dest='dbUser', default=ssDrmsParams.get('SUMS_MANAGER'))

        try:
            arguments = Arguments(parser)
        except:
            # the argparse parser will print an error message describing the messed-up argument(s)
            parser.print_help()
            raise DisplayHelpException()
        
        if arguments.getArg('displayHelp'):
            parser.print_help()
            raise DisplayHelpException()
        
        arguments.setArg('lockFile', os.path.join(ssDrmsParams.get('DRMS_LOCK_DIR'), ssDrmsParams.get('SS_LOCKFILE')))
        arguments.setArg('logFile', os.path.join(ssDrmsParams.get('SUMLOG_BASEDIR'), LOG_FILE_BASE_NAME + '_' + datetime.now().strftime('%Y%m%d') + '.txt'))
        pid = os.getpid()

        # Create/Initialize the log file.
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ss_log = Log(arguments.getArg('logFile'), arguments.getArg('logLevel'), formatter)
        ss_log.write_critical([ 'starting up SUMS Steward daemon' ])
        ss_log.write_critical([ 'logging threshold level is ' + ss_log.get_level() ]) # critical - always write the log level to the log
        arguments.dump(ss_log)
    
        # TerminationHandler opens a DB connection to the RS database (which is the same as the DRMS database, most likely).
        with TerminationHandler(lockFile=arguments.getArg('lockFile'), log=ss_log, pid=pid, dbHost=arguments.getArg('dbHost'), dbName=arguments.getArg('database'), dbPort=arguments.getArg('dbPort'), dbUser=arguments.getArg('dbUser')) as th:
            # start the finder thread - it will run for the entire duration of the steward
                        
            # find SUs to delete in all specified partitions
            # on error, this raises (terminating stewie)
            partition_list = get_partitions(arguments.getArg('partitionSets'))
            
            if len(partition_list) == 0:
                raise ArgsException('no valid partitions were specified in the setlist argument')
                
            max_sudirs = arguments.getArg('suChunkSize') * 10 # keep max_sudirs in each partition's for-deletion pool
            ss_log.write_info([ 'caching ' + str(max_sudirs) + ' for deletion (per partition)' ])

            main_loop_event = threading.Event() # the main_loop_timer fires this event when it is time for the next main loop iteration

            loWater = arguments.getArg('loWater')
            hiWater = arguments.getArg('hiWater')

            partitionsStr = ','.join(partition_list)
            ss_log.write_info([ 'monitoring partitions ' + partitionsStr ])

            # does not block
            factory = ScrubberFactory(log=ss_log)
            scrubbers = [ factory.create_scrubber(cls=DirectoryPartitionScrubber, name=partition, max_sudirs=max_sudirs, lowater=loWater, hiwater=hiWater) for partition in partition_list ]
            ss_log.write_info([ 'successfully created SUFinders for partitions ' + ','.join(partition.getName() for partition in partitions) ])
            
            rehydrationTimer = threading.Timer(arguments.getArg('rehydrateInterval'), SUFinder.rehydrateFinders, )
            rehydrationTimer.start()

            active_scrubbers = []
            
            # cleaning-session loop
            while True:
                # clear out old timer and create new one
                main_loop_timer = threading.Timer(arguments.getArg('sleepInterval'), set_main_loop_event, arg=main_loop_event, kwargs=None)

                # main loop
                ss_log.write_debug([ 'starting main loop iteration' ])

                # find out which partitions are eligible for cleaning (usage is above the hi-water mark)
                for scrubber in scrubbers:
                    th.disableInterrupts()
                    try:                    
                        try:
                            usage = scrubber.usage
                            if usage > arguments.getArg('hiWater'):
                                ss_log.write_debug([ 'insufficient storage for partition ' + scrubber.partition + '; scheduling for scrubbing' ])
                                # collect a chunk of SUs to delete; scrub() is called by the main thread; the suFinder thread 
                                # periodically updates its list of SUs ready for deletion; when a chunk's worth of SUs are available, 
                                # scrub() atomically removes the SUs from its list of delete-ready SUs, and returns the containing 
                                # chunk to the main thread; if removal of SUs from its list of delete-ready SUs causes enough
                                # slots to open up, the suFinder queries the SUMS DB for more SUs to delete
                                active_scrubbers.append(scrubber)
                        except FileStatException as exc:
                            ss_log.write_warning([ exc.args[0] + '; skipping partition ' + scrubber.partition ])
                            pass
                    finally:
                        th.enableInterrupts()
                        
                # wait for chunks to complete
                activeChunkCV = Partition.getActiveChunkCV()
                with activeChunkCV:
                    # acquired CV lock
                    for scrubber in active_scrubbers:
                        # scrub() - pop one SU queue item (representing one SU dir) per SU chunk item and create a SUChunk instance
                        # (a thread) that handles the actual rm call and the removal from the SUMS db;
                        # call scrub() only while holding the activeChunkCV lock so that scrub() does not call cv.notify()
                        # before main has called cv.wait(); otherwise, main would get stuck blocked on the cv.wait() call
                        scrubber.scrub(scrubber.usage) # usage before scrubbing
                
                    # release the CV lock and wait for any partition to finish scrubbing
                    activeChunkCV.wait()
                    # acquire the CV lock
                    
                    # determine which partition's scrubbing has completed
                    scrubber_complete = None
                    for scrubber in active_scrubbers:
                        if len(scrubber.active_chunks) == 0:
                            # this partition has been scrubbed
                            scrubber_complete = scrubber
                            status = scrubber_complete.status
                            ss_log.write_info([ 'completed scrubbing partition ' + scrubber_complete.partition + '; returned status ' +  status ])
                            break
                    
                    if scrubber_complete:
                        if status == 'done':
                            # below lo water mark - partition has been 
                            ss_log.write_info([ scrubber_complete.partition + ' is now below low-water mark; it will not be scrubbed until usage exceeds the hi-water mark' ])
                            active_scrubbers.remove(scrubber_complete)
                        elif status == 'reschedule':
                            # we are below the high-water mark, but above the low-water mark; leave in the active_scrubbers 
                            # list so we will continue to delete SU chunks; since we are below the high-water mark, the partition
                            # will not be added to active_scrubbers in the next main-loop iteration
                            ss_log.write_info([ partition.getName() + ' is still above the low-water mark; it will be undergo another round of scrubbing' ])
                            pass
                        elif status == 'disabled':
                            # could not get below low water mark; no more writing to this partition allowed
                            ss_log.write_info([ scrubber_complete.partition + ' is still above the low-water mark, but there are no more expired SUs; ']
                            scrubber_complete.disable()
                            active_scrubbers.remove(scrubber_complete)
                        elif status = 'cantstat':
                            ss_log.write_error([ 'cannot get usage status for ' + scrubber_complete.partition ])
                            active_scrubbers.remove(scrubber_complete)
                        else:
                            # status should be continue - still above hi water mark; will get scrubbed again; do not
                            # keep in active_scrubbers; 
                            assert status == 'continue'
                            active_scrubbers.remove(scrubber_complete)                        

                # start a main-loop timer; when this fires, it is time to execute another main-loop iteration
                main_loop_timer.start()

                # sleep until next iteration
                main_loop_event.wait()
    except DisplayHelpException:
        msg = None
        # rv is SS_SUCCESS
    except SSException as exc:
        msg = exc.args[0]
        if ss_log:
            ss_log.write_error([ msg ])
        else:
            print(msg, file=sys.stderr)

        rv = exc.retcode
    except:
        import traceback
        msg = traceback.format_exc(5)
        if ss_log:
            ss_log.write_error([ msg ])
        else:
            print(msg, file=sys.stderr)
        rv = SS_UNKNOWNERROR

    if msg is not None:
        msg = 'exiting with return code ' + str(rv)
        if ss_log:
            ss_log.write_critical([ msg ])
        else:
            print(msg, file=sys.stderr)
    
    logging.shutdown()

    sys.exit(rv)
