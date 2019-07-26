#!/usr/bin/env python

# The client sends a 'subscribe' request to request-subs.py cgi. That cgi inserts
# a row into the requests table with status N (new). The client then polls, by sending a 'polldump' 
# request to request-subs.py, for the DUMP file that manage-subs.py creates in response to 
# the N request. Until the DUMP file has been created, request-subs.py finds either an 
# N or P (pending) request status, and in response returns a 'wait' response to the client.
# In the meantime, the main thread of manage-subs.py finds the N request, 
# sets the request status to P, and spawns a Worker thread. The Worker thread 
# creates the DUMP file, and then it sets the request status to D (dump ready). In response
# to the client polldump request, request-subs.py discovers the D status and 
# now returns a 'continue' response to the client.
# In response to this, the client scps the DUMP file to the client machine, 
# and ingests it. When the client is done ingesting the DUMP file, it sends a 'pollcomplete' 
# request to request-subs.py, indicating to the the server it has successfully
# ingested the DUMP file and is awaiting completion of the subscription request. request-subs.py
# sets the status to I (ingested), which causes the manager to start the clean-up process.
# During clean-up, request-subs.py returns 'wait' to the client. When the manager
# finishing cleaning-up, it sets the status to C (complete) or E (error). Then in response to
# the client pollcomplete request, request-subs.py returns 'success' or 'failure', and
# then it deletes the request from the request table altogether. If the
# response is success, then the client script terminates, and the client can use the
# series subscribed to. If the response is failure, then the client must run get_slony_logs.pl
# (to download any logs with the new series in it) and then delete the series whose
# subscription attempt failed before the client script terminates.
#
# [ client submits request ] --> N --> [ server starts worker ] --> P -->
#    [ server creates the dump ] --> D --> [ client downloads dump ] --> A -->
#    [ client ingests dump ] --> I --> [ server cleans up OR back to D ] --> C -->
#    [ client receives notice ] --> S
#    

# Responses to client polling:
# State       Set By    Response to pollDump        Response to pollComplete
# -----       -------   --------------------        ------------------------
#   N         client    requestQueued               invalidArgument
#   P         server    requestProcessing           invalidArgument
#   D         server    dumpReady                   invalidArgument
#   A         client    invalidArgument             requestFinalizing
#   I         client    invalidArgument             requestFinalizing
#   C         server    invalidArgument             requestComplete
#   S         client    internalError               internalError
#   E         either    requestFailed               requestFailed

import os
import sys
import time
import logging
import threading
import socket
import select
import argparse
import io
import signal
import re
import gzip
import queue
from importlib import reload
from subprocess import check_call, Popen, CalledProcessError, PIPE
from multiprocessing import Process, Lock, Queue, set_start_method
import psycopg2
from datetime import datetime, timedelta, timezone
import shutil
import glob
import fcntl
import psutil
import gc
import json
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../base/libs/py'))
from drmsCmdl import CmdlParser
from drmsLock import DrmsLock
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
from toolbox import getCfg

if sys.version_info < (3, 0):
    raise Exception("You must run the 3.0 release, or a more recent release, of Python.")

RV_TERMINATED = 0
RV_DRMSPARAMS = 1
RV_ARGS = 2
RV_CLIENTCONFIG = 3
RV_REQTABLEWRITE = 4
RV_REQTABLEREAD = 5
RV_INVALIDARGUMENT = 6
RV_UNKNOWNREQUESTID = 7
RV_GETSUBLIST = 8
RV_DBRESPONSE = 9
RV_DBCOMMAND = 10
RV_LOCK = 11
RV_UKNOWNERROR = 12

LST_TABLE_SERIES = 'series'
LST_TABLE_NODE = 'node'
CFG_TABLE_NODE = 'node'

REQUEST_TYPE_GETPENDING = 'getPending'
REQUEST_TYPE_ERROR = 'error'
REQUEST_TYPE_SETSTATUS = 'setStatus'
REQUEST_TYPE_DONE = 'done'


SLONY_DB_OBJECTS = """\
-- ----------------------------------------------------------------------
-- SCHEMA _<cluster>
-- ----------------------------------------------------------------------
CREATE SCHEMA _<cluster>;

-- ----------------------------------------------------------------------
-- TABLE sl_sequence_offline
-- ----------------------------------------------------------------------
CREATE TABLE _<cluster>.sl_sequence_offline (
	seq_id				int4,
	seq_relname			name NOT NULL,
	seq_nspname			name NOT NULL,

	CONSTRAINT "sl_sequence-pkey"
		PRIMARY KEY (seq_id)
);

-- ----------------------------------------------------------------------
-- TABLE sl_archive_tracking
-- ----------------------------------------------------------------------
CREATE TABLE _<cluster>.sl_archive_tracking (
	at_counter			bigint,
	at_created			timestamp,
	at_applied			timestamp
);

-- -----------------------------------------------------------------------------
-- FUNCTION sequenceSetValue_offline (seq_id, last_value)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _<cluster>.sequencesetvalue_offline(int4, int8) RETURNS int4
AS '
declare
	p_seq_id			alias for $1;
	p_last_value		alias for $2;
	v_fqname			text;
begin
	-- ----
	-- Get the sequences fully qualified name
	-- ----
	select "pg_catalog".quote_ident(seq_nspname) || ''.'' ||
			"pg_catalog".quote_ident(seq_relname) into v_fqname
		from _<cluster>.sl_sequence_offline
		where seq_id = p_seq_id;
	if not found then
		raise exception ''Slony-I: sequence % not found'', p_seq_id;
	end if;

	-- ----
	-- Update it to the new value
	-- ----
	execute ''select setval('''''' || v_fqname ||
			'''''', '''''' || p_last_value || '''''')'';
	return p_seq_id;
end;
' language plpgsql;
-- ---------------------------------------------------------------------------------------
-- FUNCTION finishTableAfterCopy(table_id)
-- ---------------------------------------------------------------------------------------
-- This can just be a simple stub function; it does not need to do anything...
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _<cluster>.finishTableAfterCopy(int4) RETURNS int4 AS
  'select 1'
language sql;

-- ---------------------------------------------------------------------------------------
-- FUNCTION archiveTracking_offline (new_counter, created_timestamp)
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _<cluster>.archivetracking_offline(int8, timestamp) RETURNS int8
AS '
declare
	p_new_seq	alias for $1;
	p_created	alias for $2;
	v_exp_seq	int8;
	v_old_seq	int8;
begin
	select at_counter into v_old_seq from _<cluster>.sl_archive_tracking;
	if not found then
		raise exception ''Slony-I: current archive tracking status not found'';
	end if;

	v_exp_seq := p_new_seq - 1;
	if v_old_seq <> v_exp_seq then
		raise exception ''Slony-I: node is on archive counter %, this archive log expects %'', 
			v_old_seq, v_exp_seq;
	end if;
	raise notice ''Slony-I: Process archive with counter % created %'', p_new_seq, p_created;

	update _<cluster>.sl_archive_tracking
		set at_counter = p_new_seq,
			at_created = p_created,
			at_applied = CURRENT_TIMESTAMP;
	return p_new_seq;
end;
' language plpgsql;
"""

CAPTURE_SUNUMS_SQL = """\
-- ---------------------------------------------------------------------------------------
-- TABLE drms.ingested_sunums
--
-- When a client ingests a Slony log, each SUNUM of each data series record is copied
-- into this table. The client can use this table of SUNUMs to prefetch SUs from the 
-- providing site. This SQL is ingested as the pg_user database user, the same
-- user that will be ingesting Slony logs and prefetching SUs, so permissions on 
-- this table will be correct without having to execute a GRANT statement.
--
-- The namespace drms is required, and is created by NetDRMS.sql during the 
-- NetDRMS installation process.
-- ---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS drms.ingested_sunums;
CREATE TABLE drms.ingested_sunums (sunum bigint PRIMARY KEY, starttime timestamp with time zone NOT NULL);
CREATE INDEX ingested_sunums_starttime ON drms.ingested_sunums(starttime);

-- TABLE drms.capturesunum_series
--
-- A table with one row per series and three columns: series, timecol, timewindow. Each row identifies the 
-- name of the column in a DRMS series table that will be used for the purpose of determining the 
-- observation time of the series. window is the time interval ending NOW() - SU downloads
-- will proceed only if the observation time lies within this interval. The rows are optional - 
-- not every series needs to identify its time column.
-- ---------------------------------------------------------------------------------------
DROP TABLE IF EXISTS drms.capturesunum_series;
CREATE TABLE drms.capturesunum_series (series text PRIMARY KEY, timecol text NOT NULL, timewindow interval NOT NULL);

-- ---------------------------------------------------------------------------------------
-- FUNCTION drms.capturesunum
--
-- For each table under replication, a trigger is created that calls this function, which
-- then copies SUNUMs into the underlying table, drms.ingested_sunums.
--
-- drms.ingested_sunums may not exist (older NetDRMSs did not receive the SQL to create
-- this table). If this table does not exist, then this function is a no-op.
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION drms.capturesunum() RETURNS TRIGGER AS
$capturesunumtrig$
DECLARE
  time_col          text;
  time_val          double precision;
  time_window       interval;
  day_interval      double precision;
  carr_epoch        timestamp;
  isRecent          boolean;
BEGIN
    IF EXISTS (SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drms' AND c.relname = 'ingested_sunums') THEN
      IF (TG_OP='INSERT' AND new.sunum > 0) THEN
        IF EXISTS (SELECT 1 FROM drms.ingested_sunums WHERE sunum = new.sunum) THEN
          RETURN NULL;
        END IF;

        SELECT timecol, timewindow INTO time_col, time_window FROM drms.capturesunum_series WHERE series = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
        
        IF FOUND THEN
          -- EXECUTE format('SELECT ($1).%s::text', time_col)
          EXECUTE 'SELECT cast(NEW.' || time_col || ' as double precision)'
          INTO time_val;
          -- time_val is the value of the time-column property in NEW
          
          IF time_col = 't_rec' OR time_col = 't_obs' THEN
            -- timestamp 'epoch' +  '220924785 seconds' is the SDO epoch in unix time
            -- convert interval from epoch to DRMS time from seconds to days
            -- THESE TIMES DO NOT ACCOUNT FOR LEAP SECONDS - we don't need that kind of resolution
            day_interval := time_val / 86400;
            SELECT age(now(), timestamp 'epoch' + '220924785 seconds' + interval '1 day' * day_interval) < time_window INTO isRecent;
          ELSIF time_col = 'carrrot' OR time_col = 'car_rot' THEN
            -- for Rick's series (time is a Carrington Rotation value)
            -- USING THE SYNODIC ROTATION PERIOD OF 27.2753 and Carrington epoch as November 9, 1853; again, 
            -- accuracy is not that important
            day_interval := time_val * 27.2753;
            SELECT age(now(), timestamp 'November 9, 1853' + interval '1 day' * day_interval) < time_window INTO isRecent;
          ELSE
            -- some future thing; default to FALSE for now so we don't accidentally download all SUs and swamp disk
            isRecent := FALSE;
          END IF;

          IF NOT isRecent THEN
            RETURN NULL;
          END IF;
        END IF;

        INSERT INTO drms.ingested_sunums (sunum, starttime) VALUES (new.sunum, clock_timestamp());
      END IF;
    END IF;  
  
  RETURN NEW;
END
$capturesunumtrig$ LANGUAGE plpgsql;

"""

# EXCEPTION SUB-CLASSES
class MSException(Exception):
    def __init__(self, msg):
        super(MSException, self).__init__(msg)
        self.msg = msg

class SendMsg(MSException):
    def __init__(self, msg):
        super(SendMsg, self).__init__(msg)

class ReceiveMsg(MSException):
    def __init__(self, msg):
        super(ReceiveMsg, self).__init__(msg)

class ExtractRequestException(MSException):
    def __init__(self, msg):
        super(ExtractRequestException, self).__init__(msg)

class RequestTypeException(MSException):
    def __init__(self, msg):
        super(RequestTypeException, self).__init__(msg)


def getNewSiteSQL(cluster):
    slony = SLONY_DB_OBJECTS.replace('<cluster>', cluster)
    capture = CAPTURE_SUNUMS_SQL
    return slony + capture
    
class InstanceRunning(Exception):
    pass

def terminator(*args):
    # Raise the SystemExit exception (which will be caught by the __exit__() method below).
    sys.exit(0)

class TerminationHandler(DrmsLock):
    def __new__(cls, thContainer):
        return super(TerminationHandler, cls).__new__(cls, thContainer[0], thContainer[2], False)

    def __init__(self, thContainer):
        self.container = thContainer
        self.msLockFile = thContainer[0]
        self.subLockFile = thContainer[1]
        self.pidStr = thContainer[2]
        self.log = thContainer[3]
        self.proc = thContainer[4]
        self.saveState = False

        super(TerminationHandler, self).__init__(self.msLockFile, self.pidStr, False)
        
    def __enter__(self):
        signal.signal(signal.SIGINT, terminator)
        signal.signal(signal.SIGTERM, terminator)
        signal.signal(signal.SIGHUP, terminator)
        
        # Returns self if lock was acquired.
        if not super(TerminationHandler, self).__enter__():
            # Unable to acquire lock.
            self.__exit__(None, None, None)
            raise InstanceRunning
            
        return self

    # Normally, __exit__ is called if an exception occurs inside the with block. And since SIGINT is converted
    # into a KeyboardInterrupt exception, it will be handled by __exit__(). However, SIGTERM will not - 
    # __exit__() will be bypassed if a SIGTERM signal is received. Use the signal handler installed in the
    # __enter__() call to handle SIGTERM.
    def __exit__(self, etype, value, traceback):
        if etype == SystemExit:
            self.log.writeInfo(['termination signal handler called'])
            self.container[5] = RV_TERMINATED
            self.saveState = True

        self.finalStuff()

        # Clean up subscription lock
        if os.path.exists(self.subLockFile):
            os.remove(self.subLockFile)

        # Remove subscription lock file by calling parent __exit__().
        super(TerminationHandler, self).__exit__(etype, value, traceback)
        
    def finalStuff(self):
        self.log.writeInfo(['halting worker threads'])
    
        # stop worker threads and wait for them to complete
        for worker in Worker.tList:
            worker.stop(self.saveState)
            self.log.writeDebug([ 'Waiting for worker (request ID ' + str(worker.requestID) + ') to halt.' ])
            worker.join() # Cannot interrupt threads at an unsafe point to do so. If there is a long-running thread,
                          # then we block here and we do not process new requests. If we need to shut down for reals, 
                          # then we have to kill -9 manage-subs.py.
            self.log.writeDebug([ 'Worker (request ID ' + str(worker.requestID) + ') halted.' ])
        
        # 
        if hasattr(self, 'dispatcher'):
            self.log.writeInfo([ 'halting dispatcher' ])
            self.dispatcher.stop()
            if self.dispatcher.isAlive():
                # can't hold worker lock here - when the worker terminates, it acquires the same lock
                self.log.writeInfo([ 'waiting for dispatcher to halt' ])
                self.dispatcher.join() # will block, possibly for ever
                self.log.writeInfo([ 'dispatcher successfully halted' ])

        # Do not do this! The call of Log::__del__ is deferred until after the program exits. If you end your program by
        # calling sys.exit(), then sys.exit() gets called BEFORE Log::__del__ is called, and this causes a race condition.
        # del self.log
        # self.log.close()
 
        # let's not close the log; just flush for now, and close() at the end of the program run
        self.log.flush()


class ManageSubsParams(DRMSParams):

    def __init__(self):
        super(ManageSubsParams, self).__init__()

    def get(self, name):
        val = super(ManageSubsParams, self).get(name)

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
            raise Exception('args', 'Attempt to set an argument that already exists: ' + name + '.')
            
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
            raise Exception('clientConfig', 'Unable to open or parse client-side configuration file ' + file + '.')
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
        if self.log:
            for line in text:
                self.log.debug(line)
            self.fileHandler.flush()
            
    def writeInfo(self, text):
        if self.log:
            for line in text:
                self.log.info(line)
        self.fileHandler.flush()
    
    def writeWarning(self, text):
        if self.log:
            for line in text:
                self.log.warning(line)
            self.fileHandler.flush()
    
    def writeError(self, text):
        if self.log:
            for line in text:
                self.log.error(line)
            self.fileHandler.flush()
            
    def writeCritical(self, text):
        if self.log:
            for line in text:
                self.log.critical(line)
            self.fileHandler.flush()
            
        
class IntervalLogger(object):
    """create one object per loop"""
    def __init__(self, log, logInterval):
        self.log = log
        self.logInterval = logInterval
        self.lastWriteTime = None

    def timeToLog(self):        
        if self.lastWriteTime is None:
            return True
        else:
            timeNow = datetime.now(timezone.utc)
            if timeNow > self.lastWriteTime + self.logInterval:
                return True
            else:
                return False
            
    def updateLastWriteTime(self):
        if self.timeToLog():
            # update last print time only if the interval between last print and now has elapsed
            self.lastWriteTime = datetime.now(timezone.utc)

    def writeDebug(self, text):
        if self.timeToLog():
            self.log.writeDebug(text)

    def writeInfo(self, text):
        if self.timeToLog():
            self.log.writeInfo(text)

    def writeWarning(self, text):
        if self.timeToLog():
            self.log.writeWarning(text)

    def writeError(self, text):
        if self.timeToLog():
            self.log.writeError(text)

    def writeCritical(self, text):
        if self.timeToLog():
            self.log.writeCritical(text)
        

# in child process
class DumperProcessTerminator(object):
    def __new__(cls, thContainer):
        return super(DumperProcessTerminator, cls).__new__(cls)    

    def __init__(self, logQueue):
        self.logQ = logQueue        
        self.savedSignals = None
        super(DumperProcessTerminator, self).__init__()
        
    def __enter__(self):    
        self.enableInterrupts()
        return self
        
    def __exit__(self, etype, value, traceback):
        if etype is not None:
            # If the context manager was exited without an exception, then etype is None
            import traceback
            self.logQ.put([ 'debug', traceback.format_exc(10) ])

    def saveSignal(self, signo, frame):
        if self.savedSignals == None:
            self.savedSignals = []

        self.savedSignals.append((signo, frame))
        self.logQ.put([ 'debug', 'saved signal ' +  str(signo) ])

    def disableInterrupts(self):
        signal.signal(signal.SIGINT, self.saveSignal)
        signal.signal(signal.SIGTERM, self.saveSignal)
        signal.signal(signal.SIGHUP, self.saveSignal)
        
    def enableInterrupts(self):
        signal.signal(signal.SIGINT, terminator)
        signal.signal(signal.SIGTERM, terminator)
        signal.signal(signal.SIGHUP, terminator)

        if type(self.savedSignals) is list:
            for signalReceived in self.savedSignals:
                # XXX - I don't think this is right
                terminator(*signalReceived)
        
        self.savedSignals = None


class DBTableDumperInfo(object):
    def __init__(self, worker):
        # cmd-line arguments
        self.args = {}
        self.args['subscribingDB'] = worker.arguments.getArg('SLAVEDBNAME')
        self.args['subscribingDBHost'] = worker.arguments.getArg('SLAVEHOSTNAME')
        self.args['subscribingDBPort'] = int(worker.arguments.getArg('SLAVEPORT'))
        self.args['subscribingDBUser'] = worker.arguments.getArg('REPUSER')
        
        # subscription information
        self.subscriptionInfo = {}
        self.subscriptionInfo['series'] = worker.series
        self.subscriptionInfo['columns'] = worker.columns
        self.subscriptionInfo['requestID'] = worker.request.requestid

        # client information
        self.clientInfo = {}
        self.clientInfo['client'] = worker.client
        self.clientInfo['newSite'] = worker.newSite
        
        # server information
        self.serverInfo = {}
        self.serverInfo['drmsRoot'] = worker.arguments.getArg('kJSOCRoot')
        self.serverInfo['replicationCluster'] = worker.replicationCluster
        self.serverInfo['subscriptionLock'] = worker.subscribeLock
        self.serverInfo['parserConfig'] = worker.parserConfig
        self.serverInfo['slonyConfig'] = worker.slonyCfg
        self.serverInfo['repScriptPath'] = worker.repScriptPath
        self.serverInfo['dumpDir'] = worker.dumpDir
        self.serverInfo['newClientLogDir'] = worker.newClientLogDir
        self.serverInfo['newLstPath'] = worker.newLstPath
        self.serverInfo['seriesDBTable'] = worker.request.dbtable
        
    # methods - cannot pass instance methods via pickle, must pass class methods only


class Worker(threading.Thread):
    """ This kinda does what the old subscription Updater code did. """

    tList = [] # A list of running worker threads.
    maxThreads = 16 # Default. Can be overriden with the Downloader.setMaxThreads() method.
    eventMaxThreads = threading.Event() # Event fired when the number of threads decreases.
    lock = threading.Lock() # Guard tList.

    def __init__(self, request, newSite, arguments, connMaster, connSlave, log, fclean, msProc):
        threading.Thread.__init__(self)
        self.request = request
        self.requestID = request.requestid
        self.newSite = newSite
        self.arguments = arguments
        self.connMaster = connMaster # Master db
        self.connSlave = connSlave # Slave db
        self.log = log
        self.proc = msProc
        self.sdEvent = threading.Event()
        
        self.reqTable = self.request.reqtable
        self.client = self.request.client
        self.series = self.request.series
        self.columns = None
        self.action = self.request.action
        self.archive = self.request.archive
        self.retention = self.request.retention
        self.tapegroup = self.request.tapegroup
        self.subuser = self.request.subuser
        
        self.siteLogDir = self.arguments.getArg('subscribers_dir')
        self.repScriptPath = self.arguments.getArg('kRepDir')
        self.slonyCfg = self.arguments.getArg('slonyCfg')
        self.clientLstDbTable = self.arguments.getArg('kLstTable')
        self.dumpDir = self.arguments.getArg('dumpDir')
        self.lstDir = self.arguments.getArg('tables_dir')
        self.parserConfig = self.arguments.getArg('parser_config')
        self.parserConfigTmp = self.parserConfig + '.tmp'
        self.subscribeLock = os.path.join(self.arguments.getArg('kServerLockDir'), 'subscribelock.txt')
        self.oldClientLogDir = os.path.join(self.siteLogDir, self.client)
        self.newClientLogDir = os.path.join(self.siteLogDir, self.client + '.new')
        self.oldLstPath = os.path.join(self.lstDir, self.client + '.lst')
        self.newLstPath = os.path.join(self.lstDir, self.client + '.new.lst')
        self.sqlgenArgFile = os.path.join(self.dumpDir, self.client + '-sqlgen.txt.tmp')
        self.saveState = False
        self.cleanTempOnStart = fclean
        self.replicationCluster = self.arguments.getArg('CLUSTERNAME')

        self.slonDaemonLock = os.path.join(self.arguments.getArg('kServerLockDir'), 'slon_daemon_lock.txt')
        self.masterSlonPIDFile = self.arguments.getArg('kMSMasterPIDFile')
        self.slaveSlonPIDFile = self.arguments.getArg('kMSSlavePIDFile')

    def run(self):
        # There is at most one thread per client.
        self.log.writeDebug([ 'Memory usage at Worker thread start (MB) for request ' + str(self.requestID) + ': ' + str(self.proc.memory_info().vms / 1048576) + '.' ])
        errorMsg = None
        try:
            self.log.writeInfo([ 'starting worker thread to handle request: (' + self.request.dump(False) + ')'])
            # XXX self.writeStream.logLines()
            
            if self.cleanTempOnStart:
                self.cleanTemp()

            # We don't want the main thread modifying this the request part-way through the following operations.
            try:
                gotLock = self.reqTable.acquireLock()
                
                if not gotLock:
                    raise Exception('lock', 'unable to acquire req-table lock')
                
                self.log.writeDebug([ 'worker thread acquired req table lock ' + hex(id(self.reqTable.lock)) ])
                reqStatus = self.request.getStatus()[0]
            finally:
                if gotLock:
                    self.reqTable.releaseLock()
                    gotLock = None
                    self.log.writeDebug([ 'Worker thread released req table lock ' + hex(id(self.reqTable.lock)) + '.'])

            # Check for download error or completion
            self.log.writeDebug([ 'Request ' + str(self.requestID) + ' status is ' + reqStatus + '.' ])
            if reqStatus.upper() == 'P':
                self.log.writeDebug([ 'Processing status P for request ' + str(self.requestID) + '.' ])
                try:
                    gotLock = self.reqTable.acquireLock()
                    
                    if not gotLock:
                        raise Exception('lock', 'Unable to acquire req-table lock.')
                    # Don't clean up old files - we could be resuming after a manual shutdown.

                    # Create client.new.lst so that it contains the series that the client will be subscribed to
                    # should the request succeed. This means that client.new.lst should contain all series in
                    # the existing client.lst, minus series being un-subscribed from (if action == unsubscribe), plus
                    # the series being subscribed to (if action == subscribe or resubscribe).
                    #
                    # It is important to note that client.new.lst is not reachable and therefore will not be
                    # used until after the dump file, which contains a dump of the series to which the client is 
                    # subscribing, is created and successfully ingested by the client. After the dump file is
                    # successfully ingested, then client.new.lst is put into slony_parser.cfg and su_production.slonycfg.

                    # Create the client.new.lst file and su_production.slonylst row.
                    # We must add the series to both the lst file and to su_production.slonylst so
                    # that if the parser is modified to use the su_production tables, it will see
                    # series for node.new.
                    subListObj = SubscriptionList(self.clientLstDbTable, self.connMaster, self.client)
                    if self.newSite:
                        subList = [ series.lower() for series in self.series ]
        
                        if self.action.lower() != 'subscribe':
                            raise Exception('invalidRequest', 'As a new client, ' + self.client + ' cannot make a ' + self.action.lower() + ' request.')
                    else:
                        # Read the client's subscription list from su_production.slonylst. There is no need 
                        # for a lock since only one thread can modify the subscription list for a client.
                        subList = subListObj.getSubscriptionList()
        
                        if self.action == 'subscribe' or self.action == 'resubscribe':
                            # append to subList; subscribe and resubscribe are identical, except that for resubscribe
                            # we omit all db-object-manipulation SQL
                            subList.extend([ series.lower() for series in self.series ])
                        elif self.action == 'unsubscribe':
                            # Remove from subList.
                            for series in self.series:                            
                                subList.remove(series.lower())
                        else:
                            # Error, unknown action.
                            raise Exception('invalidArgument', 'Invalid request action: ' + self.action + '.')
            
                    # If the request was successfully processed, then the cached subscription list must be updated.
                    if self.action == 'subscribe' or self.action == 'resubscribe' or self.action == 'unsubscribe':
                        # We must acquire the global subscribe lock before proceeding. Loop until we obtain the lock
                        # (or time-out after some number of failed attempts). The global subscribe lock needs to be available
                        # to shell scripts, as well as Perl and Python programs, and since shell does not support
                        # flock-type of lock commands, we have to simulate an atomic locking shell command:
                        #   (set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null
                
                        # Release reqTable lock to allow other client requests to be processed.
                        if gotLock:
                            self.reqTable.releaseLock()
                            gotLock = None
                
                        maxLoop = 30
                        while True:
                            if self.sdEvent.isSet():
                                raise Exception('shutDown', 'Received shut-down message from main thread.')
                            try:
                                try:
                                    cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                                    check_call(cmd, shell=True)
                                except CalledProcessError as exc:
                                    # Other exceptions will re-raise to the outer-most exception handler.
                                    raise Exception('subLock')
                        
                                # Obtained global subscription lock.

                                gotLock = self.reqTable.acquireLock()
                                
                                if not gotLock:
                                    raise Exception('lock', 'Unable to acquire req-table lock.')
                        
                                # Modify the client.lst file by creating a temporary file...
                                with open(self.newLstPath, 'w') as fout:
                                    # if we are unsubscribing from the only subscribed series, then len(subList) == 0, and this is OK;
                                    # sublist contains the list of all series to which the client will be subscribed should the 
                                    # subscription request succeed
                                    for aseries in subList:
                                        print(aseries.lower(), file=fout)
                
                                # ...and the su_production.slonylst rows for client.new.lst.
                                cmdList = [ os.path.join(self.repScriptPath, 'subscribe_manage', 'gentables.pl'), 'op=replace', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new', '--lst=' + self.newLstPath ]
                                # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                                check_call(cmdList)
                                #XXX - self.writeStream.logLines() # Log all gentables.pl output

                                if self.action == 'subscribe' or self.action == 'resubscribe':
                                    # Do not create a temp site-log dir, unless we are subscribing to a new series. During un-subscription,
                                    # client.new.lst is not added slon_parser.cfg, so no logs are ever written to self.newClientLogDir.
                                    # ...and make the temporary client.new directory in the site-logs directory.
                                    self.log.writeInfo([ 'Making directory ' +  self.newClientLogDir + '.' ])
                                    os.mkdir(self.newClientLogDir) # umask WILL mask-out bits if it is not 0000; os.chmod() is better.
                                    os.chmod(self.newClientLogDir, 0O2755) # This is the way you specify Octal in Py 3 (not Py 2).
                                break
                            except Exception as exc:
                                if len(exc.args) == 1 and exc.args[0] == 'subLock':
                                    # Could not obtain lock; try again (up to 30 tries)
                                    if maxLoop <= 0:
                                        raise Exception('subLock', 'Could not obtain subscription lock after 30 tries.')
                                    time.sleep(1)
                                    maxLoop -= 1
                                else:
                                    # Failure to run gentables.pl create client.new dir or chmod client.new, re-raise the exception generated.
                                    raise
                            finally:
                                if gotLock:
                                    self.reqTable.releaseLock()
                                    gotLock = None
                                if os.path.exists(self.subscribeLock):
                                    os.remove(self.subscribeLock)

                        gotLock = self.reqTable.acquireLock()
                        
                        if not gotLock:
                            raise Exception('lock', 'Unable to acquire req-table lock.')

                        # Refresh this script's cache FROM DB.
                        subListObj.refreshSubscriptionList()

                    if self.action == 'subscribe' or self.action == 'resubscribe':
                        # Time to create the DUMP. There are two dump files. One contains SQL commands 
                        # that, when ingested on the client site, will create the schema containing the 
                        # series being subscribed to. This dump file is generated only if the site's 
                        # database does not have the series' schema at the time of subscription. 
                        # The other dump file contains a set of SQL commands that depends on the state 
                        # of client subscribing to the series:
                        # COMMAND                                 APPLIES TO
                        # -------------------------------------------------------------------------------
                        # create Slony _jsoc schema               A client subscribing for the first time
                        # create Slony _jsoc.sl* tables           A client subscribing for the first time
                        # create Slony PG functions in _jsoc      A client subscribing for the first time
                        # populate _jsoc.sl_sequence_offline      All client subscriptions/resubscriptions
                        # populate _jsoc.sl_archive_tracking      All client subscriptions/resubscriptions
                        # createns                                All client subscriptions
                        # copy command containing series data     All client subscriptions/resubscriptions
        
                        # Now that the dump file exists and is complete, tell the client that it is ready for
                        # consumption. The client is polling by checking for a request status (via request-subs.py) 
                        # of 'D'.
        
                        # The dump script wrapper is sql_gen. sql_gen calls sdo_slony1_dump.sh, which calls dumpreptables.pl. 
                        # LOCKS: The last program calls parse_slon_logs.pl (which must obtain both the parse lock and 
                        # the subscription lock), and it also modifies subscription files/directories (so it obtains the 
                        # subscription lock). So, we do not need to acquire any locks before calling sql_gen (in fact, if we do that, 
                        # we would deadlock). It could take a little time to 

                        # . $kRepDir/subscribe_manage/sql_gen $node $new_site $archive $retention $tapegroup $input_file
                        # The last argument is file containing two tab-separated columns. The first column is the series, 
                        # and the second column is the subscribe command. This is all legacy stuff - I didn't want to modify
                        # sql_gen so I am using the existing, somewhat-messy interface (instead of passing arguments on the
                        # command line).            
                        if self.newSite:
                            newSiteStr = '1'
                        else:
                            newSiteStr = '0'
                    
                        regExp = re.compile(r'\s*(\S+)\.(\S+)\s*')
                        matchObj = regExp.match(self.series[0].lower())
                        if matchObj is not None:
                            ns = matchObj.group(1)
                            table = matchObj.group(2)
                        else:
                            raise Exception('invalidArgument', 'Not a valid DRMS series name: ' + self.series[0].lower() + '.')

                        # 86 sql_gen. Instead do the work directly in manage-subs.py.
                        # 1. We are going to dump a database table. Ensure the table exists in the slave database.                    
                        if not dbTableExists(connSlave, ns, table):
                            raise Exception('invalidArgument', 'DB table ' + self.series[0].lower() + ' does not exist.')
                
                        if self.action == 'subscribe':
                            # 2. Run createns for the schema of the series being subscribed to.
                            cmdList = [ os.path.join(self.arguments.getArg('kModDir'), 'createns'), 'JSOC_DBHOST=' + self.arguments.getArg('SLAVEHOSTNAME'), 'ns=' + ns, 'nsgroup=user', 'dbusr=' + self.subuser ]
                            if not os.path.exists(self.dumpDir):
                                os.mkdir(self.dumpDir)
                                os.chmod(self.dumpDir, 0O2755)
                            outFile = os.path.join(self.dumpDir, self.client + '.' + ns + '.createns.sql.gz')

                            wroteIntMsg = False
                            with gzip.open(outFile, mode='wt', compresslevel=9, encoding='UTF8') as fout:
                                self.log.writeInfo([ 'Creating createns.sql file:' + ' '.join(cmdList) + '.' ])
                                
                                # You can't use a GzipFile to receive the stdout from createns. Instead, make a 
                                # pair of pipes - createns writes to the write end of the pipe, and then this script
                                # reads from the read end, and then it writes to the GzipFile.
                                pipeReadEndFD, pipeWriteEndFD = os.pipe()
                                
                                # Make the read-pipe not block.
                                flag = fcntl.fcntl(pipeReadEndFD, fcntl.F_GETFL)
                                fcntl.fcntl(pipeReadEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)

                                pipeReadEnd = os.fdopen(pipeReadEndFD, 'r', encoding='UTF8')
                                pipeWriteEnd = os.fdopen(pipeWriteEndFD, 'w', encoding='UTF8')

                                print('BEGIN;', file=fout)
                                print("SET CLIENT_ENCODING TO 'UTF8';", file=fout)
                                fout.flush()
                                proc = Popen(cmdList, stdout=pipeWriteEnd)

                                try:
                                    if gotLock:
                                        self.reqTable.releaseLock()
                                        gotLock = None
            
                                    maxLoop = 60 # 1-minute timeout
                                    while True:
                                        if maxLoop <= 0:
                                            raise Exception('sqlDump', 'Time-out waiting for dump file to be generated.')
            
                                        if not wroteIntMsg and self.sdEvent.isSet():
                                            self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                                            wroteIntMsg = True
                                            # proc.kill()
                                            # raise Exception('shutDown', 'Received shut-down message from main thread.')

                                        # Read what is available from the pipe, convert to utf8, and write it to fout.
                                        while True:
                                            pipeBytes = pipeReadEnd.read(4096)
                                            if len(pipeBytes) > 0:
                                                print(pipeBytes, file=fout, end='')
                                            else:
                                                break
            
                                        if proc.poll() is not None:
                                            #XXX-self.writeStream.logLines() # log all createns output
                                            if proc.returncode != 0:
                                                raise Exception('sqlDump', 'Failure generating dump file, createns returned ' + str(proc.returncode) + '.') 
                                            
                                            pipeWriteEnd.flush()
                                            pipeWriteEnd.close()
                                            
                                            # Read the remaining bytes from the pipe.
                                            while True:
                                                pipeBytes = pipeReadEnd.read(4096)
                                                if len(pipeBytes) > 0:
                                                    print(pipeBytes, file=fout, end='')
                                                else:
                                                    break
                                            pipeReadEnd.close()
                                            
                                            break

                                        maxLoop -= 1
                                        time.sleep(1)
                                    print('COMMIT;', file=fout)
                                    fout.flush()
                                finally:
                                    gotLock = self.reqTable.acquireLock()
                                    if not gotLock:
                                        raise Exception('lock', 'Unable to acquire req-table lock.')

                        # 3. PORT sdo_slony1_dump.sh into this script so we can run multiple COPY TO commands within a single
                        # database transaction. First, we need some database information, then we pass that information
                        # to the PORT (encapsulated in dumpAndStartGeneratingClientLogs()).

                        # Must get the Slony table IDs so we can call _jsoc.copyfields(). This copies the replicated table's 
                        # column names into a string column. This is then used when dumping the replicated table.                        
                        try:            
                            with self.connSlave.cursor() as cursor:
                                # Get the Slony node ID for the current replication cluster. The return value is an integer.
                                cmd = 'SELECT _' + self.replicationCluster + ".getLocalNodeId('_" + self.replicationCluster + "')"
                                cursor.execute(cmd)
                                records = cursor.fetchall()
                                if len(records) != 1:
                                    raise Exception('dbResponse', cmd + ' did not return a valid node ID.')
                                nodeID = records[0][0]
                                
                                self.log.writeInfo([ 'Node ID for replicaton cluster ' + self.replicationCluster + ' is ' + str(nodeID) + '.' ])
                                
                                # Get the table ID of the table being replicated in this cluster. The return value is an integer.
                                cmd = 'SELECT tab_id FROM _' + self.replicationCluster + '.sl_table, _' + self.replicationCluster + ".sl_set WHERE tab_set = set_id AND tab_nspname = '" + ns + "' AND tab_relname = '" + table + "' AND exists (SELECT 1 FROM _" + self.replicationCluster +'.sl_subscribe WHERE sub_set = set_id AND sub_receiver = ' + str(nodeID) + ')'
                                cursor.execute(cmd)
                                records = cursor.fetchall()
                                if len(records) != 1:
                                    raise Exception('dbResponse', cmd + ' did not return a valid table ID.')
                                tabID = records[0][0]
                                
                                self.log.writeInfo([ 'Replication table ID for series ' + self.series[0].lower() + ' is ' + str(tabID) + '.' ])
                                
                                # The comma-separated list of columns of the table being replicated. The return value is a text string.
                                cmd = 'SELECT _' + self.replicationCluster + '.copyfields(' +  str(tabID) + ')'
                                cursor.execute(cmd)
                                records = cursor.fetchall()
                                if len(records) != 1:
                                    raise Exception('dbResponse', cmd + ' did not return a valid column list.')
                                columnList = records[0][0].strip('()') # strip start and end parenthesis.
                                self.log.writeInfo([ 'Column list for series table ' + self.series[0].lower() + ' is ' + columnList + '.' ])
                                
                                # Convert to a format needed by dumpAndStartGeneratingClientLogs().
                                self.columns = [ col.strip() for col in columnList.split(',') ]
                                
                                # We need to terminate transaction. dumpAndStartGeneratingClientLogs() has to run various things in a specific order
                                # and has to time the start of a new transaction properly.
                        except psycopg2.Error as exc:
                            raise Exception('dbCmd', exc.diag.message_primary)
                                
                        finally:
                            self.connSlave.rollback()
                            
                        # The next part is very tricky and critical. We have to make sure that there is no overlap of series records in the series-table dump
                        # and the Slony site-specific logs generated after the dump has completed. At the same time, we have to make sure that all series records
                        # inserted into the slave database after the dump has completed appear in the site-specific logs generated after the dump has completed.
                        
                        # The request-table lock is held here. Some of the tasks in dumpAndStartGeneratingClientLogs() can be slow: stopping the slony daemons,
                        # parsing the slony logs, generating the series dump files. Release the lock, then acquire it again after.
                        try:
                            if gotLock:
                                self.reqTable.releaseLock()
                                gotLock = None
                            self.log.writeDebug([ 'memory usage (MB) before initial dump ' + str(self.proc.memory_info().vms / 1048576) ])
                            
                            # this will create a new process to handle the dump; the only shared state that the new process 
                            # modifies is the status of the the single request row in the requests DB table; at the same 
                            # time, the main thread will read and possibly write the same status; this worker thread will NOT 
                            # be modifying any shared state; so we need to synchronize the new process and the main thread
                            # when they access the request DB table
                            self.dumpAndStartGeneratingClientLogs()
                            # END of new process
                        finally:
                            gotLock = self.reqTable.acquireLock()
                            if not gotLock:
                                raise Exception('lock', 'Unable to acquire req-table lock.')

                        # If there are no errors, the request status will be 'I'.
                        reqStatus = 'I'
                
                        time.sleep(1)
                finally:
                    if gotLock:
                        self.reqTable.releaseLock()
                        gotLock = None
            
            # No lock held here.
            if reqStatus == 'I' or (reqStatus == 'P' and self.action == 'unsubscribe'):
                # In the subscribe/resubscribe-action case, the script sdo_slony1_dump.sh puts an entry for client.new / client.new.lst into
                # the slon_parser.cfg and su_production.slonycfg files. But for the unsubscribe-action case, we do not do that.
                # Instead, simply make client.new.lst and the client.new.lst row in su_production.slonycfg have the post-unsubscribe
                # list of tables, and then in clean-up, we COPY this list over client.lst and the client.lst row in su_production.slonycfg.
                # At no point we do populate site_logs/client.new with all series minus the ones being unsubscribed from. Instead,
                # if there are no errors, we instantaneously update the client.new.lst file and the client's row in su_production.slonycfg.
                self.log.writeDebug([ 'Cleaning up request ' + str(self.requestID) + '.' ])
                # Clean-up, regardless of action.
                # reqTable lock is not held here.
                wroteIntMsg = False
                maxLoop = 30
                while True:
                    if not wroteIntMsg and self.sdEvent.isSet():
                        self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                        wroteIntMsg = True

                    try:
                        try:
                            cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                            check_call(cmd, shell=True)
                        except CalledProcessError as exc:
                            raise Exception('subLock')
                    
                        # Obtained global subscription lock.
                    
                        gotLock = self.reqTable.acquireLock()
                        if not gotLock:
                            raise Exception('lock', 'Unable to acquire req-table lock.')
                
                        # LEGACY - Remove client.new.lst from slon_parser.cfg
                        with open(self.parserConfigTmp, 'w') as fout, open(self.parserConfig, 'r') as fin:
                            regExp = re.compile(re.escape(self.client + '.new.lst'))

                            for line in fin:
                                matchObj = regExp.match(line)
                                if matchObj is None:
                                    print(line, file=fout)
                
                        os.rename(self.parserConfigTmp, self.parserConfig)

                        # Remove client.new from su_production.slonylst and su_production.slonycfg. Do not call legacy code in
                        # SubTableMgr::Remove. This would delete $node.new.lst, but it is still being used.
                        # cmd="$kRepDir/subscribe_manage/gentables.pl op=remove config=$config_file --node=$node.new"
                        cmdList = [ os.path.join(self.repScriptPath, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new' ]
                        # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                        check_call(cmdList)
                        #XXX-self.writeStream.logLines() # Log all gentables.pl output
                
                        if self.newSite:
                            # If we are unsubscribing or resubscribing, we cannot be a new site.
                            if self.action == 'unsubscribe':
                                raise Exception('invalidArgument', self.client + ' is a new subscriber. Cannot cancel a series subscription.')
                            if self.action == 'resubscribe':
                                raise Exception('invalidArgument', self.client + ' is a new subscriber. Cannot reset a series subscription.')
                        
                            # LEGACY - Add a line for client to slon_parser.cfg.
                            with open(self.parserConfig, 'a') as fout:
                                print(self.oldClientLogDir + '        ' + self.oldLstPath, file=fout)
                        
                            # Add client to su_production.slonycfg. At the same time, insert records for client in
                            # su_production.slonylst. The client's sitedir doesn't exit yet, but it will shortly.
                            # Use client.new.lst to populate su_production.slonylst.
                            # $kRepDir/subscribe_manage/gentables.pl op=add config=$config_file --node=$node --sitedir=$subscribers_dir --lst=$tables_dir/$node.new.lst
                            cmdList = [ os.path.join(self.repScriptPath, 'subscribe_manage', 'gentables.pl'), 'op=add', 'conf=' + self.slonyCfg, '--node=' + self.client, '--sitedir=' + self.oldClientLogDir, '--lst=' + self.newLstPath ]
                            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                            check_call(cmdList)
                            #XXX-self.writeStream.logLines() # Log all gentables.pl output

                            # Rename the client.new site dir.
                            if os.path.exists(self.oldClientLogDir):
                                shutil.rmtree(self.oldClientLogDir)
                    
                            # We are assuming that perms on newClientLogDir were created correctly and that we
                            # do not want to change them at this point.
                            os.rename(self.newClientLogDir, self.oldClientLogDir)
                        else:
                            # Update su_production.slonylst for the client with the new list of series. 
                            cmdList = [ os.path.join(self.repScriptPath, 'subscribe_manage', 'gentables.pl'), 'op=replace', 'conf=' + self.slonyCfg, '--node=' + self.client, '--lst=' + self.newLstPath ]
                            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                            check_call(cmdList)
                            #XXX-self.writeStream.logLines() # log all gentables.pl output
                    
                            # Copy all the log files in newClientLogDir to oldClientLogDir, overwriting logs of the same name.
                            # There is a period of time where we allow the log parser to run while the client is ingesting
                            # the dump file. During that time, two parallel universes exist - in one universe, the set of 
                            # logs produced is identical to the one that would have been produced had the client not 
                            # taken any subscription/resubscription action. In the other universe, the set of logs produced is 
                            # what you'd expect had the subscription/resubscription action succeeded. So, if we have a successful 
                            # subscription/resubscription, then we need to discard the logs generated in the first universe, 
                            # in the oldClientLogDir, overwriting them with the analogous logs in the newClientLogDir.
                            #
                            # self.newClientLogDir is not used for un-subscription, nor is it used for re-subscription.
                            if self.action == 'subscribe' or self.action == 'resubscribe':
                                for logFile in os.listdir(self.newClientLogDir):
                                    src = os.path.join(self.newClientLogDir, logFile)
                                    dst = os.path.join(self.oldClientLogDir, logFile)
                                    # copy() copies permissing bits as well, and the destination is overwritten if it exists.
                                    shutil.copy(src, dst)

                                # Remove newClientLogDir. I think this is better than deletion during iteration in the
                                # previous loop. If an error happens part way through in the above loop, then we could
                                # get in a weird, indeterminate state.
                                shutil.rmtree(self.newClientLogDir)

                        # LEGACY - Rename the client.new.lst file. If oldLstPath happens to exist, then rename() will overwrite it.
                        os.rename(self.newLstPath, self.oldLstPath)
                
                        break # out of lock-acquisition loop
                    except Exception as exc:
                        if len(exc.args) == 1 and exc.args[0] == 'subLock':
                            # Could not obtain lock; try again (up to maxLoop tries)
                            if maxLoop <= 0:
                                raise Exception('subLock', 'Could not obtain subscription lock after ' + str(maxLoop) + ' tries.')
                            time.sleep(1)
                            maxLoop -= 1
                        else:
                            # gentables.pl failed or creating client.new, re-raise the exception generated by check_call()
                            # or os.mkdir() or os.chmod().
                            raise
                    finally:
                        if gotLock:
                            self.reqTable.releaseLock()
                            gotLock = None
                        if os.path.exists(self.subscribeLock):
                            os.remove(self.subscribeLock)
                        
            # Outside of lock-acquisition loop.
            # reqTable lock is NOT held.
            # The request has been completely processed without error. Set status to 'C'.
            try:
                gotLock = self.reqTable.acquireLock()
                if not gotLock:
                    raise Exception('lock', 'Unable to acquire req-table lock.')

                self.request.setStatus('C')
            finally:
                if gotLock:
                    self.reqTable.releaseLock()
                    gotLock = None
        except ValueError as exc:
            # Popen() and check_call() will raise this exception if arguments are bad.
            errorMsg = 'Command called with invalid arguments: ' + ' '.join(cmdList) + '.'
            self.log.writeError([ errorMsg ])
        except FileNotFoundError as exc:
            import traceback
            
            errorMsg = 'File not found.'
            self.log.writeError([ errorMsg ])
            self.log.writeError([ traceback.format_exc(5) ])
        except IOError as exc:
            import traceback
            
            errorMsg = 'IO error.'
            self.log.writeError([ traceback.format_exc(10) ])
        except OSError as exc:
            # Popen() and check_call() can raise this.
            errorMsg = exc.diag.message_primary
            self.log.writeError([ errorMsg ])
        except CalledProcessError as exc:
            # check_call() can raise this.
            errorMsg = 'Command returned non-zero status code ' + str(exc.returncode) + ': '+ ' '.join(cmdList) + '.'
            self.log.writeError([ errorMsg ])
        except Exception as exc:
            if len(exc.args) == 2:
                eType, eMsg = exc.args
                
                if eType == 'shutDown':
                    msg = 'worker (request ID ' + str(self.requestID) + ') received shutdown signal'
                    self.saveState = True # Do not clean up temp files; we shut down, perhaps in the middle of a request that was being processed.
                                          # Resume on restart.
                else:
                    msg = 'problem in worker thread (request ID ' + str(self.requestID) + '): ' + eMsg + ' ('+ eType + ')'
                    errorMsg = msg

                if self.log:
                    self.log.writeError([ msg ])
            else:
                import traceback
                
                errorMsg = 'unknown error in worker thread (request ID ' + str(self.requestID) + '); thread terminating'
                self.log.writeError([ errorMsg ])
                self.log.writeError([ traceback.format_exc(10) ])
        finally:
            self.reqTable.acquireLock()
            try:
                self.log.writeInfo([ 'Worker thread (request ID ' + str(self.requestID) + ') terminating.' ])
                
                if errorMsg:
                    # Set request status to error.
                    self.log.writeDebug([ 'Setting status for request ' + str(self.requestID) + ' to E (' + errorMsg + ').' ])
                    self.request.setStatus('E', errorMsg)
                    time.sleep(1)
                
                # Detach this worker from the request that it is doing work for.
                self.request.removeWorker()
            finally:
                # always release reqTable lock
                self.reqTable.releaseLock()

            maxLoop = 30
            while True:
                try:
                    try:
                        self.log.writeDebug([ 'end of worker thread - acquiring subscription lock' ])
                        cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                        check_call(cmd, shell=True)
                    except CalledProcessError as exc:
                        raise Exception('subLock')
                        
                    # Obtained global subscription lock.
                    self.log.writeDebug([ 'end of worker thread - cleaning files' ])
                    if errorMsg:
                        self.log.writeDebug([ 'end of worker thread - errored out, calling cleanOnError()' ])
                        # There was some kind of error. We need to clean up several temporary things, and we need to remove the client
                        # entry from the configuration table IFF the client was a new subscriber.
                        self.cleanOnError()
                    elif not self.saveState:
                        self.log.writeDebug([ 'end of worker thread - not saving state, cleaning temp files by calling cleanTemp()' ])
                        self.cleanTemp()
                    break
                except Exception as exc:
                    if len(exc.args) == 1 and exc.args[0] == 'subLock':
                        # Could not obtain lock; try again (up to maxLoop tries)
                        if maxLoop <= 0:
                            raise Exception('subLock', 'Could not obtain subscription lock after ' + str(maxLoop) + ' tries.')
                        time.sleep(1)
                        maxLoop -= 1
                    else:
                        # gentables.pl failed or creating client.new, re-raise the exception generated by check_call()
                        # or os.mkdir() or os.chmod().
                        raise
                finally:
                    if os.path.exists(self.subscribeLock):
                        os.remove(self.subscribeLock)

            self.log.writeDebug([ 'Memory usage at Worker thread termination (MB) for request ' + str(self.requestID) + ': ' + str(self.proc.memory_info().vms / 1048576) + '.' ])
   
            # This thread is about to terminate. 
            # We need to check the class tList variable to update it, so we need to acquire the lock.
            try:
                Worker.lock.acquire()
                Worker.tList.remove(self) # This thread is no longer one of the running threads.
                if len(Worker.tList) == Worker.maxThreads - 1:
                    # Fire event so that main thread can process additional requests.
                    Worker.eventMaxThreads.set()
                    # Clear event so that main will block the next time it calls wait.
                    Worker.eventMaxThreads.clear()
            finally:
                Worker.lock.release()

    def stop(self, saveState=False):
        self.log.writeInfo([ 'stopping worker (request ID ' + str(self.requestID) + ')'])
        self.saveState = saveState
        self.sdEvent.set()
            
    def cleanTemp(self):
        try:
            # LEGACY - Remove the client.lst.new file entry from slon_parser.cfg.
            (head, tail) = os.path.split(self.parserConfig)
            if not os.path.exists(head):
                os.mkdir(head)
            if not os.path.exists(self.parserConfig):
                open(self.parserConfig, 'w').close()
            if os.path.exists(self.parserConfigTmp):
                os.remove(self.parserConfigTmp)

            self.log.writeDebug([ 'removing ' + self.client + '.new.lst from ' + self.parserConfig ])
            with open(self.parserConfigTmp, 'w') as fout, open(self.parserConfig, 'r') as fin:
                regExp = re.compile(r'' + self.client + '\.new\.lst')
                regExpSp = re.compile(r'\s*$')

                for line in fin:
                    line = line.rstrip()
                    matchObj = regExpSp.match(line)
                    if matchObj is not None:
                        continue
                
                    matchObj = regExp.search(line)
                    if matchObj is None:
                        self.log.writeDebug([ 'Added ' + line + ' to ' + self.parserConfigTmp + '.'])
                        print(line, file=fout)

            os.rename(self.parserConfigTmp, self.parserConfig)
            self.log.writeDebug([ 'renamed ' + self.parserConfigTmp + ' to ' + self.parserConfig + '.' ])

            # LEGACY - Delete the client.lst.new file.
            if os.path.exists(self.newLstPath):
                os.remove(self.newLstPath)
                self.log.writeDebug([ 'removed ' + self.newLstPath ])
            
            # Remove client.new from su_production.slonylst and su_production.slonycfg. 
            cmdList = [ os.path.join(self.repScriptPath, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client + '.new' ]
            self.log.writeDebug( [ 'calling check_call(): ' + ' '.join(cmdList) ])
            # Raises CalledProcessError on error (non-zero returned by gentables.pl).
            check_call(cmdList)
            #XXX-self.writeStream.logLines() # Log all gentables.pl output
            self.log.writeDebug( [ 'success calling check_call()' ])
        
            # Remove client.new site-log directory.
            if os.path.exists(self.newClientLogDir):
                self.log.writeInfo([ 'removing temporary site-log directory ' + self.newClientLogDir ])
                shutil.rmtree(self.newClientLogDir)
        
            # LEGACY - Remove slon_parser.cfg.tmp from tables dir.
            if os.path.exists(self.parserConfigTmp):
                os.remove(self.parserConfigTmp)

            # Remove client-sqlgen.txt.tmp temporary argument file from triggers directory.
            if os.path.exists(self.sqlgenArgFile):
                os.remove(self.sqlgenArgFile)
        
            # LEGACY - Other crap from the initial implementation. The newer implementation does not
            # use files for passing information back and forth to/from the client.
        
            # Remove client's dump files.
            createNSFiles = glob.glob(os.path.join(self.dumpDir, self.client + '.*.createns.sql.gz'))
            for afile in createNSFiles:
                os.remove(afile)
            
            dumpFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.sql.gz')
            if os.path.exists(dumpFile):
                os.remove(dumpFile)

        except OSError as exc:
            raise Exception('fileIO', exc.diag.message_primary)
        except CalledProcessError as exc:
            raise Exception('cleanTemp', 'Command returned non-zero status code ' + str(exc.returncode) + ': '+ ' '.join(cmdList) + '.')

    def cleanOnError(self):
        # Stuff to remove if an error happens anywhere during the subscription process. Remove site from
        # set of subscribers completely, if the site was a new subscriber.
        try:
            # If dump files exist, rename them for debugging purposes.
            createNSFiles = glob.glob(os.path.join(self.dumpDir, self.client + '.*.createns.sql.gz'))
            dumpFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.sql.gz')

            for afile in createNSFiles:
                savedFile = afile + '.' + datetime.now().strftime('%Y-%m-%d-%H%M%S')
                self.log.writeDebug([ 'Saving dump file ' + afile + ' to ' + savedFile + '.' ])
                os.rename(afile, savedFile)
            if os.path.exists(dumpFile):
                savedFile = dumpFile + '.' + datetime.now().strftime('%Y-%m-%d-%H%M%S')
                self.log.writeDebug([ 'Saving dump file ' + dumpFile + ' to ' + savedFile + '.' ])
                os.rename(dumpFile, savedFile)
        
            self.cleanTemp()

            # LEGACY - If this was a newsite subscription, then we need to remove the client entry
            # from slon_parser.cfg and remove client.lst from the table dir.
            if self.newSite:
                self.log.writeDebug([ 'Removing ' + self.client + '.lst from ' + self.parserConfig + '.' ])
                with open(self.parserConfigTmp, 'w') as fout, open(self.parserConfig, 'r') as fin:
                    regExp = re.compile(r'' + self.client + '\.lst')
                    regExpSp = re.compile(r'\s*$')

                    for line in fin:
                        line = line.rstrip()

                        matchObj = regExpSp.match(line)
                        if matchObj is not None:
                            continue
                        
                        matchObj = regExp.search(line)
                        if matchObj is None:
                            self.log.writeDebug([ 'Added ' + line + ' to ' + self.parserConfigTmp + '.'])
                            print(line, file=fout)                            

                os.rename(self.parserConfigTmp, self.parserConfig)
                
                if os.path.exists(self.oldLstPath):
                    os.remove(self.oldLstPath)
                
                if os.path.exists(self.newLstPath):
                    os.remove(self.newLstPath)
    
            # If this was a newsite subscription, then we need to remove the client entry
            # from su_production.slonycfg and remove the client's rows from su_production.slonylst.
            if self.newSite:            
                cmdList = [ os.path.join(self.repScriptPath, 'subscribe_manage', 'gentables.pl'), 'op=remove', 'conf=' + self.slonyCfg, '--node=' + self.client ]
                # Raises CalledProcessError on error (non-zero returned by gentables.pl).
                check_call(cmdList)
                #XXX-self.writeStream.logLines() # Log all gentables.pl output
        except CalledProcessError as exc:
            self.log.writeError([ 'Error running gentables.pl, status ' +  str(exc.returncode) + '.'])

    # runs in a Worker thread
    # all changes to request statuses are made within a single transaction, regardless of the thread
    # that the change is initiated from
    # MUST BE HOLDING REQUEST TABLE LOCK
    def checkClientStatus(self, fileNoIngested, nextFileIsReadyForDownload):
        code, msg = self.request.getStatus()
        logMsg = None

        if code.upper() == 'E':
            raise Exception('clientSignalledError', self.client + ' cancelling request')
        elif code.upper() == 'D':
            if fileNoIngested is None:
                logMsg = 'waiting for client ' + self.client + ' to download DDL dump file (request ' + str(self.requestID) + '); status ' + code.upper()
            else:
                logMsg = 'waiting for client ' + self.client + ' to download DML dump file ' + str(fileNoIngested + 1) + ' (request ' + str(self.requestID) + '); status ' + code.upper()
        elif code.upper() == 'A':
            if fileNoIngested is None:
                logMsg = 'waiting for client ' + self.client + ' to ingest DDL dump file (request ' + str(self.requestID) + '); status ' + code.upper()
            else:
                logMsg = 'waiting for client ' + self.client + ' to ingest DML dump file ' + str(fileNoIngested + 1) + ' (request ' + str(self.requestID) + '); status ' + code.upper()
        elif code.upper() == 'I':
            if fileNoIngested is None:
                logMsg = 'client ' + self.client + ' has signaled that they have successfully ingested DDL dump file (request ' + str(self.requestID) + '); status ' + code.upper()
                if nextFileIsReadyForDownload:
                    fileNoIngested = 0
            else:
                logMsg = 'client ' + self.client + ' has signaled that they have successfully ingested DML dump file number ' + str(fileNoIngested + 1) + ' (request ' + str(self.requestID) + '); status ' + code.upper()
                if nextFileIsReadyForDownload:
                    fileNoIngested += 1

            if nextFileIsReadyForDownload:
                outFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.sql.gz')
                if os.path.exists(outFile):
                    # clean up current dump file
                    os.remove(outFile)
            
                nextFileIsReadyForDownload = False
        else:
            raise Exception('invalidReqStatus', 'unexpected request status ' + code.upper())
                
        return (fileNoIngested, nextFileIsReadyForDownload, code.upper(), logMsg)
    
    # in worker thread
    def initializeSubscription(self):
        try:
            dumperQueue = Queue() # to hold the DBTableDumperInfo object
            exceptionQueue = Queue() # to hold a single exception if generated by child
            logQueue = Queue()
        
            # create an object suitable for passing via pickle to child process; do not pass self, a Worker object, because it
            # contains attributes, like threading.Lock, that cannot be serialized via pickle
            dumperQueue.put(DBTableDumperInfo(self))
        
            self.log.writeDebug([ 'memory usage (MB) before initializing subscription ' + str(self.proc.memory_info().vms / 1048576) ])
            pipeRead, pipeWrite = os.pipe()
            proc = Process(target=Worker.__initializeSubscription, args=(dumperQueue, exceptionQueue, logQueue, pipeRead, pipeWrite))

            fileNo = 0 # 0-based number of file generated at server
            fileNoIngested = None # the last file ingested at the client (None means no file has been ingested)
            nextFileIsReadyForDownload = True # set to true when we have the next file created and ready for client download
            limit = 3221225472 # 3 GB of compressed data
            makeNewFile = True
            fileDone = False
            tableDone = False
            fout = None
            offset = 0
            clientHappy = False
            maxLoop = 172800 # 48-hour timeout
            loggingDone = False
            fin = None
            chunkGen = None
            ddlFile = None
            ddlFileCreated = False
            dumpLoopIter = 0
            doClientCheck = True

            proc.start()
            self.log.writeInfo([ 'successfully forked child process' ])
            os.close(pipeWrite) # the parent process will be reading only            

            # log all messages generated by the child
            dataBuf = bytearray(b'')
            while True:
                if dumpLoopIter % 8192 == 0:
                    doClientCheck = True
            
                # check for shutdown each iteration
                if self.sdEvent.isSet():
                    # stop child process
                    proc.terminate() # send the child a SIGTERM; parent must not use the queues after this call; child must 
                                     # release lock, if held, before exiting
                    raise Exception('shutDown', 'received shut-down message from main thread')

                if not ddlFileCreated:
                    # move the DDL file into place and set status
                    if not ddlFile:
                        ddlFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.ddl.sql.gz')
                    
                    if os.path.exists(ddlFile):
                        self.reqTable.acquireLock()
                        try:
                            destFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.sql.gz')
                            shutil.move(ddlFile, destFile)
                            logQueue.put([ 'debug', 'successfully moved DDL dump file to ' + destFile ])

                            # tell client that the DDL file is ready for download
                            self.request.setStatus('D')
                            
                            fin = os.fdopen(pipeRead, 'rb')
                            chunkGen = Worker.__readDumpChunk(file=fin, chunksize=16384) # 16 KB
                            
                            ddlFileCreated = True
                        finally:
                            self.reqTable.releaseLock()
                    else:
                        # waiting for child to finish creating the DDL file
                        time.sleep(0.5)

                if fin is not None:
                    try:
                        # each iteration, read a chunk of data from the dump pipe that the child is writing to
                        # fin is a binary file that contains utf8 bytes                        
                        try:
                            # this is an array containing [ <total number of bytes read>, <bytearray 1 of data>, [ <bytearray 2 of data> ] ] 
                            dataChunk = next(chunkGen)
                        except StopIteration:
                            logQueue.put([ 'debug', 'read last line of COPY TO dump file' ])
                            tableDone = True
                            fileDone = True

                        if not makeNewFile and fileDone:
                            # send an EOF to the COPY command
                            dataBuf.extend(b'\.\n' + b'COMMIT;\n')
                            fout.write(bytes(dataBuf))
                            dataBuf = bytearray(b'')
                            fout.flush()
                            fout.close()
                            fout = None
                            logQueue.put([ 'info', 'successfully completed dump file ' + str(fileNo) + ' (' + outFile + ')' ])

                        if tableDone:
                            raise Exception('dumpDone')
                        elif fileDone:
                            makeNewFile = True

                        # table rows (DML)
                        if makeNewFile:
                            makeNewFile = False
                            fileDone = False
                            fileNo += 1
        
                            logQueue.put([ 'info', 'generating DML dump file ' + str(fileNo) ])
                            offset = 0 # byte offset into the dumpFile
                            outFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.dml' + str(fileNo).zfill(3) + '.sql.gz')
    
                            # although we are opening a binary file, the output from TableDumper is text, where UTF8 chars are encoded 
                            # as octal strings; so the file is all ASCII and we could have opened a text file
                            fout = gzip.open(outFile, mode='wb', compresslevel=5)
                            
                            dataBuf.extend(b'BEGIN;\n' + b"SET CLIENT_ENCODING TO 'UTF8';\n")
                            # encode() will convert a unicode string to a UTF8 byte array
                            dataBuf.extend(b'COPY ' + self.series[0].lower().encode() + ' ('.encode() + ','.join(self.columns).encode() + ') FROM STDIN;\n'.encode())
        
                            logQueue.put([ 'debug', 'created new dump file ' +  outFile ])

                        # these data are all UTF8 bytes; we are writing to a compressed file so write a lot of data at once
                        dataBuf.extend(dataChunk[1])
                        dataBuf.extend(dataChunk[2])
                        offset += dataChunk[0]
                        
                        dataChunk[1] = None
                        dataChunk[2] = None
                        dataChunk = None
                        # if offset >= limit, then we start a new dump file
                        fileDone = offset >= limit
                        if fileDone:
                            doClientCheck = True

                        if doClientCheck:
                            doClientCheck = False

                            # dumping an entire table could take a while; check for client-side error every N number of loop iterations
                            self.reqTable.acquireLock()
                            try:
                                logQueue.put([ 'debug', 'checking client DL/ingestion status' ])
                                (fileNoIngested, nextFileIsReadyForDownload, status, logMsg) = self.checkClientStatus(fileNoIngested, nextFileIsReadyForDownload)
                                logQueue.put([ 'debug', logMsg ])
                                if status == 'I':
                                    logQueue.put([ 'debug', 'client is ready for next DML file' ])
                                    if fileNo - 1 > fileNoIngested:
                                        logQueue.put([ 'debug', 'and the next DML file is ready for download' ])
                                        # fileNo is the file being currently generated; fileNo - 1 is the last one generated;
                                        # tell client another file is ready for download
        
                                        # rename file to name client looks for
                                        outFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.dml' + str(fileNoIngested + 1).zfill(3) + '.sql.gz')
                                        destFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.sql.gz')
                                        shutil.move(outFile, destFile)
                                        logQueue.put([ 'debug', 'successfully moved DML to ' + destFile ])
        
                                        # Set request status to 'D' to indicate, to client, that a dump is ready for
                                        # download and ingestion. The client could have sent either a polldump request (for the first
                                        # dump file) or a pollcomplete request (for all subsequent dump files). Both of those requests
                                        # should handle a 'D' status. If the client makes a pollcomplete request, and the client
                                        # sees a 'D' status, then it should go back to the code where it sets the status to 'A' and
                                        # ingests the dump file, then sets status to 'I'.
                        
                                        # it is OK to set the status outside of the child-process transaction; the checking and
                                        # setting of the request status can happen asynchronously because the child-process
                                        # transaction does not depend on the status; if an error does occur in the child or the client 
                                        # cancels the request, then those scenarios are handled in the parent process
                                        self.request.setStatus('D')
                                        nextFileIsReadyForDownload = True
                                        logQueue.put([ 'info', 'notified client ' + self.client + ' that dump file ' + outFile + ' is ready for download' ])
                                    else:
                                        logQueue.put([ 'debug', 'but next DML file is currently being dumped' ])
                            except shutil.Error as exc:
                                import traceback
                                logQueue.put([ 'error', traceback.format_exc(5) ])
                                raise Exception('fileIO', 'unable to rename ' + outFile + ' to ' + destFile)
                            finally:
                                self.reqTable.releaseLock()

                            # Because the client character encoding was set to UTF8, copy_expert() will convert
                            # the db's server encoding from Latin-1 to UTF8.

                            # the copy_to() command is running in a separate thread; we read a chunk of N lines here, and
                            # then write them to fout; if there are fewer than N lines available, and we have not hit EOF, 
                            # then we block here; once we have read N lines (or encountered EOF), we print them all to fout;
                            # and we also check for client feedback (error, done downloading or ingesting previous dump file
                    except Exception as exc:
                        if len(exc.args) > 0 and exc.args[0] == 'dumpDone':
                            logQueue.put([ 'debug', 'no more rows to dump' ])
                            fin.close() # do not close contained pipe fd now
                            fin = None
                
                            logQueue.put([ 'debug', 'entering loop that checks for completion of all downloads by client' ])
                            # all dump files have been created; as client ingests each one, remove it and move on to
                            # the next one
                        else:
                            raise
                elif ddlFileCreated and not clientHappy:
                    # this is the final subscription loop - there are no more DDL files to generate
                    # the child process closed the dump pipe
                    if maxLoop <= 0:
                        raise Exception('sqlAck', 'time-out waiting for client to ingest dump file')
                    
                    logQueue.put([ 'debug', '[ final subscription loop ] checking client DL/ingestion status' ])
                    self.reqTable.acquireLock()
                    try:
                        (fileNoIngested, nextFileIsReadyForDownload, status, logMsg) = self.checkClientStatus(fileNoIngested, nextFileIsReadyForDownload)
                        logQueue.put([ 'debug', '[ final subscription loop ]' + logMsg ])
                        if status == 'I':
                            logQueue.put([ 'debug', '[ final subscription loop ] client is ready for next DML file' ])
                            if fileNoIngested == fileNo:
                                logQueue.put([ 'debug', 'but there are no more DML files to download; done' ])
                                # no more files to download; the calling function will set status to 'C'
                                clientHappy = True
                                # this will cause the loop reading the logQueue to terminate
                                logQueue.put([ 'done', None ])
                            elif fileNo > fileNoIngested:
                                logQueue.put([ 'debug', 'and the next DML file is ready for download' ])
                                # fileNo is the last file generated; tell client the next file is ready for download
        
                                # rename file to name client looks for
                                outFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.dml' + str(fileNoIngested + 1).zfill(3) + '.sql.gz')
                                destFile = os.path.join(self.dumpDir, self.client + '.subscribe_series.sql.gz')
                                shutil.move(outFile, destFile)
                                logQueue.put([ 'debug', 'successfully moved DML to ' + destFile ])
        
                                # Set request status to 'D' to indicate, to client, that a dump is ready for
                                # download and ingestion. The client could have sent either a polldump request (for the first
                                # dump file) or a pollcomplete request (for all subsequent dump files). Both of those requests
                                # should handle a 'D' status. If the client makes a pollcomplete request, and the client
                                # sees a 'D' status, then it should go back to the code where it sets the status to 'A' and
                                # ingests the dump file, then sets status to 'I'.
                                self.request.setStatus('D')
                                nextFileIsReadyForDownload = True
                                logQueue.put([ 'info', 'notified client ' + self.client + ' that dump file ' + outFile + ' is ready for download' ])
                            else:
                                # error (can't have ingested more files than were created)
                                raise Exception('sqlDump', 'cannot ingest more files than were created')
                    except shutil.Error as exc:
                        import traceback
                        logQueue.put([ 'debug', traceback.format_exc(8) ])
                        raise Exception('fileIO', 'unable to rename ' + outFile + ' to ' + destFile)                                        
                    finally:
                        self.reqTable.releaseLock()

                    maxLoop -= 1
                    time.sleep(1) # no more DDL files to dump, so we are waiting on client to ingest (check once a second)
                
                if not loggingDone:
                    # ART - loop until there are no more items to print
                    try:
                        while True:
                            level, line = logQueue.get(False) # do not block
                            if level == 'done':
                                self.log.writeDebug([ 'done fetching output from child process' ])
                                loggingDone = True
                            elif line is not None and len(line) > 0:
                                if level == 'debug':
                                    self.log.writeDebug([ line ])
                                elif level == 'info':
                                    self.log.writeInfo([ line ])
                                elif level == 'warning':
                                    self.log.writeWarning([ line ])
                                elif level == 'error':
                                    self.log.writeError([ line ])
                                elif level == 'critical':
                                    self.log.writeCritical([ line ])
                            
                                line = None
                    except queue.Empty:
                        pass

                if clientHappy and loggingDone:
                    break
                    
                dumpLoopIter += 1
                
                # end while loop
        finally:
            gc.collect()
            self.log.writeDebug([ 'memory usage (MB) after initializing subscription ' + str(self.proc.memory_info().vms / 1048576) ])

        # check for an exception generated by the child
        exc = exceptionQueue.get() # if the logQueue-read loop exited about, then exceptionQueue is not empty, so this will not block
        
        # join the child process
        proc.join()
        proc = None
        self.log.writeDebug([ 'child process terminated' ])
        
        if exc is not None:
            # an exception occurred in the child process
            raise exc
            
    @classmethod
    def __readDumpChunk(cls, **kwargs):
        if 'file' in kwargs:
            binaryFileObj = kwargs['file']
        if 'chunksize' in kwargs:
            chunkSize = kwargs['chunksize']
        else:
            chunkSize = 4096
                
        while True:
            data = bytearray(binaryFileObj.read(chunkSize))
            if data == b'':
                break
            
            numBytes = len(data)
        
            if data[-1:] != b'\n':
                newData = bytearray(binaryFileObj.readline())
                if newData != b'':
                    numBytes += len(newData)
                    yield [ numBytes, data, newData ]
                else:
                    yield [ numBytes, data, bytearray(b'') ]
            else:
                yield [ numBytes, data, bytearray(b'') ]
                
    
    # child process
    @classmethod
    def __initializeSubscription(cls, dumperQueue, exceptionQueue, logQueue, readFD, writeFD):
        import pg8000
        import pgpasslib
    
        sys.stdout = open('child-proc-' + str(os.getpid()) + '.out', 'w')
        sys.stderr = sys.stdout
        
        os.close(readFD)

        print('in child process')
        with DumperProcessTerminator(logQueue) as dpt:
            print('got term handler')
            
            try:
                dumperInfo = dumperQueue.get() # object with arguments for this method
            
                password = pgpasslib.getpass(host=dumperInfo.args['subscribingDBHost'], port=dumperInfo.args['subscribingDBPort'], dbname=dumperInfo.args['subscribingDB'], user=dumperInfo.args['subscribingDBUser'])
                subscriberDBConn = pg8000.connect(database=dumperInfo.args['subscribingDB'], user=dumperInfo.args['subscribingDBUser'], host=dumperInfo.args['subscribingDBHost'], port=dumperInfo.args['subscribingDBPort'], password=password)

                cursor = subscriberDBConn.cursor()
                cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                cursor.execute("SET CLIENT_ENCODING TO 'UTF8'")

                logQueue.put([ 'debug', 'started a new serializable transaction on subscribing db' ])

                if dumperInfo.clientInfo['newSite']:            
                    # 6. Create an insert statement that, when run on the client, will set the client's Slony tracking number.
                    cmd = "SELECT 'INSERT INTO _" + dumperInfo.serverInfo['replicationCluster'] + ".sl_archive_tracking values (' || ac_num::text || ', ''' || ac_timestamp::text || ''', CURRENT_TIMESTAMP);' FROM _" + dumperInfo.serverInfo['replicationCluster'] + ".sl_archive_counter"
                    cursor.execute(cmd)
                    records = cursor.fetchall()
                    if len(records) != 1:
                        raise Exception('dbResponse', 'Unexpected response to db command: ' + cmd)

                    # Write to first dump file (below).
                    trackingInsert = records[0][0]
                    logQueue.put([ 'info', 'got Slony tracking number ' + trackingInsert ])
                    logQueue.put([ 'info', 'success running ' + cmd ])
                else:
                    trackingInsert = None
                
                cursor.close()
                # do not commit DB transaction here - we want to put the above SELECT into the same transaction as the COPY below
                print('got tracking insert')
                sys.stdout.flush()

                maxLoop = 30
                while True:
                    print('trying to disable interrupts')
                    sys.stdout.flush()
                    dpt.disableInterrupts()
                    print('disabled interrupts')
                    sys.stdout.flush()
                    try:
                        try:
                            cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + dumperInfo.serverInfo['subscriptionLock'] + ') 2> /dev/null'
                            check_call(cmd, shell=True)
                        except CalledProcessError as exc:
                            raise Exception('subLock')
                        # Obtained global subscription lock.
                        print('got subscription lock')
                        sys.stdout.flush()
            
                        # 7. add the client's sitedir/newlst file to the end of slon_parser.cfg, and add the client's sitedir
                        #    to su_production.slonycfg
                        with open(dumperInfo.serverInfo['parserConfig'], 'a') as fout:
                            print(dumperInfo.serverInfo['newClientLogDir'] + '        ' + dumperInfo.serverInfo['newLstPath'], file=fout)
                            
                        print('modified parser config')
                        sys.stdout.flush()

                        # do not provide the --lst option. We already inserted the lst-file info into su_production.slonylst earlier
                        cmdList = [ os.path.join(dumperInfo.serverInfo['repScriptPath'], 'subscribe_manage', 'gentables.pl'), 'op=add', 'conf=' + dumperInfo.serverInfo['slonyConfig'], '--node=' + dumperInfo.clientInfo['client'] + '.new', '--sitedir=' + dumperInfo.serverInfo['newClientLogDir'] ]
                        # raises CalledProcessError on error (non-zero returned by gentables.pl)
                        check_call(cmdList)
                        logQueue.put([ 'info', 'success running ' + ' '.join(cmdList) ])
                        # XXX - maybe log all gentables.pl output

                        break
                    except Exception as exc:
                        if len(exc.args) == 1 and exc.args[0] == 'subLock':
                            # could not obtain lock; try again (up to 30 tries)
                            if maxLoop <= 0:
                                raise Exception('subLock', 'could not obtain subscription lock after 30 tries')
                            time.sleep(1)
                            maxLoop -= 1
                        else:
                            # Failure to run gentables.pl, re-raise the exception generated.
                            raise
                    finally:
                        if os.path.exists(dumperInfo.serverInfo['subscriptionLock']):
                            os.remove(dumperInfo.serverInfo['subscriptionLock'])
                        dpt.enableInterrupts()
                        
                print('got sub lock and ran gentables')
                sys.stdout.flush()

                
                # 8. turn on the slon daemons; when the Slony-log parser runs, it will generate new logs for all clients;
                # if the slony start-up get interrupted by the termination handler, the parent Worker thread will
                # restart the slony daemons; the main manage-subs.py thread will block until the Worker threads terminate
                # properly; if we interrupt out of this child process, the calling Worker function (initializeSubscription())
                # will return and the enclosing finally clause (in dumpAndStartGeneratingClientLogs()) will fire, 
                # and that clause will start the slony daemons before the Worker terminates
                try:
                    cmdList = [ os.path.join(dumperInfo.serverInfo['drmsRoot'], 'base', 'drms', 'replication', 'manageslony', 'sl_start_slon_daemons.sh'), dumperInfo.serverInfo['slonyConfig'] ]
                    logQueue.put([ 'info', 'running ' + ' '.join(cmdList) ])
                    proc = Popen(cmdList, stdout=PIPE, stderr=PIPE, start_new_session=True)
                    outs, errs = proc.communicate()
                    if len(errs) > 0:
                        logQueue.put([ 'error', errs ])
                    if len(outs) > 0:
                        logQueue.put([ 'info', outs ])                        
                except CalledProcessError as exc:
                    raise Exception('startSlony', 'failure starting slon daemons')

                print('started slony')
                sys.stdout.flush()

                # 9. Sequences - at some point in the distant past, we used to replicate sequences too. The list
                #    of such sequences was stored in _jsoc.sl_sequence. However, we do not replicate sequences
                #    so we now skip the step of dumping them.
    
                # 10. Pump out chunks of the series database table.            
                # Use the server-side cursor to create an iterator that will efficiently pass data from
                # the server to the client.
                            
                # Because the client character encoding was set to UTF8, copy_to() will convert
                # the db's server encoding from Latin-1 to UTF8.
        
                # ACK - you cannot use psycopg2 AT ALL to do a COPY TO!! The only thing psycopg2
                # will allow you to do is to dump an entire table. With copy_to() and with copy_expert(), 
                # you cannot use a query to specify rows to dump. You cannot use cursor.execute() either, 
                # which normally would allow you to select records with a query. psycopg2 prohibits
                # you from running an SQL query that contains COPY TO in it.
                #
                # Give up and use a SELECT statement and then dump into fout. This is not great because
                # we have to send data over the network from server to client, then the client dumps to
                # fout, which is on the server.
                #
                # BTW, cursor.rowcount does not get properly set.


                # the first file - DDL only
                logQueue.put([ 'info', 'generating DDL dump file' ])
            
                # write to the temporary dot file that was created in Worker.dumpAndStartGeneratingClientLogs()
                outFile = os.path.join(dumperInfo.serverInfo['dumpDir'], '.' + dumperInfo.clientInfo['client'] + '.subscribe_series.ddl.sql.gz')
                # this actually creates a binary file (UTF8), but the file data in memory is represented as strs
                with gzip.open(outFile, mode='at', compresslevel=5, encoding='UTF8') as fout:                                
                    if trackingInsert:
                        # the first dump file contains the SQL that sets the correct sequence number
                        # at the client (new sites only)
                        print(trackingInsert, file=fout)
                    print('COMMIT;', file=fout)
                    fout.flush()
                
                # final file - the parent process will rename this file after it exists to the name that the client is looking for
                destFile = os.path.join(dumperInfo.serverInfo['dumpDir'], dumperInfo.clientInfo['client'] + '.subscribe_series.ddl.sql.gz')
                shutil.move(outFile, destFile)
                
                print('created  ddl file')
                sys.stdout.flush()
                
                # now do the copy into the write-end of the pipe
                fout = os.fdopen(writeFD, 'wb')
                try:
                    print('started the table dumper - starts a new transaction')
                    cursor = subscriberDBConn.cursor()
                    # with self.dbconn.cursor() as cursor:
                        # connection client is UTF8 encoding, so the output from copy_to() is UTF8, and self.file is a binary file;
                        # will block if the output buffer is full
                        # cursor.copy_to(self.file, self.table, columns=self.columns)
                        #cursor.copy_to(fout, self.table)
                        
                    # this write to stdout in binary format
                    cursor.execute('COPY ' + dumperInfo.subscriptionInfo['series'][0].lower() + ' TO STDOUT', stream=fout)
                    cursor.close()
                    print('leaving dbtabledumper child process')
                    sys.stdout.flush()
                except:
                    print('problem running PG COPY command in child; COPY terminated')
                    raise
                finally:
                    fout.flush()
                    fout.close() # very, very necessary, cannot close contained pipe fd now

                # the PG COPY command will have started a new transaction, but it is a read-only transaction so we can
                # roll it back
                subscriberDBConn.rollback()

                # after connection with block; EXITING THE with BLOCK DOES NOT FREE THE CONNECTION!! must call conn.close();
                subscriberDBConn.close()
                
                # tell parent process that there was no exception
                exceptionQueue.put(None)
            except pg8000.Error as exc:
                import traceback
                
                print('got a DB exception in child')
                # tell parent process that there was a db exception
                exceptionQueue.put(Exception('dbCmd', traceback.format_exc(8)))
            except Exception as exc:
                import traceback
                
                print('got an exception in child')
                sys.stdout.flush()
                
                # tell parent process that there was an exception while dumping DB table
                exceptionQueue.put(Exception('dumpProcess', traceback.format_exc(8)))
            finally:
                sys.stdout.close()

    # runs in a Worker thread
    def dumpAndStartGeneratingClientLogs(self):
        doAppend = False
                
        if self.action == 'subscribe':
            # Run createtabstruct for the table of the series being subscribed to.
            cmdList = [ os.path.join(self.arguments.getArg('kModDir'), 'createtabstructure'), '-u', 'JSOC_DBHOST=' + self.arguments.getArg('SLAVEHOSTNAME'), 'in=' + self.series[0].lower(), 'out=' + self.series[0].lower(), 'archive=' + str(self.archive), 'retention=' + str(self.retention), 'tapegroup=' + str(self.tapegroup), 'owner=' + self.subuser ]
            
            # create a temporary dot file; this gets made a permanent file (by removal of the dot) inside the
            # child process (Worker.__initializeSubscription())
            outFile = os.path.join(self.dumpDir, '.' + self.client + '.subscribe_series.ddl.sql.gz')

            wroteIntMsg = False
            with gzip.open(outFile, mode='wt', compresslevel=5, encoding='UTF8') as fout:
                # fout is actually a io.TextIOWrapper, a file object which I believe is incompatible with
                # Popen(). We need to filter the stream through a pipe whose ends are real file streams.
                pipeReadEndFD, pipeWriteEndFD = os.pipe()

                # Make these pipes not block on read.
                flag = fcntl.fcntl(pipeReadEndFD, fcntl.F_GETFL)
                fcntl.fcntl(pipeReadEndFD, fcntl.F_SETFL, flag | os.O_NONBLOCK)
                
                pipeReadEnd = os.fdopen(pipeReadEndFD, 'r', encoding='UTF8')
                pipeWriteEnd = os.fdopen(pipeWriteEndFD, 'w', encoding='UTF8')
            
                print('BEGIN;', file=fout)
                print("SET CLIENT_ENCODING TO 'UTF8';", file=fout)
            
                if self.newSite:
                    # Dump the database-object-creation SQL needed for a site to accept Slony data from the server.
                    sql = getNewSiteSQL(self.replicationCluster)
                    print(sql, file=fout)

                fout.flush()
            
                self.log.writeInfo([ 'Dumping table structure: ' + ' '.join(cmdList) + '.' ])
                proc = Popen(cmdList, stdout=pipeWriteEnd)

                maxLoop = 60 # 1-minute timeout
                while True:
                    if maxLoop <= 0:
                        raise Exception('sqlDump', 'time-out waiting for dump file to be generated')

                    if not wroteIntMsg and self.sdEvent.isSet():
                        self.log.writeInfo([ 'Cannot interrupt dump-file generation. Send SIGKILL message to force termination.' ])
                        wroteIntMsg = True
                        # proc.kill()
                        # raise Exception('shutDown', 'Received shut-down message from main thread.')
                        
                    # Read what is available from the pipe, convert to utf8, and write it to fout.
                    while True:
                        pipeBytes = pipeReadEnd.read(4096)
                        if len(pipeBytes) > 0:
                            print(pipeBytes, file=fout, end='')
                        else:
                            break

                    if proc.poll() is not None:
                        fout.flush()
                        #XXX-self.writeStream.logLines() # Log all createtabstructure output.
                        if proc.returncode != 0:
                            raise Exception('sqlDump', 'Failure generating dump file, createtabstructure returned ' + str(proc.returncode) + '.') 
                            
                        pipeWriteEnd.flush()
                        pipeWriteEnd.close()
                        
                        # Read the remaining bytes from the pipe.
                        while True:
                            pipeBytes = pipeReadEnd.read(4096)
                            if len(pipeBytes) > 0:
                                print(pipeBytes, file=fout, end='')
                            else:
                                break
                        pipeReadEnd.close()
                        break

                    maxLoop -= 1
                    time.sleep(1)

                fout.flush()
                
                # add the sunum-capturing trigger to the DRMS dataseries database table; disable the client psql's ON_ERROR_STOP 
                # feature - it may be that drms.capturesunum() does not exist on the client; in that case, do not error out; continue
                # on to the next SQL statements (the table dump insert statements)
                print(r'\set ON_ERROR_ROLLBACK on', file=fout)
                print(r'\unset ON_ERROR_STOP', file=fout)
                print('DROP TRIGGER IF EXISTS capturesunumtrig ON ' + self.series[0].lower() + ';', file=fout)                
                print('CREATE TRIGGER capturesunumtrig AFTER INSERT ON ' + self.series[0].lower() + ' FOR EACH ROW EXECUTE PROCEDURE drms.capturesunum();', file=fout)
                # re-enable the ON_ERROR_STOP feature
                print(r'\set ON_ERROR_STOP 1', file=fout)
                print(r'\unset ON_ERROR_ROLLBACK', file=fout)
                fout.flush()
                
            self.log.writeInfo([ 'Successfully created ' + outFile + ' and dumped tab structure commands.' ])
            self.log.writeDebug([ 'Memory usage (MB) ' + str(self.proc.memory_info().vms / 1048576) + '.' ])

            doAppend = True

        # This is the only way to synchronize everything:
        try:
            # 1. Turn off Slony-log parser. We are going to manually run the parser after the slon daemons are dead. If the parser is
            # already running at that time, it could interfere. We disable the parser by acquiring the subscription lock.
            maxLoop = 30
            while True:
                if self.sdEvent.isSet():
                    raise Exception('shutDown', 'Received shut-down message from main thread.')
                try:
                    try:
                        cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.subscribeLock + ') 2> /dev/null'
                        check_call(cmd, shell=True)
                    except CalledProcessError as exc:
                        # Other exceptions will re-raise to the outer-most exception handler.
                        raise Exception('subLock')
    
                    # Obtained global subscription lock.
                
                    # 2. Turn off slon daemons. When this script returns, the daemons will be dead. Stopping Slony will prevent the 
                    # Slony-log parser from doing any parsing (after we release the subscription lock). However, the parser will continue 
                    # to run as a cron job - but it will not have any effect. Stopping the parser from running while the slon daemons 
                    # are dead (below) would be difficult to do since we'd need to acquire the subscription lock a couple of times and 
                    # release it in the middle the database transaction (but that would make the nesting of exception handlers tricky).
                    # So, just allow the parser to run
                    try:
                        cmdList = [ os.path.join(self.arguments.getArg('kJSOCRoot'), 'base', 'drms', 'replication', 'manageslony', 'sl_stop_slon_daemons.sh'), self.slonyCfg ]
                        check_call(cmdList)
                    except CalledProcessError as exc:
                        raise Exception('stopSlony', 'failure stopping slon daemons')
                    finally:
                        pass
                        #XXX-self.writeStream.flush()
                        #XXX-self.writeStream.logLines() # log all sl_stop_slon_daemons.sh output

                    break
                except Exception as exc:
                    if len(exc.args) == 1 and exc.args[0] == 'subLock':
                        # Could not obtain lock; try again (up to 30 tries)
                        if maxLoop <= 0:
                            raise Exception('subLock', 'could not obtain subscription lock after 30 tries')
                        time.sleep(1)
                        maxLoop -= 1
                    else:
                        # Failure to run gentables.pl, re-raise the exception generated.
                        raise
                finally:
                    # 3. Turn on the Slony-log parser by releasing the subscription lock.
                    if os.path.exists(self.subscribeLock):
                        os.remove(self.subscribeLock)

            # 4. Parse existing slony logs (just in case the series being subscribed is modified in an unparsed Slony log). The -s flag causes logging to
            # be directed to stdout. It could be that the cron job caused the logs to be parsed, in which case the following command is a no-op.
            try:
                cmdList = [ os.path.join(self.arguments.getArg('kJSOCRoot'), 'base', 'drms', 'replication', 'parselogs', 'parse_slon_logs.pl'), self.slonyCfg, 'parselock.txt', 'subscribelock.txt', '-s' ]
                self.log.writeInfo([ 'running ' + ' '.join(cmdList) ])
                proc = Popen(cmdList, stdout=PIPE, stderr=PIPE)
                outs, errs = proc.communicate()
                if len(errs) > 0:
                    self.log.writeError([ errs ])
                if len(outs) > 0:
                    self.log.writeInfo([ outs ])
            except CalledProcessError as exc:
                raise Exception('parseLogs', 'failure running parse_slon_logs.pl')

            # There are now no unparsed logs with mods to self.series[0] in them now. The parser will continue to run, however
            # there will be no new logs to parse since the slon daemons have been terminated.
    
            # 5. Start a serializable transaction. No more changes to self.series[0] will appear in the dump file. However, such changes
            # will appear in the parsed log files. We need to make the next transaction start in the serializable isolation level, and
            # we need to start the client with a character encoding of UTF-8. When we commit the transaction, we must switch back to 
            # the original settings (which are likely READ COMMITTED and Latin-1).

            # capture client tracking number, add series to client subscription list, start slon daemons, generate client dump files
            self.initializeSubscription()
            self.log.writeInfo([ 'finished intializing subscription of series ' + self.series[0] + ' for client ' + self.client ])
        finally:
            # make sure the slon daemons have been restored, otherwise we might accidentally stop the generation of Slony logs if
            # an error occurs
            restartDaemons = False
            maxLoop = 30
            while True:
                try:
                    try:
                        cmd = '(set -o noclobber; echo ' + str(os.getpid()) + ' > ' + self.slonDaemonLock + ') 2> /dev/null'
                        check_call(cmd, shell=True)
                    except CalledProcessError as exc:
                        # Other exceptions will re-raise to the outer-most exception handler.
                        raise Exception('subLock')
    
                    # Obtained global slon daemon lock.
                    if not os.path.exists(self.masterSlonPIDFile) or not os.path.exists(self.slaveSlonPIDFile):
                        # The slon-daemon start script will only attempt to start daemons not running. We can't
                        # try to start daemons while we are holding the slon-daemon lock.
                        restartDaemons = True    
                    
                    break
                except Exception as exc:
                    if len(exc.args) == 1 and exc.args[0] == 'subLock':
                        # Could not obtain lock; try again (up to 30 tries)
                        if maxLoop <= 0:
                            raise Exception('subLock', 'Could not obtain subscription lock after 30 tries.')
                        time.sleep(1)
                        maxLoop -= 1
                    else:
                        raise
                finally:
                    if os.path.exists(self.slonDaemonLock):
                        os.remove(self.slonDaemonLock)
            
            if restartDaemons:
                try:
                    cmdList = [ os.path.join(self.arguments.getArg('kJSOCRoot'), 'base', 'drms', 'replication', 'manageslony', 'sl_start_slon_daemons.sh'), self.slonyCfg ]
                    self.log.writeInfo([ 'running ' + ' '.join(cmdList) ])
                    proc = Popen(cmdList, stdout=PIPE, stderr=PIPE, start_new_session=True)
                    outs, errs = proc.communicate()
                    if len(errs) > 0:
                        self.log.writeError([ errs ])
                    if len(outs) > 0:
                        self.log.writeInfo([ outs ])
                except CalledProcessError as exc:
                    raise Exception('startSlony', 'failure starting slon daemons')

    @staticmethod
    def newThread(request, newSite, arguments, connMaster, connSlave, log, fclean, msProc):
        worker = Worker(request, newSite, arguments, connMaster, connSlave, log, fclean, msProc)
        worker.tList.append(worker)
        
        # set status to P so that worker thread executes the new-worker branch; set the worker attribute in the request;
        # no need to hold a lock since the Worker thread for this request has not been spawned yet
        request.setStatus('P')
        request.setWorker(worker)
        log.writeDebug([ 'main thread set request ' + str(request.requestid) + ' status to P' ])

        log.writeDebug([ 'calling worker.start()' ])
        # spawn new thread
        worker.start()
        log.writeDebug([ 'successfully called worker.start()' ])
        return worker


class Request(object):
    def __init__(self, conn, log, reqtable, dbtable, requestid, client, starttime, action, series, archive, retention, tapegroup, subuser, status, errmsg):
        self.conn = conn
        self.log = log
        self.reqtable = reqtable
        self.dbtable = dbtable
        self.requestid = requestid
        self.client = client
        self.starttime = starttime
        self.action = action
        self.series = series
        self.archive = archive
        self.retention = retention
        self.tapegroup = tapegroup
        self.subuser = subuser
        self.status = status
        self.errmsg = errmsg
        
    def copy(self, source):
        self.conn = source.conn
        self.log = source.log
        self.reqtable = source.reqtable
        self.dbtable = source.dbtable
        self.requestid = source.requestid
        self.client = source.client
        self.starttime = source.starttime
        self.action = source.action
        self.series = source.series
        self.archive = source.archive
        self.retention = source.retention
        self.tapegroup = source.tapegroup
        self.subuser = source.subuser
        self.status = source.status
        self.errmsg = source.errmsg
        
    def setStatus(self, code, msg=None):
        # ART - Validate code
        self.status = code
        if msg is not None:
            self.errmsg = msg

        try:            
            # write to the DB (but do not commit to disk just yet, because that ends the transaction); call commit right before 
            # refreshing the requests table
            with self.conn.cursor() as cursor:
                # The requestid column is an integer.
                if self.errmsg:
                    cmd = 'UPDATE ' + self.dbtable + " SET status='" + self.status + "', errmsg='" + self.errmsg + "' WHERE requestid=" + str(self.requestid)
                else:
                    cmd = 'UPDATE ' + self.dbtable + " SET status='" + self.status + "' WHERE requestid=" + str(self.requestid)

                cursor.execute(cmd)
        except psycopg2.Error as exc:
            self.conn.rollback()
            raise Exception('reqtableWrite', exc.diag.message_primary)
        except:
            self.conn.rollback()
            raise
            
    def getStatus(self):
        return (self.status, self.errmsg)
    
    def hasWorker(self):
        return hasattr(self, 'worker') and self.worker
        
    def getWorker(self):
        if self.hasWorker():
            return self.worker
        return None
     
    def setWorker(self, worker):
        if not isinstance(worker, Worker):
            raise Exception('invalidArgument', 'Type of argument to setWorker argument must be Worker; ' + type(worker) + ' was provided.')
        self.worker = worker
        
    def stopWorker(self, **kwargs):
        if 'saveState' in kwargs:
            saveState = kwargs['saveState']
        else:
            saveState = False
        if 'wait' in kwargs:
            wait = kwargs['wait']
        else:
            wait = False
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']
        else:
            timeout = 30;
            
        if hasattr(self, 'worker') and self.worker:
            self.worker.stop(saveState)
            if wait and self.worker.isAlive():
                 self.worker.join(timeout)
            self.worker = None
                 
    def removeWorker(self):
        if hasattr(self, 'worker') and self.worker:
            self.worker = None

    def dump(self, toLog=True):
        text = 'requestID=' + str(self.requestid) + ', client=' + self.client + ', starttime=' + self.starttime.strftime('%Y-%m-%d %T') + ', series=' + ','.join(self.series) + ', action=' + self.action + ', archive=' + str(self.archive) + ', retention=' + str(self.retention) + ', tapegroup=' + str(self.tapegroup) + ', subuser=' + self.subuser + ', status=' + self.status + ', errmsg=' + str(self.errmsg if (self.errmsg and len(self.errmsg) > 0) else "''")
        if toLog:
            self.log.writeInfo([ text ])
        return text

class ReqTable(object):    
    def __init__(self, tableName, timeOut, conn, log):
        self.tableName = tableName
        self.timeOut = timeOut
        self.conn = conn
        self.log = log
        self.lock = threading.Lock()
        self.reqDict = {}
        self.clientMap = {} # map client to requests (client -> [ reqid1, reqid2, ... ]
        
        self.tryRead()
    
    def read(self):
        # requests(client, requestid, starttime, action, series, status, errmsg)
        cmd = 'SELECT requestid, client, starttime, action, series, archive, retention, tapegroup, subuser, status, errmsg FROM ' + self.tableName

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(cmd)        
        
                for record in cursor:
                    requestidStr = str(record[0])

                    requestid = record[0] # bigint
                    client = record[1]    # text
                    # Whoa! For starttime, pyscopg2 returns timestamps as datetime.datetime objects already!
                    starttime = record[2] # datetime.datetime
                    action = record[3]    # text            
                    series = [ aseries for aseries in record[4].split(',') ] # text (originally)
                    archive = record[5]   # int
                    retention = record[6] # int
                    tapegroup = record[7] # int
                    subuser = record[8] # text
                    status = record[9]    # text
                    errmsg = record[10]    # text
                    req = Request(self.conn, self.log, self, self.tableName, requestid, client, starttime, action, series, archive, retention, tapegroup, subuser, status, errmsg)
                    self.reqDict[requestidStr] = req
                    if client not in self.clientMap:
                        self.clientMap[client] = set()
                    self.clientMap[client].add(requestid)
        except psycopg2.Error as exc:
            raise Exception('reqtableRead', exc.diag.message_primary + ': ' + cmd + '.')
        finally:
            self.conn.rollback()        

    def tryRead(self):
        nAtts = 0
        while True:
            try:
                self.read()
                break
            except Exception as exc:
                if len(exc.args) != 2:
                    raise # Re-raise

                etype = exc.args[0]

                if etype == 'reqtableRead':
                    if nAtts > 10:
                        raise # Re-raise
                else:
                    raise

            nAtts += 1
            time.sleep(1)
            
    def addRequest(self, req):
        if str(req.requestid) in self.reqDict:
            raise
            
        self.reqDict[str(req.requestid)] = req
        if req.client not in self.clientMap:
            self.clientMap[req.client] = set()
        self.clientMap[req.client].add(req.requestid)
        
    def removeRequest(self, req):
        if req.client in self.clientMap:
            self.clientMap[req.client].remove(req.requestid)

        del self.reqDict[str(req.requestid)]

    # This method finds new requests ('N' records) inserted since the last time it was run (or since the table was first read). It ignores
    # all other changes to the database table (made from outside this program) that have happened. To read those changes,
    # shut down this program, then make the changes, then start this program again.
    def refresh(self):
        # Get a fresh copy of the requests from the DB.
        latestReqTable = ReqTable(self.tableName, self.timeOut, self.conn, self.log)
        latestReqs = latestReqTable.get()
        oldReqs = self.get()
        
        latestReqs.sort(key=lambda req : req.requestid)
        oldReqs.sort(key=lambda req : req.requestid)

        if len(oldReqs) == 0:
            # copy all from latestReqs into reqtable (no requests currently exist in reqtable)
            for latestReq in latestReqs:
                req = Request(self.conn, self.log, self, self.tableName, latestReq.requestid, latestReq.client, latestReq.starttime, latestReq.action, latestReq.series, latestReq.archive, latestReq.retention, latestReq.tapegroup, latestReq.subuser, latestReq.status, latestReq.errmsg)
                # This new request has a pointer to the old request table, which is correct.
                self.addRequest(req)
                
            return
        
        if len(latestReqs) == 0:
            # delete all requests (somebody deleted all our existing requests in the reqtable)
            for oldReq in oldReqs:
                # Stop any associated worker.
                oldReq.stopWorker(wait=True, timeout=120)
                
            self.reqDict.clear()
            self.clientMap = {}
            
            return

        # Step through all reqs in both lists. If the current requestIDs match, then copy from latest to old, and 
        # increment both pointers. If they do not match, then if the smaller requestID is from the latest, copy over.
        # Increment the iLatest pointer. If the smaller requestID is from the old, delete and increment iOld.
        toDel = [] # requests deleted from the reqtable db table externally
        toAdd = [] # new requests added to the reqtable db table externally
        iLatest = 0
        iOld = 0
        
        while iLatest < len(latestReqs) or iOld < len(oldReqs):
            if iLatest < len(latestReqs):
                latestReq = latestReqs[iLatest]
                
                # we don't want the new request table being referenced from any new request
                latestReq.conn = self.conn
                latestReq.log = self.log
                latestReq.reqtable = self
                latestReq.tableName = self.tableName
            else:
                latestReq = None
                        
            if iOld < len(oldReqs):
                oldReq = oldReqs[iOld]                
            else:
                oldReq = None
        
            if latestReq and oldReq and (latestReq.requestid == oldReq.requestid):
                # Don't delete the oldReq - just update its attributes. There could be a worker currently processing
                # the request. The only thing that should change is the status (request-subs.py could change it).
                
                # this could overwrite the in-memory status value not saved to disk
                if oldReq.client != latestReq.client:
                    # do not allow anybody to change the client for an existing request
                    raise
                self.reqDict[str(oldReq.requestid)].copy(latestReq)
                
                iLatest += 1
                iOld += 1
            elif latestReq and oldReq:
                if latestReq.requestid < oldReq.requestid:
                    toAdd.append(latestReq)
                    iLatest += 1
                else:
                    toDel.append(oldReq)
                    iOld += 1
            elif latestReq:
                toAdd.append(latestReq)
                iLatest += 1
            else:
                toDel.append(oldReq)
                iOld += 1
                
        for req in toDel:
            req.stopWorker(wait=True, timeout=120)
            self.removeRequest(req)
            
        for req in toAdd:
            self.addRequest(req)
        
    def acquireLock(self, blocking=True, timeout=-1):
        return self.lock.acquire(blocking=blocking, timeout=timeout)

    def releaseLock(self):
        self.lock.release()
        
    def setStatus(self, requestids, code, msg=None):
        for arequestid in requestids:
            requestidStr = str(arequestid)
    
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')

            if msg:
                self.reqDict[requestidStr].setStatus(code, msg)
                self.reqDict[requestidStr]['errmsg'] = msg
            else:
                self.reqDict[requestidStr].setStatus(code)

    def getStatus(self, requestids):
        statuses = []
        for arequestid in requestids:
            requestidStr = str(arequestid)
            
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')
            
            # A 2-tuple: code, msg
            status = self.reqDict[requestidStr].getStatus()[0]
            
            statuses.append(status)       
            
        return statuses

    def get(self, requestids=None):
        toRet = []
    
        if not requestids:
            return [ self.reqDict[key] for (key, val) in self.reqDict.items() ]
        
        for arequestid in requestids:
            requestidStr = str(arequestid)
            
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')
    
            toRet.append(self.reqDict[requestidStr])
    
        return toRet
    
    def getPending(self, client=None):
        pendLst = []
    
        for requestidStr in self.reqDict.keys():
            if (self.reqDict[requestidStr].status == 'P' or self.reqDict[requestidStr].status == 'D' or self.reqDict[requestidStr].status == 'I') and (client is None or self.reqDict[requestidStr].client == client):
                pendLst.append(self.reqDict[requestidStr])

        if len(pendLst) > 0:    
            # Sort by start time. Sorts in place - and returns None.
            pendLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return pendLst
    
    def getNew(self, client=None):
        newLst = []

        for requestidStr in self.reqDict.keys():
            if self.reqDict[requestidStr].status == 'N' and (client is None or self.reqDict[requestidStr].client == client):
                newLst.append(self.reqDict[requestidStr])            
        
        if len(newLst) > 0:
            # Sort by start time. Sorts in place - and returns None.
            newLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return newLst

    def getProcessing(self, client=None):
        procLst = []

        # Return results for client only.
        for requestidStr in self.reqDict.keys():
            if (self.reqDict[requestidStr].status == 'P' or self.reqDict[requestidStr].status == 'D' or self.reqDict[requestidStr].status == 'I') and (client is None or self.reqDict[requestidStr].client == client):
                procLst.append(self.reqDict[requestidStr])

        if len(procLst) > 0:    
            # Sort by start time. Sorts in place - and returns None.
            procLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return procLst

    def getInError(self, client=None):
        errLst = []
    
        for requestidStr in self.reqDict.keys():
            if self.reqDict[requestidStr].status == 'E' and (client is None or self.reqDict[requestidStr].client == client):
                errLst.append(self.reqDict[requestidStr])

        if len(errLst) > 0:
            # Sort by start time. Sorts in place - and returns None.
            errLst.sort(key=lambda req : req.starttime.strftime('%Y-%m-%d %T'))

        return errLst

    def deleteRequests(self, requestids):
        for arequestid in requestids:
            requestidStr = str(arequestid)
    
            if not requestidStr in self.reqDict or not self.reqDict[requestidStr]:
                raise Exception('unknownRequestid', 'No request-table record exists for ID ' + requestidStr + '.')

            try:
                cmd = 'DELETE FROM ' + self.tableName + ' WHERE requestid=' + str(self.reqDict[requestidStr].requestid)
                with self.conn.cursor() as cursor:
                    cursor.execute(cmd)
            except psycopg2.Error as exc:
                self.conn.rollback()
                raise Exception('reqtableWrite', exc.diag.message_primary + ': ' + cmd + '.')
            except:
                self.conn.rollback()
                raise

            self.reqDict[requestidStr].stopWorker(wait=True) # Will stop worker and clean-up temp files, if a worker exists. Waits for worker to terminate.
            self.removeRequest(self.reqDict[requestidStr])
            
    def getClientRequests(self, **kwargs):
        client= kwargs['client']
        status = kwargs['status']
        ids = []
        
        if client in self.clientMap:
            ids = self.clientMap[client]

        return [ req for req in self.get(ids) if req.status in status ]

    def getTimeout(self):
        return self.timeOut

class SubscriptionList(object):
    def __init__(self, sublistDbTable, conn, client):
        self.sublistDbTable = sublistDbTable
        self.conn = conn
        self.client = client
        self.list = []
        self.getSubscriptionList()

    def getSubscriptionList(self):
        if len(self.list) == 0:
            cmd = 'SELECT series FROM ' + self.sublistDbTable + " WHERE node = '" + self.client + "' ORDER BY series"

            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(cmd)
                    records = cursor.fetchall()
                    self.list = [ rec[0] for rec in records ]            
            except psycopg2.Error as exc:
                raise Exception('getSubList', exc.diag.message_primary)
            finally:
                self.conn.rollback()

        return self.list

    def refreshSubscriptionList(self):
        if len(self.list) > 0:
            del self.list[:]
        self.getSubscriptionList()
        
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
        
def clientIsNew(arguments, conn, client):
    # Master database.
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + arguments.getArg('kCfgTable') + ' WHERE ' + CFG_TABLE_NODE + " = '" + client + "'"
    
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

def cleanAllSavedFiles(dumpDir, log):
    regExpNs = re.compile(r'.+\.createns\.sql\.gz\.(.+)$')
    regExpDump = re.compile(r'.+\.subscribe_series\.sql\.gz\.(.+)$')
    
    for file in os.listdir(dumpDir):
        date = None
        matchNs = regExpNs.match(file)
        
        if matchNs:
            date = matchNs.group(1)
        else:
            matchDump = regExpDump.match(file)
            if matchDump:
                date = matchDump.group(1)

        if date:
            try:
                # This was difficult to get right - you cannot use %T for strptime(), although you can use it for strftime().
                datetime.strptime(date, '%Y-%m-%d-%H%M%S')

                # OK to remove the file.
                log.writeInfo([ 'Removing saved dump file: ' + os.path.join(dumpDir, file) + '.' ])
                os.remove(os.path.join(dumpDir, file))
            except ValueError:
                pass


class RequestFactory(object):
    def __init__(self, worker):
        self.worker = worker

    def extractRequest(self, jsonStr):
        requestDict = json.loads(jsonStr)
        
        if 'reqtype' not in requestDict:
            raise ExtractRequestException('reqtype is missing from request')
        
        reqType = requestDict['reqtype']
        if reqType == REQUEST_TYPE_GETPENDING:
            return GetPendingRequest(args=requestDict, reqtable=self.worker.reqtable, worker=self.worker)
        elif reqType == REQUEST_TYPE_ERROR:
            return ErrorRequest(args=requestDict, reqtable=self.worker.reqtable, worker=self.worker)
        elif reqType == REQUEST_TYPE_SETSTATUS:
            return SetStatusRequest(args=requestDict, reqtable=self.worker.reqtable, worker=self.worker)
        elif reqType == REQUEST_TYPE_DONE:
            return DoneRequest(args=requestDict, reqtable=self.worker.reqtable, worker=self.worker)
        else:
            raise RequestTypeException('the request type ' + reqType + ' is not supported')

                
class RequestWorker(threading.Thread):    
    MSGLEN_NUMBYTES = 8 # This is the hex-text version of the number of bytes in the response message.
                        # So, we can send back 4 GB of response!
    MAX_MSG_BUFSIZE = 4096 # Don't receive more than this in one call!
    
    def __init__(self, **kwargs):
        threading.Thread.__init__(self)
        self.sock = kwargs['sock']
        self.reqtable = kwargs['reqtable']
        self.clientResponseTimeout = kwargs['timeout']
        self.log = kwargs['log']
    
    def run(self):
        try:
            while True: # request loop
                # read request first - it has the timeout value in it
                msgStr = self.receiveJson() # msgStr is a string object.
                req = self.extractRequest(msgStr) # will raise if reqtype is not supported
                rspDict = None
                peerName = self.getID()

                # always acquire the reqtable lock
                if self.reqtable.acquireLock(blocking=True, timeout=req.timeout):
                    try:
                        self.log.writeDebug([ 'processing ' + req.__class__.__name__ + ' client request' ])
                        rspDict = req.process() # optionally acquires request object lock
                        if hasattr(req, 'logMsg'):
                            self.log.writeDebug([ req.logMsg ])
                
                        # send response to client
                        self.log.writeDebug([ 'sending response to client: ' + str(rspDict) ])
                        self.sendJson(json.dumps(rspDict))
            
                        # there may be additional client requests in this connection
                        if isinstance(req, DoneRequest):            
                            # wait for client to close connection, and when that happens, release all locks
                            try:                                                    
                                textReceived = self.sock.recv(RequestWorker.MAX_MSG_BUFSIZE) # blocks until data avail, or EOF
                                # self.sock can be dead if the client broke the socket 
                                if textReceived == b'':
                                    # the client closed their end of the socket (they shutdown the write half of the socket);
                                    # so the client most likely called shutdown() followed by close()
                                    self.log.writeDebug([ 'client ' + peerName + ' properly terminated connection' ])
                                else:
                                    self.log.writeDebug([ 'client ' + peerName + ' sent extraneous data over socket connection (ignoring)' ])
                                    
                                    # dump the miscellany on floor - try for 15 seconds
                                    timeStart = datetime.now()
                                    timeOutTime = timeStart + timedelta(seconds=15)
                                    toRead = [ self.sock ]
                                    while datetime.now() < timeOutTime:
                                        readable, writeable, problematic = select.select(toRead, [], [], 1) # blocks for 1 second

                                        if len(readable) > 0:
                                            # got junk
                                            textReceived = self.sock.recv(RequestWorker.MAX_MSG_BUFSIZE)
                                            if textReceived == b'':
                                                break
                            except OSError:
                                self.log.writeDebug([ 'problem reading from socket (client ' + peerName + ')' ])
                                rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': 'broken socket' } }
                                self.sendJson(json.dumps(rspDict))
                            finally:
                                break # out of request loop
                    finally:
                        # release reqtable lock
                        self.reqtable.releaseLock()
                else:
                    # generate a failure response and send it to the client
                    self.log.writeDebug([ '[ RequestWorker::run() ] time-out acquiring reqtable lock' ])
                    rspDict = { 'serverstatus': { 'code': 'timeout', 'errmsg': 'unable to acquire request table lock' } }
                    self.sendJson(json.dumps(rspDict))
        except MSException as exc:
            self.log.writeError([ '[ RequestWorker::run() ] error: ' + exc.msg ])
        finally:
            self.log.writeDebug([ 'RequestWorker ' + self.getID() + ' terminating' ])
            
    def getID(self):
        if hasattr(self, 'peerName') and self.peerName and len(self.peerName) > 0:
            return self.peerName
        else:
            # will raise if the socket has been closed by client
            self.peerName = str(self.sock.getpeername())
        
        return self.peerName
        
    def extractRequest(self, msg):
        if not hasattr(self, 'reqFactory') or not self.reqFactory:
            self.reqFactory = RequestFactory(self)
            
        return self.reqFactory.extractRequest(msg)
    
    # Returns a bytes object.
    def receiveMsg(self):
        # First, receive length of message.
        allTextReceived = bytearray(b'')
        bytesReceivedTotal = 0

        # time-out time
        timeStart = datetime.now()
        timeOutTime = timeStart + self.clientResponseTimeout

        skipSleep = True # force a skip of the select() code for the first minute
        toRead = [ self.sock ]
        while bytesReceivedTotal < RequestWorker.MSGLEN_NUMBYTES:
            if datetime.now() > timeOutTime:
                raise ReceiveMsg('[ receiveMsg()-1 ] timeout waiting for request from client')

            if skipSleep:
                if datetime.now() > timedelta(minutes=1) + timeStart:
                    skipSleep = False

            readable, writeable, problematic = select.select(toRead, [], [], 0.1) # blocks for 0.1 seconds

            if len(readable) > 0:
                self.log.writeDebug([ '[ receiveMsg()-1 ] server found something on socket to read' ])
                # textReceived is a bytes object
                textReceived = self.sock.recv(min(RequestWorker.MSGLEN_NUMBYTES - bytesReceivedTotal, RequestWorker.MAX_MSG_BUFSIZE))
                if textReceived == b'':
                    raise ReceiveMsg('[ receiveMsg()-1 ] socket broken - cannot receive message-length data from client')
                allTextReceived.extend(textReceived)
                bytesReceivedTotal += len(textReceived)
            else:
                # a recv() would block
                if not skipSleep:
                    self.log.writeDebug([ '[ receiveMsg()-1 ] waiting for client ' + self.peerName + ' to send request' ])
                    time.sleep(1)
            
        # Convert hex string to number.
        numBytesMessage = int(allTextReceived.decode('UTF-8'), 16)
        
        # Then receive the message.
        allTextReceived = bytearray(b'')
        bytesReceivedTotal = 0

        skipSleep = True # force a skip of the select() code for the first minute
        toRead = [ self.sock ]
        while bytesReceivedTotal < numBytesMessage:
            if datetime.now() > timeOutTime:
                raise ReceiveMsg('[ receiveMsg()-2 ] timeout waiting for request from client')
            
            if skipSleep:
                if datetime.now() > timedelta(minutes=1) + timeStart:
                    skipSleep = False

            readable, writeable, problematic = select.select(toRead, [], [], 0.1) # blocks for 0.1 seconds

            if len(readable) > 0:
                self.log.writeDebug([ '[ receiveMsg()-2 ] server found something on socket to read' ])
                # textReceived is a bytes object          
                textReceived = self.sock.recv(min(numBytesMessage - bytesReceivedTotal, RequestWorker.MAX_MSG_BUFSIZE))
                if textReceived == b'':
                    raise ReceiveMsg('[ receiveMsg()-2 ] socket broken - cannot receive message data from client')
                allTextReceived.extend(textReceived)
                bytesReceivedTotal += len(textReceived)
            else:
                # a recv() would block
                if not skipSleep:
                    self.log.writeDebug([ '[ receiveMsg()-2 ] waiting for client ' + self.peerName + ' to send request' ])
                    time.sleep(1)

        # Return a bytes object (not a string). The unjsonize function will need a str object for input.
        return allTextReceived
        
    # msg is a bytes object.
    def sendMsg(self, msg):
        # First send the length of the message.
        bytesSentTotal = 0
        numBytesMessage = '{:08x}'.format(len(msg))
        
        # send the size of the message
        while bytesSentTotal < RequestWorker.MSGLEN_NUMBYTES:
            bytesSent = self.sock.send(bytearray(numBytesMessage[bytesSentTotal:], 'UTF-8'))
            if not bytesSent:
                raise SendMsg('socket broken - cannot send message-length data to client')
            bytesSentTotal += bytesSent
        
        # then send the actual message
        bytesSentTotal = 0
        while bytesSentTotal < len(msg):
            bytesSent = self.sock.send(msg[bytesSentTotal:])
            if not bytesSent:
                raise SendMsg('socket broken - cannot send message data to client')
            bytesSentTotal += bytesSent

        self.log.writeDebug([ self.getID() + ' - sent ' + str(bytesSentTotal) + ' bytes response' ])

    def receiveJson(self):
        msgBytes = self.receiveMsg() # a bytearray object, not a str object. json.loads requires a str object
        return msgBytes.decode('UTF-8') # convert bytearray to str
        
    # msg is a str object.
    def sendJson(self, msgStr):
        msgBytes = bytes(msgStr, 'UTF-8')
        self.sendMsg(msgBytes)


class ServerRequest(object):
    def __init__(self, **kwargs):
        self.reqtype = kwargs['args']['reqtype']
        self.client = kwargs['args']['client']
        self.timeout = kwargs['args']['timeout']
        self.reqtable = kwargs['reqtable']
        self.worker = kwargs['worker']

class GetPendingRequest(ServerRequest):
    def __init__(self, **kwargs):
        super(GetPendingRequest, self).__init__(**kwargs)

    def process(self):
        rspDict = None
        try:
            rspDict = { 'serverstatus': { 'code': 'ok', 'errmsg': '' }, 'requests': [] }
            requests = self.reqtable.getClientRequests(client=self.client.lower(), status=[ 'N', 'P', 'D', 'A', 'I', 'C' ])
            for request in requests:
                rspDict['requests'].append({ 'requestid': request.requestid, 'action': request.action, 'series': request.series, 'archive': request.archive, 'retention': request.retention, 'tapegroup': request.tapegroup, 'subuser': request.subuser, 'status': request.status, 'errmsg': request.errmsg })
        except:
            import traceback
            
            rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': traceback.format_exc(1) } }
            
        return rspDict

class ErrorRequest(ServerRequest):
    def __init__(self, **kwargs):
        super(ErrorRequest, self).__init__(**kwargs)
        self.requestid = kwargs['args']['requestid']
        self.errmsg = kwargs['args']['errmsg']
        
    def process(self):
        rspDict = None
        try:
            requests = self.reqtable.get([ self.requestid ])
            if len(requests) == 0:
                rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': 'request ' + str(self.requestid) + ' does not exist' } }
            else:                
                requests[0].setStatus('E', self.errmsg)

            rspDict = { 'serverstatus': { 'code': 'ok', 'errmsg': '' } }
        except:
            import traceback
            
            rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': traceback.format_exc(1) } }
            
        return rspDict

        
class SetStatusRequest(ServerRequest):
    def __init__(self, **kwargs):
        super(SetStatusRequest, self).__init__(**kwargs)
        self.requestid = kwargs['args']['requestid']
        self.status = kwargs['args']['status']
        if 'errmsg' in kwargs['args']:
            self.errmsg = kwargs['args']['errmsg']
        
    def process(self):
        rspDict = None
        try:
            requests = self.reqtable.get([ self.requestid ])
            if len(requests) == 0:
                rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': 'request ' + str(self.requestid) + ' does not exist' } }
            else:
                if hasattr(self, 'errmsg'):
                    errmsg = self.errmsg
                else:
                    errmsg = None

                requests[0].setStatus(self.status, errmsg)
                self.logMsg = 'SetStatusRequest: set status to ' + self.status

            rspDict = { 'serverstatus': { 'code': 'ok', 'errmsg': '' } }
        except:
            import traceback
            
            rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': traceback.format_exc(1) } }
            
        return rspDict
        
class DoneRequest(ServerRequest):
    def __init__(self, **kwargs):
        super(DoneRequest, self).__init__(**kwargs)
    
    def process(self):
        rspDict = None
        try:
            rspDict = { 'serverstatus': { 'code': 'ok', 'errmsg': '' } }
        except:
            import traceback
            
            rspDict = { 'serverstatus': { 'code': 'error', 'errmsg': traceback.format_exc(1) } }

        return rspDict    

        
# a single thread that accepts socket connects, and then spawns a threads to handle the requests
class RequestDispatcher(threading.Thread):
    def __init__(self, **kwargs):
        super(RequestDispatcher, self).__init__()
        self.reqtable = kwargs['reqtable']
        self.log = kwargs['log']
        self.serverhost = kwargs['serverhost']
        self.listenport = kwargs['listenport']        
        self.sdEvent = threading.Event()

    def run(self):
        try:
            # use getaddrinfo() to try as many families/protocols as are supported; it returns a list
            info = socket.getaddrinfo(self.serverhost, str(self.listenport))

            for oneAddrInfo in info:
                family, sockType, proto, canonName, sAddress = oneAddrInfo
            
                try:
                    self.log.writeInfo([ 'attempting to create socket with family ' + str(family) + ' and socket type ' + str(sockType) ])
                    serverSock = socket.socket(family, sockType, proto)
                    self.log.writeInfo([ 'successfully created socket with family ' + str(family) + ' and socket type ' + str(sockType) ])
                except OSError:
                    import traceback
                
                    self.log.writeWarning([ traceback.format_exc(5) ])
                    self.log.writeWarning([ 'trying next address (could not create socket)' ])
                    if serverSock:
                        serverSock.close()
                    serverSock = None
                    continue
            
                # now try binding
                try:
                    serverSock.bind(sAddress)
                    self.log.writeInfo([ 'successfully bound socket to address ' + str(serverSock.getsockname()) + ':' + str(self.listenport) ])
                    break # we're good!
                except OSError:
                    import traceback
                
                    self.log.writeWarning([ traceback.format_exc(5) ])
                    self.log.writeWarning([ 'trying next address (could not bind address)' ])
                    if serverSock:
                        serverSock.close()
                    serverSock = None
                    continue

            # it is possible that we never succeeded in creating and binding the socket
            try:
                serverSock.listen(5)
            except OSError:
                self.log.writeError([ 'could not create socket to listen for client requests' ])
                raise
                
            self.log.writeCritical([ 'listening for client requests on ' + str(serverSock.getsockname()) ])
        except Exception as exc:
            if len(exc.args) > 0:
                raise Exception('socket', str(exc.args[0]))
            else:
                raise Exception('socket', 'failure creating a socket to listen for incoming connections')

        pollObj = select.poll()
        pollObj.register(serverSock, select.POLLIN | select.POLLPRI)

        # while not sigThread.isShuttingDown():
        try:
            while not self.sdEvent.is_set():
                try:
                    fdList = pollObj.poll(500)
                except IOError as exc:
                  raise Exception('poll', 'a failure occurred while checking for new client connections')

                if len(fdList) == 0:
                    # nobody is knocking on the door
                    continue
                else:
                    (clientSock, address) = serverSock.accept() # blocking socket
                    self.log.writeCritical([ 'accepting a client request from ' + str(clientSock.getpeername()) + ', connected to server ' + str(clientSock.getsockname()) ])
                    
                    # spawn a short-lived thread to handle the request-subs.py requests
                    self.log.writeDebug([ 'instantiating a RequestWorker for client ' + str(address) ])
                    rw = RequestWorker(sock=clientSock, reqtable=self.reqtable, timeout=timedelta(seconds=15), log=self.log)
                    rw.start()
        finally:    
            pollObj.unregister(serverSock)
    
            # kill server socket
            serverSock.shutdown(socket.SHUT_RDWR)
            serverSock.close()
            self.log.writeInfo([ 'closed server socket' ])
            self.log.writeInfo([ 'request dispatcher thread halted' ])
    
    # called from main thread
    def stop(self):
        self.sdEvent.set()
        
    def extractAddresses(self, family):
        addresses = []
        for interface, snics in psutil.net_if_addrs().items():
            for snic in snics:
                if snic.family == family:
                    addresses.append(snic.address)

        return addresses


# Main Program
if __name__ == "__main__":
    rv = RV_TERMINATED
    
    try:        
        msLog = None
        manageSubsParams = ManageSubsParams()
        arguments = Arguments()

        parser = CmdlParser(usage='%(prog)s [ slonycfg=<configuration file> ] [ loglevel=<critical, error, warning, info, or debug > ]')    
        parser.add_argument('cfg', '-c', '--cfg', help='The configuration file that contains information needed to locate database information.', metavar='<slony configuration file>', dest='slonyCfg', action=CfgAction, arguments=arguments, default=manageSubsParams.get('SLONY_CONFIG'))
        parser.add_argument('loglevel', '-l', '--loglevel', help='Specifies the amount of logging to perform. In increasing order: critical, error, warning, info, debug', dest='loglevel', action=LogLevelAction, default=logging.ERROR)
        parser.add_argument('fclean', '-f', '--fclean', help='Force clean up temporary files from an earlier run.', dest='fclean', action='store_true', default=False)

        arguments.setParser(parser)
    
        pid = os.getpid()
        proc = psutil.Process(pid)
        
        # Create/Initialize the log file.
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        msLog = Log(os.path.join(arguments.getArg('kSMlogDir'), 'manage-subs-log.txt'), arguments.getArg('loglevel'), formatter)

        arguments.dump(msLog)
        msLog.writeCritical(['Logging threshold level is ' + msLog.getLevel() + '.']) # Critical - always write the log level to the log.
        msLog.writeDebug([ 'Initial memory usage (MB) ' + str(proc.memory_info().vms / 1048576) + '.' ])
            
        thContainer = [ os.path.join(arguments.getArg('kServerLockDir'), 'manage-subs-lock.txt'), os.path.join(arguments.getArg('kServerLockDir'), arguments.getArg('kSubLockFile')), str(pid), msLog, proc, None ]
        with TerminationHandler(thContainer) as th:
            with psycopg2.connect(database=arguments.getArg('SLAVEDBNAME'), user=arguments.getArg('REPUSER'), host=arguments.getArg('SLAVEHOSTNAME'), port=str(arguments.getArg('SLAVEPORT'))) as connSlave, psycopg2.connect(database=arguments.getArg('MASTERDBNAME'), user=arguments.getArg('REPUSER'), host=arguments.getArg('MASTERHOSTNAME'), port=str(arguments.getArg('MASTERPORT'))) as connMaster:
                msLog.writeInfo([ 'connected to database ' + arguments.getArg('SLAVEDBNAME') + ' on ' + arguments.getArg('SLAVEHOSTNAME') + ':' + str(arguments.getArg('SLAVEPORT')) + ' as user ' + arguments.getArg('REPUSER') ])
                msLog.writeInfo([ 'connected to database ' + arguments.getArg('MASTERDBNAME') + ' on ' + arguments.getArg('MASTERHOSTNAME') + ':' + str(arguments.getArg('MASTERPORT')) + ' as user ' + arguments.getArg('REPUSER') ])
                msLog.writeDebug([ 'memory usage (MB) ' + str(proc.memory_info().vms / 1048576) ])
                
                # We hang on to old dump files only until we restart manage-subs.py, then we deleted them.
                cleanAllSavedFiles(arguments.getArg('dumpDir'), msLog)

                # Read the requests table into memory.
                reqTable = ReqTable(arguments.getArg('kSMreqTable'), timedelta(seconds=int(arguments.getArg('kSMreqTableTimeout'))), connSlave, msLog)
                msLog.writeInfo([ 'Requests table ' + arguments.getArg('kSMreqTable') + ' loaded from ' + arguments.getArg('SLAVEHOSTNAME') + '.'])
                msLog.writeDebug([ 'Memory usage (MB) ' + str(proc.memory_info().vms / 1048576) + '.' ])
                
                # spawn CGI handler - will accept connections from request-subs.py (CGI)
                dispatcher = RequestDispatcher(reqtable=reqTable, serverhost=arguments.getArg('SM_SERVER'), listenport=arguments.getArg('SM_SERVER_PORT'), log=msLog)
                setattr(th, 'dispatcher', dispatcher)
                dispatcher.start()

                # Main dispatch loop. When a SIGINT (ctrl-c), SIGTERM, or SIGHUP is received, the 
                # terminator context manager will be exited.

                # log every 60 seconds                    
                ilogger = IntervalLogger(msLog, timedelta(seconds=60))
                firstIter = True
                while True:
                    ######################
                    ## Pending Requests ##
                    ######################
                    
                    # Deal with pending requests first. If a request has been pending too long, then delete the request now
                    # and send an error message back to the requestor. This will clean-up requests that died somewhere
                    # while being processed.
                    reqTable.acquireLock()
                    try:
                        reqsPending = reqTable.getPending()
                                                    
                        for areq in reqsPending:
                            ilogger.writeInfo([ 'Found a pending request: ' + areq.dump(False) + '.'])

                            timeNow = datetime.now(areq.starttime.tzinfo)
                            if timeNow > areq.starttime + reqTable.getTimeout():
                                msLog.writeError(['The processing of request ' + str(areq.requestid) + ' timed-out.'])
            
                                # Kill the Worker thread (if it exists). The Worker will clean-up subscription-management files, 
                                # like client.new.lst.
                                msLog.writeError([ 'Setting status to E (request ' + str(areq.requestid) + ' timed-out).' ])
                                areq.setStatus('E', 'Processing timed-out.')
                                areq.stopWorker(wait=False)
                            else:
                                if not areq.hasWorker() or not areq.getWorker().isAlive():
                                    # Spawn a worker to handle pending request that has no worker.
                                    newSite = clientIsNew(arguments, connMaster, areq.client)
                                
                                    countDown = 5
                                    while countDown > 0:
                                        # Must have request-table lock. The worker thread is going to check the request status to
                                        # ensure it is pending. But we don't want to do that until the main thread has set it
                                        # to pending.
                                        Worker.lock.acquire()
                                        try:
                                            if len(Worker.tList) < Worker.maxThreads:
                                                msLog.writeInfo([ 'instantiating a worker thread for request ' + str(areq.requestid) + ' for client ' + areq.client + ' (LOST WORKER RECOVERY)' ])
                                                if not areq.hasWorker():
                                                    doClean = arguments.getArg('fclean')
                                                else:
                                                    # The worker thread died. Remove old Worker object.
                                                    areq.stopWorker(wait=False)
                                                    doClean = False
                                                # XXX - newThread already sets status to P
                                                areq.setWorker(Worker.newThread(areq, newSite, arguments, connMaster, connSlave, msLog, doClean, proc))
                                                if doClean:
                                                    reqTable.setStatus([ areq.requestid ], 'P') # The new request is now being processed. If there was an issue creating the new thread, an exception will have been raised, and we will not execute this line.
                                                break # The finally clause will ensure the Worker lock is released.
                                        finally:
                                            Worker.lock.release()
                
                                        # Worker.eventMaxThreads.wait() # Wakes up when a worker thread completes. Don't use.
                                        # Poll instead so that we can get other things done while we are waiting for a thread 
                                        # to become available.
                                        time.sleep(1)
                                        countDown -= 1

                        # Completed requests, success and failures alike are handled by request-subs.py. There is nothing to do here.
                    finally:
                        reqTable.releaseLock()                    
                    
                    ##########################
                    ## Errored-out Requests ##
                    ##########################
                        
                    # Remove old errored-out requests. The records of these errored-out requests should be
                    # available for a while so that request-subs.py can pass the error along to the requestor,
                    # but after a time-out, the errored-out request record should be deleted.
                    reqTable.acquireLock()
                    try:
                        reqsInError = reqTable.getInError()
                        expiredAndInError = []

                        for areq in reqsInError:
                            ilogger.writeInfo([ 'found an errored-out request: ' + areq.dump(False) ])
                            timeNow = datetime.now(areq.starttime.tzinfo)
                            if timeNow > areq.starttime + reqTable.getTimeout():
                                msLog.writeInfo([ 'time-out: deleting the record for errored-out request: (' + areq.dump(False) + ')' ])
                                expiredAndInError.append(areq.requestid)
                                
                        if len(expiredAndInError) > 0:
                            reqTable.deleteRequests(expiredAndInError)
                            
                    finally:
                        reqTable.releaseLock()
                            
                    ##################
                    ## New Requests ##
                    ##################
                    
                    # If the requestor is making a subscription request, but there is already a request
                    # pending for that requestor, then send an error message back to the requestor 
                    # that will cause the client code to exit without modifying any client state. This is 
                    # actually already handled in request-subs.py, but handle it here too (don't send a
                    # message to the client - manage-subs.py is not communicating directly to the client)
                    # by killing the request (that should not have been allowed to get to the manager
                    # in the first place).
                    
                            
                    # no need to hold the reqtable lock since the main thread is the only thread processing these new
                    # requests at this point
                    reqsNew = reqTable.getNew()

                    for areq in reqsNew:
                        msLog.writeInfo([ 'Found a new request: (' + areq.dump(False) + ').' ])
                    
                        timeNow = datetime.now(areq.starttime.tzinfo)
                        if timeNow > areq.starttime + reqTable.getTimeout():
                            msLog.writeError(['The processing of request ' + str(areq.requestid) + ' timed-out.'])                
                            areq.setStatus('E', 'Processing timed-out.')
                        else:
                            # Start a Worker thread to handle the subscription request.
                            if len(reqTable.getProcessing(areq.client)) > 0:
                                # Set the status of this pending request to an error code.
                                msLog.writeError([ 'Server busy processing request for client ' + areq.client + ' already.' ])
                                areq.setStatus('E', 'Server busy processing request for client ' + areq.client + ' already.')
                            else:
                                # At this point, the wrapper CGI, request-subs.py, has ensured that all requests in 
                                # the request table are valid. 
                                newSite = clientIsNew(arguments, connMaster, areq.client)
    
                                # Process the request, but time-out after five seconds so we don't get stuck here
                                # waiting for a slot to open up. If all slots are full and could take a long time to process,
                                # then we could get stuck in this loop for a long time if not for a time-out. It
                                # would be better do another iteration of the outer, main loop, wherein
                                # lies code that can finalize pending request.
                                countDown = 5
                                while countDown > 0:
                                    # Must have request-table lock. The worker thread is going to check the request status to
                                    # ensure it is pending. But we don't want to do that until the main thread has set it
                                    # to pending.
                                    Worker.lock.acquire()
                                    try:
                                        if len(Worker.tList) < Worker.maxThreads:
                                            msLog.writeInfo([ 'instantiating a worker thread for request ' + str(areq.requestid) + ' for client ' + areq.client ])
                                            Worker.newThread(areq, newSite, arguments, connMaster, connSlave, msLog, arguments.getArg('fclean'), proc)
                                            break # the finally clause will ensure the Worker lock is released
                                    finally:
                                        Worker.lock.release()
                
                                    # Worker.eventMaxThreads.wait() # Wakes up when a worker thread completes. Don't use.
                                    # Poll instead so that we can get other things done while we are waiting for a thread 
                                    # to become available.
                                    time.sleep(1)
                                    countDown -= 1
                    

                    # Refresh the requests table. This could result in new requests being added to the table, or completed requests being
                    # deleted. 
                    # XXX - problem: if setstatus was not called in this transaction, then if the worker thread has changed the status
                    # refresh() will not see change because the worker changes the status in its own transaction, and the main thread
                    # is using old transaction to get status
                    reqTable.acquireLock()
                    
                    # end the current transaction so that the refresh() will start a new transaction and pick-up new requests;
                    # the changes committed will be changes to the status/errmsg columns of requests, and deletions of old 
                    # errored-out requests 
                    connSlave.commit()

                    try:
                        reqTable.refresh()
                    finally:
                        reqTable.releaseLock()
            
                    firstIter = False

                    # if we wrote during this iteration, update the last-print time
                    ilogger.updateLastWriteTime()

                    time.sleep(1)                
            
            connSlave.close()
            connMaster.close()
        if thContainer[8] == RV_TERMINATED:
            pass
    except InstanceRunning:
        msg = 'an instance of the server is already running; the current instance will be terminated'
        msLog.writeError([ msg ])
        print(msg)
        rv = RV_TERMINATED
    except FileNotFoundError as exc:
        type, value, traceback = sys.exc_info()
        if msLog:
            msLog.writeError([ value ])
        else:
            print(value, file=sys.stderr)
    except Exception as exc:
        if len(exc.args) == 2:
            eType, eMsg = exc.args
            
            if eType == 'drmsParams':
                rv = RV_DRMSPARAMS
            elif eType == 'args':
                rv = RV_ARGS
            elif eType == 'clientConfig':
                rv = RV_CLIENTCONFIG
            elif eType == 'reqtableWrite':
                rv = RV_REQTABLEWRITE
            elif eType == 'reqtableRead':
                rv = RV_REQTABLEREAD
            elif eType == 'invalidArgument':
                rv = RV_INVALIDARGUMENT
            elif eType == 'unknownRequestid':
                rv = RV_UNKNOWNREQUESTID
            elif eType == 'getSubList':
                rv = RV_GETSUBLIST
            elif eType == 'dbResponse':
                rv = RV_DBRESPONSE
            elif eType == 'dbCmd':
                rv = RV_DBCOMMAND
            elif eType == 'lock':
                rv = RV_LOCK
            else:
                raise
            
            if msLog:
                msLog.writeError([ eMsg ])
            else:
                print(eMsg, file=sys.stderr)
        else:
            rv = RV_UKNOWNERROR
            import traceback
            if msLog:
                msLog.writeError([ traceback.format_exc(5) ])
                
    if msLog:
        if rv == RV_TERMINATED:
            msLog.writeInfo([ 'Normal shut-down complete.' ])
        msLog.close()
    logging.shutdown()

    sys.exit(rv)
