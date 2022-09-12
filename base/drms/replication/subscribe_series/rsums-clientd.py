#!/usr/bin/env python3

# The production user runs rsums-clientd.py in the background. The script needs read/write access to
# the Remote SUMS requests table (drms.rs_requests) and the capture table (drms.ingested_sunums). It also
# needs read-only access to the SUMS partition alloc table - it connects to the SUMS DB as the read-only user
# identified with by the SUMS_READONLY_DB_USER user. Ensure that the SUMS DB has this user and that this
# user has the appropriate permissions:
#
# kasi2_sums=# CREATE ROLE readonlyuser;
# CREATE ROLE
# kasi2_sums=# GRANT CONNECT ON DATABASE kasi2_sums TO readonlyuser;
# GRANT
# kasi2_sums=# GRANT USAGE ON SCHEMA public TO readonlyuser;
# GRANT
# kasi2_sums=# GRANT SELECT ON sum_partn_alloc TO readonlyuser;
# GRANT
# kasi2_sums=# ALTER USER readonlyuser WITH LOGIN;
# ALTER ROLE

import sys

if sys.version_info < (3, 4):
    raise Exception("you must run the 3.4 release, or a more recent release, of Python")

import os
import pwd
import logging
import time
from datetime import datetime, timedelta
import argparse
import signal
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../libs/py'))
from drmsCmdl import CmdlParser
from drmsLock import DrmsLock


# return code
RV_SUCCESS = 0
RV_INITIALIZATION = 1
RV_DRMSPARAMS = 2
RV_ARGS = 3
RV_LOG_INITIALIZATION = 4
RV_DBCONNECTION = 5
RV_DBCOMMAND = 6
RV_DBCOMMAND_RESULT = 7
RV_TERMINATED = 8

CAPTURE_TABLE = 'drms.ingested_sunums'

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
BEGIN
    IF EXISTS (SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'drms' AND c.relname = 'ingested_sunums') THEN
      IF (TG_OP='INSERT' AND new.sunum > 0) THEN
        IF EXISTS (SELECT 1 FROM drms.ingested_sunums WHERE sunum = new.sunum) THEN
          RETURN NULL;
        END IF;
        INSERT INTO drms.ingested_sunums (sunum, starttime) VALUES (new.sunum, clock_timestamp());
      END IF;
    END IF;

  RETURN NEW;
END
$capturesunumtrig$ LANGUAGE plpgsql;
"""


class RsumsDrmsParams(DRMSParams):

    def __init__(self):
        super(RsumsDrmsParams, self).__init__()

    def get(self, name):
        val = super(RsumsDrmsParams, self).get(name)

        if val is None:
            raise ParamsException('unknown DRMS parameter: ' + name + '.')
        return val


def terminator(*args):
    # Raise the SystemExit exception (which will be caught by the __exit__() method below).
    sys.exit(0)

class TerminationHandler(object):
    class Break(Exception):
        """break out of the TerminationHandler context block"""

    def __new__(cls, thContainer):
        return super(TerminationHandler, cls).__new__(cls)

    def __init__(self, thContainer):
        self.container = thContainer
        self.arguments = thContainer[0]
        self.pidStr = thContainer[1]
        self.log = thContainer[2]
        self.pendingRequests = thContainer[4]

        self.lockFile = os.path.join(self.arguments.DRMS_LOCK_DIR, 'rsums-client.lck')

        self.dbname = self.arguments.dbdatabase
        self.dbuser = self.arguments.dbuser
        self.dbhost = self.arguments.dbhost
        self.dbport = self.arguments.dbport

        self.dbnameSums = self.dbname + '_sums'
        self.dbuserSums = self.arguments.SUMS_READONLY_DB_USER # connect to SUMS as SUMS_READONLY_DB_USER user
        self.dbhostSums = self.arguments.SUMS_DB_HOST
        self.dbportSums = self.arguments.SUMPGPORT

        self.conn = None
        self.connSums = None

        self.savedSignals = None

        super(TerminationHandler, self).__init__()

    def __enter__(self):
        self.enableInterrupts()

        # Acquire locks.
        self.rsLock = DrmsLock(self.lockFile, self.pidStr)
        self.rsLock.acquireLock()

        # Make main DB connection to RS database. We also have to connect to the SUMS database, so connect to that too.
        # The connections are NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        # Do this instead of using BEGIN and END/COMMIT statements, cuz I don't know if the psycopg2/libpq interaction
        # supports this properly.
        try:
            self.openRsConn()
            self.openRsConnSums()
        except DBConnectionException() as exc:
            self.log.writeError([ exc.args[0] ])
            self.__exit__(*sys.exc_info()) # try cleaning up (will return False since this is not a self.Break)
            raise InitializationException('failure initializing client instance')
        except:
            raise InitializationException('failure initializing client instance')

        return self

    # Normally, __exit__ is called if an exception occurs inside the with block. And since SIGINT is converted
    # into a KeyboardInterrupt exception, it will be handled by __exit__(). However, SIGTERM will not -
    # __exit__() will be bypassed if a SIGTERM signal is received. Use the signal handler installed in the
    # __enter__() call to handle SIGTERM.
    def __exit__(self, etype, value, traceback):
        if etype is not None:
            # If the context manager was exited without an exception, then etype is None
            import traceback
            self.log.writeDebug([ traceback.format_exc(5) ])

        if etype == SystemExit:
            self.log.writeInfo([ 'termination signal handler called' ])
            self.container[3] = RV_TERMINATED

        print('Remote SUMS client shutting down (this could take up to 10 minutes)...')
        self.finalStuff()

        # Clean up lock
        try:
            self.rsLock.releaseLock()
            self.rsLock.close()
            self.rsLock = None
        except IOError:
            pass

        self.log.writeDebug([ 'exiting TerminationHandler' ])

        if etype == self.Break:
            self.log.writeInfo([ 'completed generating set-up script' ])
            self.container[3] = RV_SUCCESS
            return True

    def saveSignal(self, signo, frame):
        if self.savedSignals == None:
            self.savedSignals = []

        self.savedSignals.append((signo, frame))
        self.log.writeDebug([ 'saved signal ' +  signo ])

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
                terminator(*signalReceived)

        self.savedSignals = None

    def finalStuff(self):
        # clean up status-E and status-C requests from drms.rs_requests that did not get cleaned up prior to interruption;
        # these are the pending requests at the time that the interrupt happened; this script is single-threaded, so
        # no other thread can modify self.pendingRequests while this method is executing
        requestsTable = self.arguments.rtable

        pendingReqIDs = self.pendingRequests.keys() # an array of strings (self.pendingRequests is a reference to a global)
        pendingRequestsStr = ','.join(pendingReqIDs)

        if len(pendingReqIDs) > 0:
            starttime = datetime.now()
            self.log.writeInfo([ 'waiting for Remote SUMS to finish processing pending SUs' ])
            while True:
                # wait until rsumsd.py has completely processed all requests to either completion or error
                with self.conn.cursor() as cursor:
                    cmd = 'SELECT count(*) FROM ' + requestsTable + ' WHERE requestid IN (' + pendingRequestsStr + ')' + " AND status != 'E' AND status != 'C'"

                    try:
                        cursor.execute(cmd)
                    except psycopg2.Error as exc:
                        # handle database-command errors.
                        import traceback
                        raise DBCommandException(traceback.format_exc(5))

                    rows = cursor.fetchall()
                    if len(rows) > 1:
                        raise DBCommandResultException('unexpected number of rows returned')

                    count = rows[0][0]
                    self.log.writeInfo([ 'still ' + str(count) + ' pending requests in the requests table' ])
                    if (count == 0):
                        break

                    # don't wait forever
                    if datetime.now(starttime.tzinfo)  > starttime + timedelta(minutes=10):
                        self.log.writeInfo([ 'timed-out waiting for Remote SUMS to finish processing pending SUs' ])
                        break

                    time.sleep(2)

        if len(pendingReqIDs) > 0:
            # delete the requests that DID complete (but only the requests that rsums-clientd.py issued - those in pendingReqIDs)
            count = 0
            with self.conn.cursor() as cursor:
                cmd = 'SELECT requestid FROM ' + requestsTable + ' WHERE requestid IN (' + pendingRequestsStr + ')' + " AND (status = 'E' OR status = 'C')"

                try:
                    self.log.writeInfo([ 'executing SQL: ' + cmd ])
                    cursor.execute(cmd)
                except psycopg2.Error as exc:
                    # handle database-command errors.
                    import traceback
                    raise DBCommandException(traceback.format_exc(5))

                count = cursor.rowcount
                rows = cursor.fetchall()
                idsToDel = [ row[0] for row in rows ]
                idsToDelStr = ','.join([ str(id) for id in idsToDel ])

                if count > 0:
                    self.log.writeInfo([ str(count) + ' completed Remote SUMS requests ' + idsToDelStr + ' are being removed from the requests table' ])
                    cmd = 'DELETE FROM ' + requestsTable + ' WHERE requestid IN (' + idsToDelStr + ')'

                    try:
                        self.log.writeInfo([ 'executing SQL: ' + cmd ])
                        cursor.execute(cmd)
                        count = cursor.rowcount
                    except psycopg2.Error as exc:
                        # handle database-command errors.
                        import traceback
                        raise DBCommandException(traceback.format_exc(5))

                    self.log.writeInfo([ 'removed ' + str(count) + ' completed Remote SUMS requests' ])

                    # remove from drms.ingested_sunums the SUNUMs of the completed requests identified and removed above
                    for requestID in idsToDel:
                        csus = self.pendingRequests[str(requestID)]

                        with self.conn.cursor() as cursor:
                            sunumsStr = ','.join([ str(csu.getSUNUM()) for csu in csus ])
                            cmd = 'DELETE FROM ' + CAPTURE_TABLE + ' WHERE sunum IN ' + '(' + sunumsStr + ')'

                            try:
                                self.log.writeInfo([ 'executing SQL: ' + cmd ])
                                cursor.execute(cmd)
                            except psycopg2.Error as exc:
                                # handle database-command errors.
                                import traceback
                                raise DBCommandException(traceback.format_exc(5))

                        self.log.writeInfo([ 'removed SUs ' + sunumsStr + ' from sunum capture table' ])

        self.log.writeInfo([ 'closing DB connections' ])
        self.closeRsConnSums()
        self.closeRsConn()

        self.log.writeInfo([ 'terminating logging' ])
        self.log.flush()

    def closeRsConn(self, commit=True):
        if self.conn:
            if commit:
                self.conn.commit()
            else:
                self.conn.rollback()

            self.conn.close()
            self.conn = None

    def closeRsConnSums(self, commit=True):
        if self.connSums:
            if commit:
                self.connSums.commit()
            else:
                self.connSums.rollback()
            self.connSums.close()
            self.connSums = None

    def openRsConn(self):
        if self.conn:
            raise DBConnectionException('cannot open DRMS database connection; connection already exists')

        try:
            self.conn = psycopg2.connect(database=self.dbname, user=self.dbuser, host=self.dbhost, port=self.dbport)
            self.log.writeInfo([ 'connected to DRMS database ' + self.dbname + ' on ' + self.dbhost + ':' + str(self.dbport) + ' as user ' + self.dbuser ])
        except psycopg2.DatabaseError as exc:
            self.closeRsConn()
            self.container[3] = RV_DBCONNECTION
            raise DBConnectionException('unable to connect to DRMS database')
        except psycopg2.Error as exc:
            self.closeRsConn()
            self.container[3] = RV_DBCOMMAND
            raise DBConnectionException(exc.diag.message_primary)

    def openRsConnSums(self):
        if self.connSums:
            raise DBConnectionException('cannot open SUMS database connection; connection already exists')

        try:
            self.connSums = psycopg2.connect(database=self.dbnameSums, user=self.dbuserSums, host=self.dbhostSums, port=self.dbportSums)
            self.log.writeInfo([ 'connected to SUMS database (read-only)' + self.dbnameSums + ' on ' + self.dbhostSums + ':' + str(self.dbportSums) + ' as user ' + self.dbuserSums ])
        except psycopg2.DatabaseError as exc:
            self.closeRsConnSums()
            self.container[3] = RV_DBCONNECTION
            raise DBConnectionException('unable to connect to SUMS database')
        except psycopg2.Error as exc:
            self.closeRsConnSums()
            self.container[3] = RV_DBCOMMAND
            raise DBConnectionException(exc.diag.message_primary)


    def rsConn(self):
        return self.conn

    def rsConnSums(self):
        return self.connSums


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
            raise ArgsException('attempt to set an argument that already exists: ' + name)

    def set(self, name, value):
        # Sets attribute, even if it exists already.
        setattr(self, name, value)

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)

    def setDictArgs(self, dict):
        for key, val in dict.items():
            self.setArg(key, val)

    def getArg(self, name):
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise ArgsException('unknown argument: ' + name + '.')

    def get(self, name):
        # None is returned if the argument does not exist.
        return getattr(self, name, None)

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


class RSException(Exception):

    def __init__(self, msg):
        super(RSException, self).__init__(msg)

class InitializationException(RSException):

     def __init__(self, msg):
        super(InitializationException, self).__init__(msg)
        self.rv = RV_INITIALIZATION

class ParamsException(RSException):

    def __init__(self, msg):
        super(ParamsException, self).__init__(msg)
        self.rv = RV_DRMSPARAMS

class ArgsException(RSException):

    def __init__(self, msg):
        super(ArgsException, self).__init__(msg)
        self.rv = RV_ARGS

class LogException(RSException):

    def __init__(self, msg):
        super(LogException, self).__init__(msg)
        self.rv = RV_LOG_INITIALIZATION

class DBConnectionException(RSException):

    def __init__(self, msg):
        super(DBConnectionException, self).__init__(msg)
        self.rv = RV_DBCONNECTION

class DBCommandException(RSException):

    def __init__(self, msg):
        super(DBCommandException, self).__init__(msg)
        self.rv = RV_DBCOMMAND

class DBCommandResultException(RSException):

    def __init__(self, msg):
        super(DBCommandResultException, self).__init__(msg)
        self.rv = RV_DBCOMMAND_RESULT

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

class CapturedSU(object):
    __sus = set()

    def __init__(self, **kwargs):
        self.__su = kwargs['su']
        self.__starttime = kwargs['starttime']

    def getSUNUM(self):
        return self.__su

    def getST(self):
        return self.__starttime

    def __eq__(self, other):
        if isinstance(other, type(self).__name__):
            return self.__su == other.__su
        else:
            return False

    def __hash__(self):
        return hash((self.__su))

    def isPending(self):
        if self in __sus:
            return True
        return False

    def add(self):
        self.__sus.add(self)

    def remove(self):
        self.__sus.remove(self)

    @classmethod
    def getCSUs(cls):
        csus = ()

        for csu in cls.__sus:
            csus = csus + (csu,)

        return csus # tuple

    @classmethod
    def getSUNUMs(cls):
        sunums = () # immutable

        for csu in cls.__sus:
            sunums = sunums + (csu.__su,)

        return sunums # tuple

# must communicate with SUMS database
def getFailedSUs(suList, conn):
    failedSUs = []

    try:
        with conn.cursor() as cursor:
            cmd = 'SELECT ds_index FROM sum_partn_alloc WHERE ds_index IN (' + ','.join([ str(sunum) for sunum in suList ]) + ')'
            sus = set()

            try:
                cursor.execute(cmd)
                rows = cursor.fetchall()

                for row in rows:
                    sus.add(row[0])
            except psycopg2.Error as exc:
                # handle database-command errors.
                import traceback
                raise DBCommandException("error executing DB command '" + cmd + "'" + '\n' + traceback.format_exc(5))

        # end SUMS DB transaction
    except psycopg2.Error as exc:
        import traceback
        raise DBCommandException(traceback.format_exc(5))

    for su in suList:
        if su not in sus:
            failedSUs.append(su)

    return failedSUs

if __name__ == "__main__":
    rv = RV_SUCCESS
    log = None

    try:
        rsumsDrmsParams = RsumsDrmsParams()

        parser = CmdlParser(usage='%(prog)s [ -dht ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user>] [--loglevel=<verbosity level>] [ --logfile=<log-file name> ]  [ --capturetable=<capture db table> ] [ --requesttable=<RS requests db table> ] [ --setup [ --capturesetup ] ]')
        parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains both the SUNUM-capture table and the Remote SUMS requests table.', metavar='<db host machine>', dest='dbhost', default=rsumsDrmsParams.get('RS_DBHOST'))
        parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database that contains the capture and requests tables.', metavar='<db host port>', dest='dbport', type=int, default=rsumsDrmsParams.get('RS_DBPORT'))
        parser.add_argument('-N', '--dbname', help='The name of the database that contains the capture and requests tables.', metavar='<db name>', dest='dbdatabase', default=rsumsDrmsParams.get('RS_DBNAME'))
        parser.add_argument('-U', '--dbuser', help='The name of the database user account.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
        parser.add_argument('-l', '--loglevel', help='Specifies the amount of logging to perform. In order of increasing verbosity: critical, error, warning, info, debug', dest='loglevel', action=LogLevelAction, default=logging.ERROR)
        parser.add_argument('-L', '--logfile', help='The file to which logging is written.', metavar='<log file>', dest='logfile', default=os.path.join(rsumsDrmsParams.get('RS_LOGDIR'), 'rsums-client-' + datetime.now().strftime('%Y%m%d') + '.log'))
        parser.add_argument('-r', '--requesttable', help='The database table in which remote SUMS requests are stored.', metavar='<requests table>', dest='rtable', default=rsumsDrmsParams.get('RS_REQUEST_TABLE'))
        parser.add_argument('-t', '--timeout', help='The maximum amount of time, in minutes, to attempt to download an SU.', metavar='<timeout>', dest='timeout', type=int, default=rsumsDrmsParams.get('RSCLIENT_TIMEOUT'))
        parser.add_argument('-s', '--setup', help='Create an initialization SQL script to be run by the remote-sums-client database user.', dest='setup', action='store_true', default=False)
        parser.add_argument('-C', '--capturesetup', help='Create the capture table and the associated capture trigger function.', dest='capsetup', action='store_true', default=False)
        parser.add_argument('-S', '--seriessetup', help='Add a capture trigger to one or more series.', dest='seriessetup', action='append', default=[])

        arguments = Arguments(parser)

        # add all drmsParams to arguments
        arguments.setArg('RS_MAXTHREADS', int(rsumsDrmsParams.get('RS_MAXTHREADS')))
        arguments.setArg('DRMS_LOCK_DIR', rsumsDrmsParams.get('DRMS_LOCK_DIR'))
        arguments.setArg('SUMS_READONLY_DB_USER', rsumsDrmsParams.get('SUMS_READONLY_DB_USER'))
        arguments.setArg('SUMS_DB_HOST', rsumsDrmsParams.get('SUMS_DB_HOST'))
        arguments.setArg('SUMPGPORT', int(rsumsDrmsParams.get('SUMPGPORT')))

        pid = os.getpid()

        # Create/Initialize the log file.
        try:
            logFile = arguments.logfile
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            log = Log(logFile, arguments.loglevel, formatter)
        except exc:
            raise LogException('unable to initialize logging')

        log.writeCritical([ 'starting rsums-clientd.py server' ])
        arguments.dump(log)

        # no dupes
        pendingRequests = {}
        pendingSUs = set()

        thContainer = [ arguments, str(pid), log, rv, pendingRequests ]

        with TerminationHandler(thContainer) as th:
            if arguments.setup:
                print('-- run this script as a DB superuser')
                if arguments.capsetup:
                    print(CAPTURE_SUNUMS_SQL)

                if len(arguments.seriessetup) > 0:
                    for series in arguments.seriessetup:
                        print('DROP TRIGGER IF EXISTS capturesunumtrig ON ' + series.lower() + ';')
                        print('CREATE TRIGGER capturesunumtrig AFTER INSERT ON ' + series.lower() + ' FOR EACH ROW EXECUTE PROCEDURE drms.capturesunum();\n')

                print('GRANT ALL ON ' + CAPTURE_TABLE + ' TO ' + arguments.dbuser)
                raise TerminationHandler.Break

            # we are connected to both the DRMS and SUMS databases
            connSums = th.rsConnSums()


            # main loop
            loopIteration = 1
            while True:
                # re-connect to the db after some number of iterations
                if loopIteration % 100 == 0:
                    th.closeRsConnSums(True)
                    th.closeRsConn(True)
                    th.openRsConn()
                    th.openRsConnSums()

                with th.rsConn():
                    # start a transaction (transaction is committed when with block is left)
                    try:
                        with th.rsConn().cursor() as cursor:
                            requestsTable = arguments.rtable

                            # we want to keep RS_MAXTHREADS SUs pending; operate on a small chunk of SUs at one time
                            while len(CapturedSU.getCSUs()) < arguments.RS_MAXTHREADS:
                                # read 4 rows from the sunum-capture table
                                if len(CapturedSU.getCSUs()) > 0:
                                    whereClause = ' WHERE sunum NOT IN (' + ','.join([ str(sunum) for sunum in CapturedSU.getSUNUMs() ]) + ')'
                                else:
                                    whereClause = ''

                                csus = set()
                                cmd = 'SELECT sunum, starttime FROM ' + CAPTURE_TABLE + whereClause + ' LIMIT 4'
                                try:
                                    cursor.execute(cmd)
                                    rows = cursor.fetchall()
                                    if len(rows) > 4:
                                        raise DBCommandResultException('unexpected number of rows returned')

                                    for row in rows:
                                        capturedSU = CapturedSU(su=row[0], starttime=row[1])
                                        log.writeDebug([ 'adding su ' + str(capturedSU.getSUNUM()) + ' to pendingSUs list' ])
                                        csus.add(capturedSU)

                                    sunumsStr = ','.join([ str(csu.getSUNUM()) for csu in csus ])
                                except psycopg2.Error as exc:
                                    # handle database-command errors.
                                    import traceback
                                    raise DBCommandException(traceback.format_exc(5))

                                if len(rows) <= 0:
                                    # no more SUs for which to request downloads; break
                                    break
                                else:
                                    th.disableInterrupts()

                                    log.writeInfo([ 'RS server has open slots and there are SUs to download - making new SU-download requests' ])
                                    # making a new request - generate next request ID
                                    cmd = "SELECT nextval('" + requestsTable + "_seq')"

                                    try:
                                        cursor.execute(cmd)
                                        rows = cursor.fetchall()
                                        if len(rows) != 1:
                                            raise DBCommandResultException(exc.diag.message_primary)

                                        requestID = rows[0][0]
                                    except psycopg2.Error as exc:
                                        # handle database-command errors.
                                        import traceback
                                        raise DBCommandException(traceback.format_exc(5))

                                    # make the Remote SUMS request (by inserting a row into the RS requests table)
                                    cmd = 'INSERT INTO ' + requestsTable + '(requestid, dbhost, dbport, dbname, type, starttime, sunums, status) VALUES(' + str(requestID) + ", '" + arguments.dbhost + "', " + str(arguments.dbport) + ", '" + arguments.dbdatabase + "', 'M', " + 'clock_timestamp()' + ", '" + sunumsStr + "', " + "'N'" + ')'
                                    log.writeDebug([ 'running SQL: ' + cmd ])
                                    try:
                                        cursor.execute(cmd)
                                    except psycopg2.Error as exc:
                                        # handle database-command errors.
                                        import traceback
                                        raise DBCommandException(traceback.format_exc(5))

                                    log.writeInfo([ 'requested download of ' + sunumsStr + ' (id ' + str(requestID) + ')' ])

                                    # no errors, so we can update pendingSUs list
                                    for csu in csus:
                                        # put each member of the csus set into the CapturedSU.__sus class variable
                                        csu.add() # will add only if SUNUM is unique

                                    pendingRequests[str(requestID)] = csus # map to a set
                                    log.writeDebug([ 'adding request ' + str(requestID) + ' to pendingRequests list' ])

                                    th.enableInterrupts()

                            # give Remote SUMS a chance to process existing requests
                            time.sleep(1)

                            # we've got our pendingSUs all started; now monitor them for completion
                            if len(pendingRequests) > 0:
                                log.writeInfo([ 'checking on pending requests' ])

                                toDelFromRequestsTable = []
                                toDelFromCaptureTable = []
                                toDelFromPendingRequests = []
                                toDelFromPendingSUs = []

                                pendingRequestsStr = ','.join(pendingRequests.keys())
                                cmd = 'SELECT requestid, starttime, status, errmsg FROM ' + requestsTable + ' WHERE requestid IN (' + pendingRequestsStr + ')'

                                try:
                                    cursor.execute(cmd)
                                    rows = cursor.fetchall()

                                    # for a variety of reasons, items in pendingRequests could now be absent from the db response;
                                    # remove those requests from pendingRequests and log an error
                                    reqsAlive = set()

                                    for row in rows:
                                        # these are rows from the RS requests table
                                        requestID = row[0] # int
                                        starttime = row[1] # datetime.datetime
                                        status = row[2] # string
                                        errmsg = row[3] # string

                                        reqsAlive.add(requestID)
                                        log.writeDebug([ 'request ' + str(requestID) + ' is alive and pending' ])

                                        if status == 'E':
                                            # log error
                                            log.writeError([ 'error processing requestID ' +  str(requestID) ])

                                            # find out which SUs, specifically, failed
                                            failedSUs = getFailedSUs([ csu.getSUNUM() for csu in pendingRequests[str(requestID)] ], th.rsConnSums())
                                            for su in failedSUs:
                                                log.writeError([ 'error downloading SU ' +  str(su) ])

                                            toDelFromRequestsTable.append(requestID)
                                            toDelFromPendingRequests.append(requestID)
                                            # do not remove SUNUMs from the capture table; new attempts will be made to download the SU
                                            # until a timeout occurs
                                            for csu in pendingRequests[str(requestID)]:
                                                if datetime.now(csu.getST().tzinfo) > csu.getST() + timedelta(minutes=arguments.timeout):
                                                    toDelFromCaptureTable.append(csu)
                                                    log.writeWarning([ 'timed-out attempting to download ' + str(csu.getSUNUM()) + '; will not try again' ])

                                            # each request contains a unique set of SUNUMs (no SUNUM appears in more than one request)
                                            # so it is OK to remove the SUs in pendingRequests[str(requestID)] - we won't be removing
                                            # them from other unrelated requests
                                            toDelFromPendingSUs.extend(pendingRequests[str(requestID)])
                                        elif status == 'C':
                                            log.writeInfo([ 'RS server has completed processing request ' + str(requestID) ])
                                            toDelFromRequestsTable.append(requestID)
                                            toDelFromPendingRequests.append(requestID)
                                            toDelFromCaptureTable.extend(pendingRequests[str(requestID)])
                                            # each request contains a unique set of SUNUMs (no SUNUM appears in more than one request)
                                            # so it is OK to remove the SUs in pendingRequests[str(requestID)] - we won't be removing
                                            # them from other unrelated requests
                                            toDelFromPendingSUs.extend(pendingRequests[str(requestID)])
                                        elif datetime.now(starttime.tzinfo) > starttime + timedelta(minutes=30):
                                            # time-out; let the server continue to attempt to download the SU, but
                                            # the client gives up waiting and pretends that there was basically an error
                                            log.writeError([ 'time-out processing requestID ' +  str(requestID) ])

                                            for csu in pendingRequests[str(requestID)]:
                                                log.writeError([ 'time-out downloading SU ' +  str(csu.getSUNUM()) ])

                                            # do not delete the request from the requests table; do not delete the SU from
                                            # the capture table again - this way the client will attempt to download the
                                            # SU again later (at which point, it may already be downloaded)
                                            toDelFromPendingRequests.append(requestID)
                                            # each request contains a unique set of SUNUMs (no SUNUM appears in more than one request)
                                            # so it is OK to remove the SUs in pendingRequests[str(requestID)] - we won't be removing
                                            # them from other unrelated requests
                                            toDelFromPendingSUs.extend(pendingRequests[str(requestID)])

                                    # handle lost requests - do not delete from capture table so the client will try again
                                    # later
                                    for requestID in [ int(key) for key in pendingRequests.keys() ]:
                                        if requestID not in reqsAlive:
                                            log.writeError([ 'pending request ' + str(requestID) + ' lost' ])

                                            toDelFromPendingRequests.append(requestID)
                                            # each request contains a unique set of SUNUMs (no SUNUM appears in more than one request)
                                            # so it is OK to remove the SUs in pendingRequests[str(requestID)] - we won't be removing
                                            # them from other unrelated requests
                                            toDelFromPendingSUs.extend(pendingRequests[str(requestID)])
                                except psycopg2.Error as exc:
                                    # handle database-command errors.
                                    import traceback
                                    raise DBCommandException(traceback.format_exc(5))

                                th.disableInterrupts()

                                # remove from drms.rs_requests
                                if len(toDelFromRequestsTable) > 0:
                                    requestIDStr = ','.join([ str(requestID) for requestID in toDelFromRequestsTable ])
                                    cmd = 'DELETE FROM ' + requestsTable + ' WHERE requestid in (' + requestIDStr + ')'

                                    try:
                                        cursor.execute(cmd)
                                    except psycopg2.Error as exc:
                                        # handle database-command errors.
                                        import traceback
                                        raise DBCommandException(traceback.format_exc(5))

                                    log.writeInfo([ 'removed requests ' + requestIDStr + ' from RS requests table' ])

                                # remove from pendingRequests
                                if len(toDelFromPendingRequests):
                                    for requestID in toDelFromPendingRequests:
                                        del pendingRequests[str(requestID)]

                                    log.writeDebug([ 'removed ' + ','.join([ str(requestID) for requestID in toDelFromPendingRequests ]) + ' from pending requests list' ])

                                # remove from drms.ingested_sunums
                                if len(toDelFromCaptureTable) > 0:
                                    sunumsStr = ','.join([ str(csu.getSUNUM()) for csu in toDelFromCaptureTable ])
                                    cmd = 'DELETE FROM ' + CAPTURE_TABLE + ' WHERE sunum IN ' + '(' + sunumsStr + ')'

                                    try:
                                        cursor.execute(cmd)
                                    except psycopg2.Error as exc:
                                        # handle database-command errors.
                                        import traceback
                                        raise DBCommandException(traceback.format_exc(5))

                                    log.writeInfo([ 'removed SUs ' + sunumsStr + ' from sunum capture table' ])

                                # remove from pendingSUs (due to successful download, or error plus timeout on retries)
                                if len(toDelFromPendingSUs) > 0:
                                    for csu in toDelFromPendingSUs:
                                        csu.remove()

                                    log.writeDebug([ 'removed ' + ','.join([ str(csu.getSUNUM()) for csu in toDelFromPendingSUs ]) + ' from pending SUs list' ])

                                th.rsConn().commit()
                                th.enableInterrupts()
                    except psycopg2.Error as exc:
                        import traceback
                        raise DBCommandException(traceback.format_exc(5))

                # end of DRMS DB transaction
                loopIteration += 1
            # leaving termination-handler block
        rv = thContainer[3]
    except RSException as exc:
        if log:
            log.writeError([ exc.args[0] ])
        else:
            print(exc.args[0])

        rv = exc.rv

    if log:
        log.close()
    logging.shutdown()

    sys.exit(rv)
