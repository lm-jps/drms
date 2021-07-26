#!/usr/bin/env python3

# For each partition, a single scrubber is assigned. Each scrubber identifies, with a single SUFinder, a set of SU directories
# to delete.

import asyncio
from ctypes import c_double
from multiprocessing import Process as MpProcess, Lock as MpLock
from multiprocessing.sharedctypes import Value as CtypeValue
from os import getpid, statvfs
from os.path import exists as path_exists, join as path_join, split as path_split
from shlex import quote
from shutil import rmtree as sh_rmtree
import signal
from threading import Event, Timer
from sys import exit

from drms_parameters import DRMSParams, DPMissingParameterError
from drms_utils import Arguments as Args, CmdlParser, Error as DRMSError, ErrorCode as DRMSErrorCode, DrmsLock, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction

STEWARD_LOCK_FILE = '.sums_steward_lock'
SUM_MAIN = 'sum_main'
PARTITION_TABLE = 'sum_partn_alloc'
PARTITION_TABLE_WD = 'wd'
PARTITION_TABLE_EFFECTIVE_DATE = 'effective_date'
DEFAULT_LOG_FILE = 'sums_steward_log.txt'

class ErrorCode(DRMSErrorCode):
    PARAMETERS = 1, 'failure locating DRMS parameters'
    ARGUMENTS = 2, 'bad arguments'
    SUBPROCESS = 3, 'subprocess'
    FILE_STATUS = 4, 'file status'
    LOGGING = 5, 'logging'
    TERMINATION = 6, 'termination handler'

class ParametersError(DRMSError):
    _error_code = ErrorCode(ErrorCode.PARAMETERS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class ArgumentsError(DRMSError):
    _error_code = ErrorCode(ErrorCode.ARGUMENTS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class SubprocessError(DRMSError):
    _error_code = ErrorCode(ErrorCode.SUBPROCESS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class FileStatusError(DRMSError):
    _error_code = ErrorCode(ErrorCode.FILE_STATUS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class LoggingError(DRMSError):
    _error_code = ErrorCode(ErrorCode.LOGGING)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class TerminationError(DRMSError):
    _error_code = ErrorCode(ErrorCode.TERMINATION)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)


class TerminationHandler(object):
    def __new__(cls, **kwargs):
        return super(TerminationHandler, cls).__new__(cls)

    def __init__(self, **kwargs):
        self._lock_file = kwargs['lock_file']
        self._log = kwargs['log']
        self._pid = kwargs['pid']

        self._ss_lock = None
        self._saved_signals = None

        super(TerminationHandler, self).__init__()

    def __enter__(self):
        self.enable_interrupts()

        # acquire file lock
        self._ss_lock = DrmsLock(self._lock_file, str(self._pid))
        self._ss_lock.acquireLock()

        return self

    # __exit__() is called if an EXCEPTION occurs inside the with block; since SIGINT is converted
    # into a KeyboardInterrupt exception, it would be handled by __exit__(); however, SIGTERM and SIGHUP are not converted
    # into an exceptions, so __exit__() would normally not execute; to ensure that __exit__() is called
    # for all three signals, we need to install signal handlers for SIGTERM and SIGHUP
    def __exit__(self, exc_type, exc, traceback):
        self._log.write_debug([ f'[ TerminationHandler.__exit__ ]' ])
        if exc_type is not None:
            # if the context manager was exited without an exception, then etype is None;
            # the context manager was interrupted by an exception
            self._log.write_error([ f'[ TerminationHandler.__exit__ ] {exc_type.__name__} (LINE {str(traceback.tb_lineno)}): {str(exc)}' ])

        message = f'SU Steward is shutting down...'
        self._log.write_info([ f'[ TerminationHandler.__exit__ ] {message}' ])
        self._final_stuff()
        message = f'...and done'
        self._log.write_info([ f'[ TerminationHandler.__exit__ ] {message}' ])

        # clean up lock
        try:
            self._ss_lock.releaseLock()
            self._ss_lock.close()
            self._ss_lock = None
        except IOError:
            pass

        if exc_type == SystemExit:
            # the context manager was interrupted by either SIGINT, SIGTERM, or SIGHUP (or a call to sys.exit(), but that
            # should not be called anywhere in the context)
            self._log.write_info([ f'[ TerminationHandler.__exit__ ] exiting termination handler normally' ])
            return True # do not propagate SystemExit
        else:
            self._log.write_info([ f'[ TerminationHandler.__exit__ ] exiting termination handler with exception' ])
            raise TerminationError(msg='termination context manager interrupted abnormally')
            # exception will propagate

    def _terminator(self, signo, frame):
        # raise the SystemExit exception (which will be handled by the __exit__() method)
        self._log.write_info([ f'[ TerminationHandler._terminator] handling signal {str(signo)}' ])

        # cancel timer
        th.timer.cancel()
        self._log.write_info([ f'[ TerminationHandler._terminator] cancelled main-loop timer' ])

        # unblock wait() on loop event
        set_main_loop_event(th.loop_event)
        self._log.write_info([ f'[ TerminationHandler._terminator] set main-loop event (to unblock a wait() on the event)' ])
        exit(0)

    def _save_signal(self, signo, frame):
        if self._saved_signals == None:
            self._saved_signals = []

        self._saved_signals.append((signo, frame))
        self._log.write_debug([ f'[ TerminationHandler._save_signal] saved signal {signo}' ])

    def disable_interrupts(self):
        self._log.write_debug([ f'[ TerminationHandler.disable_interrupts]' ])
        signal.signal(signal.SIGINT, self._save_signal)
        signal.signal(signal.SIGTERM, self._save_signal)
        signal.signal(signal.SIGHUP, self._save_signal)

    def enable_interrupts(self):
        self._log.write_debug([ f'[ TerminationHandler.enable_interrupts]' ])
        signal.signal(signal.SIGINT, self._terminator)
        signal.signal(signal.SIGTERM, self._terminator)
        signal.signal(signal.SIGHUP, self._terminator)

        if self._saved_signals is not None:
            for signal_received in self._saved_signals:
                self._log.write_debug([ f'[ TerminationHandler.enable_interrupts] calling TerminationHandler._terminator with signal {str(signal_received[0])}' ])
                self._terminator(*signal_received)

        self._saved_signals = None

    def _final_stuff(self):
        msg = f'sums_steward (pid {str(self._pid)}) exiting now'
        self._log.write_info([ f'[ TerminationHandler._final_stuff ] {msg}' ])
        print(msg)


class Arguments(Args):
    _arguments = None

    @classmethod
    def get_arguments(cls, *, program_args, drms_params):
        if cls._arguments is None:
            try:
                sums_log_dir = drms_params.get_required('SUMLOG_BASEDIR')
                lock_file_dir = drms_params.get_required('DRMS_LOCK_DIR')
                db_host = drms_params.get_required('SERVER')
                db_port = int(drms_params.get_required('SUMPGPORT'))
                db_user = drms_params.get_required('SUMS_MANAGER')
                db_name = f'{drms_params.get_required("DBNAME")}_sums'
                low_water = int(drms_params.get_required('SS_LOW_WATER'))
                high_water = int(drms_params.get_required('SS_HIGH_WATER'))
                scrub_interval = int(drms_params.get_required('SS_SLEEP_INTERVAL'))
            except DPMissingParameterError as exc:
                raise ParametersError(msg=str(exc))

            args = None

            if program_args is not None and len(program_args) > 0:
                args = program_args

            parser = CmdlParser(usage='%(prog)s ')

            # optional
            parser.add_argument('-b', '--low_water', help='the low water mark (percentage)', metavar='<low water mark>', dest='low_water', type=int, default=low_water)
            parser.add_argument('-c', '--chunk_size', help='the number of SU directories, per partition, to process at one time', metavar='<chunk size>', dest='chunk_size', default=1024)
            parser.add_argument('-H', '--dbhost', help='the machine hosting the database that serves DRMS data products', metavar='<db host>', dest='db_host', default=db_host)
            parser.add_argument('-i', '--interval', help='the time interval, in seconds, between scrubbing events', metavar='<scrubbing interval>', dest='scrub_interval', type=int, default=scrub_interval)
            parser.add_argument('-l', '--log_file', help='the path to the log file', metavar='<log file>', dest='log_file', default=path_join(sums_log_dir, DEFAULT_LOG_FILE))
            parser.add_argument('-L', '--logging_level', help='the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', metavar='<logging level>', dest='logging_level', action=DrmsLogLevelAction, default=DrmsLogLevel.ERROR)
            parser.add_argument('-N', '--dbname', help='the DB name', metavar='<db name>', dest='db_name', default=db_name)
            parser.add_argument('-p', '--partition_sets', help='a pds_set_num value which identifies a set of SUMS partitions', metavar='<partition set list>', dest='partition_sets', action='append', default=['0'])
            parser.add_argument('-P', '--dbport', help='the port on the DB host', metavar='<db port>', dest='db_port', type=int, default=db_port)
            parser.add_argument('-t', '--high_water', help='the high water mark (percentage)', metavar='<high water mark>', dest='high_water', type=int, default=high_water)
            parser.add_argument('-U', '--dbuser', help='the DB role to use when connecting to the DBMS', metavar='<db user>', dest='db_user', default=db_user)

            arguments = Arguments(parser=parser, args=args)

            # add needed drms parameters
            arguments.lock_file = path_join(lock_file_dir, STEWARD_LOCK_FILE)

            cls._arguments = arguments

        return cls._arguments


def get_arguments(**kwargs):
    args = []
    for key, val in kwargs.items():
        args.append(f'{key} = {val}')

    drms_params = DRMSParams()

    if drms_params is None:
        raise ParametersError(msg=f'unable to locate DRMS parameters file')

    return Arguments.get_arguments(program_args=args, drms_params=drms_params)

def call_stat_vfs(partition_path, rv):
    fs_stats = statvfs(partition_path)
    if fs_stats is not None and hasattr(fs_stats, 'f_bsize') and hasattr(fs_stats, 'f_bavail'):
        rv.value = (1 - fs_stats.f_bavail / fs_stats.f_blocks) * 100

async def get_partitions(partition_sets, log, **connection_info):
    # get all partitions that belong to the partition sets passed in (if None is passed in, or the empty list is passed in, then return all partitions)
    partitions = []

    try:
        if partition_sets and len(partition_sets) > 0:
            partition_sets_list = ','.join(partition_sets)
            sql = f'SELECT partn_name FROM public.sum_partn_avail WHERE pds_set_prime IN ({partition_sets_list})'
        else:
            sql = f'SELECT partn_name FROM public.sum_partn_avail'

        cmd = [ 'psql', '-qt', f'-h {connection_info["db_host"]}', f'-p {str(connection_info["db_port"])}', f'-U {connection_info["db_user"]}', f'{connection_info["db_name"]}' ]
        cmd.append(f'-c {quote(sql)}')
        log.write_debug([ f'[ get_partitions ] running psql command `{" ".join(cmd)}`'])
        proc = await asyncio.subprocess.create_subprocess_shell(' '.join(cmd), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        while True:
            # read data package file from export subprocess
            bytes_read = await proc.stdout.readline()
            if not bytes_read:
                break

            # strip leading whitespace and newline
            partition = bytes_read.decode().strip()
            if partition is not None and len(partition) > 0:
                log.write_debug([ f'[ get_partitions ] appending partition `{partition}` to list of partitions to return' ])
                partitions.append(partition)

        await proc.wait()
        if proc.returncode is not None:
            # child process terminated
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                # error
                if stderr is not None and len(stderr) > 0:
                    raise SubprocessError(msg=f'[ get_parititions ] failure running psql: {stderr.decode()}')
                else:
                    raise SubprocessError(msg=f'[ get_parititions ] failure running psql')
        else:
            raise SubprocessError(msg=f'[ get_parititions ] failure running psql; no return code')
    except:
        partitions = None
        raise

    return partitions

class PartitionScrubber(object):
    # abstract class
    def __init__(self, *, partition, chunk_size, low_water, high_water, log=None, **connection_info):
        self._partition = partition # name AND path to SUMS partition root; might end in '/', might not
        self._chunk_size = chunk_size # the number of SU dirs to delete during a single iteration
        self._low_water = low_water
        self._high_water = high_water
        self._log = log
        self._db_host = connection_info["db_host"]
        self._db_port = connection_info["db_port"]
        self._db_user = connection_info["db_user"]
        self._db_name = connection_info["db_name"]

        self._previous_usage = None
        self._continue_scrubbing = None

    @property 
    def partition(self):
        return self._partition

    @property
    def partition_name(self):
        return self._partition

    @property
    def partition_path(self):
        return self._partition

    async def _find_chunk(self):
        # SELECT wd AS sudir FROM sum_partn_alloc WHERE effective_date < to_char(current_timestamp, 'YYYYMMDD') AND status = 2 AND wd LIKE '/SUM8/%' ORDER BY wd LIMIT 2000
        sql = f"SELECT {PARTITION_TABLE_WD} AS sudir FROM {PARTITION_TABLE} where {PARTITION_TABLE_EFFECTIVE_DATE} < to_char(current_timestamp, 'YYYYMMDD') AND status = 2 AND {PARTITION_TABLE_WD} LIKE '{self._partition.rstrip('/')}/%' ORDER BY {PARTITION_TABLE_WD} LIMIT {self._chunk_size}"

        cmd = [ 'psql', '-qt', f'-h {self._db_host}', f'-p {str(self._db_port)}', f'-U {self._db_user}', f'{self._db_name}' ]
        cmd.append(f'-c {quote(sql)}')
        proc = await asyncio.subprocess.create_subprocess_shell(' '.join(cmd), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        sudir_chunk = []
        while True:
            # read data package file from export subprocess
            bytes_read = await proc.stdout.readline()
            if not bytes_read:
                break

            # strip leading whitespace and newline
            sudir_chunk.append(bytes_read.decode().strip())

        await proc.wait()
        if proc.returncode is not None:
            # child process terminated
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                # error
                if stderr is not None and len(stderr) > 0:
                    raise SubprocessError(msg=f'[ PartitionScrubber._find_chunk ] failure running psql: {stderr.decode()}')
                else:
                    raise SubprocessError(msg=f'[ PartitionScrubber._find_chunk ] failure running psql')
        else:
            raise SubprocessError(msg=f'[ PartitionScrubber._find_chunk ] failure running psql; no return code')

        return sudir_chunk

    async def _disable_partition(self):
        cmd = [ 'psql', f'-h {self._db_host}', f'-p {str(self._db_port)}', f'-U {self._db_user}', f'{self._db_name}' ]
        sql = f"UPDATE {PARTITION_TABLE} SET pds_set_num = -1 WHERE partn_name = '{self._partition}'"
        cmd.append(f'-c {quote(sql)}')
        proc = await asyncio.subprocess.create_subprocess_shell(' '.join(cmd), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        await proc.wait()
        if proc.returncode is not None:
            # child process terminated
            stdout, stderr = proc.communicate()

            # child process terminated
            if proc.returncode != 0:
                # error
                if stderr is not None and len(stderr) > 0:
                    raise SubprocessError(msg=f'[ PartitionScrubber._disable_partition ] failure running psql: {stderr.decode()}' )
                else:
                    raise SubprocessError(msg=f'[ PartitionScrubber._disable_partition ] failure psql' )
        else:
            raise SubprocessError(msg=f'[ PartitionScrubber._disable_partition ] failure running psql; no return code' )

    async def scrub(self):
        self._log.write_debug([ f'[ PartitionScrubber.scrub ] examining partition {self.partition}' ])
        current_usage = self.usage

        if self._continue_scrubbing == False and current_usage > self._previous_usage:
            # more sus were written - will be able to scrub more now
            self._continue_scrubbing = True

        # (self._continue_scrubbing is None) ==> have not started scrubbing / scrubbed to below low water
        # (self._continue_scrubbing is True) ==> have started scrubbing, but not below low water yet
        # (self._continue_scrubbing is False) ==> have started scrubbing, but cannot scrub more (above low-water)
        if (current_usage > self._high_water and self._continue_scrubbing is None) or self._continue_scrubbing:
            self._log.write_info([ f'[ PartitionScrubber.scrub ] time to scrub partition `{self._partition}`' ])
            if self._continue_scrubbing is True:
                self._log.write_info([ f'[ PartitionScrubber.scrub ] current usage of {self.partition} is {str(round(current_usage, 2))}%; still above low-water mark `{self._low_water}%`; continue scrubbing' ])
            else:
                self._log.write_info([ f'[ PartitionScrubber.scrub ] current usage of {self.partition} is {str(round(current_usage, 2))}%; above high-water mark `{self._high_water}%`; scrubbing till below low-water mark `{self._low_water}`%' ])
            sudirs = await self._find_chunk()
            if sudirs is None or len(sudirs) == 0:
                # nothing to clean
                self._log.write_debug([ f'[ PartitionScrubber.scrub ] nothing to scrub in partition {self._partition}' ])
                self._continue_scrubbing = False
                self._previous_usage = current_usage

                if current_usage > self._high_water:
                    # no more sus to delete, but we are still above low water - disable writing to partition
                    self._log.write_info([ f'[ PartitionScrubber.scrub ] disabling write-access to partition {self._partition}' ])
                    self._disable_partition()
            else:
                deleted_sudirs = []
                self._log.write_info([ f'[ PartitionScrubber.scrub ] deleting SUdirs {",".join(sudirs)} from partition `{self._partition}`' ])
                for sudir in sudirs:
                    try:
                        if path_exists(sudir):
                            sh_rmtree(sudir)

                        # paths that do not exist should be treated as if they were deleted (the SUMS DB should be updated)
                        deleted_sudirs.append(sudir.rstrip('/'))
                    except Exception as exc:
                        self._log.write_warning([ f'[ PartitionScrubber.scrub ] unable to delete SUdir `{sudir}`: {str(exc)}' ])
                        # swallow exception so that stewie keeps running; continue on to next sudir

                if len(deleted_sudirs) == 0:
                    # we had sudirs to delete, but we could not delete any
                    self._log.write_warning([ 'f[ PartitionScrubber.scrub ] unable to delete SUdirs' ])
                    self._continue_scrubbing = False
                    self._previous_usage = current_usage

                    if current_usage > self._high_water:
                        # no more sus to delete, but we are still above low water - disable writing to partition
                        self._log.write_info([ f'[ PartitionScrubber.scrub ] disabling write-access to partition {self._partition}' ])
                        self._disable_partition()
                else:
                    current_usage = self.usage # update current usage
                    self._previous_usage = current_usage

                    if current_usage > self._low_water:
                        self._log.write_info([ f'[ PartitionScrubber.scrub ] deleted SUdirs from {self._partition}, current usage `{str(round(current_usage, 2))}%` is still above low-water mark; will continue scrubbing next iteration' ])
                        self._continue_scrubbing = True
                    else:
                        self._log.write_info([ f'[ PartitionScrubber.scrub ] deleted SUdirs from {self._partition}; current usage `{str(round(current_usage, 2))}%` is below low-water mark' ])
                        self._continue_scrubbing = None

                    # update the DB (if rmdir succeeded)
                    # DELETE FROM SUM_PARTN_ALLOC WHERE ds_index IN (self.sudirsStr); DELETE FROM SUM_MAIN WHERE ds_index IN (self.sudirsStr);
                    # + 2 ==> 1 for '/', 1 for 'D'
                    sunums = [ sudir[len(self._partition.rstrip('/')) + 2:] for sudir in deleted_sudirs if sudir.startswith(self._partition) ]
                    sunums_str = ",".join(sunums)

                    sql = f'DELETE FROM {PARTITION_TABLE} WHERE ds_index IN ({sunums_str});DELETE FROM {SUM_MAIN} WHERE ds_index IN ({sunums_str})'

                    # give up on trying to use a psysopg2 that is compatible both with modern python3 and with our very old PG; fork() a
                    # new process
                    cmd = [ 'psql', f'-h {self._db_host}', f'-p {str(self._db_port)}', f'-U {self._db_user}', f'{self._db_name}' ]
                    cmd.append(f'-c {quote(sql)}')
                    proc = await asyncio.subprocess.create_subprocess_shell(' '.join(cmd), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

                    await proc.wait()
                    if proc.returncode is not None:
                        # child process terminated
                        stdout, stderr = await proc.communicate()

                        # child process terminated
                        if proc.returncode != 0:
                            # error
                            if stderr is not None and len(stderr) > 0:
                                raise SubprocessError(msg=f'[ PartitionScrubber.scrub ] failure running psql: {stderr.decode()}' )
                            else:
                                raise SubprocessError(msg=f'[ PartitionScrubber.scrub ] failure psql' )
                    else:
                        raise SubprocessError(msg=f'[ PartitionScrubber.scrub ] failure running psql; no return code' )
        else:
            if self._continue_scrubbing is None:
                self._log.write_debug([ f'[ PartitionScrubber.scrub ] partition `{self._partition}` usage is at {str(round(current_usage, 2))}%, below high-water mark `{str(self._high_water)}%`; not time to scrub' ])
            else:
                # self._continue_scrubbing must be False ==> cannot scrub more, unless a new expired chunk has suddently materialized
                sudirs = await self._find_chunk()
                if sudirs and len(sudirs) > 0:
                    self._log.write_debug([ f'[ PartitionScrubber.scrub ] partition `{self._partition}` usage is now at {str(round(current_usage, 2))}%, above low-water mark `{str(self._low_water)}%` and a chunk of SUs has expired; time to scrub' ])
                    self._continue_scrubbing = True
                else:
                    self._log.write_debug([ f'[ PartitionScrubber.scrub ] partition `{self._partition}` usage is now at {str(round(current_usage, 2))}%, above low-water mark `{str(self._low_water)}%` and no expired chunks exsit; not time to scrub' ])

class DirectoryPartitionScrubber(PartitionScrubber):
    @property
    def usage(self):
        lock = MpLock()
        percent_usage = CtypeValue(c_double, 0, lock=lock)
        proc = MpProcess(target=call_stat_vfs, args=(self.partition_path, percent_usage))
        proc.start()
        proc.join(5) # timeout after 5 seconds

        if proc.exitcode is None:
            raise FileStatusError(f'os.statvfs({self.partition_path}) did not terminate')

        return percent_usage.value

class ScrubberFactory(object):
    '''
    '''
    def __init__(self, *, log=None, **connection_info):
        self._log = log
        self._connection_info = connection_info

    def create_scrubber(self, *, cls, partition, chunk_size, low_water, high_water):
        '''
        instantiate a scrubber for a single SUMS partition; the scrubber object has a SUFinder instance associated with it
        '''
        return cls(partition=partition, chunk_size=chunk_size, low_water=low_water, high_water=high_water, log=self._log, **self._connection_info)

async def launch_scrubbers(scrubbers):
    await asyncio.gather(*[ scrubber.scrub() for scrubber in scrubbers ])

def set_main_loop_event(ml_event):
    ml_event.set()
    ml_event.clear()

if __name__ == "__main__":
    try:
        arguments = get_arguments()
        log = None

        try:
            formatter = DrmsLogFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            log = DrmsLog(arguments.log_file, arguments.logging_level, formatter)
            print(f'log file is {arguments.log_file}')
        except Exception as exc:
            raise LoggingError(msg=f'{str(exc)}')

        # TerminationHandler opens a DB connection to the RS database (which is the same as the DRMS database, most likely).
        with TerminationHandler(lock_file=arguments.lock_file, log=log, pid=getpid()) as th:
            # start the finder thread - it will run for the entire duration of the steward

            # find SUs to delete in all specified partitions
            log.write_info([ f'obtaining partitions in partition set(s) {str(arguments.partition_sets)}' ])
            partition_list = asyncio.run(get_partitions(arguments.partition_sets, log, db_host=arguments.db_host, db_port=arguments.db_port, db_user=arguments.db_user, db_name=arguments.db_name))

            if len(partition_list) == 0:
                raise ArgumentsError(msg='no valid partitions were specified in the setlist argument')

            log.write_info([ f'monitoring partitions `{",".join(partition_list)}`' ])

            factory = ScrubberFactory(log=log, **{ 'db_host' : arguments.db_host, 'db_port' : arguments.db_port, 'db_user' : arguments.db_user, 'db_name' : arguments.db_name})
            scrubbers = [ factory.create_scrubber(cls=DirectoryPartitionScrubber, partition=partition, chunk_size=arguments.chunk_size, low_water=arguments.low_water, high_water=arguments.high_water) for partition in partition_list ]
            log.write_info([ f'successfully created scrubbers for partitions `{", ".join([ scrubber.partition_name for scrubber in scrubbers ])}`' ])

            main_loop_event = Event() # the main_loop_timer fires this event when it is time for the next main loop iteration
            th.loop_event = main_loop_event

            # cleaning-session loop
            while True:
                # clear out old timer and create new one
                main_loop_timer = Timer(arguments.scrub_interval, set_main_loop_event, args=(main_loop_event,))
                th.timer = main_loop_timer

                # main loop
                log.write_info([ 'starting main loop iteration' ])

                th.disable_interrupts()
                try:
                    # gather all scrubbers - this will run all scrubbers concurrently
                    asyncio.run(launch_scrubbers(scrubbers))
                finally:
                    th.enable_interrupts()

                log.write_info([ f'successfully scrubbed all partitions this iteration' ])

                # start a main-loop timer; when this fires, it is time to execute another main-loop iteration
                main_loop_timer.start()

                # sleep until next iteration - when loop timer fires, it will set main loop event, which will cause wait() to unblock
                main_loop_event.wait()
    except DRMSError as exc:
        if log is not None:
            log.write_error([ str(exc) ])
        else:
            print(str(exc))
    
    exit(0)
else:
    pass
