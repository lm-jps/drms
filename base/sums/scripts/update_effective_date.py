#!/usr/bin/env python3

# Must run the dumped SQL as a DB user who has namespace-creation and table-creation privileges

import sys
import os
import argparse
import uuid
import shlex
import threading
from datetime import datetime
from subprocess import check_call, CalledProcessError
import psycopg2

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

PROGRAM_GET_SUNUM_LIST = 'show_info'
SUMS_UPDATE_EFFECTIVE_DATE_FUNCTION = 'sums.update_effective_date'
EFFECTIVE_DATE_UED_STRPTIME_FORMAT1 = '%Y%m%d%H%M'
EFFECTIVE_DATE_UED_STRPTIME_FORMAT2 = '%Y%m%d'
EFFECTIVE_DATE_UED_FORMAT = 'YYYYMMDDHHMM'
EFFECTIVE_DATE_DB_FORMAT = 'YYYYMMDDHH24MI'
SUMS_UPDATE_EFFECTIVE_DATE_FUNCTION = 'sums.update_retention'
SUM_PARTN_ALLOC = 'sum_partn_alloc'
SUM_PARTN_ALLOC_DS_INDEX = 'sum_partn_alloc.ds_index'

class UEDError(Exception):
    '''
    base exception class for all Series Snapshot exceptions
    '''
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

    def __str__(self):
        return self.error_message

class UEDArgumentsError(UEDError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class UEDChildProcessError(UEDError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class UEDResponseError(UEDError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class UEDDbError(UEDError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class EffectiveDateAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):

        date_time = None
        try:
            if len(value) == 8:
                date_time = datetime.strptime(value, EFFECTIVE_DATE_UED_STRPTIME_FORMAT2)
            elif len(value) == 12:
                date_time = datetime.strptime(value, EFFECTIVE_DATE_UED_STRPTIME_FORMAT1)
            else:
                raise ValueError(None)
        except ValueError:
            raise UEDArgumentsError('invalid time string {value}; acceptable format is {date_format}'.format(value=value, date_format=EFFECTIVE_DATE_UED_FORMAT))

        setattr(namespace, self.dest, date_time)

class UEDArguments(object):
    __parser = None

    def __init__(self):
        self.parsed_arguments = self.parse_arguments()
        self.validate_arguments()
        self.ingest_arguments()

    def parse_arguments(self, arguments=None):
        # arguments == None ==> sys.argv
        return self.__parser.parse_args(arguments)

    def validate_arguments(self):
        args_dict = vars(self.parsed_arguments)
        if 'effective_date' not in args_dict and 'retention' not in args_dict:
            raise UEDArgumentsError('you must specify either an effective date, or a retention value')

    def ingest_arguments(self):
        for key,val in vars(self.parsed_arguments).items():
            if not hasattr(self, key):
                setattr(self, key, val)
            else:
                raise UEDArgumentsError('attempt to set an argument that already exists: ' + name)

    @classmethod
    def get_arguments(cls):
        cls.set_parser()
        return UEDArguments()

    @classmethod
    def set_parser(cls):
        if cls.__parser is None:
            cls.__parser = CmdlParser(usage='%(prog)s dbhost=<db host> dbport=<db port> dbname=<db name> dbuser=<db user> spec=<record-set specification>')

            # required
            cls.__parser.add_argument('db_host', help='the machine hosting the database management system that contains SUMS data', metavar='<db host>', dest='db_host', required=True)
            cls.__parser.add_argument('db_port', help='the port on `db_host` accepting connections', metavar='<db port>', type=int, dest='db_port', required=True)
            cls.__parser.add_argument('db_name', help='the name of the SUMS database on `db_host`', metavar='<db name>', dest='db_name', required=True)
            cls.__parser.add_argument('db_user', help='the database role on `db_host` used to write data to the `db_name` database', metavar='<db user>', dest='db_user', required=True)
            cls.__parser.add_argument('drms_bin', help='the path to DRMS executables', metavar='<drms bin>', dest='drms_bin', required=True)
            cls.__parser.add_argument('spec', help='the record-set specification identifying records to export', metavar='<spec>', dest='spec', required=True)

            # optional
            cls.__parser.add_argument('-e', '--effective_date', help='the new effective date', metavar='<effective_date>', dest='effective_date', action=EffectiveDateAction)
            cls.__parser.add_argument('-r', '--retention', help='the new retention value', type=int, metavar='<retention in days>', dest='retention')

def generate_db_args(arguments):
    return [ 'JSOC_DBHOST=' + arguments.db_host, 'JSOC_DBPORT=' + str(arguments.db_port), 'JSOC_DBNAME=' + arguments.db_name, 'JSOC_DBUSER=' + arguments.db_user ]

# def dump_sunum_list(make_sunum_list, write_end_fd):
def dump_sunum_list(make_sunum_command, write_end_fd):
    write_end = os.fdopen(write_end_fd, 'w')

    try:
        # check_call(make_sunum_list, shell=False, stdout=write_end)
        check_call(make_sunum_command, shell=True, stdout=write_end)
    except ValueError as error:
        raise UEDChildProcessError('invalid command-line arguments: ' + error.cmd)
    except CalledProcessError as error:
        raise UEDChildProcessError('failure running child process: ' +  error.cmd)

    write_end.close()

def ingest_sunum_list(*, drms_command, sunums_table, cursor):
    # spawn a thread to run the program that dumps the list of requested SUNUMs numbers into a pipe
    read_end_fd, write_end_fd = os.pipe()

    dump_sunums_thread = threading.Thread(target=dump_sunum_list, args=(drms_command, write_end_fd))
    dump_sunums_thread.start()

    # text response
    read_end = os.fdopen(read_end_fd, 'r')

    # read the list of SUNUMs from the other end of the pipe
    cursor.copy_from(read_end, sunums_table)

    dump_sunums_thread.join()

def update_effective_date(*, effective_date, sunums_table, cursor):
    # effective_date is a datetime; convert date to YYYYMMDDHHMM
    effective_date_str = "to_timestamp('{effective_date}', '{effective_date_format}')".format(effective_date=effective_date.strftime(EFFECTIVE_DATE_UED_STRPTIME_FORMAT1), effective_date_format=EFFECTIVE_DATE_DB_FORMAT)

    command = "SELECT {update_effective_date}({sunum_column}, {effective_date_str}) FROM {sums_su_table} WHERE {sums_su_table_sunum_column} IN (SELECT sunum FROM {sunums_table})".format(update_effective_date=SUMS_UPDATE_EFFECTIVE_DATE_FUNCTION, sunum_column=SUM_PARTN_ALLOC_DS_INDEX, effective_date_str=effective_date_str, sums_su_table=SUM_PARTN_ALLOC, sums_su_table_sunum_column=SUM_PARTN_ALLOC_DS_INDEX, sunums_table=sunums_table)

    cursor.execute(command)

if __name__ == "__main__":
    try:
        arguments = UEDArguments.get_arguments()

        # connect to the SUMS DB
        with psycopg2.connect(database=arguments.db_name, user=arguments.db_user, host=arguments.db_host, port=str(arguments.db_port)) as db_connection:
            with db_connection.cursor() as cursor:
                try:
                    # a temporary table to hold the list of desired SUNUMs
                    sunums_table = 'ued_sunums_' + str(uuid.uuid4()).replace('-', '_')
                    command = 'CREATE TEMPORARY TABLE ' + sunums_table + '(sunum BIGINT PRIMARY KEY) ON COMMIT DROP'
                    cursor.execute(command)

                    # call show_info to dump a list of SUNUMs, and then ingest them into the temporary table in the SUMS DB
                    # make_sunum_list = [ os.path.join(arguments.drms_bin, PROGRAM_GET_SUNUM_LIST), 'S=1', 'q=1', 'ds=' + arguments.spec ]
                    make_sunum_list = [ os.path.join(arguments.drms_bin, PROGRAM_GET_SUNUM_LIST), 'S=1', 'q=1', 'ds=' + arguments.spec ]
                    make_sunum_command = ' '.join([ shlex.quote(element) for element in make_sunum_list ]) + '| sort -u'
                    # ingest_sunum_list(drms_command=make_sunum_list, sunums_table=sunums_table, cursor=cursor)
                    ingest_sunum_list(drms_command=make_sunum_command, sunums_table=sunums_table, cursor=cursor)

                    # update the effective date of the specified storage units
                    update_effective_date(effective_date=arguments.effective_date, sunums_table=sunums_table, cursor=cursor)
                except psycopg2.Error as error:
                    raise UEDDbError('failure executing SQL: {error}'.format(error=error.pgerror))
    except UEDError as error:
        print('failure updating effective date: {error}'.format(error=str(error)), file=sys.stderr)
        sys.exit(1)

sys.exit(0)
