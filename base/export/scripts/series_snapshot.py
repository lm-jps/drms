#!/usr/bin/env python3

# Must run the dumped SQL as a DB user who has namespace-creation and table-creation privileges

import sys
import os
import argparse
import uuid
import shlex
import json
import time
import fcntl
import threading
from io import StringIO
from subprocess import run, check_call, check_output, Popen, PIPE, CalledProcessError
from multiprocessing import Process, Pipe
import psycopg2

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

PROGRAM_PARSE_SPECIFICATION = 'drms_parserecset'
PROGRAM_CREATE_NAMESPACE = 'createns'
PROGRAM_CREATE_TABLE = 'createtabstructure'
PROGRAM_GET_RECNUM_LIST = 'show_info'


class SSError(Exception):
    '''
    base exception class for all Series Snapshot exceptions
    '''
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

    def __str__(self):
        return self.error_message

class SSArgumentsError(SSError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class SSChildProcessError(SSError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class SSResponseError(SSError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class SSDbError(SSError):
    def __init__(self, message):
        super().__init__(message)
        self.error_message = message

class SSArguments(object):
    __parser = None

    def __init__(self):
        self.parsed_arguments = self.parse_arguments()
        self.ingest_arguments()

    def parse_arguments(self, arguments=None):
        # arguments == None ==> sys.argv
        return self.__parser.parse_args(arguments)

    def ingest_arguments(self):
        for key,val in list(vars(self.parsed_arguments).items()):
            if not hasattr(self, key):
                setattr(self, key, val)
            else:
                raise SSArgumentsError('attempt to set an argument that already exists: ' + name)

    @classmethod
    def get_arguments(cls):
        cls.set_parser()
        return SSArguments()

    @classmethod
    def set_parser(cls):
        if cls.__parser is None:
            cls.__parser = CmdlParser(usage='%(prog)s dbhost=<db host> dbport=<db port> dbname=<db name> dbuser=<db user> spec=<record-set specification> [ --filename=<> ]')

            # Required
            cls.__parser.add_argument('db_host', help='the machine hosting the database management system that serves DRMS data series', metavar='<db host>', dest='db_host', required=True)
            cls.__parser.add_argument('db_port', help='the port on `db_host` accepting connections', metavar='<db port>', type=int, dest='db_port', required=True)
            cls.__parser.add_argument('db_name', help='the name of the DRMS database on `db_host`', metavar='<db name>', dest='db_name', required=True)
            cls.__parser.add_argument('db_user', help='the database role on `db_host` used to read data from the `db_name` database', metavar='<db user>', dest='db_user', required=True)

            cls.__parser.add_argument('drms_bin', help='the path to DRMS executables', metavar='<drms bin>', dest='drms_bin', required=True)
            cls.__parser.add_argument('spec', help='the record-set specification identifying records to export (provide no filters for all records)', metavar='<spec>', dest='spec', required=True)
            cls.__parser.add_argument('owner', help='the database role that will own any created namespace, table, sequence, etc.', metavar='<owner>', dest='owner', required=True)

            # Optional
            cls.__parser.add_argument('-n', '--skip_namespace', help='do not create namespace', dest='create_namespace', action='store_false')
            cls.__parser.add_argument('-t', '--skip_table', help='do not create series table', dest='create_table', action='store_false')

def generate_db_args(arguments):
    return [ 'JSOC_DBHOST=' + arguments.db_host, 'JSOC_DBPORT=' + str(arguments.db_port), 'JSOC_DBNAME=' + arguments.db_name, 'JSOC_DBUSER=' + arguments.db_user ]

def parse_specification(arguments, specification):
    # do not use shlex.quote() if providing commands by list (quote() is for string command lines)
    command = [ os.path.join(arguments.drms_bin, PROGRAM_PARSE_SPECIFICATION), 'DRMS_DBUTF8CLIENTENCODING=1', 'spec=' + specification ]
    command.extend(generate_db_args(arguments))

    try:
        response = check_output(command, shell=False)
        json_response = json.loads(response.decode('utf-8'))
    except ValueError as error:
        raise SSChildProcessError('invalid command-line arguments: ' + error.cmd)
    except CalledProcessError as error:
        raise SSChildProcessError('failure running child process: ' +  error.cmd)

    if json_response['nsubsets'] != 1:
        raise SSResponseError('unexpected number of record sets: ' + str(json_response['nsubsets']))

    return json_response['subsets'][0]['seriesname'], json_response['subsets'][0]['seriesns'], json_response['subsets'][0]['seriestab']

def dump_namespace_sql(arguments, namespace):
    command = [ os.path.join(arguments.drms_bin, PROGRAM_CREATE_NAMESPACE), 'ns=' + namespace, 'nsgroup=user', 'dbusr=' + arguments.owner ]
    command.extend(generate_db_args(arguments))

    try:
        # pipe directly to stdout
        check_call(command, stdout=sys.stdout, shell=False)
    except CalledProcessError as error:
        raise SSChildProcessError('failure calling ' +  ' '.join(error.cmd))

def dump_table_sql(arguments, series_table):
    command = [ os.path.join(arguments.drms_bin, PROGRAM_CREATE_TABLE), 'in=' + series_table, 'out=' + series_table, 'owner=' + arguments.db_user ]

    try:
        # pipe directly to stdout
        check_call(command, stdout=sys.stdout, shell=False)
    except CalledProcessError as error:
        raise SSChildProcessError('failure calling ' +  ' '.join(error.cmd))

def dump_record_list(make_record_list, write_end_fd):
    write_end = os.fdopen(write_end_fd, 'w')

    try:
        check_call(make_record_list, shell=False, stdout=write_end)
    except ValueError as error:
        raise SSChildProcessError('invalid command-line arguments: ' + error.cmd)
    except CalledProcessError as error:
        raise SSChildProcessError('failure running child process: ' +  error.cmd)

    write_end.close()

def dump_series(arguments, specification, series_table):
    make_record_list = [ os.path.join(arguments.drms_bin, PROGRAM_GET_RECNUM_LIST), 'r=1', 'q=1', 'ds=' + specification ]

    with psycopg2.connect(database=arguments.db_name, user=arguments.db_user, host=arguments.db_host, port=str(arguments.db_port)) as db_connection:
        with db_connection.cursor() as cursor:
            try:
                # convert specification to a list of record numbers
                temporary_table_recnums = 'ss_recnums_' + str(uuid.uuid4()).replace('-', '_')
                command = 'CREATE TEMPORARY TABLE ' + temporary_table_recnums + '(record_number BIGINT PRIMARY KEY) ON COMMIT DROP'
                cursor.execute(command)

                # spawn a thread to run the program that dumps the list of requested record numbers into a pipe
                read_end_fd, write_end_fd = os.pipe()

                dump_records_thread = threading.Thread(target=dump_record_list, args=(make_record_list, write_end_fd))
                dump_records_thread.start()

                read_end = os.fdopen(read_end_fd, 'r')

                # read the list of record numbers from the other end of the pipe
                cursor.copy_from(read_end, temporary_table_recnums)

                dump_records_thread.join()

                if False:
                    command = 'SELECT * FROM ' + temporary_table_recnums
                    print('2 running ' + command)
                    cursor.execute(command)
                    for row in cursor:
                        print("a row " + str(row[0]))

                # create temporary table to hold desired rows
                temporary_table = 'ss_results_' + str(uuid.uuid4()).replace('-', '_')
                command = 'CREATE TEMPORARY TABLE ' + temporary_table + '(LIKE ' + series_table + ') ON COMMIT DROP'
                cursor.execute(command)

                command = 'INSERT INTO ' + temporary_table + ' SELECT * FROM ' + series_table + ' WHERE recnum IN (SELECT record_number FROM ' + temporary_table_recnums + ')'
                cursor.execute(command)

                # `temporary_table` now has the rows we wish to dump to stdout
                cursor.copy_to(sys.stdout, temporary_table)
            except psycopg2.Error as error:
                raise SSDbError('failure executing SQL: '  + ', last command ' + command)


if __name__ == "__main__":
    try:
        arguments = SSArguments.get_arguments()

        specification = arguments.spec

        # parse spec and extract namespace and table, and filters
        series, namespace, table = parse_specification(arguments, specification)

        # dump namespace-creation sql to sql
        if arguments.create_namespace:
            dump_namespace_sql(arguments, namespace)

            # dump table-creation sql
            dump_table_sql(arguments, series)
        elif not arguments.create_table:
            # dump table-creation sql
            dump_table_sql(arguments, series)

        # dump series table rows (entire table for now)
        print('COPY ' + series + ' FROM STDIN;')
        dump_series(arguments, specification, series)
        print('\.')
    except SSError as error:
        print('failure creating Snapshot: ' + str(error), file=sys.stderr)
        sys.exit(1)

sys.exit(0)
