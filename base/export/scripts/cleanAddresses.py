#!/usr/bin/env python3

import sys

if sys.version_info < (3, 5):
    raise Exception('You must run the 3.5 release, or a more recent release, of Python.')

import os
import pwd
from string import Template
from datetime import datetime, timedelta
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

# Return values
RV_ERROR_NONE = 0
RV_ERROR_DBCONNECT = 1
RV_ERROR_DBCMD = 2
RV_ERROR_ARGS = 3
RV_ERROR_PARAMS = 4

def get_args(drms_params):
    try:
        parser = CmdlParser(usage='%(prog)s [ -dh ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user> ] [ --timeout=<minutes>]')

        # Optional
        parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains the address and domain tables from which records are to be deleted.', metavar='<db host machine>', dest='dbhost', default=drms_params.SERVER)
        parser.add_argument('-P', '--dbport', help='The port on the machine hosting DRMS address and domain tables.', metavar='<db host port>', type=int, dest='dbport', default=drms_params.DRMSPGPORT)
        parser.add_argument('-N', '--dbname', help='The name of the database serving the address and domain.', metavar='<db name>', dest='dbname', default=drms_params.DBNAME)
        parser.add_argument('-U', '--dbuser', help='The user to log-in as when connecting to the the serving database.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
        parser.add_argument('-t', '--timeout', help='The number of minutes the user has to complete the registration process.', metavar='<timeout>', type=int, dest='timeout', default=drms_params.REGEMAIL_TIMEOUT)
        parser.add_argument('-d', '--doit', help='If provided, the needed database modifications are perfomed. Otherwise, the SQL to be executed is printed and no database changes are made.', dest='doit', action='store_true', default=False)

        args = parser.parse_args()
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise

        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
            raise Exception('getArgs', 'Unable to parse command-line arguments. ' + msg + '\n' + parser.format_help(), RV_ERROR_ARGS)

    return args

def log_output(msg):
    time_stamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    print('[' + time_stamp + '] ' + msg)

if __name__ == "__main__":
    rv = RV_ERROR_NONE

    try:
        drms_params = DRMSParams()
        if drms_params is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).', RV_ERROR_PARAMS)

        args = get_args(drms_params)

        try:
            log_output('starting clean-up')

            with psycopg2.connect(database=args.dbname, user=args.dbuser, host=args.dbhost, port=str(args.dbport)) as conn:
                log_output('connected to database - dbname ==> ' + args.dbname + ', dbuser ==> ' + args.dbuser + ', dbhost ==> ' + args.dbhost + ', dbport ==> ' + str(args.dbport))
                with conn.cursor() as cursor:
                    # use new email-address db functions (they update the lower-level tables); checkAddress.py inserts into
                    # both jsoc.export_addresses/domains and jsoc.export_user_info, so we can call jsoc.user_unregister()
                    # to remove the timed-out export registration; jsoc.user_unregister() will also remove unused
                    # domains
                    expired_addresses = None
                    cmd = 'SELECT address FROM ' + drms_params.EXPORT_ADDRESS_INFO_FN + "() WHERE confirmation IS NOT NULL AND current_timestamp > starttime + interval '" + str(args.timeout) + " minutes'"
                    try:
                        log_output('checking for expired registration attempts')
                        cursor.execute(cmd)
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            expired_addresses = [row[0] for row in rows]
                            log_output('found expired registration attempts (' + ','.join(expired_addresses) + ')')
                        else:
                            log_output('found NO expired registration attempts')
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise Exception('dbCmd', exc.diag.message_primary + ": " + cmd, RV_ERROR_DBCMD)

                    # since rows is a nx1 2d array, `rows` ends up being a 1d array
                    if args.doit:
                        if expired_addresses is not None and len(expired_addresses) > 0:
                            cmd = 'SELECT * FROM ' + drms_params.EXPORT_USER_UNREGISTER_FN + "(string_to_array('" + ','.join(expired_addresses) + "', ','))"
                            try:
                                log_output('found expired registration attempts (' + ','.join(expired_addresses) + ')')
                                cursor.execute(cmd)
                                rows = cursor.fetchall()
                                if rows[0][0] is True:
                                    log_output('deleted expired registration attempts (' + ','.join(expired_addresses) + ')')
                                else:
                                    raise Exception('dbCmd', 'failure deleting registered users' + ": " + cmd, RV_ERROR_DBCMD)
                            except psycopg2.Error as exc:
                                # Handle database-command errors.
                                raise Exception('dbCmd', exc.diag.message_primary + ": " + cmd, RV_ERROR_DBCMD)
        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection.

            # Man, there is no way to get an error message from any exception object that will provide any information why
            # the connection failed.
            raise Exception('dbConnect', 'Unable to connect to the database.', RV_ERROR_DBCONNECT)

    except Exception as exc:
        if len(exc.args) != 3:
            raise # Re-raise

        etype = exc.args[0]

        if etype == 'dbConnect' or etype == 'getArgs' or etype == 'drmsParams' or etype == 'dbCmd':
            msg = exc.args[1]
            rv = exc.args[2]

            print(msg)
        else:
            raise # Re-raise

    sys.exit(rv)
