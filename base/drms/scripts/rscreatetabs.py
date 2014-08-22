#!/usr/bin/env python

# rscreatetabs.py op=create

from __future__ import print_function
import sys
import os
import pwd
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

RET_SUCCESS = 0
RET_INVALIDARGS = 1
RET_DBCONNECT = 2
RET_BADDBQUERY = 3
RET_TABEXISTS = 4

def getOption(val, default):
    if val:
        return val
    else:
        return default

def getArgs():

    optD = {}

    parser = CmdlParser(usage='%(prog)s [ -h ] [ -t ] op=<operation> [ sitetab=<site table> ] [ --dbname=<db name> ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --webuser=<web user> ]')

    # Optional parameters - no default argument is provided, so the default is None, which will trigger the use of what exists in the configuration file
    # (which is drmsparams.py).
    parser.add_argument('o', 'op', '--op', help='The operation to perform', metavar='<operation>', dest='op', required=True)
    parser.add_argument('s', 'sitetable', '--sitetable', help='The database table that contains remote-site information. If provided, overrides the default specified in configuration file.', metavar='<site table>', dest='sitetable')
    parser.add_argument('-t', '--test', help='Print the SQL to be otherwise executed, but do not execute the sql.', dest='test', action='store_true', default=False)
    parser.add_argument('-N', '--dbname', help='The name of the database that contains the series table from which records are to be deleted.', metavar='<db name>', dest='dbname')
    parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains the series table from which records are to be deleted.', metavar='<db host machine>', dest='dbhost')
    parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database that contains the series table from which records are to be deleted.', metavar='<db host port>', dest='dbport')
    parser.add_argument('-U', '--dbuser', help='The user to connect to the database as.', metavar='<db user>', dest='dbuser')
    parser.add_argument('-W', '--webuser', help='The user that runs the cgi that reads from the sitetable.', metavar='<web user>', dest='webuser')

    args = parser.parse_args()

    drmsParams = DRMSParams()
    if drmsParams is None:
        raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')

    # Get configuration information.
    optD['cfg'] = drmsParams

    # Override defaults.
    optD['op'] = getOption(args.op, None)
    optD['test'] = getOption(args.test, None)
    optD['sitetable'] = getOption(args.sitetable, drmsParams.get('RS_SITE_TABLE'))
    optD['dbname'] = getOption(args.dbname, drmsParams.get('RS_DBNAME'))
    optD['dbhost'] = getOption(args.dbhost, drmsParams.get('RS_DBHOST'))
    optD['dbport'] = getOption(args.dbport, drmsParams.get('RS_DBPORT'))
    optD['dbuser'] = getOption(args.dbuser, drmsParams.get('RS_DBUSER'))
    optD['webuser'] = getOption(args.webuser, drmsParams.get('WEB_DBUSER'))

    return optD

def tabExists(cursor, table):
    rv = False

    sql = "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '" + table[0] + "' AND c.relname = '" + table[1] + "'"

    try:
        cursor.execute(sql)
        rsp = cursor.fetchall()
    except psycopg2.Error as exc:
        raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)

    if len(rsp) > 0:
        rv = True

    return rv

rv = RET_SUCCESS

if __name__ == "__main__":
    status = RET_SUCCESS
    optD = {}
    rootObj = {}
    
    try:
        optD = getArgs()
    
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]

        if type == 'CmdlParser-ArgUnrecognized' or type == 'CmdlParser-ArgBadformat':
            print(msg, file=sys.stderr)
            rv = RET_INVALIDARGS
        elif type == 'drmsParams':
            print('Error obtaining localized configuration: ' + msg, file=sys.stderr)
            rv = RET_DRMSPARAMS
        else:
            raise # Re-raise
                
    if rv == RET_SUCCESS:
        # Connect to the database.
        try:
            # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
            with psycopg2.connect(database=optD['dbname'], user=optD['dbuser'], host=optD['dbhost'], port=optD['dbport']) as conn:
                print('Connected to database ' + optD['dbname'] + ' as user ' + optD['dbuser'] + ' on port ' + optD['dbport'] + ' on host ' + optD['dbhost'] + '.')
                with conn.cursor() as cursor:
                    pieces = optD['sitetable'].split('.')
                    if len(pieces) != 2:
                        raise Exception('badArgs', 'The site-table name must be a fully qualified database table name (<namespace>.<table name>)')
                    
                    tabName = pieces[1]
    
                    if optD['op'] == 'create':
                        # Fail if the table already exists.
                        if tabExists(cursor, pieces):
                            raise Exception('tabExists', 'The database table ' + optD['sitetable'] + ' already exists.')

                        sql = 'CREATE TABLE ' + optD['sitetable'] + ' (name text NOT NULL, code integer NOT NULL, baseurl text NOT NULL); '
                        sql += 'ALTER TABLE ' + optD['sitetable'] + ' ADD CONSTRAINT ' + tabName + '_pkey PRIMARY KEY (name); '
                        sql += 'GRANT SELECT ON ' + optD['sitetable'] + ' TO ' + optD['webuser']
                    
                        if optD['test']:
                            print(sql)
                        else:
                            try:
                                cursor.execute(sql)
                            except psycopg2.Error as exc:
                                raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)
                    elif optD['op'] == 'drop':
                        # Fail if the table doesn't exist.
                        if not tabExists(cursor, pieces):
                            raise Exception('tabExists', 'The database table ' + optD['sitetable'] + ' does not exist.')
                        
                        sql = 'DROP TABLE ' + optD['sitetable'] + ' CASCADE'
                        
                        if optD['test']:
                            print(sql)
                        else:
                            try:
                                cursor.execute(sql)
                            except psycopg2.Error as exc:
                                raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)
                    else:
                        raise Exception('badArgs', 'Unsupported operation ' + optD['op'] + '.')

        except psycopg2.Error as exc:
            print('Unable to connect to the database. ' + exc.diag.message_primary, file=sys.stderr)
            
            # No need to close cursor - leaving the with block does that.
            rv = RET_DBCONNECT

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise
                    
            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'tabExists':
                print(msg, file=sys.stderr)
                rv = RET_TABEXISTS
            if etype == 'badArgs':
                print(msg, file=sys.stderr)
                rv = RET_INVALIDARGS
            elif etype == 'badSql':
                print(msg, file=sys.stderr)
                rv = RET_BADDBQUERY
            else:
                raise # Re-raise

sys.exit(rv)
