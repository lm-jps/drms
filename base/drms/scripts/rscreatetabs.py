#!/usr/bin/env python

# rscreatetabs.py op=create tabs=req
# rscreatetabs.py op=create tabs=req,site,su
# rscreatetabs.py op=create tabs=site,su
# rscreatetabs.py op=drop tabs=req
# rscreatetabs.py op=drop tabs=req,site,su

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

    parser = CmdlParser(usage='%(prog)s [ -h ] [ -c ] op=<operation> tabs=<tables> [ reqtab=<request table> ] [ sitetab=<site table> ] [ sutab=<storage-unit table> ] [ --dbname=<db name> ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --webuser=<web user> ]')

    # Optional parameters - no default argument is provided, so the default is None, which will trigger the use of what exists in the configuration file
    # (which is drmsparams.py).
    parser.add_argument('o', 'op', '--op', help='The operation to perform.', metavar='<operation>', dest='op', required=True)
    parser.add_argument('t', 'tabs', '--tabs', help='The table(s) to operate on.', metavar='<tables>', dest='tabs', required=True)
    parser.add_argument('r', 'reqtable', '--reqtable', help='The database table that contains remote-storage-unit-request information. If provided, overrides the default specified in configuration file.', metavar='<request table>', dest='reqtable')
    parser.add_argument('s', 'sitetable', '--sitetable', help='The database table that contains remote-site information. If provided, overrides the default specified in configuration file.', metavar='<site table>', dest='sitetable')
    parser.add_argument('u', 'sutable', '--sutable', help='The database table that contains remote-storage-unit-download information. If provided, overrides the default specified in configuration file.', metavar='<storage-unit table>', dest='sutable')
    parser.add_argument('-c', '--check', help='Print the SQL to be otherwise executed, but do not execute the sql.', dest='check', action='store_true', default=False)
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
    optD['check'] = getOption(args.check, None)    
    optD['op'] = getOption(args.op, None)
    optD['tabs'] = getOption(args.tabs, None)
    optD['reqtable'] = getOption(args.reqtable, drmsParams.get('RS_REQUEST_TABLE'))
    optD['sutable'] = getOption(args.sutable, drmsParams.get('RS_SU_TABLE'))
    optD['sitetable'] = getOption(args.sitetable, drmsParams.get('RS_SITE_TABLE'))
    optD['dbname'] = getOption(args.dbname, drmsParams.get('RS_DBNAME'))
    optD['dbhost'] = getOption(args.dbhost, drmsParams.get('RS_DBHOST'))
    optD['dbport'] = getOption(args.dbport, drmsParams.get('RS_DBPORT'))
    optD['dbuser'] = getOption(args.dbuser, drmsParams.get('RS_DBUSER'))
    optD['webuser'] = getOption(args.webuser, drmsParams.get('WEB_DBUSER'))

    # Check validity of tabs argument.
    tabsMsg = "Invalid 'tabs' argument. Must be a comma-separated list of any set of 'req', 'site', and 'su'."
    if not optD['tabs']:
        raise Exception('badTabsarg', tabsMsg)
        
    tabsLst = optD['tabs'].split(',')
    optD['tabs'] = []
    for tab in tabsLst:
        if tab.lower() == 'req':
            optD['tabs'].append(optD['reqtable'])
        elif tab.lower() == 'site':
            optD['tabs'].append(optD['sitetable'])
        elif tab.lower() == 'su':
            optD['tabs'].append(optD['sutable'])
        else:
            raise Exception('badTabsarg', tabsMsg)

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

        if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat':
            print(msg, file=sys.stderr)
            rv = RET_INVALIDARGS
        elif etype == 'drmsParams':
            print('Error obtaining localized configuration: ' + msg, file=sys.stderr)
            rv = RET_DRMSPARAMS
        elif etype == 'badTabsarg':
            print('Invalid argument: ' + msg, file=sys.stderr)
            rv = RET_INVALIDARGS
        else:
            raise # Re-raise
                
    if rv == RET_SUCCESS:
        # Connect to the database.
        try:
            # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
            with psycopg2.connect(database=optD['dbname'], user=optD['dbuser'], host=optD['dbhost'], port=optD['dbport']) as conn:
                print('Connected to database ' + optD['dbname'] + ' as user ' + optD['dbuser'] + ' on port ' + optD['dbport'] + ' on host ' + optD['dbhost'] + '.')
                with conn.cursor() as cursor:
                    if optD['op'] == 'create':
                        # Iterate through tables.
                        for tab in optD['tabs']:
                            pieces = tab.split('.')
                            if len(pieces) != 2:
                                raise Exception('badArgs', 'The site-table name must be a fully qualified database table name (<namespace>.<table name>)')                            
                            tabName = pieces[1]
                            
                            # Fail if the table already exists.
                            if tabExists(cursor, pieces):
                                raise Exception('tabExists', 'The database table ' + tab + ' already exists.')

                            if tab == optD['reqtable']:
                                if tabExists(cursor, (pieces[0], pieces[1] + '_seq')):
                                    raise Exception('tabExists', 'The database table ' + tab + '_seq already exists.')

                            if tab == optD['reqtable']:
                                sql = 'CREATE TABLE ' + tab + ' (requestid bigint NOT NULL, starttime timestamp with time zone NOT NULL, sunums text NOT NULL, status character(1) NOT NULL, errmsg text); '
                                sql += 'ALTER TABLE ' + tab + ' ADD CONSTRAINT ' + tabName + '_pkey PRIMARY KEY (requestid); '

                                # All users must be able to insert new records into this table. That is how users request storage-unit downloads. They must
                                # also be able to read the results of their requests.
                                sql += 'GRANT INSERT, SELECT ON ' + tab + ' TO public; '

                                # We must also make a sequence that will be used to populate the requestid column.
                                sql += 'CREATE SEQUENCE ' + tab + '_seq; '
                                sql += 'GRANT USAGE, SELECT on ' + tab + '_seq TO public'
                            elif tab == optD['sitetable']:
                                sql = 'CREATE TABLE ' + tab + ' (name text NOT NULL, code integer NOT NULL, baseurl text NOT NULL); '
                                sql += 'ALTER TABLE ' + tab + ' ADD CONSTRAINT ' + tabName + '_pkey PRIMARY KEY (name); '
                                sql += 'GRANT SELECT ON ' + tab + ' TO ' + optD['webuser']
                            elif tab == optD['sutable']:
                                sql = 'CREATE TABLE ' + tab + ' (sunum bigint NOT NULL, starttime timestamp with time zone NOT NULL, refcount integer NOT NULL, status character(1) NOT NULL, errmsg text); '
                                sql += 'ALTER TABLE ' + tab + ' ADD CONSTRAINT ' + tabName + '_pkey PRIMARY KEY (sunum)'
                    
                            if optD['check']:
                                print(sql)
                            else:
                                try:
                                    cursor.execute(sql)
                                except psycopg2.Error as exc:
                                    raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)
                    elif optD['op'] == 'drop':
                        # Iterate through tables.
                        for tab in optD['tabs']:
                            pieces = tab.split('.')
                            if len(pieces) != 2:
                                raise Exception('badArgs', 'The site-table name must be a fully qualified database table name (<namespace>.<table name>)')
                            
                            # Fail if the table doesn't exist.
                            if not tabExists(cursor, pieces):
                                raise Exception('tabExists', 'The database table ' + tab + ' does not exist.')

                            if tab == optD['reqtable']:
                                if not tabExists(cursor, (pieces[0], pieces[1] + '_seq')):
                                    raise Exception('tabExists', 'The database sequence ' + tab + '_seq does not exist.')
                        
                            sql = 'DROP TABLE ' + tab + ' CASCADE'

                            if tab == optD['reqtable']:
                                # Drop the sequence too.
                                sql += '; DROP SEQUENCE ' + tab + '_seq'
                        
                            if optD['check']:
                                print(sql)
                            else:
                                try:
                                    cursor.execute(sql)
                                except psycopg2.Error as exc:
                                    raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)
                    else:
                        raise Exception('badArgs', 'Unsupported operation ' + optD['op'] + '.')

        except psycopg2.DatabaseError as exc:
            print('Unable to connect to the database (no, I do not know why).', file=sys.stderr)
            
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
            elif etype == 'badArgs':
                print(msg, file=sys.stderr)
                rv = RET_INVALIDARGS
            elif etype == 'badSql':
                print(msg, file=sys.stderr)
                rv = RET_BADDBQUERY
            else:
                raise # Re-raise

sys.exit(rv)
