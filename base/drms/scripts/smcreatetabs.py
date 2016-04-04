#!/usr/bin/env python3

# smcreatetabs.py op=create tabs=req
# smcreatetabs.py op=drop tabs=req


from __future__ import print_function
import sys
import os
import pwd
import argparse
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../replication'))
from toolbox import getCfg


RET_SUCCESS = 0
RET_ARGS = 1
RET_SERVER_CONFIG = 2
RET_DRMSPARAMS = 3
RET_INVALIDARGS = 4
RET_DBCONNECT = 5
RET_BADDBQUERY = 6
RET_TABEXISTS = 7

class SmCtParams(DRMSParams):

    def __init__(self):
        super(SmCtParams, self).__init__()

    def get(self, name):
        val = super(SmCtParams, self).get(name)

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
                type, msg = exc
                  
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

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)
        
    def getArg(self, name):
        # An exception is raised if the argument does not exist.
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
            raise Exception('serverConfig', 'Unable to open or parse server-side configuration file ' + file + '.')
        for key, val in cfileDict.items():
            self.setArg(key, val)

class ListAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))

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
    
    try:
        arguments = Arguments()
            
        smCtParams = SmCtParams()
        if smCtParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')
                
        # Add the server-side subscription arguments to arguments.
        arguments.setArg('slonyCfg', smCtParams.get('SLONY_CONFIG'))
        arguments.addFileArgs(arguments.getArg('slonyCfg'))
    
        parser = CmdlParser(usage='%(prog)s [ -h ] [ -c ] op=<operation> tabs=<tables> [ reqtab=<request table> ] [ --dbname=<db name> ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --webuser=<web user> ]')

        # Optional parameters - no default argument is provided, so the default is None, which will trigger the use of what exists in the configuration file
        # (which is drmsparams.py).
        parser.add_argument('o', 'op', '--op', help='The operation to perform.', metavar='<operation>', dest='op', required=True)
        parser.add_argument('t', 'tabs', '--tabs', help='The table(s) to operate on.', metavar='<tables>', dest='tabShorts', action=ListAction, required=True)
        parser.add_argument('r', 'reqtable', '--reqtable', help='The database table that contains remote-storage-unit-request information. If provided, overrides the default specified in configuration file.', metavar='<request table>', dest='reqtable', default=arguments.getArg('kSMreqTable'))
        parser.add_argument('-c', '--check', help='Print the SQL to be otherwise executed, but do not execute the sql.', dest='check', action='store_true', default=False)
        parser.add_argument('-N', '--dbname', help='The name of the database that contains the series table from which records are to be deleted.', metavar='<db name>', dest='dbname', default=arguments.getArg('SLAVEDBNAME'))
        parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains the series table from which records are to be deleted.', metavar='<db host machine>', dest='dbhost', default=arguments.getArg('SLAVEHOSTNAME'))
        parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database that contains the series table from which records are to be deleted.', metavar='<db host port>', dest='dbport', type=int, default=int(arguments.getArg('SLAVEPORT')))
        parser.add_argument('-U', '--dbuser', help='The user to connect to the database as.', metavar='<db user>', dest='dbuser', default=arguments.getArg('REPUSER'))
        parser.add_argument('-W', '--webuser', help='The user that runs the cgi that reads from the sitetable.', metavar='<web user>', dest='webuser', default=smCtParams.get('WEB_DBUSER'))
        
        arguments.setParser(parser)

        tabs = []
        for tab in arguments.getArg('tabShorts'):
            if tab.lower() == 'req':
                tabs.append(arguments.getArg('reqtable'))
            else:
                raise Exception('badTabsarg', "Invalid 'tabs' argument. Must be a comma-separated list of any set of 'req'.")
                
        arguments.setArg('tabs', tabs)
        
        # Connect to the database.
        # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        with psycopg2.connect(database=arguments.getArg('dbname'), user=arguments.getArg('dbuser'), host=arguments.getArg('dbhost'), port=str(arguments.getArg('dbport'))) as conn:
            print('Connected to database ' + arguments.getArg('dbname') + ' on ' + arguments.getArg('dbhost') + ':' + str(arguments.getArg('dbport')) + ' as user ' + arguments.getArg('dbuser'))
            with conn.cursor() as cursor:
                if arguments.getArg('op') == 'create':
                    # Iterate through tables.
                    for tab in arguments.getArg('tabs'):
                        pieces = tab.split('.')
                        if len(pieces) != 2:
                            raise Exception('badArgs', 'The database table name must be a fully qualified database table name (<namespace>.<table name>)')                            
                        tabName = pieces[1]
                        
                        # Fail if the table already exists.
                        if tabExists(cursor, pieces):
                            raise Exception('tabExists', 'The database table ' + tab + ' already exists.')

                        if tab == arguments.getArg('reqtable'):
                            if tabExists(cursor, (pieces[0], pieces[1] + '_seq')):
                                raise Exception('tabExists', 'The database table ' + tab + '_seq already exists.')

                        if tab == arguments.getArg('reqtable'):
                            sql = 'CREATE TABLE ' + tab + ' (requestid bigint NOT NULL, client text NOT NULL, starttime timestamp with time zone NOT NULL, action text NOT NULL, series text NOT NULL, archive int NOT NULL, retention int NOT NULL, tapegroup int NOT NULL, status character(1) NOT NULL, errmsg text); '
                            sql += 'ALTER TABLE ' + tab + ' ADD CONSTRAINT ' + tabName + '_pkey PRIMARY KEY (requestid); '

                            # The web db user and the slony super user must be able to read and write to/from the reqtable.
                            sql += 'GRANT INSERT, SELECT, UPDATE ON ' + tab + ' TO ' + arguments.getArg('webuser') + '; '

                            # We must also make a sequence that will be used to populate the requestid column.
                            sql += 'CREATE SEQUENCE ' + tab + '_seq; '
                            sql += 'GRANT USAGE, SELECT on ' + tab + '_seq TO ' + arguments.getArg('webuser')
                
                        if arguments.getArg('check'):
                            print(sql)
                        else:
                            try:
                                cursor.execute(sql)
                            except psycopg2.Error as exc:
                                raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)
                elif arguments.getArg('op') == 'drop':
                    # Iterate through tables.
                    for tab in arguments.getArg('tabs'):
                        pieces = tab.split('.')
                        if len(pieces) != 2:
                            raise Exception('badArgs', 'The site-table name must be a fully qualified database table name (<namespace>.<table name>)')
                        
                        # Fail if the table doesn't exist.
                        if not tabExists(cursor, pieces):
                            raise Exception('tabExists', 'The database table ' + tab + ' does not exist.')

                        if tab == arguments.getArg('reqtable'):
                            if not tabExists(cursor, (pieces[0], pieces[1] + '_seq')):
                                raise Exception('tabExists', 'The database sequence ' + tab + '_seq does not exist.')
                    
                        sql = 'DROP TABLE ' + tab + ' CASCADE'

                        if tab == arguments.getArg('reqtable'):
                            # Drop the sequence too.
                            sql += '; DROP SEQUENCE ' + tab + '_seq'
                    
                        if arguments.getArg('check'):
                            print(sql)
                        else:
                            try:
                                cursor.execute(sql)
                            except psycopg2.Error as exc:
                                raise Exception('badSql', sql + ' failed. ' + exc.diag.message_primary)
                else:
                    raise Exception('badArgs', 'Unsupported operation ' + arguments.getArg('op') + '.')

    except psycopg2.DatabaseError as exc:
        print('Unable to connect to the database (no, I do not know why).', file=sys.stderr)
        
        # No need to close cursor - leaving the with block does that.
        rv = RET_DBCONNECT

    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
                
        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'args':
            rv = RET_ARGS
        elif etype == 'serverConfig':
            rv = RET_SERVER_CONFIG
        elif etype == 'drmsParams':
            rv = RET_DRMSPARAMS
        elif etype == 'tabExists':
            rv = RET_TABEXISTS
        elif etype == 'badArgs':
            rv = RET_INVALIDARGS
        elif etype == 'badSql':
            rv = RET_BADDBQUERY
        else:
            raise # Re-raise
            
        print(msg, file=sys.stderr)

sys.exit(rv)
