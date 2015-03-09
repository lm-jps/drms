#!/usr/bin/env python

from __future__ import print_function

import sys
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

def getDRMSParam(drmsParams, param):
    rv = drmsParams.get(param)
    if not rv:
        raise Exception('drmsParams', 'DRMS parameter ' + param + ' is not defined.', RV_ERROR_PARAMS)

    return rv

def getArgs(drmsParams):
    optD = {}
    
    try:
        parser = CmdlParser(usage='%(prog)s [ -dh ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user> ] [ --timeout=<minutes>]')

        # Optional
        parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains the address and domain tables from which records are to be deleted.', metavar='<db host machine>', dest='dbhost', default=getDRMSParam(drmsParams, 'SERVER'))
        parser.add_argument('-P', '--dbport', help='The port on the machine hosting DRMS address and domain tables.', metavar='<db host port>', dest='dbport', default=getDRMSParam(drmsParams, 'DRMSPGPORT'))
        parser.add_argument('-N', '--dbname', help='The name of the database serving the address and domain.', metavar='<db name>', dest='dbname', default=getDRMSParam(drmsParams, 'DBNAME'))
        parser.add_argument('-U', '--dbuser', help='The user to log-in as when connecting to the the serving database.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
        parser.add_argument('-t', '--timeout', help='The number of minutes the user has to complete the registration process.', metavar='<timeout>', dest='timeout', default=getDRMSParam(drmsParams, 'REGEMAIL_TIMEOUT'))

        parser.add_argument('-d', '--doit', help='If provided, the needed database modifications are perfomed. Otherwise, the SQL to be executed is printed and no database changes are made.', dest='doit', action='store_true', default=False)

        args = parser.parse_args()
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]
        
        if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
            raise Exception('getArgs', 'Unable to parse command-line arguments. ' + msg + '\n' + parser.format_help(), RV_ERROR_ARGS)

    # Cannot loop through attributes since args has extra attributes we do not want.
    optD['dbhost'] = args.dbhost
    optD['dbport'] = int(args.dbport)
    optD['dbname'] = args.dbname
    optD['dbuser'] = args.dbuser
    optD['timeout'] = int(args.timeout)
    optD['doit'] = args.doit
    
    # Get configuration information.
    optD['cfg'] = drmsParams
    
    return optD

def logOutput(msg):
    timeStamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    print('[' + timeStamp + '] ' + msg)

if __name__ == "__main__":
    rv = RV_ERROR_NONE

    try:
        drmsParams = DRMSParams()
        if drmsParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).', RV_ERROR_PARAMS)
        
        optD = getArgs(drmsParams)

        try:
            logOutput('Starting clean-up.')

            with psycopg2.connect(database=optD['dbname'], user=optD['dbuser'], host=optD['dbhost'], port=str(optD['dbport'])) as conn:
                logOutput('Connected to database - dbname ==> ' + optD['dbname'] + ', dbuser ==> ' + optD['dbuser'] + ', dbhost ==> ' + optD['dbhost'] + ', dbhost ==> ' + str(optD['dbport']))
                with conn.cursor() as cursor:
                    domainMap = {}
                                            
                    # We will map domainid to domain name, so create dictionary now.
                    cmd = 'SELECT domainid, domainname FROM jsoc.export_addressdomains'
                    try:
                        cursor.execute(cmd)
                        rows = cursor.fetchall()
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise Exception('dbCmd', exc.diag.message_primary + ": " + cmd, RV_ERROR_DBCMD)
                    
                    for row in rows:
                        domainid, domainname = row
                        domainMap[str(domainid)] = domainname

                    # Get a list of each email address.
                    cmd = 'SELECT localname, domainid, confirmation, starttime FROM jsoc.export_addresses'
                    try:
                        cursor.execute(cmd)
                        rows = cursor.fetchall()
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise Exception('dbCmd', exc.diag.message_primary + ": " + cmd, RV_ERROR_DBCMD)
                    
                    # Loop through results, deleting anything sufficiently old.
                    prepDone = False
                    usedDomains = {}

                    logOutput('Checking for expired registration attempts.')
                    for row in rows:
                        localname, domainid, confirmation, starttime = row
                        
                        # Track which domains are currently in use.
                        if str(domainid) not in usedDomains:
                            usedDomains[str(domainid)] = 1
                        else:
                            usedDomains[str(domainid)] += 1
                        
                        # Remove old unregistered email addresses. An address is unregistered if the confirmation
                        # column is not empty.
                        if confirmation and datetime.now(starttime.tzinfo) > starttime + timedelta(minutes=optD['timeout']):
                            if not prepDone:
                                logOutput('Found expired registration attempts.')
                                cmd = "DELETE FROM jsoc.export_addresses WHERE localname = $1 AND domainid = $2"
                                prepCmd = 'PREPARE deladdress AS ' + cmd
                                
                                try:
                                    cursor.execute(prepCmd)
                                except psycopg2.Error as exc:
                                    # Handle database-command errors.
                                    raise Exception('dbCmd', exc.diag.message_primary + ": " + prepCmd, RV_ERROR_DBCMD)

                                prepDone = True

                            # We have an address that should be deleted. Use the prepared statement.
                            if optD['doit']:
                                exeCmd = 'EXECUTE deladdress (%s, %s)'
                                try:
                                    cursor.execute(exeCmd, (localname, str(domainid)))
                                except psycopg2.Error as exc:
                                    # Handle database-command errors.
                                    raise Exception('dbCmd', exc.diag.message_primary + ": " + exeCmd, RV_ERROR_DBCMD)
                            else:
                                completed = cmd.replace('$1', "'" + localname + "'")
                                completed = completed.replace('$2', str(domainid))
                                logOutput(completed)
                            
                            logOutput("Deleted expired registration attempt for '" + localname + '@' + domainMap[str(domainid)] +"'")
                            
                            # Decrement refcount on used domains.
                            usedDomains[str(domainid)] -= 1
                    
                    if not prepDone:
                        logOutput('No expired registration attempts found.')


                    logOutput('Checking for unused email domains.')
                    # In Py 2, keys() returns a list. In Py 3, it returns a view.
                    delQueue = []
                    keys = list(usedDomains.keys())
                    for domainidStr in keys:
                        if usedDomains[domainidStr] == 0:
                            # Put in the delete queue.
                            delQueue.append(int(domainidStr))

                    prepDone = False
                    for domainid in delQueue:
                        # This domain is not in use - delete record of it.
                        if not prepDone:
                            logOutput('Found unused email domains.')
                            cmd = "DELETE FROM jsoc.export_addressdomains WHERE domainid = $1"
                            prepCmd = 'PREPARE deldomain AS ' + cmd
                            
                            try:
                                cursor.execute(prepCmd)
                            except psycopg2.Error as exc:
                                # Handle database-command errors.
                                raise Exception('dbCmd', exc.diag.message_primary + ": " + prepCmd, RV_ERROR_DBCMD)
                                    
                            prepDone = True
                        
                        if optD['doit']:
                            exeCmd = 'EXECUTE deldomain (%s)'
                            try:
                                # Python needs the second argument to be a tuple, and in order to make one with one elements, you need a comma.
                                cursor.execute(exeCmd, (str(domainid), ))
                            except psycopg2.Error as exc:
                                # Handle database-command errors.
                                raise Exception('dbCmd', exc.diag.message_primary + ": " + exeCmd, RV_ERROR_DBCMD)
                        else:
                            completed = cmd.replace('$1', str(domainid))
                            logOutput(completed)
                                    
                        logOutput("Deleted unused domain '" + domainMap[str(domainid)] + "'")

                    if not prepDone:
                        logOutput('No unused email domains found.')

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
