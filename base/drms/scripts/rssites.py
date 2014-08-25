#!/usr/bin/env python

# To test on the command line:
#    rssites.py 'name=JSOC&dbhost=hmidb&dbport=5432'

from __future__ import print_function
import sys
import os
import cgi
import json
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

RET_SUCCESS = 0
RET_ARGS = 1
RET_DBCONNECT = 2
RET_DRMSPARAMS = 3
RET_SQL = 4

def getOption(val, default):
    if val:
        return val
    else:
        return default

def GetArgs(args):
    istat = False
    optD = {}

    # Defaults.
    optD['name'] = None
    optD['code'] = None
    optD['dbname'] = None
    optD['dbhost'] = None
    optD['dbport'] = None
    optD['dbuser'] = None
    
    try:
        # Try to get arguments with the cgi module. If that doesn't work, then fetch them from the command line.
        arguments = cgi.FieldStorage()
        
        if arguments:
            for key in arguments.keys():
                val = arguments.getvalue(key)
                
                if val.lower() == 'none':
                    continue
                
                if key in ('name', 'n'):
                    optD['name'] = val.split(',')
                elif key in ('c', 'code'):
                    optD['code'] = val
                elif key in ('N', 'dbname'):
                    optD['dbname'] = val
                elif key in ('H', 'dbhost'):
                    optD['dbhost'] = val
                elif key in ('P', 'dbport'):
                    optD['dbport'] = val
                elif key in ('U', 'dbuser'):
                    optD['dbuser'] = val
                    
    except ValueError:
        raise Exception('badArgs', 'Usage: ' + getUsage())

    try:
        drmsParams = DRMSParams()
        if drmsParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')

        # Get configuration information.
        optD['cfg'] = drmsParams
        optD['sitetable'] = drmsParams.get('RS_SITE_TABLE')
        optD['dbname'] = getOption(optD['dbname'], drmsParams.get('RS_DBNAME'))
        optD['dbhost'] = getOption(optD['dbhost'], drmsParams.get('RS_DBHOST'))
        optD['dbport'] = getOption(optD['dbport'], drmsParams.get('RS_DBPORT'))
        optD['dbuser'] = getOption(optD['dbuser'], drmsParams.get('WEB_DBUSER'))

    except KeyError as exc:
        type, value, traceback = sys.exc_info()
        raise Exception('drmsParams', exc.strerror)

    if optD:
        if optD['name'] is not None and optD['code'] is not None:
            raise Exception('badArgs', 'Both name and code arguments cannot be provided.')

    return optD


if __name__ == "__main__":
    status = RET_SUCCESS
    optD = {}
    rootObj = {}

    try:
        optD = GetArgs(sys.argv[1:])

    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]
        
        if etype == 'badArgs':
            status = RET_INVALIDARGS
            rootObj['status'] = 'errorArgs'
            rootObj['statusMsg'] = 'Invalid arguments supplied: ' + msg
        elif etype == 'drmsParams':
            rootObj['status'] = 'errorArgs'
            rootObj['statusMsg'] = 'Error reading DRMS parameters: ' + msg
        else:
            raise # Re-raise
        
    if status == RET_SUCCESS:
        # Connect to the database
        try:
            # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
            with psycopg2.connect(database=optD['dbname'], user=optD['dbuser'], host=optD['dbhost'], port=optD['dbport']) as conn:
                with conn.cursor() as cursor:
                    # Read from sites table.
                    # sites(name, code, baseurl)
                    cmd = 'SELECT name, code, baseurl FROM ' + optD['sitetable']
                    
                    if optD['name']:
                        cmd += ' WHERE name in (' + ', '.join("'" + elem + "'" for elem in optD['name']) + ')'
                    
                    try:
                        cursor.execute(cmd)

                    except psycopg2.Error as exc:
                        raise Exception('badSql', exc.diag.message_primary)

                    rootObj['status'] = 'success'

                    for record in cursor:
                        siteName = record[0]
                        rootObj[siteName] = {}
                        rootObj[siteName]['code'] = record[1]
                        rootObj[siteName]['baseurl'] = record[2]

        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection
            rootObj['status'] = 'errorDbconn'
            # Man, there is no way to get an error message from any exception object that will provide any information why
            # the connection failed.
            rootObj['statusMsg'] = 'Unable to connect to the database (no, I do not know why).'
            
            # No need to close cursor - leaving the with block does that.
            rv = RET_DBCONNECT

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise
            
            etype = exc.args[0]
            msg = exc.args[1]
            
            if etype == 'badArgs':
                status = RET_ARGS
                rootObj['status'] = etype
                rootObj['statusMsg'] = msg
            elif etype == 'drmsParams':
                status = RET_DRMSPARAMS
                rootObj['status'] = etype
                rootObj['statusMsg'] = msg
            elif etype == 'badSql':
                status = RET_SQL
                rootObj['status'] = etype
                rootObj['statusMsg'] = msg
            else:
                raise # Re-raise

    print('Content-type: application/json\n')
    print(json.dumps(rootObj))

    # Always return 0. If there was an error, an error code (the 'status' property) and message (the 'statusMsg' property) goes in the returned HTML.
    sys.exit(0)
