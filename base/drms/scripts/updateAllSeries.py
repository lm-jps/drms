#!/usr/bin/env python

from __future__ import print_function

import sys
import os.path
import argparse
import pwd
import re
import json
import cgi
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser

# Return codes for cmd-line run.
RET_SUCCESS = 0
RET_BADARGS = 1
RET_DRMSPARAMS = 2
RET_DBCONNECT = 3
RET_SQL = 4

def getArgs(drmsParams):
    optD = {}
    
    try:
        parser = CmdlParser(usage='%(prog)s [ -h ]\nop=insert --info=<series information>\nop=delete --dbhost=<host of series to delete> --dbport=<port on host of series to delete> --dbname=<name of database containing series to delete> --series=<series to delete>')
        
        # Required
        parser.add_argument('o', 'op', '--op', help='The operation to perform on the allseries, either "insert" or "delete".', metavar='<operation>', dest='op', required=True)
        
        # Optional
        parser.add_argument('-i', '--info', help='The series information if the operation is "insert". The information must be a comma-separated list of strings.', metavar='<info>', dest='info')
        parser.add_argument('-H', '--dbhost', help='The host of the series to delete from the allseries table if the operation is "delete".', metavar='<dbhost>', dest='seriesdbhost')
        parser.add_argument('-P', '--dbport', help='The port on the host of the series to delete from the allseries table if the operation is "delete".', metavar='<dbport>', dest='seriesdbport')
        parser.add_argument('-D', '--dbname', help='The name of the database containing the series to delete from the allseries table if the operation is "delete".', metavar='<dbname>', dest='seriesdbname')
        parser.add_argument('-s', '--series', help='The series to delete from the allseries table if the operation is "delete".', metavar='<series>', dest='series')
        
        args = parser.parse_args()
    
    # Read the series info from
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]
        
        if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
            raise Exception('getArgs', 'Unable to parse command-line arguments.')
        else:
            raise # Re-raise.

    optD['op'] = args.op.lower()
    optD['seriesdbhost'] = args.seriesdbhost
    optD['seriesdbport'] = args.seriesdbport
    optD['seriesdbname'] = args.seriesdbname
    optD['series'] = args.series
    optD['info'] = args.info

    if optD['op'] == 'insert':
        if optD['info']:
            # The caller has to put the info in the right order.
            cols = args.info.split(',')
            
            if len(cols) != 15:
                raise Exception('badArgs', 'Incorrect number of fields in the info argument.')
            
            optD['info'] = []
            optD['info'].append("'" + cols[0] + "'") # dbhost (text)
            optD['info'].append(cols[1])             # dbport (integer)
            optD['info'].append("'" + cols[2] + "'") # dbname (text)
            optD['info'].append("'" + cols[3] + "'") # seriesname (text)
            optD['info'].append("'" + cols[4] + "'") # author (text)
            optD['info'].append("'" + cols[5] + "'") # owner (text)
            optD['info'].append(cols[6])           # unitsize (integer)
            optD['info'].append(cols[7])           # archive (integer)
            optD['info'].append(cols[8])           # retention (integer)
            optD['info'].append(cols[9])           # tapegroup (integer)
            optD['info'].append("'" + cols[10].replace('|', ',') + "'") # primary_idx (text)
            optD['info'].append("'" + cols[11] + "'") # created (text)
            optD['info'].append("'" + cols[12] + "'") # description (text)
            optD['info'].append("'" + cols[13].replace('|', ',') + "'") # dbidx (text)
            optD['info'].append("'" + cols[14] + "'") # version (text)
        else:
            raise Exception('badArgs', 'Missing required argument for insert operation: info.')
    elif optD['op'] == 'delete':
        if not optD['series']:
            raise Exception('badArgs', 'Missing required argument for delete operation: series.')
    else:
        raise Exception('badArgs', 'Invalid op value: ' + optD['op'])
    
    # Override defaults.
    try:
        optD['dbhost'] = drmsParams.get('SERVER')
        optD['dbport'] = int(drmsParams.get('DRMSPGPORT'))
        optD['dbname'] = drmsParams.get('DBNAME')
        optD['dbuser'] = pwd.getpwuid(os.getuid())[0]
    except KeyError as exc:
        raise Exception('drmsArgs', 'Undefined DRMS parameter.\n' + exc.strerror)

    optD['cfg'] = drmsParams

    return optD

def getUsage():
    return 'updateAllSeries.py [ -h ] op=<operation to perform> [ series=<series to delete> ] [ --info=<information about the series> ]'


rv = RET_SUCCESS

if __name__ == "__main__":
    optD = {}
    
    try:
        drmsParams = DRMSParams()
        if drmsParams is None:
            raise Exception('drmsParams', rtype, 'Unable to locate DRMS parameters file (drmsparams.py).')
            
        optD = getArgs(drmsParams)
        
        # Connect to the database
        # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        with psycopg2.connect(database=optD['dbname'], user=optD['dbuser'], host=optD['dbhost'], port=optD['dbport']) as conn:
            with conn.cursor() as cursor:
                
                if optD['op'] == 'insert':
                    infoStr = ','.join(optD['info'])
                    cmd = 'INSERT INTO drms.allseries(dbhost, dbport, dbname, seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version) VALUES (' + infoStr + ')'
                else:
                    cmd = "DELETE FROM drms.allseries WHERE dbhost='" + optD['seriesdbhost'] + "' AND dbport=" + str(optD['seriesdbport']) + " AND dbname='" + optD['seriesdbname'] + "' AND seriesname='" + optD['series'] + "'"
                try:
                    cursor.execute(cmd)
                except psycopg2.Error as exc:
                    raise Exception('sql', exc.diag.message_primary + ': ' + cmd)
    except psycopg2.Error as exc:
        # Closes the cursor and connection.
        # If we are here, we know that optD['source'] exists.
        msg = 'Unable to connect to the database.\n' + exc.diag.message_primary
        print(msg, file=sys.stderr)
        rv = RET_DBCONNECT
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype, msg = exc.args

        if etype == 'getArgs' or etype == 'badArgs':
            msg += '\nUsage:\n  ' + getUsage()
            print(msg, file=sys.stderr)
            rv = RET_BADARGS
        elif etype == 'drmsParams':
            print(msg, file=sys.stderr)
            rv = RET_DRMSPARAMS
        else:
            raise

sys.exit(rv)
