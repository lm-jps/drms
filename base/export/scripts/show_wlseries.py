#!/usr/bin/env python

# Displays a list of all series in the database specified by the dbhost argument. If the host is not our internal host, then
# this script will also list the white-listed series in the internal host.

# {
#    "seriesList":
#     [
#        {"su_arta.Ic_45s": {"primeIndex": ["T_REC", "CAMERA"], "description": "continuum intensities with a cadence of 45 seconds."}},
#        {"su_arta.Ld_45s": {"primeIndex": ["T_REC", "CAMERA"], "description": "linedepths with a cadence of 45 seconds."}},
#        ...
#     ],
#    "errMsg": ""
# }      

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


# Debug flag.
DEBUG_CGI = False

# Return codes for cmd-line run.
RET_SUCCESS = 0
RET_BADARGS = 1
RET_DRMSPARAMS = 2
RET_BADFILTER = 3
RET_DBCONNECT = 4
RET_UNKNOWNRUNTYPE = 5
RET_SQL = 6
RET_WHITELIST = 7

def getUsage():
    return 'show_wlseries.py [ n=1 ] server=<db server> [ filter=<DRMS regular expression> ] [ dbmergeport=<db port> ] [ dbmergename=<db name> ] [ dbmergeuser=<db user> ] [ wlfile=<white-list text file> ]'

def getOption(val, default):
    if val:
        return val
    else:
        return default

def getArgs(args, drmsParams):
    istat = False
    optD = {}
    etype = ''
    
    # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
    if os.getenv('REQUEST_URI') or DEBUG_CGI:
        optD['source'] = 'cgi'
    else:
        optD['source'] = 'cl'

    optD['filter'] = None
    
    if optD['source'] == 'cgi':
        optD['noheader'] = None
        optD['dbhost'] = None
        optD['dbport'] = None
        optD['dbname'] = None
        optD['dbuser'] = None
        optD['wlfile'] = None
        
        try:
            # Try to get arguments with the cgi module. If that doesn't work, then fetch them from the command line.
            arguments = cgi.FieldStorage()
            
            if arguments:
                for key in arguments.keys():
                    val = arguments.getvalue(key)

                    if key in ('n', 'noheader'):
                        optD['noheader'] = val
                    elif key in ('s', 'server'):
                        optD['dbserver'] = val
                    elif key in ('f', 'filter'):
                        optD['filter'] = val
                    elif key in ('H', 'dbmergehost'):
                        optD['dbhost'] = val
                    elif key in ('P', 'dbmergeport'):
                        optD['dbport'] = val
                    elif key in ('N', 'dbmergename'):
                        optD['dbname'] = val
                    elif key in ('U', 'dbmergeuser'):
                        optD['dbuser'] = val
                    elif key in ('w', 'wlfile'):
                        optD['wlfile'] = val

            optD['noheader'] = bool(getOption(optD['noheader'], False))
            optD['dbhost'] = getOption(optD['dbhost'], drmsParams.get('SERVER'))
            optD['dbport'] = int(getOption(optD['dbport'], drmsParams.get('DRMSPGPORT')))
            optD['dbname'] = getOption(optD['dbname'], drmsParams.get('DBNAME'))
            optD['dbuser'] = getOption(optD['dbuser'], pwd.getpwuid(os.getuid())[0])
            optD['wlfile'] = getOption(optD['wlfile'], drmsParams.get('WL_FILE'))

            # Enforce requirements.
            if not 'dbserver' in optD:
                raise Exception('getArgsCgi', 'cgi', 'Missing required argument ' + "'dbserver'.")
        except ValueError as exc:
            raise Exception('getArgs', 'cgi', 'Invalid arguments.')
        except KeyError as exc:
            raise Exception('drmsArgs', 'cgi', 'Undefined DRMS parameter.\n' + exc.strerror)
    else:
        try:
            parser = CmdlParser(usage='%(prog)s [ -hn ] dbserver=<db server> [ --filter=<DRMS regular expression> ] [ --dbmergeport=<db port> ] [ --dbmergename=<db name> ] [ --dbmergeuser=<db user> ] [--wlfile=<white-list text file> ]')
        
            # Required
            parser.add_argument('s', 'server', '--server', help='The machine hosting the database that serves DRMS data series names.', metavar='<db host>', dest='dbserver', required=True)

            # Optional
            parser.add_argument('-n', '--noheader', help='Supress the HTML header (cgi runs only).', dest='noheader', action='store_true', default=False)
            parser.add_argument('-f', '--filter', help='The DRMS-style regular expression to filter series.', metavar='<regexp>', dest='filter')
            parser.add_argument('-H', '--dbmergehost', help='The port on the machine hosting DRMS data series names.', metavar='<db host port>', dest='dbhost')
            parser.add_argument('-P', '--dbmergeport', help='The port on the machine hosting DRMS data series names.', metavar='<db host port>', dest='dbport')
            parser.add_argument('-N', '--dbmergename', help='The name of the database serving DRMS series names.', metavar='<db name>', dest='dbname')
            parser.add_argument('-U', '--dbmergeuser', help='The user to log-in to the serving database as.', metavar='<db user>', dest='dbuser')
            parser.add_argument('-w', '--wlfile', help='The text file containing the definitive list of internal series accessible via the external web site.', metavar='<white-list file>', dest='wlfile')
        
            args = parser.parse_args()
        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
                raise Exception('getArgsCmdline', 'cl', 'Unable to parse command-line arguments.')
            else:
                raise # Re-raise.

        # Override defaults.
        try:
            optD['noheader'] = getOption(args.noheader, False)
            optD['dbserver'] = args.dbserver # Required arguments are always available.
            optD['filter'] = getOption(args.filter, None)
            optD['dbhost'] = getOption(args.dbhost, drmsParams.get('SERVER'))
            optD['dbport'] = int(getOption(args.dbport, drmsParams.get('DRMSPGPORT')))
            optD['dbname'] = getOption(args.dbname, drmsParams.get('DBNAME'))
            optD['dbuser'] = getOption(args.dbuser, pwd.getpwuid(os.getuid())[0])
            optD['wlfile'] = getOption(args.wlfile, drmsParams.get('WL_FILE'))
        except KeyError as exc:
            raise Exception('drmsArgs', 'cl', 'Undefined DRMS parameter.\n' + exc.strerror)
    
    # Get configuration information.
    optD['cfg'] = drmsParams

    return optD

def insertJson(jsonDict, property, value):
    jsonDict[property] = value

def parseFilter(filterStr):
    # By default, Python matches at the beginning of the string only. NOT must be in all caps.
    regexp = re.compile(r"\s*NOT\s+(\S.*)")
    matchobj = regexp.match(filterStr)
    if matchobj is not None:
        exclude = True
        remainder = matchobj.group(1)
    else:
        exclude = False
        remainder = filterStr

    # Look for a namespace.
    regexp = re.compile(r"\s*(\w+)(\.\S+)(.*)")
    matchobj = regexp.match(remainder)
    if matchobj is not None:
        namespace = matchobj.group(1)
        regexpStr = namespace + matchobj.group(2)
        remainder = matchobj.group(3)
    else:
        namespace = None
        regexp = re.compile(r"\s*(\S+)(.*)")
        matchobj = regexp.match(remainder)
        if matchobj is not None:
            regexpStr = matchobj.group(1)
            remainder = matchobj.group(2)
        else:
            regexpStr = ''
            remainder = ''

    # Ensure there are no non-white-space chars in remainder.
    regexp = re.compile(r"\s*(\S+)")
    matchobj = regexp.match(remainder)
    if matchobj is not None:
        # Bad syntax
        raise Exception('parseFilter', optD['source'], 'Invaid filter ' + filterStr)

    filter = (namespace, exclude, regexpStr)

    return filter
        
rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = {}
    rootObj = {}
    seriesListArr = []
    errMsg = ''
    
    try:
        drmsParams = DRMSParams()
        if drmsParams is None:
            if os.getenv('REQUEST_URI'):
                rtype = 'cgi'
            else:
                rtype = 'cl'
            raise Exception('drmsParams', rtype, 'Unable to locate DRMS parameters file (drmsparams.py).')
                
        optD = getArgs(sys.argv[1:], drmsParams)

        # To use the whitelist feature, the DRMS site must have an allseries table (which combines the series from the internal and
        # external databases when the user wants the list of series accessible from the external site).
        if not drmsParams.getBool('WL_HASWL'):
            raise Exception('whitelist', optD['source'], 'This DRMS does not support series whitelists.')

        # Connect to the database
        # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        with psycopg2.connect(database=optD['dbname'], user=optD['dbuser'], host=optD['dbhost'], port=optD['dbport']) as conn:
            with conn.cursor() as cursor:
                # The definitive list of white-listed series is kept in a text file. But we need that information in the database so
                # we can do the filtering in the db. So, we update the database with this information before we do anything else.
                with open(optD['wlfile'], 'r') as fin:
                    try:
                        cursor.copy_from(fin, 'drms.whitelist', columns=('seriesname',))
                    except psycopg2.Error as exc:
                        raise Exception('whitelist', optD['source'], 'Error reading white-list file ' + optD['wlfile'] + '.\n' + exc.diag.message_primary)

                if optD['dbserver'] == drmsParams.get('SERVER'):
                    internalUser = True
                else:
                    internalUser = False
        
                # The filter is an extended POSIX regular expression, optionally preceded by the word NOT.
                # filter is a tuple. The first element is a namespace (if not None, then search in this namespace only).
                # The second is a boolean. If True, then the filter specifies which series to exclude. If False,
                # then the filter specifies with series to include.
                # The third is a regular expression to send to PG that, in combination with the second element,
                # selects series to print. This element can be None, in which case all series are printed.
                if optD['filter'] is not None:
                    filter = parseFilter(optD['filter'])
                    
                    # filter[0] is a namespace (or None).
                    # filter[1] is either True (exclude specified series from result) or False (include specified series).
                    # filter[2] is a regexp (or None).
                    
                    if filter[1]:
                        matchOp = '!~*'
                    else:
                        matchOp = '~*'
                    
                    if internalUser:
                        # The whitelist is irrelevant for internal users.
                        if filter[0]:
                            # An internal user has specfied a namespace. Use ns.drms_series().
                            if filter[2]:
                                # There is a filter on the series within the namespace.
                                cmd = 'SELECT seriesname, primary_idx, description FROM ' + filter[0] + '.drms_series WHERE seriesname ' + matchOp + " '" + filter[2] + "'"
                            else:
                                # There is no filter on the series within the namespace.
                                cmd = 'SELECT seriesname, primary_idx, description FROM ' + filter[0] + '.drms_series'
                        else:
                            # An internal user has NOT specified a namespace. Use allseries table (which must exist).
                            if filter[2]:
                                # No namespace, use allseries, there is a filter on the series.
                                cmd = "SELECT seriesname, primary_idx, description FROM drms.allseries WHERE dbhost='" + optD['dbhost'] + "' AND seriesname " + matchOp + " '" + filter[2] + "'"
                            else:
                                # No namespace, use allseries, there is NO filter on the series.
                                cmd = "SELECT seriesname, primary_idx, description FROM drms.allseries WHERE dbhost='" + optD['dbhost'] + "'"
                    else:
                        # The series information originates from the internal database, in the allseries table. This must be
                        # joined with the whitelist.
                        if filter[0] or filter[2]:
                            # The user has specified a filter, so they are asking for a subset of the records in the joined table.
                            # Add to the where clause.
                            if filter[0] and filter[2]:
                                regExp = filter[0] + '.' + filter[2]
                            elif filter[0]:
                                regExp = filter[0]
                            else:
                                regExp = filter[2]
                            
                            cmd = "SELECT A.seriesname, A.primary_idx, A.description INTO TEMPORARY TABLE wlmerge FROM drms.allseries AS A, drms.whitelist AS W WHERE A.seriesname = W.seriesname AND A.dbhost='" + optD['dbhost'] + "' AND A.seriesname " + matchOp + " '" + regExp + "';"
                            cmd += "INSERT INTO wlmerge(seriesname, primary_idx, description) SELECT seriesname, primary_idx, description FROM drms.allseries WHERE dbhost='" + optD['dbserver'] + "' AND seriesname " + matchOp + " '" + regExp + "';"
                        else:
                            # The user has not specified a filter, so they are asking for all series.
                            cmd = "SELECT A.seriesname, A.primary_idx, A.description INTO TEMPORARY TABLE wlmerge FROM drms.allseries AS A, drms.whitelist AS W WHERE A.seriesname = W.seriesname AND A.dbhost='" + optD['dbhost'] + "';"
                            cmd += "INSERT INTO wlmerge(seriesname, primary_idx, description) SELECT seriesname, primary_idx, description FROM drms.allseries WHERE dbhost='" + optD['dbserver'] + "';"

                        cmd += 'SELECT seriesname, primary_idx, description FROM wlmerge'
                else:
                    # No filter was specfied at all.
                    if internalUser:
                        # The whitelist is irrelevant for internal users.
                        # No namespace, use allseries, there is NO filter on the series.
                        cmd = "SELECT seriesname, primary_idx, description FROM drms.allseries WHERE dbhost='" + optD['dbhost'] + "'"
                    else:
                        cmd = "SELECT A.seriesname, A.primary_idx, A.description INTO TEMPORARY TABLE wlmerge FROM drms.allseries AS A, drms.whitelist AS W WHERE A.seriesname = W.seriesname AND A.dbhost='" + optD['dbhost'] + "';"
                        cmd += "INSERT INTO wlmerge(seriesname, primary_idx, description) SELECT seriesname, primary_idx, description FROM drms.allseries WHERE dbhost='" + optD['dbserver'] + "';"
                        cmd += 'SELECT seriesname, primary_idx, description FROM wlmerge'

                cmd += ' ORDER BY seriesname'

                try:
                    cursor.execute(cmd)
                except psycopg2.Error as exc:
                    raise Exception('sql', optD['source'], exc.diag.message_primary + ': ' + cmd)

                try:
                    if optD['source'] == 'cl':
                        for record in cursor:
                            print(record[0])
                    else:
                        for record in cursor:
                            seriesObj = {}
                            seriesObj[record[0]] = {}

                            # Parse the primary_idx string into array elements.
                            seriesObj[record[0]]['primeIndex'] = record[1].split(',')
                            seriesObj[record[0]]['description'] = record[2]
                            seriesListArr.append(seriesObj)
                except psycopg2.Error as exc:
                    raise Exception('sql', optD['source'], exc.diag.message_primary)

                # Make sure that we always delete all records from the white-list table for the next use.
                # Depending on what this does to db vacuum (and how it works), we might need to re-consider how to do this.
                # Maybe all of this is kept in client memory so we never even touch the database? Who knows.
                #
                # If there is an exception that prevents this DELETE statement from running, then the insertion into
                # the whitelist table is also rolledback.
                cmd = 'DELETE FROM drms.whitelist'

                try:
                    cursor.execute(cmd)
                except psycopg2.Error as exc:
                    raise Exception('sql', optD['source'], exc.diag.message_primary + ': ' + cmd)
    except psycopg2.Error as exc:
        # Closes the cursor and connection.
        # If we are here, we know that optD['source'] exists.
        if optD['source'] == 'cgi':
            rtype = 'cgi'
            msg = 'Unable to connect to the database.'
            seriesListArr = []
        else:
            rtype = 'cl'
            msg = 'Unable to connect to the database.\n' + exc.diag.message_primary
            rv = RET_DBCONNECT

        if rtype == 'cgi':
            errMsg += msg
        else:
            print(msg, file=sys.stderr)
        # No need to close cursor - leaving the with block does that.
    except Exception as exc:
        if len(exc.args) != 3:
            msg = 'Unhandled exception.'
            raise # Re-raise
        
        etype, rtype, msg = exc.args

        if rtype == 'cgi':
            etype = etype + 'CGI'
            seriesListArr = []
        elif rtype == 'cl':
            etype = etype + 'CL'
        else:
            # The finally clause will print msg before sys.exit() is called.
            msg += 'Unknown run type.'
            sys.exit(RET_UNKNOWNRUNTYPE)

        if etype == 'getArgsCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'getArgsCL':
            msg += '\nUsage:\n  ' + getUsage()
            errMsg += msg
            rv = RET_BADARGS
        elif etype == 'drmsArgsCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'drmsArgsCL':
            rv = RET_DRMSPARAMS
        elif etype == 'parseFilterCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'parseFilterCL':
            rv = RET_BADFILTER
        elif etype == 'sqlCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'sqlCL':
            rv = RET_SQL
        elif etype == 'whitelistCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'whitelistCL':
            rv = RET_WHITELIST
        elif rtype == 'cl':
            raise # Re-raise
        else:
            # We created the error message for 'errMsg' already.
            pass

        if rtype == 'cgi':
            errMsg += msg
        else:
            print(msg, file=sys.stderr)

    if optD['source'] == 'cgi':
        insertJson(rootObj, 'errMsg', errMsg) # A string
        insertJson(rootObj, 'seriesList', seriesListArr)
        if not optD['noheader']:
            print('Content-type: application/json\n')
        print(json.dumps(rootObj))
        sys.exit(0)
    else:
        sys.exit(rv)

