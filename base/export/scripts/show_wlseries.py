#!/home/jsoc/bin/linux_x86_64/activepython

# Displays a list of all series in the database specified by the dbhost argument. If the host is not our internal host, then
# this script will also list the white-listed series in the internal host.

import sys
import os.path
import argparse
import pwd
import re
import json
import cgi
import psycopg2

# Debug flag
DEBUG_CGI = False

# Return codes
RET_SUCCESS = 0
RET_INVALIDARG = 1
RET_DBCONNECT = 2

def getUsage():
    return 'show_wlseries.py intdsn=<internal-db dsn> [ extdsn=<external-db dsn> wltab=<white-list table> ] [ filter=<series name filter> ]\n\n  (where dsn=dbname:<db name>;dbhost=<hostname>;dbport=<port number>[;dbuser=<PG user>])'

def parseArg(options, args, arg, regexp, isList, etype):
    if not arg is None:
        matchobj = regexp.match(arg)
        if not(matchobj is None):
            # Ensure that the name of the argument is legitimate.
            if args is None or matchobj.group(1) in args:
                if isList:
                    options[matchobj.group(1)] = matchobj.group(2).split(',')
                else:
                    options[matchobj.group(1)] = matchobj.group(2)
            else:
                raise Exception(etype, 'Arguments \'' + matchobj.group(1) + '\' not recognized.')
        else:
            raise Exception(etype, 'Invalid argument format ' + arg + '.')

def parseDSN(dsnStr, etype):
    dsnD = {}
    if not dsnStr is None:
        regexp = re.compile(r"(\S+)=(\S+)")
        dsnArr = dsnStr.split(';')
        for elem in dsnArr:
            parseArg(dsnD, None, elem, regexp, False, etype)

    if not 'dbname' in dsnD or not 'dbhost' in dsnD or not 'dbport' in dsnD:
        raise Exception(etype, 'Invalid dsn: ' + dsnStr)

    if not 'dbuser' in dsnD:
        dsnD['dbuser'] = pwd.getpwuid(os.getuid())[0]

    return dsnD

def GetArgs(args):
    istat = bool(0)
    optD = {}
    etype = ''
    
    # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
    if os.getenv('REQUEST_URI'):
        optD['source'] = 'cgi'
        etype = 'getArgsCgi'
    else:
        optD['source'] = 'cmdline'
        etype = 'getArgsCmdline'
    
    if optD['source'] == 'cgi' or DEBUG_CGI:
        try:
            # Try to get arguments with the cgi module. If that doesn't work, then fetch them from the command line.
            arguments = cgi.FieldStorage()
            
            if arguments:
                for key in arguments.keys():
                    val = arguments.getvalue(key)
                    if key in ('f', 'filter'):
                        optD['filter'] = val
                    elif key in ('i', 'intdsn'):
                        optD[''] = val
                    elif key in ('e', 'extdsn'):
                        optD[''] = val
                    elif key in ('w', 'wltab'):
                        optD[''] = val

        except ValueError:
            insertJson(rootObj, 'errMsg', getUsage())
            raise Exception(etype, 'Invalid arguments.')
    else:
        # Try this argparse stuff.
        parser = argparse.ArgumentParser()
        parser.add_argument('filter', help='[filter=<regexp>] A regular expression identifying series to search for.', nargs='?')
        parser.add_argument('intdsn', help='[intdsn=<internal dsn>] A semicolon-separated string of parameters that identifies the internal database instance.', nargs='?')
        parser.add_argument('extdsn', help='[extdsn=<external dsn>] A semicolon-separated string of parameters that identifies the external database instance.', nargs='?')
        parser.add_argument('wltab', help='[wltab=<white-list table>] The database table in the internal database that enumerates the white-listed series.', nargs='?')
        args = parser.parse_args()
        
        # The non-optional arguments are positional. It is possible that the caller could put the arguments for one argument into
        # the position for a different argument. Positional arguments are a nuisance. So we must parse the values of these three arguments
        # and get the name of the arguments from the LHS of the equal sign.
        regexp = re.compile(r"([^\s=]+)\s*=\s*(\S.*)")
        parseArg(optD, args, args.filter, regexp, False, etype)
        parseArg(optD, args, args.intdsn, regexp, False, etype)
        parseArg(optD, args, args.extdsn, regexp, False, etype)
        parseArg(optD, args, args.wltab, regexp, False, etype)
    
    # Make sure all required arguments are present.
    if not 'intdsn' in optD:
        raise Exception(etype, 'Missing required argument: intdsn.')
    elif 'extdsn' in optD and 'wltab' not in optD:
        raise Exception(etype, 'Missing required argument: wltab.')

    # Parse both dsns.
    optD['intdsn'] = parseDSN(optD['intdsn'], etype)
    if 'extdsn' in optD:
        optD['extdsn'] = parseDSN(optD['extdsn'], etype)
    else:
        optD['extdsn'] = None

    # If any optional arguments were not present, set them to None in the options dictionary.
    if not 'filter' in optD:
        optD['filter'] = None
    if not 'wltab' in optD:
        optD['wltab'] = None

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
        print('really')
    else:
        print('bummer ' + filterStr)
        exclude = False
        remainder = filterStr

    # Look for a namespace.
    regexp = re.compile(r"\s*(\S+)(\.\S+)(.*)")
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
        raise Exception('parseFilter', 'Invaid filter ' + filterStr)

    filter = (namespace, exclude, regexpStr)

    return filter
        
rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = {}
    try:
        optD = GetArgs(sys.argv[1:])
        if optD is None:
            # Return JSON just in case this script was initiated by web.
            raise Exception('getArgsCgi', 'No arguments provided.')
    
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]
        
        if etype == 'getArgsCgi':
            rootObj = {}
            listObj = []
            insertJson(rootObj, 'seriesList', listObj) # An array
            insertJson(rootObj, 'errMsg', msg) # A string

            print('Content-type: application/json\n')
            print(json.dumps(rootObj))
            optD['source'] = 'cgi'
        elif etype == 'getArgsCmdline':
            print(msg)
            print('Usage:\n  ' + getUsage())
            optD['source'] = 'cmdline'
            rv = RET_INVALIDARG
        else:
            raise # Re-raise

if rv == RET_SUCCESS:
    # Connect to the database
    try:
        # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        with psycopg2.connect(database=optD['intdsn']['dbname'], user=optD['intdsn']['dbuser'], host=optD['intdsn']['dbhost'], port=optD['intdsn']['dbport']) as conn:
            with conn.cursor() as cursor:
                # The filter is an extended POSIX regular expression, optionally preceded by the word NOT.
                # filter is a tuple. The first element is a namespace (if not None, then search in this namespace only).
                # The second is a boolean. If True, then the filter specifies which series to exclude. If False,
                # then the filter specifies with series to include.
                # The third is a regular expression to send to PG that, in combination with the second element,
                # selects series to print. This element can be None, in which case all series are printed.
                if optD['filter'] is not None:
                    filter = parseFilter(optD['filter'])
                    if filter[1]:
                        matchOp = '!~*'
                    else:
                        matchOp = '~*'
                
                    if filter[2] is None:
                        cmd = 'SELECT seriesname FROM drms_series()'
                    else:
                        if filter[0] is not None and not filter[1]:
                            cmd = 'SELECT seriesname FROM ' + filter[0] + '.drms_series WHERE seriesname ' + matchOp + " '" + filter[2] + "'"
                        else:
                            print('here')
                            cmd = 'SELECT seriesname FROM drms_series() WHERE seriesname ' + matchOp + " '" + filter[2] + "'"
                            print(cmd)
                                
                else:
                    cmd = 'SELECT seriesname FROM drms_series()'

                try:
                    cursor.execute(cmd)

                except psycopg2.Error as exc:
                    raise Exception('sql', exc.diag.message_primary, cmd)

                for record in cursor:
                    print(record[0])

    except psycopg2.Error as exc:
        # Closes the cursor and connection
        print('Unable to connect to the database', file=sys.stderr)
        print(exc.diag.message_primary, file=sys.stderr)

        # No need to close cursor - leaving the with block does that.
        rv = RET_DBCONNECT

    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'parseFilter':
            if optD['source'] == 'cgi':
                rootObj = {}
                listObj = []
                insertJson(rootObj, 'seriesList', listObj) # An array
                insertJson(rootObj, 'errMsg', msg) # A string
            
                print('Content-type: application/json\n')
                print(json.dumps(rootObj))
            elif optD['source'] == 'cmdline':
                print(msg, file=sys.stderr)
            else:
                raise # Re-raise
        else:
            raise # Re-raise