#!/home/jsoc/bin/linux_x86_64/activepython

# This script prints series publication / subscription information. By default, a list of published series
# is printed, but if the caller specifies the "-subs" option, then for each series provided in the argument
# the list of institutions subscribed to that series is print. And if the caller specifies the "--insts"
# option, then for each institution provided in the argument the list of series to which the instituion
# is subscribed is printed.

import sys
import os.path
import argparse
import pwd
import re
import json
import cgi
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
import psycopg2

from toolbox import getCfg

# Debug flag
DEBUG_CGI = False

# Return codes
RET_SUCCESS = 0
RET_INVALIDARG = 1
RET_DBCONNECT = 2

DB_NAME = 'jsoc'
DB_HOST = 'hmidb'
SLONY_CLUSTER = 'jsoc'
SLONY_TABLE = 'sl_table'
SLONY_TABLE_NSP = 'tab_nspname'
SLONY_TABLE_REL = 'tab_relname'

LST_TABLE_SERIES = 'series'
LST_TABLE_NODE = 'node'
CFG_TABLE_NODE = 'node'

# Read arguments
# (c)fg     - Path to configuration file
# (i)insts  - [OPTIONAL] Comma-separated list of institutions ('all' for all institutions). For each institution in this
#             list, print the list of series to which the institution is subscribed.
# (s)series - [OPTIONAL] comma-separated list of series ('all' for all series). For each series in this list, print
#             the list of institutions that are subscribed to that series.
# (u)ser    - [OPTIONAL] db user to connect to the db as. If not provided, then the linux user name is used.
#
# Flags (all optional)
# d         - In addition to a list of series, fetch series descriptions (and print them too, when printing series).
# p         - Do not print list of published series (by default, this list is generated).
# t         - If this flag is specified, then the output is formatted text. If not provided, then the
#             output is html that contains a JSON object.
# j         - Return pure JSON (no html headers).
#
# By default, a list of all published series is printed.
#
# This function contains two methods for getting program arguments. It uses one method, cgi.FieldStorage(), when the script
# is invoked from within a CGI context, and it uses another, the argparse module, when it is run from the command line.
# The script looks for the presence of the REQUEST_URI environment variable to indicate that program invocation came from within a
# CGI context (although this is not 100% reliable since the application calling this script could really do what it wants and not
# set this variable. In general, though, this environment variable will be set). cgi.FieldStorage() knows how to parse
# both HTTP GET and POST requests. A nice feature is that we can test the script as it runs in a CGI context
# by simply running on the command line with a single argument that is equivalent to an HTTP GET parameter string
# (e.g., cfg=/home/jsoc/cvs/Development/JSOC/proj/replication/etc/repserver.cfg&insts=kis&d=1&p=1).
#
# The script uses the argparse Python module when it is invoked from the command line. Although thie module nicely handles options
# and arguments whose names begin with a '-', it does not support other kinds of arguments, unless they are positional.
# And we're not interested in positional arguments. So I had to essentially implement the <key>=<val> arguments myself,
# treating them as optional positional arugments. As far as I can tell, Python does not provide a good <key>=<val>-argument cmd-line
# parser. I was then hoping I could convert the mixed <key>=<val>-argument and -arg-argument command-line into a form that
# could be used by cgi.FieldStorage() [ampersand-separated arguments]. However, the arguments in a string cannot be passed to
# to cgi.FieldStorage().
#
# So we're stuck with clunky argument parsing.
def parseArg(options, args, arg, regexp, isList):
    if not arg is None:
        matchobj = regexp.match(arg)
        if not(matchobj is None):
            # Ensure that the name of the argument is legitimate.
            if matchobj.group(1) in args:
                if isList:
                    options[matchobj.group(1)] = matchobj.group(2).split(',')
                else:
                    options[matchobj.group(1)] = matchobj.group(2)
            else:
                raise Exception('getArgsCmdline', 'Invalid arguments ' + matchobj.group(1) + '.')
        else:
            raise Exception('getArgsCmdline', 'Invalid argument format ' + arg + '.')

def GetArgs(args):
    istat = bool(0)
    optD = {}
    
    # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
    if os.getenv('REQUEST_URI'):
        optD['source'] = 'cgi'
    else:
        optD['source'] = 'cmdline'

    # Options default to False.
    optD['descs'] = False
    optD['publist'] = False
    optD['nojson'] = False
    optD['json'] = False

    if optD['source'] == 'cgi' or DEBUG_CGI:
        try:
            # Try to get arguments with the cgi module. If that doesn't work, then fetch them from the command line.
            arguments = cgi.FieldStorage()
        
            if arguments:
                for key in arguments.keys():
                    val = arguments.getvalue(key)
                    if key in ('c', 'cfg'):
                        regexp = re.compile(r"(\S+)/?")
                        matchobj = regexp.match(val)
                        if not(matchobj is None):
                            optD['cfg'] = matchobj.group(1)
                    elif key in ('d'):
                        optD['descs'] = True
                    elif key in ('p'):
                        optD['publist'] = True
                    elif key in ('t'):
                        optD['nojson'] = True
                    elif key in ('j'):
                        optD['json'] = True
                    elif key in ('i', 'insts'):
                        optD['insts'] = val.split(',') # a list
                    elif key in ('s', 'series'):
                        optD['series'] = val.split(',') # a list

        except ValueError:
            insertJson(rootObj, 'errMsg', 'Invalid usage.\nUsage:\n  publist.py [ d=1 ] [ p=1 ] [ t=1 ] cfg=<configuration file> [ insts=<institution list> ] [ series=<series list> ]')
            raise Exception('getArgsForm', 'Invalid arguments.')
    else:
        # Try this argparse stuff.
        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--descs', help="Print each series' description.", action='store_true')
        parser.add_argument('-p', '--publist', help='Print the list of published series.', action='store_true')
        parser.add_argument('-t', '--text', help='Print output in text format (not html containing JSON).', action='store_true')
        parser.add_argument('-j', '--json', help='Return a JSON object.', action='store_true')
        parser.add_argument('cfg', help='[cfg=<configuration file>] The configuration file that contains information needed to locate database information.', nargs='?')
        parser.add_argument('insts', help='[insts=<institution list>] A comma-separated list of institutions. The series to which these institutions are printed.', nargs='?')
        parser.add_argument('series', help='[series=<series list>] A comma-separated list of series. The institutions subscribed to these series are printed.', nargs='?')
        parser.add_argument('dbuser', help='[dbuser=<database user>] The database user-account name to login to the database as.', nargs='?')
        args = parser.parse_args()
        
        # Collect the optional arguments.
        if args.descs:
            optD['descs'] = True
        if args.publist:
            optD['publist'] = True
        if args.text:
            optD['nojson'] = True
        if args.json:
            optD['json'] = True

        # The non-optional arguments are positional. It is possible that the caller could put the arguments for one argument into
        # the position for a different argument. Positional arguments are a nuisance. So we must parse the values of these three arguments
        # and get the name of the arguments from the LHS of the equal sign.
        regexp = re.compile(r"(\S+)=(\S+)")
        parseArg(optD, args, args.cfg, regexp, False)
        parseArg(optD, args, args.insts, regexp, True)
        parseArg(optD, args, args.series, regexp, True)
        parseArg(optD, args, args.dbuser, regexp, False)

    return optD

def GetPubList(cursor, plDict, descsDict):
    list = []
    if not descsDict is None:
        cmd = 'SELECT T1.' + SLONY_TABLE_NSP + "||'.'||T1." + SLONY_TABLE_REL + ' AS series, T2.description AS description FROM _' + SLONY_CLUSTER + '.' + SLONY_TABLE + ' T1 LEFT OUTER JOIN drms_series() T2 ON (T1.' + SLONY_TABLE_NSP + "||'.'||T1." + SLONY_TABLE_REL + ' = lower(T2.seriesname)) ORDER BY SERIES'
    else:
        cmd = 'SELECT ' + SLONY_TABLE_NSP + "||'.'||" + SLONY_TABLE_REL + ' AS series FROM _' + SLONY_CLUSTER + '.' + SLONY_TABLE + ' ORDER BY series;'
    
    try:
        cursor.execute(cmd)
    
    except psycopg2.Error as exc:
        raise Exception('getPubList', exc.diag.message_primary)

    if not descsDict is None:
        for record in cursor:
            list.append(record[0])
            descsDict[record[0]] = record[1]
            plDict[record[0]] = 'Y'
    else:
        for record in cursor:
            list.append(record[0])
            plDict[record[0]] = 'Y'

    return list

def GetSubList(cursor, cfgDict, inst):
    list = []
    cmd = 'SELECT ' + LST_TABLE_SERIES + ' FROM ' + cfgDict['kLstTable'] + ' WHERE ' + LST_TABLE_NODE + " = '" + inst + "' ORDER BY " + LST_TABLE_SERIES

    try:
        cursor.execute(cmd)

    except psycopg2.Error as exc:
        raise Exception('getSubList', exc.diag.message_primary, inst)

    for record in cursor:
        list.append(record[0])
            
    return list

def GetSubscribedInsts(cursor, cfgDict):
    list = []
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + cfgDict['kCfgTable'] + ' GROUP BY ' + CFG_TABLE_NODE + ' ORDER BY lower(' + CFG_TABLE_NODE + ')'

    try:
        cursor.execute(cmd)

    except psycopg2.Error as exc:
        raise Exception('getSubscribedInsts', exc.diag.message_primary)
        
    for record in cursor:
        list.append(record[0])
        
    return list

def GetNodeList(cursor, cfgDict, series):
    list = []
    cmd = 'SELECT ' + LST_TABLE_NODE + ' FROM ' + cfgDict['kLstTable'] + ' WHERE lower(' + LST_TABLE_SERIES + ") = '" + series.lower() + "' GROUP BY " + LST_TABLE_NODE + ' ORDER BY lower(' + LST_TABLE_NODE + ')'
    
    try:
        cursor.execute(cmd)
    
    except psycopg2.Error as exc:
        raise Exception('getNodeList', exc.diag.message_primary, series)
    
    for record in cursor:
        list.append(record[0])
    
    return list

def checkSeries(series, pubListDict, errMsg):
    if series.lower() in pubListDict:
        return True
    else:
        errMsg.extend(list('Series ' + series + ' is not published; skipping.'))
        
        return False

def insertJson(jsonDict, property, value):
    jsonDict[property] = value

rv = RET_SUCCESS
cfgDict = {}

# Parse arguments
if __name__ == "__main__":
    optD = {}
    try:
        optD = GetArgs(sys.argv[1:])
        if optD is None:
            # Return JSON just in case this script was initiated by web.
            raise Exception('getArgsForm', 'No arguments provided.')
    
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'getArgsForm':
            rootObj = {}
            listObj = {}
            insertJson(listObj, 'errMsg', msg)
            insertJson(rootObj, 'publist', listObj)
            if 'json' not in optD or not optD['json']:
                print('Content-type: application/json\n')
            print(json.dumps(rootObj))
            optD['source'] = 'form'
        elif etype == 'getArgsCmdline':
            print('Invalid arguments.')
            print('Usage:\n  publist.y [ -h ] [ -d ] [ -p ] [ -t ] [ -j ] cfg=<configuration file> [ insts=<institution list> ] [ series=<series list> ] [ dbuser=<dbuser> ]')
            optD['source'] = 'cmdline'
            rv = RET_INVALIDARG

if rv == RET_SUCCESS:
    if 'cfg' in optD:
        cfgFile = optD['cfg']
    else:
        if optD['source'] == 'cmdline':
            print('Missing required argument, cfg=<configuration file>', file=sys.stderr)
            print('Usage:\n  publist.y [ -h ] [ -d ] [ -p ] [ -t ] [ -j ] cfg=<configuration file> [ insts=<institution list> ] [ series=<series list> ] [ dbuser=<dbuser> ]')
            rv = RET_INVALIDARG
        else:
            rootObj = {}
            listObj = {}
            insertJson(listObj, 'errMsg', "Required argument 'cfg' missing.")
            insertJson(rootObj, 'publist', listObj)
            if 'json' not in optD or not optD['json']:
                print('Content-type: application/json\n')
            print(json.dumps(rootObj))

    if rv == RET_SUCCESS:
        descs = optD['descs']
        dispPubList = optD['publist']
        nojson = optD['nojson']
        jsonobj = optD['json']
        insts = None
        series = None
        
        # If the user has specified neither -p, -i, or -s, then default to displaying the publication list
        if not(optD['publist']) and not('insts' in optD) and not('series' in optD):
            dispPubList = True

        if 'insts' in optD:
            insts = optD['insts']

        if 'series' in optD:
            series = optD['series']

        if 'dbuser' in optD:
            dbuser = optD['dbuser']
        else:
            # Assume the dbuser is the linux user.
            dbuser = pwd.getpwuid(os.getuid())[0]

if rv == RET_SUCCESS:
    rv = getCfg(cfgFile, cfgDict)

if rv == RET_SUCCESS:
    # Connect to the database
    try:
        # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        with psycopg2.connect(database=DB_NAME, user=dbuser, host=DB_HOST, port=5432) as conn:
            with conn.cursor() as cursor:
                if not nojson:
                    rootObj = {}
                    listObj = {}
                    subListObj = {}
                    nodeListObj = {}

                # Need pubListDict if printing all instituions subscribed to series
                if dispPubList or not(series is None):
                    # First print all published series
                    pubListDict = {}
                    if descs:
                        descsDict = {}
                    else:
                        descsDict = None

                    pubList = GetPubList(cursor, pubListDict, descsDict)

                if dispPubList:
                    if nojson:
                        print('All published series')
                        print('--------------------')
                    
                        if descs:
                            for seriesname in pubList:
                                if seriesname in descsDict:
                                    print(seriesname + ' - ' + descsDict[seriesname])
                                else:
                                    print(seriesname)
                        else:
                            for seriesname in pubList:
                                print(seriesname)
                            print('')
                    else:
                        if descs:
                            # Make a list of elements, each of which is a tuple of length 2: (name, description)
                            pubListWithDescs = [(seriesname, descsDict[seriesname]) for seriesname in pubList]
                            insertJson(listObj, 'list', pubListWithDescs)
                        else:
                            # Make a list of elements, each of which is a tuple of length 1: (name).
                            pubListWithoutDescs = [(seriesname,) for seriesname in pubList]
                            insertJson(listObj, 'list', pubListWithoutDescs)
                    
                        insertJson(rootObj, 'publist', listObj)

                if not(insts is None):
                    if insts[0].lower() == 'all'.lower():
                        # Print sublists for all institutions by first querying the cfg db table for a list of all nodes
                        # that have at least one subscription, and then by pushing each node's name into the insts
                        # list (after removing the 'all' element).
                        allInsts = GetSubscribedInsts(cursor, cfgDict)
                        insts = [inst for inst in allInsts]
                    
                    # For each institution, print a list of subscribed-to series
                    for inst in insts:
                        subList = GetSubList(cursor, cfgDict, inst)
                        if nojson:
                            print('Series subscribed to by ' + inst)
                            print('------------------------------')
                        
                            if len(subList) == 0:
                                print('Institution ' + inst + ' is not subscribed to any published series.\n')
                            else:
                                for oneSeries in subList:
                                    print(oneSeries)
                                print('')
                        else:
                            insertJson(subListObj, inst, subList)
            
                    if not nojson:
                        insertJson(rootObj, 'sublist', subListObj)

                if not(series is None):
                    if series[0].lower() != 'all':
                        # Print nodelists for the series specified in the subs list by first modifying pubList to contain the subs list,
                        # unless the user has specified all series, in which case pubList can be used as it (it contains a list of all
                        # published series).
                        warnMsg = []
                        pubListClean = [seriesname for seriesname in series if checkSeries(seriesname, pubListDict, warnMsg)]
                        if nojson:
                            print(''.join(warnMsg), file=sys.stderr)
                        pubList = pubListClean
                    
                    # For each published series, print a list of institutions subscribed to that series
                    for seriesname in pubList:
                        nodeList = GetNodeList(cursor, cfgDict, seriesname)
                        if nojson:
                            print('Institutions subscribed to series ' + seriesname)
                            print('---------------------------------------------------')
                            if len(nodeList) == 0:
                                print('No institutions are subscribed to series ' + seriesname + '.\n')
                            else:
                                for oneInst in nodeList:
                                    print(oneInst)
                                print('')
                        else:
                            insertJson(nodeListObj, seriesname, nodeList)

                    if not nojson:
                        insertJson(rootObj, 'nodelist', nodeListObj)

    except psycopg2.Error as exc:
        # Closes the cursor and connection
        print('Unable to connect to the database', file=sys.stderr)
        print(exc.diag.message_primary, file=sys.stderr)
        # No need to close cursor - leaving the with block does that.
        rv = RET_DBCONNECT

    except Exception as exc:
        etype = exc.args[0]
        dbmsg = exc.args[1]
        
        if etype == 'getPubList':
            msg = 'Error retrieving from the database the publication list.'
        elif etype == 'getSubList':
            inst = exc.args[2]
            msg = 'Error retrieving from the database the subscription list for institution ' + inst + '.'
        elif etype == 'getSubscribedInsts':
            msg = 'Error retrieving from the database the list of institutions subscribed to at least one series.'
        elif etype == 'getNodeList':
            seriesname = exc.args[2]
            msg = 'Error retrieving from the database the list of institutions subscribed to series ' + seriesname + '.'

        if nojson:
            print(msg + '\n' + dbmsg, file=sys.stderr)
        else:
            insertJson(listObj, 'errMsg', msg + '\n' + dbmsg)

    # There is no need to call conn.commit() since connect() was called from within a with block. If an exception was not raised in the with block,
    # then a conn.commit() was implicitly called. If an exception was raised, then conn.rollback() was implicitly called.

    if not nojson:
        # dump json to a string - don't forget that fracking newline! Boy did that stump me.
        if not jsonobj:
            print('Content-type: application/json\n')
        print(json.dumps(rootObj))

if optD['source'] == 'cmdline':
    sys.exit(rv)
else:
    sys.exit(0)
