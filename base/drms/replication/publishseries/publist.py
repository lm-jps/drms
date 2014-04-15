#!/home/jsoc/bin/linux_x86_64/activepython

# This script prints series publication / subscription information. By default, a list of published series
# is printed, but if the caller specifies the "-subs" option, then for each series provided in the argument
# the list of institutions subscribed to that series is print. And if the caller specifies the "--insts"
# option, then for each institution provided in the argument the list of series to which the instituion
# is subscribed is printed.

import sys
import os.path
import getopt
import pwd
import re
import json
import cgi
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
import psycopg2

from toolbox import getCfg

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
# (c)fg    - path to configuration file
# (i)insts - [OPTIONAL] comma-separated list of institutions ('all' for all institutions). For each institution in this
#            list, print the list of series to which the institution is subscribed.
# (n)ojson - [OPTIONAL] If this flag is specified, then the output is formatted text. If not provided, then the
#            output is json.
# (s)ubs   - [OPTIONAL] comma-separated list of series ('all' for all series). For each series in this list, print
#            the list of institutions that are subscribed to that series.
# (u)ser   - [OPTIONAL] db user to connect to the db as. If not provided, then the linux user name is used.
#
# By default, a list of all published series is printed.

def GetArgs(args):
    istat = bool(0)
    optD = {}
    
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
                elif key in ('i' or 'insts'):
                    optD['insts'] = arg.split(',') # a list
                elif key in ('n'):
                    optD['nojson'] = 1
                elif key in ('s' or 'subs'):
                    optD['subs'] = arg.split(',') # a list

            optD['source'] = 'form'

    except ValueError:
        insertJson(rootObj, 'errMsg', 'Invalid usage.\nUsage:\n  publist.py cfg=<configuration file> [ insts=<institution list> ] [ series=<series list> ]')
        raise Exception('getArgsForm', 'Invalid arguments.')

    if not(optD):
        try:
            opts, remainder = getopt.getopt(args, "hc:i:ns:u:", ["cfg=", "insts=", "subs=", "user="])
            for opt, arg in opts:
                if opt == '-h':
                    print('Usage:\n  publist.y [ -h ] [ -n ] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]')
                    sys.exit(0)
                elif opt in ("-c", "--cfg"):
                    regexp = re.compile(r"(\S+)/?")
                    matchobj = regexp.match(arg)
                    if matchobj is None:
                        istat = bool(1)
                    else:
                        optD['cfg'] = matchobj.group(1)
                elif opt in ("-i", "--insts"):
                    optD['insts'] = arg.split(',') # a list
                elif opt in ("-n"):
                    optD['nojson'] = 1
                elif opt in ("-s", "--subs"):
                    optD['subs'] = arg.split(',') # a list
                elif opt in ("-u", "--user"):
                    optD['user'] = arg
                else:
                    optD[opt] = arg

            optD['source'] = 'cmdline'
    
        except getopt.GetoptError:
            print('Usage:\n  publist.py [-h] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]', file=sys.stderr)
            raise Exception('getArgs', 'Invalid arguments.')
	
    return optD

def GetPubList(cursor, plDict):
    list = []
    cmd = 'SELECT ' + SLONY_TABLE_NSP + "||'.'||" + SLONY_TABLE_REL + ' AS series FROM _' + SLONY_CLUSTER + '.' + SLONY_TABLE + ' ORDER BY series;'
    
    try:
        cursor.execute(cmd)
    
    except psycopg2.Error as exc:
        raise Exception('getPubList', exc.diag.message_primary)

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
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + cfgDict['kCfgTable'] + ' GROUP BY ' + CFG_TABLE_NODE

    try:
        cursor.execute(cmd)

    except psycopg2.Error as exc:
        raise Exception('getSubscribedInsts', exc.diag.message_primary)
        
    for record in cursor:
        list.append(record[0])
        
    return list

def GetNodeList(cursor, cfgDict, series):
    list = []
    cmd = 'SELECT ' + LST_TABLE_NODE + ' FROM ' + cfgDict['kLstTable'] + ' WHERE lower(' + LST_TABLE_SERIES + ") = '" + series.lower() + "' GROUP BY " + LST_TABLE_NODE + ' ORDER BY ' + LST_TABLE_NODE
    
    try:
        cursor.execute(cmd)
    
    except psycopg2.Error as exc:
        raise Exception('getNodeList', exc.diag.message_primary, series)
    
    for record in cursor:
        list.append(record[0])
    
    return list

def checkSeries(series, pubListDict, errMsg):
    if series in pubListDict:
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
    try:
        optD = GetArgs(sys.argv[1:])
        if optD is None:
            # Return JSON just in case this script was initiated by web.
            raise Exception('getArgsForm', 'No arguments provided.')
    
    except Exception as exc:
        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'getArgsForm':
            rootObj = {}
            listObj = {}
            insertJson(listObj, 'errMsg', msg)
            insertJson(rootObj, 'publist', listObj)
            print('Content-type: application/json\n')
            print(json.dumps(rootObj))
            sys.exit(0)
        elif etype == 'getArgs':
            print('Invalid arguments.')
            print('Usage:\n  publist.y [ -h ] [ -n ] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]')
            rv = RET_INVALIDARG

    if 'cfg' in optD:
        cfgFile = optD['cfg']
    else:
        if optD['source'] == 'cmdline':
            print('Missing required argument, -c <configuration file>', file=sys.stderr)
            print('Usage:\n  publist.y [ -h ] [ -n ] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]')
            rv = RET_INVALIDARG
        else:
            rootObj = {}
            listObj = {}
            insertJson(listObj, 'errMsg', "Required argument 'cfg' missing.")
            insertJson(rootObj, 'publist', listObj)
            print('Content-type: application/json\n')
            print(json.dumps(rootObj))
            sys.exit(0)

    if rv == RET_SUCCESS:
        insts = None
        nojson = None
        subs = None

        if 'insts' in optD:
            insts = optD['insts']

        if 'nojson' in optD:
            nojson = (optD['nojson'] == 1)

        if 'subs' in optD:
            subs = optD['subs']

        if 'user' in optD:
            dbuser = optD['user']
        else:
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
            
                # First print all published series
                pubListDict = {}
                pubList = GetPubList(cursor, pubListDict)
                
                if nojson:
                    print('All published series')
                    print('--------------------')
                    for series in pubList:
                        print(series)
                    print('')
                else:
                    insertJson(listObj, 'list', pubList)
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

                if not(subs is None):
                    if subs[0].lower() != 'all':
                        # Print nodelists for the series specified in the subs list by first modifying pubList to contain the subs list,
                        # unless the user has specified all series, in which case pubList can be used as it (it contains a list of all
                        # published series).
                        warnMsg = ()
                        pubListClean = [series for series in subs if checkSeries(series, pubListDict, warnMsg)]
                        if nojson:
                            print(''.join(warnMsg), file=sys.stderr)
                        pubList = pubListClean
                    
                    # For each published series, print a list of institutions subscribed to that series
                    for series in pubList:
                        nodeList = GetNodeList(cursor, cfgDict, series)
                        if nojson:
                            print('Institutions subscribed to series ' + series)
                            print('---------------------------------------------------')
                            if len(nodeList) == 0:
                                print('No institutions are subscribed to series ' + series + '.\n')
                            else:
                                for oneInst in nodeList:
                                    print(oneInst)
                                print('')
                        else:
                            insertJson(nodeListObj, series, nodeList)

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
            series = exc.args[2]
            msg = 'Error retrieving from the database the list of institutions subscribed to series ' + series + '.'

        if nojson:
            print(msg + '\n' + dbmsg, file=sys.stderr)
        else:
            insertJson(listObj, 'errMsg', msg + '\n' + dbmsg)

    # There is no need to call conn.commit() since connect() was called from within a with block. If an exception was not raised in the with block,
    # then a conn.commit() was implicitly called. If an exception was raised, then conn.rollback() was implicitly called.

    if not nojson:
        # dump json to a string - don't forget that fracking newline! Boy did that stump me.
        print('Content-type: application/json\n')
        print(json.dumps(rootObj))

if optD['source'] == 'cmdline':
    sys.exit(rv)
else:
    sys.exit(0)
