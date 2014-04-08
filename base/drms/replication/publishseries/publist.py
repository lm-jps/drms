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
        opts, remainder = getopt.getopt(args, "hc:i:ns:u:", ["cfg=", "insts=", "subs=", "user="])
    except getopt.GetoptError:
        print('Usage:\n  publist.py [-h] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]', file=sys.stderr)
        istat = bool(1)
    
    if istat == bool(0):
        for opt, arg in opts:
            if opt == '-h':
                print('Usage:\n  publist.y [-h] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]')
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
	
    return optD

def GetPubList(cursor, plDict):
    istat = bool(0)
    list = []
    cmd = 'SELECT ' + SLONY_TABLE_NSP + "||'.'||" + SLONY_TABLE_REL + ' AS series FROM _' + SLONY_CLUSTER + '.' + SLONY_TABLE + ' ORDER BY series;'
    
    try:
        cursor.execute(cmd)
    
    except psycopg2.Error as exc:
        print(exc.diag.message_primary, file=sys.stderr)
        istat = bool(1)

    if not(istat):
        for record in cursor:
            list.append(record[0])
            plDict[record[0]] = 'Y'

    return list

def GetSubList(cursor, cfgDict, inst):
    istat = bool(0)
    list = []
    cmd = 'SELECT ' + LST_TABLE_SERIES + ' FROM ' + cfgDict['kLstTable'] + ' WHERE ' + LST_TABLE_NODE + " = '" + inst + "' ORDER BY " + LST_TABLE_SERIES

    try:
        cursor.execute(cmd)

    except psycopg2.Error as exc:
        print(exc.diag.message_primary, file=sys.stderr)
        istat = bool(1)
        
    if not(istat):
        for record in cursor:
            list.append(record[0])
            
    return list

def GetSubscribedInsts(cursor, cfgDict):
    istat = bool(0)
    list = []
    cmd = 'SELECT ' + CFG_TABLE_NODE + ' FROM ' + cfgDict['kCfgTable'] + ' GROUP BY ' + CFG_TABLE_NODE

    try:
        cursor.execute(cmd)

    except psycopg2.Error as exc:
        print(exc.diag.message_primary, file=sys.stderr)
        istat = bool(1)
        
    if not(istat):
        for record in cursor:
            list.append(record[0])
        
    return list

def GetNodeList(cursor, cfgDict, series):
    istat = bool(0)
    list = []
    cmd = 'SELECT ' + LST_TABLE_NODE + ' FROM ' + cfgDict['kLstTable'] + ' WHERE lower(' + LST_TABLE_SERIES + ") = '" + series.lower() + "' GROUP BY " + LST_TABLE_NODE + ' ORDER BY ' + LST_TABLE_NODE
    
    try:
        cursor.execute(cmd)
    
    except psycopg2.Error as exc:
        print(exc.diag.message_primary, file=sys.stderr)
        istat = bool(1)
    
    if not(istat):
        for record in cursor:
            list.append(record[0])
    
    return list

def checkSeries(series, pubListDict):
    if series in pubList:
        return True
    else:
        print('Series ' + series + ' is not published; skipping.', file=sys.stderr)
        return False


rv = RET_SUCCESS
cfgDict = {}

# Parse arguments
if __name__ == "__main__":
    optD = GetArgs(sys.argv[1:])
    if not(optD is None):
        if 'cfg' in optD:
            cfgFile = optD['cfg']
        else:
            print('Missing required argument, -c <configuration file>', file=sys.stderr)
            print('Usage:\n  publist.y [-h] -c <configuration file> [ -i <institution list> ] [ -s <series list> ]')
            rv = RET_INVALIDARG

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
                # First print all published series
                pubListDict = {}
                print('All published series')
                print('--------------------')
                pubList = GetPubList(cursor, pubListDict)
                for series in pubList:
                    print(series)
                print('')
                
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
                        print('Series subscribed to by ' + inst)
                        print('------------------------------')
                        if len(subList) == 0:
                            print('Institution ' + inst + ' is not subscribed to any published series.\n')
                        else:
                            for oneSeries in subList:
                                print(oneSeries)
                            print('')

                if not(subs is None):
                    if subs[0].lower() != 'all':
                        # Print nodelists for the series specified in the subs list by first modifying pubList to contain the subs list,
                        # unless the user has specified all series, in which case pubList can be used as it (it contains a list of all
                        # published series).
                        pubListClean = [series for series in subs if checkSeries(series, pubListDict)]
                        pubList = pubListClean
                    
                    # For each published series, print a list of institutions subscribed to that series
                    for series in pubList:
                        nodeList = GetNodeList(cursor, cfgDict, series)
                        print('Institutions subscribed to series ' + series)
                        print('---------------------------------------------------')
                        if len(nodeList) == 0:
                            print('No institutions are subscribed to series ' + series + '.\n')
                        else:
                            for oneInst in nodeList:
                                print(oneInst)
                            print('')

    except psycopg2.Error as exc:
        # Closes the cursor and connection
        print('Unable to connect to the database', file=sys.stderr)
        print(exc.diag.message_primary, file=sys.stderr)
        # No need to close cursor - leaving the with block does that.
        rv = RET_DBCONNECT

    # There is no need to call conn.commit() since connect() was called from within a with block. If an exception was not raised in the with block,
    # then a conn.commit() was implicitly called. If an exception was raised, then conn.rollback() was implicitly called.
