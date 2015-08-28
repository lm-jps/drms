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
import psycopg2

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../../base/libs/py'))
from drmsCmdl import CmdlParser
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
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

class PublistDrmsParams(DRMSParams):

    def __init__(self):
        super(PublistDrmsParams, self).__init__()

    def get(self, name):
        val = super(PublistDrmsParams, self).get(name)

        if val is None:
            raise Exception('drmsParams', 'Unknown DRMS parameter: ' + name + '.')
        return val

class Arguments(object):

    def __init__(self, parser):
        # This could raise in a few places. Let the caller handle these exceptions.
        self.parser = parser
        
        # Parse the arguments.
        self.parse()
        
        # Set all args.
        self.setAllArgs()
        
    def parse(self):
        try:
            self.parsedArgs = self.parser.parse_args()      
        except Exception as exc:
            if len(exc.args) == 2:
                type, msg = exc
                  
                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise Exception('args', msg)
            else:
                raise # Re-raise

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
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise Exception('args', 'Unknown argument: ' + name + '.')

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

def GetArgs(publistDrmsParams):
    istat = bool(0)
    optD = {}
    
    # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
    if os.getenv('REQUEST_URI') or DEBUG_CGI:
        optD['source'] = 'cgi'
    else:
        optD['source'] = 'cmdline'

    # Options default to False.
    optD['descs'] = False
    optD['publist'] = False
    optD['nojson'] = False
    optD['json'] = False
    optD['cfg'] = publistDrmsParams.get('SLONY_CONFIG')

    if optD['source'] == 'cgi':
        arguments.setArg('source', 'cgi')
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
            insertJson(rootObj, 'errMsg', 'Invalid usage.\nUsage:\n  publist.py [ d=1 ] [ p=1 ] [ t=1 ] [ cfg=<configuration file> ] [ insts=<institution list> ] [ series=<series list> ]')
            raise Exception('getArgsForm', 'Invalid arguments.')
    else:
        parser = CmdlParser(usage='%(prog)s [ -djpt ] [ cfg=<configuration file> ] [ --insts=<institution list> ] [ --series=<series list> ]')
        parser.add_argument('-d', '--descs', help="Print each series' description.", dest='descs', action='store_true', default=False)
        parser.add_argument('-p', '--publist', help='Print the list of published series.', dest='publist', action='store_true', default=False)
        parser.add_argument('-t', '--text', help='Print output in text format (not JSON, no HTML headers).', dest='nojson', action='store_true', default=False)
        parser.add_argument('-j', '--json', help='Return a JSON object (no HTML headers).', dest='json', action='store_true', default=False)        
        parser.add_argument('-c', '--cfg', help='The configuration file that contains information needed to locate database information.', metavar='<slony configuration file>', dest='cfg', default=publistDrmsParams.get('SLONY_CONFIG'))
        parser.add_argument('-i', '--insts', help='A comma-separated list of institutions. The series to which these institutions are printed.', metavar='<institution list>', dest='insts', default=None)
        parser.add_argument('-s', '--series', help='A comma-separated list of series. The institutions subscribed to these series are printed.', metavar='<series list>', dest='series', default=None)
        parser.add_argument('-U', '--dbuser', help='The database user-account name to login to the database as.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])

        arguments = Arguments(parser)
        
        arguments.setArg('source', 'cmdline')

    return arguments

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
    try:
        publistDrmsParams = PublistDrmsParams()
    
        arguments = GetArgs(publistDrmsParams)
        if arguments is None:
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
            if not arguments.getArg('json'):
                print('Content-type: application/json\n')
            print(json.dumps(rootObj))
            arguments.setArg('source', 'form')
        elif etype == 'getArgsCmdline':
            print('Invalid arguments.')
            print('Usage:\n  publist.y [ -h ] [ -d ] [ -p ] [ -t ] [ -j ] cfg=<configuration file> [ insts=<institution list> ] [ series=<series list> ] [ dbuser=<dbuser> ]')
            arguments.setArg('source', 'cmdline')
            rv = RET_INVALIDARG

if rv == RET_SUCCESS:
    if arguments.getArg('cfg'):
        cfgFile = arguments.getArg('cfg')
    else:
        if arguments.getArg('source') == 'cmdline':
            print('Missing required argument, cfg=<configuration file>', file=sys.stderr)
            print('Usage:\n  publist.y [ -h ] [ -d ] [ -p ] [ -t ] [ -j ] cfg=<configuration file> [ insts=<institution list> ] [ series=<series list> ] [ dbuser=<dbuser> ]')
            rv = RET_INVALIDARG
        else:
            rootObj = {}
            listObj = {}
            insertJson(listObj, 'errMsg', "Required argument 'cfg' missing.")
            insertJson(rootObj, 'publist', listObj)
            if not arguments.getArg('json'):
                print('Content-type: application/json\n')
            print(json.dumps(rootObj))

    if rv == RET_SUCCESS:
        descs = arguments.getArg('descs')
        dispPubList = arguments.getArg('publist')
        nojson = arguments.getArg('nojson')
        jsonobj = arguments.getArg('json')
        insts = None
        series = None
        
        # If the user has specified neither -p, -i, or -s, then default to displaying the publication list
        if not arguments.getArg('publist') and not arguments.getArg('insts') and not arguments.getArg('series'):
            dispPubList = True

        if arguments.getArg('insts'):
            insts = arguments.getArg('insts').split(',')

        if arguments.getArg('series'):
            series = arguments.getArg('series').split(',')

        if arguments.getArg('dbuser'):
            dbuser = arguments.getArg('dbuser')
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

                # Need pubListDict if printing all institutions subscribed to series. Also need descsDict if 
                # insts argument was provided.
                if dispPubList or not series is None or not insts is None:
                    # First print all published series
                    pubListDict = {}
                    if descs:
                        descsDict = {}
                    else:
                        descsDict = None

                    pubList = GetPubList(cursor, pubListDict, descsDict)
                    
                # If we are printing text in the CGI context, print the HTTP header.
                if arguments.getArg('source') == 'cgi' and nojson:
                    print('Content-type: text/plain\n')
                
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
                                if descs:
                                    for oneSeries in subList:
                                        if oneSeries in descsDict:
                                            print(oneSeries + ' - ' + descsDict[oneSeries])
                                        else:
                                            print(oneSeries)
                                else:
                                    for oneSeries in subList:
                                        print(oneSeries)
                                print('')
                        else:
                            if descs:
                                insertJson(subListObj, inst, [oneSeries + ' - ' + descsDict[oneSeries] for oneSeries in subList])
                            else:
                                insertJson(subListObj, inst, subList)
            
                    if not nojson:
                        insertJson(rootObj, 'sublist', subListObj)

                if not(series is None):
                    if series[0].lower() != 'all':
                        # Print nodelists for the series specified in the subs list by first modifying pubList to contain the subs list,
                        # unless the user has specified all series, in which case pubList can be used as it (it contains a list of all
                        # published series).
                        pubList = series # pubList is all series otherwise
                    
                    # For each published series, print a list of institutions subscribed to that series
                    for seriesname in pubList:
                        warnMsg = []
                        if series[0].lower() != 'all' and not checkSeries(seriesname, pubListDict, warnMsg):
                            nodeList = [ ''.join(warnMsg) ]
                        else:
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
        elif etype == 'drmsParams':
            msg = dbmsg
            
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

if arguments.getArg('source') == 'cmdline':
    sys.exit(rv)
else:
    sys.exit(0)
