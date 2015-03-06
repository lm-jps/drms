#!/usr/bin/env python

# Returns { "server" : "hmidb2", "series" : [{ "hmi.M_45s" : { "server" : "hmidb" } }, { "hmi.internalonly" : { "server" : "hmidb2" }}]}

from __future__ import print_function

import sys
import os
import pwd
from subprocess import check_output, check_call, CalledProcessError, STDOUT
import json
import cgi
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
RET_UNKNOWNRUNTYPE = 3
RET_WHITELIST = 4
RET_ARCH = 5
RET_SHOWSERIES = 6

def getDRMSParam(drmsParams, param):
    rv = drmsParams.get(param)
    if not rv:
        raise Exception('drmsParams', 'DRMS parameter ' + param + ' is not defined.', RET_DRMSPARAMS)
    
    return rv

def getOption(val, default):
    if val:
        return val
    else:
        return default

def getArgs(drmsParams):
    istat = False
    optD = {}
    etype = ''
    useCGI = False

    # Ack - A disaster of Hellerian proportions. We want to determine the method of argument parsing chosen by the user, but in order to select a method the user
    # must set an argument! So, parse the cmd-line and if the debug flag is set, then strip that and pass the rest to the cgi command-line parser.
    optD['debug'] = False
    index = 0
    for arg in sys.argv:
        if arg in ('-d', '--debug'):
            useCGI = True
            optD['debug'] = True
            sys.argv.pop(index)
            break
        index += 1

    # Use REQUEST_URI as surrogate for the invocation coming from a CGI request.
    if os.getenv('REQUEST_URI') or DEBUG_CGI or useCGI:
        optD['source'] = 'cgi'
    else:
        optD['source'] = 'cl'

    if optD['source'] == 'cgi':
        # Options
        optD['noheader'] = None
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
                        if int(val) == 1:
                            optD['noheader'] = True
                        else:
                            optD['noheader'] = False
                    elif key in ('H', 'dbhost'):
                        optD['dbhost'] = val
                    elif key in ('P', 'dbport'):
                        optD['dbport'] = val
                    elif key in ('N', 'dbname'):
                        optD['dbname'] = val
                    elif key in ('U', 'dbuser'):
                        optD['dbuser'] = val
                    elif key in ('s', 'series'):
                        optD['series'] = map(lambda s: s.strip(), val.split(','))
                    elif key in ('w', 'wlfile'):
                        optD['wlfile'] = val

            optD['noheader'] = bool(getOption(optD['noheader'], False))
            optD['dbport'] = int(getOption(optD['dbport'], getDRMSParam(drmsParams, 'DRMSPGPORT')))
            optD['dbname'] = getOption(optD['dbname'], getDRMSParam(drmsParams, 'DBNAME'))
            optD['dbuser'] = getOption(optD['dbuser'], pwd.getpwuid(os.getuid())[0])
            optD['wlfile'] = getOption(optD['wlfile'], getDRMSParam(drmsParams, 'WL_FILE'))

            # Enforce requirements.
            if not 'dbhost' in optD:
                raise Exception('getArgs', 'Missing required argument ' + "'dbhost'.", RET_BADARGS)
            elif not 'series' in optD:
                raise Exception('getArgs', 'Missing required argument ' + "'series'.", RET_BADARGS)
        except ValueError as exc:
            raise Exception('getArgs', 'Invalid arguments.', RET_BADARGS)
    else:
        # Non CGI invocation.
        try:
            parser = CmdlParser(usage='%(prog)s [ -dhn ] dbhost=<db host> series=<DRMS series list> [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user> ] [--wlfile=<white-list text file> ]')

            # Required
            parser.add_argument('H', 'dbhost', help='The machine hosting the EXTERNAL database that serves DRMS data series names.', metavar='<db host>', dest='dbhost', required=True)
            parser.add_argument('s', 'series', help='A comma-separated list of series to be checked.', metavar='<series>', dest='series', required=True)

            # Optional
            parser.add_argument('-d', '--debug', help='Run in CGI mode, and print helpful diagnostics.).', dest='debug', action='store_true', default=False)
            parser.add_argument('-n', '--noheader', help='Supress the HTML header (cgi runs only).', dest='noheader', action='store_true', default=False)
            parser.add_argument('-P', '--dbport', help='The port on the machine hosting DRMS data series names.', metavar='<db host port>', dest='dbport', default=drmsParams.get('DRMSPGPORT'))
            parser.add_argument('-N', '--dbname', help='The name of the database serving DRMS series names.', metavar='<db name>', dest='dbname', default=getDRMSParam(drmsParams, 'DBNAME'))
            parser.add_argument('-U', '--dbuser', help='The user to log-in to the serving database as.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
            parser.add_argument('-w', '--wlfile', help='The text file containing the definitive list of internal series accessible via the external web site.', metavar='<white-list file>', dest='wlfile', default=getDRMSParam(drmsParams, 'WL_FILE'))

            args = parser.parse_args()
        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise

            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
                raise Exception('getArgs', 'Unable to parse command-line arguments. ' + msg + '\n' + parser.format_help(), RET_BADARGS)
            else:
                raise # Re-raise.

        # Cannot loop through attributes since args has extra attributes we do not want.
        optD['debug'] = args.debug
        optD['noheader'] = args.noheader
        optD['dbhost'] = args.dbhost
        optD['series'] = map(lambda s: s.strip(), args.series.split(','))
        optD['dbport'] = int(args.dbport)
        optD['dbname'] = args.dbname
        optD['dbuser'] = args.dbuser
        optD['wlfile'] = args.wlfile

    # The server specified must not be the internal server. Raise an exception otherwise.
    if optD['dbhost'].lower() == getDRMSParam(drmsParams, 'SERVER'):
        raise Exception('getArgs', 'Host specified is the internal server. Must select an external one.', RET_BADARGS)

    # Get configuration information.
    optD['cfg'] = drmsParams

    return optD

rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = {}
    rootObj = {}
    errMsg = ''
    server = 'Unknown' # This is the database host to use when querying about all the series provided as the series-list argument.
    seriesObjs = [] # A list of series objects with info about the series (like the db server for that series

    try:
        if os.getenv('REQUEST_URI'):
            rtype = 'cgi'
        else:
            rtype = 'cl'
    
        # Default to command-line invocation.
        optD['source'] = rtype
        
        drmsParams = DRMSParams()
        if drmsParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).', RET_DRMSPARAMS)
        
        optD = getArgs(drmsParams)
        
        if not drmsParams.getBool('WL_HASWL'):
            raise Exception('whitelist', 'This DRMS does not support series whitelists.', RET_WHITELIST)
        
        # If being run by apache in the CGI context, then the environment is virtually empty. And this script needs to be runnable
        # outside the CGI context too. This is one argument for not systeming processes. So, call this csh script to figure out which
        # architecture's binary to run.
        binDir = getDRMSParam(drmsParams, 'BIN_EXPORT');
        
        # Before calling anything, make sure that QUERY_STRING is not set in the child process. Some DRMS modules, like show_series,
        # branch into "web" code if they see QUERY_STRING set.
        if 'QUERY_STRING' in os.environ:
            del os.environ['QUERY_STRING']
        
        cmdList = [os.path.join(binDir, '..', 'build', 'jsoc_machine.csh')]
        
        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
        except ValueError:
            raise Exception('arch', "Unable to run command: '" + ' '.join(cmdList) + "'.", RET_ARCH)
        except CalledProcessError as exc:
            raise Exception('arch', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_ARCH)
        
        if output is None:
            raise Exception('arch', 'Unexpected response from jsoc_machine.csh', RET_ARCH)
        outputList = output.splitlines()
        
        # There should be only one output line.
        arch = outputList[0];
        
        # Call show_series on the external host. If all series are on the external host, return the external server optD['dbhost']. show_series
        # does not provide a way to search for a list of series, so we have to make a hash of all series, then assess each series for membsership in the list
        # of external series.
        cmdList = [os.path.join(binDir, arch, 'show_series'), '-qz', 'JSOC_DBHOST=' + optD['dbhost'], 'JSOC_DBPORT=' + str(optD['dbport']), 'JSOC_DBNAME=' + optD['dbname'], 'JSOC_DBUSER=' + optD['dbuser']]

        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
            jsonObj = json.loads(output)
        except ValueError:
            raise Exception('showseries', "Unable to run command: '" + ' '.join(cmdList) + "'.", RET_SHOWSERIES)
        except CalledProcessError as exc:
            raise Exception('showseries', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_SHOWSERIES)

        if output is None:
            raise Exception('showseries', 'Unexpected response from show_series.', RET_SHOWSERIES)

        externalMap = {}
        external = []
        notExternal = []
        passThruSeries = {}
        
        for series in jsonObj['names']:
            externalMap[str(series['name']).strip().lower()] = str(series['name']).strip()

        for series in optD['series']:
            if series.lower() in externalMap:
                external.append(series)
            else:
                notExternal.append(series)

        # If all series are external, then we can return the external database server.
        if len(notExternal) == 0:
            server = getDRMSParam(drmsParams, 'SERVER')
        else:
            # Some series may be on the internal db server.
            cmdList = [os.path.join(binDir, arch, 'show_series'), '-qz', 'JSOC_DBHOST=' + getDRMSParam(drmsParams, 'SERVER'), 'JSOC_DBPORT=' + str(optD['dbport']), 'JSOC_DBNAME=' + optD['dbname'], 'JSOC_DBUSER=' + optD['dbuser']]
                
            try:
                resp = check_output(cmdList, stderr=STDOUT)
                output = resp.decode('utf-8')
                jsonObj = json.loads(output)
            except ValueError:
                raise Exception('showseries', "Unable to run command: '" + ' '.join(cmdList) + "'.", RET_SHOWSERIES)
            except CalledProcessError as exc:
                raise Exception('showseries', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_SHOWSERIES)

            if output is None:
                raise Exception('showseries', 'Unexpected response from show_series.', RET_SHOWSERIES)

            # Hash all series in the internal DB.
            internalMap = {}
            for series in jsonObj['names']:
                # Remove various whitespace too.
                internalMap[str(series['name']).strip().lower()] = str(series['name']).strip()

            # Fetch whitelist.
            with open(optD['wlfile'], 'r') as whitelist:
                # NOTE: This script does not attempt to validate the series in the whitelist - there could be invalid entries in that
                # file. Series from the whitelist that match a series in internal-DB series are returned to the caller.
                for series in whitelist:
                    if series.strip().lower() in internalMap:
                        passThruSeries[series.strip().lower()] = True

            # Finally, check to see if all the series in notExternal are in passThruSeries.
            for series in notExternal:
                if series.lower() not in passThruSeries:
                    raise Exception('getArgs', 'Series ' + series + ' is not a valid series accessible from ' + optD['dbhost'] + '.', RET_BADARGS)

            # Set server to internal server.
            server = getDRMSParam(drmsParams, 'SERVER')

            # seriesObjs - [{ "hmi.M_45s" : { "server" : "hmidb" } }, { "hmi.internalonly" : { "server" : "hmidb2" }}, ...]
            for series in optD['series']:
                if series.lower() in externalMap:
                    sobj = { externalMap[series.lower()] : {} }
                    sobj[externalMap[series.lower()]]['server'] = optD['dbhost']
                else:
                    sobj = { internalMap[series.lower()] : {} }
                    sobj[internalMap[series.lower()]]['server'] = getDRMSParam(drmsParams, 'SERVER')
                seriesObjs.append(sobj)

    except Exception as exc:
        if len(exc.args) != 3:
            msg = 'Unhandled exception.'
            raise # Re-raise
        
        etype, msg, rv = exc.args
        
        server = 'Unknown'
        seriesObjs = []
        
        # It is possible that an exception has caused optD == None.
        if not optD:
            optD['noheader'] = None
            optD['dbhost'] = None
            optD['dbport'] = None
            optD['dbname'] = None
            optD['dbuser'] = None
            optD['series'] = None
            optD['wlfile'] = None
        
        if not 'source' in optD:
            optD['source'] = rtype
        
        if etype != 'getArgs' and etype != 'drmsArgs' and etype != 'whitelist' and etype != 'arch' and etype != 'showseries':
            raise
        
        if msg and len(msg) > 0:
            if errMsg and len(errMsg) > 0:
                errMsg += ' '
            errMsg += msg

    printCGI = True
    if 'source' in optD:
        if optD['source'] != 'cgi':
            printCGI = False
    elif rtype != 'cgi':
        printCGI = False

    if printCGI:
        # Returns - { "server" : "hmidb2", "series" : [{ "hmi.M_45s" : { "server" : "hmidb" } }, { "hmi.internalonly" : { "server" : "hmidb2" }}], "err" : 0}
        rootObj['err'] = rv # A number
        if rv != RET_SUCCESS:
            rootObj['errMsg'] = errMsg # A string
        rootObj['server'] = server
        rootObj['series'] = seriesObjs
        if not optD['noheader']:
            print('Content-type: application/json\n')
        print(json.dumps(rootObj))
        sys.exit(0)
    else:
        if rv == RET_SUCCESS:
            if not optD['noheader']:
                print('Database server information for series: \n\t', end='')
            
            print(','.join(optD['series']))
            
            if not optD['noheader']:
                print('\nserver: \n\t', end='')
            print(server)

            if not optD['noheader']:
                print('\nseries and information: \n', end='')
            for seriesObj in seriesObjs:
                if not optD['noheader']:
                    print('\t', end='')
                print(list(seriesObj.keys())[0] + '\t' + seriesObj[list(seriesObj.keys())[0]]['server'])
        else:
            if errMsg:
                print(errMsg, file=sys.stderr)
        
        sys.exit(rv)
