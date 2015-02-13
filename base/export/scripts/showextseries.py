#!/usr/bin/env python

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

# Return codes for cmd-line run.
RET_SUCCESS = 0
RET_BADARGS = 1
RET_DRMSPARAMS = 2
RET_DBCONNECT = 3
RET_UNKNOWNRUNTYPE = 4
RET_WHITELIST = 5
RET_ARCH = 6
RET_SHOWSERIES = 7

def getOption(val, default):
    if val:
        return val
    else:
        return default

# Debug flag.
DEBUG_CGI = False

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
        optD['filter'] = None
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
                    elif key in ('f', 'filter'):
                        optD['filter'] = val
                    elif key in ('w', 'wlfile'):
                        optD['wlfile'] = val
            
            optD['noheader'] = bool(getOption(optD['noheader'], False))
            optD['dbport'] = int(getOption(optD['dbport'], drmsParams.get('DRMSPGPORT')))
            optD['dbname'] = getOption(optD['dbname'], drmsParams.get('DBNAME'))
            optD['dbuser'] = getOption(optD['dbuser'], pwd.getpwuid(os.getuid())[0])
            optD['filter'] = getOption(optD['filter'], None)
            optD['wlfile'] = getOption(optD['wlfile'], drmsParams.get('WL_FILE'))
            
            # Enforce requirements.
            if not 'dbhost' in optD:
                raise Exception('getArgs', 'cgi', 'Missing required argument ' + "'dbhost'.")
        except ValueError as exc:
            raise Exception('getArgs', 'cgi', 'Invalid arguments.')
        except KeyError as exc:
            raise Exception('drmsArgs', 'cgi', 'Undefined DRMS parameter.\n' + exc.strerror)
    else:
        # Non CGI invocation.
        try:
            parser = CmdlParser(usage='%(prog)s [ -hn ] dbhost=<db host> [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user> ] [ --filter=<DRMS regular expression> ] [--wlfile=<white-list text file> ]')
            
            # Required
            parser.add_argument('H', 'dbhost', '--dbhost', help='The machine hosting the EXTERNAL database that serves DRMS data series names.', metavar='<db host>', dest='dbhost', required=True)
            
            # Optional
            parser.add_argument('-d', '--debug', help='Run in CGI mode, and print helpful diagnostics.).', dest='debug', action='store_true', default=False)
            parser.add_argument('-n', '--noheader', help='Supress the HTML header (cgi runs only).', dest='noheader', action='store_true', default=False)
            parser.add_argument('-P', '--dbport', help='The port on the machine hosting DRMS data series names.', metavar='<db host port>', dest='dbport', default=drmsParams.get('DRMSPGPORT'))
            parser.add_argument('-N', '--dbname', help='The name of the database serving DRMS series names.', metavar='<db name>', dest='dbname', default=drmsParams.get('DBNAME'))
            parser.add_argument('-U', '--dbuser', help='The user to log-in to the serving database as.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
            parser.add_argument('-f', '--filter', help='The DRMS-style regular expression to filter series.', metavar='<regexp>', dest='filter', default=None)
            parser.add_argument('-w', '--wlfile', help='The text file containing the definitive list of internal series accessible via the external web site.', metavar='<white-list file>', dest='wlfile', default=drmsParams.get('WL_FILE'))
            
            args = parser.parse_args()
        except KeyError as exc:
            raise Exception('drmsArgs', 'cl', 'Undefined DRMS parameter.\n' + exc.strerror)
        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise
            
            etype = exc.args[0]
            msg = exc.args[1]
            
            if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
                raise Exception('getArgs', 'cl', 'Unable to parse command-line arguments. ' + msg + '\n' + parser.format_help())
            else:
                raise # Re-raise.

        # Cannot loop through attributes since args has extra attributes we do not want.
        optD['noheader'] = args.noheader
        optD['dbhost'] = args.dbhost
        optD['dbport'] = int(args.dbport)
        optD['dbname'] = args.dbname
        optD['dbuser'] = args.dbuser
        optD['filter'] = args.filter
        optD['wlfile'] = args.wlfile

    # The server specified must not be the internal server. Raise an exception otherwise.
    if optD['dbhost'].lower() == drmsParams.get('SERVER'):
        raise Exception('getArgs', optD['source'], 'Host specified is the internal server. Must select an external one.')

    # Get configuration information.
    optD['cfg'] = drmsParams
    
    return optD

rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = {}
    rootObj = {}
    errMsg = ''
    passThruSeries = []
    combinedSeries = []
    rtype = 'cgi' # Gotta assume CGI, since if we are in a CGI context, but we have assumed we are not, and an exception happens, 
                  # an error would be printed causing the CGI script to fail without returning some kind of message to the caller.
    
    try:
        drmsParams = DRMSParams()
        if drmsParams is None:
            if os.getenv('REQUEST_URI'):
                rtype = 'cgi'
            else:
                rtype = 'cl'
            raise Exception('drmsParams', rtype, 'Unable to locate DRMS parameters file (drmsparams.py).')
        
        optD = getArgs(drmsParams)
        
        if not drmsParams.getBool('WL_HASWL'):
            raise Exception('whitelist', optD['source'], 'This DRMS does not support series whitelists.')
        
        # If being run by apache in the CGI context, then the environment is virtually empty. And this script needs to be runnable
        # outside the CGI context too. This is one argument for not systeming processes. So, call this csh script to figure out which
        # architecture's binary to run.
        binDir = drmsParams.get('BIN_EXPORT');

        cmdList = [os.path.join(binDir, '..', 'build', 'jsoc_machine.csh')]
        
        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
        except ValueError:
            raise Exception('arch', rtype, "Unable to run command: '" + ' '.join(cmdList) + "'.")
        except CalledProcessError as exc:
            raise Exception('arch', rtype, "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))

        if output is None:
            raise Exception('arch', rtype, 'Unexpected response from jsoc_machine.csh')
        outputList = output.splitlines()

        # There should be only one output line.
        arch = outputList[0];
        
        # Call show_series on the internal host with the optional filter.
        cmdList = [os.path.join(binDir, arch, 'show_series'), '-q', 'JSOC_DBHOST=' + drmsParams.get('SERVER'), 'JSOC_DBPORT=' + str(optD['dbport']), 'JSOC_DBNAME=' + optD['dbname']]
        if optD['filter']:
            cmdList.append(optD['filter'])

        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
        except ValueError:
            raise Exception('showseries', rtype, "Unable to run command: '" + ' '.join(cmdList) + "'.")
        except CalledProcessError as exc:
            raise Exception('showseries', rtype, "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))
        
        if output is None:
            raise Exception('showseries', rtype, 'Unexpected response from show_series.')

        outputList = output.splitlines()
        # Hash all series in the internal DB.
        internal = {}
        for series in outputList:
            # Remove various whitespace too.
            internal[series.strip().lower()] = 1

        # Fetch whitelist.
        with open(optD['wlfile'], 'r') as whitelist:
            # NOTE: This script does not attempt to validate the series in the whitelist - there could be invalid entries in that
            # file. Series from the whitelist that match a series in internal-DB series are returned to the caller.
            for series in whitelist:
                if series.strip().lower() in internal:
                    passThruSeries.append(series.rstrip('\n'))
        
        # Call show_series on the external host with the optional filter.
        cmdList = [os.path.join(binDir, arch, 'show_series'), '-q', 'JSOC_DBHOST=' + optD['dbhost'], 'JSOC_DBPORT=' + str(optD['dbport']), 'JSOC_DBNAME=' + optD['dbname']]
        if optD['filter']:
            cmdList.append(optD['filter'])
        
        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
        except ValueError:
            raise Exception('showseries', rtype, "Unable to run command: '" + ' '.join(cmdList) + "'.")
        except CalledProcessError as exc:
            raise Exception('showseries', rtype, "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))

        if output is None:
            raise Exception('showseries', rtype, 'Unexpected response from show_series.')

        outputList = output.splitlines()
        external = {}
        for series in outputList:
            combinedSeries.append(series.strip())
            external[series.strip().lower()] = 1

        # Combine the passThru list with the list of external-DB series. Do not add any internal series that is already present in the external list.
        # This shouldn't be the case, but the whitelist is world-writeable.
        for series in passThruSeries:
            if series.lower() not in external:
                combinedSeries.append(series)

        # Sort by series.
        combinedSeries.sort()

    except Exception as exc:
        if len(exc.args) != 3:
            msg = 'Unhandled exception.'
            raise # Re-raise

        etype, rtype, msg = exc.args

        combinedSeries = []

        # It is possible that an exception has caused optD == None.
        if not optD:
            optD['noheader'] = None
            optD['dbhost'] = None
            optD['dbport'] = None
            optD['dbname'] = None
            optD['dbuser'] = None
            optD['filter'] = None
            optD['wlfile'] = None

        if rtype == 'cgi':
            etype = etype + 'CGI'
        elif rtype == 'cl':
            etype = etype + 'CL'
        else:
            # The finally clause will print msg before sys.exit() is called.
            if msg and len(msg) > 0:
                msg += ' '
            msg += 'Unknown run type.'
            rv = RET_UNKNOWNRUNTYPE
        if etype == 'getArgsCGI':
            # The usage string that is in the errMsg variable is too long. Replace with a short message.
            errMsg = 'Unable to parse arguments.'
            pass
        elif etype == 'getArgsCL':
            rv = RET_BADARGS
            # Show usage.            
        elif etype == 'drmsArgsCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'drmsArgsCL':
            rv = RET_DRMSPARAMS
        elif etype == 'whitelistCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'whitelistCL':
            rv = RET_WHITELIST
        elif etype == 'archCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'archCL':
            rv = RET_ARCH
        elif etype == 'showseriesCGI':
            # Nothing extra for now. Could append to msg.
            pass
        elif etype == 'showseriesCL':
            rv = RET_SHOWSERIES
        elif rtype == 'cl':
            raise # Re-raise
        else:
            # We created the error message for 'errMsg' already.
            pass

        if msg and len(msg) > 0:
            errMsg += ' '
        errMsg += msg

    printCGI = True
    if 'source' in optD:
        if optD['source'] != 'cgi':
            printCGI = False
    elif rtype != 'cgi':
        printCGI = False

    if printCGI:
        rootObj['errMsg'] = errMsg # A string
        rootObj['seriesList'] = combinedSeries
        if not optD['noheader']:
            print('Content-type: application/json\n')
        print(json.dumps(rootObj))
        sys.exit(0)
    else:
        if errMsg:
            print(errMsg, file=sys.stderr)
        for series in combinedSeries:
            print(series)
        sys.exit(rv)
