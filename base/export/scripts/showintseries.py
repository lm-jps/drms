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
RET_ARCH = 5
RET_SHOWSERIES = 6

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
        optD['info'] = None
        optD['dbuser'] = None
        optD['filter'] = None
        
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
                    if key in ('i', 'info'):
                        if int(val) == 1:
                            optD['info'] = True
                        else:
                            optD['info'] = False
                    elif key in ('U', 'dbuser'):
                        optD['dbuser'] = val
                    elif key in ('f', 'filter'):
                        optD['filter'] = val
            
            optD['noheader'] = bool(getOption(optD['noheader'], False))
            optD['info'] = bool(getOption(optD['info'], False))
            optD['dbuser'] = getOption(optD['dbuser'], pwd.getpwuid(os.getuid())[0])
            optD['filter'] = getOption(optD['filter'], None)
            
        except ValueError as exc:
            raise Exception('getArgs', 'cgi', 'Invalid arguments.')
    else:
        # Non CGI invocation.
        try:
            parser = CmdlParser(usage='%(prog)s [ -dhin ] [ --dbuser=<db user> ] [ --filter=<DRMS regular expression> ]')
            
            # Optional
            parser.add_argument('-d', '--debug', help='Run in CGI mode, and print helpful diagnostics.).', dest='debug', action='store_true', default=False)
            parser.add_argument('-i', '--info', help='Print additional series information (such as series description)', dest='info', action='store_true', default=False)
            parser.add_argument('-n', '--noheader', help='Supress the HTML header (cgi runs only).', dest='noheader', action='store_true', default=False)
            parser.add_argument('-U', '--dbuser', help='The user to log-in to the serving database as.', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
            parser.add_argument('-f', '--filter', help='The DRMS-style regular expression to filter series.', metavar='<regexp>', dest='filter', default=None)
            
            args = parser.parse_args()
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
        optD['info'] = args.info
        optD['dbuser'] = args.dbuser
        optD['filter'] = args.filter

    # Get configuration information.
    optD['cfg'] = drmsParams
    
    return optD

rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = {}
    rootObj = {}
    errMsg = ''
    internalSeries = []
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
        
        # If being run by apache in the CGI context, then the environment is virtually empty. And this script needs to be runnable
        # outside the CGI context too. This is one argument for not systeming processes. So, call this csh script to figure out which
        # architecture's binary to run.
        binDir = drmsParams.get('BIN_EXPORT');
        if not binDir:
            raise Exception('drmsParams', optD['source'], 'Missing DRMS parameter: BIN_EXPORT.')

        # Before calling anything, make sure that QUERY_STRING is not set in the child process. Some DRMS modules, like show_series,
        # branch into "web" code if they see QUERY_STRING set. If that happens, then they will use the filter argument in QUERY_STRING,
        # which will not contain a filter argument necessarily.
        if 'QUERY_STRING' in os.environ:
            del os.environ['QUERY_STRING']

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
        cmdList = [os.path.join(binDir, arch, 'show_series'), '-qz', 'JSOC_DBHOST=' + drmsParams.get('SERVER'), 'JSOC_DBPORT=' + drmsParams.get('DRMSPGPORT'), 'JSOC_DBNAME=' + drmsParams.get('DBNAME'), 'JSOC_DBUSER=' + optD['dbuser']]
        if optD['filter']:
            cmdList.append(optD['filter'])

        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
            jsonObj = json.loads(output)
        except ValueError:
            raise Exception('showseries', rtype, "Unable to run command: '" + ' '.join(cmdList) + "'.")
        except CalledProcessError as exc:
            raise Exception('showseries', rtype, "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))
        
        if output is None:
            raise Exception('showseries', rtype, 'Unexpected response from show_series.')

        for series in jsonObj['names']:
            if optD['info']:
                internalSeries.append({ str(series['name']).strip() : { 'description' : str(series['note']) } })
            else:
                internalSeries.append(str(series['name']).strip())

        # Sort by series. If optD['info'], then internalSeries is a list of dictionaries.
        if optD['info']:
            internalSeries.sort(key=lambda k: list(k.keys())[0])
        else:
            internalSeries.sort()

    except Exception as exc:
        if len(exc.args) != 3:
            msg = 'Unhandled exception.'
            raise # Re-raise

        etype, rtype, msg = exc.args

        internalSeries = []

        # It is possible that an exception has caused optD == None.
        if not optD:
            optD['noheader'] = None
            optD['info'] = None
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
        rootObj['errMsg'] = errMsg # A string
        rootObj['seriesList'] = internalSeries
        if not optD['noheader']:
            print('Content-type: application/json\n')
        print(json.dumps(rootObj))
        sys.exit(0)
    else:
        if rv == RET_SUCCESS:
            if optD['info']:
                if not optD['noheader']:
                    print('series\tdescription')
                for series in internalSeries:
                    # series is a dictionary.
                    # Python 3 keys() returns a view object, not a list.
                    print(list(series.keys())[0] + '\t', end='')
                    print(list(series.values())[0]['description'])
            else:
                if not optD['noheader']:
                    print('series')
                for series in internalSeries:
                    # series is a string
                    print(series)
        else:
            if errMsg:
                print(errMsg, file=sys.stderr)

        sys.exit(rv)
