#!/usr/bin/env python3

# This is a wrapper for jsoc_info.c, so it must adhere to its API:
#   If no error has occurs, then return "status" : 0.
#   If an error occurs, return a non-zero status. jsoc_info.c does not return anything other than
#     "status" : 1, but jsocextinfo.py distinguishes several different kinds of errors. jsocextinfo.py
#     returns one of its return values (success is still 0). An error message is returned via
#     the "error" property.

import sys
import io
import os
import cgi
import json
from distutils.util import strtobool
from subprocess import check_output, Popen, SubprocessError, STDOUT, PIPE, DEVNULL
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

RET_SUCCESS = 0
RET_BADARGS = 1
RET_DRMSPARAMS = 2
RET_ARCH = 3
RET_PARSESPEC = 4
RET_CHECKSERVER = 5
RET_JSOCINFO = 6

err = None
errMsg = None
jstdout = None

def getDRMSParam(drmsParams, param):
    rv = drmsParams.get(param)
    if not rv:
        raise Exception('drmsParams', 'DRMS parameter ' + param + ' is not defined.', RET_DRMSPARAMS)

    return rv

def setOutputEncoding(encoding='UTF8', errors='strict'):
    sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding=encoding, errors=errors, line_buffering=sys.stdout.line_buffering)

try:
    optD = {}
    allArgs = []
    optD['noheader'] = False

    # If jsocextinfo.py is processing an HTTP POST request, then cgi.FieldStorage() reads from STDIN to get the arguments. This
    # means that the jsoc_info binary will not find the arguments when it tries to get them from STDIN!!! This is assuming that it is
    # getting them from STDIN, but I cannot see how it is doing that. So, we have to read them in here, then pass them to jsoc_info
    # in the check_call() function. dbhost is a parameter for jsocextinfo.py, but not for jsoc_info. It gets stripped off
    # from the list of arguments passed in, and the remaining parameters are passed onto jsoc_info. 'dbhost' is the external db host
    # that the external user is attempting to access.
    arguments = cgi.FieldStorage()

    if arguments:
        for key in arguments.keys():
            val = arguments.getvalue(key)

            if key in ('H', 'dbhost'):
                optD['dbhost'] = val
            elif key in ('N'):
                optD['noheader'] = strtobool(val) == True
            else:
                if key in ('ds'):
                    optD['spec'] = val

                allArgs.append(key + '=' + val)

    # Enforce requirements.
    if not 'dbhost' in optD:
        raise Exception('invalidArgs', "Missing required argument '" + "dbhost'.")
    if not 'spec' in optD:
        raise Exception('invalidArgs', "Missing required argument '" + "ds'.")

    drmsParams = DRMSParams()

    if drmsParams is None:
        raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')

    # Before calling anything, make sure that QUERY_STRING is not set in the child process. Some DRMS modules, like show_series,
    # branch into "web" code if they see QUERY_STRING set.
    if 'QUERY_STRING' in os.environ:
        del os.environ['QUERY_STRING']

    series = []
    server = None

    ############################
    ## Determine Architecture ##
    ############################
    binDir = getDRMSParam(drmsParams, 'BIN_EXPORT')
    cmdList = [os.path.join(binDir, '..', 'build', 'jsoc_machine.csh')]

    try:
        resp = check_output(cmdList, stderr=STDOUT)
        output = resp.decode('utf-8')
    except ValueError as exc:
        raise Exception('arch', exc.args[0], RET_ARCH)
    except CalledProcessError as exc:
        raise Exception('arch', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_ARCH)

    if output is None:
        raise Exception('arch', 'Unexpected response from jsoc_machine.csh', RET_ARCH)

    # There should be only one output line.
    outputList = output.splitlines()
    arch = outputList[0];

    ####################
    ## Extract series ##
    ####################
    cmdList = [os.path.join(binDir, arch, 'drms_parserecset'), 'spec=' + optD['spec']]

    # Returns {"spec":"hmi.m_45s[2015.6.1]","atfile":false,"hasfilts":true,"nsubsets":1,"subsets":[{"spec":"hmi.m_45s[2015.6.1]","settype":"drms","seriesname":"hmi.m_45s","filter":"[2015.6.1]","autobang":false}],"errMsg":null}
    try:
        resp = check_output(cmdList, stderr=STDOUT)
        output = resp.decode('utf-8')
        jsonObj = json.loads(output)
    except ValueError as exc:
        raise Exception('parsespec', exc.args[0], RET_PARSESPEC)
    except CalledProcessError as exc:
        raise Exception('parsespec', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_PARSESPEC)

    if jsonObj is None or jsonObj['errMsg'] is not None:
        raise Exception('parsespec', jsonObj['errMsg'], RET_PARSESPEC)

    for aset in jsonObj['subsets']:
        series.append(aset['seriesname'])

    ################################
    ## Determine Series DB Server ##
    ################################
    # Ask checkExpDbServer.py to provide the name of the db server that can handle all the series in series.
    binPy = getDRMSParam(drmsParams, 'BIN_PY3')
    scriptsDir = getDRMSParam(drmsParams, 'SCRIPTS_EXPORT')

    cmdList = [binPy, os.path.join(scriptsDir, 'checkExpDbServer.py'), '-c', 'n=1&dbhost=' + optD['dbhost'] + '&series=' + ','.join(series)]

    try:
        resp = check_output(cmdList, stderr=STDOUT)
        output = resp.decode('utf-8')
        jsonObj = json.loads(output)
    except ValueError as exc:
        raise Exception('checkserver', exc.args[0], RET_CHECKSERVER)
    except CalledProcessError as exc:
        raise Exception('checkserver', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_CHECKSERVER)

    # Returns {"series": [{"hmi.M_45s": {"server": "hmidb2"}}, {"hmi.tdpixlist": {"server": "hmidb"}}], "err": 0, "server": "hmidb"}
    if jsonObj is None or jsonObj['err'] != 0:
        raise Exception('checkserver', jsonObj['errMsg'], RET_CHECKSERVER)

    server = jsonObj['server']

    ###################
    ## Run jsoc_info ##
    ###################
    cmdList = [ os.path.join(binDir, arch, 'jsoc_info'), 'JSOC_DBHOST=' + server, 'DRMS_DBTIMEOUT=600000', 'DRMS_QUERY_MEM=4096', 'DRMS_DBUTF8CLIENTENCODING=1' ]

    if optD['noheader']:
        cmdList.append('-s')

    # Provide all jsoc_info arguments passed through jsocextinfo.py to jsoc_info.
    cmdList.extend(allArgs)

    try:
        if not optD['noheader']:
            print('running ' + ' '.join(cmdList), file=sys.stderr)
        proc = Popen(cmdList, stdin=None, stderr=PIPE, stdout=PIPE)
        jstdout, jstderr = proc.communicate()

        if jstderr is not None:
            errMsg = jstderr.decode('UTF8')

        if proc.returncode != 0:
            if errMsg is None:
                errMsg = "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(proc.returncode)

            raise Exception('jsocinfo', errMsg , RET_JSOCINFO)
    except ValueError as exc:
        raise Exception('jsocinfo', exc.args[0], RET_JSOCINFO)
    except SubprocessError as exc:
        raise Exception('jsocinfo', 'failure running jsoc_info', RET_JSOCINFO)

except Exception as exc:
    if len(exc.args) != 3:
        msg = 'Unhandled exception.'
        raise # Re-raise

    etype, msg, rv = exc.args

    if etype != 'invalidArgs' and etype != 'drmsParams' and etype != 'arch' and etype != 'parsespec' and etype != 'checkserver' and etype != 'jsocinfo':
        raise

    err = rv
    errMsg = ''

    if msg and len(msg) > 0:
        if errMsg and len(errMsg) > 0:
            errMsg += ' '
        errMsg += msg

# jsoc_info creates webpage content, if there is no failure. But if it or this script fails, then we have to create content that contains
# an error code and error message.
rootObj = {}

setOutputEncoding('UTF8')

if err:
    rootObj['status'] = err
    rootObj['error'] = errMsg
    if not optD['noheader']:
        print('Content-type: application/json\n')

    print(json.dumps(rootObj))
else:
    # use jsoc_info's output
    if jstdout is not None:
        print(jstdout.decode('UTF8'), end='')

sys.exit(0)
