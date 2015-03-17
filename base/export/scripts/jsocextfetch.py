#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import re
import cgi
import json
from subprocess import check_output, check_call, CalledProcessError, STDOUT
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

RET_SUCCESS = 0
RET_BADARGS = 1
RET_DRMSPARAMS = 2
RET_ARCH = 3
RET_PARSESPEC = 4
RET_CHECKSERVER = 5
RET_JSOCFETCH = 6

err = None
errMsg = None

def getDRMSParam(drmsParams, param):
    rv = drmsParams.get(param)
    if not rv:
        raise Exception('drmsParams', 'DRMS parameter ' + param + ' is not defined.', RET_DRMSPARAMS)

    return rv
    
def validRequestID(id):
    regexp = re.compile(r'JSOC_\d\d\d\d\d\d\d\d_\d+_X_IN')
    match = regexp.match(id)
    return match is not None
    
def printUTF8(unicode):
    print(str(unicode).rstrip())
    
try:
    optD = {}
    allArgs = []

    # If jsocextfetch.py is processing an HTTP POST request, then cgi.FieldStorage() reads from STDIN to get the arguments. This
    # means that the jsoc_fetch binary will not find the arguments when it tries to get them from STDIN!!! This is assuming that it is
    # getting them from STDIN, but I cannot see how it is doing that. So, we have to read them in here, then pass them to jsoc_fetch
    # in the check_call() function. dbhost is a parameter for jsocextfetch.py, but not for jsoc_fetch. It gets stripped off
    # from the list of arguments passed in, and the remaining parameters are passed onto jsoc_fetch. 'dbhost' is the external db host
    # that the external user is attempting to access. 
    arguments = cgi.FieldStorage()

    if arguments:
        for key in arguments.keys():
            val = arguments.getvalue(key)

            if key in ('H', 'dbhost'):
                # If the caller is providing the ds argument, then dbhost is required. It is used for determining which db server
                # provides series information.
                optD['dbhost'] = val
            else:
                if key in ('ds'):
                    optD['spec'] = val
                elif key in ('op'):
                    optD['op'] = val
                elif key in ('requestid'):
                    optD['requestid'] = val

                allArgs.append(key + '=' + val)
     
    # Enforce requirements.   
    if 'op' not in optD:
        raise Exception('invalidArgs', 'Missing required argument ' + "'op'.", RET_BADARGS)

    if optD['op'] == 'exp_request':
        if not 'dbhost' in optD:
            raise Exception('invalidArgs', 'Missing required argument ' + "'dbhost'.", RET_BADARGS)
        if not 'spec' in optD:
            raise Exception('invalidArgs', 'Missing required argument ' + "'ds'.", RET_BADARGS)
    elif optD['op'] == 'exp_status' or optD['op'] == 'exp_repeat':
        if not 'requestid' in optD:
            raise Exception('invalidArgs', 'Missing required argument ' + "'requestid'.", RET_BADARGS)
    
        # Validate requestid - this script is called only from the external website when status is requested for a request handled by the internal database.
        if not validRequestID(optD['requestid']):
            raise Exception('invalidArgs', 'requestid ' + optD['requestid'] + ' is not for a pass-through request (acceptable format is JSOC_YYYYMMDD_NNN_X_IN).', RET_BADARGS)    
    
    if 'requestid' in optD and 'spec' in optD:
        raise Exception('invalidArgs', 'Cannot provide both ' + "'requestid' and 'ds'.", RET_BADARGS)
    
    drmsParams = DRMSParams()

    if drmsParams is None:
        raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).', RET_DRMSPARAMS)

    # Before calling anything, make sure that QUERY_STRING is not set in the child process. Some DRMS modules, like show_series,
    # branch into "web" code if they see QUERY_STRING set.
    if 'QUERY_STRING' in os.environ:
        del os.environ['QUERY_STRING']

    # To run various DRMS modules, we'll need to know the path to the modules, and the architecture of the module to run.
    binDir = getDRMSParam(drmsParams, 'BIN_EXPORT')

    ############################
    ## Determine Architecture ##
    ############################
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


    if 'spec' in optD:
        series = []
        server = None

        # Run the jsoc_fetch command to initiate a new export request. To do that, we need to know which db server, 
        # internal or external, to send the request to. To do that, we have to call the drms_parserecset module
        # to extract the series from the record-set argument provided to jsocextfetch.py. Then checkExpDbServer.py can be called
        # which will provide the name of the db server that can handle the series. Finally, we pass the name of that server
        # to jsoc_fetch, via the JSOC_DBHOST module argument.

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
        binPy = getDRMSParam(drmsParams, 'BIN_PY')
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
        ## Run jsoc_fetch ##
        ###################
        # Run the command to initiate a new export request.
        # jsoc_fetch DOES print the HTML header needed when it is run from the command line (unlike show_info).

        # W=1 ==> Do not print HTML headers. This script should do that.
        cmdList = [os.path.join(binDir, arch, 'jsoc_fetch'), 'JSOC_DBHOST=' + server, 'DRMS_DBTIMEOUT=900000', 'W=1']
        # Provide all jsoc_fetch arguments passed through jsocextfetch.py to jsoc_fetch.
        cmdList.extend(allArgs)

        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
            jsonObj = json.loads(output)
        except ValueError as exc:
            raise Exception('jsocfetch', exc.args[0], RET_JSOCFETCH)
        except CalledProcessError as exc:
            raise Exception('jsocfetch', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_JSOCFETCH)
            
        if jsonObj is None or int(jsonObj['status']) != 0 and int(jsonObj['status']) != 1 and int(jsonObj['status']) != 2 and int(jsonObj['status']) != 3 and int(jsonObj['status']) != 4 and int(jsonObj['status']) != 5 and int(jsonObj['status']) != 6:
            raise Exception('jsocfetch', 'jsoc_fetch did not return a known status code (code ' + jsonObj['status']+ ').', RET_JSOCFETCH)

        # Print the JSON object as returned by jsoc_fetch.
        print('Content-type: application/json\n')
        printUTF8(output)

    else:		
        ####################
        ## Run jsoc_fetch ##
        ####################
        # Run the command to check on the status of a previous export request, or to re-request an old export.
        # The db server we have jsoc_fetch operate on is the server that was used to call the original jsoc_fetch
        # op=exp_request command. The name of request ID encodes that information.

        # W=1 ==> Do not print HTML headers. This script should do that.
        # JSOC_DBHOST is the internal server. jsocextfetch.py is called from the external website only when the original request was supported by the 
        # internal database.
        cmdList = [os.path.join(binDir, arch, 'jsoc_fetch'), 'JSOC_DBHOST=' + getDRMSParam(drmsParams, 'SERVER'), 'DRMS_DBTIMEOUT=900000', 'W=1']
        # Provide all jsoc_fetch arguments passed through jsocextfetch.py to jsoc_fetch.
        cmdList.extend(allArgs)

        try:
            resp = check_output(cmdList, stderr=STDOUT)
            output = resp.decode('utf-8')
            jsonObj = json.loads(output)
        except ValueError as exc:
            raise Exception('jsocfetch', exc.args[0], RET_JSOCFETCH)
        except CalledProcessError as exc:
            raise Exception('jsocfetch', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_JSOCFETCH)
            
        if jsonObj is None or int(jsonObj['status']) != 0 and int(jsonObj['status']) != 1 and int(jsonObj['status']) != 2 and int(jsonObj['status']) != 3 and int(jsonObj['status']) != 4 and int(jsonObj['status']) != 5 and int(jsonObj['status']) != 6:
            raise Exception('jsocfetch', 'jsoc_fetch did not return a known status code (code ' + jsonObj['status']+ ').', RET_JSOCFETCH)

        # Print the JSON object as returned by jsoc_fetch.
        print('Content-type: application/json\n')
        printUTF8(output)
        
except Exception as exc:
    if len(exc.args) != 3:
        msg = 'Unhandled exception.'
        raise # Re-raise

    etype, msg, rv = exc.args

    if etype != 'invalidArgs' and etype != 'drmsParams' and etype != 'arch' and etype != 'parsespec' and etype != 'checkserver' and etype != 'jsocfetch':
        raise

    err = 4 # Regardless of the error status of this script, this script has to return jsoc_fetch error codes. 4 means abject failure.
    errMsg = msg

# jsoc_fetch creates webpage content, if there is no failure. But if it or this script fails, then we have to create content that contains
# an error code and error message.
if err:
    print('Content-type: application/json\n')
    rootObj = { "status" : err, "error" : errMsg}
    print(json.dumps(rootObj))
    
sys.exit(0)
