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
RET_LOGCGI = 7

err = None
errMsg = None

def getDRMSParam(drmsParams, param):
    rv = drmsParams.get(param)
    if not rv:
        raise Exception('drmsParams', 'DRMS parameter ' + param + ' is not defined.', RET_DRMSPARAMS)

    return rv

def validRequestID(id):
    regexp = re.compile(r'JSOC_\d\d\d\d\d\d\d\d_\d+_X_IN\s*$')
    match = regexp.match(id)
    if match is not None:
        return 'int'
    else:
        regexp = re.compile(r'JSOC_\d\d\d\d\d\d\d\d_\d+\s*$')
        match = regexp.match(id)
        if match is not None:
            return 'ext'

    return None

def printUTF8(unicode):
    print(str(unicode).rstrip())

try:
    optD = {}
    allArgs = []

    # Default to JSON output.
    optD['json'] = True
    optD['noheader'] = False

    # This script may be invoked in 4 ways:
    #   1. It can be called from the jsoc_fetch CGI via an HTTP POST request. In this case, jsoc_fetch reads the arguments
    #      from stdin and puts them int the QUERY_STRING environment variable. cgi.FieldStorage() then reads them.
    #   2. It can be called from the jsoc_fetch CGI via an HTTP GET request. In this case, the arguments are also in
    #      the QUERY_STRING environment variable.
    #   3. It can be called from the jsocextfetch CGI via an HTTP POST request. In this case, the arguments are sent to
    #      this script via stdin. This script extracts them with the cgi.FieldStorage() call.
    #   4. It can be called from the jsocextfetch CGI via an HTTP GET request. In this case, the arguments are in
    #      the QUERY_STRING environment variable.
    #
    # If jsocextfetch.py is reading the arguments from STDIN then, the jsoc_fetch binary will not find the arguments
    # when it tries to get them from STDIN with qDecoder. So, we have to read them in here, then pass them to jsoc_fetch
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
            elif key in ('n'):
                optD['noheader'] = True
            else:
                if key in ('ds',):
                    optD['spec'] = val
                elif key in ('op',):
                    optD['op'] = val
                elif key in ('requestid',):
                    optD['requestid'] = val
                elif key in ('format',) and val.lower() == 'txt':
                    optD['json'] = False

                allArgs.append(key + '=' + val)

    drmsParams = DRMSParams()

    if drmsParams is None:
        raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).', RET_DRMSPARAMS)

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
        expSys = validRequestID(optD['requestid'])
        if expSys == 'int':
            server = getDRMSParam(drmsParams, 'SERVER')
        elif expSys == 'ext':
            if not 'dbhost' in optD:
                raise Exception('invalidArgs', 'Missing required argument ' + "'dbhost'.", RET_BADARGS)
            server = optD['dbhost']
        else:
            raise Exception('invalidArgs', 'requestid ' + optD['requestid'] + ' is not an acceptable ID for the external export system (acceptable format is JSOC_YYYYMMDD_NNN_X_IN or JSOC_YYYYMMDD_NNN).', RET_BADARGS)

    if 'requestid' in optD and 'spec' in optD:
        raise Exception('invalidArgs', 'Cannot provide both ' + "'requestid' and 'ds'.", RET_BADARGS)

    # Before calling anything, make sure that QUERY_STRING is not set in the child process. Some DRMS modules, like show_series,
    # branch into "web" code if they see QUERY_STRING set. Also remove the REQUEST_METHOD environment variable - if that
    # is set to POST, then that could interfere with code called from here. We want all code called to get arguments from
    # the command line, not from CGI environment variables.
    if 'QUERY_STRING' in os.environ:
        del os.environ['QUERY_STRING']

    if 'REQUEST_METHOD' in os.environ:
        method = os.environ['REQUEST_METHOD']
        del os.environ['REQUEST_METHOD']
    else:
        method = 'unknown-method'

    if 'SCRIPT_FILENAME' in os.environ:
        script = os.path.basename(os.environ['SCRIPT_FILENAME'])
    else:
        script = 'unknown-script'

    if 'REQUEST_URI' in os.environ:
        url = os.environ['REQUEST_URI']
    else:
        url = 'unknown-URL'

    if 'SERVER_NAME' in os.environ:
        webserver = os.environ['SERVER_NAME']
    else:
        webserver = 'unknown-webserver'

    if 'REMOTE_ADDR' in os.environ:
        ip = os.environ['REMOTE_ADDR']
    else:
        ip = 'unknown-ip'

    # To run various DRMS modules, we'll need to know the path to the modules, and the architecture of the module to run.
    binDir = getDRMSParam(drmsParams, 'BIN_EXPORT')
    scriptDir = getDRMSParam(drmsParams, 'SCRIPTS_EXPORT')
    binPy3 = getDRMSParam(drmsParams, 'BIN_PY3')

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
        # NEW EXPORT

        series = []
        server = None

        # Run the jsoc_fetch command to initiate a new export request. To do that, we need to know which db server,
        # internal or external, to send the request to (this could be a pass-through request - made on the external site, but
        # handled by the internal dataabase). To determine the DB server, we have to call the drms_parserecset module
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

        #############################
        ## Run log-cgi-instance.py ##
        #############################
        # create row for instance ID in instance table and fetch instance ID argument

        # JSOC_DBHOST is the internal server; jsocextfetch.py is called from the external website only when the original request was supported by the
        # internal database.
        # W=1 ==> do not print HTML headers; this script should do that
        # p=1 ==> a pass-through to internal server is occurring (generate a requestID with an "_X" to denote this)
        extraArgs = [ 'JSOC_DBHOST=' + server, 'DRMS_DBTIMEOUT=900000', 'W=1', 'p=1' ]
        formData = '&'.join(allArgs + extraArgs)

        cmdList = [ binPy3, os.path.join(scriptDir, 'log-cgi-instance.py'), script, webserver, url, formData, method, ip ]

        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8').rstrip()
        except ValueError as exc:
            output = ''
            import traceback
            raise Exception('logcgi', traceback.format_exc(1), RET_LOGCGI)
        except CalledProcessError as exc:
            # the partial output is saved in exc.output
            # if log-cgi-instance.py is not found, then python3 returns error code 2; a message gets printed to stderr
            output = exc.output.decode('utf-8').rstrip()

        # output is either the empty string (on error) or the new instance ID
        if len(output) > 0:
            instanceID = int(output)
        else:
            instanceID = -1

        ####################
        ## Run jsoc_fetch ##
        ####################
        # Run the command to initiate a new export request.
        # jsoc_fetch DOES print the HTML header needed when it is run from the command line (unlike show_info).
        cmdList = [ os.path.join(binDir, arch, 'jsoc_fetch'), 'instid=' + str(instanceID) ]
        # Provide all jsoc_fetch arguments passed through jsocextfetch.py to jsoc_fetch.
        cmdList.extend(allArgs + extraArgs)

        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8')
        except ValueError as exc:
            raise Exception('jsocfetch', exc.args[0], RET_JSOCFETCH)
        except CalledProcessError as exc:
            # jsoc_fetch sometimes exits with a status code of 1 when it should not - like when it a request ID does not exist.
            # It creates JSON to be used in a web page, but exits with 1 (which signifies that the JSON created should not
            # be used). Fortunately, the output is saved in exc.output.
            output = exc.output.decode('utf-8')

        # ACK - jsoc_fetch can produce text output in the CGI context. Let the caller of this script handle this.
        if optD['json']:
            try:
                jsonObj = json.loads(output)
            except ValueError as exc:
                raise Exception('jsocfetch', exc.args[0], RET_JSOCFETCH)

            if jsonObj is None or int(jsonObj['status']) != 0 and int(jsonObj['status']) != 1 and int(jsonObj['status']) != 2 and int(jsonObj['status']) != 3 and int(jsonObj['status']) != 4 and int(jsonObj['status']) != 5 and int(jsonObj['status']) != 6 and int(jsonObj['status']) != 7 and int(jsonObj['status']) != 12:
                raise Exception('jsocfetch', 'jsoc_fetch did not return a known status code (code ' + str(jsonObj['status'])+ ')', RET_JSOCFETCH)

            if not optD['noheader']:
                # Print the JSON object as returned by jsoc_fetch.
                print('Content-type: application/json\n')
        else:
            if not optD['noheader']:
                # For now, the only other option for output format is text.
                print('Content-type: text/plain\n')

        printUTF8(output)

    else:
        # NOT A NEW EXPORT REQUEST (a status request or a re-export request)

        #############################
        ## Run log-cgi-instance.py ##
        #############################
        # create row for instance ID in instance table and fetch instance ID argument

        # W=1 ==> do not print HTML headers; this script should do that
        # JSOC_DBHOST is the internal server; jsocextfetch.py is called from the external website only when the original request was supported by the
        # internal database
        extraArgs = [ 'JSOC_DBHOST=' + server, 'DRMS_DBTIMEOUT=900000', 'W=1' ]
        formData = '&'.join(allArgs + extraArgs)

        cmdList = [ binPy3, os.path.join(scriptDir, 'log-cgi-instance.py'), script, webserver, url, formData, method, ip ]

        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8').rstrip()
        except ValueError as exc:
            output = ''
            import traceback
            raise Exception('logcgi', traceback.format_exc(1), RET_LOGCGI)
        except CalledProcessError as exc:
            # the partial output is saved in exc.output
            # if log-cgi-instance.py is not found, then python3 returns error code 2; a message gets printed to stderr
            output = exc.output.decode('utf-8').rstrip()

        # output is either the empty string (on error) or the new instance ID
        if len(output) > 0:
            instanceID = int(output)
        else:
            instanceID = -1

        ####################
        ## Run jsoc_fetch ##
        ####################
        # Run the command to check on the status of a previous export request, or to re-request an old export.
        # The db server we have jsoc_fetch operate on is the server that was used to call the original jsoc_fetch
        # op=exp_request command. The name of request ID encodes that information.
        cmdList = [ os.path.join(binDir, arch, 'jsoc_fetch'), 'instid=' + str(instanceID) ]
        # Provide all jsoc_fetch arguments passed through jsocextfetch.py to jsoc_fetch.
        cmdList.extend(allArgs + extraArgs)

        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8')
        except ValueError as exc:
            raise Exception('jsocfetch', exc.args[0], RET_JSOCFETCH)
        except CalledProcessError as exc:
            # jsoc_fetch sometimes exits with a status code of 1 when it should not - like when it a request ID does not exist.
            # It creates JSON to be used in a web page, but exits with 1 (which signifies that the JSON created should not
            # be used). Fortunately, the output is saved in exc.output.
            output = exc.output.decode('utf-8')

        # ACK - jsoc_fetch can produce text output in the CGI context. Let the caller of this script handle this.
        if optD['json']:
            try:
                jsonObj = json.loads(output)
            except ValueError as exc:
                raise Exception('jsocfetch', exc.args[0], RET_JSOCFETCH)

            if jsonObj is None or int(jsonObj['status']) != 0 and int(jsonObj['status']) != 1 and int(jsonObj['status']) != 2 and int(jsonObj['status']) != 3 and int(jsonObj['status']) != 4 and int(jsonObj['status']) != 5 and int(jsonObj['status']) != 6 and int(jsonObj['status']) != 7:
                raise Exception('jsocfetch', 'jsoc_fetch did not return a known status code (code ' + str(jsonObj['status']) + ')', RET_JSOCFETCH)

            if not optD['noheader']:
                # Print the JSON object as returned by jsoc_fetch.
                print('Content-type: application/json\n')
        else:
            if not optD['noheader']:
                # For now, the only other option for output format is text.
                print('Content-type: text/plain\n')

        printUTF8(output)

except Exception as exc:
    if len(exc.args) != 3:
        msg = 'Unhandled exception.'
        raise # Re-raise

    etype, msg, rv = exc.args

    if etype != 'invalidArgs' and etype != 'drmsParams' and etype != 'arch' and etype != 'parsespec' and etype != 'checkserver' and etype != 'jsocfetch' and etype != 'logcgi':
        raise

    err = 4 # Regardless of the error status of this script, this script has to return jsoc_fetch error codes. 4 means abject failure.
    errMsg = msg

# jsoc_fetch creates webpage content, if there is no failure. But if it or this script fails, then we have to create content that contains
# an error code and error message.
if err:
    if not optD['noheader']:
        print('Content-type: application/json\n')
    rootObj = { "status" : err, "error" : errMsg}
    print(json.dumps(rootObj))

sys.exit(0)
