#!/usr/bin/env python

# This CGI script runs at the SU provider (the server). Each provider is required to implement a cgi named
# rs.sh. Stanford provides an implementation - rs.py - which is wrapped by rs.sh at Stanford. Sites can
# use rs.py, or they can make their own implementation if desired, but the wrapper must be named rs.sh.
# rs.sh is called by rsumsd.py, which runs at the DRMS site (the client) that does not own the requested SU.

# This CGI script returns URLs to the requested SUs. The caller provides a list of SUNUMs, and rs.py determines the
# path to the identified SUs, returning one URL for each SU requested. If all SUs are online at the time that the
# request is made, then the list of URLs is provided in the 'paths' JSON property. The 'paths' property is an array of
# arrays, each with two elements. The first element is the SUNUM, and the second is the complete scp path to the source
# file. The caller can use the second element directly as the source file of an scp command. There is one element
# in the 'paths' array for each SU requested. In this case, rs.py returns a 'status' property of 'complete'. If at least
# one SU requested is offline, then rs.py will request SUMS to asynchronously retrieve the requested SUs from tape and put
# them online. In addition to rs.py returning a 'status' property of 'pending', it also returns a 'requestid' property to the caller
# that contains a string that uniquely identifies the set of requested SUs. The caller must then periodically call rs.py
# with a requestid argument that contains this request ID. Each call of rs.py will cause the script to check for
# the completion of the retrieval of the SUs. If the request is not complete, rs.py returns a 'status' property of 'pending'
# and a 'requestid' with the same request ID provided in the request. If the request is complete, then rs.py returns
# a 'status' property of 'complete' and it returns the 'paths' array as described above.
#
# A useable path will be returned in the 'paths' array for each requested SU that results in an online SU. If a
# an invalid SUNUM is provided, then the second element of the 'paths' array will be null. If a valid SUNUM is
# provided, but the SU is offline and not archived and cannot be retrieved, then the second element of the
# 'paths' array element will be the empty string. If a null or empty string is returned, the caller should not
# attempt repeat the request to obtain a path for that SU. However, if the empty string is returned, the
# caller should add the SUNUM to SUMS (since it refers to a valid SU).

# All DRMS sites that provide SUs must implement rs.py. It must support the published API, but the lower-level implementation
# details do not need to adhere to any standard. Stanford uses jsoc_fetch to obtain SU paths, bringing SUs online if necessary,
# but other DRMS sites are free to support these functions by any desired method (including using jsoc_fetch).
#
# rs.py API:
#   status codes:
#     'complete'      - all requested SUs are online. All URL paths are returned in the HTTP response body.
#     'pending'       - at least one requested SU was offline, but archived. Or the request is in jsoc.export_new, but
#                       not yet in jsoc.export (in other words, processing of the request hasn't started yet).
#     'errorArgs'     - a bad argument was provided to this script.
#     'errorInteral'  - there was a problem calling an internal program.


import sys
import cgi
import re
import json
from subprocess import check_output, CalledProcessError

# Return values
RET_SUCCESS = 0
RET_INVALIDARGS = 1
RET_INTERNALPROG = 2

# Because this script has to run in an environment that has a trivial PATH environment variable, we need to hard-code some
# things as much as I hate to.
BIN_PATH = '/home/jsoc/cvs/Development/JSOC/bin/linux_x86_64'
SCP_USER = 'jsocexp'
SCP_HOST = 'jsocport.stanford.edu'
SCP_PORT = '55000'

def getUsage():
    return 'rs.py requestid=<ID> sunums=<comma-separated list of SUNUMs>'

# cgi.FieldStorage() knows how to parse both HTTP GET and POST requests.
# A nice, but not documented, feature is that we can test the script as it runs in a CGI context
# by simply running on the command line with a single argument that is equivalent to an HTTP GET query string
# (e.g., requestid=JSOC_20140512_882&sunum=none). For some reason, cgi.FieldStorage() knows to look for the
# query string on the command line, in addition to obtaining it from the QUERY_STRING environment variable
# (for HTTP GET requests) or stdin (for HTTP POST requests).

def GetArgs(args):
    istat = False
    optD = {}
    
    # Defaults.
    optD['requestid'] = None
    optD['sunums'] = None
    optD['dbname'] = 'jsoc'
    optD['dbhost'] = 'hmidb' # Can have port appended after a ':'
    optD['dbuser'] = 'apache'
    
    try:
        # Try to get arguments with the cgi module. If that doesn't work, then fetch them from the command line.
        arguments = cgi.FieldStorage()
            
        if arguments:
            for key in arguments.keys():
                val = arguments.getvalue(key)
                
                if val.lower() == 'none':
                    continue
                
                if key in ('r', 'requestid'):
                    optD['requestid'] = val
                elif key in ('s', 'sunums'):
                    # Split string on commas to get a list of SUNUMs to process
                    optD['sunums'] = val.split(',')
                elif key in ('n', 'dbname'):
                    optD['dbname'] = val
                elif key in ('h', 'dbhost'):
                    optD['dbhost'] = val
                elif key in ('u', 'dbuser'):
                    optD['dbuser'] = val
    
    except ValueError:
        raise Exception('badArgs', 'Usage: ' + getUsage())
    
    if optD['requestid'] is None and optD['sunums'] is None:
        raise Exception('badArgs', 'Either requestid or sunums argument must be provided.')
    elif optD['requestid'] is not None and optD['sunums'] is not None:
        raise Exception('badArgs', 'Both requestid or sunums argument cannot be provided.')

    return optD

# In the main section of the output from jsoc_fetch is the status field. If there was a failure obtaining SU informaton
# (drms_getsuinfo() failed, probably due to SUMS failing), then this field's value will be 1. If not, then the value
# depends on the type of jsoc_fetch request (exp_su versus exp_status) and disposition of the SUs requested. If
# an exp_su request was made and the SUs requested were all online, then the status field will be 0. If instead
# at least one SU was offline, then the status field will be either 2 (to set non-debug mode in jsoc_export_manage)
# or 12 (to set debug mode). In this case, the export system will have initiated an asynchronous export (op == exp_request)
# and the caller must periodically call rs.py with the request ID as an argument to poll for completion of retrieval
# from tape. When retrieval is complete, jsoc_fetch returns the list of URLs, and the status field is set to 0.
# Otherwise, it will be set to some non-0 value to indicate that the retrieval has not yet completed.
# If an invalid SUNUM was provided to jsoc_fetch, then in the list of URLs returned there will be a status (the third
# column) of 'I'.

# In the data section of the output from jsoc_fetch, the fourth column is the online status of the SU.
# The values this column can have are:
#   'Y' - The SU is valid and online.
#   'N' - The SU is valid, but offline and archived (so it can be retrieved).
#   'X' - The SU is valid, offline, and not archived (so it cannot be retrieved).
#   'I' - The SU is invalid (it is unknown by SUMS).
def runJsocfetch(**kwargs):
    rv = {}
    
    cmd = [BIN_PATH + '/jsoc_fetch']
    for key in kwargs:
        arg = key + '=' + kwargs[key]
        cmd.append(arg)

    try:
        resp = check_output(cmd)
        output = resp.decode('utf-8')

    except ValueError:
        raise Exception('jfetch', "Unable to run command: '" + ' '.join(cmd) + "'.")
    except CalledProcessError as exc:
        raise Exception('jfetch', "Command '" + ' '.join(cmd) + "' returned non-zero status code " + str(exc.returncode))

    # Parse out the status field.
    outputList = output.splitlines()
    regExp = re.compile(r'\s*status\s*=\s*(\d+)', re.IGNORECASE)
    for line in outputList:
        matchObj = regExp.match(line)
        if matchObj is not None:
            jfStatus = int(matchObj.group(1))
            break

    if jfStatus is None:
        raise Exception('jfetch', 'Unexpected jsoc_fetch output - status field is missing.')

    if 'requestid' in kwargs:
        rv['requestid'] = kwargs['requestid']
    else:
        rv['requestid'] = None

    if jfStatus == -1:
        # jsoc_fetch was aborted.
        raise Exception('jfetch', 'Internal program timed-out.')
    elif jfStatus == 0:
        # We have the paths at the ready. Return those.
        rv['paths'] = []
        rv['status'] = 'pathsReady'
        regExpData = re.compile(r'\s*#\s+data', re.IGNORECASE)
        regExpDataLine = re.compile(r'\s*(\d+)\s+\S+\s+(\S+)\s+(\S)')
        
        # Parse out paths frmom the jsoc_fetch output. If the SU is valid, but offline and cannot be retrieved,
        # then we still want to tell the client site to add an entry in its SUMS for the SU, even though the SU
        # will be empty. We indicate that by setting the second element in the SU's 'paths' element to the
        # empty string. If the SU is invalid, then we do not want the client site to add an entry into its SUMS.
        # We indicate that by setting the second element to null.
        dataLine = False
        for line in outputList:
            if dataLine == False:
                matchObj = regExpData.match(line)
                if matchObj is not None:
                    dataLine = True
                continue
            # This is a data line:
            #   Col 1 is the SUNUM.
            #   Col 3 is the SUMS path.
            #   Col 4 is the online status of the SU. If status == 'Y', set the second element to the URL of the SU.
            #     If status == 'X', set the second element to the empty string. If status == 'I', set the second
            #     element to null.
            matchObj = regExpDataLine.match(line)
            if matchObj is not None:
                sunum = matchObj.group(1)
                suStatus = matchObj.group(3)
                
                if suStatus.lower() == 'y':
                    path = matchObj.group(2)
                elif suStatus.lower() == 'x':
                    path = ''
                elif suStatus.lower() == 'i':
                    path = None
            
            rv['paths'].append([sunum, path])
    elif jfStatus == 1 or jfStatus == 2 or jfStatus == 12:
        # jsoc_fetch started an asynchronous export request. Tell the caller to poll.
        regExp = re.compile(r'\s*requestid\s*=\s*(.+)', re.IGNORECASE)
        for line in outputList:
            matchObj = regExp.match(line)
            if matchObj is not None:
                requestid = matchObj.group(1)
                break
        
        if requestid is None:
            raise Exception('jfetch', 'Unexpected jsoc_fetch output - requestid field is missing.')

        rv['requestid'] = requestid
        rv['paths'] = None
        rv['status'] = 'pathsNotReady'
    elif jfStatus == 3:
        # A bad recordset (I'm not sure how this applies when the recordset consists of a list of SUNUMs).
        raise Exception('jfetch', 'Bad argument sent to internal program.')
    elif jfStatus == 4:
        # A bad request (I'm not sure how this differs from a bad recordset).
        raise Exception('jfetch', 'Bad argument sent to internal program.')
    elif jfStatus == 5:
        # Original request timed-out and results are no longer available
        raise Exception('jfetch', 'Internal program timed-out.')
    elif jfStatus == 6:
        # Unrecognized requestID. It is probably the case that the request was added to jsoc.export_new, but has not been seen
        # by jsoc_export_manage yet. The user should try again.
        rv['status'] = 'pathsNotReady'
    else:
        # Unexpected response from jsoc_fetch
        raise Exception('jfetch', 'Unexpected response from internal program. Unknown status code returned ' + str(jfStatus) + '.')

    return rv

if __name__ == "__main__":
    status = RET_SUCCESS
    optD = {}
    rootObj = {}
    
    try:
        optD = GetArgs(sys.argv[1:])
    
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise
        
        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'badArgs':
            status = RET_INVALIDARGS
            rootObj['status'] = 'errorArgs'
            rootObj['statusMsg'] = 'Invalid arguments supplied. ' + msg
        else:
            raise # Re-raise

    if status == RET_SUCCESS:
        try:
            if 'sunums' in optD and optD['sunums'] is not None:
                # Call jsoc_fetch to obtain paths for the requested SUNUMs. If any SU is offline, then jsoc_fetch will
                # initiate an su-as-is export request. In this case, jsoc_fetch will return a requestid that is given back to rsumsd.py, which
                # will poll for completion of the export request.
                resp = runJsocfetch(op='exp_su', method='url_quick', format='txt', protocol='su-as-is', sunum=','.join(optD['sunums']), JSOC_DBNAME=optD['dbname'], JSOC_DBHOST=optD['dbhost'], JSOC_DBUSER=optD['dbuser'])
            elif 'requestid' in optD:
                # A previous run of rs.py started an su-as-is export request. Call jsoc_fetch to check if the export request
                # has completed.
                resp = runJsocfetch(op='exp_status', requestid=optD['requestid'], format='txt')

        except Exception as exc:
            if len(exc.args) != 2:
                raise # Re-raise
        
            etype = exc.args[0]
            msg = exc.args[1]

            if etype == 'jfetch':
                status = RET_INTERNALPROG
                rootObj['status'] = 'errorInternal'
                rootObj['statusMsg'] = 'Failure running internal program. ' + msg
            else:
                raise # Re-raise

        if status == RET_SUCCESS:
            if resp['status'] == 'pathsReady':
                # We have the paths to all SUs. Return them in JSON.
                rootObj['status'] = 'complete'
                rootObj['requestid'] = resp['requestid']
                rootObj['scpUser'] = SCP_USER
                rootObj['scpHost'] = SCP_HOST
                rootObj['scpPort'] = SCP_PORT
                rootObj['paths'] = resp['paths']
            elif resp['status'] == 'pathsNotReady':
                # An asynchronous export request was initiated; tell the caller to keep polling.
                rootObj['status'] = 'pending'
                rootObj['requestid'] = resp['requestid']
            else:
                rootObj['status'] = 'errorInternal'
                rootObj['statusMsg'] = 'Error calling jsoc_fetch.'
                rootObj['requestid'] = resp['requestid']

    print('Content-type: application/json\n')
    print(json.dumps(rootObj))

    # Always return 0. If there was an error, an error code (the 'status' property) and message (the 'statusMsg' property) goes in the returned HTML.
    sys.exit(0)
