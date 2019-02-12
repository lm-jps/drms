#!/usr/bin/env python3

import sys
import os
import re
import argparse
import json
from urllib.parse import urlunsplit, SplitResult
from urllib.request import urlopen
from subprocess import check_output, CalledProcessError, STDOUT, PIPE, Popen
from distutils.util import strtobool
import cgi
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser



DUMMY_TAR_FILE_NAME = 'data.tar'
DUMMY_FITS_FILE_NAME = 'data.FITS'
FILE_NAME_SIZE = 256

DE_RETURN_CODE_SUCCESS = 0
DE_RETURN_CODE_BAD_ARGUMENT = 1
DE_RETURN_CODE_BAD_PARAMETER = 2
DE_RETURN_CODE_BAD_STREAM_FORMAT = 3
DE_RETURN_CODE_CANT_RUN_SERVICE = 4
DE_RETURN_CODE_BAD_SERVICE_RESPONSE = 5

# global
debug = False


class DEException(Exception):
    def __init__(self, msg):
        self.msg = msg
        self.code = None

class ArgumentException(DEException):
    def __init__(self, msg):
        super(ArgumentException, self).__init__(msg)
        self.code = DE_RETURN_CODE_BAD_ARGUMENT

class DrmsParameterException(DEException):
    def __init__(self, msg):
        super(DrmsParameterException, self).__init__(msg)
        self.code = DE_RETURN_CODE_BAD_PARAMETER
        
class StreamFormatException(DEException):
    def __init__(self, msg):
        super(StreamFormatException, self).__init__(msg)
        self.code = DE_RETURN_CODE_BAD_STREAM_FORMAT

class RunServiceException(DEException):
    def __init__(self, msg):
        super(RunServiceException, self).__init__(msg)
        self.code = DE_RETURN_CODE_CANT_RUN_SERVICE

class ServiceResponseException(DEException):
    def __init__(self, msg):
        super(ServiceResponseException, self).__init__(msg)
        self.code = DE_RETURN_CODE_BAD_SERVICE_RESPONSE

class Choices(list):
    def __init__(self, items):
        super(Choices, self).__init__(map(str.lower, items))
        
    def __contains__(self, other):
        return super(Choices, self).__contains__(other.lower())


class CompressionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            setattr(namespace, self.dest, values)
            if values != 'none':
                setattr(namespace, 'a', '1' if strtobool(values) else '0')
            setattr(namespace, 'd', strtobool(values)) # if there is only a single file to export                
        except ValueError:
            raise ArgumentException('invalid ' + self.dest + ' value: ' + str(values))


class SkiptarAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            # the action gets invoked only by the parser if skiptar is on the command line; since we always
            # want to have this action run, even if skiptar is not on the command line, the action is called
            # with the default before the parser is run to set the default value; this will be overridden if
            # the parser also calls this action
            setattr(namespace, self.dest, '1' if strtobool(values) else '0')
            setattr(namespace, 'd', '1' if strtobool(values) else '0') # if there is only a single file to export                
        except ValueError:
            raise ArgumentException('invalid ' + self.dest + ' value: ' + str(values))


class WebappRequest(object):
    '''
    abstract class
    '''
    def __init__(self, url):
        self.url = url

    def get(self):
        with urllib.request.urlopen(self.url) as response:
            self.info = json.loads(response.read().decode('UTF-8'))
            
        if debug:
            print('web response ' + self.info)
            
        return self.info


class CGIAppRequest(WebappRequest):
    def __init__(self, url):
        super(CGIAppRequest, self).__init__(url)
        

class FlaskAppRequest(WebappRequest):
    pass

def CollectArguments():
    global debug
    ns = argparse.Namespace()
    arguments = cgi.FieldStorage()
    args = {}

    if arguments:
        for key in arguments.keys():
            val = arguments.getvalue(key)
            
            if key.lower() in ('debug'):
                # strip off the debug argument, setting the global variable 'debug'
                debug = val
            else:
                args[key] = val

    # now, pass the arguments to CmdParser so that we can do the needed error-checking
    parser = CmdlParser(usage='%(prog)s dbhost=<db host> webserver=<> spec=<> [ --filename=<> ] [ --skiptar=<boolean value> ] [ --compression=<boolean value ]')

    # Required
    parser.add_argument('dbhost', help='the machine hosting the database that serves DRMS data series', metavar='<db host>', dest='dbhost', required=True)
    parser.add_argument('webserver', help='the webserver invoking this script', metavar='<webserver>', dest='webserver', required=True)
    parser.add_argument('spec', help='the record-set specification identifying records to export', metavar='<spec>', dest='spec', required=True)

    # Optional
    parser.add_argument('--compression', 'compression', help='if rice, then Rice-compress all segment files; otherwise, do not compress any file', dest='cparms', choices=Choices([ 'rice', 'none' ]), default='none')
    parser.add_argument('--filename', 'filename', help='the filename template for the names of the exported files', dest='ffmt')
    skiptarAction = parser.add_argument('--skiptar', 'skiptar', help='if a true value, then do not produce a tar file', dest='s', action=SkiptarAction, default='true') # do not create a tar file, by default
    skiptarAction(parser, ns, skiptarAction.default)

    arglist = [ elem[0] + '=' + elem[1] for elem in list(zip(args.keys(), args.values())) ]
    
    if debug:
        print('drms-export.py arglist ' + str(arglist), file=sys.stderr)

    # will call SkiptarAction.__call__() again if skiptar is on the command line 
    return parser.parse_args(arglist, ns)

# for now, form a URL, then call the program with subprocess; when I convert things to Flask, the URL will instead
# be used with a requests object to send a request to the webapp's dedicated daemon:
#   @app.route('/some-url')
#   def get_data()
#     arg1 = request.args.get('name')
#     arg2 = request.args.get('age')
#     return requests.get('http://example.com/app?' + 'name=' + arg1 + '&' + 'age=' + arg2).content
def MakeAppRequest(domain, service, **args):
    argStr = ','.join([ key + '=' + str(val) for key, val in args.items() ])

    urlParts = SplitResult(scheme='http', netloc=domain, path='/cgi-bin/' + service, query=argStr, fragment='')
    url = urlunsplit(urlParts)
    
    if debug:
        print('making web request '+ url, file=sys.stderr)

    request = CGIAppRequest(url)
    return request.get()


try:
    args = CollectArguments()
    drmsParams = DRMSParams()
    if drmsParams is None:
        raise DrmsParameterException('unable to locate DRMS parameters file (drmsparams.py)')

    if args.dbhost.lower() == drmsParams.get('SERVER'):
        # internal server
        dbserver = args.dbhost.lower()
    else:
        # one of the external servers
        ####################
        ## Extract series ##
        ####################

        # Returns {"spec":"hmi.m_45s[2015.6.1]","atfile":false,"hasfilts":true,"nsubsets":1,"subsets":[{"spec":"hmi.m_45s[2015.6.1]","settype":"drms","seriesname":"hmi.m_45s","filter":"[2015.6.1]","autobang":false}],"errMsg":null}
        response = MakeAppRequest(args.webserver, 'drms_parserecset', spec=args.spec)

        series = []
        for aset in response['subsets']:
            print(aset['seriesname'])
            series.append(aset['seriesname'])
    
        ################################
        ## Determine Series DB Server ##
        ################################
        response = MakeAppRequest(domain=WEBAPP_DOMAIN, service='checkexpdbserver', dbhost=args.dbhost, series=','.join(series))
        dbserver = response['server'] # the server use when making the export request (possibly the internal server if whitelisted series are present)
        
    exportArgs = []
    exportArgs.extend([ arg + '=' + getattr(args, arg) for arg in vars(args) ])
    exportArgs.extend([ 'JSOC_DBHOST=' + dbserver, 'JSOC_DBNAME=' + drmsParams.get('DBNAME'), 'JSOC_DBUSER=' + drmsParams.get('WEB_DBUSER') ])
    print('export args ' + str(exportArgs), file=sys.stderr)
    
    binPy3 = drmsParams.get('BIN_PY3')
    scriptsDir = drmsParams.get('SCRIPTS_EXPORT')
    binDir = drmsParams.get('BIN_EXPORT')

    #############################
    ## Run log-cgi-instance.py ##
    #############################    
    # create row for instance ID in instance table and fetch instance ID argument
    formData = '&'.join(exportArgs)

    if 'SCRIPT_FILENAME' in os.environ:
        script = os.path.basename(os.environ['SCRIPT_FILENAME'])
    else:
        script = 'unknown-script'

    if 'SERVER_NAME' in os.environ:
        webserver = os.environ['SERVER_NAME']
    else:
        webserver = 'unknown-webserver'

    if 'REQUEST_URI' in os.environ:
        url = os.environ['REQUEST_URI']
    else:
        url = 'unknown-URL'

    if 'REQUEST_METHOD' in os.environ:
        method = os.environ['REQUEST_METHOD']
        del os.environ['REQUEST_METHOD']
    else:
        method = 'unknown-method'
        
    if 'REMOTE_ADDR' in os.environ:
        ip = os.environ['REMOTE_ADDR']
    else:
        ip = 'unknown-ip'

    # there is no webapp for this service
    cmdList = [ binPy3, os.path.join(scriptsDir, 'log-cgi-instance.py'), script, webserver, url, formData, method, ip ]
    output = None
    if debug:
        print('running ' + ' '.join(cmdList), file=sys.stderr)

    try:
        resp = check_output(cmdList)
        output = resp.decode('utf-8').rstrip()
    except ValueError as exc:
        raise RunServiceException('invalid command-line arguments: ' + ' '.join(cmdList))
    except CalledProcessError as exc:
        raise RunServiceException("command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))
    
    # output is either the empty string (on error) or the new instance ID
    if output is not None and len(output) > 0:
        instanceID = int(output)
    else:
        instanceID = -1
        
    if debug:
        print('log-cgi-instance.py returned instance ID ' + str(instanceID), file=sys.stderr)
        
    ############################
    ## Determine Architecture ##
    ############################
    # there is no webapp for this service
    cmdList = [ os.path.join(binDir, '..', 'build', 'jsoc_machine.csh') ]
    output = None
    if debug:
        print('running ' + ' '.join(cmdList), file=sys.stderr)


    try:
        resp = check_output(cmdList, stderr=STDOUT)
        output = resp.decode('utf-8')
    except ValueError as exc:
        raise RunServiceException('invalid command-line arguments: ' + ' '.join(cmdList))
    except CalledProcessError as exc:
        raise RunServiceException("command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))

    if output is None or len(output) == 0:
        raise ServiceResponseException('unexpected response from jsoc_machine.csh')

    # there should be only one output line
    outputList = output.splitlines()
    arch = outputList[0];
    
    if debug:
        print('ljsoc_machine.csh returned arch ' + arch, file=sys.stderr)

    ###############################
    ## Run drms-export-to-stdout ##
    ###############################
    # there is no webapp for this service
    
    cmdList = [ os.path.join(binDir, arch, 'drms-export-to-stdout') ]
    cmdList.extend(exportArgs)
    
    if debug:
        print('running '+ ' '.join(cmdList), file=sys.stderr)
    
    try:
        # open a pipe to the stdout of drms-export-to-stdout
        proc = Popen(cmdList, stdout=PIPE)
        
        htmlHeaderPrinted = False
        filename = ''
        while True:
            if strtobool(args.d):
                # not a tar file, a FITS file prepended with filename; read the filename first
                while not htmlHeaderPrinted and len(filename) < FILE_NAME_SIZE:
                    pipeBytes = proc.stdout.read(FILE_NAME_SIZE - len(filename))
                    if len(pipeBytes) == 0:
                        raise StreamFormatException('improper formatting for FITS-file-name header')
                    filename = filename + pipeBytes.decode('UTF8')
                    
                # truncate padding (0 bytes)
                filename = filename.rstrip('\x00')
                    
            if len(filename) == 0:
                if not strtobool(args.s):
                    # tar file
                    filename = DUMMY_TAR_FILE_NAME
                else:
                    # FITS file, which may have a filename, or it may not
                    filename = DUMMY_FITS_FILE_NAME
                    
            if not htmlHeaderPrinted:
                # force alphanumeric (plus '_')
                filename = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
                
                if debug:
                    print('file dumped ' + filename, file=sys.stderr)

                # we are streaming either a tar file (of FITS files) or a single FITS file        
                sys.stdout.buffer.write(b'Content-type: application/octet-stream\n')
                sys.stdout.buffer.write(b'Content-Disposition: attachment; filename="' + filename.encode() + b'"\n')
                sys.stdout.buffer.write(b'Content-transfer-encoding: binary\n\n')
                htmlHeaderPrinted = True;
                
            pipeBytes = proc.stdout.read(4096)
            if len(pipeBytes) > 0:
                # binary data (could be a FITS file for a tar file)                    
                sys.stdout.buffer.write(pipeBytes)
                sys.stdout.buffer.flush()
            else:
                break                    
    except ValueError as exc:
        # invalid Popen arguments
        raise RunServiceException('invalid command-line arguments: ' + ' '.join(cmdList))
    except OSError as exc:
        raise RunServiceException(exc.args[0])
except DEException as exc:
    print('Content-type: application/json\n\n')

    rootObj = { "status" : exc.code if 'code' in exc else 'unknown exit code', "error" : exc.msg if 'msg' in exc else '' }
    print(json.dumps(rootObj))
    
sys.exit(0)
