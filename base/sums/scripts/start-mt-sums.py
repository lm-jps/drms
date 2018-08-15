#!/usr/bin/env python

# Use this script to start one or more instances of the Python SUMS (sumsd.py) on a host. The instances will be launched on the host on which the script is run. The first time this script is run, it creates a state file (the "instances" file). The global variable DEFAULT_INSTANCES_FILE identifies the name of the file. By default, the file is created in the directory identified by the SUMLOG_BASEDIR parameter of config.local. The caller of this script can override the name and full path to the script with the --instancesfile option.
# 
# This script will spawn one sumsd.py per port identified in the required ports argument. The value of this argument is a comma-separated list of port numbers. The caller identifies the path to the sumsd.py script to launch with the daemon argument. By default, for each instance, this script will start call sumsd.py with a single argument: the --sockport option. The value of the --sockport option is an element of the ports argument. The caller can pass two additional and optional arguments to sumsd.py: --loglevel (specifies the amount of sumsd.py logging to perform; the value specified will be passed as the --loglevel argument to sumsd.py), and --logfile (specifies the full path to the sumsd.py log file; the value specified will be passed as the --logfile argument to sumsd.py).
# 
# To start a sumsd.py instance that will listen to the port that DRMS modules connect to, run this script on the host identified by the SUMSERVER config local parameter, and ensure that at least one value in the ports argument is the same as the SUMSD_LISTENPORT value in config.local.
# 
# To ensure that the state file accurately reflects the set of actively running sumsd.py processes, please do not manually start or stop sumsd.py processes. Always use start-mt-sums.py and stop-mt-sums.py to start and stop them.
#
# The instances file (json) contains one property for each sumsd.py script:
#   <absolute path to script> (object) - key: the port on which the instance is listening; value: the pid identifying the sumsd.py instance
#
#   {
#      "/home/jsoc/cvs/Development/JSOC/base/sums/scripts/sumsd.py" : { "5008": 12695, "5010": 14888 },
#      "/home/jsoc/cvs/Development/JSOC_20180708/base/sums/scripts/sumsd.py" : { "6028" : 18742}
#   }
import json
from subprocess import Popen
import psutil
import sys
import argparse
import os
import copy
from signal import SIGINT, SIGKILL
import time
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser


DEFAULT_INSTANCES_FILE = 'sumsd-instances.txt'

class SMSException(object):
    pass

class ArgsException(SMSException):
    pass

class SumsDrmsParams(DRMSParams):

    def __init__(self):
        super(SumsDrmsParams, self).__init__()

    def get(self, name):
        val = super(SumsDrmsParams, self).get(name)

        if val is None:
            raise ParamsException('unknown DRMS parameter: ' + name)
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
                type, msg = exc.args
                  
                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise ArgsException(msg)
            else:
                raise # Re-raise

    def setArg(self, name, value):
        if not hasattr(self, name):
            # Since Arguments is a new-style class, it has a __dict__, so we can
            # set attributes directly in the Arguments instance.
            setattr(self, name, value)
        else:
            raise ArgsException('attempt to set an argument that already exists: ' + name)
            
    def replArg(self, name, newValue):
        if hasattr(self, name):
            setattr(self, name, newValue)
        else:
            raise ArgsException('attempt to replace an argument value for an argument that does not already exist: ' + name)

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)
        
    def getArg(self, name):
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise ArgsException('unknown argument: ' + name)


class ListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values.split(','))


def ShutDownInstance(path, pid):
    if psutil.pid_exists(pid):
        # send a SIGINT signal to the pid
        os.kill(pid, SIGINT)

        # wait for shutdown; if a timeout occurs, force shut down
        count = 0
        while psutil.pid_exists(pid) and count < 30:
            time.sleep(1)
            count += 1
            
        # kill -9 it
        if psutil.pid_exists(pid):
            os.kill(pid, SIGKILL)
            
            count = 0
            while psutil.pid_exists(pid) and count < 10:
                time.sleep(1)
                count += 1

def StartUpInstance(path, port, loglevel, logfile):
    cmdList = [ sys.executable, path, '--sockport=' + str(port) ]
    
    if loglevel is not None:
        cmdList.append('--loglevel=' + loglevel)
    if logfile is not None:
        cmdList.append('--logfile=' + logfile)
    print('running ' + ' '.join(cmdList))
    proc = Popen(cmdList) # spawn a new process
    return proc.pid

# read arguments
sumsDrmsParams = SumsDrmsParams()
parser = CmdlParser(usage='%(prog)s [ -h ] [ --instancesfile=<instances file path> ]')

# required
parser.add_argument('d', 'daemon', help='path of the sumsd.py daemon to launch', metavar='<path to daemon>', dest='daemon', required=True)
parser.add_argument('p', 'ports', help='a comma-separated list of listening-port numbers, one for each instance to be spawned', metavar='<listening ports>', dest='ports', action=ListAction, required=True)

# optional
parser.add_argument('-i', '--instancesfile', help='the json file which contains a list of all the sumsd.py instances running', metavar='<instances file path>', dest='instancesfile', default=os.path.join(sumsDrmsParams.get('SUMLOG_BASEDIR'), DEFAULT_INSTANCES_FILE))
parser.add_argument('-l', '--loglevel', help='specifies the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', dest='loglevel', default='info')
parser.add_argument('-L', '--logfile', help='the file to which sumsd logging is written', metavar='<file name>', dest='logfile')

arguments = Arguments(parser)

# load instance file
instances = None # object to flush to instances file
instancesFile = arguments.getArg('instancesfile')
path = arguments.getArg('daemon')
ports = arguments.getArg('ports')
loglevel = arguments.getArg('loglevel')
logfile = arguments.getArg('logfile') # will be None if no --logfile argument is provided

usedPorts = {} # portStr : [ path, pid ]

try:
    with open(instancesFile, mode='r', encoding='UTF8') as fin:
        if os.stat(instancesFile).st_size > 0:
            instances = json.load(fin)

            if instances is not None:
                # ensure that each instance listed is indeed running, if not, remove it from the instance file
                
                # copy instances since we are iterating over them
                instancesCopy = copy.deepcopy(instances)
                for onePath in instancesCopy:
                    for portStr in instancesCopy[onePath]:
                        port = int(portStr)
                        pid = instancesCopy[onePath][portStr]
        
                        if not psutil.pid_exists(pid):
                            # an entry in the instances file that should not exist
                            print('pid ' + str(pid) + ' from instances file does not exist', file=sys.stderr)
                            del instances[onePath][portStr]
                        else:                
                            if portStr in usedPorts:
                                print('port ' + portStr + ' is already being used', file=sys.stderr)
                                # cannot have duplicate port numbers in use
                                pathOrig, pidOrig = usedPorts[portStr]
                    
                                # shut-down and delete duplicate instance
                                ShutDownInstance(onePath, pid)
                                del instances[onePath][portStr]
                    
                                # shut-down and delete original instance
                                if portStr not in instances[pathOrig]:
                                    ShutDownInstance(pathOrig, pidOrig)
                                    del instances[pathOrig][portStr]
                            else:
                                # track used ports
                                usedPorts[portStr] = [ onePath, pid ]
          
                    if len(instances[onePath].keys()) == 0:
                        # we removed ALL port instances of this path
                        del instances[onePath]
                        
        # leaving open block - the instances file will be closed
except FileNotFoundError:
    # ignore a file-not-found error (this means that this is the first time sumsd.py was started)
    pass
    # any other exception will cause an exit with a return code of 1

if instances is None:
    instances = {} # even though the json module says this is an object, it is actually a dictionary

# launch the instances and add them to the instances file
for port in ports:
    portStr = str(port)

    # do not use ports in existence
    if portStr in usedPorts:
        raise Exception('port ' + portStr + ' is already in use')

    pid = StartUpInstance(path, port, loglevel, logfile)
    # add another instance of this path
    if path not in instances:
        instances[path] = {}

    instances[path][portStr] = pid

try:
    with open(instancesFile, mode='w', encoding='UTF8') as fout:
        json.dump(instances, fout)
        print('', file=fout) # add a newline to the end of the instances file
        # leaving open block - this will save file changes
except:
    print('ERROR: unable to write instances file ' + instancesFile, file=sys.stderr)
    raise # will cause an exit with a return code of 1

sys.exit(0)
