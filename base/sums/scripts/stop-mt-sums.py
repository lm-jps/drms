#!/usr/bin/env python

# Use this script to stop one or more instances of Python SUMS (sumsd.py) on a host. The instances to be stopped must be running on the host on which the script is run. The global variable DEFAULT_INSTANCES_FILE identifies the name of the instances file. The default path to the instances file is identified by the SUMLOG_BASEDIR parameter of config.local. The caller of this script can override the name and full path to the script with the --instancesfile option.
# 
# By default, this script will terminate all sumsd.py instances identified by the required daemon argument. To terminate a subset of these instances, the caller can provide the ports argument. The value of this argument is a comma-separated list of port numbers. If this optional ports argument is provided, then the instances identified by the daemon and ports arguments will be terminated. 
# 
# To stop a sumsd.py instance that DRMS modules connect to, ensure that at least one value in the ports argument is the same as the SUMSD_LISTENPORT value in config.local.
# 
# To ensure that the state file accurately reflects the set of actively running sumsd.py processes, please do not manually start or stop sumsd.py processes. Always use start-mt-sums.py and stop-mt-sums.py.
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


class SetAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, set([ int(val) for val in values.split(',') ]))


def ShutDownInstance(path, pid):
    if psutil.pid_exists(pid):
        # send a SIGINT signal to the pid
        print('sending SIGINT to pid ' + str(pid))
        os.kill(pid, SIGINT)

        # wait for shutdown; if a timeout occurs, force shut down
        count = 0
        while psutil.pid_exists(pid) and count < 30:
            time.sleep(1)
            count += 1
            
        # kill -9 it
        if psutil.pid_exists(pid):
            print('sending SIGKILL to pid ' + str(pid))
            os.kill(pid, SIGKILL)
            
            count = 0
            while psutil.pid_exists(pid) and count < 10:
                time.sleep(1)
                count += 1


# read arguments
sumsDrmsParams = SumsDrmsParams()
parser = CmdlParser(usage='%(prog)s [ -h ] [ --instancesfile=<instances file path> ]')

# required
parser.add_argument('d', 'daemon', help='path of the sumsd.py daemon to halt', metavar='<path to daemon>', dest='daemon', required=True)

# optional
parser.add_argument('-p', '--ports', help='a comma-separated list of listening-port numbers, one for each instance to be stopped', metavar='<listening ports>', dest='ports', action=SetAction, default=set())
parser.add_argument('-i', '--instancesfile', help='the json file which contains a list of all the sumsd.py instances running', metavar='<instances file path>', dest='instancesfile', default=os.path.join(sumsDrmsParams.get('SUMLOG_BASEDIR'), DEFAULT_INSTANCES_FILE))

arguments = Arguments(parser)

# load instance file
instances = None # object to flush to instances file
instancesFile = arguments.getArg('instancesfile')
path = arguments.getArg('daemon')
ports = arguments.getArg('ports') # a set

# return list
terminatedInstances = { 'terminated' : [] }

usedPorts = {} # portStr : [ path, pid ]

try:
    with open(instancesFile, mode='r', encoding='UTF8') as fin:
        # fix any inconsistencies in the instances file, as with start-mt-sums.py, but also
        # remove the instances specified by the arguments
        if os.stat(instancesFile).st_size > 0:
            instances = json.load(fin)

            if instances is not None:
                # ensure that each instance listed is indeed running, if not, remove it from the instance file
                
                portsList = list(ports)
                if len(portsList) > 0:
                    portsSpecified = True
                else:
                    portsSpecified = False

                # copy instances since we are iterating over them
                instancesCopy = copy.deepcopy(instances)
                for onePath in instancesCopy:
                    for onePortStr in instancesCopy[onePath]:
                        onePort = int(onePortStr)
                        pid = instancesCopy[onePath][onePortStr]
        
                        if not psutil.pid_exists(pid):
                            # an entry in the instances file that should not exist
                            print('WARNING: pid ' + str(pid) + ' from instances file does not exist; removing', file=sys.stderr)
                            del instances[onePath][onePortStr]
                        else:                
                            if onePortStr in usedPorts:
                                print('WARNING: instances file has entry for port ' + onePortStr + ' already; shutting-down and removing', file=sys.stderr)
                                # cannot have duplicate port numbers in use
                                pathOrig, pidOrig = usedPorts[onePortStr]
                    
                                # shut-down and delete duplicate instance
                                print('shutting down duplicate instance ' + onePath + ':' + onePortStr)
                                ShutDownInstance(onePath, pid)
                                del instances[onePath][onePortStr]
                                if onePort not in terminatedInstances['terminated']:
                                    terminatedInstances['terminated'].append(onePort)
                    
                                # shut-down and delete original instance (unless the original has already been removed)
                                if onePortStr in instances[pathOrig]:
                                    print('shutting down original instance ' + pathOrig + ':' + portStr)
                                    ShutDownInstance(pathOrig, pidOrig)
                                    del instances[pathOrig][onePortStr]
                                    if onePort not in terminatedInstances['terminated']:
                                        terminatedInstances['terminated'].append(onePort)
                            else:
                                if onePath == path:
                                    if not portsSpecified or onePort in ports:
                                        # this is an instance to be shut down
                                        print('shutting down instance ' + onePath + ':' + onePortStr)
                                        ShutDownInstance(onePath, pid)
                                        del instances[onePath][onePortStr]
                                        terminatedInstances['terminated'].append(onePort)
                                        if len(ports) > 0:
                                            portsList.remove(onePort)
                                else:                            
                                    # track used ports
                                    usedPorts[onePortStr] = [ onePath, pid ]
                                    
                    # there should be no items in portsList - if there is, then the user supplied an invalid port number
                    for port in portsList:
                        print('invalid port argument ' + str(port), file=sys.stderr)
                    
                    if len(instances[onePath].keys()) == 0:
                        # we removed ALL port instances of this path
                        del instances[onePath]
            else:
                # it is a warning, not an error, if the user requests the shutdown of an instance, but no instances are running
                print('WARNING: no instances are running - there is nothing to shutdown', file=sys.stderr)
        # leaving open block - the instances file will be closed
except FileNotFoundError:
    # it is an error if the user requests the shutdown of an instance, but no instances are running
    print('WARNING: no instances are running - there is nothing to shutdown', file=sys.stderr)
    # any other exception will cause an exit with a return code of 1

if instances is not None:
    # save the new version of the instances file
    try:            
        with open(instancesFile, mode='w', encoding='UTF8') as fout:
            json.dump(instances, fout)
            print('', file=fout) # add a newline to the end of the instances file
            # leaving open block - this will save file changes
    except:
        print('ERROR: unable to write instances file ' + instancesFile, file=sys.stderr)
        raise # will cause an exit with a return code of 1

terminatedInstancesJSON = json.dumps(terminatedInstances, ensure_ascii=False) + '\n'
sys.stdout.buffer.write(terminatedInstancesJSON.encode('UTF8'))

sys.exit(0)
