#!/usr/bin/env python3

import sys

if sys.version_info < (3, 4):
    raise Exception('You must run the 3.4 release, or a more recent release, of Python.')

import argparse
import urllib.request, urllib.error, urllib.parse
import copy
import re

INFO_CGI_URL = 'http://jsoc.stanford.edu/cgi-bin/ajax/show_info'

RV_SUCCESS = 0
RV_ARGS = 1
RV_URL = 2
RV_HTTP = 3
RV_OS = 4


class CmdlParser(argparse.ArgumentParser):

    def __init__(self, **kwargsin):
        kwargs = copy.deepcopy(kwargsin)
        
        # Make all arguments optional (in the ArgumentParser sense). All arguments that start with a 
        # prefix_chars char are considered optional UNLESS the required=True argument to add_argument
        # is specified.
        if 'prefix_chars' not in kwargs:
            # cannot put 'h' in here; it collides with the help argument
            kwargs['prefix_chars'] = '-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgijklmnopqrstuvwxyz'

        super(CmdlParser, self).__init__(**kwargs)
        self.required = {}
        self.reqGroup = None

    def parse_args(self, args=None, namespace=None):
        argsMod = []
        
        if args is None:
            # use the command line, but do some pre-parsing; we need to prefix command-line arguments that do not
            # start with a '-' with a '-'; this is only true for required arguments, so ensure that such
            # arguments are in self.required; command-line arguments that start with a '-' are optional, and their
            # keynames should not exist in self.required
            regexpDash = re.compile(r"-+(.+)")
            regexpEqual = re.compile(r"([^=\s]+)=(.+)")
            for arg in sys.argv[1:]:
                matchObj = regexpDash.match(arg)
                if matchObj is not None:
                    argsMod.append(arg)
                else:
                    # Argument does not start with a '-', so it must have the format arg=value
                    matchObj = regexpEqual.match(arg)
                    if matchObj is  None:
                        raise Exception('CmdlParser-ArgBadformat', 'Unrecognized argument format ' + "'" + arg + "'.")
                    
                    argsMod.append(arg)
        else:
            argsMod = args

        return super(CmdlParser, self).parse_args(argsMod, namespace)
    
    def add_argument(self, *args, **kwargs):
        # make sure the caller provided the 'dest' argument (in kwargs); this is the name of the property in the dictionary returned by 
        # parse_args(); if the caller were not to provide this argument, then ArgumentParser would apply some logic to determine the 
        # property name; however,let's simplify things and just require this argument; the base-class constructor will call 
        # add_argument() for the help argument, and it will not provide a 'dest' argument.; skip the help argument
        if '--help' not in args and 'dest' not in kwargs:
            raise Exception('CmdlParser', "Required 'dest' argument to add_argument() missing for " + str(args) + ".")
        
        # keep track of required arguments. A user who provides an argument that starts with a '-' is providing an optional
        # argument; if the option's keyname matches a reqiured argument's keyname, this is an error.
        if 'required' in kwargs and kwargs['required'] == True:
            for arg in args:
                self.required[arg] = True
            
            if self.reqGroup is None:
                self.reqGroup = super(CmdlParser, self).add_argument_group('required arguments')
            
            self.reqGroup.add_argument(*args, **kwargs)
        else:
            super(CmdlParser, self).add_argument(*args, **kwargs)

class Arguments(object):

    def __init__(self, parser=None):
        # This could raise in a few places. Let the caller handle these exceptions.
        if parser:
            self.parser = parser
        
        if parser:
            # Parse the arguments.
            self.parse()
        
            # Set all parsed args.
            self.setAllArgs()
        
    def parse(self):
        try:
            if self.parser:
                self.parsedArgs = self.parser.parse_args()      
        except Exception as exc:
            if len(exc.args) == 2:
                type, msg = exc.args
                  
                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise ArgsException(msg)
            else:
                raise # Re-raise
                
    def setParser(self, parser):
        if parser:
            self.parser = parser
            self.parse()
            self.setAllArgs()

    def setArg(self, name, value):
        if not hasattr(self, name):
            # Since Arguments is a new-style class, it has a __dict__, so we can
            # set attributes directly in the Arguments instance.
            setattr(self, name, value)
        else:
            raise ArgsException('Attempt to set an argument that already exists: ' + name + ':' + str(value) + '.')

    def setAllArgs(self):
        for key,val in list(vars(self.parsedArgs).items()):
            self.setArg(key, val)

    def getArg(self, name):
        try:
            return getattr(self, name)
        except AttributeError as exc:
            raise ArgsException('Unknown argument: ' + name + '.')


class GKException(Exception):

    def __init__(self, msg):
        self.msg = msg
        super(GKException, self).__init__(msg)

    def getMsg(self):
        return self.msg

    def printMsg(self):
        print(self.msg, file=sys.stderr)


class ArgsException(GKException):

    def __init__(self, msg):
        super(ArgsException, self).__init__(msg)



if __name__ == "__main__":
    rv = RV_SUCCESS
    msg = ''
    
    try:
        arguments = Arguments()

        parser = CmdlParser(usage='%(prog)s recset=<DRMS record-set specification> keys=<comma-separated list of keywords>')
        parser.add_argument('recset', '--recset', help='A DRMS record-set specification to select a set of records from a data series.', metavar='<DRMS record-set specification>', dest='recSet', required=True)        
        parser.add_argument('keys', '--keys', help='A comma-separated list of keywords whose values are to be downloaded.', metavar='<comma-separated list of keywords>', dest='keys', required=True)

        arguments.setParser(parser)

        cgiArgs = { 'ds' : arguments.getArg('recSet'), 'key' : arguments.getArg('keys'), 'q' : 1}
        urlArgs = urllib.parse.urlencode(cgiArgs) # for use with HTTP GET requests (not POST).
        url = INFO_CGI_URL + '?' + urlArgs

        with urllib.request.urlopen(url) as response:
            while True:
                line = response.readline().decode('UTF-8')
                if len(line) == 0:
                    break
                print(line.rstrip('\n'), file=sys.stdout)
    except ArgsException as exc:
        rv = RV_ARGS
        msg = exc.getMsg()
    except urllib.error.URLError as exc:
        rv = RV_URL
        if isinstance(exc.reason, str):
            msg = 'Inavalid URL (' + exc.reason + '): ' + url
        else:
            msg = 'Inavalid URL: ' + url
    except urllib.error.HTTPError as exc:
        rv = RV_HTTP
        msg = 'HTTP Error ' + str(exc.code) + ': ' + exc.reason
    except OSError as exc:
        rv = RV_OS
        msg = exc.strerror
    except Exception as exc:
        if len(exc.args) == 2:
            type, msg = exc.args
            
            if type != 'CmdlParser':
                raise            
        else:
            raise

    if len(msg) > 0:
        print(msg, file=sys.stderr)
    sys.exit(rv)
