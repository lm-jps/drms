import sys
import argparse
import copy
import re

class CmdlParser(argparse.ArgumentParser):
    def __init__(self, **kwargsin):
        kwargs = copy.deepcopy(kwargsin)
        
        # Make all arguments optional (in the ArgumentParser sense). All arguments that start with a 
        # prefix_chars char are considered optional UNLESS the required=True argument to add_argument
        # is specified. If we do not do this, then we cannot have program arguments of the form
        # myvar=x be optional. Also, argparse considers all arguments that do not start with a prefix_chars 
        # char as positional arguments, which means that the position of a value in the command-line
        # determines which argument the value get assigned to, which is not a good thing for the 
        # normal way we use arguments at the JSOC. At the JSOC, we like to use command-line arguments
        # of the form <argname>=<argvalue>. Only argparse options, not positional arguments, allow the
        # user to use the <option>=<value> syntax.
        if 'prefix_chars' not in kwargs:
            # Can't put 'h' in here for some reason. It collides with the help argument.
            kwargs['prefix_chars'] = '-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgijklmnopqrstuvwxyz'

        # ACK! super() with no arguments is compatible with Py 3, not Py 2, in which case 1 argument is required.
        super(CmdlParser, self).__init__(**kwargs)
        self.required = {}
        self.reqGroup = None
    
    
    def parse_args(self, args=None, namespace=None):
        argsMod = []
        
        if args is None:
            # Use the command line, but do some preparsing. We need to prefix command-line arguments that do not
            # start with a '-' with a '-'. This is only true for required arguments, so ensure that such
            # arguments are in self.required. Command-line arguments that start with a '-' are optional, and their
            # keynames should not exist in self.required.
            regexpDash = re.compile(r"-+(.+)")
            regexpEqual = re.compile(r"([^=\s]+)=(.+)")
            for arg in sys.argv[1:]:
                matchObj = regexpDash.match(arg)
                if matchObj is not None:
                    argsMod.append(arg)
                else:
                    # Argument does not start with a '-'. It must be a required argument.
                    matchObj = regexpEqual.match(arg)
                    if matchObj is not None:
                        if matchObj.group(1) not in self.required:
                            raise Exception('CmdlParser-ArgUnrecognized', 'Unrecognized argument ' + "'" + arg + "'.")
                    else:
                        raise Exception('CmdlParser-ArgBadformat', 'Unrecognized argument format ' + "'" + arg + "'.")
                    
                    argsMod.append(arg)
        else:
            argsMod = args

        # ACK! super() with no arguments is compatible with Py 3, not Py 2, in which case 1 argument is required.
        return super(CmdlParser, self).parse_args(argsMod, namespace)
    
    def add_argument(self, *args, **kwargs):
        # Make sure the caller provided the 'dest' argument (in kwargs). This is the name of the property in the dictionary returned by parse_args().
        # If the caller were not to provide this argument, then ArgumentParser would apply some logic to determine the property name. However,
        # let's simplify things and just require this argument. The base-class constructor will call add_argument() for the help argument,
        # and it will not provide a 'dest' argument. Skip the help argument.
        if '--help' not in args and 'dest' not in kwargs:
            raise Exception('CmdlParser', "Required 'dest' argument to add_argument() missing for " + str(args) + ".")
        
        # Keep track of required arguments. A user who provides an argument that starts with a '-' is providing an optional
        # argument. If the option's keyname matches a reqiured argument's keyname, this is an error.
        if 'required' in kwargs and kwargs['required'] == True:
            for arg in args:
                self.required[arg] = True
            
            if self.reqGroup is None:
                # ACK! super() with no arguments is compatible with Py 3, not Py 2, in which case 1 argument is required.
                self.reqGroup = super(CmdlParser, self).add_argument_group('required arguments')
            
            self.reqGroup.add_argument(*args, **kwargs)
        else:
            # ACK! super() with no arguments is compatible with Py 3, not Py 2, in which case 1 argument is required.
            super(CmdlParser, self).add_argument(*args, **kwargs)
