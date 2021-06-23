import cgi
import argparse
from cmdline_parser import CmdlParser

__all__ = [ 'CgiParser' ]

class CgiParser(CmdlParser):
    def __init__(self, **kwargs):
        super(CgiParser, self).__init__(**kwargs)

    def parse_args(self, args=None, namespace=None):
        if args is None:
            argsMod = []
            # Use the arguments obtained from the cgi package (which obtains the arguments
            # from either STDIN (for an HTTP POST request) or from command line (for an HTTP
            # GET request)).
            try:
                # Try to get arguments with the cgi module. If that doesn't work, then fetch them from the command line.
                cgiArgs = cgi.FieldStorage()

                if cgiArgs:
                    for key in cgiArgs.keys():
                        val = cgiArgs.getvalue(key)
                        argsMod.append(key + '=' + val)

            except ValueError:
                raise Exception('CgiParser-ArgBadformat', 'Arguments do not conform to recognized format.')
        else:
            argsMod = args

        return super(CgiParser, self).parse_args(argsMod, namespace)

    def add_argument(self, *args, **kwargs):
        super(CgiParser, self).add_argument(*args, **kwargs)
