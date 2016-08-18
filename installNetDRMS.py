#!/usr/bin/env python3

import sys

if sys.version_info < (3, 2):
    raise Exception("You must run the 3.2 release, or a more recent release, of Python.")
    
import os
from subprocess import check_call, CalledProcessError

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.environ['LOCALIZATIONDIR']))
from drmsparams import DRMSParams

if __name__ == "__main__":
    rv = 0
    try:
        # Turn off debug builds.
        os.environ['JSOC_DEBUG'] = '0'

        # Make sure the JSOCROOT is current directory.
        os.environ['JSOCROOT'] = os.path.realpath(os.getcwd())

        try:
            # make DRMS
            cmdList = [ '/usr/bin/make' ]
            check_call(cmdList)
        
            # make SUMS
            cmdList = [ '/usr/bin/make', 'sums' ]
            check_call(cmdList)
        except CalledProcessError as exc:
            raise Exception('runMake', 'Unable to build NetDRMS.')
    except:
        import traceback
        
        print(traceback.format_exc(5), file=stderr)
        rv = 1
        
    sys.exit(rv)
