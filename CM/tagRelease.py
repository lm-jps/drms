#!/home/jsoc/bin/linux_x86_64/activepython
import sys, getopt
import re
import os
import shutil
from subprocess import call

# Return values
kRetSuccess   = 0
kRetArgs      = 1
kRetOS        = 2
kRetRegexp    = 3

# Constants
kTagCmd = '/home/jsoc/dlsource.pl -o tag'
kUntagCmd = '/home/jsoc/dlsource.pl -o untag'
kTmpFile = '/tmp/.versfile.tmp'

# Classes

# This class changes the current working directory, and restores the original working directory when 
# the context is left.
class Chdir:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = os.path.realpath(newPath)
    
    def __enter__(self):
        self.savedPath = os.path.realpath(os.getcwd())
        os.chdir(self.newPath)
        cdir = os.path.realpath(os.getcwd())
        print('new path is ' + self.newPath + ', cdir is ' + cdir)
        if cdir == self.newPath:
            return 0
        else:
            return 1
    
    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
        cdir = os.path.realpath(os.getcwd())
        if cdir == self.savedPath:
            return 0
        else:
            return 1

# Read arguments
def GetArgs(args):
    rv = kRetSuccess
    optD = {}

    # tag by defuault
    optD['untag'] = 0
    
    try:
        opts, remainder = getopt.getopt(args,"ht:uv:",["tree=", "version="])
    except getopt.GetoptError:
        print('Usage:')
        print('tagRelease.py [-hu] -t <CVS tree with jsoc_version.h> -v <version string>')
        rv = kRetArgs

    if rv == kRetSuccess:
        for opt, arg in opts:
            if opt == '-h':
                print('tagRelease.py [-hu] -t <CVS tree with jsoc_version.h> -v <version string>')
            elif opt in ("-t", "--tree"):
                regexp = re.compile(r"(\S+)/?")
                matchobj = regexp.match(arg)
                if matchobj is None:
                    rv = kRetArgs
                else:
                    optD['tree'] = matchobj.group(1)
            elif opt == '-u':
                optD['untag'] = 1
            elif opt in ("-v", "--version"):
                regexp = re.compile(r"\d+(\.\d+)?")
                ret = regexp.match(arg)
                if ret is None:
                    print('Invalid version string ' + arg)
                    rv = kRetArgs
                optD['version'] = arg
            else:
                optD[opt] = arg
	
    return optD

# Create version strings for jsoc_version.h and CVS, returns a tuple
# containing either the two release strings (e.g., ("V8R1", "(801)", "8-1")), 
# or the two development strings (e.g., ("V8R1X", "(-801)", "8-1")). The
# third member of the tuple is always "8-1". Which 
# tuple is returned is controlled by the 'dev' argument
def CreateVersString(versin, dev):
    regexp = re.compile(r"(\d+)\.(\d+)")
    matchobj = regexp.match(versin)
    
    if not(matchobj is None):
        try:
            maj = matchobj.group(1)
            min = matchobj.group(2)
        except IndexError:
            print('Invalid regexp group number.')
            return None
        
        tagstring = maj + "-" + min
        
        if dev == 0:
            return ("V" + maj + "R" + min, "(" + maj + "{0:02d}".format(int(min)) + ")", tagstring)
        else:
            return ("V" + maj + "R" + min + "X", "(-" + maj + "{0:02d}".format(int(min)) + ")", tagstring)
    else:
        return None

def EditVersionFile(versfile, verstuple):
    rv = kRetSuccess
    
    print(versfile)
    
    try:
        with open(versfile, 'r') as fin, open(kTmpFile, 'w') as fout: 
            regexp1 = re.compile(r"#define\s+jsoc_version\s+\"\w+\"")
            regexp2 = re.compile(r"#define\s+jsoc_vers_num\s+\(\-?\d+\)")
        
            for line in fin:
                matchobj1 = regexp1.match(line)
                matchobj2 = regexp2.match(line)
                if not (matchobj1 is None):
                    fbuf = "#define jsoc_version \"" + verstuple[0] + "\"\n"
                    fout.write(fbuf)
                elif not (matchobj2 is None):
                    fbuf = "#define jsoc_vers_num " + verstuple[1] + "\n"
                    fout.write(fbuf)
                else:
                    # Simply copy input to output
                    fout.write(line)
    except OSError:
        print('Unable to read or write input or output file.')
        rv = kRetOS
    except re.error:
        print('Bad regular expression string.')
        rv = kRetRegexp

    if rv == kRetSuccess:
        # Rename tmp file
        try:
            shutil.move(kTmpFile, versfile)
        except OSError:
            print('Unable to rename file ', kTmpFile, 'to ', versfile)
            rv = kRetOS

    return rv


rv = kRetSuccess
optD = {}
tree = ''
version = ''
untag = 0
cmd = ''
ret = 0

if __name__ == "__main__":
    optD = GetArgs(sys.argv[1:])
    
if not(optD is None):
    if not(optD['version'] is None) and not(optD['untag'] is None):
        tree = optD['tree']
        version = optD['version']
        untag = optD['untag']
        versfile = tree + '/base/jsoc_version.h'
        verstuple = CreateVersString(version, 0)
        
        if verstuple is None:
            print('Invalid version string ' + version)
            rv = kRetArgs

        if rv == kRetSuccess:
            # Edit jsoc_version.h - set the release version of the version numbers.
            rv = EditVersionFile(versfile, verstuple)
        
        if rv == kRetSuccess:
            # Commit jsoc_version.h back to CVS
            try:
                with Chdir(tree) as ret:
                    if ret == 0:
                        cmd = 'cvs commit -m "Set the release versions of the version macros for the ' + version + ' release."' + versfile
                        ret = call(cmd, shell=True)
                    else:
                        print('Unable to cd to ' + tree + '.')
                        rv = kRetOS
            except OSError:
                print('Unable to cd to ' + tree + '.')
                rv = kRetOS
            except ValueError:
                print('Unable to run cvs cmd: ' + cmd + '.')
                rv = kRetOS
            
        if not(ret == 0):
            rv = kRetOS

        if rv == kRetSuccess:
            # Create the tags
            try:
                # Untag existing tags (if they exist). If the tag does not exist, then 
                # no error is returned. Calling dlsource.pl is a bit inefficient since
                # each time a new CVS tree is downloaded.
                
                # Full DRMS-release tags
                cmd = '/home/jsoc/dlsource.pl -o untag -f sdp -t Ver_' + verstuple[2]
                ret = call(cmd, shell=True)
                if not(ret == 0):
                    print('ERROR: Unable to delete tag Ver_' + verstuple[2])
                    rv = kRetOS
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o untag -f sdp -t Ver_LATEST'
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to delete tag Ver_LATEST')
                        rv = kRetOS
            
                # NetDRMS-release tags
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o untag -f net -t NetDRMS_Ver_' + verstuple[2]
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to delete tag NetDRMS_Ver_' + verstuple[2])
                        rv = kRetOS
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o untag -f net -t Ver_DRMSLATEST'
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to delete tag Ver_DRMSLATEST')
                        rv = kRetOS
            
                # Create new tags

                # Full DRMS-release tags
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o tag -f sdp -t Ver_' + verstuple[2]
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to create tag Ver_' + verstuple[2])
                        rv = kRetOS
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o tag -f sdp -t Ver_LATEST'
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to create tag Ver_LATEST')
                        rv = kRetOS

                # NetDRMS-release tags
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o tag -f net -t NetDRMS_Ver_' + verstuple[2]
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to create tag NetDRMS_Ver_' + verstuple[2])
                        rv = kRetOS
                if rv == kRetSuccess:
                    cmd = '/home/jsoc/dlsource.pl -o tag -f net -t Ver_DRMSLATEST'
                    ret = call(cmd, shell=True)
                    if not(ret == 0):
                        print('ERROR: Unable to create tag Ver_DRMSLATEST')
                        rv = kRetOS
            except ValueError:
                print('Unable to run cvs cmd: ' + cmd + '.')
                rv = kRetOS

        if rv == kRetSuccess:
            # Edit jsoc_version.h - set the development version of the version number.
            verstuple = CreateVersString(version, 1)

        if verstuple is None:
            print('Invalid version string ' + version)
            rv = kRetArgs

        if rv == kRetSuccess:
            # Edit jsoc_version.h - set the development version of the version numbers.
            rv = EditVersionFile(versfile, verstuple)
                    
        if rv == kRetSuccess:
            # Commit jsoc_version.h back to CVS
            try:
                with Chdir(tree) as ret:
                    if ret == 0:
                        cmd = 'cvs commit -m "Set the development version of the version macros for the ' + version + ' release." versfile'
                        ret = call(cmd, shell=True)
                    else:
                        rv = kRetOS
            except OSError:
                print('Unable to cd to ' + tree + '.')
                rv = kRetOS
            except ValueError:
                print('Unable to run cvs cmd: ' + cmd + '.')
                rv = kRetOS

            if not(ret == 0):
                rv = kRetOS

    else:
        print('Invalid arguments.')
        rv = kRetArgs
else:
    print('Invalid arguments.')
    rv = kRetArgs

exit(rv)
