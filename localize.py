#!/home/jsoc/bin/linux_x86_64/activepython

import os.path
import sys
import getopt
import re
from subprocess import check_output, CalledProcessError

# Constants
VERS_FILE = 'jsoc_version.h'
SDP_CFG = 'configsdp.txt'
NET_CFG = 'config.local'
NET_CFGMAP = 'config.local.map'
RET_SUCCESS = 0
RET_NOTDRMS = 1

PERL_PREFIX = """
#!/usr/bin/perl

package drmsparams;
"""

PERL_FXNS = """
sub new
{
    my($clname) = shift;
    
    my($self) = 
    {
        _paramsH => undef
    };
    
    bless($self, $clname);
    $self->{_paramsH} = $self->initialize();
    
    return $self;
}
    
sub DESTROY
{
    my($self) = shift;
}
    
"""

# Read arguments
#  d - localization directory
#  b - base name of all parameter files (e.g., -b drmsparams --> drmsparams.h, drmsparams.mk, drmsparams.pm, etc.)
#  m - make file
def GetArgs(args):
    rv = bool(0)
    optD = {}
    
    try:
        opts, remainder = getopt.getopt(args, "hd:b:",["dir=", "base="])
    except getopt.GetoptError:
        print('Usage:')
        print('localize.py [-h] -d <localization directory> -b <parameter file base>')
        rv = bool(1)
    
    if rv == bool(0):
        for opt, arg in opts:
            if opt == '-h':
                print('localize.py [-h] -d <localization directory> -b <parameter file base>')
            elif opt in ("-d", "--dir"):
                regexp = re.compile(r"(\S+)/?")
                matchobj = regexp.match(arg)
                if matchobj is None:
                    rv = bool(1)
                else:
                    optD['dir'] = matchobj.group(1)
            elif opt in ("-b", "--base"):
                optD['base'] = arg
            else:
                optD[opt] = arg
	
    return optD

def createMacroStr(key, val, keyColLen, status):
    if keyColLen < len(key):
        status = bool(1)
        return None
    else:
        nsp = keyColLen - len(key)
        spaces = str()
        for isp in range(nsp):
            spaces += ' '
        status = bool(0)
        return '#define ' + key + spaces + val + '\n'

def createPerlConst(key, val, keyColLen, status):
    if keyColLen < len(key):
        status = bool(1)
        return None
    else:
        nsp = keyColLen - len(key)
        spaces = str()
        for isp in range(nsp):
            spaces += ' '
        status = bool(0)
        return 'use constant ' + key + ' => ' + spaces + val + ';\n'

def processParam(cfgfile, line, regexpComm, regexpDefs, regexpMake, regexpQuote, regexp, keymap, defs, cDefs, mDefs, perlConstSection, perlInitSection, section):
    status = 0
    
    matchobj = regexpComm.match(line)
    if not matchobj is None:
        # Skip comment line
        return bool(0)
    
    matchobj = regexpDefs.match(line)
    if not matchobj is None:
        section.extend(list('defs'))
        return bool(0)
    
    matchobj = regexpMake.match(line)
    if not matchobj is None:
        section = 'make'
        return bool(1)
            
    if ''.join(section) == 'defs' or not cfgfile:
        matchobj = regexp.match(line)
        if not matchobj is None:
            # We have a key-value line
            keyCfgSp = matchobj.group(1)
            val = matchobj.group(2)
            
            # Must map the indirect name to the actual name
            if keymap:
                # Map to actual name only if the keymap is not empty (which signifies NA).
                if keyCfgSp in keymap:
                    key = keymap[keyCfgSp]
                elif keyCfgSp == 'LOCAL_CONFIG_SET' or keyCfgSp == 'DRMS_SAMPLE_NAMESPACE':
                    # Ignore parameters that are not useful and shouldn't have been there in the first place
                    return bool(0)
                elif not cfgfile:
                    # Should not be doing mapping for addenda
                    key = keyCfgSp
                else:
                    raise Exception('badKeyMapKey', keyCfgSp)
            else:
                key = keyCfgSp
            
            matchobj = regexpQuote.match(key)
            if not matchobj is None:
                quote = matchobj.group(1)
                key = matchobj.group(2)

                # master defs dictionary
                defs[key] = val
                
                # C header file
                if quote == "q":
                    # Add double-quotes
                    cDefs.extend(list(createMacroStr(key, '"' + val + '"', 40, status)))
                elif quote == "p":
                    # Add parentheses
                    cDefs.extend(list(createMacroStr(key, '(' + val + ')', 40, status)))
                elif quote == "a":
                    # Leave as-is
                    cDefs.extend(list(createMacroStr(key, val, 40, status)))
                else:
                    # Unknown quote type
                    raise Exception('badQuoteQual', key)
                
                if status:
                    raise Exception('paramNameTooLong', key)
                
                # Make file - val should never be quoted; just use as is
                mDefs.extend(list(key + ' = ' + val + '\n'))
                
                # Perl file - val should ALWAYS be single-quote quoted
                # Save const info to a string
                perlConstSection.extend(list(createPerlConst(key, "'" + val + "'", 40, status)))
                
                if status:
                    raise Exception('paramNameTooLong', key)
                
                # Save initialization information as a string. Now that we've defined
                # constants (the names of which are the parameter names) 
                # we can refer to those in the init section. The key variable holds the
                # name of the constant.
                perlInitSection.extend(list('  $self->{_paramsH}->{' + key + '} = ' + key + ';\n'))
            else:
                # No quote qualifier
                raise Exception('missingQuoteQual', key)
    
    return bool(0)

# We have some extraneous line or a newline - ignore.

# defs is a dictionary containing all parameters (should they be needed in this script)
def parseConfig(fin, cfile, mfile, pfile, keymap, addenda, defs, cDefs, mDefs, perlConstSection, perlInitSection):
    rv = bool(0)
    
    # Open required config file (config.local)
    try:
        # Examine each line, looking for key=value pairs.
        regexpDefs = re.compile(r"^__DEFS__")
        regexpMake = re.compile(r"^__MAKE__")
        regexpComm = re.compile(r"^\s*#")
        regexpQuote = re.compile(r"^\s*(\w):(.+)")
        regexp = re.compile(r"^\s*(\S+)\s+(.+)")
        
        section = list()
        
        # Process the parameters in the configuration file
        iscfg = bool(1)
        if not fin is None:
            for line in fin:
                ppRet = processParam(iscfg, line, regexpComm, regexpDefs, regexpMake, regexpQuote, regexp, keymap, defs, cDefs, mDefs, perlConstSection, perlInitSection, section)
                if ppRet:
                    break;
            
        # Process addenda - these are parameters that are not configurable and must be set in the 
        # NetDRMS build.
        iscfg = bool(0)
        for key in addenda:
            item = key + ' ' + addenda[key]
            ppRet = processParam(iscfg, item, regexpComm, regexpDefs, regexpMake, regexpQuote, regexp, keymap, defs, cDefs, mDefs, perlConstSection, perlInitSection, section)
            if ppRet:
                break;

    except Exception as exc:
        msg, violator = exc.args
        if msg == 'badKeyMapKey':
            # If we are here, then there was a non-empty keymap, and the parameter came from
            # the configuration file.
            print('Unknown parameter name ' + "'" + violator + "'" + ' in ' + cfgfile + '.', file=sys.stderr)
            rv = bool(1)
        elif msg == 'badQuoteQual':
            # The bad quote qualifier came from the configuration file, not the addenda, since
            # we will have fixed any bad qualifiers in the addenda (which is populated by code).
            print('Unknown quote qualifier ' + "'" + violator + "'" + ' in ' + cfgfile + '.', file=sys.stderr)
            rv = bool(1)
        elif msg == 'missingQuoteQual':
            print('Missing quote qualifier for parameter ' + "'" + violator + "'" + ' in ' + cfgfile + '.', file=sys.stderr)
            rv = bool(1)
        elif msg == 'paramNameTooLong':
            print('Macro name ' + "'" + violator + "' is too long.", file=sys.stderr)
            rv = bool(1)
        else:
            # re-raise the exception
            raise

    return rv

def getMgrUIDLine(defs, uidParam):
    rv = bool(0)
    
    cmd = 'id -u ' + defs['SUMS_MANAGER']
    try:
        ret = check_output(cmd, shell=True)
        uidParam['q:SUMS_MANAGER_UID'] = ret.decode("utf-8")
    except ValueError:
        print('Unable to run cmd: ' + cmd + '.')
        rv = bool(1)
    except CalledProcessError:
        print('Command ' + "'" + cmd + "'" + ' ran improperly.')
        rv = bool(1)

    return rv

def writeFiles(base, cfile, mfile, pfile, cDefs, mDefs, perlConstSection, perlInitSection):
    rv = bool(0)
    
    try:
        with open(cfile, 'w') as cout, open(mfile, 'w') as mout, open(pfile, 'w') as pout:
            # C file of macros
            buf = '__' + base.upper() + '_H'
            print('#ifndef ' + buf, file=cout)
            print('#define ' + buf, file=cout)
            print(''.join(cDefs), file=cout)
            print('#endif', file=cout)
            
            # Make file of make variables
            print('# This file contains a set of make-variable values - one for each configuration parameter.', file=mout)
            print(''.join(mDefs), file=mout)
            
            # Create the constants in the Perl file (mapping parameter name from config.local namespace to
            # DRMS-module namespace
            print(PERL_PREFIX, file=pout)
            print(''.join(perlConstSection), file=pout)
            print(PERL_FXNS, file=pout)
            print('sub initialize', file=pout)
            print('{', file=pout)
            print('  my($self) = shift;', file=pout)
            print('', file=pout)
            print(''.join(perlInitSection), file=pout)
            print('}', file=pout)
            
    except IOError as exc:
        sys.stderr.write(exc.strerror)
        sys.stderr.write('Unable to open a parameter vile.')
        rv = bool(1)

    return rv
            
def configureNet(cfgfile, cfile, mfile, pfile, base, keymap, addenda):
    rv = bool(0)
    
    defs = {}
    cDefs = list()
    mDefs = list()
    perlConstSection = list()
    perlInitSection = list()

    try:
        with open(cfgfile, 'r') as fin:
            rv = parseConfig(fin, cfile, mfile, pfile, keymap, addenda, defs, cDefs, mDefs, perlConstSection, perlInitSection)
            if rv == bool(0):
                # Must add a parameter for the SUMS_MANAGER UID (for some reason)
                uidParam = {}
                rv = getMgrUIDLine(defs, uidParam)
                if rv == bool(0):
                    rv = parseConfig(None, cfile, mfile, pfile, keymap, uidParam, defs, cDefs, mDefs, perlConstSection, perlInitSection)

                if rv == bool(0):
                    rv = writeFiles(base, cfile, mfile, pfile, cDefs, mDefs, perlConstSection, perlInitSection)
    except IOError as exc:
        sys.stderr.write(exc.strerror)
        sys.stderr.write('Unable to read configuration file ' + cfgfile + '.')
        

def configureSdp(cfgfile, cfile, mfile, pfile, base, keymap, addenda):
    rv = bool(0)
    
    defs = {}
    cDefs = list()
    mDefs = list()
    perlConstSection = list()
    perlInitSection = list()
    
    try:
        with open(cfgfile, 'r') as fin:
            rv = parseConfig(cfgfile, cfile, mfile, pfile, keymap, addenda, defs, cDefs, mDefs, perlConstSection, perlInitSection)
            if rv == bool(0):
                rv = writeFiles(base, cfile, mfile, pfile, cDefs, mDefs, perlConstSection, perlInitSection)
    except IOError as exc:
        sys.stderr.write(exc.strerror)
        sys.stderr.write('Unable to read configuration file ' + cfgfile + '.')

# Beginning of program
rv = RET_SUCCESS
net = bool(1)

# Parse arguments
if __name__ == "__main__":
    optD = GetArgs(sys.argv[1:])

if not(optD is None):
    # Ensure we are configuring a DRMS tree
    cdir = os.path.realpath(os.getcwd())
    versfile = cdir + '/base/' + VERS_FILE

    if not os.path.isfile(versfile):
        rv = RET_NOTDRMS

# Determine whether we are localizing a Stanford build, or a NetDRMS build. If configsdp.txt exists, then
# it is a Stanford build, otherwise it is a NetDRMS build.
if rv == RET_SUCCESS:
    stanfordFile = cdir + '/' + SDP_CFG
    if os.path.isfile(stanfordFile):
        net = bool(0)
    
    cfile = optD['dir'] + '/' + optD['base'] + '.h'
    mfile = optD['dir'] + '/' + optD['base'] + '.mk'
    pfile = optD['dir'] + '/' + optD['base'] + '.pm'

    if net:
        addenda = {}

        try:
            with open(NET_CFGMAP, 'r') as fin:
                regexpComm = re.compile(r"^\s*#")
                regexp = re.compile(r"^\s*(\S+)\s+(\w:\S+)")
                # Must map from config.local namespace to DRMS namespace (e.g., the names used for the C macros)
                keymap = {}
                for line in fin:
                    matchobj = regexpComm.match(line)
                    if not matchobj is None:
                        # Skip comment line
                        continue

                    matchobj = regexp.match(line)
                    if not(matchobj is None):
                        # We have a key-value line
                        key = matchobj.group(1)
                        val = matchobj.group(2)
                        keymap[key] = val
        except OSError:
            sys.stderr.write('Unable to read configuration map-file ' + NET_CFGMAP + '.')
            rv = bool(1)
            
        # There are three parameters that are not configurable and must be set.
        addenda['a:USER'] = 'NULL'
        addenda['a:PASSWD'] = 'NULL'
        addenda['p:DSDS_SUPPORT'] = '0'

        # We also need to set the UID of the SUMS manager. We have the name of the
        # SUMS manager (it is in the configuration file)
        configureNet(NET_CFG, cfile, mfile, pfile, optD['base'], keymap, addenda)
    else:
        configureSdp(SDP_CFG, cfile, mfile, pfile, optD['base'], {}, addenda)
    
    

