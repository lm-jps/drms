#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import re
from subprocess import check_output, check_call, CalledProcessError
import smtplib
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../base/libs/py'))
from drmsCmdl import CmdlParser

# Hard code a bunch of stuff since I don't have time to do this correctly.
PROD_ROOTDIR = '/home/jsoc/cvs/Development'
WAYSTATION = 'waystation'
WAYSTATION_USER = 'arta'
WAYSTATION_DOMAIN = 'sun.stanford.edu'

RV_SUCCESS = 0
RV_ERROR_MAIL = 1
RV_ERROR_ARGS = 2

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

def sendMail(localName, domainName, details):
    subject = 'Production Binary Tree Check'
    fromAddr = 'jsoc@solarpost.stanford.edu'
    toAddrs = [ localName + '@' + domainName ]
    msg = 'From: jsoc@solarpost.stanford.edu\nTo: ' + ','.join(toAddrs) + '\nSubject: ' + subject + '\n' + details

    try:
        server = smtplib.SMTP('solarpost.stanford.edu')
        server.sendmail(fromAddr, toAddrs, msg)
        server.quit()
    except Exception as exc:
        # If any exception happened, then the email message was not received.
        raise Exception('emailBadrecipient', 'Unable to send email message.', RV_ERROR_MAIL)

def getArgs():
    optD = {}

    try:
        parser = CmdlParser(usage='%(prog)s [ -h ] [ -m | --mail ]')

        # Optional
        # Cannot combine metavar with action for some unknown reason.
        parser.add_argument('-m', '--mail', help='Send the waystation user the list of changed files in a mail message', dest='mail', action='store_true')

        args = parser.parse_args()

        # Read the series info from
    except Exception as exc:
        if len(exc.args) != 2:
            raise # Re-raise

        etype = exc.args[0]
        msg = exc.args[1]

        if etype == 'CmdlParser-ArgUnrecognized' or etype == 'CmdlParser-ArgBadformat' or etype == 'CmdlParser':
            raise Exception('getArgs', 'Unable to parse command-line arguments.', RV_ERROR_ARGS)
        else:
            raise # Re-raise.

    optD['mail'] = args.mail

    return optD

msg = None
rv = RV_SUCCESS


# Run the linux rsync command to see which files have timestamps that are newer than the waystation files.
# CAVEAT: rsync runs differently and takes different arguments on different machines.
#   su - arta
#   cd /home/jsoc/cvs/Development
#   rsync -aluv --dry-run JSOC/ waystation/JSOC
try:
    optD = getArgs()
    changedFiles = []
    with Chdir(PROD_ROOTDIR) as ret:
        cmdList = ['rsync', '-aluv', '--dry-run', 'JSOC/', 'waystation/JSOC']
        if ret == 0:
            resp = check_output(cmdList)
            output = resp.decode('utf-8')
            
            if output:
                # Parse out the status field.
                outputList = output.splitlines()
                firstLine = True
                
                # One of the bad things about calling an external program is that you cannot control what it does.
                # In this case, different implementations of rsync format the output differently. So, take a guess
                # and assume the first line is not part of the file list, and that there is a newline separating
                # the last item in the file list and the output footer.
                regExp = re.compile(r'\s*(\S+)\s*')
                
                for line in outputList:
                    if firstLine:
                        firstLine = False
                        continue
                    
                    matchObj = regExp.match(line)
                    if matchObj is not None:
                        # We have a non-empty line.
                        # Skip directories. Report files only.
                        item = matchObj.group(1)
                        if os.path.isfile('JSOC/' + item):
                            changedFiles.append(item)
                        else:
                            continue
                    else:
                        # This line is one past the last item in the file list.
                        break
    
                # Now if there is at least one file that changed, send an email to WAYSTATION_USER
                if changedFiles and len(changedFiles) >= 1:
                    if optD['mail']:
                        sendMail(WAYSTATION_USER, WAYSTATION_DOMAIN, 'List of files changed:\n' + '\n'.join(changedFiles))
                    else:
                        print('Files Modified in Production Directory:\n' + '\n'.join(changedFiles))

        else:
            print('Unable to change directory to ' + PROD_ROOTDIR, file=sys.stderr)

except CalledProcessError as exc:
    if exc.output:
        print('Error calling rsync: ' + exc.output, file=sys.stderr)
except ValueError:
    print('Bad arguments to rsync: \n' + '\n'.join(cmdList[1:]))
except Exception as exc:
    if len(exc.args) != 3:
        raise # Re-raise

    etype = exc.args[0]

    if etype == 'emailBadrecipient':
        msg = exc.args[1]
        rv = exc.args[2]
    else:
        raise # Re-raise

if msg:
    print(msg, file=sys.stderr)

sys.exit(rv)
