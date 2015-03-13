#!/usr/bin/env python

from __future__ import print_function
import sys
import os
import cgi
import json
from subprocess import check_output, check_call, CalledProcessError, STDOUT
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

RET_SUCCESS = 0
RET_BADARGS = 1
RET_DRMSPARAMS = 2
RET_ARCH = 3
RET_PARSESPEC = 4
RET_CHECKSERVER = 5
RET_SHOWINFO = 6

err = None
errMsg = None

def getDRMSParam(drmsParams, param):
    rv = drmsParams.get(param)
    if not rv:
        raise Exception('drmsParams', 'DRMS parameter ' + param + ' is not defined.', RET_DRMSPARAMS)

    return rv
    
try:
	optD = {}
	allArgs = []

	arguments = cgi.FieldStorage()

	if arguments:
		for key in arguments.keys():
			val = arguments.getvalue(key)

			if key in ('H', 'dbhost'):
				optD['dbhost'] = val
			else:
				if key in ('ds'):
					optD['spec'] = val

				allArgs.append(key + '=' + val)
                
	# Enforce requirements.
	if not 'dbhost' in optD:
		raise Exception('invalidArgs', "Missing required argument '" + "dbhost'.")
	if not 'spec' in optD:
		raise Exception('invalidArgs', "Missing required argument '" + "ds'.")

	drmsParams = DRMSParams()

	if drmsParams is None:
		raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).')

	# Before calling anything, make sure that QUERY_STRING is not set in the child process. Some DRMS modules, like show_series,
	# branch into "web" code if they see QUERY_STRING set.
	if 'QUERY_STRING' in os.environ:
		del os.environ['QUERY_STRING']

	series = []
	server = None

	############################
	## Determine Architecture ##
	############################
	binDir = getDRMSParam(drmsParams, 'BIN_EXPORT')
	cmdList = [os.path.join(binDir, '..', 'build', 'jsoc_machine.csh')]

	try:
		resp = check_output(cmdList, stderr=STDOUT)
		output = resp.decode('utf-8')
	except ValueError as exc:
		raise Exception('arch', exc.args[0], RET_ARCH)
	except CalledProcessError as exc:
		raise Exception('arch', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_ARCH)

	if output is None:
		raise Exception('arch', 'Unexpected response from jsoc_machine.csh', RET_ARCH)

	# There should be only one output line.    
	outputList = output.splitlines()
	arch = outputList[0];

	####################
	## Extract series ##
	####################
	cmdList = [os.path.join(binDir, arch, 'drms_parserecset'), 'spec=' + optD['spec']]

	# Returns {"spec":"hmi.m_45s[2015.6.1]","atfile":false,"hasfilts":true,"nsubsets":1,"subsets":[{"spec":"hmi.m_45s[2015.6.1]","settype":"drms","seriesname":"hmi.m_45s","filter":"[2015.6.1]","autobang":false}],"errMsg":null}
	try:
		resp = check_output(cmdList, stderr=STDOUT)
		output = resp.decode('utf-8')
		jsonObj = json.loads(output)
	except ValueError as exc:
		raise Exception('parsespec', exc.args[0], RET_PARSESPEC)
	except CalledProcessError as exc:
		raise Exception('parsespec', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_PARSESPEC)

	if jsonObj is None or jsonObj['errMsg'] is not None:
		raise Exception('parsespec', jsonObj['errMsg'], RET_PARSESPEC)

	for aset in jsonObj['subsets']:
		series.append(aset['seriesname'])

	################################
	## Determine Series DB Server ##
	################################
	# Ask checkExpDbServer.py to provide the name of the db server that can handle all the series in series.
	binPy = getDRMSParam(drmsParams, 'BIN_PY')
	scriptsDir = getDRMSParam(drmsParams, 'SCRIPTS_EXPORT')

	cmdList = [binPy, os.path.join(scriptsDir, 'checkExpDbServer.py'), '-c', 'n=1&dbhost=' + optD['dbhost'] + '&series=' + ','.join(series)]

	try:
		resp = check_output(cmdList, stderr=STDOUT)
		output = resp.decode('utf-8')
		jsonObj = json.loads(output)
	except ValueError as exc:
		raise Exception('checkserver', exc.args[0], RET_CHECKSERVER)
	except CalledProcessError as exc:
		raise Exception('checkserver', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_CHECKSERVER)

	# Returns {"series": [{"hmi.M_45s": {"server": "hmidb2"}}, {"hmi.tdpixlist": {"server": "hmidb"}}], "err": 0, "server": "hmidb"}
	if jsonObj is None or jsonObj['err'] != 0:
		raise Exception('checkserver', jsonObj['errMsg'], RET_CHECKSERVER)

	server = jsonObj['server']

	###################
	## Run show_info ##
	###################
	# show_info does NOT print the HTML header needed when it is run from the command line.
	print('Content-type: text/plain\n')
	sys.stdout.flush()
	
	cmdList = [os.path.join(binDir, arch, 'show_info'), 'JSOC_DBHOST=' + server]
	# Provide all show_info arguments passed through showextinfo.py to show_info.
	cmdList.extend(allArgs)

	try:
		check_call(cmdList)
	except ValueError as exc:
		raise Exception('showinfo', exc.args[0], RET_SHOWINFO)
	except CalledProcessError as exc:
		raise Exception('showinfo', "Command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode), RET_SHOWINFO)

except Exception as exc:
	if len(exc.args) != 3:
		msg = 'Unhandled exception.'
		raise # Re-raise

	etype, msg, rv = exc.args

	if etype != 'invalidArgs' and etype != 'drmsParams' and etype != 'arch' and etype != 'parsespec' and etype != 'checkserver' and etype != 'showinfo':
		raise

	err = rv
	errMsg = ''

	if msg and len(msg) > 0:
		if errMsg and len(errMsg) > 0:
			errMsg += ' '
		errMsg += msg

# show_info creates webpage content, if there is no failure. But if it or this script fails, then we have to create content that contains
# an error code and error message.
if err:
	print('Content-type: text/plain\n')
	print('Error status: ' + err)
	print('Error message: ' + errMsg)

sys.exit(0)
