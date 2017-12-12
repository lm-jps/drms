#!/usr/bin/env python3

# 1/ run on fumble.stanford.edu
# 2/ write to /surge28/spikes-tars
# 3/ use jsocport to serve the tar files via scp
# 4/ so we can run in batches, input a start date (2011.1.1_UTC) and end end date (2011.1.2_UTC) as arguments
# 5/ show_info aia.lev1'[2011.1.1_UTC-2011.1.2_UTC]' segment=spikes -P  key=WAVELNTH,T_OBS

# when specifying time ranges, DRMS will fetch information for the record immediately before the endDate, but not the 
# endDate
import sys

if sys.version_info < (3, 4):
    raise Exception('You must run the 3.4 release, or a more recent release, of Python.')

import re
import os
import logging
import inspect
import tarfile
from datetime import datetime, timedelta
from subprocess import check_output, CalledProcessError

START_DATE = '2011.1.1'
NUM_DAYS = 1
LOG_FILE = 'spikes-tar.log.txt'
LOG_LEVEL = getattr(logging, 'INFO')
TAR_DIR = '/surge28/spikes-tars'
TAR_FILE_PREFIX = 'aia-lev1-spikes_'

class Log(object):
    """Manage a logfile."""
    def __init__(self, file, level, formatter):
        self.fileName = file
        self.log = logging.getLogger()
        self.log.setLevel(level)
        self.fileHandler = logging.FileHandler(file)
        self.fileHandler.setLevel(level)
        self.fileHandler.setFormatter(formatter)
        self.log.addHandler(self.fileHandler)
        
    def close(self):
        if self.log:
            if self.fileHandler:
                self.log.removeHandler(self.fileHandler)
                self.fileHandler.flush()
                self.fileHandler.close()
                self.fileHandler = None
            self.log = None
            
    def flush(self):
        if self.log and self.fileHandler:
            self.fileHandler.flush()
            
    def getLevel(self):
        # Hacky way to get the level - make a dummy LogRecord
        logRecord = self.log.makeRecord(self.log.name, self.log.getEffectiveLevel(), None, '', '', None, None)
        return logRecord.levelname
        
    def __prependFrameInfo(self, msg):
        frame, fileName, lineNo, fxn, context, index = inspect.stack()[2]
        return os.path.basename(fileName) + ':' + str(lineNo) + ': ' + msg

    def writeDebug(self, text):
        if self.log:
            for line in text:                
                self.log.debug(self.__prependFrameInfo(line))
            self.fileHandler.flush()
            
    def writeInfo(self, text):
        if self.log:
            for line in text:
                self.log.info(self.__prependFrameInfo(line))
        self.fileHandler.flush()
    
    def writeWarning(self, text):
        if self.log:
            for line in text:
                self.log.warning(self.__prependFrameInfo(line))
            self.fileHandler.flush()
    
    def writeError(self, text):
        if self.log:
            for line in text:
                self.log.error(self.__prependFrameInfo(line))
            self.fileHandler.flush()
            
    def writeCritical(self, text):
        if self.log:
            for line in text:
                self.log.critical(self.__prependFrameInfo(line))
            self.fileHandler.flush()

if __name__ == "__main__":
    curDir = os.environ['PWD']
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log = Log(os.path.join(curDir, LOG_FILE), LOG_LEVEL, formatter)

    # iterate through output lines
    regExpLine = re.compile(r'\s*(\S+)\s*(\S+)\s*(\S+)\s*')
    regExpObs = re.compile(r'(\S+)T')
    tarFile = None
    tarFilePath = None
    tarf = None
    
    # day loop
    for day in range(0, NUM_DAYS):
        drmsDate = datetime.strptime(START_DATE, '%Y.%m.%d')
        cmdList = [ '/home/jsoc/cvs/Development/JSOC/bin/linux_avx/show_info', 'aia.lev1[' + drmsDate.strftime('%Y.%m.%d_UTC') + '/1d][? QUALITY>=0 ?]', 'segment=spikes', 'key=T_OBS,WAVELNTH' , '-Pq' ]

        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8')
        except ValueError as exc:
            raise Exception('invalid arguments to show_info: ' + ','.join(cmdList))
        except CalledProcessError as exc:
            raise Exception("command '" + ' '.join(cmdList) + "' returned non-zero status code " + str(exc.returncode))
                
        for line in output.split('\n'):
            # parse the stdout
            matchObj = regExpLine.match(line)
            if matchObj is not None:
                obsTime = matchObj.group(1)
                wavelength = matchObj.group(2)
                dir = matchObj.group(3)
            
                matchObj = regExpObs.match(obsTime)
                if matchObj is not None:
                    obsDay = matchObj.group(1)
                    spikesFile = os.path.join(dir, 'spikes.fits')
        
                    # run tar

                    # if the current tar file contains data files of the same day as the current data file, then
                    # add the current data file to the current tar file, otherwise, create a new tar file and
                    # add the current data file to it
                    tarFileArchiveFile = obsTime + '_' + wavelength.zfill(4) + '.spikes.fits'
                    if tarFile is None or TAR_FILE_PREFIX + obsDay != tarFile.split('.')[0]:
                        if tarf:
                            tarf.close()
                            log.writeInfo([ 'closed tar file ' + tarFilePath ])
                            tarf = None

                        tarFile = TAR_FILE_PREFIX + obsDay + '.tar'
                        tarFilePath = os.path.join(TAR_DIR, tarFile)
                        try:
                            tarf = tarfile.open(tarFilePath, 'w') # could raise
                            log.writeInfo([ 'created new tar file ' + tarFilePath ])
                        except OSError:
                            log.writeError([ 'could not create new tar file ' + tarFilePath ])
                            continue
                
                    if os.path.exists(spikesFile):
                        try:
                            tarf.add(spikesFile, arcname=tarFileArchiveFile) # could raise
                            log.writeInfo([ 'added ' + spikesFile + ' to archive ' + tarFilePath + ' as ' + tarFileArchiveFile ])
                        except OSError:
                            log.writeError([ 'unable to add spikes file ' + spikesFile + ' to archive ' + tarFilePath ])
                            continue
                else:
                    log.writeError([ 'invalid observation time value format: ' + obsTime ])
            else:
                # show_info prints an extra blank line at the end of its output - ignore it
                if len(line) != 0:
                    log.writeError([ 'invalid show_info output line: ' + line.strip() ])
                    
        # close tar for last day
        if tarf:
            tarf.close()
            log.writeInfo([ 'closed tar file ' + tarFilePath ])
            tarf = None

        # next day
        drmsDate = drmsDate + timedelta(days=1)
