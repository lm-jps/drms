#!/usr/bin/python

from __future__ import print_function
from sys import stderr
from os import unlink
from time import sleep
from fcntl import lockf, LOCK_EX, LOCK_NB, LOCK_UN


class DrmsLock(object):
    def __new__(cls, fileName, content, retry=True):
        # Create the DrmsLock object (this is going to be self).
        return super(DrmsLock, cls).__new__(cls)

    def __init__(self, fileName, content, retry=True):
        if self:
            try:
                # save the lock file information
                self._lockFile = fileName
                self._lfObj = None
                self._lfContent = content # the content to write to the lock file if the lock is successfully acquired
                self._hasLock = False
                self._retry = retry
            except Exception as exc:
                raise Exception('drmsLock', 'Unable to open lock file ' + fileName + '.')

    def __enter__(self):
        self.acquireLock()
        if self._hasLock:
            return self
        else:
            return None
    
    def __exit__(self, etype, value, traceback):
        self.close()

    def acquireLock(self):
        '''use acquireLock() when not using DrmsLock in a context-manager context'''
        if not self._hasLock:
            if not self._lfObj:
                try:
                    self._lfObj = open(self._lockFile, 'a')
                    # read the existing value so we can restore it if we fail to acquire lock
                except Exception as exc:
                    raise Exception('drmsLock', 'unable to open lock file ' + self._lockFile)

            natt = 0
            gotLock = False

            while True:
                try:
                    # Raises an exception if the lock cannot be acquired.
                    lockf(self._lfObj.fileno(), LOCK_EX | LOCK_NB)
                    gotLock = True
                except IOError:
                    pass
        
                if gotLock or not self._retry or natt >= 10:
                    break
        
                sleep(1)
                natt += 1

            self._hasLock = gotLock

        if self._hasLock: 
            # write the content into the file; but first truncate the file (because we opened the file in 'a' mode)
            if self._lfContent and len(self._lfContent) > 0:
                self._lfObj.seek(0)
                self._lfObj.truncate()            
                self._lfObj.write(self._lfContent)
                self._lfObj.flush()
        else:
            # we did not acquire lock; because we opened in append mode, the value in the lock file will not be changed
            pass
                
        return self._hasLock

    def releaseLock(self):
        '''Use releaseLock() when not using DrmsLock in a context-manager context.'''
        if self._hasLock:
            # I believe this could cause an exception. The caller should put the with clause in a try clause and
            # catch this excepton.
            try:
                lockf(self._lfObj.fileno(), LOCK_UN)
            except IOError:
                raise Exception('drmsLock', 'Unable to release lock.')
                    
            self._hasLock = False
            
    def close(self):
        '''Use close(), after calling releaseLock(), when not using DrmsLock in a context-manager context.'''
        # release the lock
        if self._hasLock:
            self.releaseLock()
            
        # close the file
        if self._lfObj:
            self._lfObj.close()
            self._lfObj = None
    
            # delete the lock file
            unlink(self._lockFile)
