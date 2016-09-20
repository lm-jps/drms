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
            # Try to open lock file.
            try:
                fobj = open(fileName, 'a')

                # Save the lock file information.
                self._lockFile = fileName
                self._lfObj = fobj
                self._lfContent = content
                self._hasLock = False
                self._retry = retry
            except Exception as exc:
                raise Exception('drmsLock', 'Unable to open lock file ' + fileName + '.')

    def __enter__(self):
        self.acquireLock()
        if self._hasLock:
            # Write the content into the file.
            self._lfObj.write(self._lfContent)
            self._lfObj.flush()

            return self
        else:
            return None
    
    def __exit__(self, etype, value, traceback):
        self.close()

    def acquireLock(self):
        if not self._hasLock:
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

    def releaseLock(self):
        if self._hasLock:
            # I believe this could cause an exception. The caller should put the with clause in a try clause and
            # catch this excepton.
            try:
                lockf(self._lfObj.fileno(), LOCK_UN)
            except IOError:
                raise Exception('drmsLock', 'Unable to release lock.')
                    
            self._hasLock = False
            
    def close(self):
        '''Use close() when not using DrmsLock in a context-manager context.'''
        # Release the lock.
        if self._hasLock:
            self.releaseLock()
            
            # Close the file.
            self._lfObj.close()
            self._lfObj = None
        
            # Delete the lock file.
            unlink(self._lockFile)
            self._lockFile = None
        else:
            # Close the file.
            self._lfObj.close()
            self._lfObj = None
