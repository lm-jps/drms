#!/usr/bin/python

from __future__ import print_function
from sys import stderr
from os import unlink
from time import sleep
from fcntl import lockf, LOCK_EX, LOCK_NB, LOCK_UN


class DrmsLock(object):
    lockFile = None
    fileObj = None
    hasLock = False
    
    def __new__(cls, fileName, content):
        # Create the object (this is going to be self).
        rObj = super(DrmsLock, cls).__new__(cls, fileName, content)
        
        # Try to open lock file.
        try:
            fobj = open(fileName, 'w')
            
            # Write the content into the file.
            fobj.write(content)
            
            # Save the lock file information.
            rObj.lockFile = fileName
            rObj.fileObj = fobj
        except:
            return None

        # Keep file open
        return rObj

    def __init__(self, fileName, content):
        if self:
            pass

    def __enter__(self):
        self.acquireLock()
        if self.hasLock:
            return self
        else:
            return None
    
    def __exit__(self, etype, value, traceback):
        # Release the lock.
        if self.hasLock:
            self.releaseLock()
        
        # Close the file.
        self.fileObj.close()
        self.fileObj = None

        # Delete the lock file.
        unlink(self.lockFile)
        self.lockFile = None

    def acquireLock(self):
        if not self.hasLock:
            natt = 0
            gotLock = False
            
            while True:
                try:
                    # Raises an exception if the lock cannot be acquired.
                    lockf(self.fileObj.fileno(), LOCK_EX | LOCK_NB)
                    gotLock = True
                except IOError:
                    pass
        
                if gotLock or natt >= 10:
                    break
        
                sleep(1)
                natt += 1
            
            
            self.hasLock = gotLock

    def releaseLock(self):
        if self.hasLock:
            # I believe this could cause an exception. The caller should put the with clause in a try clause and
            # catch this excepton.
            try:
                lockf(self.fileObj.fileno(), LOCK_UN)
            except IOError:
                raise Exception('drmsLock', 'Unable to release lock.')
                    
            self.hasLock = False

