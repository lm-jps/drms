#!/usr/bin/env python3

from argparse import Action as ApAction
from enum import Enum
import inspect
import logging
import os

__all__ = [ 'Formatter', 'Log', 'LogLevel', 'LogLevelAction' ]

class LogLevel(Enum):
    CRITICAL = 1, 'critical'
    ERROR = 2, 'error'
    WARNING = 3, 'warning'
    INFO = 4, 'info'
    DEBUG = 5, 'debug'

class Formatter(logging.Formatter):
    pass

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

    def write_debug(self, text):
        if self.log:
            for line in text:
                self.log.debug(self.__prependFrameInfo(line))
            self.fileHandler.flush()

    def write_info(self, text):
        if self.log:
            for line in text:
                self.log.info(self.__prependFrameInfo(line))
        self.fileHandler.flush()

    def write_warning(self, text):
        if self.log:
            for line in text:
                self.log.warning(self.__prependFrameInfo(line))
            self.fileHandler.flush()

    def write_error(self, text):
        if self.log:
            for line in text:
                self.log.error(self.__prependFrameInfo(line))
            self.fileHandler.flush()

    def write_critical(self, text):
        if self.log:
            for line in text:
                self.log.critical(self.__prependFrameInfo(line))
            self.fileHandler.flush()

class LogLevelAction(ApAction):
    def __call__(self, parser, namespace, value, option_string=None):
        valueLower = value.lower()
        if valueLower == 'critical':
            level = logging.CRITICAL
        elif valueLower == 'error':
            level = logging.ERROR
        elif valueLower == 'warning':
            level = logging.WARNING
        elif valueLower == 'info':
            level = logging.INFO
        elif valueLower == 'debug':
            level = logging.DEBUG
        else:
            level = logging.ERROR

        setattr(namespace, self.dest, level)
