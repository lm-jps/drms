#!/usr/bin/env python3

from argparse import Action as ApAction
from enum import Enum
import inspect
from io import IOBase
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
        if isinstance(file, IOBase):
            self._file_name = None
            self._handler = logging.StreamHandler(file)
        else:
            self._file_name = file
            self._handler = logging.FileHandler(file)

        self._log = logging.getLogger()
        self._log.setLevel(level)
        self._handler.setLevel(level)
        self._handler.setFormatter(formatter)
        self._log.addHandler(self._handler)

    def close(self):
        if self._log:
            if self._handler:
                self._log.removeHandler(self._handler)
                self._handler.flush()
                self._handler.close()
                self._handler = None
            self._log = None

    def flush(self):
        if self._log and self._handler:
            self._handler.flush()

    def getLevel(self):
        # Hacky way to get the level - make a dummy LogRecord
        logRecord = self._log.makeRecord(self._log.name, self._log.getEffectiveLevel(), None, '', '', None, None)
        return logRecord.levelname

    def __prependFrameInfo(self, msg):
        frame, fileName, lineNo, fxn, context, index = inspect.stack()[2]
        return os.path.basename(fileName) + ':' + str(lineNo) + ': ' + msg

    def write_debug(self, text):
        if self._log:
            for line in text:
                self._log.debug(self.__prependFrameInfo(line))
            self._handler.flush()

    def write_info(self, text):
        if self._log:
            for line in text:
                self._log.info(self.__prependFrameInfo(line))
        self._handler.flush()

    def write_warning(self, text):
        if self._log:
            for line in text:
                self._log.warning(self.__prependFrameInfo(line))
            self._handler.flush()

    def write_error(self, text):
        if self._log:
            for line in text:
                self._log.error(self.__prependFrameInfo(line))
            self._handler.flush()

    def write_critical(self, text):
        if self._log:
            for line in text:
                self.log.critical(self.__prependFrameInfo(line))
            self._handler.flush()

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
