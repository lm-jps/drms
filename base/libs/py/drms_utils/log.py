#!/usr/bin/env python3

from argparse import Action as ApAction
from enum import Enum
import inspect
from io import IOBase
import logging
import os

__all__ = [ 'Formatter', 'Log', 'LogLevel', 'LogLevelAction' ]

class LogLevel(Enum):
    CRITICAL = (1, 'critical')
    ERROR = (2, 'error')
    WARNING = (3, 'warning')
    INFO = (4, 'info')
    DEBUG = (5, 'debug')

    def __new__(cls, value, name):
        member = object.__new__(cls)
        member._value = value
        member._fullname = name
        return member

    def __int__(self):
        return self._value

class Formatter(logging.Formatter):
    pass

class Log(object):
    """Manage a logfile."""
    def __init__(self, file, drms_log_level, formatter):
        if isinstance(file, IOBase):
            self._file_name = None
            self._handler = logging.StreamHandler(file)
        else:
            self._file_name = file
            self._handler = logging.FileHandler(file)

        self._log = logging.getLogger()

        logging_level = self._level_to_logging_level(drms_log_level)
        self._log.setLevel(logging_level)
        self._handler.setLevel(logging_level)
        self._handler.setFormatter(formatter)
        self._log.addHandler(self._handler)

    def _level_to_logging_level(self, drms_log_level):
        if drms_log_level == LogLevel.CRITICAL:
            logging_level = logging.CRITICAL
        if drms_log_level == LogLevel.ERROR:
            logging_level = logging.ERROR
        if drms_log_level == LogLevel.WARNING:
            logging_level = logging.WARNING
        if drms_log_level == LogLevel.INFO:
            logging_level = logging.INFO
        if drms_log_level == LogLevel.DEBUG:
            logging_level = logging.DEBUG

        return logging_level

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
        level = self.string_to_level(value)
        setattr(namespace, self.dest, level)

    @classmethod
    def string_to_level(cls, level_str):
        level = None

        level_str_lower = level_str.lower()
        if level_str_lower == 'critical':
            level = LogLevel.CRITICAL
        elif level_str_lower == 'error':
            level = LogLevel.ERROR
        elif level_str_lower == 'warning':
            level = LogLevel.WARNING
        elif level_str_lower == 'info':
            level = LogLevel.INFO
        elif level_str_lower == 'debug':
            level = LogLevel.DEBUG
        else:
            level = LogLevel.ERROR

        return level
