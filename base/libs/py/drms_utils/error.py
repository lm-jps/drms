#!/usr/bin/env python3
from enum import Enum
from .statuscode import StatusCode as SC

__all__ = [ 'ErrorCode', 'Error' ]

class ErrorCode(SC):
    # do not define enum members here - you cannot subclass an Enum class if the parent has members
    pass

class Error(Exception):
    _error_code = None

    def __init__(self, *, error_message=None):
        if error_message is None:
            self._error_message = f'{self._error_code.description()}'
        else:
            self._error_message = error_message

        if not hasattr(self, '_header'):
            self._header = f'[ {self.__class__.__name__} ]'

        super().__init__(self._error_message)

    @property
    def message(self):
        return self._error_message
