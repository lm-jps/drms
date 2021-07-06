#!/usr/bin/env python3                                                                                                                                                                            
from enum import Enum
from .statuscode import StatusCode as SC

__all__ = [ 'ErrorCode', 'Error' ]

class ErrorCode(SC):
    # do not define enum members here - you cannot subclass an Enum class if the parent has members
    pass

class Error(Exception):
    _error_code = None

    def __init__(self, *, msg=None):
        if msg is None:
            self._msg = f'{self._error_code.fullname}'
        else:
            self._msg = msg

        if not hasattr(self, '_header'):
            self._header = f'[ {self.__class__.__name__} ]'

        self._response = None

        super().__init__(self._msg)
