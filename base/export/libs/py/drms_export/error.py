#!/usr/bin/env python3                                                                                                                                                                           \                                                                                                                                                                                                  from enum import Enum

from .response import ErrorResponse
from drms_utils import ErrorCode as DrmsErrorCode, Error as DrmsError

__all__ = [ 'ErrorCode', 'Error' ]

class ErrorCode(DrmsErrorCode):
    # do not define enum members here - you cannot subclass an Enum class if the parent has members
    pass

class Error(DrmsError):
    _error_code = None

    def __init__(self, *, msg=None):
        super().__init__(msg=self._msg)
        self._response = None

    def _generate_response(self):
        if self._msg is None or len(self._msg) == 0:
            msg = f'{self._header}'
        else:
            msg = f'{self._header} {self._msg}'

        self._response = ErrorResponse(error_code=self._error_code, msg=self._msg)

    @property
    def response(self):
        if self._response is None:
            self._generate_response()

        return self._response
