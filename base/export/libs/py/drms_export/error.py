#!/usr/bin/env python3                                                                                                                                                                           \                                                                                                                                                                                                  from enum import Enum

from .response import ErrorResponse
from drms_utils import ErrorCode as DrmsErrorCode, Error as DrmsError

__all__ = [ 'ErrorCode', 'Error' ]

class ErrorCode(DrmsErrorCode):
    # do not define enum members here - you cannot subclass an Enum class if the parent has members
    pass

class Error(DrmsError):
    _error_code = None
    _header = None

    def __init__(self, *, error_message=None):
        super().__init__(error_message=error_message)
        self._response = None

    def _generate_response(self):
        if self._header is not None:
            if self._error_message is None or len(self._error_message) == 0:
                error_message = f'{self._header}'
            else:
                error_message = f'{self._header} {self._error_message}'

        self._response = ErrorResponse(error_code=self._error_code, error_message=self._error_message)

    @property
    def response(self):
        if self._response is None:
            self._generate_response()

        return self._response
