#!/usr/bin/env python3

from copy import deepcopy
from collections import namedtuple
from json import dumps, loads
from re import sub

from drms_utils import MakeObject

__all__ = [ 'ErrorResponse', 'Response' ]

class Response(object):
    def __init__(self, *, status_code, **kwargs):
        self._status_code = status_code
        self._kwargs = kwargs
        self._json_obj_response = None
        self._json_response = None
        self._dict_response = None

    def __str__(self):
        return str(self._status_code)

    def generate_json_obj(self):
        # certain JSON attribute names would be invalid python identifiers; drop such characters; identifies may not start with a digit
        if self._json_obj_response is None:
            # need to first call generate_json() to make the dict serializable
            self._json_obj_response = MakeObject(name='JO', data=self.generate_json())()

        return self._json_obj_response

    def generate_json(self):
        if self._json_response is None:
            self.generate_dict()
            working = deepcopy(self._dict_response)
            working['drms_export_status_code'] = int(self._dict_response['drms_export_status_code']) # serialize for json
            self._json_response = dumps(working)
        return self._json_response

    def generate_dict(self):
        if self._dict_response is None:
            self._dict_response = { 'drms_export_status' : str(self._status_code), 'drms_export_status_code' : self._status_code, 'drms_export_status_description' : self._status_code.description(**self._kwargs) }
            self._dict_response.update(self._kwargs)
        return self._dict_response

    @property
    def status_code(self):
        return self._status_code

    @property
    def attributes(self):
        self.generate_json_obj()
        return self._json_obj_response

    @classmethod
    def generate_response(cls, *, status_code=None, **kwargs):
        status_code_final = status_code if status_code is not None else cls._status_code
        return cls(status_code=status_code_final, **kwargs)

class ErrorResponse(Response):
    def __init__(self, *, error_code, error_message=None):
        super().__init__(status_code=error_code, error_message=error_message)

    @classmethod
    def generate_response(cls, *, error_code=None, **kwargs):
        error_code_final = error_code if error_code is not None else cls._error_code
        return cls(error_code=error_code_final, **kwargs)
