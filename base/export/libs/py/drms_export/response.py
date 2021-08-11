#!/usr/bin/env python3

from copy import deepcopy
from collections import namedtuple
from json import dumps, loads

__all__ = [ 'Response' ]

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
        if self._json_obj_response is None:
            self._json_obj_response = loads(self.generate_json(), object_hook = lambda d : namedtuple('JO', d.keys()) (*d.values()))
        return self._json_obj_response

    def generate_json(self):
        if self._json_response is None:
            self.generate_dict()
            working = deepcopy(self._dict_response)
            working['status_code'] = int(self._dict_response['status_code']) # serialize for json
            self._json_response = dumps(working)
        return self._json_response

    def generate_dict(self):
        if self._dict_response is None:
            self._dict_response = { 'status' : str(self._status_code), 'status_code' : self._status_code, 'status_description' : self._status_code.description(**self._kwargs) }
            self._dict_response.update(self._kwargs)
        return self._dict_response

    @classmethod
    def generate_response(cls, *, status_code=None, **kwargs):
        status_code_final = status_code if status_code is not None else cls._status_code
        return cls(status_code=status_code_final, **kwargs)

class ErrorResponse(Response):
    def __init__(self, *, error_code, error_message=None):
        super().__init__(status_code=error_code, error_message=error_message)
