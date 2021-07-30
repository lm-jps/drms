#!/usr/bin/env python3

from json import dumps
from collections import namedtuple

__all__ = [ 'Response' ]

class Response(object):
    def __init__(self, *, status_code, msg=None, **kwargs):
        self._status_code = status_code
        self._msg = msg
        self._kwargs = kwargs
        self._json_obj_response = None
        self._json_response = None
        self._dict_response = None

    def __str__(self):
        return self._msg

    def generate_json_obj(self):
        if self._json_obj_response is None:
            self._json_obj_response = json.loads(self.generate_json(), object_hook = lambda d : namedtuple('JO', d.keys()) (*d.values()))
        return self._json_obj_response

    def generate_json(self):
        if self._json_response is None:
            self._json_response = dumps(self.generate_dict())
        return self._json_response

    def generate_dict(self):
        if self._dict_response is None:
            self._dict_response = { "status" : int(self._status_code), "msg" : self._msg }
            self._dict_response.update(self._kwargs)
        return self._dict_response

    @classmethod
    def generate_response(cls, *, status_code=None, msg=None, **kwargs):
        status_code_final = status_code if status_code is not None else cls._status_code

        if msg is None:
            msg = status_code_final.fullname(**kwargs)

        return cls(status_code=status_code_final, msg=msg, **kwargs)

class ErrorResponse(Response):
    def __init__(self, *, error_code, msg=None):
        super().__init__(status_code=error_code, msg=msg)
