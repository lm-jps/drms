#!/usr/bin/env python3

from json import dumps
from collections import namedtuple

__all__ = [ 'Response' ]

class Response(object):
    def __init__(self, *, msg=None, **kwargs):
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
            self._dict_response = { "status" : int(self._status), "msg" : self._msg }
            self._dict_response.update(self._kwargs)
        return self._dict_response

class ErrorResponse(Response):
    def __init__(self, *, msg=None):
        super().__init__(msg=msg)
