from collections import namedtuple
from json import decoder, dumps, loads
from re import sub
from .error import Error as DrmsError, ErrorCode as DrmsErrorCode

__all__ = [ 'MakeObject' ]

class ErrorCode(DrmsErrorCode):
    ARGUMENTS = 1, 'bad arguments'

class ArgumentsError(DrmsError):
    _error_code = ErrorCode(ErrorCode.ARGUMENTS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class MakeObject():
    _valid_types = set([ dict, str ])

    def __init__(self, *, name, data):
        if type(data) not in self._valid_types:
            raise ArgumentsError(f'data structures supported: {str(self._valid_types)}')

        if type(data) == dict:
            self._json = dumps(data)
        elif type(data) == str:
            try:
                loads(data)
                self._json = data
            except decoder.JSONDecodeError as exc:
                raise ArgumentsError(f'invalid json {str(exc)}')

        self._name = name # name of namedtuple
        self._data = data

    def __call__(self):
        # d.values() become positional arguments to the method returned by namedtuple
        return loads(self._json, object_hook = lambda d : namedtuple(self._name, self._make_identifier(d.keys())) (*d.values()))

    # certain characters that are valid JSON attribute characters are not valid python identifier characters;
    # drop such characters; also, identifiers may not start with a digit so drop leading digits
    def _make_identifier(self, json_keys):
        rv = []

        for key in json_keys:
            s = sub('[^0-9a-zA-Z_]', '', key)
            s = sub('^[^a-zA-Z_]+', '', s)
            rv.append(s)

        return rv
