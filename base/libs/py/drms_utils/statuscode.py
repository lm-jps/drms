#!/usr/bin/env python3

from enum import Enum

__all__ = [ 'StatusCode' ]

class StatusCode(Enum):
    # do not define enum members here - you cannot subclass an Enum class if the parent has members
    def __new__(cls, value, name):
        member = object.__new__(cls)
        member._value = value
        member._fullname = name
        return member

    def __int__(self):
        return self._value

    def fullname(self, **kwargs):
        return self._fullname.format(**kwargs)
