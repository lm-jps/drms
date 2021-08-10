#!/usr/bin/env python3

from enum import Enum

__all__ = [ 'StatusCode' ]

class StatusCode(Enum):
    # do not define enum members here - you cannot subclass an Enum class if the parent has members; members are 2-tuples: <int_value>, <description>, as in
    # REQUEST_COMPLETE = (0, 'request has been completely processed')
    def __new__(cls, int_value, description):
        member = object.__new__(cls)
        member._int_value = int_value
        member._description = description

        if not hasattr(cls, '_all_members'):
            cls._all_members = {}
        cls._all_members[str(int_value)] = member

        return member

    def __int__(self):
        return self._int_value

    def description(self, **kwargs):
        return self._description.format(**kwargs) # 'request has been completely processed'

    @classmethod
    def _missing_(cls, int_value):
        return cls._all_members[str(int_value)]
