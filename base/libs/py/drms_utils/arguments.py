#!/usr/bin/env python3

__all__ = [ 'Arguments' ]


class Arguments(object):
    def __init__(self, *, parser, args=None):
        # this could raise in a few places; let the caller handle these exceptions.
        self.parser = parser

        # parse the arguments - if args is None, then sys.argv is used
        self.parse(args=args)

        # set all args
        self.set_all_args()

    def parse(self, *, args=None):
        try:
            self.parsed_args = self.parser.parse_args(args)
        except Exception as exc:
            if len(exc.args) == 2:
                type, msg = exc

                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise ArgumentError(msg=msg)
            else:
                raise # Re-raise

    def __getattr__(self, name):
        # only called if object.__getattribute__(self, name) raises; and if that is true, then we want
        # to look in self.parsed_args for it, and set the instance attribute if it does exist in self.params
        value = None
        if name in vars(self.parsed_args):
            value = vars(self.parsed_args)[name]
            object.__setattr__(self, name, value)
            return value

        raise AttributeError('invalid argument ' + name)

    def set_all_args(self):
        # store in instance dict
        for name, value in vars(self.parsed_args).items():
            setattr(self, name, value)

    def set_arg(self, name, value):
        setattr(self, name, value)
