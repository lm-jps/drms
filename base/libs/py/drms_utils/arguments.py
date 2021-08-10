#!/usr/bin/env python3

__all__ = [ 'Arguments', 'ArgumentsError' ]

class ArgumentsError(Exception):
    def __init__(self, *, error_message=None):
        super().__init__(error_message)

class Arguments(object):
    def __init__(self, *, parser, args=None):
        self._parsed_args = None
        if parser is not None:
            # this could raise in a few places; let the caller handle these exceptions.
            self._parser = parser

            # parse the arguments - if args is None, then sys.argv is used
            self.parse(args=args)

            self._dict_of_args = None
        else:
            # args contains a dict of arguments
            if type(args) != dict:
                raise ArgumentsError(error_message=f'must provide either parser or arguments dict to Arguments constructor')

            self._dict_of_args = args

        # set all args
        self.set_all_args()

    def parse(self, *, args=None):
        try:
            self._parsed_args = self._parser.parse_args(args)
        except Exception as exc:
            if len(exc.args) == 2:
                type, error_message = exc

                if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                    raise # Re-raise

                raise ArgumentsError(error_message=error_message)
            else:
                raise # Re-raise

    def __getattr__(self, name):
        # only called if object.__getattribute__(self, name) raises; and if that is true, then we want
        # to look in self._parsed_args for it, and set the instance attribute if it does exist in self.params
        value = None

        if self._parsed_args is not None:
            if name in vars(self._parsed_args):
                value = vars(self._parsed_args)[name]
                object.__setattr__(self, name, value)
                return value
        elif self._dict_of_args is not None:
            if name in self._dict_of_args:
                value = self._dict_of_args[name]
                object.__setattr__(self, name, value)
                return value

        raise ArgumentsError(error_message=f'invalid argument `{name}`')

    def set_all_args(self):
        # store in instance dict
        if self._parsed_args is not None:
            for name, value in vars(self._parsed_args).items():
                setattr(self, name, value)
        elif self._dict_of_args is not None:
            for name, value in self._dict_of_args.items():
                setattr(self, name, value)

    def set_arg(self, name, value):
        setattr(self, name, value)
