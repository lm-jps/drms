#!/usr/bin/env python3

import psutil
import socket

from drms_utils import CmdlParser, Arguments as Args, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction

DEFAULT_LOG_FILE = 'dx_client_log.txt'

class ErrorCode(ExportErrorCode):
    PARAMETERS = 1, 'failure locating DRMS parameters'
    ARGUMENTS = 2, 'bad arguments'
    MESSAGE_SYNTAX = 3, 'message syntax'
    MESSAGE_TIMEOUT = 4, 'message timeout event'
    SOCKET_SERVER = 5, 'TCP socket server creation error'
    DATA_CONNECTION = 6, 'data connection'
    LOGGING = 7, 'logging'

class ParametersError(ExportError):
    _error_code = ErrorCode(ErrorCode.PARAMETERS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class ArgumentsError(ExportError):
    _error_code = ErrorCode(ErrorCode.ARGUMENTS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class MessageSyntaxError(ExportError):
    _error_code = ErrorCode(ErrorCode.MESSAGE_SYNTAX)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class MessageTimeoutError(ExportError):
    _error_code = ErrorCode(ErrorCode.MESSAGE_TIMEOUT)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class SocketserverError(ExportError):
    _error_code = ErrorCode(ErrorCode.SOCKET_SERVER)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class DataConnectionError(ExportError):
    _error_code = ErrorCode(ErrorCode.DATA_CONNECTION)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)


class Arguments(Args):
    _arguments = None

    @classmethod
    def get_arguments(cls, *, program_args, drms_params):
        if cls._arguments is None:
            try:
                server = drms_params.get_required('DX_SERVER')
                server_port = int(drms_params.get_required('DX_LISTEN_PORT'))
                log_file = os.path.join('/tmp'), DEFAULT_LOG_FILE)
            except DPMissingParameterError as exc:
                raise ParametersError(msg=str(exc))

            args = None

            if program_args is not None and len(program_args) > 0:
                args = program_args

            parser = CmdlParser(usage='%(prog)s ')

            # required

            # optional
            parser.add_argument('-n', '--number_ids', help='the number of DRMS_IDs to process at one time', metavar='<number of drms_ids>', dest='number_of_ids', default=1024)
            parser.add_argument('-l', '--log_file', help='the path to the log file', metavar='<log file>', dest='log_file', default=log_file)
            parser.add_argument('-L', '--logging_level', help='the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', metavar='<logging level>', dest='logging_level', action=DrmsLogLevelAction, default=DrmsLogLevel.ERROR)

            arguments = Arguments(parser=parser, args=args)

            # add needed drms parameters
            arguments.server = server
            argumens.server_port = server_port

            cls._arguments = arguments

        return cls._arguments

def get_arguments(**kwargs):
    args = []
    for key, val in kwargs.items():
        args.append(f'{key} = {val}')

    drms_params = DRMSParams()

    if drms_params is None:
        raise ParametersError(msg=f'unable to locate DRMS parameters file')

    return Arguments.get_arguments(program_args=args, drms_params=drms_params)

if __name__ == "__main__":
    try:
        arguments = get_arguments()

        info = socket.getaddrinfo(arguments.server, arguments.server_port)
        connected = False
        for address_info in info:
            family = address_info[0]
            sock_type = address_info[1]

            if family == socket.AF_INET and sock_type == socket.SOCK_STREAM:
                socket_address = address_info[4] # 2-tuple for AF_INET family

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_fd:
                    try:
                        socket_fd.connect(socket_address)
                        connected = True
                    except OSError as exc:
                        socket_fd.close()
                        socket_fd = None
                        continue

                    # connected to server; perform workflow


                    # send CLIENT_READY


                    #
                    sock.sendall(bytes(message, 'ascii'))
                    response = str(sock.recv(1024), 'ascii')
                    print("Received: {}".format(response))

            if connected:
                break
    except OSError as exc:
        print(str(exc))
        raise SocketserverError(f'failure creating TCP client')

    sys.exit(0)
else:
    # stuff run when this module is loaded into another module; export things needed to call check() and cancel()
    # return json and let the wrapper layer convert that to whatever is needed by the API
    pass
