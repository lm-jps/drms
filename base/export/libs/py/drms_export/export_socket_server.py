import asyncio
from functools import partial
from json import loads as json_loads, dumps as json_dumps, decoder
from os.path import join as path_join
from shlex import quote
from signal import SIGINT
from socket import AF_INET, error as socket_error, getaddrinfo, SHUT_RDWR, SOCK_STREAM, socket, timeout as socket_timeout
from sys import exc_info as sys_exc_info, exit as sys_exit

from drms_export import Error as ExportError, ErrorCode as ExportErrorCode
from drms_parameters import DRMSParams, DPMissingParameterError
from drms_utils import Arguments as Args, ArgumentsError as ArgsError, CmdlParser, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction, StatusCode as SC

DEFAULT_LOG_FILE = 'exp_log.txt'

__all__ = [ 'Connection', 'get_arguments', 'get_message', 'send_message', 'ExpServerBaseError' ]

class StatusCode(SC):
    # info success status codes
    SUCCESS = (0, 'success')

class ErrorCode(ExportErrorCode):
    # info failure error codes
    FAILURE = (1, 'info failure')

    PARAMETERS = (101, 'failure locating DRMS parameters')
    ARGUMENTS = (102, 'bad arguments')
    LOGGING = (103, 'failure logging messages')
    MESSAGE_TIMEOUT = (104, 'timeout waiting for client to send message')
    MESSAGE_SYNTAX = (105, 'bad message format')
    INVALID_MESSAGE = (106, 'invalid message')
    MESSAGE_SEND = (107, 'error sending message')
    MESSAGE_RECEIVE = (108, 'error receiving message')
    SUBPROCESS = (109, 'error calling DRMS module')
    TCP_CLIENT = (110, 'TCP socket error')

class ExpServerBaseError(ExportError):
    def __init__(self, *, exc_info=None, error_message=None):
        if exc_info is not None:
            import traceback
            self.exc_info = exc_info
            e_type, e_obj, e_tb = exc_info
            file_info = traceback.extract_tb(e_tb)[0]
            file_name = file_info.filename if hasattr(file_info, 'filename') else ''
            line_number = str(file_info.lineno) if hasattr(file_info, 'lineno') else ''

            if error_message is None:
                error_message = f'{file_name}:{line_number}: {e_type.__name__}: {str(e_obj)}'
            else:
                error_message = f'{error_message} [ {file_name}:{line_number}: {e_type.__name__}: {str(e_obj)} ]'

        super().__init__(error_message=error_message)

class ParametersError(ExpServerBaseError):
    _error_code = ErrorCode.PARAMETERS

class ArgumentsError(ExpServerBaseError):
    _error_code = ErrorCode.ARGUMENTS

class LoggingError(ExpServerBaseError):
    _error_code = ErrorCode.LOGGING

class MessageTimeoutError(ExpServerBaseError):
    _export_error = ErrorCode.MESSAGE_TIMEOUT

class MessageSyntaxError(ExpServerBaseError):
    _export_error = ErrorCode.MESSAGE_SYNTAX

class InvalidMessageError(ExpServerBaseError):
    _export_error = ErrorCode.INVALID_MESSAGE

class MessageSendError(ExpServerBaseError):
    _export_error = ErrorCode.MESSAGE_SEND

class MessageReceiveError(ExpServerBaseError):
    _export_error = ErrorCode.MESSAGE_RECEIVE

class SubprocessError(ExpServerBaseError):
    _export_error = ErrorCode.SUBPROCESS

class TCPClientError(ExpServerBaseError):
    _export_error = ErrorCode.TCP_CLIENT

class Request:
    def __init__(self):
        self._args_dict = {}

    @property
    def args_dict(self):
        return self._args_dict

class ParseSpecificationRequest(Request):
    def __init__(self, *, specification):
        super().__init__()
        self._args_dict['spec'] = specification

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = ParseSpecificationResponse(self, db_host, db_port, db_user, export_bin)
        return response

class SeriesInfoRequest(Request):
    def __init__(self, *, series):
        super().__init__()
        self._args_dict['ds'] = series
        self._args_dict['op'] = 'series_struct'
        self._args_dict['s'] = 1

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = SeriesInfoResponse(self, db_host, db_port, db_user, export_bin)
        return response

class RecordInfoRequest(Request):
    def __init__(self, specification, keywords=None, links=None, segments=None, record_info=False, number_records=None):
        super().__init__()
        self._args_dict['ds'] = specification
        self._args_dict['key'] = None if keywords is None else ','.join(keywords)
        self._args_dict['link'] = None if links is None else ','.join(links)
        self._args_dict['seg'] = None if segments is None else ','.join(segments)
        self._args_dict['R'] = 1 if record_info else 0
        self._args_dict['n'] = number_records
        self._args_dict['op'] = 'rs_list'
        self._args_dict['s'] = 1

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = RecordInfoResponse(self, db_host, db_port, db_user, export_bin)
        return response

class PremiumExportRequest(Request):
    def __init__(self, address, specification, method, requestor=None, processing=None, file_format=None, file_format_args=None, file_name_format=None, number_records=None):
        super().__init__()
        self._args_dict['notify'] = address
        self._args_dict['ds'] = specification
        self._args_dict['method'] = method
        self._args_dict['requestor'] = requestor

        protocol = None if file_format is None else [ file_format ]
        if protocol is not None:
            if file_format_args is not None:
                for key, val in file_format_args.items():
                    protocol.append(f'{key}={str(val)}')

            self._args_dict['protocol'] = ','.join(protocol)
        else:
            self._args_dict['protocol'] = None

        self._args_dict['processing'] = json_dumps(processing) if processing is not None else json_dumps({})
        self._args_dict['filenamefmt'] = file_name_format
        self._args_dict['max_recs'] = number_records
        self._args_dict['W'] = 1
        self._args_dict['op'] = 'exp_request'
        self._args_dict['format'] = 'json'

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = PremiumExportResponse(self, db_host, db_port, db_user, export_bin)
        return response

class MiniExportRequest(Request):
    def __init__(self, address, specification, requestor=None, file_name_format=None, number_records=None):
        super().__init__()
        self._args_dict['notify'] = address
        self._args_dict['ds'] = specification
        self._args_dict['method'] = 'url_quick'
        self._args_dict['requestor'] = requestor
        self._args_dict['protocol'] = 'as-is'
        self._args_dict['processing'] = json_dumps({})
        self._args_dict['filenamefmt'] = file_name_format
        self._args_dict['max_recs'] = number_records
        self._args_dict['W'] = 1
        self._args_dict['op'] = 'exp_request'
        self._args_dict['format'] = 'json'

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = MiniExportResponse(self, db_host, db_port, db_user, export_bin)
        return response

class StreamedExportRequest(Request):
    def __init__(self, address, specification, file_name_format=None):
        super().__init__()
        self._args_dict['address'] = address
        self._args_dict['spec'] = specification
        self._args_dict['ffmt'] = file_name_format
        self._args_dict['a'] = 0
        self._args_dict['d'] = 1
        self._args_dict['e'] = 1
        self._args_dict['s'] = 1

class ExportStatusRequest(Request):
    def __init__(self, address, request_id):
        super().__init__()
        self._args_dict['requestid'] = request_id
        self._args_dict['W'] = 1
        self._args_dict['op'] = 'exp_status'
        self._args_dict['format'] = 'json'

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = ExportStatusResponse(self, db_host, db_port, db_user, export_bin)
        return response

class QuitRequest(Request):
    def __init__(self):
        super().__init__()

    def generate_response(self, db_host, db_port, db_user, export_bin):
        response = QuitResponse(self, db_host, db_port, db_user, export_bin)
        return response

class Response():
    def __init__(self, request, db_host, db_port, db_user, export_bin):
        self._request = request
        self._args_dict = { 'DRMS_DBUTF8CLIENTENCODING' : 1, 'JSOC_DBHOST' : f'{db_host}:{str(db_port)}', 'JSOC_DBUSER' : db_user }
        self._export_bin = export_bin

    async def send(self, writer, timeout):
        command_path = path_join(f'{self._export_bin}', f'{self.__class__._cmd}')
        Session.log.write_debug([ f'[ Response.send ] running DRMS module {command_path}' ])
        args_list = [ command_path ]

        for key, val in self._args_dict.items():
            if val is not None:
                args_list.append(f'{key}={quote(str(val))}')

        for key, val in self._request.args_dict.items():
            if val is not None:
                args_list.append(f'{key}={quote(str(val))}')

        args_list.append('2>/dev/null')

        Session.log.write_debug([ f'[ Response.send ] arguments: ' ])
        Session.log.write_debug(args_list)

        proc = await asyncio.subprocess.create_subprocess_shell(' '.join(args_list), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        while True:
            response = await proc.stdout.read(8192) # bytes
            if not response or response == b'':
                break

            await send_message_async(writer, response.decode('utf8'))

        await proc.wait()

        if proc.returncode is not None:
            # child process terminated
            if proc.returncode == 0:
                pass
            else:
                stdout, stderr = proc.communicate()
                if stderr is not None and len(stderr) > 0:
                    raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command: {stderr.decode()}' )
                else:
                    raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command' )
        else:
            raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command; no return code' )

class ParseSpecificationResponse(Response):
    _cmd = 'drms_parserecset'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class SeriesInfoResponse(Response):
    _cmd = 'jsoc_info'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class RecordInfoResponse(Response):
    _cmd = 'jsoc_info'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class PremiumExportResponse(Response):
    _cmd = 'jsoc_fetch'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class MiniExportResponse(Response):
    _cmd = 'jsoc_fetch'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class StreamedExportResponse(Response):
    _cmd = 'drms-export-to-stdout'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class ExportStatusResponse(Response):
    _cmd = 'jsoc_fetch'

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

class QuitResponse(Response):
    _cmd = None

    def __init__(self, request, db_host, db_port, db_user, export_bin):
        super().__init__(request, db_host, db_port, db_user, export_bin)

    async def send(self, writer, timeout):
        message = { 'instructions' : 'quit request received; please shut down socket connection' }
        await send_message_async(writer, json_dumps(message))

class Arguments(Args):
    _arguments = None

    @classmethod
    def get_arguments(cls, *, is_program, program_name=None, program_args=None, module_args=None, drms_params, refresh=True):
        if cls._arguments is None or refresh:
            try:
                listen_port = int(drms_params.get_required('EXP_LISTEN_PORT'))
                server = drms_params.get_required('EXP_SERVER')
                message_timeout = int(drms_params.get_required('EXP_MESSAGE_TIMEOUT'))
            except DPMissingParameterError as exc:
                raise ParametersError(exc_info=sys_exc_info(), error_message=str(exc))

            if is_program:
                try:
                    db_port =int(drms_params.get_required('DRMSPGPORT'))
                    export_bin = drms_params.get_required('BIN_EXPORT')
                    export_production_db_user = drms_params.get_required('EXPORT_PRODUCTION_DB_USER')
                    log_file = path_join(drms_params.get_required('EXPORT_LOG_DIR'), DEFAULT_LOG_FILE)
                except DPMissingParameterError as exc:
                    raise ParametersError(exc_info=sys_exc_info(), error_message=str(exc))

                args = None

                parser_args = { 'usage' : f'%(prog)s dbhost=<db host> [ -l/--log-file=<log file path> [ -L/--logging-level=<critical/error/warning/info/debug> ] [ -p/--port=<listen port> ] [ -P/--dbport=<db port> ] [ -S/--server=<socket-server host>] [ -U/--dbuser=<db user>] [ --webserver=<host> ]' }

                if program_name is not None and len(program_name) > 0:
                    parser_args['prog'] = program_name

                parser = CmdlParser(**parser_args)

                # required
                parser.add_argument('dbhost', '--dbhost', help='the machine hosting the database that serves DRMS data products', metavar='<db host>', dest='db_host', required=True)

                # optional
                parser.add_argument('-l', '--log-file', help='the path to the log file', metavar='<log file>', dest='log_file', default=log_file)
                parser.add_argument('-L', '--logging-level', help='the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', metavar='<logging level>', dest='logging_level', action=DrmsLogLevelAction, default=DrmsLogLevel.INFO)
                parser.add_argument('-p', '--port', help='the port to listen on for new client connections', metavar='<listening port>', dest='listen_port', default=listen_port)
                parser.add_argument('-P', '--dbport', help='the port on the host machine that is accepting connections for the database', metavar='<db host port>', dest='db_port', type=int, default=db_port)
                parser.add_argument('-S', '--server', help='the server host accepting client connections', metavar='<server>', dest='server', default=server)
                parser.add_argument('-U', '--dbuser', help='the name of the database user account', metavar='<db user>', dest='db_user', default=export_production_db_user)

                arguments = Arguments(parser=parser, args=args)

                # add needed drms parameters
                arguments.export_bin = path_join(export_bin, 'linux_avx')
                arguments.export_production_db_user = export_production_db_user
            else:
                # module invocation
                def extract_module_args(*, listen_port=listen_port, server=server):
                    arguments = {}

                    arguments['listen_port'] = listen_port
                    arguments['server'] = server

                    return arguments

                module_args_dict = extract_module_args(**module_args)
                arguments = Arguments(parser=None, args=module_args_dict)

            # add needed drms parameters
            arguments.message_timeout = message_timeout

            cls._arguments = arguments

        return cls._arguments

def parse_message(json_message_buffer, buffer, open_count, close_count):
    if len(json_message_buffer) == 0:
        # look for opening curly brace
        buffer = buffer.lstrip()
        if buffer[0] == '{':
            open_count += 1
        else:
            raise MessageSyntaxError(error_message=f'[ Message.receive ] invalid message synax; first character must be {str("{")}')

        json_message_buffer.append(buffer[0])
        buffer = buffer[1:]

    # count opening braces
    open_count += len(buffer.split('{')) - 1

    # count closing braces
    close_count += len(buffer.split('}')) - 1

    json_message_buffer.append(buffer)

    return json_message_buffer, buffer, open_count, close_count

class Session:
    log = None

class Connection:
    '''
    context manager to establish connection to server
    '''
    def __init__(self, *, server, listen_port, timeout=None, log=None):
        if log is None:
            Session.log = DrmsLog(None, None, None)
        else:
            Session.log = log

        Session.log.write_info([ f'[ Connection._init__ ] initialized logging' ])

        self._info = getaddrinfo(server, listen_port)
        self._timeout = timeout

    def __enter__(self):
        self._sock = None

        for address_info in self._info:
            family = address_info[0]
            sock_type = address_info[1]

            if family == AF_INET and sock_type == SOCK_STREAM:
                socket_address = address_info[4] # 2-tuple for AF_INET family

                try:
                    self._sock = socket(AF_INET, SOCK_STREAM)
                    Session.log.write_info([ f'created socket' ])
                    self._sock.connect(socket_address)
                    Session.log.write_info([ f'connected to server ({self._sock.getpeername()})' ])
                    if self._timeout is not None:
                        self._sock.settimeout(self._timeout)
                except socket_error as exc:
                    raise TCPClientError(exc_info=sys_exc_info(), error_message=str(exc))
                except OSError as exc:
                    raise TCPClientError(exc_info=sys_exc_info(), error_message=f'failure creating TCP client')

            if self._sock is not None:
                break

        return self._sock

    def __exit__(self, etype, value, traceback):
        if self._sock is not None:
            self._sock.shutdown(SHUT_RDWR)
            self._sock.close()
            self._sock = None

        # propagate any exception
        return False

    @classmethod
    def get_log(cls):
        return Session.log

def get_message(connection):
    json_message_buffer = []
    open_count = 0
    close_count = 0

    while True:
        try:
            # this is a non-blocking socket; timeout is 1 second
            buffer = connection.recv(4096).decode('utf8') # blocking

            if buffer == '':
                if close_count != open_count:
                    raise MessageSyntaxError(error_message=f'[ get_message ] message must be valid JSON')
                else:
                    raise MessageReceiveError(error_message=f'[ get_message ] while waiting for message, peer terminated connection')
                    break

            json_message_buffer, buffer, open_count, close_count = parse_message(json_message_buffer, buffer, open_count, close_count)
        except socket_timeout as exc:
            raise MessageTimeoutError(exc_info=sys_exc_info(), error_message=f'{str(exc)}')

        if close_count == open_count:
            break

    message = ''.join(json_message_buffer)
    Session.log.write_debug([ f'received message:', f'{message}' ])
    return message

async def get_message_async(reader):
    json_message_buffer = []
    open_count = 0
    close_count = 0

    while True:
        buffer = (await reader.read(4096)).decode('utf8')

        if buffer == '':
            if close_count != open_count:
                raise MessageSyntaxError(error_message=f'[ get_message_async ] message must be valid JSON')
            else:
                raise MessageReceiveError(error_message=f'[ get_message_async ] while waiting for message, peer terminated connection')
                break

        json_message_buffer, buffer, open_count, close_count = parse_message(json_message_buffer, buffer, open_count, close_count)

        if close_count == open_count:
            break

    message = ''.join(json_message_buffer)
    Session.log.write_debug([ f'received message:', f'{message}' ])
    return message

def send_message(connection, json_message):
    num_bytes_sent_total = 0
    num_bytes_sent = 0

    Session.log.write_debug([ f'sending message:', f'{json_message}' ])
    while num_bytes_sent_total < len(json_message.encode('utf8')):
        try:
            num_bytes_sent = connection.send(json_message[num_bytes_sent_total:].encode('utf8'))
            if not num_bytes_sent:
                raise TCPClientError(error_messsage=f'socket broken; cannot send message data to server')
            num_bytes_sent_total += num_bytes_sent
        except socket_timeout as exc:
            raise MessageTimeoutError(exc_info=sys_exc_info(), error_message=f'{str(exc)}')
    Session.log.write_debug([ f'message successfully sent' ])

async def send_message_async(writer, json_message):
    Session.log.write_debug([ f'sending message:', json_message ])
    writer.write(json_message.encode('utf8'))
    await writer.drain()
    Session.log.write_debug([ f'message successfully sent' ])

async def get_request(reader):
    json_message = await get_message_async(reader)

    # determine type of request
    try:
        message_dict = json_loads(json_message)
    except decoder.JSONDecodeError as exc:
        raise MessageSyntaxError(exc_info=sys_exc_info(), error_message=str(exc))

    request_type = message_dict['request_type'].lower()
    request_dict = {}
    for key, val in message_dict.items():
        if key.lower().strip() != 'request_type':
            request_dict[key.lower().strip()] = val

    Session.log.write_debug([ f'received {request_type} message' ])

    if request_type == 'parse_specification':
        request = ParseSpecificationRequest(**request_dict)
    elif request_type == 'series_info':
        request = SeriesInfoRequest(**request_dict)
    elif request_type == 'record_info':
        request = RecordInfoRequest(**request_dict)
    elif request_type == 'premium_export':
        request = PremiumExportRequest(**request_dict)
    elif request_type == 'mini_export':
        request = MiniExportRequest(**request_dict)
    elif request_type == 'streamed_export':
        request = StreamedExportRequest(**request_dict)
    elif request_type == 'export_status':
        request = ExportStatusRequest(**request_dict)
    elif request_type == 'quit':
        request = QuitRequest()
    else:
        raise InvalidMessageError(error_message=f'unexpected message type {request_type}')

    return request

async def send_response(writer, request, timeout, db_host, db_port, db_user, export_bin):
    response = request.generate_response(db_host, db_port, db_user, export_bin)
    await response.send(writer, timeout)

async def handle_client(reader, writer, timeout, db_host, db_port, db_user, export_bin):
    request = None

    try:
        while True:
            try:
                request = await asyncio.wait_for(get_request(reader), timeout=timeout)
            except asyncio.TimeoutError:
                raise MessageTimeoutError(exc_info=sys_exc_info(), error_message=f'[ handle_client ] timeout event waiting for client {writer.get_extra_info("peername")!r} to send message')

            try:
                await send_response(writer, request, timeout, db_host, db_port, db_user, export_bin)
            except Exception as exc:
                raise MessageSendError(exc_info=sys_exc_info(), error_message=f'[ handle_client] {str(exc)}')

            if isinstance(request, QuitRequest):
                break
    except ExpServerBaseError as exc:
        if Session.log:
            Session.log.write_error([ f'{str(exc)}' ])
    except Exception as exc:
        if Session.log:
            Session.log.write_error([ f'{str(exc)}' ])

    # wait for client to terminate connection
    buffer = await reader.read(4096)

    if buffer != b'':
        Session.log.write_error([ f'error waiting for client {writer.get_extra_info("peername")!r} to shut down connection' ])

    writer.close()

async def cancel_server():
    Session.log.write_info([ f'[ cancel_server] cancelling server task'])
    tasks = [ task for task in asyncio.all_tasks() if task is not asyncio.current_task() ]
    for task in tasks:
        task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)

async def run_server(*, host, port, timeout, db_host, db_port, db_user, export_bin):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(SIGINT, partial(asyncio.ensure_future, cancel_server()))
    server = await asyncio.start_server(lambda r, w : handle_client(r, w, timeout, db_host, db_port, db_user, export_bin), host, port)
    async with server:
        try:
            Session.log.write_info([ f'server running, server is {host}, port is {str(port)}' ])
            await server.serve_forever()
        except asyncio.exceptions.CancelledError:
            Session.log.write_info([ f'server task cancelled'])

def get_arguments(*, is_program, program_name=None, module_args=None):
    try:
        drms_params = DRMSParams()

        if drms_params is None:
            raise ParametersError(error_message='unable to locate DRMS parameters package')

        arguments = Arguments.get_arguments(is_program=is_program, program_name=program_name, module_args=module_args, drms_params=drms_params)
    except ArgsError as exc:
        raise ArgumentsError(exc_info=sys_exc_info(), error_message=f'{str(exc)}')
    except Exception as exc:
        raise ArgumentsError(exc_info=sys_exc_info(), error_message=f'{str(exc)}')

    return arguments

def initialize_logging(log_file, logging_level):
    log = None

    if log_file is not None:
        try:
            formatter = DrmsLogFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            log = DrmsLog(log_file, logging_level, formatter)
        except Exception as exc:
            raise LoggingError(exc_info=sys_exc_info(), error_message=f'{str(exc)}')
    else:
        # noop log (does not write anything)
        log = DrmsLog(None, None, None)

    return log

if __name__ == "__main__":
    # server
    arguments = get_arguments(is_program=True, module_args=None)

    # if arguments.log_file is None, then Session.log is a noop
    Session.log = initialize_logging(arguments.log_file, arguments.logging_level)
    Session.log.write_info([ f'initialized logging; log file is {arguments.log_file}' ])

    asyncio.run(run_server(host=arguments.server, port=arguments.listen_port, timeout=arguments.message_timeout, db_host=arguments.db_host, db_port=arguments.db_port, db_user=arguments.db_user, export_bin=arguments.export_bin))
else:
    # client
    pass
