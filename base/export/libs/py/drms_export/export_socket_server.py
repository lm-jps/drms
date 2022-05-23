import asyncio
from copy import deepcopy
from functools import partial
from json import loads as json_loads, dumps as json_dumps, decoder
from os.path import join as path_join
from shlex import quote
from signal import SIGINT
from socket import AF_INET, error as socket_error, getaddrinfo, SHUT_RDWR, SOCK_STREAM, socket, timeout as socket_timeout
from sys import exc_info as sys_exc_info, executable as sys_executable, exit as sys_exit

from drms_export import Error as ExportError, ErrorCode as ExportErrorCode
from drms_parameters import DRMSParams, DPMissingParameterError
from drms_utils import Arguments as Args, ArgumentsError as ArgsError, CmdlParser, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction, StatusCode as SC

DEFAULT_LOG_FILE = 'exp_log.txt'

__all__ = [ 'Connection', 'create_generator', 'get_arguments', 'get_message', 'send_message', 'ExpServerBaseError' ]

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
    BACKEND_INTERFACE = (111, 'bad arguments to backend')

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

class JsocFetchLegacyRequestError(ExpServerBaseError):
    _export_error = ErrorCode.BACKEND_INTERFACE

class Request:
    def __init__(self, db_host, db_port, db_user):
        self._db_host = db_host
        self._db_port = db_port
        self._db_user = db_user
        self._args_dict = {}
        self._positional_args = []

    @property
    def args_dict(self):
        return self._args_dict

    @property
    def positional_args(self):
        return self._positional_args

    @property
    def db_host(self):
        return self._db_host

    @property
    def db_port(self):
        return self._db_port

    @property
    def db_user(self):
        return self._db_user

class ParseSpecificationRequest(Request):
    def __init__(self, *, specification):
        super().__init__(None, None, None)
        self._args_dict['spec'] = specification

    # db_host, db_port, db_user are for the public db server, by default - this
    # is the server used for ParseSpecificationRequest since those arguments
    # are now allowed in the request (force use of the public db server)
    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = ParseSpecificationResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class SeriesListRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, series_regex):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['q'] = 1
        self._args_dict['z'] = 1
        self._positional_args.append(series_regex)

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = SeriesListResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class SeriesInfoRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, series):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['ds'] = series
        self._args_dict['op'] = 'series_struct'
        self._args_dict['s'] = 1

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = SeriesInfoResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class RecordInfoRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, specification, keywords=None, links=None, segments=None, record_info=False, number_records=None):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['ds'] = specification
        self._args_dict['key'] = None if keywords is None else ','.join(keywords)
        self._args_dict['link'] = None if links is None else ','.join(links)
        self._args_dict['seg'] = None if segments is None else ','.join(segments)
        self._args_dict['R'] = 1 if record_info else 0
        self._args_dict['n'] = number_records
        self._args_dict['op'] = 'rs_list'
        self._args_dict['s'] = 1
        self._args_dict['l'] = 1

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = RecordInfoResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class RecordInfoTableRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, specification, table_flags, keywords=None, links=None, segments=None, record_info=False, number_records=None):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['ds'] = specification
        self._args_dict['n'] = number_records
        self._args_dict['key'] = None if keywords is None else ','.join(keywords)
        self._args_dict['link'] = None if links is None else ','.join(links)
        self._args_dict['seg'] = None if segments is None else ','.join(segments)

        # table_flags
        self._args_dict['a'] = 1 if table_flags.get('all_keywords', False) else 0
        self._args_dict['A'] = 1 if table_flags.get('all_segments', False) else 0
        self._args_dict['b'] = 1 if table_flags.get('autobang', False) else 0
        self._args_dict['B'] = 1 if table_flags.get('float_as_hex', False) else 0
        self._args_dict['c'] = 1 if table_flags.get('record_count', False) else 0
        self._args_dict['C'] = 1 if table_flags.get('no_retrieve_links', False) else 0
        self._args_dict['d'] = 1 if table_flags.get('segment_info', False) else 0
        self._args_dict['e'] = 1 if table_flags.get('parse_specification', False) else 0
        self._args_dict['i'] = 1 if table_flags.get('specifications', False) else 0
        self._args_dict['I'] = 1 if table_flags.get('session_info', False) else 0
        self._args_dict['j'] = 1 if table_flags.get('jsd', False) else 0
        self._args_dict['k'] = 1 if table_flags.get('keyword_rows', False) else 0
        self._args_dict['K'] = 1 if table_flags.get('linked_records', False) else 0
        self._args_dict['l'] = 1 if table_flags.get('list_series_info', False) else 0
        self._args_dict['M'] = 1 if table_flags.get('float_max_precision', False) else 0
        self._args_dict['o'] = 1 if table_flags.get('storage_unit_statuses', False) else 0
        self._args_dict['O'] = 1 if table_flags.get('disable_timeout', False) else 0
        self._args_dict['p'] = 1 if table_flags.get('offline_segment_paths', False) else 0
        self._args_dict['P'] = 1 if table_flags.get('online_segment_paths', False) else 0
        self._args_dict['q'] = 1 if table_flags.get('no_header', False) else 0
        self._args_dict['r'] = 1 if table_flags.get('recnums', False) else 0
        self._args_dict['s'] = 1 if table_flags.get('statistics', False) else 0
        self._args_dict['R'] = 1 if table_flags.get('storage_unit_expiration_dates', False) else 0
        self._args_dict['S'] = 1 if table_flags.get('sunums', False) else 0
        self._args_dict['t'] = 1 if table_flags.get('data_types', False) else 0
        self._args_dict['T'] = 1 if table_flags.get('tape_info', False) else 0
        self._args_dict['v'] = 1 if table_flags.get('verbose', False) else 0
        self._args_dict['x'] = 1 if table_flags.get('storage_unit_archive_statuses', False) else 0
        self._args_dict['z'] = 1 if table_flags.get('storage_unit_sizes', False) else 0
        self._args_dict['sunum'] = ','.join(table_flags['sunum_list']) if table_flags.get('sunum_list', False) else -1

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = RecordInfoTableResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class PremiumExportRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, specification, method, requestor=None, processing=None, file_format=None, file_format_args=None, file_name_format=None, number_records=None):
        super().__init__(db_host, db_port, db_user)
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

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = PremiumExportResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class MiniExportRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, specification, requestor=None, file_name_format=None, number_records=None):
        super().__init__(db_host, db_port, db_user)
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

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = MiniExportResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class StreamedExportRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, specification, file_name_format=None):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['address'] = address
        self._args_dict['spec'] = specification
        self._args_dict['ffmt'] = file_name_format
        self._args_dict['a'] = 0
        self._args_dict['d'] = 1
        self._args_dict['e'] = 1
        self._args_dict['s'] = 1

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = StreamedExportResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class ExportStatusRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, request_id):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['requestid'] = request_id
        self._args_dict['W'] = 1
        self._args_dict['op'] = 'exp_status'
        self._args_dict['format'] = 'json'

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = ExportStatusResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class CheckAddressLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['address'] = address
        self._args_dict.update(kwargs);

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  CheckAddressLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class CheckAddressLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['address'] = address
        self._args_dict.update(kwargs);

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  CheckAddressLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class JsocFetchLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, address, operation, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['notify'] = address
        self._args_dict['op'] = operation
        self._args_dict['W'] = 1 # do not generate web page, return JSON instead

        legacy_arguments = deepcopy(kwargs)
        if operation.strip().lower() == 'exp_status':
            request_id = legacy_arguments.get('request_id')
            if request_id is None:
                raise JsocFetchLegacyRequestError(error_message=f'missing required argument `request_id`')

            self._args_dict['requestid'] = request_id
            del legacy_arguments['request_id']
        elif operation.strip().lower() == 'exp_request':
            specification = legacy_arguments.get('specification')
            if specification is None:
                raise JsocFetchLegacyRequestError(error_message=f'missing required argument `specification`')

            self._args_dict['ds'] = specification
            del legacy_arguments['specification']

        self._args_dict.update(legacy_arguments);

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  JsocFetchLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class JsocInfoLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, operation, specification, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['op'] = operation
        self._args_dict['ds'] = specification
        self._args_dict['s'] = 1
        self._args_dict.update(kwargs);

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  JsocInfoLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class ShowInfoLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, specification, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['ds'] = specification
        self._args_dict.update(kwargs);

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  ShowInfoLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class ShowSeriesLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, series_regex, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['q'] = 1 # do not print HTTP headers
        self._args_dict['z'] = 1 # print json
        self._args_dict.update(kwargs);
        self._positional_args.append(series_regex)

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  ShowSeriesLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class ShowExtSeriesLegacyRequest(Request):
    def __init__(self, *, db_host, db_port=None, db_user=None, series_regex, **kwargs):
        super().__init__(db_host, db_port, db_user)
        self._args_dict['filter'] = series_regex
        self._args_dict['noheader'] = 1
        self._args_dict['dbhost'] = db_host
        self._args_dict.update(kwargs);

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response =  ShowExtSeriesLegacyResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class QuitRequest(Request):
    def __init__(self):
        super().__init__(None, None, None)

    def generate_response(self, db_host, db_port, db_user, export_bin, export_scripts):
        response = QuitResponse(self, db_host, db_port, db_user, export_bin, export_scripts)
        return response

class Response():
    # db_host, db_port, db_user are defaults used if not provided in request
    def __init__(self, request, db_host, db_port, db_user, export_bin, export_scripts):
        self._request = request
        self._args_dict = { 'DRMS_DBUTF8CLIENTENCODING' : 1 }

        resolved_db_host = request.db_host if request.db_host is not None else db_host
        resolved_db_port = request.db_port if request.db_port is not None else db_port
        resolved_db_user = request.db_user if request.db_user is not None else db_user

        if resolved_db_host is not None:
            if resolved_db_port is not None:
                self._args_dict['JSOC_DBHOST'] = f'{resolved_db_host}:{str(resolved_db_port)}'
            else:
                self._args_dict['JSOC_DBHOST'] = f'{resolved_db_host}'

        if resolved_db_user is not None:
            self._args_dict['JSOC_DBUSER'] = f'{resolved_db_user}'

        self._export_bin = export_bin
        self._export_scripts = export_scripts

    def make_argslist(self):
        # default to bin command line
        args_list = None

        command_path = path_join(f'{self._export_bin}', f'{self.__class__._cmd}')
        Session.log.write_debug([ f'[ Response.make_argslist ] running DRMS module {command_path}' ])
        args_list = [ command_path ]

        for key, val in self._args_dict.items():
            if val is not None and len(str(val)) > 0:
                args_list.append(f'{key}={quote(str(val))}')

        for key, val in self._request.args_dict.items():
            if val is not None and len(str(val)) > 0:
                args_list.append(f'{key}={quote(str(val))}')

        for val in self._request.positional_args:
            args_list.append(f'{str(val)}')

        args_list.append('2>/dev/null')

        return args_list

    def escape_data(self, data):
        # noop, by default
        return data

    async def _send(self, writer, partial=False):
        args_list = self.make_argslist()
        command_path = args_list[0]

        Session.log.write_debug([ f'[ Response.send ] arguments: ' ])
        Session.log.write_debug(args_list)

        Session.log.write_debug([ f'[ Response.send ] running {" ".join(args_list)}' ])

        proc = await asyncio.subprocess.create_subprocess_shell(' '.join(args_list), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        first = True
        while True:
            response = await proc.stdout.read(8192) # bytes
            if not response or response == b'':
                break

            # write to the client socket
            if first:
                if not partial and response.lstrip()[:1] != b'{':
                    # when sending a response to the client, it must be formatted as a message (i.e., JSON object)
                    raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command; {command_path} response is not JSON (starts with {response[:8].decode("utf8")})')

            await send_text_async(writer, self.escape_data(response.decode('utf8')))
            first = False

        await proc.wait()

        if proc.returncode is not None:
            # child process terminated
            if proc.returncode == 0:
                pass
            else:
                # DRMS modules have inconsistent return values; jsoc_fetch can return 1 when returning certain kinds of
                # information, but 0 for other kinds; so ignore non-zero error codes, but log them; we cannot add a
                # `export_server_status` code to the message sent to the client because we already sent the message at this
                # point
                stdout, stderr = await proc.communicate()
                if stderr is not None and len(stderr) > 0:
                    Session.log.write_warning([ f'[ Response.send ] {command_path} returned status code {str(proc.returncode)}, plus jibber-jabber {stderr.decode()}' ])
                else:
                    Session.log.write_warning([ f'[ Response.send ] {command_path} returned status code {str(proc.returncode)}' ])

                raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command; {command_path} returned status code {str(proc.returncode)}' )
        else:
            raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command; no return code' )

    async def send(self, writer):
        # send a JSON resonse
        await self._send(writer)

    async def send_partial(self, writer):
        # send text (part of a JSON response)
        await self._send(writer, True)

class ParseSpecificationResponse(Response):
    _cmd = 'drms_parserecset'

class SeriesListResponse(Response):
    _cmd = 'show_series'

class SeriesInfoResponse(Response):
    _cmd = 'jsoc_info'

class RecordInfoResponse(Response):
    _cmd = 'jsoc_info'

class RecordInfoTableResponse(Response):
    _cmd = 'show_info'

    def escape_data(self, data):
        # noop, by default
        return data.encode('unicode_escape').decode('utf8')

    async def send(self, writer):
        suffix = None

        # start JSON (the caller needs to receive valid JSON, but show_info will dump a text string)
        await send_text_async(writer, '{\n  "table" : "')

        try:
            # dump table as a big string; parent will call show_info
            await super().send_partial(writer)

            # send success status - 0
            suffix = '",\n  "status" : 0\n}\n'
        except:
            # send faliure status - 1
            suffix = '",\n  "status" : 1\n}\n'

            # do not re-raise since this would cause the server to respond with a new JSON string, but
            # we have already started sending a JSON string
        finally:
            # send closing JSON curly brace
            await send_text_async(writer, suffix)

class PremiumExportResponse(Response):
    _cmd = 'jsoc_fetch'

class MiniExportResponse(Response):
    _cmd = 'jsoc_fetch'

class StreamedExportResponse(Response):
    _cmd = 'drms-export-to-stdout'

    async def send(self, writer):
        command_path = path_join(f'{self._export_bin}', f'{self.__class__._cmd}')
        Session.log.write_debug([ f'[ Response.send ] running DRMS module {command_path}' ])
        args_list = [ command_path ]

        for key, val in self._args_dict.items():
            if val is not None:
                args_list.append(f'{key}={quote(str(val))}')

        for key, val in self._request.args_dict.items():
            if val is not None:
                args_list.append(f'{key}={quote(str(val))}')

        for val in self._request.positional_args:
            args_list.append(f'{str(val)}')

        args_list.append('2>/dev/null')

        Session.log.write_debug([ f'[ Response.send ] arguments: ' ])
        Session.log.write_debug(args_list)

        proc = await asyncio.subprocess.create_subprocess_shell(' '.join(args_list), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        # tell client that binary payload will follow the message
        message = { 'status' : 0, 'instructions' : 'binary payload will follow this message' }
        await send_message_async(writer, json_dumps(message))

        while True:
            data = await proc.stdout.read(8192) # bytes
            if not data or data == b'':
                break

            # write binary data to the client socket
            await send_data_async(writer, data)

        await proc.wait()

        if proc.returncode is not None:
            # child process terminated
            if proc.returncode == 0:
                pass
            else:
                # DRMS modules have inconsistent return values; jsoc_fetch can return 1 when returning certain kinds of
                # information, but 0 for other kinds; so ignore non-zero error codes, but log them; we cannot add a
                # `export_server_status` code to the message sent to the client because we already sent the message at this
                # point
                stdout, stderr = await proc.communicate()
                if stderr is not None and len(stderr) > 0:
                    Session.log.write_warning([ f'[ Response.send ] {command_path} returned status code {str(proc.returncode)}, plus jibber-jabber {stderr.decode()}' ])
                else:
                    Session.log.write_warning([ f'[ Response.send ] {command_path} returned status code {str(proc.returncode)}' ])
        else:
            raise SubprocessError(error_message=f'[ Response.send ] failure running {self.__class__.__name__} command; no return code' )

class ExportStatusResponse(Response):
    _cmd = 'jsoc_fetch'

class CheckAddressLegacyResponse(Response):
    _cmd = 'checkAddress.py'

    def make_argslist(self):
        # default to bin command line
        args_list = None

        command_path = path_join(f'{self._export_scripts}', f'{self.__class__._cmd}')
        Session.log.write_debug([ f'[ CheckAddressLegacyResponse.make_argslist ] running DRMS script {command_path}' ])
        args_list = [ f'{sys_executable}', command_path ]

        query_string = []
        for key, val in self._args_dict.items():
            if val is not None and len(str(val)) > 0:
                query_string.append(f'{key}={quote(str(val))}')

        for key, val in self._request.args_dict.items():
            if val is not None and len(str(val)) > 0:
                query_string.append(f'{key}={quote(str(val))}')

        for val in self._request.positional_args:
            query_string.append(f'{str(val)}')

        args_list.append(f"'{'&'.join(query_string)}'")
        args_list.append('2>/dev/null')

        return args_list

class JsocFetchLegacyResponse(Response):
    _cmd = 'jsoc_fetch'

class JsocInfoLegacyResponse(Response):
    _cmd = 'jsoc_info'

class ShowInfoLegacyResponse(Response):
    _cmd = 'show_info'

    def escape_data(self, data):
        # noop, by default
        return data.encode('unicode_escape').decode('utf8')

    async def send(self, writer):
        suffix = None

        # start JSON (the caller needs to receive valid JSON, but show_info will dump a text string)
        await send_text_async(writer, '{\n  "text" : "')

        try:
            # dump table as a big string; parent will call show_info
            await super().send_partial(writer)

            # send success status - 0
            suffix = '",\n  "status" : 0\n}\n'
        except:
            # send faliure status - 1
            suffix = '",\n  "status" : 1\n}\n'

            # do not re-raise since this would cause the server to respond with a new JSON string, but
            # we have already started sending a JSON string
        finally:
            # send closing JSON curly brace
            await send_text_async(writer, suffix)

class ShowSeriesLegacyResponse(Response):
    _cmd = 'show_series'

class ShowExtSeriesLegacyResponse(Response):
    _cmd = 'showextseries.py'

    def make_argslist(self):
        # default to bin command line
        args_list = None

        command_path = path_join(f'{self._export_scripts}', f'{self.__class__._cmd}')
        Session.log.write_debug([ f'[ ShowExtSeriesLegacyResponse.make_argslist ] running DRMS script {command_path}' ])

        # `--debug` signals to showextseries.py that the arguments exist in a querystring
        args_list = [ f'{sys_executable}', command_path, '--debug' ]

        query_string = []
        for key, val in self._args_dict.items():
            if val is not None and len(str(val)) > 0:
                query_string.append(f'{key}={quote(str(val))}')

        for key, val in self._request.args_dict.items():
            if val is not None and len(str(val)) > 0:
                query_string.append(f'{key}={quote(str(val))}')

        args_list.append(f"'{'&'.join(query_string)}'")
        args_list.append('2>/dev/null')

        return args_list

class QuitResponse(Response):
    _cmd = None

    async def send(self, writer):
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
                    export_scripts = drms_params.get_required('SCRIPTS_EXPORT')
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
                arguments.export_scripts = export_scripts
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
            raise MessageSyntaxError(error_message=f'[ parse_message ] invalid message syntax; first character must be {str("{")}')

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

    def connect(self):
        return self.__enter__()

    def shutdown(self):
        if self._sock is not None:
            self._sock.shutdown(SHUT_RDWR)

    def close(self):
        if self._sock is not None:
            self._sock.close()

        self._sock = None

    @classmethod
    def get_log(cls):
        return Session.log

# client
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

            Session.log.write_debug([ f'[ get_message ] received message chunk from server:', buffer ])
            json_message_buffer, buffer, open_count, close_count = parse_message(json_message_buffer, buffer, open_count, close_count)
        except socket_timeout as exc:
            raise MessageTimeoutError(exc_info=sys_exc_info(), error_message=f'{str(exc)}')

        if close_count == open_count:
            break

    message = ''.join(json_message_buffer)
    Session.log.write_debug([ f'received message:', f'{message}' ])
    return message

# server
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

# client
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

# server
async def send_message_async(writer, json_message):
    Session.log.write_debug([ f'[ send_message_async ] sending message:', json_message ])
    writer.write(json_message.encode('utf8'))
    await writer.drain()
    Session.log.write_debug([ f'[ send_message_async ] message successfully sent' ])

async def send_text_async(writer, text):
    Session.log.write_debug([ f'[ send_text_async ] sending text:', text ])
    writer.write(text.encode('utf8'))
    await writer.drain()
    Session.log.write_debug([ f'[ send_text_async ] text successfully sent' ])

async def send_data_async(writer, binary_data):
    writer.write(binary_data)
    await writer.drain()

def generator_factory(*, destination):
    def generator():
        while True:
            # returns tuple; first element is boolean - True means more to read, False means done;
            # second element are data bytes
            more, bytes_read = destination['reader'](destination=destination)

            # `stream_reader` uses parse_message()-like logic so the payload must be wrapped in curly braces:
            #    payload = { b'' } --> utf8-encoded binary data
            #  when `stream_reader` sees the closing brace, it returns more == False
            if not more:
                break

            yield bytes_read

    return generator

def create_generator(*, destination):
    # first, download just the header from the output of drms-export-to-stdout
    if destination['has_header']:
        # call read proc to extract header; more should be True, bytes_read should be b''
        more, bytes_read = destination['reader'](destination=destination)

    # wrap generator around the remaining drms-export-to-stdout output
    generator = generator_factory(destination=destination)

    # return generator object
    return generator()

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
        if key.lower().strip() != 'request_type' and val is not None:
            request_dict[key.lower().strip()] = val

    Session.log.write_debug([ f'received {request_type} message' ])

    if request_type == 'parse_specification':
        request = ParseSpecificationRequest(**request_dict)
    elif request_type == 'series_list':
        request = SeriesListRequest(**request_dict)
    elif request_type == 'series_info':
        request = SeriesInfoRequest(**request_dict)
    elif request_type == 'record_info':
        request = RecordInfoRequest(**request_dict)
    elif request_type == 'record_info_table':
        request = RecordInfoTableRequest(**request_dict)
    elif request_type == 'premium_export':
        request = PremiumExportRequest(**request_dict)
    elif request_type == 'mini_export':
        request = MiniExportRequest(**request_dict)
    elif request_type == 'streamed_export':
        request = StreamedExportRequest(**request_dict)
    elif request_type == 'export_status':
        request = ExportStatusRequest(**request_dict)
    elif request_type == 'legacy_check_address':
        request = CheckAddressLegacyRequest(**request_dict)
    elif request_type == 'legacy_jsoc_fetch':
        request = JsocFetchLegacyRequest(**request_dict)
    elif request_type == 'legacy_jsoc_info':
        request = JsocInfoLegacyRequest(**request_dict)
    elif request_type == 'legacy_show_info':
        request = ShowInfoLegacyRequest(**request_dict)
    elif request_type == 'legacy_show_series':
        request = ShowSeriesLegacyRequest(**request_dict)
    elif request_type == 'legacy_show_ext_series':
        request = ShowExtSeriesLegacyRequest(**request_dict)
    elif request_type == 'quit':
        request = QuitRequest()
    else:
        raise InvalidMessageError(error_message=f'unexpected message type {request_type}')

    return request

# db_host, db_port, db_user are defaults if not provided in user request
async def send_response(writer, request, db_host, db_port, db_user, export_bin, export_scripts):
    response = request.generate_response(db_host, db_port, db_user, export_bin, export_scripts)
    await response.send(writer)

async def send_error_response(writer, error_message):
    message = { 'export_server_status' : 'export_server_error', 'error_message' : error_message }
    await send_message_async(writer, json_dumps(message))

async def handle_client(reader, writer, timeout, db_host, db_port, db_user, export_bin, export_scripts):
    request = None

    try:
        while True:
            try:
                request = await asyncio.wait_for(get_request(reader), timeout=timeout)
            except asyncio.TimeoutError:
                raise MessageTimeoutError(exc_info=sys_exc_info(), error_message=f'[ handle_client ] timeout event waiting for client {writer.get_extra_info("peername")!r} to send message')

            try:
                await asyncio.wait_for(send_response(writer, request, db_host, db_port, db_user, export_bin, export_scripts), timeout=timeout)
            except asyncio.TimeoutError:
                raise MessageTimeoutError(exc_info=sys_exc_info(), error_message=f'[ handle_client ] timeout event waiting sending message to client {writer.get_extra_info("peername")!r}')
            except Exception as exc:
                raise MessageSendError(exc_info=sys_exc_info(), error_message=f'[ handle_client ] {str(exc)}')

            if isinstance(request, QuitRequest) or isinstance(request, StreamedExportRequest):
                # StreamedExportRequest --> the server dumps the export content on the writer after sending
                # the message; since this is not a normal JSON message, the server has to send EOF to the
                # client and break out of the request loop
                writer.write_eof()
                break
    except ExpServerBaseError as exc:
        if Session.log:
            Session.log.write_error([ f'{exc.message}' ])

        # send error response
        try:
            await asyncio.wait_for(send_error_response(writer, exc.message), timeout=timeout)
        except:
            pass
    except Exception as exc:
        if Session.log:
            Session.log.write_error([ f'{str(exc)}' ])

        # send error response
        try:
            await asyncio.wait_for(send_error_response(writer, str(exc)), timeout=timeout)
        except:
            pass

    # wait for client to terminate connection
    buffer = await reader.read(4096)

    if buffer != b'':
        Session.log.write_error([ f'error waiting for client {writer.get_extra_info("peername")!r} to shut down connection' ])

    writer.close()
    await writer.wait_closed()

async def cancel_server():
    Session.log.write_info([ f'[ cancel_server] cancelling server task'])
    tasks = [ task for task in asyncio.all_tasks() if task is not asyncio.current_task() ]
    for task in tasks:
        task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)

async def run_server(*, host, port, timeout, db_host, db_port, db_user, export_bin, export_scripts):
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(SIGINT, partial(asyncio.ensure_future, cancel_server()))
    server = await asyncio.start_server(lambda r, w : handle_client(r, w, timeout, db_host, db_port, db_user, export_bin, export_scripts), host, port)
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

    asyncio.run(run_server(host=arguments.server, port=arguments.listen_port, timeout=arguments.message_timeout, db_host=arguments.db_host, db_port=arguments.db_port, db_user=arguments.db_user, export_bin=arguments.export_bin, export_scripts=arguments.export_scripts))
else:
    # client
    pass
