#!/usr/bin/env python3

import asyncio
from collections import OrderedDict
from datetime import datetime, timedelta
from enum import Enum
from heapq import heapify, heappop, heappush
import json
import os
import psutil
import re
import signal
import socket
import socketserver
import sys
import threading

from drms_utils import CmdlParser, Arguments as Args, MakeObject, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction
from drms_parameters import DRMSParams, DPMissingParameterError
from drms_export import Error as ExportError, ErrorCode as ExportErrorCode

DEFAULT_LOG_FILE = 'dx_log.txt'

class ErrorCode(ExportErrorCode):
    PARAMETERS = 1, 'failure locating DRMS parameters'
    ARGUMENTS = 2, 'bad arguments'
    MESSAGE_SYNTAX = 3, 'message syntax'
    MESSAGE_TIMEOUT = 4, 'message timeout event'
    UNEXCPECTED_MESSAGE = 5, 'unexpected message'
    SOCKET_SERVER = 6, 'TCP socket server creation error'
    CONTROL_CONNECTION = 7, 'control connection'
    DATA_CONNECTION = 8, 'data connection'
    SUBPROCESS = 9, 'subprocess'
    LOGGING = 10, 'logging'
    ERROR_MESSAGE = 11, 'error message received'

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

class UnexpectedMessageError(ExportError):
    _error_code = ErrorCode(ErrorCode.UNEXCPECTED_MESSAGE)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class SocketserverError(ExportError):
    _error_code = ErrorCode(ErrorCode.SOCKET_SERVER)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class ControlConnectionError(ExportError):
    _error_code = ErrorCode(ErrorCode.CONTROL_CONNECTION)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class DataConnectionError(ExportError):
    _error_code = ErrorCode(ErrorCode.DATA_CONNECTION)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class SubprocessError(ExportError):
    _error_code = ErrorCode(ErrorCode.SUBPROCESS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class LoggingError(ExportError):
    _error_code = ErrorCode(ErrorCode.LOGGING)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class ErrorMessageReceived(ExportError):
    _error_code = ErrorCode(ErrorCode.ERROR_MESSAGE)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class MessageType(Enum):
    CLIENT_READY = 1, f'CLIENT_READY'
    DATA_CONNECTION_READY = 2, f'DATA_CONNECTION_READY'
    DATA_VERIFIED = 3, f'DATA_VERIFIED'
    REQUEST_COMPLETE = 4, f'REQUEST_COMPLETE'
    ERROR = 5, f'ERROR'

    def __new__(cls, value, name):
        member = object.__new__(cls)
        member._value_ = value
        member.fullname = name
        return member

    def __int__(self):
        return self.value

class Message(object):
    def __init__(self, *, json_message=None, **kwargs):
        if json_message is None:
            self._data = MakeObject(name='data_class', data=kwargs)()
        else:
            self._data = MakeObject(name='data_class', data=json_message)()

    def __getattr__(self, name):
        if hasattr(self._data, name):
            return getattr(self._data, name)
        else:
            return None

    @classmethod
    def new_instance(cls, *, json_message, msg_type, **kwargs):
        if msg_type == MessageType.CLIENT_READY:
            message = ClientReadyMessage(json_message=json_message, **kwargs)
        elif msg_type == MessageType.DATA_CONNECTION_READY:
            message = DataConnectionReadyMessage(json_message=json_message, **kwargs)
        elif msg_type == MessageType.DATA_VERIFIED:
            message = DataVerifiedMessage(json_message=json_message, **kwargs)
        elif msg_type == MessageType.REQUEST_COMPLETE:
            message = RequestCompleteMessage(json_message=json_message, **kwargs)
        elif msg_type == MessageType.ERROR:
            message = ErrorMessage(json_message=json_message, **kwargs)
        else:
            raise UnexpectedMessageError(f'unknown error type `{msg_type.fullname}`')

        return message

    @classmethod
    def receive(cls, *, client_socket, msg_type, log):
        json_message_buffer = []
        open_count = 0
        close_count = 0
        timeout_event = None

        while True:
            try:
                # this is a non-blocking socket; timeout is 1 second
                binary_buffer = client_socket.recv(1024) # blocking
                if timeout_event is None:
                    timeout_event = datetime.now() + timedelta(30)
                if binary_buffer == b'':
                    break
            except socket.timeout as exc:
                # no data written to pipe
                if datetime.now() > timeout_event:
                    raise MessageTimeoutError(msg=f'[ Message.receive ] timeout event waiting for client {self.request.getpeername()} to send message')
                    log.write_debug([ f'[ Message.receive ] waiting for client {self.request.getpeername()} to send message...' ])
                continue

            buffer = binary_buffer.decode('UTF-8')

            if len(json_message_buffer) == 0:
                # look for opening curly brace
                buffer = buffer.lstrip()
                if buffer[0] == '{':
                    open_count += 1
                else:
                    raise MessageSyntaxError(msg=f'[ Message.receive ] invalid message synax; first character must be {str("{")}')

                json_message_buffer.append(buffer[0])
                buffer = buffer[1:]

            # count opening braces
            open_count += len(buffer.split('{')) - 1

            # count closing braces
            close_count += len(buffer.split('}')) - 1

            json_message_buffer.append(buffer)

            if close_count == open_count:
                break

        log.write_debug([ f'[ Message.receive ] received message from client {"".join(json_message_buffer)}' ])

        try_error = False
        try:
            message = cls.new_instance(json_message=''.join(json_message_buffer), msg_type=msg_type)
        except UnexpectedMessageError as exc:
            try_error = True

        if try_error:
            message = cls.new_instance(json_message=''.join(json_message_buffer), msg_type=MessageType.ERROR)

        if not hasattr(message, 'message'):
            raise MessageSyntaxError(msg=f'[ Message.receive ] invalid message synax; message must contain `message` attribute')

        if isinstance(message, ErrorMessage):
            raise ErrorMessageReceived(message.values[0])

        if message.message.lower() != msg_type.fullname.lower():
            raise UnexpectedMessageError(msg=f'[ Message.receive ] expecting {msg_type.fullname.lower()} message, but received {message.message.lower()} message')

        return message.values

    @classmethod
    def send(self, *, client_socket, msg_type, **kwargs):
        num_bytes_sent_total = 0
        num_bytes_sent = 0
        timeout_event = datetime.now() + timedelta(30)

        message_dict = {}
        message_dict['message'] = msg_type.fullname
        message_dict.update(kwargs)
        json_message = json.dumps(message_dict)
        encoded_message = json_message.encode('utf8')

        # no need to send length of json message since the receiver can simply find the opening and closing curly braces
        while num_bytes_sent_total < len(encoded_message):
            try:
                num_bytes_sent = client_socket.send(encoded_message[num_bytes_sent_total:])
                if not num_bytes_sent:
                    raise ControlConnectionError(msg=f'[ Message.send ] socket broken; cannot send message data to client')
                num_bytes_sent_total += num_bytes_sent
            except socket.timeout as exc:
                msg = f'timeout event waiting for client to receive message'
                if datetime.now() > timeout_event:
                    raise MessageTimeoutError(msg=f'{msg}')
                if self.log:
                    self.log.write_debug([ f'[ Message.send ] {msg}' ])

class ClientReadyMessage(Message):
    def __init__(self, *, json_message=None, **kwargs):
        super().__init__(json_message=json_message, **kwargs)
        self.values = (self.product, self.number_of_files, self.file_template)

class DataConnectionReadyMessage(Message):
    def __init__(self, *, json_message=None, **kwargs):
        super().__init__(json_message=json_message, **kwargs)
        self.values = (self.host_ip, self.port)

class DataVerifiedMessage(Message):
    def __init__(self, *, json_message=None, **kwargs):
        super().__init__(json_message=json_message, **kwargs)
        self.values = (self.number_of_files)

class RequestCompleteMessage(Message):
    def __init__(self, *, json_message=None, **kwargs):
        super().__init__(json_message=json_message, **kwargs)
        self.values = ()

class ErrorMessage(Message):
    def __init__(self, *, json_message=None, **kwargs):
        super().__init__(json_message=json_message, **kwargs)
        self.values = (self.error_message)

class Arguments(Args):
    _arguments = None

    @classmethod
    def get_arguments(cls, *, program_args, drms_params):
        if cls._arguments is None:
            try:
                server = drms_params.get_required('DX_SERVER')
                listen_port = int(drms_params.get_required('DX_LISTEN_PORT'))
                start_port = int(drms_params.get_required('DX_START_PORT'))
                number_of_ports = int(drms_params.get_required('DX_NUMBER_PORTS'))
                export_bin = drms_params.get_required('BIN_EXPORT')
                export_production_db_user = drms_params.get_required('EXPORT_PRODUCTION_DB_USER')
                log_file = os.path.join(drms_params.get_required('EXPORT_LOG_DIR'), DEFAULT_LOG_FILE)
            except DPMissingParameterError as exc:
                raise ParametersError(msg=str(exc))

            args = None

            if program_args is not None and len(program_args) > 0:
                args = program_args

            parser = CmdlParser(usage='%(prog)s ')

            # required
            parser.add_argument('dbhost', '--dbhost', help='the machine hosting the database that serves DRMS data products', metavar='<db host>', dest='db_host', required=True)

            # optional
            parser.add_argument('-c', '--chunk_size', help='the number of records to process at one time', metavar='<record chunk size>', dest='chunk_size', default=1024)
            parser.add_argument('-l', '--log_file', help='the path to the log file', metavar='<log file>', dest='log_file', default=log_file)
            parser.add_argument('-L', '--logging_level', help='the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', metavar='<logging level>', dest='logging_level', action=DrmsLogLevelAction, default=DrmsLogLevel.ERROR)
            parser.add_argument('-n', '--number_ports', help='the maximum number of data-connection ports', metavar='<number ports>', dest='number_of_ports', type=int, default=number_of_ports)
            parser.add_argument('-P', '--port', help='the port to listen on for new client connections', metavar='<listening port>', dest='listen_port', default=listen_port)
            parser.add_argument('-S', '--server', help='the server host accepting client connections', metavar='<server>', dest='server', default=server)
            parser.add_argument('-s', '--start_port', help='the first data-connection port', metavar='<start port>', dest='start_port', default=start_port)

            arguments = Arguments(parser=parser, args=args)

            # add needed drms parameters
            # arguments.export_bin = os.path.join(export_bin, 'linux_avx') # hack!
            # grrrr...
            arguments.export_bin = '/home/arta/jsoctrees/JSOC/_linux_avx/base/export/apps'
            arguments.export_production_db_user = export_production_db_user

            cls._arguments = arguments

        return cls._arguments

# use socketserver framework to create a multi-threaded TCP server
class DataTransferTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    # self.server_address has 2-tuple of (server ip address, host)

    def __init__(self, address, handler):
        self._all_ports = None
        self._t_lock = threading.Lock() # lock access to server ports to THIS server
        super().__init__(address, handler)

    def get_request(self):
        (control_socket, client_address) = super().get_request()
        self.log.write_info([ f'accepting connection request from client `{str(client_address)}`'])
        return (control_socket, client_address)

    def acquire_lock(self):
        return self._t_lock.acquire()

    def release_lock(self):
        return self._t_lock.release()

    # need to acquire thread lock
    def populate_ports(self):
        self._all_ports = list(range(self.start_port, self.start_port + self.number_of_ports))
        heapify(self._all_ports)

    def get_unused_port(self):
        if self._all_ports is None:
            self.populate_ports()

        try:
            return heappop(self._all_ports)
        except IndexError as exc:
            raise DataConnectionError(msg=f'no data-connection ports available')

    def put_unused_port(self, *, port):
        if self._all_ports is None:
            self.populate_ports()

        if port in self._all_ports:
            raise DataConnectionError(msg=f'port {port} is already unused')

        heappush(self._all_ports, port)

class DataTransferTCPRequestHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self._drms_id_regex = None
        super().__init__(request, client_address, server)

    # self.server is the DataTransferTCPServer
    def handle(self):
        self.server.log.write_info([ f'[ DataTransferTCPRequestHandler.handle ] handling session from client `{str(self.client_address)}`' ])
        terminate = False

        # make the client connection non-blocking, but make a long time out (the data transfer could take minutes), 1 hour
        self.request.settimeout(216000.0)

        try:
            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] waiting for client `{str(self.client_address)}` to send  {MessageType.CLIENT_READY.fullname} message' ])
            product, number_of_files, file_template = Message.receive(client_socket=self.request, msg_type=MessageType.CLIENT_READY, log=self.server.log)

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] initializing DATA connection for client `{str(self.client_address)}`' ])
            data_socket = self.initialize_data_connection()

            try:
                data_socket_ip, data_socket_port = data_socket.getsockname()

                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.DATA_CONNECTION_READY.fullname} message to client `{str(self.client_address)}`' ])
                Message.send(client_socket=self.request, msg_type=MessageType.DATA_CONNECTION_READY, **{ 'host_ip' : data_socket_ip, 'port' : data_socket_port}) # unblock client

                # wait for client to connect to DATA connection
                try:
                    data_socket.listen(128) # a single connection
                except OSError as exc:
                    raise DataConnectionError(msg=f'{str(exc)}')

                self._data_socket, address = data_socket.accept()

                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] DATA connection established for client `{str(self.client_address)}`' ])

                # make the dataconnection non-blocking; there really should not be any sizeable delays, so 30.0 seconds is ample
                self._data_socket.settimeout(30.0)

                # client will read from DATA stream - we want this to be synchronous; server needs to shut down DATA connection so that client sees EOF and knows download is complete
                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] streaming product data over DATA connection to client `{str(self.client_address)}`' ])
                asyncio.run(self.create_and_stream_package(product=product, number_of_files=number_of_files, file_template=file_template))

                # the export code has completed its run (we cannot do anything else until this happens - the client will not start streaming verification until after it has received the entire tar file); so now, receive manifest data and update the manifest table - these can happen asynchronously
                asyncio.run(self.receive_verification_and_update_manifest(product=product))
            finally:
                self.finalize_data_connection()

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] waiting for client `{str(self.client_address)}` to send  {MessageType.DATA_VERIFIED.fullname} message' ])
            number_of_ids = Message.receive(client_socket=self.request, msg_type=MessageType.DATA_VERIFIED, log=self.server.log)

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.REQUEST_COMPLETE.fullname} message to client `{str(self.client_address)}`' ])
            Message.send(client_socket=self.request, msg_type=MessageType.REQUEST_COMPLETE) # client unblocks on CONTROL connection, then ends CONTROL connection
        except socket.timeout as exc:
            # end session (return from handler)
            terminate = True
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] timeout waiting for client response', f'{str(exc)}'])
        except ControlConnectionError as exc:
            terminate = True
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] failure over CONTROL connection', f'{str(exc)}'])
        except ErrorMessageReceived as exc:
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] received error message from client `{str(self.client_address)}`' ])
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.REQUEST_COMPLETE.fullname} message to client `{str(self.client_address)}`' ])
            Message.send(client_socket=self.request, msg_type=MessageType.REQUEST_COMPLETE)
        except ExportError as exc:
            # send error response; client should shut down socket connection
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] error handling client request: {str(exc)}' ])
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.ERROR.fullname} message to client `{str(self.client_address)}`' ])
            Message.send(client_socket=self.request, msg_type=MessageType.ERROR, error_message=f'{str(exc)}')

        try:
            if not terminate:
                # block, waiting for client to terminate connection (to avoid TIME_WAIT)
                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] waiting for client `{str(self.client_address)}` to close CONTROL connection' ])
                if self.request.recv(128) == b'':
                    # client closed socket connection
                    self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] client `{str(self.client_address)}` closed CONTROL connection' ])
        except socket.timeout as exc:
            # end session (return from handler)
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] timeout waiting for client to terminate control connection', f'{str(exc)}' ])
        except OSError as exc:
            self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.handle ] failure waiting for client to close control connection', f'{str(exc)}' ])
        finally:
            # shut down socket
            try:
                self.request.shutdown(socket.SHUT_RDWR)
                self.request.close()
            except:
                pass

    def initialize_data_connection(self):
        self._data_socket = None
        bound = False
        attempts = 0
        previous_port = None

        server_ip_address = self.server.server_address[0]

        while attempts < 10 and not bound:
            self.server.acquire_lock()
            try:
                # try a new port
                unused_port = self.server.get_unused_port()

                # put a previously used one back
                if previous_port is not None:
                    self.server.put_unused_port(port=previous_port)
                    previous_port = None

                self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.initialize_data_connection ] using port {str(unused_port)} for DATA connection' ])
            finally:
                self.server.release_lock()

            try:
                info = socket.getaddrinfo(server_ip_address, unused_port)

                first_iteration = True
                for address_info in info:
                    if not first_iteration:
                        self.server.log.write_warning([ f'[ DataTransferTCPRequestHandler.initialize_data_connection ] unable to connect to {str(socket_address)}, trying a different IP address'])
                    else:
                        first_iteration = False

                    family = address_info[0]
                    socket_type = address_info[1]
                    proto = address_info[2]
                    socket_address = address_info[4] # 2-tuple for AF_INET family

                    if socket_type != socket.SOCK_STREAM:
                        # streaming only
                        continue

                    try:
                        server_socket = socket.socket(family, socket.SOCK_STREAM, proto)
                        server_socket.bind(socket_address)
                        bound = True
                        break
                    except OSError as exc:
                        pass
            except Exception as exc:
                self.server.log.write_warning([ f'[ DataTransferTCPRequestHandler.initialize_data_connection ] unable to connect to server, trying a different port: {str(exc)}'])

            if not bound:
                previous_port = unused_port
                attempts += 1

        if not bound:
            raise DataConnectionError(msg=f'[ DataTransferTCPRequestHandler.initialize_data_connection ] unable to create DATA-connection socket client `{str(self.client_address)}` (failed 10 attempts)')

        return server_socket

    def finalize_data_connection(self):
        if self._data_socket is not None:
            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.finalize_data_connection ]' ])
            # return used port
            host, port = self._data_socket.getsockname()
            self.server.acquire_lock()
            try:
                self.server.put_unused_port(port=port)
                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.finalize_data_connection ] put back port {str(port)}' ])
            finally:
                self.server.release_lock()

            # shut down read end of data socket and close socket (the client already closed their end)
            try:
                # do not shut down read - if the client has already shut down their end of socket, this will fail
                # self._data_socket.shutdown(socket.SHUT_RD)
                self._data_socket.close()
                self.server.log.write_info([ f'[ DataTransferTCPRequestHandler.finalize_data_connection ] successfully closed DATA connection f{str((host, port))}' ])
            except Exception as exc:
                self.server.log.write_error([ f'[ DataTransferTCPRequestHandler.finalize_data_connection ] unable to close DATA connection f{str((host, port))} properly' ])
                raise DataConnectionError(msg=f'{str(exc)}')
            finally:
                self._data_socket = None

    async def create_and_stream_package(self, *, product, number_of_files, file_template):
        # get list of DRMS_IDs
        self.server.log.write_info([ f'[ DataTransferTCPRequestHandler.create_and_stream_package ] fetching {str(number_of_files)} DRMS_IDs for product {product} for client `{str(self.client_address)}`' ])
        self._fetch_is_done = False
        self._drms_ids = asyncio.Queue()

        await asyncio.gather(self.fetch_drms_ids(product=product, number_of_ids=number_of_files), self.stream_package(product=product, file_template=file_template))

        # export code has completed

        # shut down write end of DATA connection - this sends EOF to client, but allows server to receive data verification
        self._data_socket.shutdown(socket.SHUT_WR)

    async def receive_verification_and_update_manifest(self, *, product):
        self._receive_is_done = False
        self._verified_recnums = asyncio.Queue()
        self._verification_data_exist = False

        # await asyncio.gather(self.receive_verification(), self.update_manifest(product=product))

        data_event = asyncio.Event()
        await asyncio.gather(self.receive_verification(data_event=data_event), asyncio.create_task(self.update_manifest(data_event=data_event, product=product)))

        self.finalize_data_connection()

    async def fetch_drms_ids(self, *, product, number_of_ids):
        # this command will create the manifest table should it not exist (but will fail if the shadow table does not exist)
        command = [ os.path.join(self.server.export_bin, 'data-xfer-manifest-tables'), f'series={product}', 'operation=fetch', f'n={number_of_ids}',  f'JSOC_DBUSER={arguments.export_production_db_user}', f'JSOC_DBHOST={arguments.db_host}' ]

        # starting with a recnum db query on the shadow table, create rows in the manifest table (get the recnum of the last row in the manifest table and then add n new rows, max)
        try:
            self.server.log.write_info([ f'[ DataTransferTCPRequestHandler.fetch_drms_ids ] running manifest-table manager for client `{str(self.client_address)}`: {" ".join(command)}' ])
            proc = await asyncio.subprocess.create_subprocess_shell(' '.join(command), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            while True:
                drms_id = await proc.stdout.readline() # bytes
                if not drms_id:
                    break
                await self._drms_ids.put(drms_id.decode().rstrip()) # strings

            await self._drms_ids.join()

            self._fetch_is_done = True

            await proc.wait()
            if proc.returncode is not None:
                # child process terminated
                if proc.returncode == 0:
                    pass
                else:
                    stdout, stderr = proc.communicate()
                    if stderr is not None and len(stderr) > 0:
                        raise SubprocessError(msg=f'[ fetch_drms_ids ] failure running manifest-table manager: {stderr.decode()}' )
                    else:
                        raise SubprocessError(msg=f'[ fetch_drms_ids ] failure running manifest-table manager' )
            else:
                raise SubprocessError(msg=f'[ fetch_drms_ids ] failure running manifest-table manager; no return code' )
        except (ValueError, OSError) as exc:
            raise SubprocessError(msg=f'[ fetch_drms_ids ] {str(exc)}' )

    async def stream_package(self, *, product, file_template=None):
        chunk_of_recnums = OrderedDict()
        if self._drms_id_regex is None:
            self._drms_id_regex = re.compile(r'[a-z_][a-z_0-9$]*[.][a-z_][a-z_0-9$]*:([0-9]+):[a-z_][a-z_0-9$]*')

        specification = f'{product}_manifest::[*]'
        arg_list = [ 'a=0', 's=0', 'e=1', 'm=1', 'DRMS_DBUTF8CLIENTENCODING=1', f'spec={specification}' ]

        if file_template is not None and len(file_template) > 0:
            arg_list.append(f'ffmt={file_template}')

        arg_list.append('DRMS_DBUTF8CLIENTENCODING=1')
        arg_list.append(f'JSOC_DBUSER={arguments.export_production_db_user}')
        arg_list.append(f'JSOC_DBHOST={arguments.db_host}')

        try:
            # we need to run drms-export-to-stdout, even if no DRMS_IDs are available for processing; running drms-export-to-stdout will create an empty manifest file in this case
            self.server.log.write_info([ f'[ DataTransferTCPRequestHandler.stream_package ] running export for client `{str(self.client_address)}`: {str(arg_list)}' ])
            proc = await asyncio.subprocess.create_subprocess_exec(os.path.join(self.server.export_bin, 'drms-export-to-stdout'), *arg_list, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)

            self.server.log.write_debug([f'[ DataTransferTCPRequestHandler.stream_package ] forked child process (pid {str(proc.pid)})'])

            loop = asyncio.get_running_loop()

            while True:
                # a list of recnum decimal byte strings
                while len(chunk_of_recnums) < self.server.chunk_size:
                    try:
                        drms_id = self._drms_ids.get_nowait()
                        # since DRMS_IDs are specific to a segment, a set of DRMS_IDs might contain duplicate recnums; remove dupes with an OrderedDict
                        recnum = self._drms_id_regex.match(drms_id).group(1)
                        chunk_of_recnums[recnum] = 0
                        self._drms_ids.task_done()
                    except asyncio.QueueEmpty:
                        # got all available recnums
                        if self._fetch_is_done:
                            # and there will be no more
                            break

                        # more recnums to come; wait for queue to fill a bit
                        await asyncio.sleep(0.5)

                # we have our chunk of recnums in `chunk_of_recnums`
                if proc.returncode is None:
                    if len(chunk_of_recnums) > 0:
                        # send renumcs to export subprocess
                        self.server.log.write_debug([f'[ DataTransferTCPRequestHandler.stream_package ] sending {",".join(chunk_of_recnums)} to process (pid {str(proc.pid)})'])
                        proc.stdin.write('\n'.join(list(chunk_of_recnums)).encode())
                        self.server.log.write_debug([f'[ DataTransferTCPRequestHandler.stream_package ] sent chunk of {str(len(chunk_of_recnums))} recnums to process (pid {str(proc.pid)})'])
                        await proc.stdin.drain()
                else:
                    raise SubprocessError(msg=f'export process died unexpectly, error code {str(proc.returncode)}')

                if self._fetch_is_done:
                    proc.stdin.close()
                    break

            # cannot start reading until all recnums have been sent to export code
            while True:
                # read data package file from export subprocess
                bytes_read = await proc.stdout.read(16384)
                if not bytes_read:
                    break

                # send data package file to client
                await loop.sock_sendall(self._data_socket, bytes_read)

            await proc.wait()
        except (ValueError, OSError) as exc:
            raise SubprocessError(msg=f'[ stream_package ] {str(exc)}' )

    async def receive_verification(self, *, data_event):
        self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.receive_verification ] receiving verification data from client `{str(self.client_address)}`' ])
        data_block = b''
        partial_line_start = None
        partial_line_end = None

        loop = asyncio.get_running_loop()
        while True:
            try:
                # data_block = self._data_socket.recv(8192) # non-blocking
                data_block = await loop.sock_recv(self._data_socket, 8192)
                if data_block == b'':
                    # server called shutdown on socket, but has not closed socket connection; server can still receive
                    break

                # we know we have some verification data
                self._verification_data_exist = True
                data_event.set()

                lines = data_block.decode().split('\n')
                lstripped_lines = [] # leading partial line removed
                full_lines = []

                if partial_line_start is not None:
                    # we have a partial line from last iteration
                    partial_line_end = lines[0] # first line this iteration completes partial line from last iteration
                    full_lines.append(f'{partial_line_start}{partial_line_end}')
                    lstripped_lines = lines[1:]
                    partial_line_start = None
                    partial_line_end = None
                else:
                    lstripped_lines = lines

                # last line could be partial
                if len(lstripped_lines[-1]) == 0:
                    # last element is empty string because last byte read was '\n'; not partial
                    full_lines = lstripped_lines[0:-1] # strip off empty-string element
                else:
                    # partial (full means '\n' was also read)
                    full_lines = lstripped_lines[0:-1] # remove trailing partial line
                    partial_line_start = lines[-1]

                verified_recnums = [ self._drms_id_regex.match(line.split()[0]).group(1) for line in full_lines if line.split()[1] == 'V' ]
                for recnum in verified_recnums:
                    await self._verified_recnums.put(recnum)

                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.receive_verification ] finished sending {len(verified_recnums)} recnums for verification processing (for client `{str(self.client_address)}`)' ])
            except socket.timeout as exc:
                # no data written to pipe
                raise MessageTimeoutError(msg=f'timeout event waiting for server {data_socket.getpeername()} to send data')

        self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.receive_verification ] finished receiving verification data from client `{str(self.client_address)}`' ])
        await self._verified_recnums.join()
        self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.receive_verification ] finished waiting for verification processing to complete (for client `{str(self.client_address)}`)' ])
        self._receive_is_done = True
        if not self._verification_data_exist:
            data_event.set()

    async def update_manifest(self, *, data_event, product):
        self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.update_manifest ] updating product {product} manifest with data received from client `{str(self.client_address)}`' ])

        await data_event.wait()
        if self._verification_data_exist:
            # only run the manifest-update code if there are verification data
            try:
                # unlike the subprocess module, the asyncio.subprocess module runs asynchronously
                arg_list = [ f'series={product}', f'operation=update', f'new_value=Y', f'JSOC_DBUSER={arguments.export_production_db_user}', f'JSOC_DBHOST={arguments.db_host}' ]

                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.update_manifest ] running manifest manager for client `{str(self.client_address)}`: {str(arg_list)}' ])
                proc = await asyncio.subprocess.create_subprocess_exec(os.path.join(self.server.export_bin, 'data-xfer-manifest-tables'), *arg_list, stdin=asyncio.subprocess.PIPE)

                chunk_of_recnums = []

                while True:
                    # a list of recnum decimal byte strings
                    while len(chunk_of_recnums) < self.server.chunk_size:
                        try:
                            chunk_of_recnums.append(self._verified_recnums.get_nowait())
                            self._verified_recnums.task_done()
                        except asyncio.QueueEmpty:
                            # got all available recnums
                            if self._receive_is_done:
                                # and there will be no more
                                break

                            # more recnums to come; wait for queue to fill a bit
                            await asyncio.sleep(0.5)

                    # we have our chunk of recnums in `chunk_of_recnums`
                    if len(chunk_of_recnums) > 0:
                        proc.stdin.write('\n'.join(chunk_of_recnums).encode())
                        self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.update_manifest ] updated chunk of {len(chunk_of_recnums)} recnums in manifest for client `{str(self.client_address)}`' ])
                        await proc.stdin.drain()
                        chunk_of_recnums = []

                    if self._receive_is_done:
                        proc.stdin.close()
                        self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.update_manifest ] done updating manifest for client `{str(self.client_address)}`' ])
                        break

                await proc.wait()
            except (ValueError, OSError) as exc:
                raise SubprocessError(msg=f'[ fetch_drms_ids ] {str(exc)}' )

def get_arguments(**kwargs):
    args = []
    for key, val in kwargs.items():
        args.append(f'{key} = {val}')

    drms_params = DRMSParams()

    if drms_params is None:
        raise ParametersError(msg=f'unable to locate DRMS parameters file')

    return Arguments.get_arguments(program_args=args, drms_params=drms_params)

def get_addresses(*, family, log):
     addresses = []
     for interface, snics in psutil.net_if_addrs().items():
         for snic in snics:
             if snic.family == family:
                 log.write_info([ f'identified server network address `{snic.address}`' ])
                 addresses.append(snic.address)

     return addresses

if __name__ == "__main__":
    exit_event = threading.Event()

    def quit_server(signo, _frame):
        exit_event.set()

    for sig in ('TERM', 'HUP', 'INT'):
        signal.signal(getattr(signal, f'SIG{sig}'), quit_server);

    try:
        arguments = get_arguments()
        try:
            formatter = DrmsLogFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            log = DrmsLog(arguments.log_file, arguments.logging_level, formatter)
        except exc:
            raise LogException(msg=f'{str(exc)}')

        # socket addresses available on the hosting machine
        addresses = get_addresses(family=socket.AF_INET, log=log)

        # determine suitable IP address of host
        host_ip = None
        try:
            for info in socket.getaddrinfo(arguments.server, arguments.listen_port):
                # limit to streaming internet connections
                if info[0] != socket.AF_INET:
                    continue
                if info[1] != socket.SOCK_STREAM:
                    continue

                host_ip = info[4][0]
        except OSError as exc:
            raise SocketserverError(msg=f'failure checking suitability of server: {str(exc)}')

        if host_ip is None:
            # host running the TCP server is not suitable
            raise SocketserverError(msg=f'host {arguments.server} is not a suitable for running the TCP server')

        bound = False
        for address in addresses:
            try:
                # use getaddrinfo() to try as many families/protocols as are supported; it returns a list
                info = socket.getaddrinfo(address, arguments.listen_port)
            except OSError as exc:
                raise SocketserverError(msg=f'failure checking suitability of server: {str(exc)}')

            for address_info in info:
                family = address_info[0]
                socket_address = address_info[4] # 2-tuple for AF_INET family

                if socket_address[0] != host_ip:
                    continue

                try:
                    log.write_info([ f'attempting to create {str(family)} TCP server with address`{str(socket_address)}`' ])

                    with DataTransferTCPServer(socket_address, DataTransferTCPRequestHandler) as data_server:
                        bound = True
                        # can re-use socket_address after socket is closed
                        log.write_info([ f'successfully created socketserver server, listening on {str(socket_address)}' ])

                        data_server.chunk_size = arguments.chunk_size
                        data_server.export_bin = arguments.export_bin
                        data_server.number_of_ports = arguments.number_of_ports
                        data_server.start_port = arguments.start_port
                        data_server.log = log

                        data_server_thread = threading.Thread(target=data_server.serve_forever)
                        data_server_thread.daemon = True
                        data_server_thread.start()
                        log.write_info([ f'successfully started server thread' ])

                        exit_event.wait()
                        log.write_info([ f'received shutdown signal...shutting down TCP server' ])
                        data_server.shutdown()

                    log.write_info([ f'successfully shut down TCP server' ])
                    break
                except OSError as exc:
                    log.write_warning([ f'{str(exc)}' ])
                    log.write_warning([ f'trying next address (could not create socketserver on {str(arguments.listen_port)})' ])
                    continue

            if bound:
                break
        if not bound:
            raise SocketserverError(msg=f'failure creating TCP server')
    except ExportError as exc:
        log.write_error([ str(exc) ])

    sys.exit(0)
else:
    # stuff run when this module is loaded into another module; export things needed to call check() and cancel()
    # return json and let the wrapper layer convert that to whatever is needed by the API
    pass
