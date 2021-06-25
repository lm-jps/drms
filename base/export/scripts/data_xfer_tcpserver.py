#!/usr/bin/env python3

import asyncio
from collections import namedtuple
from enum import Enum
import heapq
import json
import os
import psutil
import threading
import select
import signal
import socket
import socketserver
import sys

from drms_utils import CmdlParser, Arguments as Args, MakeObject, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction
from drms_parameters import DRMSParams
from drms_export import Error as ExportError, ErrorCode as ExportErrorCode

DEFAULT_LOG_FILE = 'dx_log.txt'

class ErrorCode(ExportErrorCode):
    PARAMETERS = 1, 'failure locating DRMS parameters'
    ARGUMENTS = 2, 'bad arguments'
    MESSAGE_SYNTAX = 3, 'message syntax'
    MESSAGE_TIMEOUT = 4, 'message timeout event'
    SOCKET_SERVER = 5, 'TCP socket server creation error'
    DATA_CONNECTION = 6, 'data connection'
    SUBPROCESS = 7, 'subprocess'
    LOGGING = 8, 'logging'

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

class SubprocessError(ExportError):
    _error_code = ErrorCode(ErrorCode.SUBPROCESS)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class LoggingError(ExportError):
    _error_code = ErrorCode(ErrorCode.LOGGING)

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
            self._data = MakeObject(name='data_class', data=kwargs)
        else:
            self._data = MakeObject(name='data_class', data=json_message)

    def __getattr__(self, name):
        return getattr(self._data, name)

    @classmethod
    def new_instance(cls, *, json_message, **kwargs):
        message = cls(json_message=json_message, **kwargs)
        return message

    @classmethod
    def receive(cls, *, socket_fd, msg_type):
        json_message = ''
        open_count = 0
        closed_count = 0
        timeout_event = None

        while True:
            try:
                # this is a non-blocking socket; timeout is 1 second
                binary_buffer = self.socket_fd.recv(1024) # blocking
                if timeout_event is None:
                    timeout_event = datetime.now() + timedelta(30)
                if binary_buffer == b'':
                    break
            except socket.timeout as exc:
                # no data written to pipe
                if datetime.now() > timeout_event:
                    raise MessageTimeoutError(f'[ Message.receive ] timeout event waiting for client {self.request.getpeername()} to send message')
                if self.log:
                    self.log.write_debug([ f'[ Message.receive ] waiting for client {self.request.getpeername()} to send message...' ])
                continue

            buffer = binary_buffer.decode('UTF-8')

            if len(json_message) == 0:
                # look for opening curly brace
                buffer = buffer.lstrip()
                if buffer[0] == '{':
                    open_count += 1
                else:
                    raise MessageSyntaxError(f'[ Message.receive ] invalid message synax; first character must be {str("{")}')

                json_message = buffer[0]
                buffer = buffer[1:]

            # count opening braces
            open_count += len(buffer.split('{')) - 1

            # count closing braces
            close_count += len(buffer.split('}')) - 1

            json_message += buffer

            if close_count == open_count:
                break

        message = cls.new_instance(json_message=message)

        if not hasattr(message, 'message'):
            raise MessageSyntaxError(f'[ Message.receive ] invalid message synax; message must contain `message` attribute')

        if message.lower() != msg_type.fullname:
            raise UnexpectedMessageError(f'[ Message.receive ] expecting {mst_type.fullname.lower()} message, but received {message.lower()} message')

        return message

    def send(self, *, socket_fd, msg_type, **kwargs):
        num_bytes_sent_total = 0
        timeout_event = datetime.now() + timedelta(30)

        message_dict = {}
        message_dict['message'] = msg_type.fullname
        message_dict.update(kwargs)
        json_message = json.dumps(message_dict)

        while True:
            try:
                num_bytes_sent = socket_fd.send(json_message.encode())
                if not num_bytes_sent:
                    raise SocketError(f'[ Message.send ] socket broken; cannot send message data to client')
                num_bytes_sent_total += num_bytes_sent
            except socket.timeout as exc:
                msg = f'timeout event waiting for client to receive message'
                if datetime.now() > timeout_event:
                    raise MessageTimeoutError(f'{msg}')
                if self.log:
                    self.log.write_debug([ f'[ Message.send ] {msg}' ])

class Arguments(Args):
    _arguments = None

    @classmethod
    def get_arguments(cls, *, program_args, drms_params):
        if cls._arguments is None:
            try:
                listen_port = int(drms_params.get_required('DX_LISTEN_PORT'))
                start_port = int(drms_params.get_required('DX_START_PORT'))
                number_of_ports = int(drms_params.get_required('DX_NUMBER_PORTS'))
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
            parser.add_argument('-n', '--number_ports', help='the maximum number of data-connection ports', metavar='<number ports>', dest='number_of_ports', default=number_of_ports)
            parser.add_argument('-p', '--port', help='the port to listen on for new client connections', metavar='<listening port>', dest='listen_port', default=listen_port)
            parser.add_argument('-s', '--start_port', help='the first data-connection port', metavar='<start port>', dest='start_port', default=start_port)

            arguments = Arguments(parser=parser, args=args)

            # add needed drms parameters
            arguments.bin_export = drms_params.bin_export

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
        (socket_fd, client_address) = super().get_request()
        self.log.write_info([ f'accepting connection request from client `{str(client_address)}`'])
        return (socket_fd, client_address)

    def acquire_lock(self):
        return self._t_lock.acquire()

    def release_lock(self):
        return self._t_lock.release()

    # need to acquire thread lock
    def populate_ports(self):
        self._all_ports = list(range(self.start_port, self.start_port + self.number_of_ports))
        heapq.heapify(self._all_ports)

    def get_unused_port(self):
        if self._all_ports is None:
            self.populate_ports()

        try:
            return heapq.heappop()
        except IndexError as exc:
            raise DataConnectionError(f'no data-connection ports available')

    def put_unused_port(self, *, port):
        if self._all_ports is None:
            self.populate_ports()

        if port in self._all_ports:
            raise DataConnectionError(f'port {port} is already unused')

        heapq.heappush(self._all_ports, port)

class DataTransferTCPRequestHandler(socketserver.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self.drms_id_regex = None
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)

    # self.server is the DataTransferTCPServer
    def handle(self):
        self.log.write_info([ f'[ DataTransferTCPRequestHandler.handle ] handling session from client `{str(self.client_address)}`' ])
        terminate = False

        # make the client connection non-blocking, but make a long time out (the data transfer could take minutes), 1 hour
        self.request.settimeout(216000.0)

        try:
            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] waiting for client `{str(self.client_address)}` to send  {MessageType.CLIENT_READY.fullname} message' ])
            product, number_files, file_template = Message.receive(socket_fd=self.request, msg_type=MessageType.CLIENT_READY)

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] initializing DATA connection for client `{str(self.client_address)}`' ])
            data_socket = self.initialize_data_connection()
            data_socket_ip, data_socket_port = data_socket.getsockname()

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.DATA_CONNECTION_READY.fullname} to client `{str(self.client_address)}`' ])
            Message.send(socket_fd=self.request, msg_type=MessageType.DATA_CONNECTION_READY, **{ 'host_ip' : data_socket_ip, 'port' : data_socket_port}) # unblock client

            # wait for client to connect to DATA connection
            try:
                data_socket.listen(128) # a single connection
            except OSError as exc:
                raise DataConnectionError(f'{str(exc)}')

            self.data_socket, address = data_socket.accept()

            # client will read from DATA stream - we want this to be synchronous; server needs to shut down DATA connection so that client sees EOF and knows download is complete
            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] streaming product data over DATA connection to client `{str(self.client_address)}`' ])
            ayncio.run(self.create_and_stream_package(socket_fd=data_socket, product=product, number_files=number_files, file_template=file_template))

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] waiting for client `{str(self.client_address)}` to send  {MessageType.DATA_VERIFIED.fullname} message' ])
            number_ids = Message.receive(socket_fd=self.request, msg_type=MessageType.DATA_VERIFIED)
            # the server can now read from the DATA connection to get verification info

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] updating product {product} manifest with data received from client `{str(self.client_address)}`' ])
            self.update_manifest(number_ids) # do this asynchronously - read from CONTROL stream `number_ids` lines, one line per DRMS ID

            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.REQUEST_COMPLETE.fullname} to client `{str(self.client_address)}`' ])
            Message.send(socket_fd=self.request, msg_type=MessageType.REQUEST_COMPLETE) # client unblocks on CONTROL connection, then ends CONTROL connection
        except socket.timeout as exc:
            # end session (return from handler)
            terminate = True
            self.server.log.write_error([ f'timeout waiting for client response', f'{str(exc)}'])
        except ExportError as exc:
            # send error response; client should shut down socket connection
            self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] sending {MessageType.ERROR.fullname} to client `{str(self.client_address)}`' ])
            Message.send(socket=self.request, msg_type=MessageType.ERROR, error_message=f'{str(exc)}')

        try:
            if not terminate:
                # block, waiting for client to terminate connection (to avoid TIME_WAIT)
                self.server.log.write_debug([ f'[ DataTransferTCPRequestHandler.handle ] waiting for client `{str(self.client_address)}` to shut down CONTROL connection' ])
                if self.request.recv(128) == b'':
                    # client closed socket connection
                    pass
        except socket.timeout as exc:
            # end session (return from handler)
            terminate = True
            self.server.log.write_error([ f'timeout waiting for client to terminate control connection', f'{str(exc)}' ])
        finally:
            # shut down socket
            self.request.close()
            self.request.shutdown(socket.SHUT_RDWR)

    def initialize_data_connection(self):
        server_ip_address = self.server.server_address[0]
        self.server.acquire_lock()
        try:
            unused_port = self.server.get_unused_port()
        finally:
            self.server.release_lock()

        info = socket.getaddrinfo(server_ip_address, unused_port)

        bound = False
        for address_info in info:
            family = address_info[0]
            proto = address_info[2]
            socket_address = address_info[4] # 2-tuple for AF_INET family

            try:
                server_socket = socket.socket(family, socket.SOCK_STREAM, proto)
                server_socket.bind((server_ip_address, unused_port))
                bound = True
            except OSError as exc:
                continue

            if not bound:
                raise DataConnectionError(f'[ DataTransferTCPRequestHandler.initialize_data_connection ] unable to create DATA-connection socket client `{str(self.client_address)}` (trying again): {str(exc)}')

        return server_socket

    async def create_and_stream_package(self, *, socket_fd, product, number_files, file_template):
        if self.drms_id_regex is None:
            self.drms_id_regex = re.compile(r'[a-z_][a-z_0-9$]*[.][a-z_][a-z_0-9$]*:([0-9]+):[a-z_][a-z_0-9$]*')

        # need to obtain lock so that threads do not duplicate efforts, or omit one or more DRMS_IDs
        self.server.acquire_lock()
        try:
            # get list of DRMS_IDs
            self.log.write_info([ f'[ DataTransferTCPRequestHandler.create_and_stream_package ] fetching {str(number_ids)} DRMS_IDs for product {product} for client `{str(self.client_address)}`' ])
            drms_ids = await fetch_drms_ids(product=product, number_ids=number_ids)
        finally:
            self.server.release_lock()

        # a list of recnum decimal byte strings
        recnums = [ self.drms_id_regex.match(id).group(0) for id in drms_ids ]

        # call drms-export-to-stdout
        await stream_package(socket_fd=socket_fd, product=product, recnums=recnums, file_template=file_template)

        self.server.acquire_lock()
        try:
            # update manifest status
            # will be called only if `stream_package` succeeded;
            # if there is some problem updating the manifest table, partial changes to it will be rolled back;
            await update_manifest(product=product, recnums=recnums)
        finally:
            self.server.release_lock()

    async def fetch_drms_ids(self, *, product, number_ids):
        drms_ids = []

        # this command will create the manifest table should it not exist (but will fail if the shadow table does not exist)
        command = [ os.path.join(self.server.export_bin, 'data-xfer-manifest-tables'), f'series={product}', 'operation=fetch', f'n={number_ids}',  f'JSOC_DBUSER={arguments.export_production_db_user}', f'JSOC_DBHOST={arguments.db_host}' ]

        # starting with a recnum db query on the shadow table, create rows in the manifest table (get the recnum of the last row in the manifest table and then add n new rows, max)
        try:
            # unlike the subprocess modules, the asyncio.subprocess module
            proc = await asyncio.subprocess.create_subprocess_shell(' '.join(command), stdout=asyncio.subprocess.PIPE)

            while True:
                drms_id = await proc.stdout.readline() # bytes
                drms_ids.append(drms_id.decode().rstrip()) # strings

            await proc.wait()
        except (ValueError, OSError) as exc:
            raise SubprocessError(f'[ fetch_drms_ids ] {str(exc)}' )

        return drms_ids

    async def stream_package(self, *, socket_fd, product, drms_ids, file_template=None):
        if self.drms_id_regex is None:
            self.drms_id_regex = re.compile(r'[a-z_][a-z_0-9$]*[.][a-z_][a-z_0-9$]*:([0-9]+):[a-z_][a-z_0-9$]*')

        # drms_ids are sorted by recnum
        min_recnum = self.drms_id_regex.match(drms_ids[0]).group(0)
        max_recnum = self.drms_id_regex.match(drms_ids[-1]).group(0)

        specification = f'{product}::[{min_recnum}-{max_recnum}]'
        arg_list = [ 'a=0', 's=0', 'e=1', 'DRMS_DBUTF8CLIENTENCODING=1' ]
        arg_list.append(shlex.quote(f'spec={specification}'))

        if file_template is not None and len(file_template) > 0:
            arg_list.append(shlex.quote(f'ffmt={file_template}'))

        arg_list.append('DRMS_DBUTF8CLIENTENCODING=1')
        arg_list.append(f'JSOC_DBUSER={arguments.export_production_db_user}')
        arg_list.append(f'JSOC_DBHOST={arguments.db_host}')

        # a list of recnum decimal byte strings
        recnums = [ self.drms_id_regex.match(id).group(0) for id in drms_ids ]

        try:
            self.log.write_info([ f'[ DataTransferTCPRequestHandler.stream_package ] running export for client `{str(self.client_address)}`: {str(arg_list)}' ])
            proc = await asyncio.subprocess.create_subprocess_exec(os.path.join(self.server.export_bin, 'drms-export-to-stdout'), *arg_list, stdout=asyncio.subprocess.PIPE)

            start_recnum_index = 0
            max_recnum_index = len(recnums) - 1
            while True:
                # write a chunk of recnums to child's stdin
                if start_recnum_index < max_recnum_index:
                    chunk = recnums[start_recnum_index:start_recnum_index + self.server.chunk_size - 1]
                    await proc.stdin.write('\n'.join(chunk).encode())
                    start_recnum_index += len(chunk)

                bytes_read = await proc.stdout.read(16384)
                if not bytes_read:
                    break

                socket_fd.write(bytes_read)

            await proc.wait()
        except (ValueError, OSError) as exc:
            raise SubprocessError(f'[ stream_package ] {str(exc)}' )

    async def update_manifest(self, *, product, recnums):
        try:
            # unlike the subprocess module, the asyncio.subprocess module runs asynchronously
            arg_list = [ f'series={product}', f'operation=update', f'new_value=Y', f'JSOC_DBUSER={arguments.export_production_db_user}', f'JSOC_DBHOST={arguments.db_host}' ]

            self.log.write_debug([ f'[ DataTransferTCPRequestHandler.update_manifest ] running manifest manager for client `{str(self.client_address)}`: {str(arg_list)}' ])
            proc = await asyncio.subprocess.create_subprocess_exec(os.path.join(self.server.export_bin, 'data-xfer-manifest-tables'), *arg_list, stdin=asyncio.subprocess.PIPE)

            start_recnum_index = 0
            max_recnum_index = len(recnums) - 1
            while True:
                # send recnums to child process
                if start_recnum_index < max_recnum_index:
                    chunk = recnums[start_recnum_index:start_recnum_index + self.server.chunk_size - 1]
                    await proc.stdin.write('\n'.join(chunk).encode())
                    start_recnum_index += len(chunk)
                else:
                    break

            await proc.wait()
        except (ValueError, OSError) as exc:
            raise SubprocessError(f'[ fetch_drms_ids ] {str(exc)}' )

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
            raise LogException(f'{str(exc)}')

        addresses = get_addresses(family=socket.AF_INET, log=log)

        bound = False
        for address in addresses:
            # use getaddrinfo() to try as many families/protocols as are supported; it returns a list
            info = socket.getaddrinfo(address, arguments.listen_port)

            for address_info in info:
                family = address_info[0]
                socket_address = address_info[4] # 2-tuple for AF_INET family

                try:
                    log.write_info([ f'attempting to create {str(family)} TCP server with address`{str(socket_address)}`' ])

                    with DataTransferTCPServer(socket_address, DataTransferTCPRequestHandler) as data_server:
                        bound = True
                        # can re-use socket_address after socket is closed
                        log.write_info([ f'successfully created socketserver server' ])

                        data_server.chunk_size = arguments.chunk_size
                        data_server.export_bin = arguments.bin_export
                        data_server.number_ports = arguments.number_of_ports
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
    except OSError as exc:
        log.write_error(str(exc))
        raise SocketserverError(f'failure creating TCP socketserver')

    sys.exit(0)
else:
    # stuff run when this module is loaded into another module; export things needed to call check() and cancel()
    # return json and let the wrapper layer convert that to whatever is needed by the API
    pass
