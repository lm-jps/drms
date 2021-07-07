#!/usr/bin/env python3

from datetime import datetime, timedelta
from enum import Enum
import json
from os.path import join as os_join
import socket
from sys import exit, stdout
import tarfile
from uuid import uuid4

from drms_export import Error as ExportError, ErrorCode as ExportErrorCode
from drms_parameters import DRMSParams, DPMissingParameterError
from drms_utils import Arguments as Args, CmdlParser, Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction, MakeObject

DEFAULT_LOG_FILE = 'dx_client_log.txt'
MANIFEST_FILE = 'jsoc/manifest.txt'

class ErrorCode(ExportErrorCode):
    PARAMETERS = 1, 'failure locating DRMS parameters'
    ARGUMENTS = 2, 'bad arguments'
    MESSAGE_SYNTAX = 3, 'message syntax'
    MESSAGE_TIMEOUT = 4, 'message timeout event'
    UNEXCPECTED_MESSAGE = 5, 'unexpected message'
    TCP_CLIENT = 6, 'TCP socket server creation error'
    CONTROL_CONNECTION = 7, 'control connection'
    DATA_CONNECTION = 8, 'data connection'
    DATA_PACKAGE_FILE = 9, 'data package'
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

class TcpClientError(ExportError):
    _error_code = ErrorCode(ErrorCode.TCP_CLIENT)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class DataConnectionError(ExportError):
    _error_code = ErrorCode(ErrorCode.DATA_CONNECTION)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class ControlConnectionError(ExportError):
    _error_code = ErrorCode(ErrorCode.CONTROL_CONNECTION)

    def __init__(self, *, msg=None):
        super().__init__(msg=msg)

class DataPackageError(ExportError):
    _error_code = ErrorCode(ErrorCode.DATA_PACKAGE_FILE)

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

class Arguments(Args):
    _arguments = None

    @classmethod
    def get_arguments(cls, *, program_args, drms_params):
        if cls._arguments is None:
            try:
                server = drms_params.get_required('DX_SERVER')
                server_port = int(drms_params.get_required('DX_LISTEN_PORT'))
            except DPMissingParameterError as exc:
                raise ParametersError(msg=str(exc))

            args = None

            if program_args is not None and len(program_args) > 0:
                args = program_args

            parser = CmdlParser(usage='%(prog)s ')

            # required
            parser.add_argument('product', help='the data product to which the files belong', metavar='<product>', dest='product', required=True)

            # optional
            parser.add_argument('-d', '--download_directory', help='the directory to which tar file will be downloaded', metavar='<download directory>', dest='download_directory', default='.')
            parser.add_argument('-e', '--export_file_format', help='the export file-name format string to be used when creating the exported files', metavar='<export file format>', dest='export_file_format', default=None)
            parser.add_argument('-l', '--logging_level', help='the amount of logging to perform; in order of increasing verbosity: critical, error, warning, info, debug', metavar='<logging level>', dest='logging_level', action=DrmsLogLevelAction, default=DrmsLogLevel.ERROR)
            parser.add_argument('-n', '--number_of_files', help='the number of data files/DRMS_IDs to process at one time', metavar='<number of files>', dest='number_of_files', default=1024)

            arguments = Arguments(parser=parser, args=args)

            # add needed drms parameters
            arguments.server = server
            arguments.server_port = server_port

            cls._arguments = arguments

        return cls._arguments

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
        return getattr(self._data, name)

    def __str__(self):
        return self.message

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
    def receive(cls, *, server_connection, msg_type, log):
        json_message_buffer = []
        open_count = 0
        close_count = 0
        timeout_event = None

        while True:
            try:
                # this is a non-blocking socket; timeout is 1 second
                binary_buffer = server_connection.recv(1024) # blocking
                if timeout_event is None:
                    timeout_event = datetime.now() + timedelta(30)
                if binary_buffer == b'':
                    break
            except socket.timeout as exc:
                # no data written to pipe
                if datetime.now() > timeout_event:
                    raise MessageTimeoutError(msg=f'[ Message.receive ] timeout event waiting for server to send message')
                log.write_info([ f'[ Message.receive ] waiting for server to send message...' ])
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

        log.write_info([ f'[ Message.receive ] received message from server {"".join(json_message_buffer)}' ])

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

        log.write_info([ f'[ Message.receive ] message is of type {str(message)}' ])

        return message.values

    @classmethod
    def send(self, *, server_connection, msg_type, log, **kwargs):
        num_bytes_sent_total = 0
        num_bytes_sent = 0
        timeout_event = datetime.now() + timedelta(30.0)

        message_dict = {}
        message_dict['message'] = msg_type.fullname
        message_dict.update(kwargs)
        json_message = json.dumps(message_dict)
        log.write_info([ f'message: {json_message}' ])
        encoded_message = json_message.encode('utf8')

        # no need to send length of json message since the receiver can simply find the opening and closing curly braces
        while num_bytes_sent_total < len(encoded_message):
            try:
                num_bytes_sent = server_connection.send(encoded_message[num_bytes_sent_total:])
                if not num_bytes_sent:
                    raise SocketError(msg=f'[ Message.send ] socket broken; cannot send message data to server')
                num_bytes_sent_total += num_bytes_sent
            except socket.timeout as exc:
                msg = f'timeout event waiting for server to receive message'
                log.write_debug([ f'[ Message.send ] {msg}' ])

                if datetime.now() > timeout_event:
                    raise MessageTimeoutError(msg=f'{msg}')

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

def get_arguments(**kwargs):
    args = []
    for key, val in kwargs.items():
        args.append(f'{key} = {val}')

    drms_params = DRMSParams()

    if drms_params is None:
        raise ParametersError(msg=f'unable to locate DRMS parameters file')

    return Arguments.get_arguments(program_args=args, drms_params=drms_params)

def download_data(data_socket, download_path, log):
    # make this a non-blocking socket so we can give up if the server appears to go away
    data_socket.settimeout(15.0)
    timeout_event = None

    log.write_info([ f'downloading data-package file to {download_path}...' ])
    with open(download_path, 'wb') as dl_file:
        while True:
            try:
                binary_buffer = data_socket.recv(4096) # non-blocking
                if binary_buffer == b'':
                    # server called shutdown on data socket, but has not closed socket connection; server can still receive
                    break
                dl_file.write(binary_buffer)
            except socket.timeout as exc:
                # no data written to pipe
                raise MessageTimeoutError(msg=f'timeout event waiting for server {data_socket.getpeername()} to send data')

    log.write_info([ f'...DONE' ])

def send_verification(data_socket, verification_data):
    total_bytes_sent = 0
    bytes_sent = 0
    while total_bytes_sent < len(verification_data):
        bytes_sent = data_socket.send(verification_data)
        if bytes_sent == 0:
            raise SocketError(msg=f'data connection to server is broken')

        total_bytes_sent += bytes_sent

def verify_and_store_data(data_socket, download_path, log):
    buffer = []
    number_of_files = 0

    # open tar file
    log.write_info([ f'opening data-package `{download_path}`' ])
    with tarfile.open(name=download_path, mode='r') as open_archive:
        log.write_info([ f'extracting manifest file `{MANIFEST_FILE}`' ])
        try:
            with open_archive.extractfile(MANIFEST_FILE) as manifest_file:
                # the manifest file is a text file - we do not need to read the whole file into memory to start processing it
                for line in manifest_file:
                    drms_id, file = line.split() # binary data

                    # verify checksum, etc.
                    log.write_info([ f'verifying data integrity of `{file.decode()}`...' ])
                    verification_status = 'V' # pretend the data are good
                    log.write_info([ f'...DONE' ])

                    # store file
                    log.write_info([ f'storing data file `{file.decode()}`...' ])
                    log.write_info([ f'...DONE' ])

                    # send server verification information
                    buffer.append(drms_id)
                    buffer.append(b' ')
                    buffer.append(verification_status.encode('utf8'))
                    buffer.append(b'\n')
                    number_of_files += 1

                    if number_of_files >= 256:
                        send_verification(data_socket, b''.join(buffer))
                        buffer = []

                if len(buffer) > 0:
                    send_verification(data_socket, b''.join(buffer))
                    buffer = []

                log.write_info([ f'sent verification data to server' ])
        except:
            raise DataPackageError(msg=f'unable to locate manifest file {MANIFEST_FILE}')

    return number_of_files


if __name__ == "__main__":
    try:
        arguments = get_arguments()
        try:
            formatter = DrmsLogFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            log = DrmsLog(stdout, arguments.logging_level, formatter)
        except Exception as exc:
            raise LoggingError(msg=f'{str(exc)}')

        info = socket.getaddrinfo(arguments.server, arguments.server_port)
        control_connected = False
        for address_info in info:
            family = address_info[0]
            sock_type = address_info[1]

            if family == socket.AF_INET and sock_type == socket.SOCK_STREAM:
                socket_address = address_info[4] # 2-tuple for AF_INET family

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as control_socket:
                        log.write_info([ f'created socket' ])
                        try:
                            log.write_info([ f'connecting to {socket_address}' ])
                            control_socket.connect((socket_address[0], socket_address[1]))
                            control_connected = True
                            log.write_info([ f'control connection established' ])
                        except OSError as exc:
                            log.write_info([ f'WARNING: connection to {socket_address} failed - {str(exc)}' ])
                            control_socket.close()
                            control_socket = None
                            continue

                        # connected to server; perform workflow
                        try:
                            number_of_files = 0

                            # send CLIENT_READY
                            log.write_info([ f'sending {MessageType.CLIENT_READY.fullname} message to server' ])
                            Message.send(server_connection=control_socket, msg_type=MessageType.CLIENT_READY, log=log, **{ 'product' : arguments.product, 'number_of_files' : arguments.number_of_files, 'export_file_format' : arguments.export_file_format })

                            # receive DATA_CONNECTION_READY (or ERROR)
                            log.write_info([ f'waiting for server to send {MessageType.DATA_CONNECTION_READY.fullname} message' ])
                            host_ip, port = Message.receive(server_connection=control_socket, msg_type=MessageType.DATA_CONNECTION_READY, log=log)

                            # make DATA connection
                            info = socket.getaddrinfo(host_ip, port)
                            data_connected = False
                            for address_info in info:
                                family = address_info[0]
                                sock_type = address_info[1]

                                if family == socket.AF_INET and sock_type == socket.SOCK_STREAM:
                                    socket_address = address_info[4] # 2-tuple for AF_INET family

                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as data_socket:
                                        try:
                                            data_socket.connect(socket_address)
                                            data_connected = True
                                            log.write_info([ f'DATA connection to server established' ])
                                        except OSError as exc:
                                            data_socket.close()
                                            data_socket = None
                                            continue

                                        try:
                                            download_file = f'{str(uuid4())}.tar'
                                            download_path = os_join(arguments.download_directory, download_file)

                                            # download product file over DATA connection
                                            download_data(data_socket, download_path, log)

                                            # data have been downloaded over DATA connection, now need to shut down read part of socket
                                            data_socket.shutdown(socket.SHUT_RD)
                                            log.write_info([ f'successfuly shut down read on DATA connection' ])

                                            # server has already shutdown its write, but is awaiting an EOF from client; send verification data back over DATA connection
                                            number_of_files = verify_and_store_data(data_socket, download_path, log)

                                            data_socket.shutdown(socket.SHUT_WR)
                                            log.write_info([ f'successfuly shut down write on DATA connection' ])
                                        except tarfile.ReadError as exc:
                                            raise DataPackageError(f'{str(exc)}')
                                        finally:
                                            if data_connected:
                                                log.write_info([ f'client closed DATA connection' ])

                                        # exiting with statement causes data_socket.close() to be called
                                    # data socket is now closed

                                if data_connected:
                                    break
                            if data_connected:
                                log.write_info([ f'sending {MessageType.DATA_VERIFIED.fullname} message to server' ])
                                # send DATA_VERIFIED message over CONTROL connection
                                Message.send(server_connection=control_socket, msg_type=MessageType.DATA_VERIFIED, log=log, **{ 'number_of_files' : number_of_files }) # unblock server

                                log.write_info([ f'waiting for server to send {MessageType.REQUEST_COMPLETE.fullname} message' ])
                                # receive REQUEST_COMPLETE message from server over CONTROL connection
                                Message.receive(server_connection=control_socket, msg_type=MessageType.REQUEST_COMPLETE,log=log)
                            else:
                                raise DataConnectionError(msg=f'unable to make data connection')
                        except ErrorMessageReceived as exc:
                            log.write_info([ f'received error message from server {str(exc)}; shutting down socket' ])
                        finally:
                            control_socket.shutdown(socket.SHUT_RDWR)
                            log.write_info([ f'client shut down CONTROL connection' ])
                    # control socket was closed
                    log.write_info([ f'client closed CONTROL connection' ])
                except OSError as exc:
                    log.write_info([ str(exc) ])
                    raise TcpClientError(msg=f'failure creating TCP client')

            if control_connected:
                break
        if not control_connected:
            raise ControlConnectionError(msg=f'unable to establish CONTROL connection')
    except LoggingError as exc:
        print(str(exc),file=sys.stderr)
    except ExportError as exc:
        log.write_info([ str(exc) ])

    exit(0)
else:
    # stuff run when this module is loaded into another module; export things needed to call check() and cancel()
    # return json and let the wrapper layer convert that to whatever is needed by the API
    pass
