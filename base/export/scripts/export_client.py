from json import dumps as json_dumps, loads as json_loads
from os.path import join as path_join
import re
from socket import SHUT_RDWR

from drms_utils import Formatter as DrmsLogFormatter, Log as DrmsLog, LogLevel as DrmsLogLevel, LogLevelAction as DrmsLogLevelAction
from drms_export import Connection, create_generator, get_arguments, get_message, send_message

FILE_NAME_SIZE = 256

def process_request(request, connection):
    json_message = json_dumps(request)
    send_message(connection, json_message)
    message = get_message(connection)
    return message

def read_from_connection(*, destination):
    if destination['has_header'] and not destination['header_extracted']:
        # this is the generator's first iteration
        # a FITS file prepended with filename; read the filename first
        number_bytes_read = 0
        file_name_buffer = []
        while True:
            bytes_read = destination['connection'].recv(FILE_NAME_SIZE) # blocking
            file_name_buffer.append(bytes_read)
            number_bytes_read += len(bytes_read)

            if number_bytes_read == FILE_NAME_SIZE:
                break

        # truncate padding (0 bytes)
        file_name = b''.join(file_name_buffer).rstrip(b'\x00').decode()

        # force alphanumeric (plus '_'), preserving the file extension
        matches = re.search(r'(^.+)[.]([^.]+$)', file_name)
        if matches is not None:
            base = matches.group(1)
            extension = matches.group(2)
            file_name = f'{re.sub(r"[^a-zA-Z0-9_]", "_", base)}.{extension}'

        destination['file_name'] = file_name # for use in `perform_action()` caller
        destination['header_extracted'] = True

        return (True, '')
    else:
        # dump the remainder of the FITS file (stdout is what the child process is dumping to); partial
        # reads are fine
        bytes_read = destination['connection'].recv(4096)
        if not bytes_read:
            # server done writing payload data (server shutdown connection) - close client end of connection
            destination['connection'].shutdown(SHUT_RDWR)
            destination['connection'].close()

            return (False, b'')
        else:
            return (True, bytes_read)

if __name__ == "__main__":
    try:
        test_module_args = { }
        arguments = get_arguments(is_program=False, module_args=test_module_args)

        formatter = DrmsLogFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        log = DrmsLog('/home/jsoc/exports/logs/exp_client_log.txt', DrmsLogLevel.DEBUG, formatter)

        if False:
            with Connection(server=arguments.server, listen_port=arguments.listen_port, timeout=15, log=log) as connection:
                # send test requests
                # 1. parse specification
                message = { 'request_type' : 'parse_specification', 'specification' : 'hmi.m_720s[2015.2.2]' }
                process_request(message, connection)

                # 2. series list
                message = { 'request_type' : 'series_list', 'series_regex' : 'm_720s', 'db_host' : 'hmidb' }
                process_request(message, connection)

                # 3. series info
                message = { 'request_type' : 'series_info', 'series' : 'hmi.v_45s', 'db_host' : 'hmidb2' }
                process_request(message, connection)

                # 4. record info
                message = { 'request_type' : 'record_info', 'specification' : 'hmi.m_720s[2015.2.2/96m]', 'keywords' : [ 't_rec', 't_sel' ], 'segments' : [ 'magnetogram' ], 'record_info' : False, 'number_records' : 128, 'db_host' : 'hmidb' }
                process_request(message, connection)

                # 5. premium export
                message = { 'request_type' : 'premium_export', 'address' : 'arta@sun.stanford.edu', 'specification' : 'hmi.v_720s[2015.3.12/24m]', 'method' : 'url', 'requestor' : 'art', 'processing' : None, 'file_format' : 'fits', 'number_records' : 1, 'db_host' : 'hmidb2' }
                process_request(message, connection)

                # 6. mini export
                message = { 'request_type' : 'mini_export', 'address' : 'arta@sun.stanford.edu', 'specification' : 'hmi.v_720s[2017.12.5/96m]', 'requestor' : 'art', 'file_name_format' : '{seriesname}.{recnum:%lld}.{segment}', 'number_records' : 2, 'db_host' : 'hmidb2' }
                process_request(message, connection)

                # 8. old request status
                message = { 'request_type' : 'export_status', 'address' : 'arta@sun.stanford.edu', 'request_id' : 'JSOC_20211019_1665', 'db_host' : 'hmidb2' }
                process_request(message, connection)

                # 9. bad request status
                message = { 'request_type' : 'export_status', 'address' : 'arta@sun.stanford.edu', 'request_id' : 'JSOC_20211203_007_IN', 'db_host' : 'hmidb2' }
                process_request(message, connection)

                # 10. good request status
                message = { 'request_type' : 'export_status', 'address' : 'arta@sun.stanford.edu', 'request_id' : 'JSOC_20211203_007_IN', 'db_host' : 'hmidb' }
                process_request(message, connection)

                # 11. tar request
                message = { 'request_type' : 'export_status', 'address' : 'arta@sun.stanford.edu', 'request_id' : 'JSOC_20211202_214_IN', 'db_host' : 'hmidb' }
                process_request(message, connection)

                # quit
                message = { 'request_type' : 'quit' }
                process_request(message, connection)

        # 7. streamed export
        destination = { 'header_extracted' : False, 'file_name' : None, 'reader' : read_from_connection, 'has_header' : True }

        connection = None
        close_connection = False

        connection = Connection(server=arguments.server, listen_port=arguments.listen_port, timeout=15, log=log).connect()
        destination['connection'] = connection

        message = { 'request_type' : 'streamed_export', 'address' : 'arta@sun.stanford.edu', 'specification' : 'hmi.v_720s[2015.8.5]', 'db_host' : 'hmidb2' }

        response = process_request(message, connection)
        export_status_dict = json_loads(response)

        if export_status_dict.get('export_server_status') == 'export_server_error':
            raise ExportServerError(error_message=f'{export_status_dict["error_message"]}')

        generator = create_generator(destination=destination)

        # create output file
        with open(path_join('/tmp', destination['file_name']), mode='wb') as file_out:
            for data in generator:
                file_out.write(data)

    except Exception as exc:
        log = Connection.get_log()
        if log:
            log.write_error([ f'{str(exc)}' ])
        else:
            print(f'{str(exc)}')
