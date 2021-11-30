from json import dumps as json_dumps

from export_socket_server import Connection, get_arguments, get_message, send_message

def process_request(request, connection):
    json_message = json_dumps(request)

    print(f'sending message to server:')
    print(f'{json_message}')
    send_message(connection, json_message)

    message = get_message(connection)
    print(f'server response:')
    print(f'{message}')

if __name__ == "__main__":
    try:
        test_module_args = { 'log_file' : '/home/jsoc/exports/logs/exp_client_log.txt', 'logging_level' : 'info' }
        arguments = get_arguments(is_program=False, module_args=test_module_args)

        with Connection(server=arguments.server, listen_port=arguments.listen_port, log_file=arguments.log_file, logging_level=arguments.logging_level) as connection:
            # send test requests
            # 1. parse specification
            message = { 'request_type' : 'parse_specification', 'specification' : 'hmi.m_720s[2015.2.2]' }
            process_request(message, connection)

            # 2. series info
            message = { 'request_type' : 'series_info', 'series' : 'hmi.v_45s' }
            process_request(message, connection)

            # 3. record info
            message = { 'request_type' : 'record_info', 'specification' : 'hmi.m_720s[2015.2.2/96m]', 'keywords' : [ 't_rec', 't_sel' ], 'segments' : [ 'magnetogram' ], 'record_info' : False, 'number_records' : 128 }
            process_request(message, connection)

            # 4. premium export
            message = { 'request_type' : 'premium_export', 'specification' : 'hmi.v_720s[2015.3.2]' }

            # 5. mini export
            message = { 'request_type' : 'mini_export', 'specification' : 'hmi.v_720s[2015.3.5]' }

            # 6. streamed export
            message = { 'request_type' : 'streamed_export', 'specification' : 'hmi.v_720s[2015.8.5]' }

            # 7. request status
            message = { 'request_type' : 'export_status', 'address' : 'arta@sun.stanford.edu', 'request_id' : 'JSOC_20211019_1665' }
            process_request(message, connection)

            # quit
            message = { 'request_type' : 'quit' }
            process_request(message, connection)
    except Exception as exc:
        log = Connection.get_log()
        if log:
            log.write_error([ f'{str(exc)}' ])
        else:
            print(f'{str(exc)}')
