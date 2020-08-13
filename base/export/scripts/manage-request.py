#!/usr/bin/env python3

# The arguments to this script are parsed by cgi.FieldStorage(), which knows how to parse
# both HTTP GET and POST requests. A nice feature is that we can test the script as it runs in a CGI context
# by simply running on the command line with a single argument that is equivalent to an HTTP GET parameter string
# (e.g., address=gimli@mithril.com&addresstab=jsoc.export_addresses&domaintab=jsoc.export_addressdomains).

# Parameters:
#   address (required) - The email address to check or register.
#   addresstab (required) - The database table containing all registered (or registration-pending) email addresses.
#   domaintab (required) - The database table containing all email domains.
#   dbuser (optional) - The database account to be used when connecting to the database. The default is the value of the WEB_DBUSER parameter in DRMSParams.
#   checkonly (optional) - If set to 1, then no attept is made to register an unregistered email. In this case, if no error occurs then the possible return status codes are RV_REGISTEREDADDRESS, RV_REGISTRATIONPENDING, or RV_UNREGISTEREDADDRESS. The default is False (unknown addresses are registered).

import sys
import os
from datetime import datetime, timedelta
import json
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams, DPError, DPMissingParameterError
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))
from drmsCmdl import CmdlParser


# error codes
MR_ERROR_UNKNOWN = -1
MR_ERROR_PARAMETER = -2
MR_ERROR_ARGUMENT = -3
MR_ERROR_DB = -4
MR_ERROR_CHECK = -5
MR_ERROR_CANCEL = -6

# request statuses
MR_STATUS_UNKNOWN = 0
MR_STATUS_NOT_PENDING = 1
MR_STATUS_PENDING = 2
MR_STATUS_REQUEST_CANCELED = 3


# classes
class Arguments(object):

    def __init__(self, parser):
        # This could raise in a few places. Let the caller handle these exceptions.
        self.parser = parser

        # Parse the arguments.
        self.parse()

        # Set all args.
        self.set_all_args()

    def parse(self):
        try:
            self.parsed_args = self.parser.parse_args()
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


# exceptions
class ManageRequestError(Exception):
    _error_code = MR_ERROR_UNKNOWN

    def __init__(self, msg='generic error'):
        self._msg = msg
        self._header = None
        self._response = None

    def __str__(self):
        return self._msg

    def _generate_response(self):
        self._response = ErrorResponse(error_code=self._error_code, msg=self._header + ' ' + self._msg)

    @property
    def response(self):
        if self._response is None:
            self._generate_response()

        return self._response

class ParameterError(ManageRequestError):
    _error_code = MR_ERROR_PARAMETER

    def __init__(self, msg=None):
        super().__init__(msg=msg)

        self._header = '[ ParameterError ]'

class ArgumentError(ManageRequestError):
    _error_code = MR_ERROR_ARGUMENT

    def __init__(self, msg=None):
        super().__init__(msg=msg)

        self._header = '[ ArgumentError ]'

class DbError(ManageRequestError):
    _error_code = MR_ERROR_DB

    def __init__(self, msg=None):
        super().__init__(msg=msg)

        self._header = '[ DbError ]'
        self._response = super()._generate_response()

class CheckError(ManageRequestError):
    _error_code = MR_ERROR_CHECK

    def __init__(self, address='UNKNOWN_EXPORT_USER', msg=None):
        self._address = address

        super().__init__(msg='unable to check export request for export user ' + self._address)

        if msg is not None:
            self._msg += '(' + msg + ')'

        self._header = '[ CheckError ]'
        self._response = super()._generate_response()

class CancelError(ManageRequestError):
    _error_code = MR_ERROR_CANCEL

    def __init__(self, not_pending=False, address='UNKNOWN_EXPORT_USER', request_id='UNKNOWN_REQUEST_ID', start_time='UNKNOWN_START_TIME', msg=None):
        self._address = address
        self._request_id = request_id
        self._start_time = start_time

        if not_pending:
            # no error looking up address (the user has a pending request), but there was a problem deleting pending request
            super().__init__(msg='unable to cancel export request ' + self._address + ' [ request_id=' + request_id + ', start_time=' + start_time.strftime('%Y-%m-%d %T') + ' ]')
        else:
            # error looking up address (it is NOT the case that a look-up succeeded, but the address was not found)
            super().__init__(msg='unable to check export request for export user ' + self._address)

        if msg is not None:
            self._msg += '(' + msg + ')'

        self._header = '[ CancelError ]'
        self._response = super()._generate_response()


class OperationFactory(object):
    def __new__(cls, operation='UNKNOWN_OPERATION', address='UNKNOWN_EXPORT_USER', table='UNKNOWN_PENDING_REQUESTS_TABLE', timeout=timedelta(minutes=60)):
        if operation.lower() == CheckOperation._name:
            return CheckOperation(address, table, timeout)
        elif operation.lower() == CancelOperation._name:
            return CancelOperation(address, table, timeout)
        else:
            raise ArgumentError(msg='invalid operation type ' + operation)


# operations
class Operation(object):
    def __init__(self, address='UNKNOWN_EXPORT_USER', table='UNKNOWN_PENDING_REQUESTS_TABLE', timeout=timedelta(minutes=60)):
        self._address = address
        self._pending_requests_table = table
        self._timeout = timeout
        self._request_id = None
        self._start_time = None
        self._response = None

    def __str__(self):
        return self._name

    def process(self, cursor):
        cmd = 'SELECT request_id, start_time FROM ' + self._pending_requests_table + " WHERE address = '" + self._address + "' AND CURRENT_TIMESTAMP - start_time < interval '" + self._timeout + " minutes'"

        try:
            cursor.execute(cmd)
            rows = cursor.fetchall()
            if len(rows) > 1:
                raise DbError(msg='unexpected number of rows returned: ' + cmd)
        except psycopg2.Error as exc:
            # handle database-command errors
            import traceback
            raise DbError(msg=traceback.format_exc(8))

        if len(rows) != 0:
            self._request_id = rows[0][0]
            self._start_time = rows[0][1]

    @property
    def response(self):
        return self._response

class CheckOperation(Operation):
    _name = 'check'

    def __init__(self, address='UNKNOWN_EXPORT_USER', table='UNKNOWN_PENDING_REQUESTS_TABLE', timeout=timedelta(minutes=60)):
        super().__init__(address, table, timeout)

    def process(self, cursor):
        try:
            super().process(cursor)
        except DbError as exc:
            raise CheckError(address=self._address, msg=str(exc))

        if not self._request_id:
            self._response = NotPendingResponse(address=self._address)
        else:
            self._response = PendingResponse(address=self._address, request_id=self._request_id, start_time=self._start_time)

class CancelOperation(Operation):
    _name = 'cancel'

    def __init__(self, address='UNKNOWN_EXPORT_USER', table='UNKNOWN_PENDING_REQUESTS_TABLE', timeout=timedelta(minutes=60)):
        super().__init__(address, table, timeout)

    def process(self, cursor):
        # first run the Operation.process() code to obtain the request_id
        try:
            super().process(cursor)
        except DbError as exc:
            raise CancelError(not_pending=True, address=self._address, msg=str(exc))

        # then run the code to delete the pending request
        if not self._request_id:
            self._response = NotPendingResponse(address=self._address)
        else:
            cmd = 'DELETE FROM ' + self._pending_requests_table + " WHERE request_id = '" + self._request_id + "'"

            try:
                cursor.execute(cmd)
            except psycopg2.Error as exc:
                import traceback
                raise CancelError(not_pending=False, address=self._address, request_id=self._request_id, start_time=self._start_time, msg=traceback.format_exc(8))

            self._response = CancelResponse(address=self._address, request_id=self._request_id, start_time=self._start_time)


# responses
class Response(object):
    def __init__(self, error_code=MR_STATUS_UNKNOWN, msg=''):
        self._status = error_code
        self._msg = msg
        self._json_response = None

    def generate_json(self):
        if self._json_response is None:
            self._json_response = { "status" : str(self._status), "msg" : self._msg }
        return self._json_response

class ErrorResponse(Response):
    def __init__(self, error_code=None, msg=None):
        super().__init__(error_code=error_code, msg=msg)

class NotPendingResponse(Response):
    def __init__(self, address='UNKNOWN_EXPORT_USER'):
        super().__init__(error_code=MR_STATUS_NOT_PENDING, msg='no existing export request for export user ' + address)

class PendingResponse(Response):
    def __init__(self, address='UNKNOWN_EXPORT_USER', request_id='UNKNOWN_REQUEST_ID', start_time='UNKNOWN_START_TIME'):
        super().__init__(error_code=MR_STATUS_PENDING, msg='existing export request for export user ' + address + ' [ request_id=' + request_id + ', start_time=' + start_time.strftime('%Y-%m-%d %T') + ' ]')

class CancelResponse(Response):
    def __init__(self, address='UNKNOWN_EXPORT_USER', request_id='UNKNOWN_REQUEST_ID', start_time='UNKNOWN_START_TIME'):
        super().__init__(error_code=MR_STATUS_REQUEST_CANCELED, msg='existing export request for export user ' + address + ' [ request_id=' + request_id + ', start_time=' + start_time.strftime('%Y-%m-%d %T') + ' ] ' + 'was canceled')


if __name__ == "__main__":
    try:
        drms_params = DRMSParams()
        if drms_params is None:
            raise ParameterError(msg='unable to locate DRMS parameters file (drmsparams.py)')

        try:
            dbhost = drms_params.get_required('SERVER')
            dbport = drms_params.get_required('DRMSPGPORT')
            dbname = drms_params.get_required('DBNAME')
            dbuser = drms_params.get_required('WEB_DBUSER')
        except DPMissingParameterError as exc:
            raise ParameterError(str(exc))

        parser = CmdlParser(usage='%(prog)s address=<registered email address> operation=<check, cancel> [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --dbname=<db name> ] [ --dbuser=<db user>] ')
        parser.add_argument('A', 'address', help='the export-registered email address', metavar='<email address>', dest='address', required=True)
        parser.add_argument('O', 'operation', help='the export-request operation to perform (check, cancel)', metavar='<operation>', dest='op', default='check', required=True)
        parser.add_argument('-H', '--dbhost', help='the host machine of the database that is used to manage pending export requests', metavar='<db host>', dest='dbhost', default=dbhost)
        parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database', metavar='<db host port>', dest='dbport', default=dbport)
        parser.add_argument('-N', '--dbname', help='the name of the database used to manage pending export requests', metavar='<db name>', dest='dbname', default=dbname)
        parser.add_argument('-U', '--dbuser', help='the name of the database user account', metavar='<db user>', dest='dbuser', default=dbuser)

        arguments = Arguments(parser)

        try:
            operation = OperationFactory(operation=arguments.op, address=arguments.address, table=drms_params.EXPORT_PENDING_REQUESTS_TABLE, timeout=drms_params.EXPORT_PENDING_REQUESTS_TIME_OUT)
            resp = None

            try:
                with psycopg2.connect(host=arguments.dbhost, port=arguments.dbport, database=arguments.dbname, user=arguments.dbuser) as conn:
                    with conn.cursor() as cursor:
                        operation.process(cursor)
                        resp = operation.response
            except psycopg2.DatabaseError as exc:
                # Closes the cursor and connection
                import traceback
                raise DbError(msg='unable to connect to the database: ' + traceback.format_exc(8))
        except AttributeError as exc:
            raise ArgumentError(msg=str(exc))
    except ManageRequestError as exc:
        resp = exc.response

    json_response = resp.generate_json()

    # Do not print application/json here. This script may be called outside of a CGI context.
    print(json.dumps(json_response))

    # Always return 0. If there was an error, an error code (the 'status' property) and message (the 'statusMsg' property) goes in the returned HTML.
    sys.exit(0)
