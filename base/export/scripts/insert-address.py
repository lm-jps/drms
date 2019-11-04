#!/usr/bin/env python3

# system packages
from datetime import datetime
from email_validator import validate_email, EmailNotValidError
import os
import psycopg2
import pwd
import sys

# local packages
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../base/libs/py'))

from drmsCmdl import CmdlParser
from drmsparams import DRMSParams

class InsertAddressError(Exception):
    '''
    base exception class for all Insert Address exceptions
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg
    
class AddressSyntaxError(InsertAddressError):
    '''
    invalid email address syntax
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class DBResponseError(InsertAddressError):
    '''
    unexcpected DB response
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg

class DBQueryError(InsertAddressError):
    '''
    error in SQL query
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg
        
class ParameterError(InsertAddressError):
    '''
    invalid DRMS parameter
    '''
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


def get_DRMS_param(drms_params, param):
    rv = drms_params.get(param)
    if not rv:
        raise ParameterError('DRMS parameter {param} is not defined'.format(param=param))
    
    return rv

if __name__ == "__main__":
    drms_params = DRMSParams()
    parser = CmdlParser(usage='%(prog)s [ -h ] address=<email address> [ --dbname=<db name> ] [ --dbuser=<db user> ] [ --dbhost=<db host> ][ dbport=<db port> ]')

    # required
    parser.add_argument('address', help='the email address to insert into the table of registered email addresses', metavar='<email address>', dest='address', required=True)
    
    # optional
    parser.add_argument('-N', '--dbname', help='the name of the database serving email addresses', metavar='<db name>', dest='dbname', default=get_DRMS_param(drms_params, 'DBNAME'))
    parser.add_argument('-U', '--dbuser', help='the user to log-in to the serving database as', metavar='<db user>', dest='dbuser', default=pwd.getpwuid(os.getuid())[0])
    parser.add_argument('-H', '--dbhost', help='the machine hosting the EXTERNAL database that serves email addresses', metavar='<db host>', dest='dbhost', default=drms_params.get('SERVER'))
    parser.add_argument('-P', '--dbport', help='the port on the machine hosting DRMS data series names', metavar='<db host port>', dest='dbport', default=drms_params.get('DRMSPGPORT'))    
    parser.add_argument('-D', '--domain-table', help='the database table of email-address domains', metavar='<db address domain table>', dest='domain_table', default='jsoc.export_addressdomains')
    parser.add_argument('-A', '--address-table', help='the database table of email addresses', metavar='<db address table>', dest='address_table', default='jsoc.export_addresses')

    args = parser.parse_args()

    # do a quick validation on the email address while parsing out local and domain names
    try:
        email_parts = validate_email(args.address)
        local_name = email_parts['local']
        domain_name = email_parts['domain']
    except EmailNotValidError as exc:
        # email is not valid, exception message is human-readable
        raise AddressSyntaxError('not a valid email address: {addr}'.format(addr=address))

    with psycopg2.connect(database=args.dbname, user=args.dbuser, host=args.dbhost, port=args.dbport) as conn:
        with conn.cursor() as cursor:
            # insert a row in the domain table, if the domain does not exist, and a row into the address table, if it the address not exist                            
            try:
                cmd = "SELECT domainid FROM {d_table} WHERE lower(domainname) = '{d_name}'".format(d_table=args.domain_table, d_name=domain_name.lower())                
                cursor.execute(cmd)
                rows = cursor.fetchall()

                if len(rows) == 0:
                    # the domain does not exist in the db; add it
                    cmd = "SELECT nextval('{d_table}_seq')".format(d_table=args.domain_table)
                    cursor.execute(cmd)
                    rows = cursor.fetchall()
                    if len(rows) > 1:
                        raise DBResponseError('unexpected number of rows returned: {cmd}'.format(cmd=cmd))
            
                    domain_id = str(rows[0][0])

                    cmd = "INSERT INTO {d_table}(domainid, domainname) VALUES({d_id}, '{d_name}')".format(d_table=args.domain_table, d_id=domain_id, d_name=domain_name)
                    cursor.execute(cmd)
                else:
                    # the domain does exist
                    domain_id = rows[0][0]

                start_time = datetime.now().strftime('%Y-%m-%d %T')

                # will raise if there is an attempt to insert a duplicate (because the primary key is (localname, domainid))
                cmd = "INSERT INTO {a_table} (localname, domainid, starttime) VALUES('{l_name}', {d_id}, '{s_time}')".format(a_table=args.address_table, l_name=local_name, d_id=domain_id, s_time=start_time)
                cursor.execute(cmd)
            except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as exc:
                raise DBQueryError(str(exc))
            
sys.exit(0)
