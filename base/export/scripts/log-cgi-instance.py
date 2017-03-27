#!/usr/bin/env python3

import sys

if sys.version_info < (3, 5):
    print('log-cgi-instance: requires the 3.5 release of Python.', file=sys.stderr)
    sys.exit(1)

import psycopg2
from datetime import datetime, timedelta, timezone

CGI_REQUESTS_TABLE = 'jsoc.cgi_requests'

# collect args in this order
if len(sys.argv) != 6:
    print('log-cgi-instance: wrong number of arguments', file=sys.stderr)
    sys.exit(1)

# script
script = sys.argv[1]

# domain
domain = sys.argv[2]

# url
url = sys.argv[3]

# http method
method = sys.argv[4]

# ip address
ip = sys.argv[5]

instanceID = ''
try:
    with psycopg2.connect(host='hmidb2', port='5432', database='jsoc', user='apache') as conn:
        with conn.cursor() as cursor:
            # insert row for this request
            cmd = 'INSERT INTO ' + CGI_REQUESTS_TABLE + '(script, domain, url, http_method, ip_address, date) VALUES (' + "'" + script + "'," + "'" + domain + "'," + "'" + url + "'," + "'" + method + "'," + "'" + ip + "', current_timestamp" + ')'
            cursor.execute(cmd)
            
            # fetch instance ID
            cmd = 'SELECT currval(pg_get_serial_sequence(' + "'" + CGI_REQUESTS_TABLE + "'" + ',' + "'instance_id'" + '))';
            cursor.execute(cmd);
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise Exception()
                
            instanceID = rows[0][0]
except:
    import traceback
    print(traceback.format_exc(1), file=sys.stderr)
    print(instanceID, file=sys.stdout)
    sys.exit(1)
        
# return cgi instance id
print(instanceID, file=sys.stdout)
sys.exit(0)

