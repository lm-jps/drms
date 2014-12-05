#!/usr/bin/env python

# The arguments to this script are parsed by cgi.FieldStorage(), which knows how to parse
# both HTTP GET and POST requests. A nice feature is that we can test the script as it runs in a CGI context
# by simply running on the command line with a single argument that is equivalent to an HTTP GET parameter string
# (e.g., address=gimli@mithril.com&addresstab=jsoc.export_addresses&domaintab=jsoc.export_addressdomains).

from __future__ import print_function
import sys
import os
import pwd
import uuid
from datetime import datetime
import smtplib
import cgi
import json
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

# Return values
RV_ERROR = -1
RV_ERROR_MAIL = -2
RV_ERROR_PARAMS = -3
RV_ERROR_DBCMD = -4
RV_ERROR_DBCONNECT = -5
RV_REGISTRATIONPENDING = 1
RV_REGISTEREDADDRESS = 2

def SendMail(localName, domainName, confirmation):
    subject = 'CONFIRM EXPORT ADDRESS [' + str(confirmation) + ']'
    fromAddr = 'jsoc@solarpost.stanford.edu'
    toAddrs = [ localName + '@' + domainName ]
    msg = 'From: jsoc@solarpost.stanford.edu\nTo: ' + ','.join(toAddrs) + '\nSubject: ' + subject + '\nThis message was automatically generated by the JSOC export system at Stanford.\n\nYou have requested that data be exported and have provided an email address that has not yet been registered. To complete the registration process, please reply to this message. Please do not modify the subject line when replying. The body of the reply will be ignored. You will receive another email message notifying you of the disposition of your registration.'

    try:
        server = smtplib.SMTP('solarpost.stanford.edu')
        server.sendmail(fromAddr, toAddrs, msg)
        server.quit()
    except Exception as exc:
        raise
        # If any exception happened, then the email message was not received.
        raise Exception('emailBadrecipient', 'Unable to send email message to address to confirm address.', RV_ERROR_MAIL)


if __name__ == "__main__":
    msg = None
    rv = RV_ERROR
    rootObj = {}

    try:
        optD = {}
        
        # Should be invoked as an HTTP POST. If this script is invoked via HTTP POST, then FieldStorage() will consume the arguments passed
        # via STDIN, and they will no longer be available to any program called by this script.
        arguments = cgi.FieldStorage()
        
        if arguments:
            for key in arguments.keys():
                val = arguments.getvalue(key)
                
                if key in ('address'):
                    optD['address'] = val
                elif key in ('addresstab'):
                    optD['addresstab'] = val
                elif key in ('domaintab'):
                    optD['domaintab'] = val
                elif key in ('dbuser'):
                    optD['dbuser'] = val
                else:
                    raise Exception('caArgs', 'Unrecognized program argument ' + key + '.', RV_ERROR_ARGS)
    
        if 'dbuser' not in optD:
            optD['dbuser'] = pwd.getpwuid(os.getuid())[0]

        drmsParams = DRMSParams()
        if drmsParams is None:
            raise Exception('drmsParams', 'Unable to locate DRMS parameters file (drmsparams.py).', RV_ERROR_PARAMS)

        localName, domainName = optD['address'].split('@')

        try:
            with psycopg2.connect(database=drmsParams.get('DBNAME'), user=optD['dbuser'], host=drmsParams.get('SERVER'), port=drmsParams.get('DRMSPGPORT')) as conn:
                with conn.cursor() as cursor:
                    cmd = 'SELECT A.confirmation FROM ' + optD['addresstab'] + ' AS A, ' + optD['domaintab'] + " AS D WHERE A.domainid = D.domainid AND A.localname = '" + localName + "' AND D.domainname = '" + domainName + "'"
                    
                    try:
                        cursor.execute(cmd)
                        rows = cursor.fetchall()
                        if len(rows) > 1:
                            raise Exception('dbCorruption', 'Unexpected number of rows returned: ' + cmd + '.', RV_ERROR_DBCMD)
                    except psycopg2.Error as exc:
                        # Handle database-command errors.
                        raise Exception('dbCmd', exc.diag.message_primary, RV_ERROR_DBCMD)
                    
                    if len(rows) == 0:
                        # Email address is not in our database. Register it.
                        # Send an email message out with a new confirmation code.
                        confirmation = uuid.uuid4()
                        SendMail(localName, domainName, confirmation)
                
                        # Insert a row in the domain table, if the domain does not exist.
                        cmd = 'SELECT domainid FROM ' + optD['domaintab'] + " WHERE lower(domainname) = '" + domainName.lower() + "'"
                        
                        try:
                            cursor.execute(cmd)
                            rows = cursor.fetchall()
                            if len(rows) > 1:
                                raise Exception('dbCorruption', 'Unexpected number of rows returned: ' + cmd + '.', RV_ERROR_DBCMD)
                    
                            if len(rows) == 0:
                                # The domain does not exist. Add the domain.
                                # Get next item in sequence.
                                cmd = "SELECT nextval('" + optD['domaintab'] + "_seq')"
                                cursor.execute(cmd)
                                rows = cursor.fetchall()
                                if len(rows) > 1:
                                    raise Exception('dbCorruption', 'Unexpected number of rows returned: ' + cmd + '.', RV_ERROR_DBCMD)
                                
                                domainid = rows[0][0]

                                cmd = 'INSERT INTO ' + optD['domaintab'] + '(domainid, domainname) VALUES(' + str(domainid) + ", '" + domainName.lower() + "')"
                                cursor.execute(cmd)
                    
                            # Insert a row into the addresses table.
                            starttime = datetime.now().strftime('%Y-%m-%d %T')
        
                            cmd = 'INSERT INTO ' + optD['addresstab'] + "(localname, domainid, confirmation, starttime) VALUES('" + localName + "', " + str(domainid) + ", '" + str(confirmation) + "', '" + starttime + "')"
                            cursor.execute(cmd)
                        except psycopg2.Error as exc:
                            # Handle database-command errors.
                            raise Exception('dbCmd', exc.diag.message_primary, RV_ERROR_DBCMD)
                
                        rv = RV_REGISTRATIONPENDING
                        msg = 'Email address is not registered. Starting registration process. You will receive an email address from user jsoc. Please reply to this email message without modifying the subject. The body will be ignored.'
                    else:
                        # Email address is in our database. Check to see if registration is pending.
                        confirmation = rows[0][0]
                        
                        if confirmation is None or len(confirmation) == 0:
                            # Email address in our database is registered.
                            rv = RV_REGISTEREDADDRESS
                            msg = 'Email address is valid and registered.'
                        else:
                            # We are in the process of registering this address already.
                            rv = RV_REGISTRATIONPENDING
                            msg = 'Email-address registration is pending. Please wait and try again later.'
        except psycopg2.DatabaseError as exc:
            # Closes the cursor and connection
            
            # Man, there is no way to get an error message from any exception object that will provide any information why
            # the connection failed.
            raise Exception('dbConnect', 'Unable to connect to the database.', RV_ERROR_DBCONNECT)
    except Exception as exc:
        if len(exc.args) != 3:
            raise # Re-raise

        etype = exc.args[0]

        if etype == 'emailBadrecipient' or etype == 'caArgs' or etype == 'drmsParams' or etype == 'dbCorruption' or etype == 'dbCmd' or etype == 'dbConnect':
            msg = exc.args[1]
            rv = exc.args[2]
        else:
            raise # Re-raise

    if rv != RV_REGISTEREDADDRESS and rv != RV_REGISTRATIONPENDING:
        if msg is None:
            msg = 'Unknown error'

    rootObj['status'] = rv
    rootObj['msg'] = msg

    print('Content-type: application/json\n')
    print(json.dumps(rootObj))

    # Always return 0. If there was an error, an error code (the 'status' property) and message (the 'statusMsg' property) goes in the returned HTML.
    sys.exit(0)
