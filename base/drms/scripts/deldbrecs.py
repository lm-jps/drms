#!/home/jsoc/bin/linux_x86_64/activepython

import sys
import os.path
import pwd
import re
import psycopg2
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../libs/py'))
from drmsCmdl import CmdlParser

# Return codes
RET_SUCCESS = 0
RET_INVALIDARG = 1
RET_DBCONNECT = 2
RET_SQL = 3

# Read arguments
# (s)table      - The series table from which we are deleting records.
# (r)ecnums     - The list of recnums that specify the db records to delete from seriestable.
#                 The format maybe a single recnum, a list of comma-separated recnums, or a file
#                 containing a list of newline-separated recnums.
# db(n)ame      - The name of the database that contains the series table from which we are deleting
#                 records.
# db(H)ost      - The host machine of the database that contains the series table from which we
#                 are deleting records.
# db(p)ort      - The port on the host machine that is accepting connections for the database that
#                 contains the series table from which we are deleting records.
# (d)oit        - If set, then the script will execute the SQL with the delete statement in it.
#                 Otherwise, the script will merely print out the SQL commands it would otherwise
#                 execute.
        

def GetArgs():
    istat = bool(0)
    optD = {}

    parser = CmdlParser(usage='%(prog)s [ -h ] [ -d ] stable=<series> recnums=<recnum list> [ --dbname=<db name> ] [ --dbhost=<db host> ] [ --dbport=<db port> ]')

    parser.add_argument('s', 'stable', '--stable', help='The series table from which records are to be deleted.', metavar='<series>', dest='stable', required=True)
    parser.add_argument('r', 'recnums', '--recnums', help='The list of recnums that specify the db records to delete from seriestable. The format maybe a single recnum, a list of comma-separated recnums, or a file containing a list of newline-separated recnums.', metavar='<recnum list or file>', dest='recnums', required=True)
    parser.add_argument('-d', '--doit', help='If provided, the deletions are perfomed. Otherwise, the SQL to be executed is printed and no deletions are performed.', dest='doit', default=False, action='store_true')
    parser.add_argument('-N', '--dbname', help='The name of the database that contains the series table from which records are to be deleted.', metavar='<db name>', dest='dbname', default='jsoc')
    parser.add_argument('-H', '--dbhost', help='The host machine of the database that contains the series table from which records are to be deleted.', metavar='<db host machine>', dest='dbhost', default='hmidb')
    parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database that contains the series table from which records are to be deleted.', metavar='<db host port>', dest='dbport', default='5432')
    
    try:
        args = parser.parse_args()
    
    except Exception as exc:
        if len(exc.args) == 2:
            type = exc[0]
            msg = exc[1]

            if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                raise # Re-raise
                    
            print(msg, file=sys.stderr)
            istat = bool(1)

        else:
            raise # Re-raise
    
    if not istat:
        optD['stable'] = args.stable
        
        if os.path.isfile(args.recnums):
            optD['recnums'] = []
            try:
                regexpRecnum = re.compile(r"\s*\d+\s*")
                with open(args.recnums, 'r') as fin:
                    while True:
                        recnumsRaw = fin.readlines(8192)
                        if not recnumsRaw:
                            break
                        recnumsNotEmpty = list(filter(lambda oneRecnum:  regexpRecnum.match(oneRecnum), recnumsRaw))
                        recnums = [recnum.strip(' \t\n,') for recnum in recnumsNotEmpty]
                        optD['recnums'].extend(recnums)
            except IOError as exc:
                type, value, traceback = sys.exc_info()
                print(exc.strerror, file=sys.stderr)
                print('Unable to open ' + "'" + value.filename + "'.", file=sys.stderr)
                istat = bool(1)
        else:
            # Otherwise, parse the argument itself.
            optD['recnums'] = args.recnums.split(',') # a list

        optD['dbname'] = args.dbname
        optD['dbhost'] = args.dbhost
        optD['dbport'] = args.dbport
        optD['doit'] = args.doit
        
    return optD

rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = GetArgs()
    
    if not optD:
        rv = RET_INVALIDARG
    else:
        series = optD['stable']
        recnums = optD['recnums']
        dbuser = pwd.getpwuid(os.getuid())[0]
        dbname = optD['dbname']
        dbhost = optD['dbhost']
        dbport = optD['dbport']
        doit = optD['doit']

if rv == RET_SUCCESS:
    # Connect to the database
    try:
        # The connection is NOT in autocommit mode. If changes need to be saved, then conn.commit() must be called.
        with psycopg2.connect(database=dbname, user=dbuser, host=dbhost, port=dbport) as conn:
            with conn.cursor() as cursor:
                if doit:
                    cursor.execute('PREPARE preparedstatement AS DELETE FROM ' + series + ' WHERE recnum in ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)')
                else:
                    print('PREPARE preparedstatement AS DELETE FROM ' + series + ' WHERE recnum in ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)')
                reclist = list()
                
                for recnum in recnums:
                    reclist.append(recnum)
                    if len(reclist) == 16:
                        if doit:
                            cursor.execute('EXECUTE preparedstatement (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', reclist)
                        else:
                            print('EXECUTE preparedstatement (' + ','.join(reclist) + ')')
                        reclist = list()

                if len(reclist) > 0:
                    # Unprocessed recnums (because len(recnums) was not a multiple of 16).
                    # Pad reclist with recnum = -1
                    for irec in range(16 - len(reclist)):
                        reclist.append('-1')
                    
                    if doit:
                        cursor.execute('EXECUTE preparedstatement (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', reclist)
                    else:
                        print('EXECUTE preparedstatement (' + ','.join(reclist) + ')')
                    reclist = list()

    except psycopg2.Error as exc:
        # Closes the cursor and connection
        print(exc.diag.message_primary, file=sys.stderr)
        # No need to close cursor - leaving the with block does that.
        if not conn:
            rv = RET_DBCONNECT
        else:
            rv = RET_SQL

    # There is no need to call conn.commit() since connect() was called from within a with block. If an exception was not raised in the with block,
    # then a conn.commit() was implicitly called. If an exception was raised, then conn.rollback() was implicitly called.

sys.exit(rv)

