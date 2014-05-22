#!/home/jsoc/bin/linux_x86_64/activepython

import sys
import os.path
import pwd
import re
import json
import psycopg2
from subprocess import check_output, CalledProcessError
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../libs/py'))
from drmsCmdl import CmdlParser

# Return codes
RET_SUCCESS = 0
RET_INVALIDARG = 1
RET_DB = 2
RET_SQL = 3
RET_CMD = 4

def GetArgs():
    istat = bool(0)
    optD = {}
    
    parser = CmdlParser(usage='%(prog)s [ -h ] [ -d ] series=<series> spec=<keyword specification> map=<keyword specification> [ --dbname=<db name> ] [ --dbhost=<db host> ] [ --dbport=<db port> ] [ --dbuser=<db user> ]')
    
    parser.add_argument('s', 'series', '--series', help='The series whose keywords are to be modified.', metavar='<series>', dest='series', required=True)
    parser.add_argument('c', 'spec', '--spec', help='Text that contains the JSD specification of the keywords to be modified', metavar='<keyword specification>', dest='spec', required=True)
    parser.add_argument('-m', '--map', help='A map from the old keyword name to the new keyword name (old1:new1,old2:new2,...)', metavar='<new-old-key map>', dest='map')
    parser.add_argument('-d', '--doit', help='If provided, the modifications are perfomed. Otherwise, the SQL to be executed is printed and no modifications are performed.', dest='doit', default=False, action='store_true')
    parser.add_argument('-N', '--dbname', help='The name of the database to which modifications are to be made.', metavar='<db name>', dest='dbname', default='jsoc')
    parser.add_argument('-H', '--dbhost', help='The host machine of the database to which modifications are to be made.', metavar='<db host machine>', dest='dbhost', default='hmidb')
    parser.add_argument('-P', '--dbport', help='The port on the host machine that is accepting connections for the database to which modifications are to be made.', metavar='<db host port>', dest='dbport', default='5432')
    parser.add_argument('-U', '--dbuser', help='The user account name for the database to which modifications are to be made.', metavar='<db user>', dest='dbuser')
    
    try:
        args = parser.parse_args()
    
    except Exception as exc:
        if len(exc.args) == 2:
            type = exc.args[0]
            msg = exc.args[1]
            
            if type != 'CmdlParser-ArgUnrecognized' and type != 'CmdlParser-ArgBadformat':
                raise # Re-raise
            
            print(msg, file=sys.stderr)
            istat = bool(1)
        
        else:
            raise # Re-raise
    
    if not istat:
        optD['series'] = args.series       
        # Could be a file path, or could be an actual JSD specification.
        optD['spec'] = args.spec

        if args.map:
            # Store in a dictionary - the key is the lower-case old name, and the value is the mixed-case new name.
            optD['map'] = {}
            elems = args.map.split(',')
            regexp = re.compile(r"([^:]+):([^:]+)")
            for ielem in elems:
                matchobj = regexp.match(ielem)
                if matchobj:
                    optD['map'][matchobj.group(1).lower()] = matchobj.group(2)
        optD['dbname'] = args.dbname
        optD['dbhost'] = args.dbhost
        optD['dbport'] = args.dbport
        if args.dbuser:
            optD['dbuser'] = args.dbuser
        else:
            optD['dbuser'] = pwd.getpwuid(os.getuid())[0]
        optD['doit'] = args.doit
    
    return optD

def getNewKeyName(oldKeyName, oldKeyNewKey):
    if oldKeyNewKey:
        return oldKeyNewKey[oldKeyName.lower()]
    else:
        return oldKeyName

def getUpdateColStr(keyDict, colName, sofar, oldKeyNewKey=None):
    if colName in keyDict and keyDict[colName] and len(keyDict[colName]) > 0:
        if oldKeyNewKey:
            keyVal = getNewKeyName(keyDict[colName], oldKeyNewKey)
        else:
            keyVal = keyDict[colName]
        
        if len(sofar) > 0:
            sofar.extend(', ' + colName + " = '" + keyVal + "'")
        else:
            sofar.extend(colName + " = '" + keyVal + "'")
    else:
        if len(sofar) > 0:
            sofar.extend(', ' + colName + " = ''")
        else:
            sofar.extend(colName + " = ''")

rv = RET_SUCCESS

# Parse arguments
if __name__ == "__main__":
    optD = GetArgs()
    
    if not optD:
        rv = RET_INVALIDARG
    else:
        series = optD['series']
        spec = optD['spec']
        oldKeyNewKey = optD['map']
        dbuser = optD['dbuser']
        dbname = optD['dbname']
        dbhost = optD['dbhost']
        dbport = optD['dbport']
        doit = optD['doit']

if rv == RET_SUCCESS:
    cmd = 'drms_parsekeys series=' + series + ' spec=' + spec
    try:
        ret = check_output(cmd, shell=True)
        keyInfoJSON = ret.decode("utf-8")
    except ValueError:
        print('Unable to run cmd: ' + cmd + '.')
        rv = RET_CMD
    except CalledProcessError:
        print('Command ' + "'" + cmd + "'" + ' ran improperly.')
        rv = RET_CMD

if rv == RET_SUCCESS:
    completeSql = []
    keyInfo = json.loads(keyInfoJSON)
    regex = re.compile(r"[^.]+[.]drms_keyword")
    for seriesName in keyInfo:
        for table in keyInfo[seriesName]:
            matchobj = regex.match(table)
            if matchobj is not None:
                # Process <ns>.drms_keyword table
                # keyInfo[seriesName][table] is an array of one-element dictionaries
                for keyDesc in keyInfo[seriesName][table]:
                    # keyDesc is a dictionary with one element
                    keyName, keyDict = keyDesc.popitem()
                    updateCols = []
                    
                    getUpdateColStr(keyDict, 'seriesname', updateCols)
                    getUpdateColStr(keyDict, 'keywordname', updateCols, oldKeyNewKey)
                    getUpdateColStr(keyDict, 'linkname', updateCols)
                    getUpdateColStr(keyDict, 'targetkeyw', updateCols)
                    getUpdateColStr(keyDict, 'type', updateCols)
                    getUpdateColStr(keyDict, 'defaultval', updateCols)
                    getUpdateColStr(keyDict, 'format', updateCols)
                    getUpdateColStr(keyDict, 'unit', updateCols)
                    getUpdateColStr(keyDict, 'islink', updateCols)
                    getUpdateColStr(keyDict, 'isconstant', updateCols)
                    getUpdateColStr(keyDict, 'persegment', updateCols)
                    getUpdateColStr(keyDict, 'description', updateCols)

                    sql = 'UPDATE ' + table + ' SET ' + ''.join(updateCols) + " WHERE lower(seriesname) = '" + keyDict['seriesname'].lower() + "' AND lower(keywordname) = '" + keyDict['keywordname'].lower() + "';"
        
                    completeSql.append(sql)

            else:
                # Process "series" table
                # keyInfo[seriesName][table] is an array of one-element dictionaries
                for keyDesc in keyInfo[seriesName][table]:
                    # keyDesc is a dictionary with one element
                    colName, colType = keyDesc.popitem()

                    sql = 'ALTER TABLE ' + table + ' RENAME COLUMN ' + colName.lower() + ' TO ' + getNewKeyName(colName, oldKeyNewKey).lower() + ';'

                    completeSql.append(sql)

if rv == RET_SUCCESS:
    if doit:
        print('Executing SQL: ' + '\n'.join(completeSql))

        # Connect to the database
        try:
            # The connection is NOT in autocommit mode.
            with psycopg2.connect(database=dbname, user=dbuser, host=dbhost, port=dbport) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('\n'.join(completeSql))

        except psycopg2.Error as exc:
            # Closes the cursor and connection
            print(exc.diag.message_primary, file=sys.stderr)
    
            # No need to close cursor - leaving the with block does that.
            rv = RET_DB

        # There is no need to call conn.commit() since connect() was called from within a with block. If an exception was not raised in the with block,
        # then a conn.commit() was implicitly called. If an exception was raised, then conn.rollback() was implicitly called.
    else:
        print('NOT executing SQL: ' + '\n'.join(completeSql))

sys.exit(rv)