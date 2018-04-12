#!/usr/bin/env python3

import sys
import os
import cgi
import json
import psycopg2
from subprocess import check_output, CalledProcessError
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../include'))
from drmsparams import DRMSParams

class SIException(Exception):
    def __init__(self, msg):
        super(SIException, self).__init__(msg)

def getAttributes(series, seriesns, cursor, jsonObj):
    # PG stores all timestamp with timezone column values in UTC
    sql = ''
    for aseries, aseriesns in zip(series, seriesns):
        if len(sql) > 0:
            sql += 'UNION\n'
        sql += "SELECT seriesname as series, author, owner, unitsize, archive, retention, tapegroup, primary_idx as drmsprimekey, dbidx, to_timestamp(created || ' UTC', 'YYYY-MM-DD HH24:MI:SS') AS created, description FROM " + aseriesns + '.drms_series WHERE lower(seriesname) = ' + "'" + aseries.lower() + "'" + '\n';

    try:
        print('executing sql:', file=sys.stderr)
        print(sql, file=sys.stderr)
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) == 0:
            errMsg = 'unknown DRMS data series ' + "'" + ','.join(series) + "'"
            raise SIException(errMsg)
        if not len(rows) == len(series):
            errMsg = 'unexpected number of rows returned for series ' + "'" + ','.join(series) + "'"
            raise SIException(errMsg)

        # put results in jsonObj
        for row in rows:
            if row[0].lower() not in jsonObj:
                jsonObj[row[0].lower()] = {}
                jsonObj[row[0].lower()]['attributes'] = {}
    
            jsonObj[row[0].lower()]['attributes']['series'] = row[0]
            jsonObj[row[0].lower()]['attributes']['author'] = row[1]
            jsonObj[row[0].lower()]['attributes']['owner'] = row[2]
            jsonObj[row[0].lower()]['attributes']['unitsize'] = row[3]
            jsonObj[row[0].lower()]['attributes']['archive'] = row[4]
            jsonObj[row[0].lower()]['attributes']['retention'] = row[5]
            jsonObj[row[0].lower()]['attributes']['tapegroup'] = row[6]
            jsonObj[row[0].lower()]['attributes']['drmsprimekey'] = [ key.strip() for key in row[7].split(',') ]
            jsonObj[row[0].lower()]['attributes']['dbindex'] = [ key.strip() for key in row[8].split(',') ]
            jsonObj[row[0].lower()]['attributes']['created'] = row[9].strftime('%Y-%m-%d %H:%M:%S UTC')
            jsonObj[row[0].lower()]['attributes']['description'] = row[10]
    except psycopg2.Error as exc:
        # handle database-command errors
        errMsg = exc.diag.message_primary
        raise SIException(errMsg)

def getKeyInfo(series, seriesns, keys, cursor, jsonObj):
    doAll = (len(keys) == 1 and keys[0] == '*')
    
    sql = ''
    for aseries, aseriesns in zip(series, seriesns):
        if len(sql) > 0:
            sql += 'UNION\n'
        sql += 'SELECT seriesname AS series, keywordname AS keyword, type AS datatype, defaultval AS constantvalue, unit, isconstant, (persegment >> 16)::integer AS rank, description FROM ' + aseriesns + '.drms_keyword WHERE lower(seriesname) = ' + "'" + aseries.lower() + "'"
    
        if not doAll:
            sql += ' AND lower(keywordname) IN (' + ','.join([ "'" + key.lower() + "'" for key in keys ]) + ')'

        sql += '\n'

    try:
        print('executing sql:', file=sys.stderr)
        print(sql, file=sys.stderr)
        cursor.execute(sql)
        seen = set()
        
        rows = cursor.fetchall()
        for row in rows:
            if row[5] == 1:
                constantValue = row[3]
            else:
                constantValue = 'na'
            
            if row[0].lower() not in jsonObj:
                jsonObj[row[0].lower()] = {}
            
            if 'keywords' not in jsonObj[row[0].lower()]:
                jsonObj[row[0].lower()]['keywords'] = {}
            
            jsonObj[row[0].lower()]['keywords'][row[1].lower()] = { 'data-type' : row[2], 'constant-value': constantValue, 'physical-unit' : row[4], 'rank' : row[6], 'description' : row[7] }
            seen.add(row[0].lower() + '::' + row[1].lower())
            
        # add elements for missing keywords
        if not doAll:
            for aseries in series:
                for key in keys:
                    if aseries.lower() + '::' + key.lower() not in seen:
                        if aseries.lower() not in jsonObj:
                            jsonObj[aseries.lower()] = {}

                        if 'keywords' not in jsonObj[aseries.lower()]:
                            jsonObj[aseries.lower()]['keywords'] = {}

                        jsonObj[aseries.lower()]['keywords'][key.lower()] = { 'data-type' : 'na', 'constant-value': 'na', 'physical-unit' : 'na', 'rank' : -1, 'description' : 'unknown keyword' }
                        seen.add(aseries.lower() + '::' + key.lower())
            
    except psycopg2.Error as exc:
        # handle database-command errors
        errMsg = exc.diag.message_primary, file=sys.stderr
        raise SIException(errMsg)

def getSegInfo(series, seriesns, segs, cursor, jsonObj):
    doAll = (len(segs) == 1 and segs[0] == '*')
    
    sql = ''
    for aseries, aseriesns in zip(series, seriesns):
        if len(sql) > 0:
            sql += 'UNION\n'
        sql += 'SELECT seriesname AS series, segmentname AS segment, type AS datatype, segnum, scope, naxis AS numaxes, axis AS dimensions, unit, protocol, description FROM ' + aseriesns + '.drms_segment WHERE lower(seriesname) = ' + "'" + aseries.lower() + "'"
    
        if not doAll:
            sql += ' AND lower(segmentname) IN (' + ','.join([ "'" + seg.lower() + "'" for seg in segs ]) + ')'

        sql += '\n'
        
    try:
        print('executing sql:', file=sys.stderr)
        print(sql, file=sys.stderr)
        cursor.execute(sql)
        seen = set()

        rows = cursor.fetchall()
        for row in rows:
            if row[0].lower() not in jsonObj:
                jsonObj[row[0].lower()] = {}
            
            if 'segments' not in jsonObj[row[0].lower()]:
                jsonObj[row[0].lower()]['segments'] = {}

            jsonObj[row[0].lower()]['segments'][row[1].lower()] = { 'data-type' : row[2], 'segment-number': row[3], 'scope' : row[4], 'number-axes' : row[5], 'dimensions' : row[6], 'physical-unit' : row[7], 'protocol' : row[8],'description' : row[9] }
            seen.add(row[0].lower() + '::' + row[1].lower())

        # add elements for missing segments
        if not doAll:
            for aseries in series:
                for seg in segs:
                    if aseries.lower() + '::' + seg.lower() not in seen:
                        if aseries.lower() not in jsonObj:
                            jsonObj[aseries.lower()] = {}

                        if 'segments' not in jsonObj[aseries.lower()]:
                                jsonObj[aseries.lower()]['segments'] = {}
                    
                        jsonObj[aseries.lower()]['segments'][seg.lower()] = { 'data-type' : 'na', 'segment-number': -1, 'scope' : 'na', 'number-axes' : -1, 'dimensions' : 'na', 'physical-unit' : 'na', 'protocol' : 'na','description' : 'unknown segment' }
                        seen.add(aseries.lower() + '::' + seg.lower())
            
    except psycopg2.Error as exc:
        # handle database-command errors
        errMsg = exc.diag.message_primary
        raise SIException(errMsg)

def getLinkInfo(series, seriesns, links, cursor, jsonObj):
    doAll = (len(links) == 1 and links[0] == '*')
    
    sql = ''
    for aseries, aseriesns in zip(series, seriesns):
        if len(sql) > 0:
            sql += 'UNION\n'
        sql += 'SELECT seriesname AS series, linkname AS link, target_seriesname as tail_series, type, description FROM ' + aseriesns + '.drms_link WHERE lower(seriesname) = ' + "'" + aseries.lower() + "'"
    
        if not doAll:
            sql += ' AND lower(linkname) IN (' + ','.join([ "'" + link.lower() + "'" for link in links ]) + ')'
            doAll = False

        sql += '\n'
    
    try:
        print('executing sql:', file=sys.stderr)
        print(sql, file=sys.stderr)
        cursor.execute(sql)
        seen = set()
                
        rows = cursor.fetchall()
        for row in rows:
            if row[0].lower() not in jsonObj:
                    jsonObj[row[0].lower()] = {}
            
            if 'links' not in jsonObj[row[0].lower()]:
                jsonObj[row[0].lower()]['links'] = {}
        
            jsonObj[row[0].lower()]['links'][row[1].lower()] = { 'tail-series' : row[2], 'type': row[2], 'description' : row[3] }
            seen.add(row[0].lower() + '::' + row[1].lower())
            
        # add elements for missing links
        if not doAll:
            for aseries in series:
                for link in links:
                    if aseries.lower() + '::' + link.lower() not in seen:
                        if aseries.lower() not in jsonObj:
                            jsonObj[aseries.lower()] = {}

                        if 'links' not in jsonObj[aseries.lower()]:
                                jsonObj[aseries.lower()]['links'] = {}
                    
                        jsonObj[aseries.lower()]['links'][link.lower()] = { 'tail-series' : 'na', 'type': 'na', 'description' : 'unknown link' }
                        seen.add(aseries.lower() + '::' + link.lower())
            
    except psycopg2.Error as exc:
        # handle database-command errors
        errMsg = exc.diag.message_primary
        raise SIException(errMsg)


if __name__ == "__main__":
    rv = 0

    try:
        optD = {}
        arguments = cgi.FieldStorage()
        jsonObj = {}

        if arguments:
            for key in arguments.keys():
                val = arguments.getvalue(key)

                if key in ('series', 'keys', 'segs', 'links'):
                    optD[key] = [ item.strip() for item in val.split(',') ]
                elif key in ('atts'):
                    if int(val) == 1:
                        optD['atts'] = True
                    else:
                        optD['atts'] = False
                elif key in ('DRMS_DBTIMEOUT'):
                    optD['dbtimeout'] = int(val)
                else:
                    errMsg = 'invalid argument ' + "'" + key + "'"
                    raise SIException(errMsg)
    

        drmsParams = DRMSParams()
        if drmsParams is None:
            errMsg = 'unable to read DRMS parameters'
            raise SIException(errMsg)

        if not 'series' in optD:
            errMsg = 'missing argument ' + "'series'"
            raise SIException(errMsg)
            
        if (not 'atts' in optD or not optD['atts']) and not 'keys' in optD and not 'segs' in optD and not 'links' in optD:
            errMsg = 'at least one argument of the arguments must be provided: atts, keys, segments, links'
            raise SIException(errMsg)

        # we'll need the series name checked and parsed
        binDir = drmsParams.get('BIN_EXPORT')
    
        # get architecture
        cmdList = [ os.path.join(binDir, '..', 'build', 'jsoc_machine.csh') ]

        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8')
        except ValueError as exc:
            errMsg = 'unable to call jsoc_machine.csh'
            raise SIException(errMsg)
        except CalledProcessError as exc:
            errMsg = 'jsoc_machine.csh returned non-zero status code ' + str(exc.returncode)
            raise SIException(errMsg)

        if output is None:
            errMsg = 'unexpected response from jsoc_machine.csh'
            raise SIException(errMsg)

        arch = output.splitlines()[0];
        print('architecture is ' + arch, file=sys.stderr)
    
        # parse series
        cmdList = [ os.path.join(binDir, arch, 'drms_parserecset'), 'spec=' + ','.join(optD['series']) ]
        if 'dbtimeout' in optD:
            cmdList.append('DRMS_DBTIMEOUT=' + str(optD['dbtimeout']))
        print('running ' + ' '. join(cmdList), file=sys.stderr)
    
        try:
            resp = check_output(cmdList)
            output = resp.decode('utf-8')
            jsonResp = json.loads(output)
        except ValueError as exc:
            errMsg = 'unable to call drms_parserecset'
            raise SIException(errMsg)
        except CalledProcessError as exc:
            errMsg = 'drms_parserecset returned non-zero status code ' + str(exc.returncode)
            raise SIException(errMsg)

        if jsonResp is None:
            errMsg = 'unexpected response from drms_parserecset'
            raise SIException(errMsg)
        elif 'errMsg' in jsonResp and jsonResp['errMsg'] is not None:            
            errMsg = jsonResp['errMsg']
            raise SIException(errMsg)
    
        # check for errors
        if 'hasfilts' in jsonResp and jsonResp['hasfilts']:
            errMsg = 'series argument must be a list of valid series'
            raise SIException(errMsg)
    
        seriesns = []
        seriestab = []
        
        print('drms_parserecset response: ', file=sys.stderr)
        print(json.dumps(jsonResp), file=sys.stderr)

        for seriesInfo in jsonResp['subsets']:
            if 'seriesns' in seriesInfo:
                seriesns.append(seriesInfo['seriesns'])
            if 'seriestab' in seriesInfo:
                seriestab.append(seriesInfo['seriestab'])
    
        with psycopg2.connect(database=drmsParams.get('DBNAME'), host=drmsParams.get('SERVER'), port=drmsParams.get('DRMSPGPORT')) as conn:
            with conn.cursor() as cursor:
                if 'atts' in optD:
                    getAttributes(optD['series'], seriesns, cursor, jsonObj)

                if 'keys' in optD:
                    getKeyInfo(optD['series'], seriesns, optD['keys'], cursor, jsonObj)

                if 'segs' in optD:
                    getSegInfo(optD['series'], seriesns, optD['segs'], cursor, jsonObj)

                if 'links' in optD:
                    getLinkInfo(optD['series'], seriesns, optD['links'], cursor, jsonObj)
                    
        jsonObj['errMsg'] = None
    except SIException as exc:
        jsonObj = {}
        if hasattr(exc, 'args'):
            jsonObj['errMsg'] = exc.args[0]
        else:
            jsonObj['errMsg'] = 'unknown error'

        rv = 1
    except Exception as exc:
        import traceback
        
        jsonObj = {}
        jsonObj['errMsg'] = traceback.format_exc(5)
        rv = 1
        
    jsonOut = json.dumps(jsonObj)
    print('json out:', file=sys.stderr)
    print(jsonOut, file=sys.stderr)
    print(jsonOut)
    sys.exit(rv)
