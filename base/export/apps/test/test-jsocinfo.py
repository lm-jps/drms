#!/usr/bin/env python3

import sys
import random
import urllib.request
import urllib.parse
import json
from jsondiff import diff
import difflib

ALL_OPS = [ 'series_struct', 'rs_summary', 'rs_list' ]

class ObjectView(object):
    def __init__(self, dictIn):
        if isinstance(dictIn, dict):
            for k,v in dictIn.items():
                if isinstance(v, dict):
                    self.__dict__[k] = ObjectView(v)
                elif hasattr(v, '__iter__') and not hasattr(v, 'strip'):
                    # a sequence, but not a string (which is a sequence)
                    sequence = type(v)()
                    for item in v:
                        if isinstance(item, dict):
                            sequence.append(ObjectView(item))
                        else:
                            sequence.append(item)
                        
                    self.__dict__[k] = sequence                    
                else: 
                    self.__dict__[k] = v

if __name__ == "__main__":
    newUrl = sys.argv[1]
    oldUrl = sys.argv[2]
    dataSet = sys.argv[3]
    
    # get series
    url = 'http://jsoc.stanford.edu/cgi-bin/drms_parserecset'
    data = urllib.parse.urlencode([ ('spec', dataSet) ])
    request = urllib.request.Request(url + '?' + data) # ugh, cannot do a GET if data is a separate argument; ugh
    with urllib.request.urlopen(request) as response:
        responseJsonStr = response.read().decode('UTF-8')
        responseJsonDict = json.loads(responseJsonStr)
        responseJson = ObjectView(responseJsonDict)
        series = responseJson.subsets[0].seriesname
        
    # get some random keys, segs, links
    url = 'http://jsoc.stanford.edu/cgi-bin/ajax/jsoc_info'
    data = urllib.parse.urlencode([ ('op', 'series_struct'), ('ds', series) ])
    request = urllib.request.Request(url + '?' + data) # ugh, cannot do a GET if data is a separate argument; ugh
    with urllib.request.urlopen(request) as response:
        responseJsonStr = response.read().decode('UTF-8')
        responseJsonDict = json.loads(responseJsonStr)
        responseJson = ObjectView(responseJsonDict)
    
    for iter in range(100):
        op = random.choice(ALL_OPS)
    
        if len(responseJson.keywords) > 0:
            keywords = random.sample(responseJson.keywords, random.randrange(len(responseJson.keywords) + 1))
        else:
            keywords = None
            
        if len(responseJson.segments) > 0:
            segments = random.sample(responseJson.segments, random.randrange(len(responseJson.segments) + 1))
        else:
            segments = None
            
        if len(responseJson.links) > 0:            
            links = random.sample(responseJson.links, random.randrange(len(responseJson.links) + 1))
        else:
            links = None
        
        followLinks = random.choice([ '0', '1' ])
        recordLimit = random.choice([ '0', '20', '-20' ])
        showSpec = random.choice([ '0', '1' ])
        printJson = random.choice([ '0', '1' ])
        verbose = random.choice([ '0', '1' ])
    
        # generate test url        
        dataList = [ ('op', op), ('ds', dataSet), ('l', followLinks), ('n', recordLimit), ('R', showSpec), ('z', printJson), ('o', verbose) ]
        
        if keywords:
            dataList.append(('key', ','.join(key.name for key in keywords)))
        if segments:
            dataList.append(('seg', ','.join(seg.name for seg in segments)))
        if links:
            dataList.append(('link', ','.join(link.name for link in links)))

        data = urllib.parse.urlencode(dataList)
        
        url = newUrl
        request = urllib.request.Request(url + '?' + data)
        print(request.get_full_url())
        with urllib.request.urlopen(request) as response:
            responseJsonStrNew = response.read().decode('UTF-8')
        responseJsonFormattedStrNew = json.dumps(json.loads(responseJsonStrNew), indent=4)
            
        url = oldUrl
        request = urllib.request.Request(url + '?' + data)
        with urllib.request.urlopen(request) as response:
            responseJsonStrOld = response.read().decode('UTF-8')
        responseJsonFormattedStrOld = json.dumps(json.loads(responseJsonStrOld), indent=4)
        
        unifiedDiff = difflib.unified_diff(responseJsonFormattedStrOld.splitlines(keepends=True), responseJsonFormattedStrNew.splitlines(keepends=True), fromfile='ORIGINAL', tofile='NEW', n=0)
        diffOut = ''.join(unifiedDiff)
        if len(diffOut) > 0:        
            print(request.get_full_url())
            print(diffOut)
