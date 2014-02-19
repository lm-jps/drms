# This Python module contains utility functions used by all the replication code.

import re
import sys

def extractKeyVal(line, regexpParam):
    matchobj = regexpParam.match(line)
    return (matchobj.group(1), matchobj.group(2))

def expandVal(cfgDictTrans, key, val, regexpVarVal):
    valInt = val
    
    while True:
        matchobj = regexpVarVal.search(valInt)
        if not matchobj is None:
            valKey = matchobj.group(1)
            valInt = re.sub(r"\${.+?}", cfgDictTrans[valKey], valInt)
        else:
            # Modify the transitional dictionary (the original dictionary with SOME of the variable values
            # expanded. After the last instance of expandVal has run, cfgDictTrans will be equivalent
            # to cfgKeyVals in the calling code.
            cfgDictTrans[key] = valInt
            break

    return valInt

def getCfg(cfgFile, cfgDict):
    rv = 0
    
    regexpValLine = re.compile(r"^[^#][^=]+=")
    regexpParam = re.compile(r"^\s*(\w+)\s*=\s*(.*)")
    regexpVarVal = re.compile(r"\${(.+?)}") # greedy + qualifier
    
    cfgDictRaw = {}
    
    try:
        with open(cfgFile, mode='r', encoding='utf-8') as cin:
            cfgLines = cin.read().splitlines()
            cfgKeyValsRaw = list(filter(lambda line: regexpValLine.match(line) and regexpParam.match(line), cfgLines))
            for iKeyVal in cfgKeyValsRaw:
                keyVal = extractKeyVal(iKeyVal, regexpParam)
                cfgDictRaw[keyVal[0]] = keyVal[1]
    
    except IOError as exc:
        type, value, traceback = sys.exc_info()
        print(exc.strerror, file=sys.stderr)
        print('Unable to open ' + "'" + value.filename + "'.", file=sys.stderr)
        rv = bool(1)

    if (not rv):
        cfgDictTrans = cfgDictRaw.copy()
        cfgDictFinal = {key: expandVal(cfgDictTrans, key, val, regexpVarVal) for key, val in cfgDictRaw.items()}
        # cfgDictTrans == cfgDictFinal here.
        # The dictionary comprehension allocates a new object, and we cannot set cfgDict to this new object, otherwise
        # cfgDict would no longer refer to the original object defined in the calling code. We need to copy cfgDictFinal
        # to cfgDict.
        cfgDict.update(cfgDictFinal)


    return rv