#!/bin/bash

echo 'Configuring NetDRMS. You must have already created config.local and you must have csh installed to run configure.'
if [ ! -e config.local ]
then
    echo 'config.local not found - please use config.local.template as a template and create config.local.'
    exit 1
else
    echo 'Please save config.local somewhere safe. If you update your NetDRMS to a newer version, you will need to copy config.local into the new source tree, and possibly add new parameters.'
fi

# Run configure
./configure
if [ $? -ne 0 ]
then
    echo 'configure did not run properly.'
    exit 1;
fi

# Ensure that the localization directory exists and contains the Python DRMS parameter file needed by all subsequently run Python scripts.
LOCALIZATIONDIR=`egrep "^LOCALIZATIONDIR" config.local | awk '{ print $2 }'`
if [ ! -z $LOCALIZATIONDIR ]
then
    locDir="$LOCALIZATIONDIR"
else
    locDir='localization'
fi

if [ ! -e "$locDir"'/drmsparams.sh' ]
then
    echo 'Parameter file '"$locDir"'/drmsparams.sh does not exist.'
    exit 1
fi

source "$locDir"'/drmsparams.sh'

# Check Python binary path and script file
if [ -z $BIN_PY3 ]
then
    'Missing required config.local parameter BIN_PY3'
    exit 1
fi

py3Bin=`readlink -e "$BIN_PY3"`
if [ ! -x "$py3Bin" ]
then
    echo "Unable to execute $py3Bin."
fi

if [ ! -x installNetDRMS.py ]
then
    echo 'Unable to locate installNetDRMS.py (it must be in the current directory).'
    exit 1
fi

# run Python
export LOCALIZATIONDIR="$locDir"
echo 'Running installNetDRMS.py...'
$BIN_PY3 installNetDRMS.py
if [ $? -ne 0 ]
then
    echo 'installNetDRMS.py ran unsuccessfully.'
    exit 1
fi

exit 0