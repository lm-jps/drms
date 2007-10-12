#!/bin/csh -f
#This script runs a basic drms connection environment from the local directory.

setenv APIDIR "$JSOCROOT/src/base/drms/scripts"
setenv IDL_STARTUP idl_startup_auto.pro
setenv JSOCDB "jsoc"

set IDLCMD = "idl"

#This code checks for parameters to see if we want to run in the current
#directory and the debugger.

cd ${APIDIR}
$IDLCMD 
exit

#########################Subroutine for printing syntax message.
#SyntaxMsg:
#   echo 'tdp  <-?> <-t>    Initializes the program'
#   echo '      -?          Displays this message.'
#   echo '      -t          Runs in test mode (uses files in current directory)'
#   echo '      -d          Runs in debugger mode (uses files in current directory)'
#   echo '      -s <server> Specifies alternate server to run on and launches debugger.'
