#! /bin/csh -f

# gendox.csh [in=<indir>] [manout=<manoutdir>] [htmlout=<htmloutdir>]
#   if "in" is not specified, then the files in the CVS respository are assumed
#   if "in" is specified, <indir> must be of the form <jsoctreeroot>/doc/doxygen
#     and <indir> must contain doxygen_publ.cfg
#   if "out" is not specified, then /home/jsoc/man/man3j is assumed

set NONE = "///none///"
set MINUS = "-"
set SLASH = "/"
set INDIR = "/tmp/doxygen/input/JSOC/doc/doxygen" # default input (contains repository copy)
set MANOUTDIR = "/home/jsoc/man/man3j" # default man output
set HTMLOUTDIR = "/home/arta/testhtml" # default html output
set TMPOUTDIR = "/tmp/doxygen/output" # tmp place to put output
set CMDIN = $NONE
set CMDHTMLOUT = $NONE
set CMDMANOUT = $NONE

foreach ARG ($argv)
    set FIRSTCH = `echo $ARG | awk '{print substr($0, 1, 1)}'`
    if ($FIRSTCH == $MINUS || $FIRSTCH == $SLASH) then
	set FLAG = `echo $ARG | awk '{print substr($0, 2)}'`
    else
	# not a flag, must be an input or output path
	set EQINDEX = `echo $ARG | awk '{print index($0, "=")}'`
	# set EV = `expr $EQINDEX \> 1`
	# if ($EV == "1") then
	if ($EV > 1)
	    # we have a name-value argument
	    # set ENDANAME = `expr $EQINDEX - 1`
	    # set BEGINVAL = `expr $EQINDEX + 1`
	    @ ENDANAME = $EQINDEX - 1
	    @ BEGINVAL = $EQINDEX + 1
	    set CMD = `echo $ARG | awk '{print tolower(substr($0, 1, $ENDNAME))}'`
	    set CMDDATA = `echo $ARG | awk '{print substr($0, $BEGINVAL)}'`

	    # look for valid cmds
	    if ($CMD == "in") then
		set CMDIN = $CMDDATA
	    else if ($CMD == "manout") then
		set CMDMANOUT = $CMDDATA
	    else if ($CMD == "htmlout") then
		set CMDHTMLOUT = $CMDDATA
	    endif
	endif
    endif
end

set WD = `pwd`

if ($CMDIN == $NONE) then
    # user didn't specify "in"; download CVS respository to $INDIR
    # and run doxygen from there
    # create download directory if it doesn't exist
    if (!(-d $INDIR)) then
	mkdir -p $INDIR/../..
    else
	find $INDIR -name "*" -exec rm -rf {} \;
    endif

    # download from CVS
    cd $INDIR/../../..
    cvs checkout -AP JSOC
    set CMDIN = $INDIR
else
    # "in" was specified; <indir> must contain doxygen_publ.cfg
    if (!(-d $CMDIN) || !(-e $CMDIN/doxygen_publ.cfg)) then
	# error
	echo "Can't access $CMDIN/doxygen_publ.cfg."
	set CMDIN = $NONE
    endif
endif


if ($CMDMANOUT == $NONE) then
    # user didn't specify "out"; put the output in the JSOC man-page directory
    if (!(-e $MANOUTDIR)) then
	echo "Can't access JSOC man page directory $MANOUTDIR."
    else
	set CMDMANOUT = $MANOUTDIR
    endif
else
    # "manout" was specified; check <manoutdir> access
    if (!(-d $CMDMANOUT)) then
	echo "Can't access $CMDMANOUT."
	set CMDMANOUT = $NONE
    endif
endif

if ($CMDHTMLOUT == $NONE) then
    # user didn't specify "out"; put the output in the JSOC man-page directory
    if (!(-e $HTMLOUTDIR)) then
	echo "Can't access JSOC html directory $HTMLOUTDIR."
    else
	set CMDHTMLOUT = $HTMLOUTDIR
    endif
else
    # "htmlout" was specified; check <htmloutdir> access
    if (!(-d $CMDHTMLOUT)) then
	echo "Can't access $CMDHTMLOUT."
	set CMDHTMLOUT = $NONE
    endif
endif

# run doxygen from $CMDIN
if ($CMDIN != $NONE && $CMDMANOUT != $NONE && $CMDHTMLOUT != $NONE) then
    # ensure tmp output directory is present
    if (!(-d $TMPOUTDIR)) then
	mkdir -p $TMPOUTDIR
	if (!(-d $TMPOUTDIR)) then
	    echo "Couldn't create tmp output directory $TMPOUTDIR; bailing!"
	endif
    endif

    # clean
    find $TMPOUTDIR -name "*" -exec rm {} \;

    # call doxygen
    cd $CMDIN
    #doxygen doxygen_publ.cfg

    find $CMDHTMLOUT -name "*" -exec rm {} \;
    #cp -r $TMPOUTDIR/html/* $CMDHTMLOUT
    
    find $CMDMANOUT -name "*" -exec rm {} \;
    #cp -r $TMPOUTDIR/man/man3j/* $CMDMANOUT
else
    echo "Invalid input or output directory; bailing!"
endif

cd $WD
