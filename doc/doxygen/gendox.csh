#! /bin/csh -f

# gendox.csh [in=<indir>] [manout=<manoutdir>] [htmlout=<htmloutdir>]
#   if "in" is not specified, then the files in the CVS respository are assumed
#   if "in" is specified, <indir> must be of the form <jsoctreeroot>/doc/doxygen
#     and <indir> must contain doxygen_publ.cfg
#   if "manout" is not specified, then /home/jsoc/man/man3 is assumed
#   if "htmlout" is not specified, then /web/jsoc/htdocs/doxygen is assumed

# set echo

set NONE = "///none///"
set MINUS = "-"
set SLASH = "\/"

set TMPINDIR = "/tmp/doxygen/input" # tmp place to put JSOC tree
set TMPOUTDIR = "/tmp/doxygen/output" # tmp place to put output
set INTREE = "$TMPINDIR/JSOC/" # default input JSOC tree
set INDIR = "$INTREE/doc/doxygen" # default input (contains repository copy)
set MANOUTDIR = "/home/jsoc/man/man3" # default man output
set HTMLOUTDIR = "/web/jsoc/htdocs/doxygen_html" # default html output
set CMDIN = $NONE
set CMDHTMLOUT = $NONE
set CMDMANOUT = $NONE

foreach ARG ($argv)
    set FIRSTCH = `echo $ARG | awk '{print substr($0, 1, 1)}'`
    if (($FIRSTCH == $MINUS) || ($FIRSTCH == $SLASH)) then
	set FLAG = `echo $ARG | awk '{print substr($0, 2)}'`
    else
	# not a flag, must be an input or output path
	set EQINDEX = `echo $ARG | awk '{print index($0, "=")}'`

	if ($EQINDEX > 1) then
	    # we have a name-value argument
	    @ ENDANAME = $EQINDEX - 1
	    @ BEGINVAL = $EQINDEX + 1

	    set CMD = `echo $ARG | awk '{endaname=ARGV[1];eq=substr(endaname,10);print tolower(substr($0, 1, eq))}' endaname=$ENDANAME`
	    set CMDDATA = `echo $ARG | awk '{beginval=ARGV[1];eq=substr(beginval,10);print substr($0, eq)}' beginval=$BEGINVAL`

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
    if (!(-d $INTREE)) then
	mkdir -p $INTREE
    else
	find $INTREE -mindepth 1 -name "*" -exec rm -rf {} \;
    endif

    # download from CVS
    cd $INTREE/..
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

# run doxygen from $CMDIN
if ($CMDIN != $NONE && $CMDMANOUT != $NONE && $CMDHTMLOUT != $NONE) then
    # ensure tmp output directory is present
    if (!(-d $TMPOUTDIR)) then
	mkdir -p $TMPOUTDIR
	if (!(-d $TMPOUTDIR)) then
	    echo "Couldn't create tmp output directory $TMPOUTDIR; bailing!"
	endif
    endif

    # clean tmp dir
    find $TMPOUTDIR -mindepth 1 -name "*" -exec rm {} \;

    # call doxygen
    cd $CMDIN
    doxygen doxygen_publ.cfg

    # clean html destination, then copy html output
    find $CMDHTMLOUT -mindepth 1 -name "*" -exec rm {} \;
    cp -v $TMPOUTDIR/html/* $CMDHTMLOUT
    chmod 664 $CMDHTMLOUT/*

    cd $CMDMANOUT
    
    # clean *.3 --> *.3j links in man destination
    find . -mindepth 1 -name "*.3j" | awk '{sub(".3j",".3");system("rm "$0)}'
    
    # clean *.3j man-page files in man destination
    find . -mindepth 1 -name "*.3j" -exec rm {} \;

    # copy *.3j man-page files into man destination
    cd $TMPOUTDIR/man/man3
    find . -name "*.3" | awk '{manpath=ARGV[1];filename=substr($0,3);realpath=substr(manpath,9);system("cp -v "$0" "realpath"/"filename"j");system("chmod 644 "realpath"/"filename"j")}' manpath=$CMDMANOUT

    # create *.3 --> *.3j links in man destination
    cd $CMDMANOUT
    find . -mindepth 1 -name "*.3j" | awk '{sub(".3j",".3");system("ln -s "$0"j " $0);system("chmod 664 "$0"j");system("chmod 664 "$0)}'
else
    echo "Invalid input or output directory; bailing!"
endif

# clean temporary files
if (-d $TMPINDIR) then
    rm -rf $TMPINDIR/*
endif

if (-d $TMPOUTDIR) then
    rm -rf $TMPOUTDIR/*
endif

cd $WD
