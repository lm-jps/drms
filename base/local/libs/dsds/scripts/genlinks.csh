#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Generating DSDS plug-in links in $cp/base/local/libs/dsds..."

ln -sf Rules.mk.su base/local/libs/dsds/Rules.mk
