#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Removing DSDS plug-in links in $cp/base/local/libs/dsds..."

# Rules.mk
rm -f base/local/libs/dsds/Rules.mk
