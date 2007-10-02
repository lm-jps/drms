#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Removing DSDS plug-in links in $cp/src/su_internal/libdsds..."

rm -f src/base/local/libs/dsds/hash_table.c
rm -f src/base/local/libs/dsds/hcontainer.c
rm -f src/base/local/libs/dsds/table.c

# Rules.mk
rm -f src/base/local/libs/dsds/Rules.mk
