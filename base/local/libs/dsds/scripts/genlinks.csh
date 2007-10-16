#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Generating DSDS plug-in links in $cp/base/local/libs/dsds..."

ln -sf ../../../libs/dstruct/hash_table.c base/local/libs/dsds/hash_table.c
ln -sf ../../../libs/dstruct/hcontainer.c base/local/libs/dsds/hcontainer.c
ln -sf ../../../libs/dstruct/table.c base/local/libs/dsds/table.c

if (-e base/local/libs/dsds/table.c) then
    ln -sf Rules.mk.su base/local/libs/dsds/Rules.mk
endif
