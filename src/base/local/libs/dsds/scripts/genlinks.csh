#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Generating DSDS plug-in links in $cp/src/su_internal/libdsds..."

ln -sf ../../../libs/dstruct/hash_table.c src/base/local/libs/dsds/hash_table.c
ln -sf ../../../libs/dstruct/hcontainer.c src/base/local/libs/dsds/hcontainer.c
ln -sf ../../../libs/dstruct/table.c src/base/local/libs/dsds/table.c

if (-e src/base/local/libs/dsds/table.c) then
    ln -sf Rules.mk.su src/base/local/libs/dsds/Rules.mk
endif
