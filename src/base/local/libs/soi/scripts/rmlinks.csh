#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Removing DSDS plug-in links in $cp/src/su_internal/libsoi..."

rm -f src/base/local/libs/soi/at.c
rm -f src/base/local/libs/soi/at_setkey.c
rm -f src/base/local/libs/soi/atoinc.c
rm -f src/base/local/libs/soi/key.c
rm -f src/base/local/libs/soi/names.c
rm -f src/base/local/libs/soi/NaNs.c
rm -f src/base/local/libs/soi/ids_clist.c
rm -f src/base/local/libs/soi/ids_etc.c
rm -f src/base/local/libs/soi/ids_sdslist.c
rm -f src/base/local/libs/soi/ids_series.c
rm -f src/base/local/libs/soi/sds_attr.c
rm -f src/base/local/libs/soi/sds_axis.c
rm -f src/base/local/libs/soi/sds_convert.c
rm -f src/base/local/libs/soi/sds_fits.c
rm -f src/base/local/libs/soi/sds_flip.c
rm -f src/base/local/libs/soi/sds_helper.c
rm -f src/base/local/libs/soi/sds_key.c
rm -f src/base/local/libs/soi/sds_llist.c
rm -f src/base/local/libs/soi/sds_malloc.c
rm -f src/base/local/libs/soi/sds_query.c
rm -f src/base/local/libs/soi/sds_set.c
rm -f src/base/local/libs/soi/sds_slice.c
rm -f src/base/local/libs/soi/sds_stats_inf.c
rm -f src/base/local/libs/soi/sds_utility.c
rm -f src/base/local/libs/soi/vds_attrs.c
rm -f src/base/local/libs/soi/vds_create.c
rm -f src/base/local/libs/soi/vds_getkey.c
rm -f src/base/local/libs/soi/vds_new.c
rm -f src/base/local/libs/soi/vds_open.c
rm -f src/base/local/libs/soi/vds_query.c
rm -f src/base/local/libs/soi/vds_select.c
rm -f src/base/local/libs/soi/vds_set.c
rm -f src/base/local/libs/soi/vds_vars.c

# Rules.mk
rm -f src/base/local/libs/soi/Rules.mk

