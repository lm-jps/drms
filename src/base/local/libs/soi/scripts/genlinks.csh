#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Generating DSDS plug-in links in $cp/src/su_internal/libsoi..."

ln -sf /home/soi/CM/src/libat.d/at.c src/base/local/libs/soi/at.c
ln -sf /home/soi/CM/src/libat.d/at_setkey.c src/base/local/libs/soi/at_setkey.c
ln -sf /home/soi/CM/src/libast.d/atoinc.c src/base/local/libs/soi/atoinc.c
ln -sf /home/soi/CM/src/libast.d/key.c src/base/local/libs/soi/key.c
ln -sf /home/soi/CM/src/libast.d/names.c src/base/local/libs/soi/names.c
ln -sf /home/soi/CM/src/libM.d/NaNs.c src/base/local/libs/soi/NaNs.c
ln -sf /home/soi/CM/src/libsds.d/ids_clist.c src/base/local/libs/soi/ids_clist.c
ln -sf /home/soi/CM/src/libsds.d/ids_etc.c src/base/local/libs/soi/ids_etc.c
ln -sf /home/soi/CM/src/libsds.d/ids_sdslist.c src/base/local/libs/soi/ids_sdslist.c
ln -sf /home/soi/CM/src/libsds.d/ids_series.c src/base/local/libs/soi/ids_series.c
ln -sf /home/soi/CM/src/libsds.d/sds_attr.c src/base/local/libs/soi/sds_attr.c
ln -sf /home/soi/CM/src/libsds.d/sds_axis.c src/base/local/libs/soi/sds_axis.c
ln -sf /home/soi/CM/src/libsds.d/sds_convert.c src/base/local/libs/soi/sds_convert.c
ln -sf /home/soi/CM/src/libsds.d/sds_fits.c src/base/local/libs/soi/sds_fits.c
ln -sf /home/soi/CM/src/libsds.d/sds_flip.c src/base/local/libs/soi/sds_flip.c
ln -sf /home/soi/CM/src/libsds.d/sds_helper.c src/base/local/libs/soi/sds_helper.c
ln -sf /home/soi/CM/src/libsds.d/sds_key.c src/base/local/libs/soi/sds_key.c
ln -sf /home/soi/CM/src/libsds.d/sds_llist.c src/base/local/libs/soi/sds_llist.c
ln -sf /home/soi/CM/src/libsds.d/sds_malloc.c src/base/local/libs/soi/sds_malloc.c
ln -sf /home/soi/CM/src/libsds.d/sds_query.c src/base/local/libs/soi/sds_query.c
ln -sf /home/soi/CM/src/libsds.d/sds_set.c src/base/local/libs/soi/sds_set.c
ln -sf /home/soi/CM/src/libsds.d/sds_slice.c src/base/local/libs/soi/sds_slice.c
ln -sf /home/soi/CM/src/libsds.d/sds_stats_inf.c src/base/local/libs/soi/sds_stats_inf.c
ln -sf /home/soi/CM/src/libsds.d/sds_utility.c src/base/local/libs/soi/sds_utility.c
ln -sf /home/soi/CM/src/libvds.d/vds_attrs.c src/base/local/libs/soi/vds_attrs.c
ln -sf /home/soi/CM/src/libvds.d/vds_create.c src/base/local/libs/soi/vds_create.c
ln -sf /home/soi/CM/src/libvds.d/vds_getkey.c src/base/local/libs/soi/vds_getkey.c
ln -sf /home/soi/CM/src/libvds.d/vds_new.c src/base/local/libs/soi/vds_new.c
ln -sf /home/soi/CM/src/libvds.d/vds_open.c src/base/local/libs/soi/vds_open.c
ln -sf /home/soi/CM/src/libvds.d/vds_query.c src/base/local/libs/soi/vds_query.c
ln -sf /home/soi/CM/src/libvds.d/vds_select.c src/base/local/libs/soi/vds_select.c
ln -sf /home/soi/CM/src/libvds.d/vds_set.c src/base/local/libs/soi/vds_set.c
ln -sf /home/soi/CM/src/libvds.d/vds_vars.c src/base/local/libs/soi/vds_vars.c

if (-e src/base/local/libs/soi/vds_vars.c) then
    ln -sf Rules.mk.su src/base/local/libs/soi/Rules.mk
endif
