#! /bin/csh -f

# requires the wd to be the root of the jsoc tree
set cp = `pwd`

echo "Generating DSDS plug-in links in $cp/base/local/libs/soi..."

ln -sf /home/soi/CM/src/libat.d/at.c base/local/libs/soi/at.c
ln -sf /home/soi/CM/src/libat.d/at_setkey.c base/local/libs/soi/at_setkey.c
ln -sf /home/soi/CM/src/libast.d/atoinc.c base/local/libs/soi/atoinc.c
ln -sf /home/soi/CM/src/libast.d/key.c base/local/libs/soi/key.c
ln -sf /home/soi/CM/src/libast.d/names.c base/local/libs/soi/names.c
ln -sf /home/soi/CM/src/libM.d/NaNs.c base/local/libs/soi/NaNs.c
ln -sf /home/soi/CM/src/libsds.d/ids_clist.c base/local/libs/soi/ids_clist.c
ln -sf /home/soi/CM/src/libsds.d/ids_etc.c base/local/libs/soi/ids_etc.c
ln -sf /home/soi/CM/src/libsds.d/ids_sdslist.c base/local/libs/soi/ids_sdslist.c
ln -sf /home/soi/CM/src/libsds.d/ids_series.c base/local/libs/soi/ids_series.c
ln -sf /home/soi/CM/src/libsds.d/sds_attr.c base/local/libs/soi/sds_attr.c
ln -sf /home/soi/CM/src/libsds.d/sds_axis.c base/local/libs/soi/sds_axis.c
ln -sf /home/soi/CM/src/libsds.d/sds_convert.c base/local/libs/soi/sds_convert.c
ln -sf /home/soi/CM/src/libsds.d/sds_fits.c base/local/libs/soi/sds_fits.c
ln -sf /home/soi/CM/src/libsds.d/sds_flip.c base/local/libs/soi/sds_flip.c
ln -sf /home/soi/CM/src/libsds.d/sds_helper.c base/local/libs/soi/sds_helper.c
ln -sf /home/soi/CM/src/libsds.d/sds_key.c base/local/libs/soi/sds_key.c
ln -sf /home/soi/CM/src/libsds.d/sds_llist.c base/local/libs/soi/sds_llist.c
ln -sf /home/soi/CM/src/libsds.d/sds_malloc.c base/local/libs/soi/sds_malloc.c
ln -sf /home/soi/CM/src/libsds.d/sds_query.c base/local/libs/soi/sds_query.c
ln -sf /home/soi/CM/src/libsds.d/sds_set.c base/local/libs/soi/sds_set.c
ln -sf /home/soi/CM/src/libsds.d/sds_slice.c base/local/libs/soi/sds_slice.c
ln -sf /home/soi/CM/src/libsds.d/sds_stats_inf.c base/local/libs/soi/sds_stats_inf.c
ln -sf /home/soi/CM/src/libsds.d/sds_utility.c base/local/libs/soi/sds_utility.c
ln -sf /home/soi/CM/src/libvds.d/vds_attrs.c base/local/libs/soi/vds_attrs.c
ln -sf /home/soi/CM/src/libvds.d/vds_create.c base/local/libs/soi/vds_create.c
ln -sf /home/soi/CM/src/libvds.d/vds_getkey.c base/local/libs/soi/vds_getkey.c
ln -sf /home/soi/CM/src/libvds.d/vds_new.c base/local/libs/soi/vds_new.c
ln -sf /home/soi/CM/src/libvds.d/vds_open.c base/local/libs/soi/vds_open.c
ln -sf /home/soi/CM/src/libvds.d/vds_query.c base/local/libs/soi/vds_query.c
ln -sf /home/soi/CM/src/libvds.d/vds_select.c base/local/libs/soi/vds_select.c
ln -sf /home/soi/CM/src/libvds.d/vds_set.c base/local/libs/soi/vds_set.c
ln -sf /home/soi/CM/src/libvds.d/vds_vars.c base/local/libs/soi/vds_vars.c

if (-e base/local/libs/soi/vds_vars.c) then
    ln -sf Rules.mk.su base/local/libs/soi/Rules.mk
endif
