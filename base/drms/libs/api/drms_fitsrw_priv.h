/**
@file drms_fitsrw_priv.h
*/
#ifndef _DRMS_FITSRW_PRIV_H
#define _DRMS_FITSRW_PRIV_H

#include "drms_types.h"

struct cfitsio_image_info;

DRMS_Type_t drms_fitsrw_Bitpix2Type(int bitpix, int *err);
int drms_fitsrw_Type2Bitpix(DRMS_Type_t type, int *err);
void drms_fitsrw_ShootBlanks(DRMS_Array_t *arr, long long blank);
int drms_fitsrw_CreateDRMSArray(struct cfitsio_image_info *info, void *data, DRMS_Array_t **arrout);
int drms_fitsrw_SetImageInfo(DRMS_Array_t *arr, struct cfitsio_image_info *info);
void drms_fitsrw_term(int verbose);
void drms_fitsrw_close(int verbose, const char *filename);

#endif /* _DRMS_FITSRW_PRIV_H */
