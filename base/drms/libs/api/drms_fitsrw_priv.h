/**
@file drms_fitsrw_priv.h
*/
#ifndef _DRMS_FITSRW_PRIV_H
#define _DRMS_FITSRW_PRIV_H

#include "drms_types.h"
#include "cfitsio.h"

DRMS_Type_t drms_fitsrw_Bitpix2Type(int bitpix, int *err);
int drms_fitsrw_Type2Bitpix(DRMS_Type_t type, int *err);
void drms_fitsrw_ShootBlanks(DRMS_Array_t *arr, long long blank);
int drms_fitsrw_CreateDRMSArray(CFITSIO_IMAGE_INFO *info, void *data, DRMS_Array_t **arrout);
int drms_fitsrw_SetImageInfo(DRMS_Array_t *arr, CFITSIO_IMAGE_INFO *info);
void drms_fitsrw_term(int verbose);
void drms_fitsrw_close(int verbose, const char *filename);

#endif /* _DRMS_FITSRW_PRIV_H */
