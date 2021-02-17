/**
@file drms_fitsrw.h
*/
#ifndef _DRMS_FITSRW_H
#define _DRMS_FITSRW_H

#include "drms_types.h"

struct cfitsio_image_info;

DRMS_Array_t *drms_fitsrw_read(DRMS_Env_t *env,
                               const char *filename,
                               int readraw,
                               HContainer_t **keywords,
                               int *status);
void drms_fitsrw_freekeys(HContainer_t **keywords);
int drms_fitsrw_readslice(DRMS_Env_t *env,
                          const char *filename,
                          int naxis,
                          int *start,
                          int *end,
                          DRMS_Array_t **arr);
int drms_fitsrw_writeslice(DRMS_Env_t *env,
                           DRMS_Segment_t *seg,
                           const char *filename,
                           int naxis,
                           int *start,
                           int *end,
                           DRMS_Array_t *arrayout);
int drms_fitsrw_writeslice_ext(DRMS_Env_t *env,
                               DRMS_Segment_t *seg,
                               const char *filename,
                               int naxis,
                               int *start,
                               int *end,
                               int *finaldims,
                               DRMS_Array_t *arrayout);
int drms_fitsrw_GetSimpleFromInfo(struct cfitsio_image_info *info);
int drms_fitsrw_GetExtendFromInfo(struct cfitsio_image_info *info);
long long drms_fitsrw_GetBlankFromInfo(struct cfitsio_image_info *info);
double drms_fitsrw_GetBscaleFromInfo(struct cfitsio_image_info *info);
double drms_fitsrw_GetBzeroFromInfo(struct cfitsio_image_info *info);

#endif /* _DRMS_FITSRW_H */
