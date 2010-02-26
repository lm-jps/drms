/**
@file drms_fitsrw.h
*/
#ifndef _DRMS_FITSRW_H
#define _DRMS_FITSRW_H

#include "drms_types.h"
#include "cfitsio.h"

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

#endif /* _DRMS_FITSRW_H */
