#ifndef _DRMS_FITSTAS_H
#define _DRMS_FITSTAS_H

#include "drms.h"

int drms_fitstas_create(const char *filename, 
                        const char *comp,
                        DRMS_Type_t type, 
                        int naxis, 
                        int *axis,
                        double bzero,
                        double bscale);

int drms_fitstas_readslice(const char *filename, 
                           int naxis,
                           int *axis,
                           int slotnum,
                           DRMS_Array_t **arr);

int drms_fitstas_writeslice(DRMS_Segment_t *seg,
                            const char *filename, 
			    int *start, 
                            DRMS_Array_t *arrayout);

#endif /* _DRMS_FITSTAS_H */
