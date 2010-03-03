#ifndef _EXPUTL_H
#define _EXPUTL_H

#include "drms_types.h"

typedef enum 
{
   kExpUtlStat_Success,
   kExpUtlStat_InvalidFmt
} ExpUtlStat_t;

ExpUtlStat_t exputl_mk_expfilename(DRMS_Segment_t *seg, 
                                   const char *filenamefmt, 
                                   char *filename);

#endif /* _EXPUTL_H */
