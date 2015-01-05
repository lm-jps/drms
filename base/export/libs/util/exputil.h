#ifndef _EXPUTL_H
#define _EXPUTL_H

#include "drms_types.h"

typedef enum 
{
   kExpUtlStat_Success,
   kExpUtlStat_InvalidFmt,
   kExpUtlStat_UnknownKey,
   kExpUtlStat_ManageHandles
} ExpUtlStat_t;

ExpUtlStat_t exputl_mk_expfilename(DRMS_Segment_t *srcseg,
                                   DRMS_Segment_t *tgtseg,
                                   const char *filenamefmt, 
                                   char *filename);

ExpUtlStat_t exputl_manage_cgibin_handles(const char *op, const char *handle, pid_t pid, const char *file);

#endif /* _EXPUTL_H */
