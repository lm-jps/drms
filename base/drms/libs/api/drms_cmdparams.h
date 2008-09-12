#ifndef _DRMS_CMDPARAMS_H
#define _DRMS_CMDPARAMS_H

#include "cmdparams.h"

int drms_cmdparams_exists(CmdParams_t *parms, const char *name);
const char *drms_cmdparams_get_str(CmdParams_t *parms, const char *name, int *status);
int drms_cmdparams_get_int(CmdParams_t *parms, const char *name, int *status);

#endif /* _DRMS_CMDPARAMS_H */
