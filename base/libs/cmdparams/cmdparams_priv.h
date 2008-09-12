#ifndef _CMDPARAMS_PRIV_H
#define _CMDPARAMS_PRIV_H

#include "cmdparams.h"

void cmdparams_freeall (CmdParams_t *parms);
void cmdparams_set (CmdParams_t *parms, const char *name, const char *value);
void cmdparams_remove (CmdParams_t *parms, char *name);

#endif
