/**
@file drms_cmdparams.h
@brief Functions that blah blah
*/

#ifndef _DRMS_CMDPARAMS_H
#define _DRMS_CMDPARAMS_H

#include "cmdparams.h"
#include "drms.h"

int drms_cmdparams_exists(CmdParams_t *parms, const char *name);
const char *drms_cmdparams_get_str(CmdParams_t *parms, const char *name, int *status);
int drms_cmdparams_get_int(CmdParams_t *parms, const char *name, int *status);
DRMS_Value_t *drms_cmdparams_get(CmdParams_t *parms, const char *name, DRMS_Type_t type, int *status);
CmdParams_Arg_t *drms_cmdparams_getnext(CmdParams_t *parms, HIterator_t **last, int *status);

/* Doxygen function documentation */

/**
   @addtogroup cmdparam_api
   @{
*/

/**
   @fn int drms_cmdparams_exists(CmdParams_t *parms, const char *name)
   blah blah
*/

/**
   @fn const char *drms_cmdparams_get_str(CmdParams_t *parms, const char *name, int *status)
   blah blah
*/


/**
   @fn int drms_cmdparams_get_int(CmdParams_t *parms, const char *name, int *status)
   blah blah
*/

/**
   @}
*/

#endif /* _DRMS_CMDPARAMS_H */
