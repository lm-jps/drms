#include "drms_cmdparams.h"
#include "drms.h"
#include "cmdparams_priv.h"

static char *blacklist[] =
{
   "DRMS_RETENTION",
   "DRMS_ARCHIVE",
   NULL
};

/* If an argument is on the black list, then we don't ever add the argument to 
 * the cmdparams structure.  */
static int OnBlackList(const char *argname)
{
   int onlist = 0;
   char **ps = blacklist;

   while(*ps)
   {
      if (strcmp(*ps, argname) == 0)
      {
         onlist = 1;
         break;
      }

      ps++;
   }

   return onlist;
}

/* Doesn't set the arg->accessed flag. */
int drms_cmdparams_exists(CmdParams_t *parms, const char *name)
{
   int exists = 0;

   /* Doesn't add new args to parms->args from the environment. */
   exists = (cmdparams_getargstruct(parms, name) != NULL);

   if (!exists && !OnBlackList(name) && cmdparams_exists(parms, name))
   {
      /* The arg was in the environment, so if user requests the arg, it will exist. */
      exists = 1;
   }

   return exists;
}

const char *drms_cmdparams_get_str(CmdParams_t *parms, const char *name, int *status)
{
   const char *str = NULL;
   int statint = DRMS_SUCCESS;

   /* Always check for existence first - this will ensure that code will not look for 
    * the black-listed variables in the environment */
   if (drms_cmdparams_exists(parms, name))
   {
      str = cmdparams_get_str(parms, name, &statint);
   }
   else
   {
      statint = DRMS_ERROR_UNKNOWNCMDARG;
   }

   if (status)
   {
      *status = statint;
   }

   return str;
}

int drms_cmdparams_get_int(CmdParams_t *parms, const char *name, int *status) 
{
   int statint = DRMS_SUCCESS;
   DRMS_Value_t value;
   const char *str = drms_cmdparams_get_str(parms, name, status);

   if (statint == DRMS_SUCCESS)
   {
      if (drms_sscanf2(str, NULL, 0, DRMS_TYPE_INT, &value) == -1)
      {
         statint = DRMS_ERROR_INVALIDCMDARGCONV;
      }
   }

   if (status)
   {
      *status = statint;
   }

   if (statint == DRMS_SUCCESS)
   {
      return value.value.int_val;
   }
   else
   {
      return DRMS_MISSING_INT;
   }
}
