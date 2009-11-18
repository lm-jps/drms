#include "drms_cmdparams.h"
#include "drms.h"
#include "cmdparams_priv.h"

static char *blacklist[] =
{
   "DRMS_RETENTION",
   "DRMS_ARCHIVE",
   NULL
};

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

int drms_cmdparams_exists(CmdParams_t *parms, const char *name)
{
   int exists = 0;
   int arg_num;

   if (name[0] == '$') 
   {
      if (sscanf(name + 1, "%d", &arg_num) == 1)
      {
         exists = arg_num < parms->numunnamed;
      }
   } 
   else 
   {
      exists = (hcon_lookup(parms->args, name) != NULL);
      if (!exists)
      {
         /* could be in environment - but don't allow "black-listed" variables */
         if (!OnBlackList(name))
         {
            exists = (getenv (name) != NULL);
         }
      }
   }

   return exists;
}

const char *drms_cmdparams_get_str(CmdParams_t *parms, const char *name, int *status)
{
   const char *str = NULL;
   int arg_num;
   CmdParams_Arg_t *arg = NULL;
   int statint = DRMS_SUCCESS;

   /* Always check for existence first - this will ensure that code will not look for 
    * the black-listed variables in the environment */
   if (drms_cmdparams_exists(parms, name))
   {
      if (name[0] == '$') 
      {
         if (sscanf(name + 1, "%d", &arg_num) == 1)
         {
            str = cmdparams_getarg(parms, arg_num);
         }
      } 
      else 
      {
         arg = (CmdParams_Arg_t *)(hcon_lookup(parms->args, name));
         
         if (arg)
         {
            str = arg->strval;
         }

         if (!str)
         {
            /* MUST be in environment - it exists, but not found in hash table */           
            str = getenv (name);
            XASSERT(str != NULL);

            if (str != NULL)
            {
               cmdparams_set(parms, name, str);
            }
         }
      }
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
