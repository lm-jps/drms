#include "drms_cmdparams.h"
#include "drms.h"
#include "cmdparams_priv.h"

/* These args cannot be set via the environment. They can be set only via the cmd-line. */
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
   const char *str = drms_cmdparams_get_str(parms, name, &statint);

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

int16_t drms_cmdparams_get_int16(CmdParams_t *parms, const char *name, int *status)
{
    int statint = DRMS_SUCCESS;
    DRMS_Value_t value;
    const char *str = drms_cmdparams_get_str(parms, name, &statint);
    
    if (statint == DRMS_SUCCESS)
    {
        if (drms_sscanf2(str, NULL, 0, DRMS_TYPE_SHORT, &value) == -1)
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
        return (int16_t)(value.value.int_val);
    }
    else
    {
        return DRMS_MISSING_SHORT;
    }
}

/* Unlike the other functions in this family, if there is a problem, this function returns NULL. */
DRMS_Value_t *drms_cmdparams_get(CmdParams_t *parms, const char *name, DRMS_Type_t type, int *status)
{
    int statint = DRMS_SUCCESS;
    DRMS_Value_t value;
    const char *str = drms_cmdparams_get_str(parms, name, &statint);
    DRMS_Value_t *rv = NULL;
    char *tmp = NULL;

    if (type == DRMS_TYPE_STRING)
    {
        /* If the string contains spaces, then drms_scsanf2 will parse out the substring to the 
         * left of the first space, unless we surround the string string with double quotes. */
        tmp = malloc(strlen(str) + 3);

        if (tmp)
        {
           snprintf(tmp, strlen(str) + 3, "\"%s\"", str);
           str = tmp;
        }
        else
        {
           statint = DRMS_ERROR_OUTOFMEMORY;
        }
    }

    if (statint == DRMS_SUCCESS)
    {
        if (drms_sscanf2(str, NULL, 0, type, &value) == -1)
        {
            statint = DRMS_ERROR_INVALIDCMDARGCONV;
        }
    }

    if (statint == DRMS_SUCCESS)
    {
        rv = malloc(sizeof(DRMS_Value_t));
        
        if (!rv)
        {
           statint = DRMS_ERROR_OUTOFMEMORY;
        }
        else
        {
           *rv = value;
        }
    }
    
    if (tmp)
    {
       free(tmp);
       tmp = NULL;
    }

    if (status)
    {
        *status = statint;
    }
    
    return rv;
}

/* Returns the next cmd-param argument if there is a next argument. Otherwise, returns NULL. */
CmdParams_Arg_t *drms_cmdparams_getnext(CmdParams_t *parms, HIterator_t **last, int *status)
{
    int istat = DRMS_SUCCESS;
    HIterator_t *hit = NULL;
    CmdParams_Arg_t *arg = NULL;
    HContainer_t *args = NULL;
    CmdParams_Arg_t *argret = NULL;
    
    if (last)
    {
        if (*last)
        {
            /* This is not the first time this function was called. */
            hit = *last;
        }
        else
        {
            hit = *last = (HIterator_t *)malloc(sizeof(HIterator_t));
            if (hit != NULL)
            {
                args = cmdparams_get_argscont(parms);
                
                if (args)
                {
                    hiter_new(hit, args);
                }
                else
                {
                    /* error */
                    istat = DRMS_ERROR_INVALIDDATA;
                }
            }
            else
            {
                istat = DRMS_ERROR_OUTOFMEMORY;
            }
        }
        
        if (istat == DRMS_SUCCESS)
        {
            arg = hiter_getnext(hit);
            
            if (arg)
            {
                argret = arg;
            }
        }
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return argret;
}
