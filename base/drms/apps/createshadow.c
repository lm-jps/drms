#include "drms.h"
#include "jsoc_main.h"

#define kSeries "series"
#define kTablename "tname"
#define kUndefined "undefined"

typedef enum
{
    kCSErr_Success = 0,
    kCSErr_BadArg,
    kCSErr_OutOfMemory,
    kCSErr_BadQuery,
    kCSErr_CantCreateShadow
} CSError_t;

char *module_name = "createshadow";

/* Command line parameter values. */
ModuleArgs_t module_args[] =
{
    {ARG_STRING, kSeries, "", "The series for which a shadow table is to be created."},
    {ARG_STRING, kTablename, kUndefined, "Optional - the name of the shadow-table to create. This is to be used for debugging only. There is only one table name that DRMS recognizes (<series name>_shadow). If a value for tname is provided, then no trigger will be installed on the <series name> table. The trigger function references <series name>_shadow, so installing it without creating the offical shadow table <series name>_shadow will result in the trigger code failing."},
    {ARG_END}
};


int DoIt(void)
{
    CSError_t err = kCSErr_Success;
    int drmsstat = DRMS_SUCCESS;

    const char *series = cmdparams_get_str(&cmdparams, kSeries, &drmsstat);
    const char *tname = cmdparams_get_str(&cmdparams, kTablename, &drmsstat);

    if (drmsstat != DRMS_SUCCESS)
    {
        err = kCSErr_BadArg;
    }
    else
    {
        if (strcmp(tname, kUndefined) == 0)
        {
            tname = NULL;
        }

        drms_series_setcreateshadows(drms_env, NULL);
        drmsstat = drms_series_createshadow(drms_env, series, tname);

        if (drmsstat != DRMS_SUCCESS)
        {
            if (drmsstat == DRMS_ERROR_OUTOFMEMORY)
            {
                err = kCSErr_OutOfMemory;
            }
            else if (drmsstat == DRMS_ERROR_BADDBQUERY)
            {
                err = kCSErr_BadQuery;
            }
            else
            {
                err = kCSErr_CantCreateShadow;
            }
        }
    }

    return err;
}
