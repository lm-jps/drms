#include "drms.h"
#include "jsoc_main.h"

#define kSeries "series"
#define kTablename "tname"
#define kUndefined "undefined"

typedef enum
{
    kDSErr_Success = 0,
    kDSErr_BadArg,
    kDSErr_OutOfMemory,
    kDSErr_BadQuery,
    kDSErr_CantDropShadow
} DSError_t;

char *module_name = "dropshadow";

/* Command line parameter values. */
ModuleArgs_t module_args[] = 
{
    {ARG_STRING, kSeries, "", "The series whose shadow table is to be dropped."},
    {ARG_STRING, kTablename, kUndefined, "Optional - the name of the shadow table to drop. This table was used for debugging since there is only one name that DRMS will recognize (<series name>_shadow). "},
    {ARG_END}
};


int DoIt(void) 
{
    DSError_t err = kDSErr_Success;
    int drmsstat = DRMS_SUCCESS;

    const char *series = cmdparams_get_str(&cmdparams, kSeries, &drmsstat);
    const char *tname = cmdparams_get_str(&cmdparams, kTablename, &drmsstat);
    
    if (drmsstat != DRMS_SUCCESS)
    {
        err = kDSErr_BadArg;
    }
    else
    {
        if (strcmp(tname, kUndefined) == 0)
        {
            tname = NULL;
        }
        
        drmsstat = drms_series_dropshadow(drms_env, series, tname);
        
        if (drmsstat != DRMS_SUCCESS)
        {
            if (drmsstat == DRMS_ERROR_OUTOFMEMORY)
            {
                err = kDSErr_OutOfMemory;
            }
            else if (drmsstat == DRMS_ERROR_BADDBQUERY)
            {
                err = kDSErr_BadQuery;
            }
            else 
            {
                err = kDSErr_CantDropShadow;
            }
        }
    }

    return err;
}
