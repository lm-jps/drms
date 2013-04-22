#include "drms.h"
#include "jsoc_main.h"

#define kSeries "series"

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
    {ARG_END}
};


int DoIt(void) 
{
    CSError_t err = kCSErr_Success;
    int drmsstat = DRMS_SUCCESS;
    
    const char *series = cmdparams_get_str(&cmdparams, kSeries, &drmsstat);
    
    if (drmsstat != DRMS_SUCCESS)
    {
        err = kCSErr_BadArg;
    }
    else
    {
        drms_series_setcreateshadows(drms_env, NULL);
        drmsstat = drms_series_createshadow(drms_env, series);
        
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
