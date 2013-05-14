/* This module will add specified keywords to an existing series, if the series is not published.
 * If the series IS published, */

#include "jsoc_main.h"


char *module_name = "drms_dropkeys";

typedef enum
{
    kDkErr_Success = 0,
    kDkErr_FileIO,
    kDkErr_NoMemory,
    kDkErr_CantModSeries,
    kDkErr_CmdParams,
    kDkErr_DropKeys
} DkError_t;

#define kSeries "series"
#define kKeys   "keys"

ModuleArgs_t module_args[] =
{
    {ARG_STRING,  kSeries,  NULL,   "Series to delete keywords from."},
    {ARG_STRINGS, kKeys,    NULL,   "The names of the keywords to delete."},
    {ARG_END}
};


int DoIt(void) 
{
    DkError_t rv = kDkErr_Success;
    int drmsstat = DRMS_SUCCESS;
    const char *series = NULL;
    char **keys = NULL;
    int nkeys = -1;
    
    series = cmdparams_get_str(&cmdparams, kSeries, &drmsstat);
    if (drmsstat == DRMS_SUCCESS)
    {
        nkeys = cmdparams_get_strarr(&cmdparams, kKeys, &keys, &drmsstat);
        if (drmsstat != DRMS_SUCCESS)
        {
            rv = kDkErr_CmdParams;
        }
    }
    else
    {
        rv = kDkErr_CmdParams;
    }
    
    if (rv == kDkErr_Success)
    {
        int ret = drms_dropkeys_fromseries(drms_env, series, keys, nkeys);
        
        if (ret == DRMS_ERROR_CANTMODPUBSERIES)
        {
            fprintf(stderr, "Unable to modify replicated series.\n");
            rv = kDkErr_CantModSeries;
        }
        else if (ret != DRMS_SUCCESS)
        {
            rv = kDkErr_DropKeys;
        }
    }
    
    return rv;
}
