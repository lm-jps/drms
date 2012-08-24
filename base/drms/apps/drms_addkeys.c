/* This module will add specified keywords to an existing series, if the series is not published.
 * If the series IS published, */

#include "jsoc_main.h"


char *module_name = "drms_addkeys";

typedef enum
{
    kAkErr_Success = 0,
    kAkErr_FileIO,
    kAkErr_NoMemory,
    kAkErr_CantModSeries,
    kAkErr_AddKeys
} AkError_t;

#define kSeries "series"
#define kSpec   "spec"

ModuleArgs_t module_args[] =
{
    {ARG_STRING, kSeries,  NULL,   "Series to add keywords to."},
    {ARG_STRING, kSpec,    NULL,   "Keyword specification(s). This can be a JSD keyword description, or a path to a file that contains JSD keyword descriptions, one per line."},
    {ARG_END}
};


int DoIt(void) 
{
    AkError_t rv = kAkErr_Success;
    
    const char *series = NULL;
    const char *spec = NULL;
    struct stat stBuf;    
    FILE *fptr = NULL;
    char *specin = NULL;
    
    series = cmdparams_get_str(&cmdparams, kSeries, NULL);
    spec = cmdparams_get_str(&cmdparams, kSpec, NULL);
    
    if (!stat(spec, &stBuf) && S_ISREG(stBuf.st_mode))
    {
        /* This is a file with keyword JSD descriptions, one keyword per line. */
        char rbuf[512];
        size_t nbytes;
        size_t ntot = 0;
        size_t szspec;
        
        fptr = fopen(spec, "r");
        if (fptr)
        {
            szspec = 1024;
            specin = calloc(1, szspec);
            
            if (specin)
            {
                while ((nbytes = fread(rbuf, sizeof(char), sizeof(rbuf) - 1, fptr)) > 0)
                {
                    rbuf[nbytes] = '\0';
                    ntot += nbytes;
                    specin = base_strcatalloc(specin, rbuf, &szspec);
                }
                
                if (ntot != stBuf.st_size)
                {
                    fprintf(stderr, "Error reading file %s.\n", spec);
                    rv = kAkErr_FileIO;
                }
            }
            else
            {
                rv = kAkErr_NoMemory;
            }
            
            fclose(fptr);
        }
        else
        {
            fprintf(stderr, "Unable to open file %s for reading.\n", spec);
            rv = kAkErr_FileIO;
        }
    }
    else
    {
        /* This is a parseable string containing one or more newline-separated keyword JSD descriptions. */
        specin = strdup(spec);
    }
    
    if (rv == kAkErr_Success)
    {
        /* If series is under replication, then sql will contain the sql needed to add columns to the 
         * series table, as well as the sql needed to insert rows in the ns.drms_keyword table. */
        char *sql = NULL;
        int ret = drms_addkeys_toseries(drms_env, series, specin, &sql);
        
        if (ret == DRMS_ERROR_CANTMODPUBSERIES)
        {
            fprintf(stderr, "Unable to modify replicated series.\n");
            
            if (sql)
            {
                fprintf(stderr, "SQL to issue with slonik to add columns to %s:\n\n", series);
                fprintf(stderr, "%s", sql);
            }
            
            rv = kAkErr_CantModSeries;
        }
        else if (ret != DRMS_SUCCESS)
        {
            rv = kAkErr_AddKeys;
        }
        
        if (sql)
        {
            free(sql);
        }
    }
    
    if (specin)
    {
        free(specin);
    }
    
    return rv;
}
