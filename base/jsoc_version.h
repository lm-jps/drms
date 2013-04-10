/*
 *  jsoc_version.h
 *
 * Contains the master version, release and build number definition.
 *
 *  Responsible:  CM
 *
 * NOTE: !!!!!!!! This should only be modified by the CM !!!!!!!!!!!!
 *
 */

#ifndef JSOC_VERSION_INCL

#define JSOC_VERSION_INCL 1

#define jsoc_version "V8R0"
#define jsoc_vers_num (800)


static inline const char *jsoc_getversion(char *verstr, int size, int *isdev)
{
    char *vers = strdup(jsoc_version);
    char *pc = NULL;
    int len = strlen(jsoc_version);
    
    if (vers)
    {
        if (isdev)
        {
            *isdev = 0;
        }
        
        if ((pc = strchr(vers, 'R')) != NULL)
        {
            *pc = '\0';
        }
        
        if (jsoc_version[len - 1] == 'X')
        {
            if (isdev)
            {
                *isdev = 1;
            }
            
            vers[len - 1] = '\0';
        }
        
        snprintf(verstr, size, "%s.%s", vers + 1, pc + 1);
        
        free(vers);
    }
    else
    {
        fprintf(stderr, "Out of memory.\n");
    }  
    
    return jsoc_version;
}

#endif

