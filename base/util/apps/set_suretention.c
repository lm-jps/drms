

/**
\defgroup set_suretention set_suretention - Modify the retention time of storage units
@ingroup drms_util

\par Synopsis:
\code
 set_suretention -O sulist=<SUNUM 1>,<SUNUM 2>,<SUNUM 3>,... retention=<number of days>
 set_suretention -O sulist=<file of SUNUMs>,... retention=<number of days>
 set_suretention -bO sulist=<record-set specification>,... retention=<number of days> [ n=<max number of records> ]
\endcode
 
\details
 
 <B>set_suretention</B> modifies the retention time of <I>existing</I> storage units in SUMS. It modifies neither the new-storage-unit retention
 nor the staging retention values stored in a series' JSD. Therefore, the retention times of storage units created after set_suretention 
 has been run will not be affected by the run of set_suretention. <B>set_suretention</B> can be used to either increase or decrease the 
 retention of any storage unit.
 
 There are three ways to specify the storage units to be modified. The caller can provide a comma-separated list of
 <I>SUNUMs</I> in the <I>sulist</I> parameter. An SUNUM is a unique integer that identifies a single storage unit. The caller can also list the
 SUNUMs in a file, and provide the path to the file in the <I>sulist</I> parameter. The SUNUMs must be white-space separated in this case.
 Finally, the caller can provide a DRMS record-set specification in the <I>sulist</I> parameter. In this case, <B>set_suretention</B> will
 resolve the specification into a set of DRMS records, and then obtain the SUNUMs of the storage units owned by those records.
 The optional <I>n</I> parameter will limit the number of DRMS records. A negative value will result in the selection of the <I>last</I> abs(n) records
 from the records identified by the record-set specification. A positive value will result in the selection of the <I>first</I> abs(n) records.
 The <I>n</I> parameter is relevant only when the <I>sulist</I> value is a record-set specification.

 The <I>retention</I> parameter contains a 15-bit integer that represents the number of days to which the storage units' retention should
 be set. The modification is applied to all identified storage units.
 
 The <I>b</I> flag slightly modifies the meaning of the DRMS record-set specification. If the flag is present, then the resulting set of
 records selected will contain any obsolete DRMS-record versions too. This flag is relevant only when the <I>sulist</I> value is a record-set specification.
 The <I>O</I> flag disables the database query time-out that otherwise exists. Without this flag, if the <B>set_suretention</B> call runs
 longer than the default time-out value (10 minutes), then the module is terminated with an error.
 
 IMPORTANT NOTE: The caller must be the owner of all DRMS series implied by the DRMS records selected. However, <B>set_suretention</B> will not exit
 or error-out should the caller not own the affected series. Instead, the retention of the relevant storage units will remain unmodified.
 
 \par Options:
 
 \li <TT>-b: display results for obsolete DRMS records too (autobang).</TT>
 \li <TT>-O: disable the default 10-minute database query time-out.</TT>
 
 \par Examples:
 
 <B>Example 1:</B>
 To reduce the retention to zero days of the storage units for a set of records, including all obsolete versions of these records
 (assuming the storage units of all these records currently have a retention time greater than 0):
 \code
 set_suretention -b sulist=hmi.M_45s[2012.05.12_23:50:15_TAI][2] retention=0
 \endcode
 
 <B>Example 2:</B>
 To reduce the retention to zero days of a set of storage units, given a set of SUNUMs:
 \code
 set_suretention sulist=324851808,324851834,324858457 retention=0
 \endcode

 <B>Example 3:</B>
 To reduce the retention to ten days of a set of storage units, given a text file containing a list of SUNUMs:
 \code
 set_suretention sulist=/tmp26/sulist.txt retention=10
 
 maelstrom:/home/arta> cat /tmp/sulist.txt
 324851808
 324851834
 324858457
 \endcode
 
 <B>Example 4:</B>
 To increase the retention to 10000 days of the storage units for a set of records (assuming the storage units of
 all these records currently have a retention time less than 10000 days). NOTE: the storage units of obsolete record versions
 are not affected:
 \code
 set_suretention sulist=hmi.M_45s[2012.05.12_23:50:15_TAI][2] retention=10000
 \endcode
*/

#include <sys/types.h>
#include <pwd.h>
#include "jsoc_main.h"

char *module_name = "set_suretention";

typedef enum
{
    kSetRetStatus_Success = 0,
    kSetRetStatus_OutOfMemory,
    kSetRetStatus_ChunkFetch,
    kSetRetStatus_SumGet,
    kSetRetStatus_Spec,
    kSetRetStatus_CleanUp,
    kSetRetStatus_Argument,
    kSetRetStatus_FileIO,
    kSetRetStatus_TimeOut
} SetRetStatus_t;

#define SULIST        "sulist"
#define NEWRETENTION  "retention"
#define NRECS         "n"
#define AUTOBANG      "b"
#define DISABLETO     "O"
#define kNotSpec      "NOTSPECIFIED"

ModuleArgs_t module_args[] =
{
    {ARG_STRING,  SULIST,       NULL, "a list of SUNUMs, or a file containing a list of SUNUMS, or a DRMS record-set specification resolving to a list of SUNUMs", NULL},
    {ARG_INT,     NEWRETENTION, NULL, "the new retention value to which all SUs will be set"},
    {ARG_INT,     NRECS,        "0",  "the number of records to accept - a positive number denotes the number of head records, a negative number denotes the number of tail records"},
    {ARG_FLAG,    AUTOBANG,     NULL, "disable prime-key logic"},
    {ARG_FLAG,    DISABLETO,    NULL, "disable the code that sets a database query time-out of 10 minutes"},
    {ARG_END}
};

#define FILE_CHUNK_SIZE -1 /* No chunking - get them all! Lower-level code sends chunks to SUMS. */

struct SUSR_Spec_struct
{
    int maxHeadRecs;
    int maxTailRecs;
    char *spec;
};

typedef struct SUSR_Spec_struct SUSR_Spec_t;

struct SUSR_List_struct
{
    int num;
    long long *arr;
};

typedef struct SUSR_List_struct SUSR_List_t;

struct SUSR_Cache_struct
{
    HContainer_t *map;
    HIterator_t *hit; /* keeps track where iteration is in map */
};

typedef struct SUSR_Cache_struct SUSR_Cache_t;

/* parameters - 
 * 1. DRMS environment.
 * 2. data (specific to the processing function, like a file name for the function that reads SUNUMs from a file),
 * 3. chunk size (this is a reference to the size - the function returns the actual size
 * of the chunk processed), 
 * 4. clean flag (if set, then the processing function cleans up resources being used). */
typedef long long *(*pFnFetchChunk_t)(DRMS_Env_t *, void *, int *, int);

/* rv == -1 means no more SUNUMs in file. */
static long long readSUNUMFromFile(FILE *fp, int *status)
{
    char oneChar;
    int count;
    char sunum[64]; /* If we parse anything that is bigger than 64 chars, reject it, since it cannot be an SUNUM. */
    long long rv;
    int istat;
    
    for (count = 0, *sunum = '\0', istat = 0; (oneChar = (char)fgetc(fp)) != EOF;)
    {
        /* Skip any whitespace. */
        if (oneChar == ' ' || oneChar == '\t')
        {
            continue;
        }
        
        if (oneChar == '\n' && count == 0)
        {
            /* This newline is acting as whitespace, but not a separator between SUNUMs. */
            continue;
        }
        
        /* oneChar is pointing to a non-whitespace character. */
        if (oneChar == '\n')
        {
            /* Got an SUNUM. */
            break;
        }
        else
        {
            /* Not a whitespace character, and not a newline. */
            if (count < sizeof(sunum) - 1)
            {
                sunum[count++] = oneChar;
            }
            else
            {
                istat = 1;
                sunum[count] = '\0';
                fprintf(stderr, "Too many chars in SUNUM: %s.\n", sunum);
                break;
            }
        }
    }
    
    sunum[count] = '\0';
    
    if (count > 0)
    {
        char *endptr = NULL;
        
        rv = strtoll(sunum, &endptr, 0);
        
        if (rv == 0 && endptr == sunum)
        {
            /* Bad text data - not an SUNUM. */
            istat = 1;
            fprintf(stderr, "Not a valid SUNUM: %s.\n", sunum);
        }
    }
    else
    {
        rv = -1;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return rv;
}

static int isAFile(const char *spec)
{
    struct stat stBuf;
    
    if (!stat(spec, &stBuf))
    {
        if (S_ISREG(stBuf.st_mode) || S_ISLNK(stBuf.st_mode))
        {
            return 1;
        }
    }
    
    return 0;
}

static long long *gimmeAChunk(DRMS_Env_t *env, pFnFetchChunk_t fxn, void *data, int *chunkSize, SetRetStatus_t *status)
{
    long long *chunk = NULL;
    SetRetStatus_t istat;
    
    istat = kSetRetStatus_Success;
    chunk = fxn(env, data, chunkSize, 0);
    
    if (chunk == (long long *)-1LL)
    {
        /* Error calling chunk-fetching function. */
        istat = kSetRetStatus_ChunkFetch;
    }
    
    if (status)
    {
        *status = istat;
    }

    return chunk;
}

static void freeAChunk(long long **chunk)
{
    if (chunk && *chunk)
    {
        free(*chunk);
        *chunk = NULL;
    }
}

/* Call SUM_get() on the chunk of SUNUMs. */
static int processChunk(DRMS_Env_t *env, int16_t newRetention, int nsus, long long *sunums)
{
    if (nsus > 0 && sunums)
    {
        return (drms_setretention(env, newRetention, nsus, sunums) != DRMS_SUCCESS);
    }
    else
    {
        return 0;
    }
}

static SetRetStatus_t callSumGet(DRMS_Env_t *env, pFnFetchChunk_t fxn, void *data, int chunkSize, int16_t newRetention)
{
    long long *sunums;
    int chunkSizeRet;
    SetRetStatus_t istat;
    int totalN;
    
    sunums = NULL;
    chunkSizeRet = chunkSize;
    totalN = 0;
    istat = kSetRetStatus_Success;
    
    while ((sunums = gimmeAChunk(env, fxn, data, &chunkSizeRet, &istat)) && istat == kSetRetStatus_Success)
    {
        totalN += chunkSizeRet;
        
        /* Call SUM_get() on the chunk of SUNUMs. */
        if (processChunk(env, newRetention, chunkSizeRet, sunums))
        {
            istat = kSetRetStatus_SumGet;
        }
        
        if (sunums)
        {
            freeAChunk(&sunums);
        }
    }
    
    if (istat == kSetRetStatus_Success)
    {
        chunkSizeRet = -1;
        if (NULL != (*fxn)(env, NULL, &chunkSizeRet, 1))
        {
            /* Error in the cleaning part of the chunk-fetch function. */
            istat = kSetRetStatus_CleanUp;
        }
    }
    
    if (totalN == 0)
    {
        fprintf(stderr, "WARNING: sulist did not specify any storage units whose retention could be modified.\n");
    }
    
    return istat;
}

static int sunumSort(const void *he1, const void *he2)
{
    long long *ps1 = (long long *)hcon_getval(*((HContainerElement_t **)he1));
    long long *ps2 = (long long *)hcon_getval(*((HContainerElement_t **)he2));
    
    XASSERT(ps1 && ps2);
    
    long long s1 = *ps1;
    long long s2 = *ps2;

    return (s1 < s2) ? -1 : (s1 > s2 ? 1 : 0);
}

/* If data is not NULL, then add the SUNUMs to the cache first. Do NOT return the next SUNUM, just
 * in case the caller might want to add SUNUMs from different sources by multipled calls to the 
 * function. Otherwise, return the next SUNUM in the cache.
 *
 * If called with data != NULL, then a return value of -1 indicates an error, and 0 indicates success. If called with
 * data == NULL, then a return value of -1 indicates an error, otherwise the return value is the next SUNUM in the 
 * sequence. If there are no more SUNUMs in the sequence, then return -2.
 */
static long long getNextOwnedSunum(DRMS_Env_t *env, SUSR_Cache_t **cache, long long *data, int num)
{
    long long rv = -1;
    int drmsStatus;
    
    drmsStatus = DRMS_SUCCESS;
    
    if (cache)
    {
        if (!*cache)
        {
            *cache = calloc(1, sizeof(SUSR_Cache_t));
            
            if (!cache)
            {
                fprintf(stderr, "Out of memory.\n");
            }
            else
            {
                (*cache)->map = hcon_create(sizeof(long long), 64, NULL, NULL, NULL, NULL, 0);
                if (!(*cache)->map)
                {
                    fprintf(stderr, "Out of memory.\n");
                }
            }
        }
        
        /* The cache of SUM_info_ts exists. */
        if (data)
        {
            if (*cache && (*cache)->map)
            {
                SUM_info_t **info = NULL;
                int isunum;
                const char *series = NULL;
                long long sunum;
                DRMS_Record_t *template = NULL;
                char sunumStr[64];
                
                info = (SUM_info_t **)calloc(num, sizeof(SUM_info_t *)); /* might allocate more memory than needed - this is ok */
                if (info)
                {
                    drmsStatus = drms_getsuinfo(env, data, num, info);
                    
                    /* Add the passed-in SUNUMs to the cache. */
                    if (drmsStatus != DRMS_SUCCESS)
                    {
                        fprintf(stderr, "Error fetching SUMS info.\n");
                    }
                    else
                    {
                        rv = 0;

                        for (isunum = 0; isunum < num; isunum++)
                        {
                            if (*info[isunum]->online_loc == '\0')
                            {
                                /* Invalid SUNUM (e.g., SU aged-off and was not archived, SUNUM was invalid, etc. )*/
                                fprintf(stderr, "Invalid sunum: %llu.\n", (unsigned long long)info[isunum]->sunum); /* uint_64t is a bad type. It is
                                                                                                                     * long, not long long. */
                                rv = -1;
                                break;
                            }

                            sunum = info[isunum]->sunum;
                            series = info[isunum]->owning_series;
                            template = drms_template_record(env, series, &drmsStatus);
                            
                            if (!template)
                            {
                                fprintf(stderr, "Unknown series %s for SUNUM %lld, skipping.\n", series, sunum);
                                continue;
                            }
                            
                            snprintf(sunumStr, sizeof(sunumStr), "%lld", sunum);
                            
                            if (!hcon_member((*cache)->map, sunumStr))
                            {
                                if (template->seriesinfo && template->seriesinfo->retention_perm)
                                {
                                    hcon_insert((*cache)->map, sunumStr, &sunum);
                                }
                                else
                                {
                                    fprintf(stderr, "WARNING: You do not have permission to modify the retention on SUNUM %s in series %s.\n", sunumStr, template->seriesinfo && *template->seriesinfo->seriesname != '\0' ? template->seriesinfo->seriesname : "<unknown>");
                                }
                            }
                        }
                    }
                    
                    for (isunum = 0; isunum < num; isunum++)
                    {
                        if (info[isunum])
                        {
                            free(info[isunum]);
                            info[isunum] = NULL;
                        }
                    }
                    
                    free(info);
                    info = NULL;
                }
                else
                {
                    fprintf(stderr, "Out of memory.\n");
                }
            }
        }
        else
        {
            /* Fetch next SUNUM in sequence. */
            if (*cache && (*cache)->map)
            {
                long long *pOneSunum = NULL;
                
                if (!(*cache)->hit)
                {
                    (*cache)->hit = calloc(1, sizeof(HIterator_t));
                    
                    if (!(*cache)->hit)
                    {
                        fprintf(stderr, "Out of memory.\n");
                    }
                    else
                    {
                        hiter_new_sort((*cache)->hit, (*cache)->map, sunumSort);
                    }
                }
                
                if ((*cache)->hit)
                {
                    if ((pOneSunum = (long long *)hiter_getnext((*cache)->hit)) != NULL)
                    {
                        rv = *pOneSunum;
                    }
                    else
                    {
                        /* no more SUNUMs */
                        rv = -2;
                    }
                }
            }
        }
    }
    
    return rv;
}

/* Return a chunk of SUNUMs from a file. Returns the number of elements in returned chunk in chunkSize.
 * If rv == NULL, then there was no error, but there were no SUNUMs in the current chunk requested. If
 * rv == -1L, then there was some kind of error.
 */
static long long *getChunkFromFile(DRMS_Env_t *env, void *data, int *chunkSize, int clean)
{
    static SUSR_Cache_t *infoCache = NULL;
    
    long long sunum;
    int count;
    int istat;
    long long *rv = NULL;
    long long *arr = NULL;
    int nSunums;

    count = 0;
    istat = 0;
    
    if (clean)
    {
        if (infoCache)
        {
            hiter_destroy(&infoCache->hit);
            hcon_destroy(&infoCache->map);
            free(infoCache);
            infoCache = NULL;
        }
    }
    else
    {
        /* Loop through SUNUMs, sending SUM_get() chunks of SUNUMs. */
        if (data && !infoCache)
        {
            const char *fileName = NULL;
            FILE *fptr = NULL;
            
            fileName = (const char *)data;
            fptr = fopen(fileName, "r");
            if (!fptr)
            {
                fprintf(stderr, "Unable to open file %s for reading.\n", fileName);
                istat = 1;
            }
            else
            {
                LinkedList_t *list = NULL;
                ListNode_t *node = NULL;
                int isunum;
                
                list = list_llcreate(sizeof(long long), NULL);
                
                /* Regardless of chunk size, read all SUNUMs into an array. Even there are millions of SUNUMs
                 * they will all fit in memory. */
                if (list)
                {
                    /* We have to parse the file to figure out how many SUNUMs are contained in it. */
                    nSunums = 0;
                    while ((sunum = readSUNUMFromFile(fptr, &istat)) != -1)
                    {
                        list_llinserttail(list, &sunum);
                        nSunums++;
                    }
                    
                    if (!istat)
                    {
                        nSunums = list->nitems;
                        arr = calloc(sizeof(long long), nSunums);
                        
                        if (arr)
                        {
                            list_llreset(list);
                            isunum = 0;
                            while ((node = list_llnext(list)) != NULL)
                            {
                                arr[isunum] = *((long long *)node->data);
                                isunum++;
                            }
                            
                            /* ingest SUNUMs into cache */
                            if (getNextOwnedSunum(env, &infoCache, arr, nSunums) == -1)
                            {
                                /* error ingesting SUNUMs */
                                istat = 1;
                            }
                            
                            free(arr);
                            arr = NULL;
                        }
                        else
                        {
                            istat = 1;
                            fprintf(stderr, "Out of memory.\n");
                        }
                    }
                    
                    list_llfree(&list);
                }
                else
                {
                    istat = 1;
                    fprintf(stderr, "Out of memory.\n");
                }
            }
        }
        
        if (!istat && infoCache)
        {
            nSunums = (*chunkSize == -1) ? hcon_size(infoCache->map) : *chunkSize;
            rv = calloc(sizeof(long long), nSunums);
            
            if (!rv)
            {
                istat = 1;
                fprintf(stderr, "Out of memory.\n");
            }
            
            if (!istat)
            {
                while ((*chunkSize == -1 || count < *chunkSize) && ((sunum = getNextOwnedSunum(env, &infoCache, NULL, -1)) >= 0))
                {
                    rv[count] = sunum;
                    count++;
                }
                
                if (sunum == -1)
                {
                    /* error fetching an SUNUM */
                    istat = 1;
                }
                else if (sunum == -2)
                {
                    /* fetched all SUNUMs from the list */
                }
            }
        }
    }
    
    if (istat)
    {
        rv = (long long *)-1LL;
    }
    
    if (!clean && count == 0 && rv && rv != (long long *)-1LL)
    {
        /* No SUNUMs in this chunk. Not an error. */
        free(rv);
        rv = NULL;
    }
        
    *chunkSize = count;
    
    return rv;
}

/* Return a chunk of SUNUMs from a comma-separated list. Returns the number of elements in returned chunk in chunkSize.
 * If rv == NULL, then there was no error, but there were no SUNUMs in the current chunk requested. If
 * rv == -1L, then there was some kind of error.
 *
 * A *chunkSize of -1 means to get ALL SUNUMs.
 */
static long long *getChunkFromList(DRMS_Env_t *env, void *data, int *chunkSize, int clean)
{
    static SUSR_Cache_t *infoCache = NULL;
    int count;
    int istat;
    long long *rv = NULL;
    
    count = 0;
    istat = 0;
    
    if (clean)
    {
        if (infoCache)
        {
            hiter_destroy(&infoCache->hit);
            hcon_destroy(&infoCache->map);
            free(infoCache);
            infoCache = NULL;
        }
    }
    else
    {
        if (data && !infoCache)
        {
            SUSR_List_t *list = NULL;
            
            list = (SUSR_List_t *)data;
            
            /* ingest SUNUMs into cache */
            if (getNextOwnedSunum(env, &infoCache, list->arr, list->num) == -1)
            {
                /* error ingesting SUNUMs */
                istat = 1;
            }
        }
        
        if (!istat && infoCache)
        {
            int nSunums;
            
            nSunums = (*chunkSize == -1) ? hcon_size(infoCache->map) : *chunkSize;
            rv = calloc(sizeof(long long), nSunums); /* might alloc more than needed - this is ok */
            
            if (!rv)
            {
                fprintf(stderr, "Out of memory.\n");
                istat = 1;
            }
            
            if (!istat)
            {
                long long sunum;
                
                while ((*chunkSize == -1 || count < *chunkSize) && ((sunum = getNextOwnedSunum(env, &infoCache, NULL, -1)) >= 0))
                {
                    rv[count] = sunum;
                    count++;
                }
                
                if (sunum == -1)
                {
                    /* error fetching an SUNUM */
                    istat = 1;
                }
                else if (sunum == -2)
                {
                    /* fetched all SUNUMs from the list */
                }
            }
        }
    }
    
    if (istat)
    {
        rv = (long long *)-1LL;
    }
    
    if (!clean && count == 0 && rv && rv != (long long *)-1LL)
    {
        /* No SUNUMs in this chunk. Not an error. */
        free(rv);
        rv = NULL;
    }
    
    *chunkSize = count;

    return rv;
}

/* Return a chunk of SUNUMs from a record-set specification. Returns the number of elements in returned chunk in chunkSize.
 * If rv == NULL, then there was no error, but there were no SUNUMs in the current chunk requested. If
 * rv == -1L, then there was some kind of error.
 *
 * A *chunkSize of -1 means to get ALL SUNUMs.
 */
static long long *getChunkFromSpec(DRMS_Env_t *env, void *data, int *chunkSize, int clean)
{
    /* Get a list of SUNUMs from the record-set specification. There is no reason to fetch all keywords, of which there
     * are potentially hundreds. Instead, use Phil's vector(column)-fetching code to obtain just the list of SUNUMs
     * from the series (after the prime-key logic has been applied). 
     *
     * We want to use the same logic as show_info.
     * 1. Check for a n=XX-limited record-set (use drms_open_nrecords()). Unfortunately, there isn't currently a way 
     *    to fetch just the SUNUMs with this API function, so we have to fetch all keywords.
     * 2. Check for a non-n=XX-limited record-set (use drms_record_getvector()).
     
     * show_info actually checks for a non-DRMS-series record-set first, but the two cases above will handle
     * non-DRMS-series record-set as well for the needs here (obtaining SUNUMs).
     */
    static DRMS_RecordSet_t *recordset = NULL;
    static SUSR_Cache_t *infoCache = NULL;
    
    long long *rv = NULL;
    int count;
    int nRecs;
    int drmsStatus;
    int istat;

    count = 0;
    drmsStatus = DRMS_SUCCESS;
    istat = 0;
    
    if (clean)
    {
        if (infoCache)
        {
            hiter_destroy(&infoCache->hit);
            hcon_destroy(&infoCache->map);
            free(infoCache);
            infoCache = NULL;
        }
        
        if (recordset)
        {
            drms_close_records(recordset, DRMS_FREE_RECORD);
            recordset = NULL;
        }
    }
    else
    {
        if (data && !recordset && !infoCache)
        {
            SUSR_Spec_t *specData = NULL;
            int maxHeadRecs;
            int maxTailRecs;
            const char *spec = NULL;
            
            /* Extract data. */
            specData = (SUSR_Spec_t *)data;
            
            maxHeadRecs = specData->maxHeadRecs;
            maxTailRecs = specData->maxTailRecs;
            spec = specData->spec;
            
            if (maxHeadRecs >= 0)
            {
                recordset = drms_open_nrecords(env, spec, maxHeadRecs, &drmsStatus);
                if (recordset)
                {
                    nRecs = recordset->n;
                }
            }
            else if (maxTailRecs >= 0)
            {
                recordset = drms_open_nrecords(env, spec, -1 * maxTailRecs, &drmsStatus);
                if (recordset)
                {
                    nRecs = recordset->n;
                }
            }
            else
            {
                DRMS_Array_t *array = NULL;
                
                /* Does not return a recordset - blech. */
                array = drms_record_getvector(env, spec, "sunum", DRMS_TYPE_LONGLONG, 0, &drmsStatus);

                if (array && drmsStatus == DRMS_SUCCESS)
                {
                    /* We are going to ping SUMS to fetch the infoStructs associated with these SUNUMs
                     * (so we can get the series names for the SUNUMs returned by drms_record_getvector(),
                     * which does not return the names of the series owning the SUNUMS, unlike drms_open_nrecords()), 
                     * so it behooves us get rid of duplicate SUNUMs, which will exist for all series that
                     * have a unit size > 1. I'm removing dupes for drms_record_getvector(), but not for 
                     * drms_open_nrecords(), because we do not need to get the infoStructs from SUMS for
                     * the drms_open_nrecords() call - the owning series are returned by that call.
                     *
                     * The most efficient way to remove dupes with a moderate-size list is to use a hash table.
                     * The code that calls SUMS, getNextOwnedSunum(), sorts the SUNUMs before sending them to
                     * SUMS, so there is no reason to do that here.
                     */
                    HContainer_t *uniqueMap = NULL;
                    long long *unique = NULL;
                    int isunum;
                    char sunumStr[64];
                    int countUnique;
                    char dummy;
                    
                    nRecs = drms_array_count(array);
                    uniqueMap = hcon_create(sizeof(char), 64, NULL, NULL, NULL, NULL, 0);
                    unique = calloc(nRecs, sizeof(long long)); /* Probably too large, since there will be dupes, but no biggie. */
                    
                    if (!uniqueMap || !unique)
                    {
                        fprintf(stderr, "Out of memory.\n");
                        istat = 1;
                    }
                    else
                    {
                        for (isunum = 0, countUnique = 0, dummy = 'y'; isunum < nRecs; isunum++)
                        {
                            snprintf(sunumStr, sizeof(sunumStr), "%lld", ((long long *)array->data)[isunum]);
                            
                            if (!hcon_member(uniqueMap, sunumStr))
                            {
                                unique[countUnique] = ((long long *)array->data)[isunum];
                                countUnique++;
                                hcon_insert(uniqueMap, sunumStr, &dummy);
                            }
                        }
                        
                        /* ingest SUNUMs into cache */
                        if (getNextOwnedSunum(env, &infoCache, unique, countUnique) == -1)
                        {
                            /* error ingesting SUNUMs */
                            istat = 1;
                        }
                        
                        free(unique);
                        unique = NULL;
                        hcon_destroy(&uniqueMap);
                    }
                }
                
                if (array)
                {
                    drms_free_array(array);
                    array = NULL;
                }
            }
            
            if (drmsStatus != DRMS_SUCCESS)
            {
                fprintf(stderr, "Error retrieving DRMS records from the database.\n");
                
                if (drmsStatus == DRMS_ERROR_QUERYFAILED)
                {
                    /* Check for error message. */
                    const char *emsg = DB_GetErrmsg(env->session->db_handle);
                    
                    if (emsg)
                    {
                        fprintf(stderr, "DB error message: %s\n", emsg);
                    }
                }
                
                istat = 1;
            }
        }
        
        if (!istat && (recordset || infoCache))
        {
            int nSunums;
            
            if (*chunkSize != -1)
            {
                nSunums = *chunkSize;
            }
            else
            {
                nSunums = recordset ? recordset->n : hcon_size(infoCache->map);
            }
            
            rv = calloc(sizeof(long long), nSunums); /* might alloc more than needed - this is ok */
            
            if (!rv)
            {
                fprintf(stderr, "Out of memory.\n");
                istat = 1;
            }
            
            if (!istat)
            {
                /* Can't use drms_recordset_fetchnext() for both types of record-fetch results. */
                if (recordset)
                {
                    DRMS_Record_t *rec = NULL;
                    DRMS_RecChunking_t cstat = kRecChunking_None;
                    int newchunk;

                    while ((*chunkSize == -1 || count < *chunkSize) && (rec = drms_recordset_fetchnext(env, recordset, &drmsStatus, &cstat, &newchunk)) != NULL && drmsStatus == 0)
                    {
                        /* Make sure the SUNUMs refer to actual SUs that are owned by the db user who this program is logged-in as. 
                         * Production users can also modify retention. */
                        if (rec->sunum >= 0 && rec->seriesinfo && rec->seriesinfo->retention_perm)
                        {
                            rv[count] = rec->sunum;
                            count++;
                        }
                    }
                    
                    if (drmsStatus != DRMS_SUCCESS)
                    {
                        istat = 1;
                    }
                }
                else if (infoCache)
                {
                    long long sunum;
                    
                    while ((*chunkSize == -1 || count < *chunkSize) && ((sunum = getNextOwnedSunum(env, &infoCache, NULL, -1)) >= 0))
                    {
                        rv[count] = sunum;
                        count++;
                    }
                    
                    if (sunum == -1)
                    {
                        /* error fetching an SUNUM */
                        istat = 1;
                    }
                    else if (sunum == -2)
                    {
                        /* fetched all SUNUMs from the list */
                    }
                }
            }
            else
            {
                fprintf(stderr, "Out of memory.\n");
            }
        }
    }
    
    if (istat)
    {
        rv = (long long *)-1LL;
    }
    
    if (!clean && count == 0 && rv && rv != (long long *)-1LL)
    {
        /* No SUNUMs in this chunk. Not an error. */
        free(rv);
        rv = NULL;
    }
    
    *chunkSize = count;
    
    return rv;
}

char *insertAutoBang(DRMS_Env_t *env, const char *spec, const char *autobangStr, char **sets, DRMS_RecordSetType_t *settypes, char **snames, int nsets, int *status)
{
    int drmsStatus;
    DRMS_Record_t *templRec = NULL;
    char *filter = NULL;
    char *filterbuf = NULL;
    size_t fbsz = 128;
    char *intermed = NULL;
    char *rv = NULL;
    int iset;
    int istat;
    
    filterbuf = calloc(fbsz, sizeof(char));
    rv = strdup(spec);
    istat = 0;

    if (!filterbuf || !rv)
    {
        fprintf(stderr, "Out of memory.\n");
        istat = 1;
    }
    else
    {
        for (iset = 0; iset < nsets && !istat; iset++)
        {
            if (settypes[iset] == kRecordSetType_DSDSPort || settypes[iset] == kRecordSetType_DRMS)
            {
                templRec = drms_template_record(env, snames[iset], &drmsStatus);
                if (DRMS_ERROR_UNKNOWNSERIES == drmsStatus)
                {
                    fprintf(stderr, "Unable to open template record for series '%s'; this series does not exist.\n", snames[iset]);
                    istat = 1;
                }
                else
                {
                    filter = drms_recordset_extractfilter(templRec, sets[iset], &istat);
                    
                    if (!istat)
                    {
                        *filterbuf = '\0';
                        if (filter)
                        {
                            filterbuf = base_strcatalloc(filterbuf, filter, &fbsz);
                            filterbuf = base_strcatalloc(filterbuf, autobangStr, &fbsz);
                            
                            /* Replace filter with filterbuf. */
                            intermed = base_strreplace(rv, filter, filterbuf);
                            free(rv);
                            rv = intermed;
                        }
                        else
                        {
                            filterbuf = base_strcatalloc(filterbuf, snames[iset], &fbsz);
                            filterbuf = base_strcatalloc(filterbuf, autobangStr, &fbsz);
                            
                            /* Replace series name with filterbuf. */
                            intermed = base_strreplace(rv, snames[iset], filterbuf);
                            free(rv);
                            rv = intermed;
                        }
                    }
                    
                    if (filter)
                    {
                        free(filter);
                        filter = NULL;
                    }
                }
            }
        } /* loop over sets */
        
        if (istat)
        {
            if (rv)
            {
                free(rv);
                rv = NULL;
            }
        }
        
        free(filterbuf);
        filterbuf = NULL;
    }
    
    if (status)
    {
        *status = istat;
    }
    
    return rv;
}

/* This program supports three methods of specifying storage units:
 * 1. A comma-separated list of SUNUMs.
 * 2. A file containing a list of SUNUMs. The SUNUMs can be separated by newline chars or other whitespace.
 * 3. A DRMS record-set specification. The resulting set of records can be filtered by the n=XX argument.
 */
int DoIt(void)
{
    SetRetStatus_t rv = kSetRetStatus_Success;
    int cpStat;
    const char *autobangStr = NULL;
    int disableTO;
    const char *sulist = NULL;
    int16_t newRetention;
    int maxHeadRecs;
    int maxTailRecs;
    int nRecs;
    
    /* The user may have specified the DRMS_RETENTION argument, which results in drms_env->retention != INT16_MIN.
     * Ignore this (although this module, as originally written, does not call code that uses drms_env->retention). */
    drms_env->retention = INT16_MIN;
    
    sulist = cmdparams_get_str(&cmdparams, SULIST, NULL);
    newRetention = cmdparams_get_int16(&cmdparams, NEWRETENTION, &cpStat);
    if (cpStat == CMDPARAMS_INVALID_CONVERSION)
    {
        /* The number provided on the cmd-line could be negative, which is not allowed. */
        if (newRetention < 0)
        {
            fprintf(stderr, "The retention-argument value must be a positive 15-bit number.\n");
            rv = kSetRetStatus_Argument;
        }
    }

    if (rv == kSetRetStatus_Success)
    {
        maxHeadRecs = -1;
        maxTailRecs = -1;
        
        nRecs = cmdparams_get_int(&cmdparams, NRECS, NULL);
        
        if (nRecs < 0)
        {
            maxTailRecs = -1 * nRecs;
        }
        else if (nRecs > 0)
        {
            maxHeadRecs = nRecs;
        }
        else
        {
            /* (nRecs == 0) ==> no record-limit requested. */
        }
        
        /* If autobang is enabled, then append a filter that will disable the "prime-key logic" in all SQL queries. */
        autobangStr = cmdparams_isflagset(&cmdparams, AUTOBANG) ? "[! 1=1 !]" : "";
        disableTO = cmdparams_isflagset(&cmdparams, DISABLETO);
    }
    
    if (rv == kSetRetStatus_Success)
    {
        // Set a 10-minute database statement time-out. This code can be disabled by providing the
        // -O flag on the command-line. THIS CAN ONLY BE DONE IN DIRECT-CONNECT CODE! We don't want
        // to have a socket-connect module affect the time-out for all drms_server clients.
#ifndef DRMS_CLIENT
        if (!disableTO && drms_env->dbtimeout == INT_MIN)
        {
            if (db_settimeout(drms_env->session->db_handle, 600000))
            {
                fprintf(stderr, "Failed to modify db-statement time-out to %d.\n", 600000);
                rv = kSetRetStatus_TimeOut;
            }
        }
#endif
    }
    
    if (rv == kSetRetStatus_Success)
    {
        if (isAFile(sulist))
        {
            /* The SULIST argument value can be a file path... */
            rv = callSumGet(drms_env, &getChunkFromFile, (void *)sulist, FILE_CHUNK_SIZE, newRetention);
        }
        else
        {
            int nsunum;
            long long *sunumList = NULL;
            
            nsunum = cmdparams_get_int64arr(&cmdparams, SULIST, (int64_t **)&sunumList, &cpStat);
            
            if (cpStat == CMDPARAMS_SUCCESS)
            {
                /* it can be a comma-separated list of SUNUMs... */
                SUSR_List_t listData;
                
                listData.num = nsunum;
                listData.arr = sunumList;
                
                rv = callSumGet(drms_env, &getChunkFromList, (void *)&listData, FILE_CHUNK_SIZE, newRetention);
            }
            else
            {
                /* or it can be a DRMS record-set specification. */
                SUSR_Spec_t specData;
                char *allvers = NULL; /* If 'y', then don't do a 'group by' on the primekey value.
                                       * The rationale for this is to allow users to get all versions
                                       * of the requested DRMS records */
                char **sets = NULL;
                DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
                char **snames = NULL;
                char **filts = NULL;
                int nsets = 0;
                DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
                int inqry; // means "has a record-set filter"
                int atfile; // means "rs spec was an atfile"
                
                if (drms_record_parserecsetspec(sulist, &allvers, &sets, &settypes, &snames, &filts, &nsets, &rsinfo) != DRMS_SUCCESS)
                {
                    rv = kSetRetStatus_Spec;
                }
                
                if (rv == kSetRetStatus_Success)
                {
                    inqry = ((rsinfo & kFilters) != 0);
                    atfile = ((rsinfo & kAtFile) != 0);

                    if (!inqry && maxHeadRecs == -1 && maxTailRecs == -1 && !atfile)
                    {
                        fprintf(stderr, "If the 'sulist' argument is a record-set specification, then it must either be an at-file path or the specification must contain a filter. If neither is true, then the record-limit argument (n=XX) must be present.\n");
                        rv = kSetRetStatus_Argument;
                    }
                }
                
                if (rv == kSetRetStatus_Success)
                {
                    int istat;
                    
                    /* Must INSERT autobangStr into record-set specification. */
                    if (strlen(autobangStr) >  0)
                    {
                        sulist = insertAutoBang(drms_env, sulist, autobangStr, sets, settypes, snames, nsets, &istat);
                        if (istat)
                        {
                            rv = kSetRetStatus_Spec;
                        }
                    }
                    
                    drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, &filts, nsets);
                    
                    specData.maxHeadRecs = maxHeadRecs;
                    specData.maxTailRecs = maxTailRecs;
                    specData.spec = strdup(sulist);
                    
                    if (specData.spec)
                    {
                        rv = callSumGet(drms_env, &getChunkFromSpec, (void *)&specData, FILE_CHUNK_SIZE, newRetention);
                        free(specData.spec);
                        specData.spec = NULL;
                    }
                    else
                    {
                        rv = kSetRetStatus_OutOfMemory;
                    }
                }
            }
            
            if (sunumList)
            {
                free(sunumList);
                sunumList = NULL;
            }
        }
    }
    
    return rv;
}
