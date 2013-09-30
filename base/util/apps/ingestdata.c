#include "drms.h"
#include "jsoc_main.h"

/* Doxygen documentation */
/* Data are provided to this module in a file or via stdin. The first row of the (virtual) file 
* must contain white-space-separated keyword names. These names must match keyword names 
* (in a case-insensitive manner) that exist in the series identified by the "series" argument. 
* Each of the remaining lines contains keyword values. Data are tabular - the values in a given 
* column are for the keyword at the head of the column. Each row corresponds to a DRMS record 
* of the series identified by the "series" argument. */

#define kArgSeries       "series"
#define kArgDatafile     "dataf"
#define kUndefined       "undefined"

#define kChunkSize       128

#define kDateKeyName     "date"
#define kDateStrKeyName  "datestr"

typedef enum
{
    kIDErr_Success = 0,
    kIDErr_InvalidArgs,
    kIDErr_DRMS,
    kIDErr_NoSeries,
    kIDErr_FileIO,
    kIDErr_OutOfMemory,
    kIDErr_SegNotSupported
} IDError_t;

char *module_name = "ingestdata";

/* Command line parameter values. */
ModuleArgs_t module_args[] = 
{
    {ARG_STRING, kArgSeries, "", "The series into which data are to be ingested."},
    {ARG_STRING, kArgDatafile, kUndefined, "Optional - data can be specified in tabular format in a file. The first row contains the keyword names."},
    {ARG_END}
};

static int IngestFile()
{
    /* Not implemented. */
    int rv;
    
    rv = 1;
    
    return rv; 
}

static FILE *OpenDataStream(const char *dfile, IDError_t *status)
{
    FILE *dfptr = NULL;
    
    *status = kIDErr_Success;
    
    if (strcmp(dfile, kUndefined) == 0)
    {
        /* No data file was supplied. Read from stdin to fetch data rows. */
        dfptr = stdin;
    }
    else
    {
        dfptr = fopen(dfile, "r");
        if (!dfptr)
        {
            fprintf(stderr, "Unable to open ");
            *status = kIDErr_FileIO;
        }
    }
    
    return dfptr;
}

/* Returns 0 if the char passed in ch is a white-space character, 1
 * if the char is not a white-space character, and -1 if an error
 * occurred. */
static int IsWS(const char *ch)
{
    int rv = -1;
    
    if (ch && *ch)
    {
        rv = (*ch == '\t' || *ch == ' ');
    }
    
    return rv;
}

static IDError_t SetValue(const char *val, LinkedList_t *objlist, DRMS_Record_t *rec, const char *series, int lineno)
{
    ListNode_t *nextobj = NULL;
    const char *objname = NULL;
    DRMS_Keyword_t *drmskey = NULL;
    DRMS_Segment_t *drmsseg = NULL;
    DRMS_Type_Value_t dval;
    int dstat = DRMS_SUCCESS;
    IDError_t rv;
    
    rv = kIDErr_Success;

    /* Ready to set keyword/segment value. */
    nextobj = list_llnext(objlist);
    objname = (const char *)(nextobj->data);
    
    if (objname)
    {
        drmskey = drms_keyword_lookup(rec, objname, 0);
        if (!drmskey)
        {
            drmsseg = drms_segment_lookup(rec, objname);
            if (!drmsseg)
            {
                fprintf(stderr, "Unable to locate keyword/segment %s in series %s.\n", objname, series);
                rv = kIDErr_DRMS;
            }
        }   
    }
    else
    {
        fprintf(stderr, "The number of headers and the number of data columns do not match.\n");
        rv = kIDErr_InvalidArgs;
    }
    
    if (rv == kIDErr_Success)
    {
        if (drmskey)
        {
            dval.string_val = strdup(val);
            
            /* Data type of DRMS_Type_Value to use for setting keyword. */
            dstat = drms_setkey(rec, objname, DRMS_TYPE_STRING, &dval);
            
            if (dstat != DRMS_SUCCESS)
            {
                fprintf(stderr, "Unable to set key %s with value %s, source line number %d, DRMS returned %d.\n", objname, val, lineno, dstat);
                rv = kIDErr_DRMS;
            }
            else
            {
                if (dval.string_val)
                {
                    free(dval.string_val);
                    dval.string_val = NULL;
                }
            }
        }
        else if (drmsseg)
        {
            fprintf(stderr, "Ingestion of DRMS segments is not currently supported.\n");
            rv = kIDErr_SegNotSupported;
        }
    }
    
    return rv;
}

int DoIt(void) 
{
    IDError_t rv = kIDErr_Success;
    int dstat = DRMS_SUCCESS;
    FILE *dfptr = NULL;
    const char *series = NULL;
    const char *dfile = NULL;
    DRMS_RecordSet_t *recset = NULL;
    int nrecs;
    int irec;
    
    series = cmdparams_get_str(&cmdparams, kArgSeries, &dstat);
    dfile = cmdparams_get_str(&cmdparams, kArgDatafile, &dstat);
    
    if (!drms_series_exists(drms_env, series, &dstat) || dstat != DRMS_SUCCESS)
    {
        fprintf(stderr, "Series %s does not exist.\n", series);
        rv = kIDErr_NoSeries;
    }
    
    if (rv == kIDErr_Success)
    {
        dfptr = OpenDataStream(dfile, &rv);
    }
    
    if (rv == kIDErr_Success)
    {
        char lineBuf[LINE_MAX];
        size_t len;
        char *fullline = NULL;
        size_t szFL;
        char *pline = NULL;
        DRMS_Record_t *rec = NULL;
        int lineno;
        char header[DRMS_MAXKEYNAMELEN];
        char *pheader = NULL;
        char val[128];
        char *pval = NULL;
        LinkedList_t *objlist = NULL;
        
        nrecs = 0;
        lineno = 1;
        
        /* Loop through input lines. */
        while (!(fgets(lineBuf, LINE_MAX, dfptr) == NULL))
        {
            /* strip \n from end of lineBuf */
            len = strlen(lineBuf);
            fullline = strdup(lineBuf);
            szFL = len + 1;
            
            if (len == LINE_MAX - 1)
            {
                /* We filled-up our buffer. There may be more chars to read on this line. */
                while (!(fgets(lineBuf, LINE_MAX, dfptr) == NULL))
                {
                    fullline = base_strcatalloc(fullline, lineBuf, &szFL);
                    
                    if (strlen(lineBuf) > 1 && lineBuf[strlen(lineBuf) - 1] == '\n')
                    {
                        break;
                    }
                }
            }
            
            len = strlen(fullline);
            
            if (fullline[len - 1] == '\n')
            {
                fullline[len - 1] = '\0';
            }
            
            /* The first line has the keyword/segment header. */
            if (lineno == 1)
            {
                if (!objlist)
                {
                    objlist = list_llcreate(DRMS_MAXNAMELEN, (ListFreeFn_t)NULL);
                }
                
                if (!objlist)
                {
                    rv = kIDErr_OutOfMemory;
                    break;
                }
                
                /* Must parse line for keyword/segment values. Values are white-spaced separated. */
                for (pheader = header, pline = fullline; *pline != '\0';)
                {
                    if (IsWS(pline))
                    {
                        if (*header != '\0')
                        {
                            /* Ready to insert the next keyword. */
                            *pheader = '\0';
                            list_llinserttail(objlist, header);
                            *header = '\0';
                            pheader = header;
                        }
                        
                        /* We're in the middle of parsing whitespace - skip this char. */
                        pline++;
                    }
                    else
                    {
                        *pheader++ = *pline++;
                    }
                }
                
                /* There is likely a last keyword on fullline that hasn't been processed yet. */
                if (*header != '\0')
                {
                    /* Ready to insert the last keyword. */
                    *pheader = '\0';
                    list_llinserttail(objlist, header);
                    *header = '\0';
                }
                
                lineno++;
                continue;
            } /* lineno == 1 */

            /* If we need to allocate a new chunk of records, then do that. */
            irec = nrecs % kChunkSize;
            if (irec == 0)
            {
                /* Time for a new chunk of records. First, close the existing chunk of records. */
                if (recset != NULL)
                {
                    if (drms_close_records(recset, DRMS_INSERT_RECORD) != DRMS_SUCCESS)
                    {
                        fprintf(stderr, "Failure inserting records into the database.\n");
                        rv = kIDErr_DRMS;
                        break;
                    }
                    
                    recset = NULL;
                }

                /* Then create the new chunk. */
                recset = drms_create_records(drms_env, kChunkSize, series, DRMS_PERMANENT, &dstat);
                if (!recset || dstat != DRMS_SUCCESS)
                {
                    rv = kIDErr_DRMS;
                    break;
                }
            }
            
            rec = recset->records[irec];
            
            /* Populate a record with the data provided in the data file. Must parse fullline to 
             * extract keyword and segment values. */
            for (pval = val, pline = fullline, list_llreset(objlist); *pline != '\0';)
            {
                if (IsWS(pline))
                {
                    if (*val != '\0')
                    {
                        /* Ready to set keyword/segment value. */
                        *pval = '\0';
                        rv = SetValue(val, objlist, rec, series, lineno);
                        if (rv != kIDErr_Success)
                        {
                            break;
                        }

                        /* Reset keyword-value buffer for next keyword. */
                        *val = '\0';
                        pval = val;
                    }
                    
                    /* We're in the middle of parsing whitespace - skip this char. */
                    pline++;
                }
                else
                {
                    *pval++ = *pline++;
                }
            }
            
            if (rv != kIDErr_Success)
            {
                break;
            }
            
            if (*val != '\0')
            {
                *pval = '\0';
                rv = SetValue(val, objlist, rec, series, lineno);
                *val = '\0';
                if (rv != kIDErr_Success)
                {
                    break;
                }
            }
            
            /* Set the date and datestr keywords. */
            /* Get current date */
            {
                time_t timenow;
                struct tm *ltime;
                char tbuf[128];
                
                time(&timenow);
                ltime = localtime (&timenow);
                strftime(tbuf, sizeof(tbuf) - 1, "%Y.%m.%d_%H:%M:%S_%Z", ltime);
                
                if (drms_keyword_lookup(rec, kDateKeyName, 0))
                {
                    /* Set the DATE_imp keyword to this value */
                    if (drms_setkey_string(rec, kDateKeyName, tbuf))
                    {
                        fprintf(stderr, "Couldn't set date keyword.\n");
                        rv = kIDErr_DRMS;
                        break;
                    }
                }
                
                if (drms_keyword_lookup(rec, kDateStrKeyName, 0))
                {
                    if (drms_setkey_string(rec, kDateStrKeyName, tbuf))
                    {
                        fprintf(stderr, "Couldn't set datestr keyword.\n");
                        rv = kIDErr_DRMS;
                        break;
                    }
                }
            }
            
            lineno++;
            nrecs++;
        } /* loop through data-file input lines */
    }
    
    if (rv == kIDErr_Success)
    {
        /* May have uncommitted records - the commit code operates on chunks, and it cannot tell when the last chunk is partially processed. 
         * recset is a chunk of records, and some may not be used. nrecs % kChunkSize tells us how many records have been used. Must free
         * (DRMS_FREE_RECORD) the unused records, and insert (DRMS_INSERT_RECORD) the used records. */
        if (recset != NULL && recset->n > 0)
        {
            DRMS_RecordSet_t *final = NULL;
            
            final = malloc(sizeof(DRMS_RecordSet_t));
            if (final)
            {
                memset(final, 0, sizeof(DRMS_RecordSet_t));
                
                /* Merge into final. */
                for (irec = 0; irec < (nrecs % kChunkSize); irec++)
                {
                    drms_merge_record(final, recset->records[irec]);
                    recset->records[irec] = NULL;
                }
                
                /* final now owns the records recset used to own. */
                
                /* final contains cached records, so it must point to the environment to which these records belong. */
                final->env = recset->env;
                
                if (drms_close_records(recset, DRMS_FREE_RECORD))
                {
                    fprintf(stderr, "Failure inserting records into the database.\n");
                    rv = kIDErr_DRMS;
                }
                else
                {
                    if (drms_close_records(final, DRMS_INSERT_RECORD) != DRMS_SUCCESS)
                    {
                        fprintf(stderr, "Failure inserting records into the database.\n");
                        rv = kIDErr_DRMS;
                    }
                }

                final = NULL;
                recset = NULL;
            }
            else
            {
                rv = kIDErr_OutOfMemory;
            }
        }
    }
    
    if (dfptr != stdin)
    {
        fclose(dfptr);
    }
    
    return rv;
}
