/* segment-file-name prints to stdout the basename of each file of each segment that the export system would generate for
 * the same record-set specification and file-format string.
 */
#include "drms.h"
#include "jsoc_main.h"
#include "exputil.h"


#define SPECIFICATION "spec"
#define FILENAME_FMT_STRING "format"
#define UNDEFINED "undefined"
#define DEFAULT_FORMAT "defformat"

typedef enum
{
    kSFN_STATUS_SUCCESS = 0,
    kSFN_STATUS_BADARGS = 1,
    kSFN_STATUS_RETRIEVE_RECORDS = 2
} kSFNStatus_t;

char *module_name = "segment-file-name";

/* Command line parameter values. */
ModuleArgs_t module_args[] = 
{
    { ARG_STRING, SPECIFICATION, "", "The record-set specification." },
    { ARG_STRING, FILENAME_FMT_STRING, DEFAULT_FORMAT, "OPTIONAL: The file-name format string." },
    { ARG_END }
};


int DoIt(void)
{
    kSFNStatus_t rv = kSFN_STATUS_SUCCESS;
    int drmsStat = DRMS_SUCCESS;
    DRMS_RecordSet_t *recSet = NULL;
    DRMS_Record_t *rec = NULL;
    DRMS_Segment_t *seg = NULL;
    DRMS_Segment_t *segLinked = NULL;
    DRMS_RecChunking_t cstat;
    int newchunk = 0;
    HIterator_t *iter = NULL;
    const char *spec = NULL;
    const char *ffmt = NULL;

    spec = cmdparams_get_str(&cmdparams, SPECIFICATION, &drmsStat);
    
    if (drmsStat != DRMS_SUCCESS)
    {
        rv = kSFN_STATUS_BADARGS;
        fprintf(stderr, "Failure parsing required argument '%s'.\n", SPECIFICATION);
    }
    else
    {
        ffmt = cmdparams_get_str(&cmdparams, FILENAME_FMT_STRING, &drmsStat);
        
        if (strcmp(ffmt, DEFAULT_FORMAT) == 0)
        {
            ffmt = NULL; /* the exputil library will use a default string of {seriesname}.{recnum:%%lld}.{segment} */
        }
    
        if (drmsStat != DRMS_SUCCESS)
        {
            rv = kSFN_STATUS_BADARGS;
            fprintf(stderr, "Failure parsing required argument '%s'.\n", FILENAME_FMT_STRING);
        }
        else
        {
            /* retrieve records */
            recSet = drms_open_recordset(drms_env, spec, &drmsStat);
            if (drmsStat != DRMS_SUCCESS)
            {
                fprintf(stderr, "Invalid record-set specification %s.\n", spec);
                rv = kSFN_STATUS_BADARGS;
            }
            else
            {
                ExpUtlStat_t expfn = kExpUtlStat_Success;
                char fileIn[DRMS_MAXPATHLEN];
                char basename[DRMS_MAXPATHLEN];

                while ((rec = drms_recordset_fetchnext(drms_env, recSet, &drmsStat, &cstat, &newchunk)) != NULL)
                {
                    if (drmsStat != DRMS_SUCCESS)
                    {
                       fprintf(stderr, "Unable to fetch next input record.\n");
                       rv = kSFN_STATUS_RETRIEVE_RECORDS;
                       break;
                    }

                    while ((seg = drms_record_nextseg(rec, &iter, 0)) != NULL)
                    {
                        if (seg->info->islink)
                        {
                            if ((segLinked = drms_segment_lookup(rec, seg->info->name)) == NULL)
                            {
                               fprintf(stderr, "Unable to locate target segment %s.\n", seg->info->name);
                               continue;
                            }
                        
                            /* fetch input seg file name */
                            drms_segment_filename(segLinked, fileIn);
                        }
                        else
                        {
                            segLinked = seg;
                            /* fetch input seg file name */
                            drms_segment_filename(seg, fileIn);
                        }    
                    
                        if ((expfn = exputl_mk_expfilename(seg, segLinked, ffmt, basename)) == kExpUtlStat_Success)
                        {
                            fprintf(stdout, "%lld\t%s\n", rec->recnum, basename);
                        }
                        else if (expfn == kExpUtlStat_InvalidFmt)
                        {
                            fprintf(stderr, "Invalid file-format string %s.\n", ffmt);
                            fprintf(stdout, "%lld\tN/A\n", rec->recnum);
                            continue;
                        }
                        else if (expfn ==  kExpUtlStat_UnknownKey)
                        {
                            fprintf(stderr, "Unknown DRMS keyword in format string %s.\n", ffmt);
                            fprintf(stdout, "%lld\tN/A\n", rec->recnum);
                            continue;
                        }
                        else
                        {
                            fprintf(stderr, "Unable to successfully parse format string %s.\n", ffmt);
                            fprintf(stdout, "%lld\tN/A\n", rec->recnum);
                            continue;                        
                        }
                    } /* end segment loop */
                    
                    hiter_destroy(&iter);
                } /* end record loop */
            } /* good record-set specification */
            
            if (recSet)
            {
                drms_close_records(recSet, DRMS_FREE_RECORD);
            }
        } /* good args: specification */
    } /* good args: file format string */

    return rv;
}
