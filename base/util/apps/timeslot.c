/* This module accepts as input a series name, the name of a time-slotted keyword in that series, and 
 * a time value for that keyword (a time string, like 2012.07.03_18:36:00_TAI). This module 
 * then prints the corresponding slot number (index value). Instead of providing a single time value, the user
 * can provide a file that contains a list of such values, in which case the module will print
 * a list of slot values.
 */

#include "jsoc_main.h"
#include "drms_types.h"
#include <sys/file.h>

char *module_name = "timeslot";

typedef enum
{
    kTSErrSuccess     = 0,
    kTSErrBadArgs     = 1,
    kTSErrCantOpenRec = 2,
    kTSErrOutOfMemory = 3,
    kTSErrConversion  = 4,
    kTSErrFileIO  = 5
} TSError_t;

#define kSeries      "series"
#define kTimeKey     "tkey"
#define kTimeVal     "tval"
#define kUndef       "undef"

ModuleArgs_t module_args[] =
{
    {ARG_STRING, kSeries,     NULL,  "The series with a time-slotted keyword."},
    {ARG_STRING, kTimeKey,    NULL,  "The time-slotted keyword."},
    {ARG_STRING, kTimeVal,    kUndef,  "The time value of the time-slotted keyword, or a file containing a list of such values."},
    {ARG_END}
};

int DoIt(void)
{
    TSError_t rv = kTSErrSuccess;
    int status;

    const char *series = cmdparams_get_str(&cmdparams, kSeries, &status);
    const char *tkey = cmdparams_get_str(&cmdparams, kTimeKey, &status);
    const char *tval = cmdparams_get_str(&cmdparams, kTimeVal, &status);
    
    /* Ensure this is a real series, and that it contains the keyword specified, and that the keyword
     * is a time-slotted keyword. */
    if (!drms_series_exists(drms_env, series, &status) || status != DRMS_SUCCESS)
    {
        fprintf(stderr, "Series %s does not exist.\n", series);
        rv = kTSErrBadArgs;
    }
    else
    {
        DRMS_Keyword_t *key = NULL;
        DRMS_Record_t *rec = NULL;
        
        rec = drms_template_record(drms_env, series, &status);
        
        if (!rec || status != DRMS_SUCCESS)
        {
            fprintf(stderr, "Series %s does not exist.\n", series);
            rv = kTSErrBadArgs;
        }
        else
        {
            if ((key = drms_keyword_lookup(rec, tkey, 0)) == NULL)
            {
                fprintf(stderr, "Invalid keyword %s.\n", tkey);
                rv = kTSErrBadArgs;
            }
            else
            {
                /* Ensure that key is a time-slotted keyword. */
                if (!drms_keyword_isslotted(key))
                {
                    fprintf(stderr, "Keyword %s is not a time-slotted keyword.\n", tkey);
                    rv = kTSErrBadArgs;
                }
                else
                {
                    DRMS_Value_t tin;
                    DRMS_Value_t sout;
                    LinkedList_t *list = NULL;
                    ListNode_t *ln = NULL;
                    TIME timetval;
                    struct stat stBuf;
                    FILE *stream = NULL;
                    
                    tin.type = DRMS_TYPE_TIME;
                    list = list_llcreate(sizeof(TIME), NULL);
                    
                    if (list == NULL)
                    {
                        rv = kTSErrOutOfMemory;
                    }
                    
                    if (rv == kTSErrSuccess)
                    {
                        if (strcmp(tval, kUndef) == 0)
                        {
                            /* No time value provided at all - read from stdin. */
                            stream = stdin;
                        }
                        else if (!stat(tval, &stBuf) && S_ISREG(stBuf.st_mode))
                        {
                            /* tval is a file containing a list of time values. Parse the file and create a list node
                             * for each item. */
                            stream = fopen(tval, "r");
                            if (!stream)
                            {
                                rv = kTSErrFileIO;
                            }
                        }
                    }
                    
                    if (rv == kTSErrSuccess)
                    {
                        if (stream == NULL)
                        {
                            /* tval is a single time value. */
                            timetval = sscan_time((char *)tval);
                            list_llinserttail(list, &timetval);
                        }
                        else
                        {
                            /* Parse the input stream. */
                            char rbuf[LINE_MAX];

                            while (!(fgets(rbuf, LINE_MAX, stream) == NULL))
                            {
                                timetval = sscan_time(rbuf);
                                list_llinserttail(list, &timetval);
                            }
                        }
                    }
                    
                    if (rv == kTSErrSuccess)
                    {
                        list_llreset(list);
                        while ((ln = (ListNode_t *)(list_llnext(list))) != NULL)
                        {
                            timetval = *((TIME *)(ln->data));
                            tin.value.time_val = timetval;
                            
                            if (drms_keyword_slotval2indexval(key, &tin, &sout, NULL) != DRMS_SUCCESS)
                            {
                                fprintf(stderr, "Unable to calculate slot number.\n");
                                rv = kTSErrConversion;
                            }
                            else
                            {
                                printf("%lld\n", sout.value.longlong_val);
                            }
                        }
                    }
                }
            }
        }
    }
    
    return rv;
}
