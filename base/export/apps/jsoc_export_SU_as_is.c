/*
 *  jsoc_export_SU_as_is - Generates index.XXX files for SUMS storage unit export.
 *
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include "printk.h"

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "Not Specified", "<Operation>"},
  {ARG_STRING, "ds", "Not Specified", "<list of sunums>"},
  {ARG_STRING, "requestid", "Not Specified", "RequestID string for export management"},
  {ARG_STRING, "method", "url", "Export method"},
  {ARG_STRING, "protocol", "as-is", "export file protocol"},
  {ARG_STRING, "format", "json", "export communication protocol"},
  {ARG_FLAG, "h", "0", "help - show usage"},
  {ARG_FLAG, "z", "0", "emit JSON output"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
  {ARG_END}
};

char *module_name = "jsoc_export_SU_as_is";
int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\njsoc_info {-h} {ds=<sunum list>}\n"
        "  details are:\n"
	"ds=<sunum list> comma delimited list of storage units\n"
        "requestid= RequestID string for export management\n"
        "method = Export method, default to url\n"
        "protocol = export file protocol, default to as-is\n"
        "format = export communication protocol, default to json\n"
	);
    return(1);
    }
  return (0);
  }

#define DIE(msg) \
  {	\
  fprintf(index_txt,"status=1\n");	\
  fprintf(index_txt, "error='%s'\n", msg);	\
  fclose(index_txt); \
  if (my_sum) \
    SUM_close(my_sum,printkerr); \
  return(1);	\
  }

TIME timenow()
  {
  TIME UNIX_epoch = -220924792.000; /* 1970.01.01_00:00:00_UTC */
  TIME now = (double)time(NULL) + UNIX_epoch;
  return(now);
  }
  
  
void freeInfostructs(SUM_info_t ***infostructs, int count)
{
    int iinfo;
    
    if (infostructs && *infostructs)
    {
        for (iinfo = 0; iinfo < count; iinfo++)
        {
            if ((*infostructs)[iinfo])
            {
                free((*infostructs)[iinfo]);
                (*infostructs)[iinfo] = NULL;
            }
        }
        
        free(*infostructs);
        *infostructs = NULL;
    }
}

void freeOutList(char ***outList, int num)
{
    int iinfo;
    
    for (iinfo = 0; iinfo < num; iinfo++)
    {
        if ((*outList)[iinfo])
        {
            free((*outList)[iinfo]);
            (*outList)[iinfo] = NULL;
        }
    }
    
    free(*outList);
    *outList = NULL;
}

static int nextInfoIndex(char **list)
{
    static char **current = NULL;
    
    if (!current)
    {
        current = list;
    }
    
    while (current)
    {
        current++;
    }
    
    return current - list;
}

/* Module main function. */
int DoIt(void)
  {
  char *sunumlist;
  char *this_sunum;
  const char *requestid;
  const char *method;
  const char *protocol;
  const char *format;

  int count;
  int status = DRMS_SUCCESS;
  long long size;
  FILE *index_txt = NULL;
  char buf[2*DRMS_MAXPATHLEN];
  char *cwd;
  TIME now = timenow();

    int64_t *sunumList = NULL; /* array of 64-bit sunums provided in the'ds=...' argument. */
    int y,m,d,hr,mn;
    char sutime[64];
    int online;
    int ioff;
    int noff;
    SUM_info_t **infostructs = NULL;
    int iinfo;
    SUM_info_t *suinfo = NULL;
    int retention;
    char **outList = NULL;
    DRMS_SuAndSeries_t *offlineSUs = NULL;
    int nextIInfo;
    long long *offlineSUList = NULL;

  if (nice_intro ()) return (0);
  
  count = cmdparams_get_int64arr(&cmdparams, "ds", &sunumList, &status);

  if (status != CMDPARAMS_SUCCESS || !sunumList)
  {
     fprintf(stderr, "Invalid argument 'sunum=%s'.\n", cmdparams_get_str(&cmdparams, "ds", NULL));
     return 1;
  }
  
  requestid = cmdparams_get_str(&cmdparams, "requestid", NULL);
  format = cmdparams_get_str(&cmdparams, "format", NULL);
  method = cmdparams_get_str(&cmdparams, "method", NULL);
  protocol = cmdparams_get_str(&cmdparams, "protocol", NULL);

  /* loop through list of storage units */
  size = 0;
  
    /* This used to make one direct SUM_info() call for each SU. Changed to use drms_getsuinfo(). This call 
     * batches the SUs by using SUM_infoArray(). */
    if (count > 0)
    {     
        infostructs = (SUM_info_t **)calloc(count, sizeof(SUM_info_t *));
        
        if (!infostructs)
        {
            fprintf(stderr, "Out of memory.\n");
            free(sunumList); 
            
            return 1;
        }
        
        status = drms_getsuinfo(drms_env, (long long *)sunumList, count, infostructs);

        if (status != DRMS_SUCCESS)
        {
            fprintf(stderr, "Unable to get SUMS information for specified SUs.\n");
            
            freeInfostructs(&infostructs, count);
            free(sunumList);

            return 1;
        }
        
        outList = calloc(count, sizeof(char *));
        if (!outList)
        {
            fprintf(stderr, "Out of memory.\n");
            freeInfostructs(&infostructs, count);
            free(sunumList); 
            
            return 1;
        }

        for (iinfo = 0, ioff = 0; iinfo < count; iinfo++)
        {
            suinfo = infostructs[iinfo];
            if (suinfo && *suinfo->online_loc != '\0')
            {
                size += (long long)suinfo->bytes;
            
                /* Valid SU. */
                if (strcmp(suinfo->online_status, "Y") == 0)
                {
                    /* No checking for format errors here. */
                    sscanf(suinfo->effective_date, "%4d%2d%2d%2d%2d", &y, &m, &d, &hr, &mn);
                    snprintf(sutime, sizeof(sutime), "%4d.%02d.%02d_%02d:%02d", y, m, d, hr, mn);
                    retention = (sscan_time(sutime) - now)/86400.0;
                
                    if (retention >= 3)
                    {
                        /* Online already. */
                        snprintf(buf, sizeof(buf), "%lld\t%s\t%s\t%s\t%d\n", suinfo->sunum, suinfo->owning_series, suinfo->online_loc, "Y", (int)suinfo->bytes);
                        outList[iinfo] = strdup(buf);
                        online = 1;
                    }
                    else
                    {
                        online = 0;
                    }
                }
                else
                {
                    online = 0;
                }
            
                if (!online)
                {
                    if (strcmp(suinfo->archive_status, "N") == 0)
                    {
                        snprintf(buf, sizeof(buf), "%lld\t%s\t%s\t%s\t%d\n", suinfo->sunum, suinfo->owning_series, "NA", "X", (int)suinfo->bytes);
                        outList[iinfo] = strdup(buf);
                    }
                    else
                    {
                        /* Save these so that we can call SUM_get() via drms_getunits() and bring them back online. We have to sort these
                         * according to owning series. */
                        if (!offlineSUs)
                        {
                            offlineSUs = calloc(count, sizeof(DRMS_SuAndSeries_t)); /* overkill - at most there will be count offlineSUs (if they are all offline). */
                            if (!offlineSUs)
                            {
                                fprintf(stderr, "Out of memory.\n");
                                freeInfostructs(&infostructs, count);
                                free(sunumList); 
            
                                return 1;
                            }
                        }
                         
                        offlineSUs[ioff].sunum = suinfo->sunum;
                        offlineSUs[ioff].series = suinfo->owning_series; /* swiper no swiping! */
                        ioff++;
                    }
                }
            }
            else
            {
                /* Invalid SU. */
                snprintf(buf, sizeof(buf), "%lld\t%s\t%s\t%s\t%d\n", sunumList[iinfo], "NA", "NA", "I", 0);
                outList[iinfo] = strdup(buf);
            }
        }
        
        free(sunumList);
        sunumList = NULL;
        
        noff = ioff;
     
        if (noff > 0)
        {
            /* We want to call SUM_get() on all these SUs with the retrieve flag set, and dontwait set to false (the original semantics).
             * That means that this module could hang if a tape is offline.
             * 
             * There is no easy way to call SUM_get(), short of having to call drms_open_records(), drms_record_directory(), and 
             * drms_close_records(). ARGH says I! If we use drms_open_records(), then we have to group by series. We would have to 
             * create a record-set specification with <series>[! sunum=XXX !] in it. Instead, we can use drms_getunits_ex().
             * We simply have to provide an array of DRMS_SuAndSeries_t. We do not have an array of DRMS_SuAndSeries_t, just headless ones.
             */
            int bail = 0;
            
            /* suAndSeriesArray is now pointing into infostructs. */
            status = drms_getunits_ex(drms_env, noff, offlineSUs, 1, 0);
                        
            /* Clear out old infostructs. offlineSUs was pointing into infostructs, but we freed offlineSUs. */
            freeInfostructs(&infostructs, count);

            if (status != DRMS_SUCCESS)
            {
                fprintf(stderr, "Out of memory.\n");
                free(offlineSUs);
                freeOutList(&outList, count);

                return 1;
            }
            
            /* For some reason, the SUMS information obtained by drms_getunits_ex() gets discarded. Have to call drms_getsuinfo() again. Bail if any SUs are 
             * not valid. */
                                      
            /* Allocate infostructs for the next call of drms_getsuinfo() - for the SUs that were originally offline. */
            infostructs = (SUM_info_t **)calloc(noff, sizeof(SUM_info_t *));
        
            if (!infostructs)
            {
                fprintf(stderr, "Out of memory.\n");
                free(offlineSUs);
                freeOutList(&outList, count);

                return 1;
            }

            /* Must create list of previously offline SUs. */
            offlineSUList = calloc(noff, sizeof(long long));
            
            if (!offlineSUList)
            {
                fprintf(stderr, "Out of memory.\n");
                freeInfostructs(&infostructs, noff);
                free(offlineSUs);
                freeOutList(&outList, count);

                return 1;
            }
            
            for (ioff = 0; ioff < noff; ioff++)
            {
                offlineSUList[ioff] = offlineSUs[ioff].sunum;
            }
            
            free(offlineSUs);
            offlineSUs = NULL;

            status = drms_getsuinfo(drms_env, (long long *)offlineSUList, noff, infostructs);
            
            free(offlineSUList);
            offlineSUList = NULL;            

            if (status != DRMS_SUCCESS)
            {
                fprintf(stderr, "Unable to get SUMS information for specified SUs.\n");
                freeInfostructs(&infostructs, noff);
                freeOutList(&outList, count);

                return 1;
            }

            for (ioff = 0; iinfo < noff; ioff++)
            {
                suinfo = infostructs[ioff];
                
                if (suinfo && *suinfo->online_loc != '\0')
                {
                    /* Valid SU. */
                    if (strcmp(suinfo->online_status, "Y") == 0)
                    {
                        /* No checking for format errors here. */
                        sscanf(suinfo->effective_date, "%4d%2d%2d%2d%2d", &y, &m, &d, &hr, &mn);
                        snprintf(sutime, sizeof(sutime), "%4d.%02d.%02d_%02d:%02d", y, m, d, hr, mn);
                        retention = (sscan_time(sutime) - now)/86400.0;
                
                        if (retention < 3)
                        {
                            /* Offline. */
                            bail = 1;
                        }
                    }
                    else
                    {
                        /* Offline. */
                        bail = 1;
                    }
                }
                else
                {
                    /* Invalid. */
                    bail = 1;
                }
                
                if (bail)
                {
                    fprintf(stderr, "Unable to stage offline SUs.\n");
                    freeInfostructs(&infostructs, noff);
                    freeOutList(&outList, count);

                    return 1;
                }
                else
                {
                    snprintf(buf, sizeof(buf), "%lld\t%s\t%s\t%s\t%d\n", suinfo->sunum, suinfo->owning_series, suinfo->online_loc, "Y", (int)suinfo->bytes);
                    nextIInfo = nextInfoIndex(outList);
                    outList[nextIInfo] = strdup(buf);
                }
            }
            
            freeInfostructs(&infostructs, noff);
        }
    }

    if (noff < 1)
    {
        freeInfostructs(&infostructs, count);
    }

    index_txt = fopen("index.txt", "w");

    if (!index_txt)
    {
        fprintf(stderr, "Unable to open index.txt for writing.\n");
        
        return 1;
    }

    fprintf(index_txt, "# JSOC Export SU List\n");
    fprintf(index_txt, "version=1\n");
    fprintf(index_txt, "requestid=%s\n", requestid);
    fprintf(index_txt, "method=%s\n", method);
    fprintf(index_txt, "protocol=%s\n", protocol);
    fprintf(index_txt, "wait=0\n");
    fprintf(index_txt, "count=%d\n", count);
    fprintf(index_txt, "size=%lld\n", size);
    fprintf(index_txt, "status=0\n");
    
    cwd = getcwd(NULL, 0);
    fprintf(index_txt,"dir=%s\n", ((strncmp("/auto", cwd, 5) == 0) ? cwd + 5 : cwd));
    free(cwd);
    fprintf(index_txt, "# DATA SU\n");
    
    /* Finally, we have info for all SUs. */
    for (iinfo = 0; iinfo < count; iinfo++)
    {
        fprintf(index_txt, outList[iinfo]);
        free(outList[iinfo]);
        outList[iinfo] = NULL;
    }

    free(outList);
    outList = NULL;
    
    fclose(index_txt);
  
    return(0);
}
