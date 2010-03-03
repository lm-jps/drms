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

SUM_t *my_sum=NULL;

SUM_info_t *drms_get_suinfo(long long sunum)
  {
  int status;
  if (my_sum && my_sum->sinfo->sunum == sunum)
    return(my_sum->sinfo);
  if (!my_sum)
    {
    if ((my_sum = SUM_open(NULL, NULL, printkerr)) == NULL)
      {
      printkerr("drms_open: Failed to connect to SUMS.\n");
      return(NULL);
      }
    }
  if (status = SUM_info(my_sum, sunum, printkerr))
    {
    printkerr("Fail on SUM_info, status=%d\n", status);
    return(NULL);
    }

  return(my_sum->sinfo);
  }

TIME timenow()
  {
  TIME UNIX_epoch = -220924792.000; /* 1970.01.01_00:00:00_UTC */
  TIME now = (double)time(NULL) + UNIX_epoch;
  return(now);
  }

/* Module main function. */
int DoIt(void)
  {
  char *in;
  char *sunumlist, *sunumlistptr;
  char *this_sunum;
  char *requestid;
  char *method;
  char *protocol;
  char *format;

  int count;
  long long sunum;
  int status=0;
  long long size;
  FILE *index_txt, *index_data;
  char buf[2*DRMS_MAXPATHLEN];
  char *cwd;
  TIME now = timenow();

  char mbuf[128];
  char onlinestat[128];
  int susize;
  char supath[DRMS_MAXPATHLEN];

  if (nice_intro ()) return (0);

  in = cmdparams_get_str (&cmdparams, "ds", NULL);
  requestid = cmdparams_get_str (&cmdparams, "requestid", NULL);
  format = cmdparams_get_str (&cmdparams, "format", NULL);
  method = cmdparams_get_str (&cmdparams, "method", NULL);
  protocol = cmdparams_get_str (&cmdparams, "protocol", NULL);

  index_txt = fopen("index.txt", "w");
  fprintf(index_txt, "# JSOC Export SU List\n");
  fprintf(index_txt, "version=1\n");
  fprintf(index_txt, "requestid=%s\n", requestid);
  fprintf(index_txt, "method=%s\n", method);
  fprintf(index_txt, "protocol=%s\n", protocol);
  fprintf(index_txt, "wait=0\n");

  /* loop through list of storage units */
  count = 0;
  size = 0;
  sunumlist = strdup(in);
  index_data = fopen("index.data", "w+");

  while (this_sunum = strtok_r(sunumlist, ",", &sunumlistptr))
    {
    SUM_info_t *sinfo;
    TIME expire;
    sunum = atoll(this_sunum);
    sunumlist = NULL;
    count += 1;
    
    susize = 0;
    memset(onlinestat, 0, sizeof(onlinestat));
    snprintf(supath, sizeof(supath), "NA");

    sinfo = drms_get_suinfo(sunum);
    if (!sinfo)
    {
       snprintf(mbuf, 
                sizeof(mbuf), 
                "Invalid sunum, drms_open_records call failed: owning series - '%s', sunum - '%lld.\n", 
                sinfo->owning_series, 
                sunum);
       fprintf(stderr, mbuf);
       *onlinestat = 'I';
    }
    else
    {
       if (strcmp(sinfo->online_status,"N")!=0)
       {
          int y,m,d,hr,mn;
          char sutime[50];
          sscanf(sinfo->effective_date,"%4d%2d%2d%2d%2d", &y,&m,&d,&hr,&mn);
          sprintf(sutime, "%4d.%02d.%02d_%02d:%02d", y,m,d,hr,mn);
          expire = (sscan_time(sutime) - now)/86400.0;
       }
       if (strcmp(sinfo->online_status,"N")==0 || expire < 3)
       {  // need to stage or reset retention time
          char query[DRMS_MAXQUERYLEN];
          char recpath[DRMS_MAXPATHLEN];
          char *slash;
          DRMS_RecordSet_t *rs;
          sprintf(query,"%s[! sunum=%lld !]", sinfo->owning_series, sunum);
          rs = drms_open_records(drms_env, query, &status);
          if (!rs || rs->n < 1)
          {
             snprintf(mbuf, 
                      sizeof(mbuf), 
                      "Invalid sunum, drms_open_records call failed: owning series - '%s', sunum - '%lld.\n", 
                      sinfo->owning_series, 
                      sunum);
             fprintf(stderr, mbuf);
             *onlinestat = 'I';
          }
          else
          {
             if (strcmp(sinfo->archive_status, "N") == 0)
             {
                *onlinestat = 'X';
             }
             else
             {
                drms_record_directory(rs->records[0], recpath, 1);

                if (strlen(recpath) == 0)
                {
                   *onlinestat = 'X';
                }
                else
                {
                   susize = (int)sinfo->bytes;
                   *onlinestat = 'Y';
                }

                strcpy(supath, recpath);
                slash = rindex(supath, '/');
                if (slash && strncmp(slash, "/S", 2) == 0)
                  slash[0] = '\0';
             }
          }

          drms_close_records(rs, DRMS_FREE_RECORD);
       }
       else
       {
          strcpy(supath, sinfo->online_loc);
          susize = (int)sinfo->bytes;
          *onlinestat = 'Y';
       }
    }

    fprintf(index_data, "%lld\t%s\t%s\t%s\t%d\n", sunum, sinfo->owning_series, supath, onlinestat, susize);
    size += sinfo->bytes;
    }

  fprintf(index_txt, "count=%d\n",count);
  fprintf(index_txt, "size=%lld\n",size);
  fprintf(index_txt, "status=0\n");
  cwd = getcwd(NULL, 0);
  fprintf(index_txt,"dir=%s\n", ((strncmp("/auto", cwd,5) == 0) ? cwd+5 : cwd));
  fprintf(index_txt, "# DATA SU\n");
  rewind(index_data);
  while (fgets(buf, DRMS_MAXPATHLEN*2, index_data))
    fputs(buf, index_txt);
  fclose(index_txt);
  fclose(index_data);
  unlink("index.data");
  
  if (my_sum)
    SUM_close(my_sum,printkerr);
  return(0);
}
