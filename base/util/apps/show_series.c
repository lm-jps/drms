/* show_series - print list of series names with optional subselection and verbosity */

/* Usage:  show_series {-v}  
 *         
 *         List currently accessible series.
 *
 */

/**
\defgroup show_series show_series
List all DRMS dataseries names.

show_series lists the names of DRMS dataseries. If the \a -p flag is set,
it displays the prime-keyword names and series description. The
information displayed is restricted to a subset of DRMS series by
specifying \a filter, a grep-like regular expression.

\par Synopsis:
\code
show_series [-hpvzDRIVER_FLAGS] [<filter>]
\endcode

\par Flags:
\c -h: Print usage message and exit
\par
\c -p: Print prime-keyword names and the series description
\par
\c -v: Verbose - noisy
\par
\c -z: Emit JSON instead of normal output

\par Driver flags: 
\ref jsoc_main

\param filter A pattern using grep-like rules to select a subset of
series

@{
*/

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>
#include <sys/types.h>
#include <regex.h>

static char x2c (char *what) {
  char digit;

  digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0'));
  return (digit);
}

static void CGI_unescape_url (char *url) {
  int x, y;

  for (x = 0, y = 0; url[y]; ++x, ++y) {
    if ((url[x] = url[y]) == '%') {
      url[x] = x2c (&url[y+1]);
      y += 2;
    }
  }
  url[x] = '\0';
}



/* Some important variables are pre-defined by main.  These include:
 *    DRMS_Env_t *drms_env;    DRMS session information.
 *    CmdParams_t *cmdparams;  Command line parameters.
 */
							 /*  arguments list  */
ModuleArgs_t module_args[] = { 
  {ARG_FLAG, "h", "0", "prints this message and quits."},   /* help flag, print usage and quit  */
  {ARG_FLAG, "p", "0", "enables print prime keys and description."},   /* print details */
  {ARG_FLAG, "v", "0", "verbose"},   /* verbose flag, normally do not use  */
  {ARG_FLAG, "z", "0", "JSON"},   /* generate output in JSON format */
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
  {ARG_END}
};

					    /* Module name presented to DRMS */
char *module_name = "show_series";
/** @} */
/* Some global variables for this module. */
int verbose = 0;

/* Check DRMS session status and calling arguments and set verbose variable */
int nice_intro() {
  int help = cmdparams_get_int (&cmdparams, "h", NULL);
  verbose = cmdparams_get_int (&cmdparams, "v", NULL);
  if (help) {
    printf("show_series gets information about JSOC data series.\n"
	"usage: show_series {filter} {-p} {-v}\n"
	"  filter is regular expression to select series names.\n"
	"  -h prints this message and quits.\n"
	"  -p enables print prime keys and description.\n"
	"  -v verbose \n"
	"  -z JSON formatted output \n");
    return (1);
  }
  if (verbose) cmdparams_printall (&cmdparams);
  return(0);
}

#define SS_OK	0
#define SS_FORMATERROR 1
#define SS_NODRMS 2

static struct show_Series_errors
  {
  int errcode;
  char errmsg[100];
  } SS_ERRORS[] =
    {
    SS_OK, "SUCCESS",
    SS_FORMATERROR, "Series filter format error",
    SS_NODRMS, "Cant find DRMS"
    };


/* Module main function. */
int DoIt (void)
{
int status = 0;

int iseries, nseries, nused;
char query[DRMS_MAXQUERYLEN];
DB_Text_Result_t *qres;
char seriesfilter[1024];
regmatch_t pmatch[10];
char *filter;
char *web_query;
int from_web;
int want_JSON;
int printinfo;
char *index();

if (nice_intro()) return(0);

web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
from_web = strcmp (web_query, "Not Specified") != 0;
want_JSON = from_web || cmdparams_get_int (&cmdparams, "z", NULL) != 0;

printinfo = cmdparams_get_int(&cmdparams, "p", NULL);

if (cmdparams_numargs(&cmdparams)==2)
  { /* series filter provided, treat as regular expression. */
  filter = cmdparams_getarg(&cmdparams, 1);
  }
else if (from_web && *web_query)
  {
  filter = index(web_query,'=')+1;
// fprintf(stderr,"raw filter=%s\n",filter);
  CGI_unescape_url(filter);
  }
else
  filter = NULL;
// fprintf(stderr,"filter=%s\n",filter);

if (filter)
  {
  if (regcomp((regex_t *)seriesfilter, filter, (REG_EXTENDED | REG_ICASE)))
    {
    status = SS_FORMATERROR;
    goto Failure;
    }
  }
  
/* Query the database to get all series names from the master list. */
sprintf(query, "select seriesname from %s()", DRMS_MASTER_SERIES_TABLE);
if ( (qres = drms_query_txt(drms_env->session, query)) == NULL)
  {
  status = SS_NODRMS;
  goto Failure;
  }

nseries = qres->num_rows;
// if (want_JSON) fprintf(stderr,"nseries=%d\n,nseries);

/*
 printf("%d series available.\n",nseries);
*/

if (want_JSON)
  {
  printf("Content-type: application/json\n\n");
  if (nseries)
    {
    printf("{\"status\":0,\n");
    printf(" \"names\":[\n");
    }
  }

nused = 0;
for (iseries=0; iseries<nseries; iseries++)
  {
  char *seriesname = qres->field[iseries][0];

  if (!filter || !regexec((regex_t *)seriesfilter, seriesname, 10, pmatch, 0)) 
    {
    if (want_JSON)
      printf("%s  {\"name\":\"%s\",",(nused ? ",\n" : ""),seriesname);
    else
      printf("  %s\n",seriesname);

    nused++;
    if (printinfo || want_JSON)
      { /* fetch series info and print */
      DRMS_Record_t *rec = drms_template_record(drms_env, seriesname, &status);
      if (!rec || status)
        {
        if (want_JSON)
	  printf("\"primekeys\":\"NOT KNOWN\",\"note\":\"SERIES READ ERROR\"\},\n"); 
        else
          printf("      Cant open series. status=%d\n", status);
        continue;
        }
      if (rec->seriesinfo->pidx_num)
        {
        int pidx;
        if (want_JSON)
	  printf("\"primekeys\":\""); 
        else
          printf("      Prime Keys are:");
        for (pidx = 0; pidx < rec->seriesinfo->pidx_num; pidx++)
          printf("%s %s", (pidx ? "," : ""), rec->seriesinfo->pidx_keywords[pidx]->info->name);
        if (want_JSON)
	  printf("\",");
        }
      else
        {
	if (want_JSON)
	  printf("\"primekeys\":\"NONE\","); 
	else
          printf("      No Prime Keys found.");
        }
      if (want_JSON)
	{
	printf("\"note\":\"%s\"}",rec->seriesinfo->description);
	}
      else
	{
        printf("\n");
        printf("      Note: %s", rec->seriesinfo->description);
        printf("\n");
	}
        
      drms_free_record(rec);
      }
    }
  }

if (want_JSON)
  {
  printf("],\n\"n\":%d}\n", nused);
  fflush(stdout);
  }
else 
  {
  if (filter)
    printf("%d series found the matching %s\n", nused, filter);
  }

db_free_text_result(qres);

return status;

Failure:
if (want_JSON)
  printf("{\"status\":1,\"errmsg\":%s}\n", SS_ERRORS[status]);
else
  printf("show_keys error: %s\n",SS_ERRORS[status]);
}
