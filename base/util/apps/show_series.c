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
show_series [-hpvDRIVER_FLAGS] [<filter>]
\endcode

\par Flags:
\c -h: Print usage message and exit
\par
\c -p: Print prime-keyword names and the series description
\par
\c -v: Verbose - noisy

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


/* Some important variables are pre-defined by main.  These include:
 *    DRMS_Env_t *drms_env;    DRMS session information.
 *    CmdParams_t *cmdparams;  Command line parameters.
 */
							 /*  arguments list  */
ModuleArgs_t module_args[] = { 
  {ARG_FLAG, "h", "0", "prints this message and quits."},   /* help flag, print usage and quit  */
  {ARG_FLAG, "p", "0", "enables print prime keys and description."},   /* print details */
  {ARG_FLAG, "v", "0", "verbose"},   /* verbose flag, normally do not use  */
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
	"  -v verbose \n");
    return (1);
  }
  if (verbose) cmdparams_printall (&cmdparams);
  return(0);
}

/* Module main function. */
int DoIt (void) {
int status = 0;

int iseries, nseries, nused;
char query[DRMS_MAXQUERYLEN];
DB_Text_Result_t *qres;
char seriesfilter[1024];
regmatch_t pmatch[10];
char *filter;
int printinfo = cmdparams_get_int(&cmdparams, "p", NULL);

if (nice_intro())
  return(0);

if (cmdparams_numargs(&cmdparams)==2)
  { /* series filter provided, treat as regular expression. */
  filter = cmdparams_getarg(&cmdparams, 1);
  if (regcomp((regex_t *)seriesfilter, filter, (REG_EXTENDED | REG_ICASE)))
    {
    fprintf(stderr,"Series filter format error in %s\n", filter);
    return 1;
    }
  }
else
  filter = NULL;
  
/* Query the database to get all series names from the master list. */
sprintf(query, "select seriesname from %s()", DRMS_MASTER_SERIES_TABLE);
if ( (qres = drms_query_txt(drms_env->session, query)) == NULL)
  {
  fprintf(stderr, "Cant find DRMS\n");
  return 1;
  }

nseries = qres->num_rows;
/*
 printf("%d series available.\n",nseries);
*/

nused = 0;
for (iseries=0; iseries<nseries; iseries++)
  {
  char *seriesname = qres->field[iseries][0];

  if (!filter || !regexec((regex_t *)seriesfilter, seriesname, 10, pmatch, 0)) 
    {
    nused++;
    printf("  %s\n",seriesname);
    if (printinfo)
      { /* fetch series info and print */
      DRMS_Record_t *rec = drms_template_record(drms_env, seriesname, &status);
      if (!rec || status)
        {
        printf("      Cant open series. status=%d\n", status);
        continue;
        }
      if (rec->seriesinfo->pidx_num)
        {
        int pidx;
        printf("      Prime Keys are:");
        for (pidx = 0; pidx < rec->seriesinfo->pidx_num; pidx++)
          printf("%s %s", (pidx ? "," : ""), rec->seriesinfo->pidx_keywords[pidx]->info->name);
        }
      else
        printf("      No Prime Keys found.");
      printf("\n");
      printf("      Note: %s", rec->seriesinfo->description);
      printf("\n");
      drms_free_record(rec);
      }
    }
  }

if (filter)
  printf("%d series found the matching %s\n", nused, filter);

db_free_text_result(qres);

return status;
}
