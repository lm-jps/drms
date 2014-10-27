/* show_series - print list of series names with optional subselection and verbosity */

/* Usage:  show_series {-v}  
 *         
 *         List currently accessible series.
 *
 */

/**
\defgroup show_series show_series - lists currently available DRMS series
@ingroup drms_util

\brief List DRMS dataseries names.

\par Synopsis:
\code
show_series [-hpvzGEN_FLAGS] [<filter>]
\endcode

show_series lists the names of DRMS dataseries. If the \a -p flag is set,
it displays the prime-keyword names and series description. The
information displayed is restricted to a subset of DRMS series by
specifying \a filter, a grep-like regular expression.

\par Flags:
\c -h: Print usage message and exit
<rb>
\c -p: Print prime-keyword names and the series description
<rb>
\c -v: Verbose - noisy
<rb>
\c -z: Emit JSON instead of normal output
<rb>

\par GEN_FLAGS: 
Ubiquitous flags present in every module.
\ref jsoc_main

\param filter A pattern using grep-like rules to select a subset of
series

*/

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>
#include <sys/types.h>
#include <regex.h>

char * json_escape (char * text);

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

/* returns series owner as static string */
char *drms_getseriesowner(DRMS_Env_t *drms_env, char *series, int *status)
   {
   char *nspace = NULL;
   char *relname = NULL;

   DB_Text_Result_t *qres = NULL;
   static char owner[256];
   owner[0] = '\0';

   if (!get_namespace(series, &nspace, &relname))
      {
      char query[1024];
      strtolower(nspace);
      strtolower(relname);

      snprintf(query, sizeof(query), "SELECT pg_catalog.pg_get_userbyid(T1.relowner) AS owner FROM pg_catalog.pg_class AS T1, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '%s') AS T2 WHERE T1.relnamespace = T2.oid AND T1.relname = '%s'", nspace, relname);

      if ((qres = drms_query_txt(drms_env->session, query)) != NULL)
         {
         if (qres->num_cols == 1 && qres->num_rows == 1)
            strcpy(owner, qres->field[0][0]);
         db_free_text_result(qres);
         }
      }
   *status = owner[0] != 0;
   return(owner);
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
	"     if the filter begins with the word NOT it will exclude the patterns\n"
	"     following the 'NOT'\n"
	"  -h prints this message and quits.\n"
	"  -p enables print prime keys and description.\n"
	"  -v verbose \n"
	"  -z JSON formatted output \n");
    return (1);
  }
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
regex_t seriesfilter;
regmatch_t pmatch[10];
char *filter;
char *filterctx = NULL;
int filterNOT;
char *web_query;
int from_web;
int want_JSON;
int printinfo;
char NameSpace[1024];
int singleNameSpace = 0;
char *index();

if (nice_intro()) return(0);

web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
from_web = strcmp (web_query, "Not Specified") != 0;
want_JSON = from_web || cmdparams_get_int (&cmdparams, "z", NULL) != 0;

printinfo = cmdparams_get_int(&cmdparams, "p", NULL);

if (cmdparams_numargs(&cmdparams)==2)
  { /* series filter provided, treat as regular expression. */
  filterctx = strdup(cmdparams_getarg(&cmdparams, 1));
  filter = filterctx;
  }
else if (from_web && *web_query)
  {
  filterctx = web_query;
  web_query = NULL;
  filter = index(filterctx,'=')+1;
// fprintf(stderr,"raw filter=%s\n",filter);
  CGI_unescape_url(filter);
  }
else
  filter = NULL;
// fprintf(stderr,"filter=%s\n",filter);
    
if (web_query)
  {
  free(web_query);
  web_query = NULL;
}

if (filter)
  {
  int i;
  char *chtmp = NULL;
      
  // check for NOT method of exclusion
  if (strncmp(filter, "NOT ", 4)==0)
    {
    filterNOT = 1;
    filter += 4;
    }
  else
    {
    filterNOT = 0;
    for (i=1; filter[i]; i++) // do not test for '^.', would be empty namespace
      if (filter[i] == '.' && filter[i-1] == '\\')
         {
         if (singleNameSpace)
           {
           singleNameSpace = 0;
           break;
           }
         singleNameSpace = 1;
         strncpy(NameSpace, filter, i-1);
         NameSpace[i-1] = '\0';
         }
    }
  if (regcomp(&seriesfilter, filter, (REG_EXTENDED | REG_ICASE | REG_NOSUB)))
    {
    status = SS_FORMATERROR;
    goto Failure;
    }
      
    chtmp = strdup(filter);
    filter = chtmp;
    chtmp = NULL;
      
    free(filterctx);
    filterctx = NULL;
  }
  
// if (singleNameSpace) fprintf(stderr,"TEST of single name space found: %s\n",NameSpace);

/* Query the database to get all series names from the master list. */
if (singleNameSpace)
  { // NameSpace contains desired namespace to search -- NEEDS TO BE "INCLUDED IN QUERY
  if (printinfo || want_JSON)
    sprintf(query, "select seriesname, primary_idx, description from %s() order by seriesname", DRMS_MASTER_SERIES_TABLE);
  else
    sprintf(query, "select seriesname from %s() order by seriesname", DRMS_MASTER_SERIES_TABLE);
  }
else
  {
     int hasWl = 0;

#if WL_HASWL
     hasWl = 1;
#endif
     if (hasWl &&
         ((drms_env->session->db_direct && strcmp(SERVER, drms_env->session->db_handle->dbhost) == 0) ||
          (!drms_env->session->db_direct && strcmp(SERVER, drms_env->session->hostname) == 0)))
     {
        /* We are connected to the database that has the white-list table of merged series information. And we need to query on all series, so use drms.allseries. */
        if (printinfo || want_JSON)
        {
           snprintf(query, sizeof(query), "SELECT seriesname, primary_idx, description FROM drms.allseries ORDER BY seriesname");
        }
        else
        {
           snprintf(query, sizeof(query), "SELECT seriesname FROM drms.allseries ORDER BY seriesname");
        }
     }
     else
     {
        if (printinfo || want_JSON)
          sprintf(query, "select seriesname, primary_idx, description from %s() order by seriesname", DRMS_MASTER_SERIES_TABLE);
        else
          sprintf(query, "select seriesname from %s() order by seriesname", DRMS_MASTER_SERIES_TABLE);
     }
  }

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
  int regex_result;
  if (filter)
    regex_result = regexec(&seriesfilter, seriesname, 10, pmatch, 0);

  if (!filter || (!filterNOT && !regex_result) || (filterNOT && regex_result)) 
    {
    if (want_JSON)
      printf("%s  {\"name\":\"%s\",",(nused ? ",\n" : ""),seriesname);
    else
      printf("  %s\n",seriesname);

    if (printinfo || want_JSON)
      { /* fix-up prime key list and print */
      char *primary_idx = qres->field[iseries][1];
      char *description = qres->field[iseries][2];
      char *p;
      int npk = 0;
      if (want_JSON)
        printf("\"primekeys\":\""); 
      else
	printf("      Prime Keys are: ");
      for (p = strtok(primary_idx, ", "); p; p = strtok(NULL, ", "))
        {
	char *ind = strstr(p, "_index");
        if (ind)
          *ind = '\0';
        if (npk++)
          printf(", ");
        printf("%s",p);
        }
      if (want_JSON)
        printf("\",");
      if (verbose)
        {
        int drmsstatus;
        DRMS_Record_t *rec = drms_template_record(drms_env, seriesname, &drmsstatus);
        if (want_JSON)
          {
	  printf("\"archive\":\"%d\",",rec->seriesinfo->archive);
	  printf("\"retention\":\"%d\",",rec->seriesinfo->retention);
	  printf("\"tapegroup\":\"%d\",",rec->seriesinfo->tapegroup);
	  printf("\"unitsize\":\"%d\",",rec->seriesinfo->unitsize);
	  printf("\"owner\":\"%s\",",drms_getseriesowner(drms_env, seriesname, &drmsstatus));
          }
        else
          {
          printf("\n      Archive: %d", rec->seriesinfo->archive);
          printf("\n      Retention: %d", rec->seriesinfo->retention);
          printf("\n      Tapegroup: %d", rec->seriesinfo->tapegroup);
          printf("\n      Unitsize: %d", rec->seriesinfo->unitsize);
          printf("\n      Owner: %s", drms_getseriesowner(drms_env, seriesname, &drmsstatus));
          }
        }
      if (want_JSON)
	{
        char *desc_escaped = json_escape(description);
	printf("\"note\":\"%s\"}",desc_escaped);
	free(desc_escaped);
	}
      else
	{
        printf("\n");
        printf("      Note: %s", description);
        printf("\n");
	}
      }
    nused++;
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

if (filter)
{
    regfree(&seriesfilter);
    free(filter);
}

db_free_text_result(qres);

return status;

Failure:
if (want_JSON)
  printf("{\"status\":1,\"errmsg\":%s}\n", SS_ERRORS[status].errmsg);
else
  printf("show_keys error: %s\n",SS_ERRORS[status].errmsg);

return status;
}


char * json_escape (char * text)
  {
  char *output, *op;
  size_t i, length;
  char buffer[6];
  /* check if pre-conditions are met */
  assert (text != NULL);

  /* defining the temporary variables */
  length = strlen(text);
  op = output = (char *)malloc(length*6*sizeof(char)); //upper bound
  if (output == NULL)
	return NULL;
  for (i = 0; i < length; i++)
        {
	if (text[i] == '\\')
                {
		*output++ = '\\';
		*output++ = '\\';
                }
	else if (text[i] == '\"')
                {
		*output++ = '\\';
		*output++ = '"';
                }
	else if (text[i] == '/')
                {
		*output++ = '\\';
		*output++ = '/';
                }
	else if (text[i] == '\b')
                {
		*output++ = '\\';
		*output++ = 'b';
                }
	else if (text[i] == '\f')
                {
		*output++ = '\\';
		*output++ = 'f';
                }
	else if (text[i] == '\n')
                {
		*output++ = '\\';
		*output++ = 'n';
                }
	else if (text[i] == '\r')
                {
		*output++ = '\\';
		*output++ = 'r';
                }
	else if (text[i] == '\t')
                {
		*output++ = '\\';
		*output++ = 't';
                }
	else if (text[i] < 0)   /* non-BMP character */
                {
		*output++ = text[i];
                }
	else if (text[i] < 0x20)
                {
		sprintf(buffer,"\\u%4.4x",text[i]);
		strncpy(output,buffer,6);
		output += 6;
                }
	else
                {
		*output++ = text[i];
                }
        }
  *output = '\0';
  return (op);
  }
                                                                                                           
