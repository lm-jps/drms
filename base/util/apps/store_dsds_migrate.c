/* store_dsds_migrate - stores a ds from DSDS into  DRMS/SUMS. */

/* Usage:
 *  store_dsds_migrate pname=<name> lname=<name> lnum=<number> sname=<name> 
 *                                  snum=<number> dirname=<dir> {note=<comment>}
 *         
 *     Store a dir into SUMS for a DSDS dataset of the given name.
 *     "dir" is the directory that will be copied in SUMS.
 *
 * The DRMS series is in name space 'dsds'. So, for example, the dsds ds
 * prog:mdi,level:lev1.5[4],series:fd_V_01h[60000] will be ingested into:
 *
 * dsds.mdi__lev1_5__fd_V_01h
 *
 * where PrimeKeys in the .jsd is the series#, and the level# is a DBIndex.
 * Note the double '__' to separate the fields.
 *
 * Example:
 *   store_dsds_migrate pname=mdi lname=lev1.5 lnum=4 sname=fd_V_01h snum=60000
 *			dirname=/PDS83/D9666638
 *
 */
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>


#define NOTSPECIFIED "***NOTSPECIFIED***"
/* List of default parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_STRING, "pname", "", "prog: name to store the dir into"},
  {ARG_STRING, "lname", "", "level: name to store the dir into"},
  {ARG_INT, "lnum", "0", ""}, /* level: number (i.e. a version #) */
  {ARG_STRING, "sname", "", "series: name to store the dir into"},
  {ARG_INT, "snum", "-1", ""}, /* series: number */
  {ARG_STRING, "dirname", "", "Dir to store into SUMS"},
  {ARG_STRING, "note", "N/A", "comment field"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_FLAG, "j", "0", "only print the jsd"},
  {ARG_END}
};

char *outputseries = "su_production.dsds_migrate";

/* Module name presented to DRMS. */
char *module_name = "store_dsds_migrate";

/* Some global variables for this module. */
int verbose = 0;
int jsdflg = 0;

/* Check DRMS session status and calling arguments and set verbose variable */
int nice_intro () {
  verbose = cmdparams_get_int (&cmdparams, "v", NULL);
  if (verbose) cmdparams_printall (&cmdparams);
  return(0);
}

/* Module main function. */
int DoIt (void) {
int status = 0;
int lnum, snum;

char *in, *pname, *lname, *sname, *note, *dirname, *cptr;
char path[DRMS_MAXPATHLEN];
char drmsseries[256];
char cmd[DRMS_MAXPATHLEN+1024];
DRMS_Record_t *rec, *template;

if (nice_intro())
  return(0);

jsdflg = cmdparams_get_int (&cmdparams, "j", NULL);
pname = cmdparams_get_str(&cmdparams, "pname", NULL);
lname = cmdparams_get_str(&cmdparams, "lname", NULL);
sname = cmdparams_get_str(&cmdparams, "sname", NULL);
note = cmdparams_get_str(&cmdparams, "note", NULL);
lnum = cmdparams_get_int (&cmdparams, "lnum", NULL);
snum = cmdparams_get_int (&cmdparams, "snum", NULL);
in = cmdparams_get_str(&cmdparams, "dirname", NULL);
/* First, check to see that the dir exists */
if (access(in, R_OK) != 0)
  {
  printf("The requested dir can not be accessed.  %s\n", in);
  return(1);
  }
  dirname = in;

/* Check to see if series exists, and if we should make it */
cptr = (char *)index(lname, '.'); //no dsds lname has more then one '.'
if(cptr) { 
  *cptr = '_';		//replace '.' with '_'
}
sprintf(drmsseries, "dsds.%s__%s__%s", pname, lname, sname);
if(jsdflg) {
  printf("%s", make_series_jsd(drmsseries));
  return(1);
}
template = drms_template_record(drms_env, drmsseries, &status);
if (template==NULL && status == DRMS_ERROR_UNKNOWNSERIES)
  {
    int create_series(char *series);
    printf("Going to make series: %s\n", drmsseries);
    if (create_series(drmsseries))
      {
      printf("Series '%s' does not exist. Create_series failed. Give up.\n", drmsseries);
      return(1);
      }
  }
else
  if(status)
    {
    fprintf(stderr, "DRMS problem looking up drmsseries.\n");
    return(status);
    }

/* Now ready to make a new record and set keywords */
rec = drms_create_record(drms_env, drmsseries, DRMS_PERMANENT, &status);
if (!rec || status)
    {
    printf("drms_create_record failed, series=%s, status=%d.  Aborting.\n", drmsseries,status);
    return(status);
    }
if ((status = drms_setkey_string(rec, "dirname", dirname)))
   {
     printf("ERROR: drms_setkey_string failed for 'dirname'\n");
     return(1);
   }
if ((status = drms_setkey_int(rec, "lnum", lnum)))
   {
     printf("ERROR: drms_setkey_int failed for 'lnum'\n");
     return(1);
   }
if ((status = drms_setkey_int(rec, "snum", snum)))
   {
     printf("ERROR: drms_setkey_int failed for 'snum'\n");
     return(1);
   }
if ((status = drms_setkey_string(rec, "note", note))) 
   {
     printf("WARNING: drms_setkey_string failed for 'note'\n");
     printf("Your note was not saved: %s\n",note);
   }
/* a SU should have been allocated since there is a segment in the series defn */
drms_record_directory(rec, path, 1);
if (! *path)
  {
  fprintf(stderr,"no path found\n");
  return(1);
  }
sprintf(cmd, "cp -rp %s %s", in, path);
status = system(cmd);
if (status)
    {
    printf("Copy failed: %s\n", cmd);
    return(status);
    }
else
    printf("Command success: %s\n", cmd);
if ((status = drms_close_record(rec, DRMS_INSERT_RECORD)))
  printf("drms_close_record failed!\n");

return(status);
}

char * make_series_jsd(char *series)
  {
  char *user = getenv("USER");
  char jsd[2048];
  sprintf(jsd,
    "Seriesname:     %s\n"
    "Author:         %s\n"
    "Owners:         %s\n"
    "Unitsize:       1\n"
    "Archive:        1\n"
    "Retention:      270\n"
    "Tapegroup:      9\n"
    "PrimeKeys:      snum\n"
    "DBIndex:        snum,lnum\n"
    "Description:    \"DSDS migrated dataset\"\n"
    "Keyword: dirname,  string, variable, record, \" \",  %%s, none, \"Original dirname\"\n"
    "Keyword: note,      string, variable, record, \" \",  %%s, none, \" \"\n"
    "Keyword: lnum,      int, variable, record, \"0\",  %%d, none, \"level: number\"\n"
    "Keyword: snum,      int, variable, record, \"-1\",  %%d, none, \"series: number\"\n"
    "Data: dir, constant, int, 0, none, generic, \" \"\n",
    series, user, user);
  return (strdup(jsd));
  }


int create_series(char *series)
  {
  char *jsd;
  int perm = cmdparams_get_int(&cmdparams, "perm", NULL);
  DRMS_Record_t *template;
  jsd = make_series_jsd(series);
  template = drms_parse_description(drms_env, jsd);
  if (template==NULL)
    {
    fprintf(stderr, "Failed to parse series description.  JSD was:\n%s\n",jsd);
    return(1);
    }
  if (drms_create_series(template, perm))
    {
    fprintf(stderr, "Failed to create series. JSD was:\n%s\n", jsd);
    return(1);
    }
  if (verbose)
    printf("Create_series successful, JSD was:\n%s\n", jsd);
  drms_free_record_struct(template);
  free(template);
  free(jsd);
  return(0);
  }
  
