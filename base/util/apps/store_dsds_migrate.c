/* store_dsds_migrate - stores a ds from DSDS into  DRMS/SUMS. */

/* Usage:
 *  store_dsds_migrate pname=<name> lname=<name> lnum=<number> sname=<name> 
 *                                  snum=<number> dirname=<dir> {note=<comment>}
 *         
 *     Store a dir into SUMS for a DSDS dataset of the given name.
 *     "dir" is the directory that will be copied in SUMS
 *     in the dataseries su_production.dsds_migrate.  
 *
 *     Multiple calls of store_dsds_migrate with the same 
 *     names and numbers will result in
 *     multiple records with the same prime key.  Only the most recent
 *     will be easily retrieved. 
 *
 * Example:
 *   store_dsds_migrate pname=mdi lname=lev1.5 lnum=2 sname=fd_V_01h snum=60000
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
  {ARG_END}
};

char *outputseries = "su_production.dsds_migrate";

/* Module name presented to DRMS. */
char *module_name = "store_dsds_migrate";

/* Some global variables for this module. */
int verbose = 0;

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

char *in, *pname, *lname, *sname, *note, *dirname;
char path[DRMS_MAXPATHLEN];
char cmd[DRMS_MAXPATHLEN+1024];
DRMS_Record_t *rec, *template;

if (nice_intro())
  return(0);

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

/* Now ready to make a new record and set keywords */
rec = drms_create_record(drms_env, outputseries, DRMS_PERMANENT, &status);
if (!rec || status)
    {
    printf("drms_create_record failed, series=%s, status=%d.  Aborting.\n", outputseries,status);
    return(status);
    }
if ((status = drms_setkey_string(rec, "dirname", dirname)))
   {
     printf("ERROR: drms_setkey_string failed for 'dirname'\n");
     return(1);
   }
if ((status = drms_setkey_string(rec, "pname", pname)))
   {
     printf("ERROR: drms_setkey_string failed for 'pname'\n");
     return(1);
   }
if ((status = drms_setkey_string(rec, "lname", lname)))
   {
     printf("ERROR: drms_setkey_string failed for 'lname'\n");
     return(1);
   }
if ((status = drms_setkey_int(rec, "lnum", lnum)))
   {
     printf("ERROR: drms_setkey_int failed for 'lnum'\n");
     return(1);
   }
if ((status = drms_setkey_string(rec, "sname", sname)))
   {
     printf("ERROR: drms_setkey_string failed for 'sname'\n");
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

