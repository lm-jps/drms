/* Retrieve_dir - retrieves a dir from SUMS saved with store_dir

/* Usage:  
 * retrieve_dir {-l} series=<seriesname>[<record_spec>] to=<target dir> 
 *			{dirkey=<dirname_keyword>}
 *         
 *	Retrieve a dir from SUMS.
 *	Dir usually archived with store_dir program.
 *	The <record_spec> should resolve to one or more records.  
 *	dirkey is the index keyword in the series that has the dir name.
 *	If this series was created by the store_dir program then the 
 *	dirkey is "dirname" and this is the default.
 *	Each matched record will have the corresponding dir retrieved. 
 *	If a note is defined for the record
 *	it will be printed after the dir name.  Thus one can e.g.
 *
 *   store_dir series=su_jim.storedirs dirname=/home/xx sel=test1 note="junk"
 *
 *     will be retrieved with simply:
 *
 *  retrieve_dir su_jim.storedirs[/home/xx] to=/tmp     or
 *  retrieve_dir su_jim.storedirs[][test1] to=/tmp
 *
 *    flags:  -l make link into SUMS.
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>


/* Some important variables are pre-defined by main.  These include:
 *    DRMS_Env_t *drms_env;    DRMS session information.
 *    CmdParams_t *cmdparams;  Command line parameters.
 */
							 /*  arguments list  */
ModuleArgs_t module_args[] = { 
 {ARG_STRING, "series", "", "Series name to retrieve from"},
 {ARG_STRING, "to", "", "Target dir to cp to"},
 {ARG_STRING, "dirkey", "dirname", "Keyword containing the dir name"},
 {ARG_FLAG, "l", "0", "Link instead to the retrieved dir in the SUMS storage"}, 
 {ARG_END, NULL, NULL} /* List must end in {NULL, NULL}. */
};
					    /* Module name presented to DRMS */
char *module_name = "retrieve_dir";

/* Some global variables for this module. */
int verbose = 0;

/* Module main function. */
int DoIt(void)
{
int status = 0;

char *series;
DRMS_RecordSet_t *recordset;
int irec;
int wantlink = cmdparams_get_int(&cmdparams, "l", NULL);
//int force = cmdparams_get_int(&cmdparams, "f", NULL);
char *destination;
char *dirkey;	/* index key in the series */

series = cmdparams_get_str(&cmdparams, "series", NULL);
destination = cmdparams_get_str(&cmdparams, "to", NULL);
dirkey = cmdparams_get_str(&cmdparams, "dirkey", NULL);


recordset = drms_open_records(drms_env, series, &status);
if (status)
  {
  fprintf(stderr,"retrieve_dir target NOT_FOUND, drms_open_records failed, series=%s, status=%d.\n"
	"Aborting.\n", series,status);
  return(1);
  }
if (recordset->n > 1)
  printf("%d records matched.\n", recordset->n);

for (irec = 0; irec< recordset->n; irec++)
  {
  DRMS_Record_t *rec = recordset->records[irec];  /* pointer to current record */
  char path[DRMS_MAXPATHLEN];
  char *dirname, *cptr;

  dirname = drms_getkey_string(rec, dirkey, &status);
  drms_record_directory(rec, path, 1);
  if (*path && !status)
    { /* target dir located in SUMS */
    char cmd[DRMS_MAXPATHLEN+1024];
    cptr = rindex(dirname, '/');
    if(wantlink)
      sprintf(cmd, "ln -s %s%s %s", path, cptr, destination);
    else
      sprintf(cmd, "cp -r %s%s %s", path, cptr, destination);
    printf("cmd = %s\n", cmd); /* !!!TEMP */
    if (system(cmd))
      {
      fprintf(stderr, "retrieve_dir failed on command: %s\n",cmd);
      return(1);
      }
      char *note = drms_getkey_string(rec, "note", &status);
      printf("%s %s to %s",dirname, (wantlink ? "linked" : "retrieved"), destination);
      if (note && *note)
	printf(", note = %s\n", note);
      else
        printf(".\n");
    }
  else
    {
    printf("retrieve_dir target dir NOT_FOUND, no path or %s keyword not found in %s, abort.\n", dirkey,series);
    status = 1;
    }
  }

return(status);
}
