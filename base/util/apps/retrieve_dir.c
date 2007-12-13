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

/**
\defgroup retrieve_dir retrieve_dir

Retrieve an arbitrary directory of files from SUMS. Typically used
to retrieve a directory stored in SUMS with \ref store_dir.

\a record_set is a database query that allows
the user to select a subset of records of a series. In particular, the
user can supply values for the \a sel and \a note keywords in \a record_set
to search for files saved with \ref store_dir.  \ref retrieve_dir
retrieves the set of records specified, and for each record uses the
value contained within the \a dirname_keyword keyword (defaults to
"dirname") to identify the subdirectory within the record's SUMS
directory (i.e., it is all of the string after the last '/' in this
value) that contains the record's files.  It then copies (or links, if
the \a -l flag is present) all files from this subdirectory to a
identically named subdirectory of the directory specified by \a dest.
\ref retrieve_dir will
overwrite any files existing in the destination subdirectory whose
name matches the name of a file to be copied from a SUMS subdirectory.

If the link flag, \a -l, is specified, then \ref retrieve_dir will create
a link to the SUMS subdirectory instead of copying from the SUMS
subdirectory to the destination subdirectory.  Over
time this link may become broken as SUMS may remove a stored file if
it is present in SUMS longer than its specified retention time.

\par Usage:

\code
retrieve_dir [-lDRIVER_FLAGS] series=<record_set> to=<dest> [dirkey=<dirname_keyword>]
\endcode

\b Example: to retrieve a directory of files:
\code
retrieve_dir series=su_arta.TestStoreDir to=/home/arta/restoredFilesJanuary/ dirkey=dirname
\endcode

\par Flags:
\c -l: Create a symbolic link to the SUMS  directory  containing the file(s) of interest.

Driver flags: \ref jsoc_main

\param record_set
A series name followed by an optional record set filter (i.e.,
\a name[\a filter]).  Causes selection of a subset of records in
the series.  Each record retrieved with this \a record_set query will
refer to one directory that will be copied to \a dest.

\param dest
The destination directory to which the SUMS subdirectory should be
copied/linked.  Do not append a final '/' to the path (i.e.,
/home/arta/dir1 is acceptable, but /home/arta/dir1/ is not) 

\param dirname_keyword
Name of the keyword containing the path of the directory originally
stored with \ref store_dir.

@{
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

/** @}*/
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
