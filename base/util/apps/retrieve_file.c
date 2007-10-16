/* Retrieve_file - retrieves a file from SUMS, needs record specified and segment if multiple segs */

/* Usage:  retrieve_file {-l}  ds=<seriesname>{[<record_spec>]} {segment=<segment_name>} out=<dir>
 *         
 *         Retrieve a file from SUMS.
 *         File usually archived with store_file program.
 *         The <record_spec> should resolve to one or more records.  Each matched
 *         record with a segment specified in the segment keyword or only
 *         a single segment with a file will have that file retrieved.
 *         The list of files retrieved will be on stdout.  If the file can not be found, the
 *         output will be NOT_FOUND.  If a note is defined for the record
 *         it will be printed after the filename.  Thus one can e.g.
 *
 *           store_file in=/tmp20/phil/file12345.fits ds=su_phil.savedfiles  note="my really important stuff"
 *
 *        Then, later:
 *
 *           retrieve_file su_phil.savedfiles[file12345.fits]
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

#define NOTSPECIFIED "###NOTSPECIFIED###"
							 /*  arguments list  */
ModuleArgs_t module_args[] = { 
  {ARG_STRING, "segment", NOTSPECIFIED, "", ""},   /* segment containing file to retrieve */
  {ARG_STRING, "ds", NOTSPECIFIED, "", ""},  /* record queryt - required. */
  {ARG_STRING, "out", NOTSPECIFIED, "", ""},  /* destination directory - required. */
  {ARG_FLAG, "f", "0", "", ""},    /* force - causes the restored file to replace an existing one, quietly */
  {ARG_FLAG, "h", "0", "", ""},    /* print usage message then quit */
  {ARG_FLAG, "l", "0", "", ""},    /* make symbolic links instead of copying files from SUMS */
  {ARG_FLAG, "v", "0", "", ""},   /* verbose flag, normally do not use  */
  {ARG_END, NULL, NULL} /* List must end in {NULL, NULL}. */
};
					    /* Module name presented to DRMS */
char *module_name = "retrieve_file";

/* Some global variables for this module. */
int verbose = 0;

/* Check DRMS session status and calling arguments and set verbose variable */
int nice_intro (int do_usage) {
  int help = cmdparams_get_int (&cmdparams, "h", NULL);
  verbose = cmdparams_get_int (&cmdparams, "v", NULL);
  if (help || do_usage) {
    printf("retrieve_file {-f} {-h} {-l} {-v} <seriesname>{[<record_spec>]} to={dest} {segment=<segment_name>}\n"
	"-f  force retreived file to overwrite existing file\n"
	"-h  print this message and exit\n"
	"-l  create symbolic link to SUMS archive instead of copying file.\n"
	"-v  verbose flag.\n"
	"ds=<seriesname> - series containing file to retrieve.\n"
	"<record_spec> - query to identify chosen record.\n"
	"out=  destination directory (required).\n"
        "segment=file - segment in series to use for getting file to retrieve.\n"
	"                   Not used if series has only one segment.\n");
    return(1);
    }
  if (verbose) cmdparams_printall (&cmdparams);
  return(0);
  }

/* Module main function. */
int DoIt(void)
{
int status = 0;

char *segname, *series;
DRMS_RecordSet_t *recordset;
int irec;
int wantlink = cmdparams_get_int(&cmdparams, "l", NULL);
int force = cmdparams_get_int(&cmdparams, "f", NULL);
char *destination;

if (nice_intro(0))
  return(0);

destination = cmdparams_get_str(&cmdparams, "out", NULL);
series = strdup(cmdparams_get_str(&cmdparams, "ds", NULL));
segname = cmdparams_get_str(&cmdparams, "segment", NULL);

if (strcmp(series, NOTSPECIFIED) == 0)
  {
  printf("\n$$$ NO ACTION - A record query must be specified as 'ds=xxx'.\n");
  return nice_intro(1);
  }
if (strcmp(destination, NOTSPECIFIED) == 0)
  {
  printf("\n$$$ NO ACTION - A destination directory must be specified as 'out=<dir>'.\n");
  return nice_intro(1);
  }

recordset = drms_open_records(drms_env, series, &status);
if (status)
  {
  fprintf(stderr,"retrieve_file target NOT_FOUND, drms_open_records failed, series=%s, status=%d.\n"
	"Aborting.\n", series,status);
  return(1);
  }
if (recordset->n > 1)
  printf("%d files matched.\n", recordset->n);

for (irec = 0; irec< recordset->n; irec++)
  {
  DRMS_Record_t *rec = recordset->records[irec];  /* pointer to current record */
  DRMS_Segment_t *seg;
  char path[DRMS_MAXPATHLEN];
  int nsegs = hcon_size(&rec->segments);
  if (nsegs == 0)
    {
    fprintf(stderr,"retrieve_file series has no segments. series=%s.\n",series);
    return(1);
    }
  /* look to see how many segments, if more than one the segment keyword must select which to use */
  if (nsegs > 1)
    {
    if (strcmp(segname, NOTSPECIFIED) == 0)
      {
      fprintf(stderr,"retrieve_file series has %d segments. desired segment must be specified. series=%s.\n",
        nsegs, series);
      return(1);
      }
    seg = drms_segment_lookup(rec, segname);
    }
  else
    seg = drms_segment_lookupnum(rec, 0);
  drms_record_directory(rec,path,1); /* needed for now to stage the SU and set sudir */
  drms_segment_filename(seg, path);
  if (*path)
    { /* target file located in SUMS */
    char cmd[DRMS_MAXPATHLEN+1024];
    sprintf(cmd, "cp %s %s %s %s", (wantlink ? "-s" : ""), (force ? "-f" : "-b" ), path, destination);
    if (system(cmd))
      {
      fprintf(stderr, "retrieve_file failed on command: %s\n",cmd);
      return(1);
      }
   if (verbose) 
      {
      char *note = drms_getkey_string(rec, "note", &status);
      printf("%s %s to %s",path, (wantlink ? "linked" : "retrieved"), destination);
      if (note && *note)
	printf(", note = %s\n", note);
      else
        printf(".\n");
      }
    }
  else
    {
    printf("retrieve_file target file NOT_FOUND,  path not found in %s, abort.\n", series);
    status = 1;
    }
  }

return(status);
}
