/* store_file - stores a file in DRMS/SUMS. */

/* Usage:  store_file ds=<series> {note=<comment>} in=<file> {sel=<desc>}
 *         
 *         Store a file into SUMS.
 *         "in" is the file that will be stored in SUMS
 *         in the dataseries <series>.  
 *         No <record spec> (i.e. no "[...]") should be given with the series.
 *         (maybe later this will be changed to allow specification of a record directly)
 *         If <note> is present the comment will be stored with the data.
 *         The keyword "sel" can be used to provide lookup selection info in case
 *         multiple files with the same name will be stored.
 *         The DRMS prime keys will be "filename" and "sel".  Multiple calls of
 *         store_file with the same seriesname, filename, and sel will result in
 *         multiple records with the same prime key.  Only the most recent
 *         will be easily retrieved. 
 *         If the filename in the "in" keyword is sufficient it can be used as
 *         the prime key.  The optional "sel" may be used to help describe the file
 *         for later retrieval.
 *
 *         if a record spec is given when storing a file, that name will be used when retrieving it.
 *         For example:
 *
 *           store_file in=0000.fits ds=su_me.myseries sel=test002 note= "fits file from test002"
 *
 *         will be retrieved with simply:
 *
 *           retrieve_file su_me.myseries[][test002]
 *
 *        or, if multiple files for test002 and multiple sels with 0000.fits then:
 *
 *           retrieve_file su_me.myseries[0000.fits][test002]
 *
 *        Prior to first use, the series should be created. 
 *        String keywords of "filename", "sel", and "note" should be included.
 *        If the series does not exist it is made from a standard template if the -c flag is
 *        given or if the user answers "yes" when asked.  If a series is made the
 *        parameter "perm" supplies the series access permissions.  Default is 1 which is like
 *        "perm=s" for create_series.  Set perm to 0 if you want to exclude other users from
 *        accessing your stored file.
 */

/**
\defgroup store_file store_file - store a file to SUMS
@ingroup drms_util

\brief Store a user's file in SUMS

\par Synopsis:
\code
store_file [-chvGEN_FLAGS] ds=<series_name> in=<file> 
                             [sel=<sel_text>] [note=<note_text>] 
\endcode

Store an arbitrary file to SUMS.  File stored with \ref
store_file can be easily retrieved by the \ref retrieve_file utility.

If the series \a series_name does not exist, \ref store_file will first create
that  series,  provided  that  the user supplies the \a -c flag. If \a perm=1,
then the series will be globally accessible, otherwise, only  the  user
calling \ref store_dir will have access. \ref store_file then creates a record in
the series \a series_name and copies the file specified by the full path \a file  into  the
record's SUMS directory (for each record there is a single SUMS directory). 
\ref store_file stores the filename part of \a file (ie., excludes the path) in the
record's \a file keyword value, so that the original filename is recoverable (the
actual file name in SUMS may differ from the original file name).

sel=\a sel_text and note=\a note_text allow the user to  link  additional
information to the record to facilitate the subsequent search for file
in the series. They set the record's \a sel and \a note keywords to have  the
values  \a sel_text  and \a note_text, respectively. In particular, \a sel
can be used to differentiate between multiple calls to \ref store_file with
the  same  filename part of \a file. \a series_name must contain prime 
keywords \a file and \a sel, and it must contain the keyword \a note.

\b Example: to store a single file:
\code
store_file in=0000.fits ds=myseries sel=test002 note= "fits file from test002"
\endcode

\par Flags:
\c -c: Silently create the series \a series_name if it does not exist.
<br>
\c -h: Print usage message and exit
<br>
\c -v: Run in verbose mode.

\par GEN_FLAGS: 
\ref jsoc_main

\param series_name
Specifies the series, \a series_name, to which the file-referring
record will be added.  Should a record-set specifier be provided, it
will be ignored.

\param file
Full path of the file to store. Only the filename (everything after
the last '/') will be stored in the \a file keyword.

\param sel_text 
Contains a string that will be the value for the \a sel prime
keyword of the record created.  This extra prime keyword facilitates
selection between multiple records containing equivalent values for
the \a file keyword (it may be
desirable to save files in the same directly repeatedly, or save the
same file over time).

\param note_text
An optional string that will be the value for the \a note keyword
of the record created.

*/

/* move up when implemented...

\param perm=0|1 
Relevant only when the \a -c flag is used and a series is created.  A perm
of 0 will make the created series accessible only by the current user.
A perm of 1 will make the series globally accessible.  

*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>

#define NOTSPECIFIED "***NOTSPECIFIED***"
/* List of default parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_STRING, "ds", NOTSPECIFIED, "", ""},   /* Series to store into */
  {ARG_STRING, "in", NOTSPECIFIED, "", ""},   /* File to store from */
  {ARG_STRING, "sel", NOTSPECIFIED, "", ""}, /* selection help keyword */
  {ARG_STRING, "note", NOTSPECIFIED, "", ""}, /* Description of the file */
  {ARG_INT, "perm", "1", "", ""}, /* permissions in case series is created */
  {ARG_FLAG, "c", "0", "", ""},   /* create new series if needed without asking */
  {ARG_FLAG, "h", "0", "", ""},   /* print usage message and quit */
  {ARG_FLAG, "v", "0", "", ""},   /* verbose flag, normally do not use  */
  {ARG_END}
};

/* Module name presented to DRMS. */
char *module_name = "store_file";

/* Some global variables for this module. */
int verbose = 0;


/* Check DRMS session status and calling arguments and set verbose variable */
int nice_intro () {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  verbose = cmdparams_get_int (&cmdparams, "v", NULL);
  if (usage) {
    printf ("store_file ds=<series> {-c} {-h} {-v} in=<file> {sel=<desc>} {note=<comment>} {perm=1}\n"
	"  -c: Quietly create series if does not exist\n"
	"  -h: print this message and exit\n"
	"  -v: verbose\n"
	"ds=    - data series to store file into\n"
	"in=    - file to store\n"
	"sel=   - optional description for retrieval key\n"
	"note=  - optional description for file\n"
        "perm=1 - optional access permissions, use 0 for private.\n");
    return (1);
  }
  if (verbose) cmdparams_printall (&cmdparams);
  return (0);
}

/* Module main function. */
int DoIt (void) {
int status = 0;

char *in, *series, *note, *filename, *sel, *rsp;
DRMS_Record_t *rec, *template;
int yes_create = cmdparams_get_int (&cmdparams, "c", NULL);

if (nice_intro())
  return (0);

series = strdup(cmdparams_get_str(&cmdparams, "ds", NULL));
note = cmdparams_get_str(&cmdparams, "note", NULL);
sel = cmdparams_get_str(&cmdparams, "sel", NULL);
in = cmdparams_get_str(&cmdparams, "in", NULL);
if (strcmp(series, NOTSPECIFIED) == 0)
  {
  fprintf(stderr, "'ds' series must be specified.  Abort\n");
  return(1);
  }
if (strcmp(in, NOTSPECIFIED) == 0)
  {
  fprintf(stderr, "'in' file must be specified.  Abort\n");
  return(1);
  }
if (strcmp(sel, NOTSPECIFIED) == 0)
  sel = " ";
if (strcmp(note, NOTSPECIFIED) == 0)
  note = " ";

/* First, check to see that the file exists */
if (access(in, R_OK) != 0)
  {
  printf("The requested file can not be accessed.  %s\n", in);
  return(1);
  }

/* Now, extract the filename from the path */
filename = rindex(in, '/');
if (filename)
  filename++;
else
  filename = in;

rsp = index(series, '[');
if (rsp)
  {
  /* a "[" was present, ignore it */
  *rsp++ = '\0';
  printf("WARNING: Destination series %s was given a record spec: %s, which will be ignored.\n",series,rsp);
  }

/* Check to see if series exists, make if should */
template = drms_template_record(drms_env, series, &status);
if (template==NULL && status == DRMS_ERROR_UNKNOWNSERIES)
  {
  int user_says_yes();
  if (yes_create || user_says_yes(series))
    {
    int create_series(char *series);
    if (create_series(series))
      {
      printf("Series '%s' does not exist. Create_series failed. Give up.\n", series);
      return(1);
      }
    }
  else
    {
    printf("Series '%s' does not exist. Give up.\n", series);
    return(1);
    }
  }
else
  if(status)
    {
    fprintf(stderr, "DRMS problem looking up series.\n");
    return(status);
    }

/* Now ready to make a new record and set keywords */
rec = drms_create_record(drms_env, series, DRMS_PERMANENT, &status);
if (!rec || status)
    {
    printf("drms_create_record failed, series=%s, status=%d.  Aborting.\n", series,status);
    return(status);
    }
if ((status = drms_setkey_string(rec, "file", filename)))
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("ERROR: series %s does not have a keyword named 'file'\n", series);
   else
     printf("ERROR: drms_setkey_string failed for 'file'\n");
   return(1);
   }
if ((status = drms_setkey_string(rec, "sel", sel)))
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("ERROR: series %s does not have a keyword named 'sel'\n", series);
   else
     printf("ERROR: drms_setkey_string failed for 'sel'\n");
   return(1);
   }
if ((status = drms_setkey_string(rec, "note", note))) 
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("WARNING: series %s does not have a keyword named 'note'\n", series);
   else
     printf("WARNING: drms_setkey_string failed for 'note'\n");
   printf("Your note was not saved: %s\n",note);
   }
if ((status = drms_segment_write_from_file(drms_segment_lookup(rec, "file_seg"), in)))
    {
    printf("drms_segment_write_file failed, status=%d\n",status);
    return(status);
    }
if ((status = drms_close_record(rec, DRMS_INSERT_RECORD)))
  printf("drms_close_record failed!\n");

return(status);
}

int user_says_yes(char *series)
  {
  char resp[DRMS_MAXPATHLEN];
  printf("Series '%s' does not exist. Do you want a standard store_file series to be created?\n", series);
  printf("Answer yes or no: ");
  fflush(stdout);
  gets(resp);
  return(strcasecmp(resp,"yes")==0);
  }

char * make_series_jsd(char *series)
  {
  char *user = getenv("USER");
  char jsd[1024];
  sprintf(jsd,
    "Seriesname:     %s\n"
    "Author:         %s\n"
    "Owners:         %s\n"
    "Unitsize:       1\n"
    "Archive:        1\n"
    "Retention:      7\n"
    "Tapegroup:      1\n"
    "Index:          file,sel\n"
    "Description:    \"File storage\"\n"
    "Keyword: file,  string, variable, record, \" \",  %%s, none, \"Original filename\"\n"
    "Keyword: sel, string, variable, record, \" \",  %%s, none, \"Version\"\n"
    "Keyword: note,      string, variable, record, \" \",  %%s, none, \" \"\n"
    "Data: file_seg, variable, int, 0, none, generic, \" \"\n",
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
  
