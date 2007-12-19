/* store_dir - stores a dir in DRMS/SUMS. */

/* Usage:
 *  store_dir series=<name> dirname=<dir> sel=<selection key> {note=<comment>}
 *         
 *     Store a dir into SUMS.
 *     "dir" is the directory that will be copied in SUMS
 *     in the dataseries <name>.  
 *     No <record spec> (i.e. no "[...]") should be given with the series.
 *     If <note> is present the comment will be stored with the data.
 *     The keyword "sel" can be used to provide lookup selection info in case
 *     multiple dirs with the same series will be stored.
 *     The DRMS prime keys will the values for "dir" and "sel".  
 *     Multiple calls of
 *     store_dir with the same seriesname, dir, and sel will result in
 *     multiple records with the same prime key.  Only the most recent
 *     will be easily retrieved. 
 *     If the dir in the "dir" keyword is sufficient it can be used as
 *     the prime key.  
 *
 *   store_dir series=su_jim.storedirs dirname=/home/xx sel=test1 note="junk"
 *
 *     will be retrieved with simply:
 *
 *       retrieve_dir su_jim.storedirs[/home/xx] to=/tmp
 *  or   retrieve_dir su_jim.storedirs[][test1] to=/tmp
 *
 *    Prior to first use, the series should be created. 
 *    String keywords of "dirname", "sel", and "note" should be included.
 *    If the series does not exist it is made from a standard template if the 
 *    -c flag is given or if the user answers "yes" when asked.  
 *    If a series is made the parameter "perm" supplies the series access 
 *    permissions.  Default is 1 which is like "perm=s" for create_series.  
 *    Set perm to 0 if you want to exclude other users from
 *    accessing your stored dir.
 */
/**
\defgroup store_dir store_dir

Store arbitrary directory of files to SUMS.  Files stored with
\ref store_dir can be easily retrieved by the \ref retrieve_dir utility.

If the series \a series_name does not exist, \ref store_dir will first create
that  series,  provided  that  the user supplies the \a -c flag. If \a perm=1,
then the series will be globally accessible, otherwise, only  the  user
calling \ref store_dir will have access.  \ref store_dir then creates a record in
the series \a series_name and creates a subdirectory, \a subdir,  in  the
record's  SUMS directory (for each record there is a single SUMS directory). 
\ref store_dir copies all files from \a prefix/subdir to \a subdir in
the  record's  SUMS  directory. It also stores \a prefix/subdir in the
record's \a dirname keyword value so that \ref retrieve_dir can locate the subdirectory
within   the   SUMS  directory  that  contains  the  files.

sel=\a sel_text and note=\a note_text allow the user to  link  additional
information to the record to facilitate the subsequent search for files
in the series. They set the record's \a sel and \a note keywords to have  the
values  \a sel_text  and \a note_text, respectively. In particular, \a sel
can be used to differentiate between multiple calls to  \ref store_dir  with
the  same  value  for \a prefix/subdir.  \a series_name must
contain prime keywords \a dirname and \a sel, and it must  contain  the
keyword \a note.

\par Synopsis:

\code
store_dir [-cvDRIVER_FLAGS] series=<series_name> dirname=<prefix>/<subdir> 
                            sel=<sel_text> [note=<note_text>] [perm=0|1]
\endcode

\b Example: To store a directory of files,
\code
store_dir series=su_arta.TestStoreDir dirname=/auto/home2/arta/savedFilesJanuary 
          sel=January note="All my January work"
\endcode

\par Flags:
\c -c: Silently create the series \a series_name if it does not exist.
\par
\c -v: Run in verbose mode.

\par Driver flags: 
\ref jsoc_main

\param series_name
Specifies the series, \a series_name, to which the file-referring
record will be added.  Should a record-set specifier be provided, it
will be ignored.

\param prefix/subdir
The local full path that contains the files to be stored in SUMS. The
record created will contain a prime keyword named dirname whose value
will be \a prefix/subdir.

\param sel_text 
Contains a string that will be the keyword value for the \a sel prime
keyword of the record created.  This extra prime keyword facilitates
selection between multiple records containing equivalent values for
the dirname (it may be
desirable to save files in the same directly repeatedly, or save the
same file over time).

\param note_text
An optional string that will be the keyword value for the \a note keyword
of the record created.

\param perm=0|1 
Relevant only when the \a -c flag is used and a series is created.  A \a perm
of 0 will make the created series accessible only by the current user.
A \a perm of 1 will make the series globally accessible.  

@{
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include <unistd.h>
#include <strings.h>

#define NOTSPECIFIED "***NOTSPECIFIED***"
/* List of default parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_STRING, "series", "", "Series name to store the dir into"},
  {ARG_STRING, "dirname", "", "Dir to store. prime key"},
  {ARG_STRING, "sel", "", "selection name. prime key"}, 
  {ARG_STRING, "note", "N/A", "comment field"},
  {ARG_INT, "perm", "1", ""}, /* permissions in case series is created */
  {ARG_FLAG, "c", "0", "create new series if needed"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_END}
};

/* Module name presented to DRMS. */
char *module_name = "store_dir";

/* Some global variables for this module. */
int verbose = 0;

/** @}*/

/* Check DRMS session status and calling arguments and set verbose variable */
int nice_intro () {
  verbose = cmdparams_get_int (&cmdparams, "v", NULL);
/********************this is old stuff before Rick's arg handling *********
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage) {
    printf ("store_dir <series> {-c} {-h} {-v} in=<dir> {sel=<desc>} {note=<comment>} {perm=1}\n"
	"  -c: Quietly create series if does not exist\n"
	"  -h: print this message and exit\n"
	"  -v: verbose\n"
	"series - series to store dir into\n"
	"in=    - dir to store\n"
	"sel=   - optional description for retrieval key\n"
	"note=  - optional description for dir\n"
        "perm=1 - optional access permissions, use 0 for private.\n");
    return (1);
  }
**********************************************************************/
  if (verbose) cmdparams_printall (&cmdparams);
  return(0);
}

/* Module main function. */
int DoIt (void) {
int status = 0;

char *in, *series, *note, *dirname, *sel, *rsp;
char path[DRMS_MAXPATHLEN];
char cmd[DRMS_MAXPATHLEN+1024];
DRMS_Record_t *rec, *template;
int yes_create = cmdparams_get_int(&cmdparams, "c", NULL);

if (nice_intro())
  return(0);

/*series = strdup(cmdparams_getarg(&cmdparams, 1));*/
series = cmdparams_get_str(&cmdparams, "series", NULL);
note = cmdparams_get_str(&cmdparams, "note", NULL);
sel = cmdparams_get_str(&cmdparams, "sel", NULL);
in = cmdparams_get_str(&cmdparams, "dirname", NULL);
/* First, check to see that the dir exists */
if (access(in, R_OK) != 0)
  {
  printf("The requested dir can not be accessed.  %s\n", in);
  return(1);
  }
  dirname = in;

/* Look in the record spec to see if a sel is given, else use dirname */
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
if ((status = drms_setkey_string(rec, "dirname", dirname)))
   {
   if (status == DRMS_ERROR_UNKNOWNKEYWORD)
     printf("ERROR: series %s does not have a keyword named 'dirname'\n", series);
   else
     printf("ERROR: drms_setkey_string failed for 'dirname'\n");
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

int user_says_yes(char *series)
  {
  char resp[DRMS_MAXPATHLEN];
  printf("Series '%s' does not exist. Do you want a standard store_dir series to be created?\n", series);
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
    "Index:          dirname,sel\n"
    "Description:    \"Dir storage\"\n"
    "Keyword: dirname,  string, variable, record, \" \",  %%s, none, \"Original dirname\"\n"
    "Keyword: sel, string, variable, record, \" \",  %%s, none, \" \"\n"
    "Keyword: note,      string, variable, record, \" \",  %%s, none, \" \"\n"
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
  
