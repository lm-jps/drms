/*
 *  show_info - Prints keyword information and/or file path for given recordset
 *
 */

/**
\defgroup show_info show_info - Examine a dataseries structure or contents
@ingroup drms_util

\par Synopsis:
\code
show_info -j {ds=}<seriesname>
show_info -l {ds=}<seriesname>
show_info -c {ds=}<record_set>
show_info -s {ds=}<seriesname>
show_info [-aAbiIKoOpPrRSTvxz] [-dkqt] {ds=}<record_set>|sunum=<sunum> [n=<count>] [key=<keylist>] [seg=<seglist>]
show_info_sock {same options as above}
\endcode

\details

\b Show_info shows various kinds of information about a data series.
This can be the series structure, the "jsoc series definition" for
the series, all or some of the keyword and segment values for a range of records,
the full path to the SUMS storage for the data segment, etc.
Exactly what information gets printed is
controlled by command-line flags (see below).

 By default, show_info will time-out on long-running database queries after a 10-minute
 wait. If -O is provided on the command-line, the time-out is disabled. Both of these
 behaviors are overridden the by DRMS_DBTIMEOUT cmd-line argument.

The argument descriptions are grouped by function.  The first group controls the overall
operation of show_info.  If any of these flags (c,h,j,l,s) is present the specified action
is taken and the program exits.  Otherwise a DRMS query is made and the
resulting records are examined and the specified quantities are printed for
each record found.  If the QUERY_STRING argument is present it is parsed to
extract command line arguments passed via a web cgi-bin call of show_info and
the results are returned as text.

\b show_info_sock is the same as show_info but configured to run in a DRMS session via a socket connection
to a drms_server session (see e.g. drms_run).

\par Options:

\par Flags controling operation to perform:

This group of arguments controls the action of show_info.  The default action is to query for the specified record-set
and perform the requested display of information.
\li \c -c: Show count of records in query and exit.  This flag requires a record set query to be specified.
\li \c -e: Parse the provided record-set query into constituent parts (e.g., "hmi.M_45s[2013.1.8][? QUALITY > 0 ?]" --> "hmi.M_45s" "[2013.1.8][? QUALITY > 0 ?]"
\li \c -h: help - print usage info and exit
\li \c -j: list series info in jsd format and exit
\li \c -l: just list series keyword, segment, and link names with descriptions then exit
\li \c -s: stats - show some statistics about the series, presently only first and last record.
\li \c  QUERY_STRING=<cgi-bin GET format command line args> - string with default "Not Specified" used when show_info is invoked
from http://jsoc.stanford.edu/cgi_bin/ajax/show_info

\par Parameters and Flags controling record subset to examine:

This group of arguments specifies a recordset to examine.  If the "sunum" argument is present it overrides an
explicit recordset specification and returns the record with that \a sunum as its record directory pointer in SUMS.
For normal recordset queries the "ds=" is optional.  In no "where clauses" (i.e. "[xxx]" clauses) are
present in the recordset query the "n" parameter is required.  Since the "where clauses" are used to
restrict the number or records retrieved, an empty clause (e.g. "[]") will match all records in the series.
\li \c  ds=<record_set query> - string with default "Not Specified", see below for more information.
\li \c  sunum=<sunum> - integer with default -1, overrides a "ds" specification.
\li \c  n=<count> - Max number of records to show, +from first, -from last, see below.

\par Parameters and Flags controling selction of keywords and segments to examine:

This group of arguments specifies the set of keywords, segments, links, or virtual keywords to display.
\li \c  key=<keylist> - string with default "Not Specified", see below.
\li \c  seg=<seglist> - string with dedfauly "Not Specified", see below.
\li \c  -a: Select all keywords and display their values for the chosen records
\li \c  -A: Select all segments and display their filenames/paths/dimensions for the chosen records
\li \c  -b: Disable the prime-key logic when opening records (implicitly adds the [! 1=1 !] to the record-set specification)
\li \c  -i: print record query, for each record, will be before any keywwords or segment data
\li \c  -I: print session information including host, sessionid, runtime, jsoc_version, and logdir
\li \c  -K: Select all links and display their targets for the chosen records
\li \c  -o: list the record's online status
\li \c  -O: disable the code that sets a database query time-out of 10 minutes
\li \c  -p: list the record's storage_unit path, waits for retrieval if offline
\li \c  -P: list the record\'s storage_unit path but no retrieve
\li \c  -r: recnum - show record number as first keyword
\li \c  -R: retention - show the online expire date for the segment data
\li \c  -S: SUNUM - show the sunum for the record
\li \c  -T: Tape - show the archive tape name for the record
\li \c  -v: verbose - print extra, helpful information
\li \c  -x: archive - show archived flag for the storage unit.
\li \c  -z: SUNUM - show the storage unit size for the SU that contains the record's segments

\par Flags controling how to display values:

\li \c  -d: Show dimensions of segment files with selected segs
\li \c  -k: keyword list one per line
\li \c  -q: quiet - skip header of chosen keywords
\li \c  -t: types - show types and print formats for keyword values

\par JSOC flags:
\ref jsoc_main

\par Usage:

\a seriesname
A seriesname is a JSOC DRMS seriesname.  It is the prefix of a record_set specification.

\a record_set
A record_set list is a comma separated list of record_set queries.
Each query is a series name followed by an optional record-set specification (i.e.,
\a seriesname[RecordSet_filter]). Causes selection of a subset of
records in the series. This argument is required, and if no record-set
filter is specified, then \a n=nrecords must be present.
The "ds=" protion of the record_set argument is optional.

\a sunum
Instead of providing a normal record_set query an explicit storage unit id (sunum) may
be provided.  In this case the provided sunum will be found in the SUMS tables and
the parent series will be queried for the record owning the specified sunum.  The
"Prime-Key" logic does not apply in this case (the "[! ... !]" form of a general
query clause is used) so the matching record may not be the latest version.

\a count specifies the maximum number of records for which
information is printed.  If \a count < 0, \ref show_info displays
information for the last \a count records in the record set. If
\a count > 0, \ref show_info displays information for the first
\a count records in the record set. If \a record_set contains a
record set filter, then \a count applies to the set of records
matching the filter.

\a keylist
Comma-separated list of keyword names. For each keyword listed,
information will be displayed.  \a keylist is ignored in the case that
the \a -a flag is set.

\a seglist
Comma-separated list of segment names.  \a seglist is ignored
in the case that the \a -A flag is set.  For each segment listed, the
segment's file is displayed.  If the \a -p flag is set the filename will
be prefaced by the full path to the file in SUMS.  If the storage unit
containing the record directory is offline, it will be staged first and
the new online location is reported.  If the \a -P flag is given the path
is displayed only if the data is online.  If offline with a \a -P flag then
only the filename is shown.  If the \a -d flag is set then the dimensions
of the segment array are displayed along with the path/filename.

\warning NOTE to the csh user, you need to excape the '[' character and
some other characters used in the record set specification.

\par Output:

In the normal table mode, show_info presents keyword and segment values in a table with
a column for each keyword, segment, or other quantity and a row for each record
in the selected record_set.  The columns are tab separated. Unless the \c -q flag is
present a header row(s) will be provided.  The normal header is a single row containing
the column name.  If the \a -t flag is present an expanded header will contain three
lines: the normal name list line, a second line containing the type of the values, e.g.
\a int, \a double, \a etc. and a third line containing the type as a \ref printf printing
format, e.g. \a %s for a string.

In the keyword format, invoked with a \a -k flag, there is one name=<value> pair per
line.  Lines for each record are preceeded with a line beginning with a '#' and containing
the record_set query that finds that record and followed by a single blank line.

The \a -q flag is provided to make it convenient to use the output of \a show_info in scripts.
Example 3 below shows such a usage.

\par Note about DRMS_RETENTION flag:

If show_info is used to modify the retention time or one or more storage units, you must
call show_info in a way that it makes a SUMS request. The easiest way to do this is to
call it with the -P flag. Retention can be reduced only if the caller is the owner of
the database "series" table (the table whose name is also the name of the series).

\par Examples:

\b Example 1:
To show the storage-unit paths for a maximum of 10
records:
\code
  show_info -p ds=su_arta.TestStoreFile n=10
\endcode

\b Example 2:
To show information, in non-table format, for all keywords,
plus the segment named file_seg and storage unit number, for a maximum of 10 records:
\code
  show_info ds=su_arta.TestStoreFile -akrS n=10 seg=file_seg
\endcode

\b Example 3:
To find the path to the most recent hmi level-0 small image and start the ds9 display program:
\code
  ds9 `show_info -q -p seg=image_sm 'hmi.lev0e[:#$]'`
\endcode

\b Example 4:
To show the structure of a series:
\code
  show_info -l su_phil.vw_V_mean
\endcode

\bug
SHOW_INFO WILL NOT WORK PROPERLY IF THE RECORD-SET STRING SPECIFIES MORE THAN ONE SERIES.
\sa
http://jsoc.stanford.edu/ajax/lookdata.html drms_query describe_series jsoc_info create_series

*/


/*
 * To emulate a POST request on the cmd-line, force qDecoder, the HTTP-request parsing library, to process a GET request.
 * We can pass the arguments via the cmd-line, or via environment variables.
 *   1. Set two shell environment variables, then run show_info:
 *      a. setenv REQUEST_METHOD GET
 *      b. setenv QUERY_STRING 'ds=hmi.m_45s[2015.5.5]&c=1' (this is an example - substitute your own arguments).
 *      c. show_info
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "cmdparams.h"
#include "drmssite_info.h"
#include "printk.h"
#include "qDecoder.h"


#define kArgParseRS "e"

struct SIParts_struct
{
  char *series;
  char *filter;
};
typedef struct SIParts_struct SIParts_t;

ModuleArgs_t module_args[] =
{
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRINGS, "key", " ", "<comma delimited keyword list>"},
  {ARG_STRINGS, "seg", " ", "<comma delimited segment list>"},
  {ARG_FLAG, "a", "0", "print all keyword values"},
  {ARG_FLAG, "A", "0", "print all segment file names/paths"},
  {ARG_FLAG, "b", NULL, "disable prime-key logic"},
  {ARG_FLAG, "B", NULL, "print floating-point values as hexadecimal strings"},
  {ARG_FLAG, "c", "0", "print number of records specified"},
  {ARG_FLAG, "C", NULL, "if the -P or p flag is set, then do not retrieve linked DRMS records; otherwise this flag is a noop"},
  {ARG_FLAG, "d", "0", "print segment data units, protocols, and dimensions"},
  {ARG_FLAG, "e", NULL, "parse the provided record-set query into series and filters"},
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_FLAG, "i", "0", "print record specifications"},
  {ARG_FLAG, "I", "0", "print DRMS session information (host, sessionid, runtime, jsoc_version, and logdir)"},
  {ARG_FLAG, "j", "0", "print series specification in jsd format"},
  {ARG_FLAG, "k", "0", "print one keyword value per line"},
  {ARG_FLAG, "K", "0", "print linked-record specifications"},
  {ARG_FLAG, "l", "0", "print keyword and segment specifications"},
  {ARG_FLAG, "M", NULL, "print floating-point values with maximum precision determined by DB"},
  {ARG_INT,  "n", "0", "select subset of records from set specified (a positive value N selects the first N records in the set, and a negative value selects that last N records)"},
  {ARG_FLAG, "o", "0", "print Storage Unit online statuses"},
  {ARG_FLAG, "O", NULL, "disable the database query time-out (defaults to 10 minutes)"},
  {ARG_FLAG, "p", "0", "print Storage Unit paths, staging offline SUs"},
  {ARG_FLAG, "P", "0", "print online Storage Unit paths"},
  {ARG_FLAG, "q", "0", "quiet - omit header row"},
  {ARG_FLAG, "r", "0", "recnum - print DRMS record numbers"},
  {ARG_FLAG, "R", "0", "print online expiration dates"},
  {ARG_FLAG, "s", "0", "stats - print series statistics"},
  {ARG_FLAG, "S", "0", "SUNUM - print SUNUMs"},
  {ARG_FLAG, "t", "0", "types - print data types and formats for keyword values"},
  {ARG_FLAG, "T", "0", "tapeinfo - print archive tapename and file number, or NA if not archived"},
  {ARG_FLAG, "v", NULL, "verbosity - print detailed information about the run"},
  {ARG_FLAG, "x", "0", "archive - print archive status for Storage Unit"},
  {ARG_FLAG, "z", "0", "size - print size (in bytes) of Storage Unit"},
  {ARG_INTS, "sunum", "-1", "select records with a comma-separated list of SUNUMs"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "show_info called as cgi-bin program args here"},
  {ARG_END}
};

char *module_name = "show_info";
int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\nshow_info [-ahjklpqr] "
	"ds=<recordset query> {n=0} {key=<keylist>} {seg=<segment_list>}\n"
        "sunum=<sunum> - use instead of ds= argument when SUNUM is known.\n"
        " summary information modes are:\n"
        "  -c: count records in query\n"
	"  -h: help - show this message then exit\n"
	"  -j: list all series, keyword, segment, and link items in jsd file format, then exit\n"
	"  -l: list all keywords with description, then exit\n"
	"  -s: stats - show some statistics for how many records, etc.\n"
        " per-record information modes are:\n"
	"  -a: show information for all keywords\n"
	"  -A: show information for all segments\n"
    "  -b: disable prime-key logic when opening records\n"
  	"  -d: Show dimensions of segment files with selected segs\n"
	"  -i: query- show the record query that matches the current record\n"
        "  -I: print session information for record creation, host, sessionid, runtime, jsoc_version, and logdir\n"
	"  -K: show information for all links\n"
	"  -o: online - tell the online state\n"
    "  -O: disable the code that sets a database query time-out of 10 minutes\n"
	"  -p: list the record's storage_unit path (retrieve if necessary)\n"
	"  -P: list the record's storage_unit path (no retrieve)\n"
	"  -r: recnum - show record number as first keyword\n"
	"  -R: retention - show the online expire date\n"
	"  -S: sunum - show sunum number as first keyword (but after recnum)\n"
	"  -T: Tapename - show archive tapename and file number \n"
        "  -v: verbose - print extra, useful information\n"
	"  -x: archive - show archive status for the record's storage unit\n"
	"  -z: size - show size of storage unit containing record's segments\n"
        " output appearance control flags are:\n"
	"  -k: list keyword names and values, one per line\n"
	"  -t: list keyword types and print formats as 2nd and 3rd lines in table mode\n"
        "  -v: print extra, helpful information"
	"  -q: quiet - skip header of chosen keywords\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"n=0 number of records in query to show, +n from start or -n from end\n"
	"key=<comma delimited keyword list>, for all use -a flag\n"
	"seg=<comma delimited segment list>, for all use -A flag\n"
	"The -p or -P flag will show the record directory by itself or as part of the\n"
	"full path to the segment file if seg=<segmentname> is specified.\n"
	"Note that the -p flag will cause the data to be staged if offline.\n");
    return(1);
    }
  return (0);
  }

/* find first record in series that owns the given record */
DRMS_RecordSet_t *drms_find_rec_first(DRMS_Record_t *rec, int wantprime, int retrieveLinks)
{
    int nprime;
    int status;
    DRMS_RecordSet_t *rs;
    char query[DRMS_MAXQUERYLEN];


    strcpy(query, rec->seriesinfo->seriesname);
    nprime = rec->seriesinfo->pidx_num;

    if (wantprime && nprime > 0)
    {
        // only first prime key is used for now
        // for (iprime = 0; iprime < nprime; iprime++)
        strcat(query, "[#^]");
    }
    else
    {
        strcat(query, "[:#^]");
    }

    rs = drms_open_records2(rec->env, query, NULL, 0, 1, retrieveLinks, &status);

    return(rs);
}

/* find last record in series that owns the given record */
DRMS_RecordSet_t *drms_find_rec_last(DRMS_Record_t *rec, int wantprime, int retrieveLinks)
{
    int nprime;
    int status;
    DRMS_RecordSet_t *rs;
    char query[DRMS_MAXQUERYLEN];


    strcpy(query, rec->seriesinfo->seriesname);
    nprime = rec->seriesinfo->pidx_num;

    if (wantprime && nprime > 0)
    {
        // only first prime key is used for now
        // for (iprime = 0; iprime < nprime; iprime++)
        strcat(query, "[#$]");
    }
    else
    {
        strcat(query, "[:#$]");
    }

    rs = drms_open_records2(rec->env, query, NULL, 0, -1, retrieveLinks, &status);

    return(rs);
}


static void list_series_info(DRMS_Record_t *rec)
  {
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;
  DRMS_Link_t *link;
  HIterator_t *last = NULL;
  char prevKeyName[DRMS_MAXNAMELEN] = "";
  char baseKeyName[DRMS_MAXNAMELEN];

  /* show the prime index keywords */
  int npkeys = rec->seriesinfo->pidx_num;
  if (npkeys > 0)
    {
    int i;
    printf("Prime Keys are:\n");
    for (i=0; i<npkeys; i++)
        {
        DRMS_Keyword_t *skey, *pkey;
	int status;
        skey = pkey = rec->seriesinfo->pidx_keywords[i];
	if (pkey->info->recscope > 1)
            pkey = drms_keyword_slotfromindex(pkey);
        printf("\t%s", pkey->info->name);
        if (pkey != skey)
	    {
	    printf(" is slotted '%s' using %s",
		drms_keyword_getrecscopestr(pkey, &status), skey->info->name);
	    }
	printf("\n");
        }
    }
  else
    printf("No Prime Keys are defined for this series.\n");

  /* show DB index keywords */
  if (rec->seriesinfo->dbidx_num > 0)
    {
    int i;
    printf("DB Index Keys are:\n");
    for (i=0; i<rec->seriesinfo->dbidx_num; i++)
        printf("\t%s\n",(rec->seriesinfo->dbidx_keywords[i])->info->name);
    }
  else
    printf("DB Index Keys are same as Prime Keys\n");

  /* show all keywords */
  printf("All Keywords for series %s:\n",rec->seriesinfo->seriesname);

  while ((key = drms_record_nextkey(rec, &last, 0)))
  {
  int persegment = key->info->kwflags & kKeywordFlag_PerSegment;
  if (persegment)
    {
    char *underscore;
    strcpy(baseKeyName, key->info->name);
    underscore = rindex(baseKeyName, '_');
    if (underscore) *underscore = '\0';
    if (strcmp(prevKeyName, baseKeyName) == 0)
      continue;  // only report the first instance of persegment keywords.
    strcpy(prevKeyName, baseKeyName);
    }
  if (!drms_keyword_getimplicit(key))
     {
        printf ("\t%-10s", (persegment ? baseKeyName : key->info->name));
        if (key->info->islink)
        {
           printf("\tlink through %s",key->info->linkname);
        }
        else
        {
           printf ("\t(%s)", drms_type_names[key->info->type]);
           if (persegment)
              printf(", per-segment");
        }
        printf ("\t%s\n", key->info->description);
     }
  }

  /* show the segments */
  if (rec->segments.num_total)
    {
    printf("Segments for series %s:\n",rec->seriesinfo->seriesname);

    if (last)
    {
       hiter_destroy(&last);
    }

    while ((seg = drms_record_nextseg(rec, &last, 0)))
        { /* segment name, units, protocol, dims, description */
	if (seg->info->islink)
	    {
	    printf("\tlink through %s",seg->info->linkname);
	    }
	else
	    {
	    char prot[DRMS_MAXNAMELEN];
	    int iaxis, naxis = seg->info->naxis;
	    strcpy(prot, drms_prot2str(seg->info->protocol));
            printf ("\t%-10s", seg->info->name);
	    printf ("\t%7s", seg->info->unit);
	    printf ("\t%7s",prot);
	    for (iaxis=0; iaxis<naxis; iaxis++)
	        {
	        if (iaxis == 0)
	            printf("\t");
	        else
		    printf("x");
		if (seg->info->scope == DRMS_VARDIM)
		    printf("VAR");
		else
		    printf("%d",seg->axis[iaxis]);
		}
            }
	    printf ("\t%s\n", seg->info->description);
        }
    }

  /* show the links */
  if (rec->links.num_total)
    {
    printf("Links for series %s:\n",rec->seriesinfo->seriesname);

    if (last)
    {
       hiter_destroy(&last);
    }
    while ((link = drms_record_nextlink(rec, &last)))
        {
        printf ("\t%-10s", link->info->name);
        if (link->info->type == STATIC_LINK)
            printf("\tSTATIC");
        else
             printf("\tDYNAMIC");
        printf ("\t%s", link->info->target_series);
        printf ("\t%s\n", link->info->description);
        }
    }

  if (last)
  {
     hiter_destroy(&last);
  }

  return;
  }


#define show_info_return(status)      \
{                                     \
   if (suinfo)                        \
   {                                  \
      hcon_destroy(&suinfo);          \
   }                                  \
   if (given_sunum)                   \
   {                                  \
      free(given_sunum);              \
      given_sunum = NULL;             \
   }                                  \
   if (sunum_rs_query)                \
   {                                  \
      free(sunum_rs_query);           \
      sunum_rs_query = NULL;          \
   }                                  \
   return(status);                    \
}                                     \

// get_session_info - get information from drms_Session table for this record
/* Returns path of directory that contains any saved log information for the given record */
/* If log is offline, returns message, if log was not saved or otherwise not found returns NULL */
/* The returned char* should be freed after use. */

/* ART - can open a SUM. */
int get_session_info(DRMS_Record_t *rec, char **runhost, char **runtime, char **jsoc_vers, char **logdir)
  {
  int status;
  char query[DRMS_MAXQUERYLEN];
  DB_Text_Result_t *qres;

  sprintf(query, "select sunum, hostname, starttime, jsoc_version "
                 " from %s.drms_session where sessionid=%lld", rec->sessionns, rec->sessionid);
  if ((qres = drms_query_txt(drms_env->session, query)) && qres->num_rows>0)
    {
    if (qres->field[0][0][0] == '\0') // get sunum and logdir
      *logdir = strdup("No log available");
    else
      {
      SUM_info_t *sinfo = rec->suinfo;
      if (!sinfo)
        *logdir = strdup("Log Lost");
      else if (strcmp("N", sinfo->online_status) == 0)
        *logdir = strdup("Log offline");
      else
        *logdir = strdup(sinfo->online_loc);
      }
    if (qres->field[0][1][0] == '\0') // get host
      *runhost = strdup("No host");
    else
      *runhost = strdup(qres->field[0][1]);
    if (qres->field[0][2][0] == '\0') // get start time
      *runtime = strdup("No time");
    else
      *runtime = strdup(qres->field[0][2]);
    if (qres->field[0][3][0] == '\0') // get jsoc_version
      *jsoc_vers = strdup("No version");
    else
      *jsoc_vers = strdup(qres->field[0][3]);
    status = 0;
    }
  else
    status = 1;
  if (qres) db_free_text_result(qres);
  return status;
  }

static void ShowInfoFreeInfo(const void *value)
{
   SUM_info_t *tofree = *((SUM_info_t **)value);
   if (tofree)
   {
      free(tofree);
   }
}

static int GetSUMinfo(DRMS_Env_t *env, HContainer_t **info, int64_t *given_sunum, int nsunums)
{
   int status = DRMS_SUCCESS;

   if (info && given_sunum && nsunums > 0)
   {
      SUM_info_t **infostructs = NULL;
      int iremote;
      DRMS_StorageUnit_t **sus = NULL;
      int iinfo;
      LinkedList_t *remotesunums = NULL;

      int natts;
      /* Holds pointers to the SUM_info structs for the SUNUMS provided in given_sunum */
      *info = hcon_create(sizeof(SUM_info_t *), 128, ShowInfoFreeInfo, NULL, NULL, NULL, 0);
      infostructs = (SUM_info_t **)calloc(nsunums, sizeof(SUM_info_t *));

      natts = 1;
      while (natts <= 2)
      {
         /* If the SUNUM belongs to a different SUMS, then this query will fail.
          * Assuming, for now, that this SUNUM is valid, but belongs to a different
          * SUMS, try again, after doing a remotesums call.
          */

         /* natt == 1 ==> haven't done a remotesums request for unknown (locally) SUNUMS.
          * natt == 2 ==> already did a remotesums request for unknown (locally) SUNUMS,
          *   now checking to see if the previously unknown SUNUMs have been ingested into
          *   the local SUMS. */

         /* Insert results an array of structs - will be inserted back into
          * the record structs when all drms_getsuinfo() calls have completed. */
         status = drms_getsuinfo(env, (long long *)given_sunum, nsunums, infostructs);

         if (status != DRMS_SUCCESS)
         {
            fprintf(stderr, "drms_record_getinfo(): failure calling drms_getsuinfo(), error code %d.\n", status);
            break;
         }
         else
         {
            /* Collect all SUNUMs that SUMS didn't know about, and assume that they are remote SUNUMs. */
            for (iinfo = 0, iremote = 0; iinfo < nsunums; iinfo++)
            {
               /* Jim says this check might be something else - he thinks the SUM_info_t * might
                * not be NULL, and instead one of the fields will indicate an invalid SUNUM. */
               if (!drmssite_sunum_is_local(infostructs[iinfo]->sunum) && *(infostructs[iinfo]->online_loc) == '\0')
               {
                  printf("### show_info: SUNUM '%llu' unknown to local SUMS - initiating remotesums call.\n", (unsigned long long)infostructs[iinfo]->sunum);

                  if (!remotesunums)
                  {
                     remotesunums = list_llcreate(sizeof(long long), NULL);
                  }

                  list_llinserttail(remotesunums, &(infostructs[iinfo]->sunum));
                  iremote++;
               }
            }
         }

         if (status)
         {
            break;
         }

         if (iremote > 0)
         {
            if (natts == 1)
            {
               /* Try a remotesums call. */
               int isu;
               DRMS_StorageUnit_t *su = NULL;
               ListNode_t *node = NULL;
               int suret = DRMS_SUCCESS;

               sus = malloc(sizeof(DRMS_StorageUnit_t *) * iremote);
               list_llreset(remotesunums);

               for (isu = 0; isu < iremote; isu++)
               {
                  sus[isu] = malloc(sizeof(DRMS_StorageUnit_t));
                  su = sus[isu];

                  node = list_llnext(remotesunums);

                  su->sunum = *((long long *)node->data);
                  *(su->sudir) = '\0';
                  su->mode = DRMS_READONLY;
                  su->nfree = 0;
                  su->state = NULL;
                  su->recnum = NULL;
                  su->refcount = 0;
                  su->seriesinfo = NULL;
               }

               /* This could return DRMS_REMOTESUMS_TRYLATER, which means that
                * the size of the requested payload is large, so remotesums_master.pl
                * launched remotesums_ingest in the background. */
               suret = drms_getsudirs(env, sus, iremote, 1, 0);

               if (suret == DRMS_REMOTESUMS_TRYLATER)
               {
                  fprintf(stdout, "Master remote SUMS script is ingesting"
                          " storage unit asynchronously.\nRetry query later.\n");
                  status = suret;
                  break;
               }
               else if (suret != DRMS_SUCCESS)
               {
                  printf("### show_info: Error calling drms_getsudirs(), must quit\n");
                  status = suret;
                  break;
               }

               /* We're going to redo the drms_getsuinfo() call, so free any SUM_info_t structs that
                * were previously returned. */
               if (infostructs)
               {
                  for (iinfo = 0; iinfo < nsunums; iinfo++)
                  {
                     if (infostructs[iinfo])
                     {
                        free(infostructs[iinfo]);
                        infostructs[iinfo] = NULL;
                     }
                  }
               }

               if (remotesunums)
               {
                  list_llfree(&remotesunums);
               }

               if (sus)
               {
                  for (isu = 0; isu < iremote; isu++)
                  {
                     su = sus[isu];
                     if (su)
                     {
                        free(su);
                        su = NULL;
                     }
                  }
               }
            }
            else
            {
               /* For at least one SUNUM, the remotesums call didn't not result in a known SUNUM - invalid
                * SUNUM, bail. */
               printf("### show_info: at least one SUNUM was invalid.\n");
               status = DRMS_ERROR_INVALIDSU;
               break;
            }
         }
         else
         {
            break;
         }

         natts++;
      } /* natts */

      if (status == DRMS_SUCCESS)
      {
         if (*info)
         {
            char key[128];

            /* Need to put SUM_info_t structs into info container. */
            for (iinfo = 0; iinfo < nsunums; iinfo++)
            {
               snprintf(key, sizeof(key), "%llu", (unsigned long long)infostructs[iinfo]->sunum);
               hcon_insert(*info, key, &(infostructs[iinfo]));
            }
         }
      }
      else
      {
         if (infostructs)
         {
            /* if success, then the SUM_info_t structs were put into the info container. */
            for (iinfo = 0; iinfo < nsunums; iinfo++)
            {
               if (infostructs[iinfo])
               {
                  free(infostructs[iinfo]);
               }
            }
         }

         if (*info)
         {
            hcon_destroy(info);
         }
      }

      if (infostructs)
      {
         free(infostructs);
      }

      if (remotesunums)
      {
         list_llfree(&remotesunums);
      }

      if (sus)
      {
         free(sus);
      }
   }

   return status;
}

#if 0
static int susort(const void *a, const void *b)
{
   SUM_info_t *first = (SUM_info_t *)hcon_getval(*((HContainerElement_t **)a));
   SUM_info_t *second = (SUM_info_t *)hcon_getval(*((HContainerElement_t **)b));

   XASSERT(first && second);

   return strcasecmp(first->owning_series, second->owning_series);
}
#endif

/* If there is no series, then this means that the show_info command did not resolve into at least
 * one valid DRMS record; we've already opened records */
static int PrintHeader(DRMS_Env_t *env, const char *series, int show_all, int show_keys, int show_all_segs, int show_segs, int show_all_links, int quiet, int keyword_list, int show_recnum, int show_sunum, int show_recordspec, int show_online, int show_retention, int show_archive, int show_tapeinfo, int show_size, int show_session, int want_dims, int want_path, int show_types, LinkedList_t *keysSpecified, LinkedList_t *segsSpecified, LinkedList_t *linksSpecified)
{
    int drmsstat = DRMS_SUCCESS;
    int col;
    ListNode_t *node = NULL;
    DRMS_Record_t *rec = NULL;
    char *name = NULL;
    DRMS_Keyword_t *rec_key = NULL;
    DRMS_Segment_t *rec_seg = NULL;
    DRMS_Link_t *rec_link = NULL;
    int displaySegPaths = 0;

    displaySegPaths = (show_segs || show_all_segs);

    if (series)
    {
        rec = drms_template_record(env, series, &drmsstat);

        if (drmsstat != DRMS_SUCCESS || !rec)
        {
            return drmsstat;
        }
    }

    if (!quiet && !keyword_list)
    {                       /* print keyword and segment name header line */
        /* first print the name line */
        col=0;
        if (show_recnum)
            printf ("%srecnum", (col++ ? "\t" : ""));
        if (show_sunum)
            printf ("%ssunum", (col++ ? "\t" : ""));
        if (show_recordspec)
            printf ("%squery", (col++ ? "\t" : ""));
        if (show_online)
            printf ("%sonline", (col++ ? "\t" : ""));
        if (show_retention)
            printf ("%sretain", (col++ ? "\t" : ""));
        if (show_archive)
            printf ("%sarchive", (col++ ? "\t" : ""));
        if (show_tapeinfo)
        {
            printf ("%stapename", (col++ ? "\t" : ""));
            printf ("%sfilenum", (col++ ? "\t" : ""));
        }
        if (show_size)
            printf ("%ssize", (col++ ? "\t" : ""));
        if (show_session)
            printf ("%shost\tsessionid\truntime\tjsoc_version\tlogdirectory", (col++ ? "\t" : ""));

        /* If we have no rec (because the show_info arguments did not resolve into at least one record),
         * then no keywords and no segment values and no link values will print. */
        if (keysSpecified)
        {
            list_llreset(keysSpecified);
            while ((node = list_llnext(keysSpecified)) != NULL)
            {
                name = (char *)(node->data);
                printf("%s%s", (col++ ? "\t" : ""), name);
            }
        }

        if (displaySegPaths)
        {
            if (segsSpecified)
            {
                list_llreset(segsSpecified);
                while ((node = list_llnext(segsSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    printf("%s%s", (col++ ? "\t" : ""), name);

                    if (want_dims)
                    {
                        printf("\t%s_info", name);
                    }
                }
            }
        }
        else if (want_path)
        {
            printf("%sSUDIR", (col++ ? "\t" : ""));
        }

        if (linksSpecified)
        {
            list_llreset(linksSpecified);
            while ((node = list_llnext(linksSpecified)) != NULL)
            {
                name = (char *)(node->data);
                printf("%s%s", (col++ ? "\t" : ""), name);
            }
        }

        printf ("\n");
        /* now, if desired, print the type and format lines. */

        if (show_types)
        {
            col=0;
            /* types first */
            /* ASSUME all records have same structure - might not be true for mixed queries, fix later */
            if (show_recnum)
                printf ("%slonglong", (col++ ? "\t" : ""));
            if (show_sunum)
                printf ("%slonglong", (col++ ? "\t" : ""));
            if (show_recordspec)
                printf ("%sstring", (col++ ? "\t" : ""));
            if (show_online)
                printf ("%sstring", (col++ ? "\t" : ""));
            if (show_retention)
                printf ("%sstring", (col++ ? "\t" : ""));
            if (show_archive)
                printf ("%sstring", (col++ ? "\t" : ""));
            if (show_tapeinfo)
            {
                printf ("%sstring", (col++ ? "\t" : ""));
                printf ("%sint", (col++ ? "\t" : ""));
            }
            if (show_size)
                printf ("%slonglong", (col++ ? "\t" : ""));
            if (show_session)
                printf ("%sstring\tlonglong\tstring\tstring\tstring", (col++ ? "\t" : ""));


            /* If we have no rec (because the show_info arguments did not resolve into at least one record),
             * then no keywords and no segment values and no link values will print. */
            if (keysSpecified)
            {
                list_llreset(keysSpecified);
                while ((node = list_llnext(keysSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    rec_key = drms_keyword_lookup (rec, name, 1);

                    if (rec_key)
                    {
                        printf("%s%s", (col++ ? "\t" : ""), drms_type_names[rec_key->info->type]);
                    }
                    else
                    {
                        printf("%s%s", (col++ ? "\t" : ""),  "TBD");
                    }
                }
            }

            if (segsSpecified)
            {
                list_llreset(segsSpecified);
                while ((node = list_llnext(segsSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    rec_seg = drms_segment_lookup(rec, name);
                    printf("%s%s", (col++ ? "\t" : ""), drms_prot2str(rec_seg->info->protocol));
                    if (want_dims)
                    {
                        printf("\tstring");
                    }
                }
            }

            if ((!segsSpecified || list_llgetnitems(segsSpecified) == 0) && want_path)
            {
                printf ("%sstring", (col++ ? "\t" : ""));
            }

            if (linksSpecified)
            {
                list_llreset(linksSpecified);
                while ((node = list_llnext(linksSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    rec_link = hcon_lookup_lower(&rec->links, name);
                    printf ("%s%s", (col++ ? "\t" : ""),  rec_link->info->type == DYNAMIC_LINK ? "dynamic" : "static");
                }
            }

            printf ("\n");

            /* now print format */
            /* ASSUME all records have same structure - might not be true for mixed queries, fix later */
            col=0;
            if (show_recnum)
                printf ("%s%%lld", (col++ ? "\t" : ""));
            if (show_sunum)
                printf ("%s%%lld", (col++ ? "\t" : ""));
            if (show_recordspec)
                printf ("%s%%s", (col++ ? "\t" : ""));
            if (show_online)
                printf ("%s%%s", (col++ ? "\t" : ""));
            if (show_retention)
                printf ("%s%%s", (col++ ? "\t" : ""));
            if (show_archive)
                printf ("%s%%s", (col++ ? "\t" : ""));
            if (show_tapeinfo)
            {
                printf ("%s%%s", (col++ ? "\t" : ""));
                printf ("%s%%04d", (col++ ? "\t" : ""));
            }
            if (show_size)
                printf ("%s%%lld", (col++ ? "\t" : ""));
            if (show_session)
                printf ("%s%%s\t%%lld\t%%s\t%%s\t%%s", (col++ ? "\t" : ""));

            /* If we have no rec (because the show_info arguments did not resolve into at least one record),
             * then no keywords and no segment values and no link values will print. */
            if (keysSpecified)
            {
                list_llreset(keysSpecified);
                while ((node = list_llnext(keysSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    rec_key = drms_keyword_lookup (rec, name, 1);
                    if (rec_key)
                    {
                        if (rec_key->info->type == DRMS_TYPE_TIME)
                        {
                            printf("%s%%s", (col++ ? "\t" : ""));
                        }
                        else
                        {
                            printf("%s%s", (col++ ? "\t" : ""), rec_key->info->format);
                        }
                    }
                    else
                    {
                        printf("%s%s", (col++ ? "\t" : ""),  "TBD");
                    }
                }
            }

            if (segsSpecified)
            {
                list_llreset(segsSpecified);
                while ((node = list_llnext(segsSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    printf("%s%%s", (col++ ? "\t" : ""));
                    if (want_dims)
                    {
                        printf("%s%%s", (col++ ? "\t" : ""));
                    }
                }
            }

            if ((!segsSpecified || list_llgetnitems(segsSpecified) == 0) && want_path)
            {
                printf("%s%%s", (col++ ? "\t" : ""));
            }

            if (linksSpecified)
            {
                list_llreset(linksSpecified);
                while ((node = list_llnext(linksSpecified)) != NULL)
                {
                    name = (char *)(node->data);
                    printf("%s%%s", (col++ ? "\t" : ""));
                }
            }
            printf ("\n");
        }
    }

    return DRMS_SUCCESS;
}

/* keys should be the keylist specified by the key= argument, not the actual valid keys */
static void PrintKeyInfo(int *col, DRMS_Record_t *rec, LinkedList_t *keysSpecified, int keyword_list, int max_precision, int binary)
{
    DRMS_Keyword_t *key = NULL;
    char *name = NULL;
    ListNode_t *node = NULL;

    /* now print keyword information */
    if (keysSpecified)
    {
        list_llreset(keysSpecified);
        while ((node = list_llnext(keysSpecified)) != NULL)
        {
            name = (char *)(node->data);

            key = drms_keyword_lookup(rec, name, 1);

            if (key)
            {
                if (keyword_list)
                {
                    printf("%s=", name);

                    /* if printing a hexadecimal string of bytes (for a string), then there is no need
                     * to escape the string with quotes - there will be no space characters in the
                     * hexadecimal string */
                    if (key->info->type != DRMS_TYPE_STRING || binary)
                    {
                        drms_keyword_printval2(key, max_precision, binary);
                    }
                    else
                    {
                        printf("\"");
                        drms_keyword_printval2(key, max_precision, binary);
                        printf("\"");
                    }

                    printf("\n");
                }
                else
                {
                    if ((*col)++)
                    {
                        printf ("\t");
                    }

                    drms_keyword_printval2(key, max_precision, binary);
                    // change here for full precision XXXXXX
                }
            }
            else
            {
                if (!keyword_list)
                {
                    printf("%sInvalidKeyname", ((*col)++ ? "\t" : ""));
                }
            }
        }
    }
}

static int PrintSegInfo(int *col, DRMS_Record_t *rec, LinkedList_t *segsSpecified, int want_path, int want_path_noret, int keyword_list, int want_dims, int displaySegPaths)
{
    int stat = DRMS_SUCCESS;
    ListNode_t *node = NULL;
    char *name = NULL;
    DRMS_Segment_t *seg = NULL;

    /* now show desired segments */
    if (segsSpecified)
    {
        if (displaySegPaths)
        {
            list_llreset(segsSpecified);
            while ((node = list_llnext(segsSpecified)) != NULL)
            {
                name = (char *)(node->data);

                /* drms_segment_lookup() will follow links. So seg->record ~= rec if
                 * the segment is a link. */
                seg = drms_segment_lookup(rec, name);
                if (seg)
                {
                    char fname[DRMS_MAXPATHLEN] = {0};
                    char path[DRMS_MAXPATHLEN] = {0};

                    if (seg->info->protocol != DRMS_DSDS && seg->info->protocol != DRMS_LOCAL)
                    {
                        if (want_path)
                        {
                            // use segs rec to get linked record's path
                            // At this point, we already called drms_stage_records(). But if there was no SU associated with an SUNUM, and
                            // the retrieve flag was set, then it used to be the case that no SU would be cached in the environment su cache.
                            // In that case, another drms_record_directory() call would have resulted into another call to SUM_get().
                            // But I changed that. Now the SU gets cached with an empty-string sudir. So, this
                            // call to drms_record_directory() will no longer result in a call to SUM_get() being made. Instead, the
                            // cached SU gets used (and the result is that "path" is the empty string). THIS ONLY APPLIES WHEN THE
                            // retrieve FLAG IS SET.
                            //
                            // When the retrieve flag is not set, and there is no SU associated with an SUNUM, no SU gets cached. We
                            // haven't really resolved the question of whether the SUNUM exists or not because we didn't ask SUMS
                            // to fetch the SU if it was on tape. The code that decides whether or not to ask SUMS for an SU looks
                            // at the SU cache - if there is no cached SU, then it asks SUMS to retrieve the SU. So if retrieve == 0, then
                            // a call to drms_record_directory() will result in another SUM_get(), unnecessarily. Instead, don't
                            // call drms_record_directory() if want_path_noret == true and the SU is not cached. Since we already called
                            // drms_stage_records(), we know that the SU is offline, or doesn't exist. Just set the path to the empty string.
                            //
                            if (want_path_noret)
                            {
                                // retrieve == 0
                                if (!seg->record->su)
                                {
                                    // SU is either offline or doesn't exist. Do not call SUM_get() - we already did that in drms_stage_records().
                                    *path = '\0';
                                    stat = DRMS_SUCCESS;
                                }
                                else
                                {
                                    // The SU is online (and cached). drms_record_directory() will not call SUM_get().  Do not call SUM_get() -
                                    // we already did that in drms_stage_records().
                                    stat = drms_record_directory(seg->record, path, 0);
                                }
                            }
                            else
                            {
                                // Since we called drms_stage_records() with the retrieve flag, if seg->record->su == NULL, then
                                // an SU with *su->sudir == '\0' will have been cached, so there will be no SUM_get() called. Instead
                                // path will be set to the empty string.
                                stat = drms_record_directory(seg->record, path, 1);
                            }

                            if (stat || *path == '\0')
                            {
                               // If there is no path, that means that the SU is offline or doesn't exit and retrieve == 0, or the SU doesn't
                               // exist and retrieve == 1.
                               strcpy(path,"**_NO_sudir_**");
                            }
                        }
                        else
                        {
                            // Empty string
                            strcpy(path,"");
                        }

                        // I guess this is just the base name of the file (no path leading to directory containing the file).
                        strncpy(fname, seg->filename, DRMS_MAXPATHLEN);
                    }
                    else
                    {
                        char *tmp = strdup(seg->filename);
                        char *sep = NULL;

                        if (tmp)
                        {
                            if ((sep = strrchr(tmp, '/')) != NULL)
                            {
                                *sep = '\0';
                                snprintf(path, sizeof(path), "%s", tmp);
                                snprintf(fname, sizeof(fname), "%s", sep + 1);
                            }
                            else
                            {
                                snprintf(fname, sizeof(fname), "%s", tmp);
                            }

                            free(tmp);
                        }

                        if (!want_path)
                        {
                            *path = '\0';
                        }
                    }

                    if (keyword_list)
                        printf("%s=", name);
                    else
                        if ((*col)++)
                            printf("\t");
                    printf("%s%s%s", path, (want_path ? "/" : ""), fname);
                    if (keyword_list)
                        printf("\n");

                    if (want_dims)
                    {
                        int iaxis, naxis = seg->info->naxis;
                        if (keyword_list)
                            printf("%s_info=", name);
                        else
                            printf("\t");
                        if (seg->info->islink)
                            sprintf("\"link to %s", seg->info->linkname);
                        else
                        {
                            printf("\"%s, %s, ", seg->info->unit, drms_prot2str(seg->info->protocol));
                            for (iaxis=0; iaxis<naxis; iaxis++)
                            {
                                if (iaxis)
                                    printf("x");
                                printf("%d",seg->axis[iaxis]);
                            }
                            printf("\"");
                        }
                        if (keyword_list)
                            printf("\n");
                    }
                }
                else
                {
                    char *nosegmsg = "InvalidSegName";
                    DRMS_Segment_t *segment = hcon_lookup_lower(&rec->segments, name);
                    if (segment && segment->info->islink)
                        nosegmsg = "BadSegLink";
                    if (!keyword_list)
                        printf ("%s%s", ((*col)++ ? "\t" : ""), nosegmsg);
                    else
                        printf("%s=%s\n", name, nosegmsg);
                }
            }
        }

        if (!displaySegPaths && want_path)
        {
            char path[DRMS_MAXPATHLEN] = {0};

            if (drms_record_numsegments(rec) <= 0)
            {
                snprintf(path, sizeof(path), "Record does not contain any segments - no record directory.");
            }
            else if (drms_record_isdsds(rec) || drms_record_islocal(rec))
            {
                /* Can get full path from segment filename.  But beware, there may be no
                 * datafile for a DSDS record.  */
                DRMS_Segment_t *seg = drms_segment_lookupnum(rec, 0); /* Only 1 seg per DSDS */
                if (*(seg->filename) == '\0')
                {
                    /* No file for this DSDS record */
                    snprintf(path, sizeof(path), "Record has no data file.");
                }
                else
                {
                    char *tmp = strdup(seg->filename);
                    char *sep = NULL;

                    if (tmp)
                    {
                        if ((sep = strrchr(tmp, '/')) != NULL)
                        {
                            *sep = '\0';
                            snprintf(path, sizeof(path), "%s", tmp);
                        }
                        else
                        {
                            snprintf(path, sizeof(path), "%s", tmp);
                        }

                        free(tmp);
                    }
                }
            }
            else
            {
                if (rec->su)
                {
                    // The SU is online. We don't have to worry about the case where SUM_get() didn't attempt to fetch an offline SU. No
                    // SUM_get() call will be made (the SU was cached).
                    if(want_path_noret)
                         stat=drms_record_directory (rec, path, 0);
                    else
                        stat=drms_record_directory (rec, path, 1);
                    if (stat)
                      strcpy(path,"**_NO_sudir_**");
                }
                else
                {
                   strcpy(path,"**_NO_sudir_**");
                }
            }

            if (keyword_list)
                printf("SUDIR=");
            else
                if ((*col)++)
                    printf("\t");
            printf("%s", path);
            if (keyword_list)
                printf("\n");
        }
    }

    return stat;
}

static void PrintLnkInfo(int *col, DRMS_Record_t *rec, LinkedList_t *linksSpecified, int keyword_list)
{
    int status = DRMS_SUCCESS;
    ListNode_t *node = NULL;
    char *name = NULL;
    DRMS_Link_t *link = NULL;
    DRMS_Record_t *linked_rec = NULL;

    /* now print link information */
    if (linksSpecified)
    {
        list_llreset(linksSpecified);
        while ((node = list_llnext(linksSpecified)) != NULL)
        {
            name = (char *)(node->data);

            link = hcon_lookup_lower(&rec->links, name);
            linked_rec = drms_link_follow(rec, name, &status);

            if (linked_rec)
            {
                if (keyword_list)
                {
                    printf("%s=", name);

                    if (link->info->type == DYNAMIC_LINK)
                    {
                        printf("\"");
                        drms_print_rec_query(linked_rec);
                        printf("\"");
                    }
                    else
                    {
                        printf("\"");
                        printf("%s[:#%lld]", linked_rec->seriesinfo->seriesname, linked_rec->recnum);
                        printf("\"");
                    }

                        printf("\n");
                }
                else
                {
                    if ((*col)++)
                    {
                        printf ("\t");
                    }

                    if (link->info->type == DYNAMIC_LINK)
                    {
                        drms_print_rec_query(linked_rec);
                    }
                    else
                    {
                        printf("%s[:#%lld]", linked_rec->seriesinfo->seriesname, linked_rec->recnum);
                    }
                }
            }
            else
            {
                if (!keyword_list)
                {
                    printf("%sInvalidLink", ((*col)++ ? "\t" : ""));
                }
            }
        }
    }
}

static int PrintStuff(DRMS_Record_t *rec, const char *rsq, int keyword_list, int show_recnum, int show_sunum, int show_recordspec, int parseRS, int show_online, int show_retention, int show_archive, int show_tapeinfo, int show_size, int show_session, int want_path, int want_path_noret, int want_dims, int displaySegPaths, LinkedList_t *keysSpecified, LinkedList_t *segsSpecified, LinkedList_t *linksSpecified, int nrecs, int nl, int showKeySegLink, int max_precision, int binary)
{
    int col;
    int status = 0;
    int printBogus = 0;
    int64_t sunum = -1;
    int noInfoReq = 0;

    if (!rec)
    {
        /* Print a record for a bad SUNUM. */
        printBogus = 1;

        /* rsq has the SUNUM (it is a pointer to an int64_t). */
        sunum = *((int64_t *)rsq);
    }

    col=0;
    if (keyword_list) /* if not in table mode, i.e. value per line mode then show record query for each rec */
    {
        if (nl)
            printf("\n");

        if (printBogus)
        {
            printf("# sunum=%lld\n", sunum);
        }
        else
        {
            printf("# ");
            drms_print_rec_query(rec);
        }
        printf("\n");
    }

    if (show_recnum)
    {
        if (printBogus)
        {
            if (keyword_list)
                printf("## recnum=NA\n");
            else
                printf ("%sNA", (col++ ? "\t" : ""));
        }
        else
        {
            if (keyword_list)
                printf("## recnum=%lld\n",rec->recnum);
            else
                printf ("%s%6lld", (col++ ? "\t" : ""), rec->recnum);
        }

    }

    if (show_sunum)
    {
        if (printBogus)
        {
            if (keyword_list)
                printf("## sunum=%lld\n", sunum);
            else
                printf ("%s%6lld", (col++ ? "\t" : ""), sunum);
        }
        else
        {
            if (keyword_list)
                printf("## sunum=%lld\n",rec->sunum);
            else
                printf ("%s%6lld", (col++ ? "\t" : ""), rec->sunum);
        }
    }

      if (!keyword_list)
      {
         if (show_recordspec)
         {
            if (col++)
            {
               printf("\t");
            }

            if (printBogus)
            {
                printf("NA");
            }
            else
            {
                if (!parseRS)
                {
                   drms_print_rec_query(rec);
                }
                else
                {
                   char querystring[DRMS_MAXQUERYLEN];
                   char *allvers = NULL; /* If 'y', then don't do a 'group by' on the primekey value.
                                          * The rationale for this is to allow users to get all versions
                                          * of the requested DRMS records */
                   char **sets = NULL;
                   DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
                   char **snames = NULL;
                   char **filts = NULL;
                   int nsets = 0;
                   DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
                   char *filter = NULL;
                   int err;

                   /* Obtain record-set specification for this query. */
                   drms_sprint_rec_query(querystring, rec);

                   /* Parse the record-set specification (put bars between parts). */
                   if (drms_record_parserecsetspec(querystring, &allvers, &sets, &settypes, &snames, &filts, &nsets, &rsinfo) != DRMS_SUCCESS || nsets != 1)
                   {
                      printf("%s(UNPARSEABLE)", querystring);
                   }
                   else
                   {
                      filter = drms_recordset_extractfilter(rec, sets[0], &err);

                      if (!err)
                      {
                         printf("%s|%s", snames[0], filter);
                      }
                      else
                      {
                         printf("%s(UNPARSEABLE)", querystring);
                      }
                   }

                   if (filter)
                   {
                      free(filter);
                   }

                   drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, &filts, nsets);
                }
            }
         }
      }

    if (show_online)
    {
        /* rec has the suinfo struct already */
        char *msg;

        if (printBogus)
        {
            msg = "NA";
        }
        else
        {
            if (!rec->suinfo)
                /* rec->sunum == -1 */
                msg = "NA";
            else if (*rec->suinfo->online_loc == '\0')
                /* rec->sunum is invalid */
                msg = "NA";
            else
                msg = rec->suinfo->online_status;
        }

        if (keyword_list)
            printf("## online=%s\n", msg);
        else
            printf("%s%s", (col++ ? "\t" : ""), msg);
    }

    if (show_retention)
    {
        /* rec has the suinfo struct already */
        char retain[20];

        if (printBogus)
        {
            strcpy(retain, "NA");
        }
        else
        {
            if (!rec->suinfo)
                /* rec->sunum == -1 */
                strcpy(retain, "NA");
            else if (*rec->suinfo->online_loc == '\0')
                /* rec->sunum is invalid */
                strcpy(retain, "NA");
            else
            {
                int y,m,d;
                if (strcmp("N", rec->suinfo->online_status) == 0)
                    strcpy(retain,"-1");
                else
                {
                    int nscanned = sscanf(rec->suinfo->effective_date, "%4d%2d%2d", &y,&m,&d);
                    if (nscanned == 3)
                        sprintf(retain, "%4d.%02d.%02d",y,m,d);
                    else
                        strcpy(retain, "NoRetValue ");
                }
            }
        }

        if (keyword_list)
            printf("## retain=%s\n", retain);
        else
            printf("%s%s", (col++ ? "\t" : ""), retain);
    }

    if (show_archive)
    {
        /* rec has the suinfo struct already */
        char *msg;

        if (printBogus)
        {
            msg = "NA";
        }
        else
        {
            if (!rec->suinfo)
            /* rec->sunum == -1 */
                msg = "NA";
            else if (*rec->suinfo->online_loc == '\0')
            /* rec->sunum is invalid */
                msg = "NA";
            else
            {
                if(rec->suinfo->pa_status == DAAP && rec->suinfo->pa_substatus == DAADP)
                    msg = "Pending";
                else
                    msg = rec->suinfo->archive_status;
            }
        }

        if (keyword_list)
            printf("## archive=%s\n", msg);
        else
            printf("%s%s", (col++ ? "\t" : ""), msg);
    }


    if (show_tapeinfo)
    {
        /* rec has the suinfo struct already */
        char *msg;
        int fn;

        if (printBogus)
        {
            msg = "NA";
            fn = -9999;
        }
        else
        {
            if (!rec->suinfo)
            {
                /* rec->sunum == -1 */
                msg = "NA";
                fn = -9999;
            }
            else if (*rec->suinfo->arch_tape == '\0')
            {
                msg = "NA";
                fn = -9999;
            }
            else
            {
                msg = rec->suinfo->arch_tape;
                fn = rec->suinfo->arch_tape_fn;
            }
        }

        if (keyword_list)
        {
            printf("## tapename=%s\n", msg);
            printf("## tapeinfo=%04d\n", fn);
        }
        else
        {
            printf("%s%s", (col++ ? "\t" : ""), msg);
            printf("%s%04d", (col++ ? "\t" : ""), fn);
        }
    }

    if (show_size)
    {
        /* rec has the suinfo struct already */
        char size[20];

        if (printBogus)
        {
            strcpy(size, "NA");
        }
        else
        {
            if (!rec->suinfo)
                /* rec->sunum == -1 */
                strcpy(size, "NA");
            else if (*rec->suinfo->online_loc == '\0')
                /* rec->sunum is invalid */
                strcpy(size, "NA");
            else
                sprintf(size, "%.0f", rec->suinfo->bytes);
        }

        if (keyword_list)
            printf("## size=%s\n", size);
        else
            printf("%s%s", (col++ ? "\t" : ""), size);
    }

    if (show_session)
    {  // show host, runtime, jsoc_version, and logdir
        char *runhost, *runtime, *jsoc_vers, *logdir;
        if (printBogus || get_session_info(rec, &runhost, &runtime, &jsoc_vers, &logdir))
        {
            if (keyword_list)
                printf("## host=ERROR\n## sessionid=ERROR\n## runtime=ERROR\njsoc_version=ERROR\nlogdir=ERROR\n");
            else
                printf("%sERROR\tERROR\tERROR\tERROR\tERROR", (col++ ? "\t" : ""));
        }
        else
        {
            if (keyword_list)
                printf("## host=%s\n## sessionid=%lld\n## runtime=%s\n## jsoc_version=%s\n## logdir=%s\n",
                       runhost, rec->sessionid, runtime, jsoc_vers, logdir);
            else
                printf("%s%s\t%lld\t%s\t%s\t%s", (col++ ? "\t" : ""), runhost, rec->sessionid, runtime, jsoc_vers, logdir);
            free(runhost);
            free(runtime);
            free(jsoc_vers);
            free(logdir);
        }
    }

    /* Not only does PrintSegInfo() print paths for specified segments, it prints paths for all
     * segments if no segments are specified (and the -p/-P flags are set). But if printBogus is True,
     * then we need to print an NA (PrintStuff() was called for a bad SU).
     */
    if (!printBogus)
    {
        /* All these functions advance the column counter as long as there is one key, seg, or link, unless
         * the keyword_list flag is set.
         *
         * keys/nkeys, segs/nsegs, links/nlinks do NOT contain a list of all keys, segments, or links. They contain
         * only those that were specified in the key= and seg= arguments. They do contain all keys or segments
         * if the -a or -A flags were set. links is empty, unless the -K flag was set, in which case links contains
         * a list of ALL links.
         */
        PrintKeyInfo(&col, rec, keysSpecified, keyword_list, max_precision, binary);

        /* PrintSegInfo() will advance the col pointer if want_path is True (and ) */
        status = PrintSegInfo(&col, rec, segsSpecified, want_path, want_path_noret, keyword_list, want_dims, displaySegPaths);
        PrintLnkInfo(&col, rec, linksSpecified, keyword_list);

        noInfoReq = (col == 0);
    }
    else
    {
        /* If we are printing a bogus line, we don't iterate through keys, segs, or links, so we do not know if
         * the user asked to print key, seg, or link information. Instead, use the showKeySegLink parameter. */
        if (want_path)
        {
            if (keyword_list)
            {
                printf("SUDIR=**_NO_sudir_**\n");
            }
            else
            {
                if (col++)
                {
                    printf("\t");
                }

                printf("**_NO_sudir_**");
            }
        }

        noInfoReq = ((col == 0) && !showKeySegLink);
    }

    if (!keyword_list && noInfoReq)
    {
        /* This test for no information requested should be moved out of this function. This function gets
         * executed for each record returned by show_info. However, it makes more sense to do
         * this check earlier and outside of the record loop. Look at the cmd-line flags to
         * see if the caller has provided at least one that causes information to be printed. */
        int count = 0;

        if (nrecs < 0)
        {
            /* We don't know how many records we have, because we used drms_open_recordset() to
             * open the records. We need to call drms_count_records() now.
             *
             * rsq must not be NULL
             */
            if (!printBogus)
            {
                XASSERT(rsq != NULL);

                count = drms_count_records(rec->env, (char *)rsq, &status);
                if (status)
                {
                    fprintf(stderr,"can't call drms_count_records() on %s.\n", rsq);
                    return 1;
                }
            }
        }
        else
        {
            /* n=XX was provided, so we know the total number of records now */
            count = nrecs;
        }

        if (!printBogus)
        {
            printf("%d records found, no other information requested\n", count);
        }
        else
        {
            printf("No information requested. Provide at least one argument that requests information be printed.\n");
        }

        return 1; /* Exit record loop. */
    }
    if (!keyword_list && (show_recnum || show_sunum || show_recordspec || show_online || show_session || show_retention || show_archive || show_tapeinfo || show_size || (keysSpecified ? list_llgetnitems(keysSpecified) : 0) || (segsSpecified ? list_llgetnitems(segsSpecified) : 0) || (linksSpecified ? list_llgetnitems(linksSpecified) : 0) || want_path))
        printf ("\n");

    return status;
}

/* returns status == 0 on successs and non-zero on failure. */
/* Bogus is an array of invalid SUNUMs. nBogus is the number of elements in this array. */
static int RecordLoopCursor(DRMS_Env_t *env, const char *rsq, DRMS_RecordSet_t *recordset, LinkedList_t *bogusList, int requireSUMinfo, int64_t *given_sunum, HContainer_t *suinfo, int want_path, int want_path_noret, const char* series, int show_all, int show_keys, int show_all_segs, int show_segs, int show_all_links, int quiet, int keyword_list, int show_recnum, int show_sunum, int show_recordspec, int parseRS, int show_online, int show_retention, int show_archive, int show_tapeinfo, int show_size, int show_session, int want_dims, int show_types, char *sunum_rs_query, LinkedList_t *keysSpecified, LinkedList_t *segsSpecified, LinkedList_t *linksSpecified, int max_precision, int binary)
{
    /* rs->n is -1 - we won't know the total number of records until the loop terminates. */
    char key[128];
    SUM_info_t **ponesuinfo = NULL;
    int status = 0;
    DRMS_RecChunking_t cstat = kRecChunking_None;
    int newchunk;
    int irec;
    DRMS_Record_t *rec = NULL;
    int atleastone = 0;
    int64_t sunum = -1;
    int isu;
    int first;
    int displaySegPaths = 0;

    displaySegPaths = (show_segs || show_all_segs);

    /* First, print out a filler line for each invalid SU specified with the sunum argument to show_info. */
    if (bogusList)
    {
        ListNode_t *node = NULL;

        list_llreset(bogusList);
        first = 1;

        while ((node = list_llnext(bogusList)) != NULL)
        {
            sunum = *((int64_t *)(node->data));

            /* If the rec argument to PrintStuff() is NULL, then pass in the SUNUM in the record-set query argument. */
            if ((status = PrintStuff(NULL, (const char *)&sunum, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_path, want_path_noret, want_dims, displaySegPaths, keysSpecified, segsSpecified, linksSpecified, -1, !first, show_all || show_keys || show_all_segs || show_segs || show_all_links, max_precision, binary)) != 0)
            {
                first = 0;
                break;
            }

            first = 0;
        }
    }

    irec = 0;
    while (!status && recordset && ((rec = drms_recordset_fetchnext(env, recordset, &status, &cstat, &newchunk)) != NULL))
    {
        atleastone = 1;

        /* ART - status may be DRMS_REMOTESUMS_TRYLATER, but there should still be a
         * valid record in rec, unless the recordset is bad or there was a db timeout. */
        if (status == DRMS_ERROR_QUERYFAILED)
        {
            /* Check for error message. */
            const char *emsg = DB_GetErrmsg(env->session->db_handle);

            if (emsg)
            {
                fprintf(stderr, "DB error message: %s\n", emsg);
            }

            status = 1;
            break;
        }

        if (rec->sunum >= 0 && rec->suinfo == NULL)
        {
            /* We may have an SUM_info_t struct for this record from a previous call
             * to drms_getsuinfo() - use it here. If drms_recordset_fetchnext() was
             * called on a record-set that was already staged, then rec->suinfo should
             * not be NULL, because drms_recordset_fetchnext() will have called
             * drms_sortandstage_records(). */
            if (requireSUMinfo && (given_sunum && given_sunum[0] >= 0))
            {
                /* We already have the SUM_info_t structs in hand - we just need to set each record's
                 * suinfo field to point to the correct one. The suinfo container is keyed by sunum. */
                snprintf(key, sizeof(key), "%lld", rec->sunum);

                if ((ponesuinfo = (SUM_info_t **)hcon_lookup(suinfo, key)) != NULL)
                {
                    /* Multiple records may share the same SUNUM - each record gets a copy
                     * of the SUM_info_t. When suinfo is destroyed, the source SUM_info_t
                     * is deleted. */
                    rec->suinfo = (SUM_info_t *)malloc(sizeof(SUM_info_t));
                    *(rec->suinfo) = **ponesuinfo;
                }
                else
                {
                    /* unknown SUNUM */
                    fprintf(stderr, "Expected SUNUM '%s' not found.\n", key);
                    show_info_return(1);
                }
            }
        }

        if (want_path && (status == DRMS_REMOTESUMS_TRYLATER || status == DRMS_ERROR_SUMSTRYLATER))
        {
            /* The user wants segment files staged, but the files are being
             * staged asynchronously via remote sums (because the payload is
             * too large for synchronous download). */
            fprintf(stdout, "One or more data files are being staged asynchronously - try again later.\n");

            /* Ideally sum_export_svc() will keep track of "pending" su transfers,
             * but for now just bail. Once sum_export_svc() tracks these,
             * then calls to drms_recordset_fetchnext() that attempt to get
             * pending sus will return some appropriate return code, and then
             * show_info can handle that code properly.
             */
            status = 0;
            break;
        }

        if (status)
        {
            break;
        }

        if ((status = PrintStuff(rec, rsq, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_path, want_path_noret, want_dims, displaySegPaths, keysSpecified, segsSpecified, linksSpecified, -1, irec != 0 || (bogusList && list_llgetnitems(bogusList) > 0), show_all || show_keys || show_all_segs || show_segs || show_all_links, max_precision, binary)) != 0)
        {
            break;
        }

        irec++;
    } /* while */

    if (!quiet && !atleastone && !env->print_sql_only)
    {
        printf ("** No records in selected data set, query was %s **\n", rsq);
    }

    return status;
}

static int RecordLoopNoCursor(DRMS_Env_t *env, DRMS_RecordSet_t *recordset, LinkedList_t *bogusList, int requireSUMinfo, int64_t *given_sunum, HContainer_t *suinfo, int want_path, int want_path_noret, const char* series, int show_all, int show_keys, int show_all_segs, int show_segs, int show_all_links, int quiet, int keyword_list, int show_recnum, int show_sunum, int show_recordspec, int parseRS, int show_online, int show_retention, int show_archive, int show_tapeinfo, int show_size, int show_session, int want_dims, int show_types, char *sunum_rs_query, LinkedList_t *keysSpecified, LinkedList_t *segsSpecified, LinkedList_t *linksSpecified, int max_precision, int binary)
{
    /* rs->n contains the accurate number of records in the record set. */
    char key[128];
    int irec;
    DRMS_Record_t *rec = NULL;
    SUM_info_t **ponesuinfo = NULL;
    int status = 0;
    int64_t sunum = -1;
    int isu;
    int first;
    int displaySegPaths = 0;


    displaySegPaths = (show_segs || show_all_segs);

    /* First, print out a filler line for each invalid SU specified with the sunum argument to show_info. */
    if (bogusList)
    {
        ListNode_t *node = NULL;

        list_llreset(bogusList);
        first = 1;
        while ((node = list_llnext(bogusList)) != NULL)
        {
            sunum = *((int64_t *)(node->data));

            /* If the rec argument to PrintStuff() is NULL, then pass in the SUNUM in the record-set query argument. */
            if ((status = PrintStuff(NULL, (const char *)&sunum, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_path, want_path_noret, want_dims, displaySegPaths, keysSpecified, segsSpecified, linksSpecified, -1, !first, show_all || show_keys || show_all_segs || show_segs || show_all_links, max_precision, binary)) != 0)
            {
                first = 0;
                break;
            }

            first = 0;
        }
    }

    for (irec = 0; !status && recordset && irec < recordset->n; irec++)
    {
        rec = recordset->records[irec];  /* pointer to current record */

        if (rec->sunum >= 0)
        {
            if (requireSUMinfo && (given_sunum && given_sunum[0] >= 0))
            {
                snprintf(key, sizeof(key), "%lld", rec->sunum);

                if ((ponesuinfo = (SUM_info_t **)hcon_lookup(suinfo, key)) != NULL)
                {
                    /* records take ownership of the SUM_info_t - so need to remove from suinfo. */
                    rec->suinfo = *ponesuinfo;
                    hcon_remove(suinfo, key);
                }
                else
                {
                    /* unknown SUNUM */
                }
            }
        }

        if ((status = PrintStuff(rec, NULL, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_path, want_path_noret, want_dims, displaySegPaths, keysSpecified, segsSpecified, linksSpecified, recordset->n, irec != 0 || (bogusList && list_llgetnitems(bogusList) > 0), show_all || show_keys || show_all_segs || show_segs || show_all_links, max_precision, binary)) != 0)
        {
            break;
        }
    }

    return status;
}

static void FreeParts(void *data)
{
   SIParts_t *parts = (SIParts_t *)data;

   if (parts->series)
   {
      free(parts->series);
      parts->series = NULL;
   }

   if (parts->filter)
   {
      free(parts->filter);
      parts->filter = NULL;
   }
}

static int SetWebArg(Q_ENTRY *req, const char *key, char **arglist, size_t *size)
{
    char *value = NULL;
    char buf[1024];

    if (req)
    {
        value = (char *)qEntryGetStr(req, key);
        if (value)
        {
            if (!cmdparams_set(&cmdparams, key, value))
            {
                return(1);
            }

            /* keep a copy of the web arguments provided via HTTP POST so that we can debug issues more easily. */
            snprintf(buf, sizeof(buf), "%s='%s' ", key, value);
            *arglist = base_strcatalloc(*arglist, buf, size);
        }
    }

    return(0);
}

static int OpenAllTemplateRecords(DRMS_Env_t *env, const char *series, LinkedList_t **listOut)
{
    DRMS_Record_t *templRec = NULL;
    HIterator_t *last = NULL;
    DRMS_Link_t *link = NULL;
    int drmsStatus = DRMS_SUCCESS;
    int rv = 0;

    XASSERT(series != NULL && *series != '\0');
    XASSERT(listOut);

    templRec = drms_template_record(env, series, NULL);

    if (!templRec)
    {
        fprintf(stderr, "series %s not found\n", series);
    }
    else
    {
        if (!*listOut)
        {
            *listOut = list_llcreate(sizeof(DRMS_Record_t *), NULL);
        }

        if (!listOut)
        {
            fprintf(stderr, "out of memory\n");
            rv = 1;
        }
        else
        {
            list_llinserttail(*listOut, &templRec);

            while ((link = drms_record_nextlink(templRec, &last)) != NULL)
            {
                if ((rv = OpenAllTemplateRecords(env, link->info->target_series, listOut)) != 0)
                {
                    break;
                }
            }
        }
    }

    if (last)
    {
        hiter_destroy(&last);
    }

    return rv;
}

/* Module main function. */
int DoIt(void)
  {
  int status = 0;
  DRMS_RecordSet_t *recordset = NULL;
  DRMS_Record_t *rec;
  int inqry; // means "has a record-set filter"
  int atfile; // means "rs spec was an atfile"
						/* Get command line arguments */
  const char *in;
    char **keysArrIn = NULL;
    char **segsArrIn = NULL;
    int lenKeysArrIn = -1;
    int lenSegsArrIn = -1;
    LinkedList_t *validKeysSpecified = NULL; /* to pass to drms_open_records() to populate a subset of all keyword structs */
    LinkedList_t *keysSpecified = NULL; /* the verified key columns to print, specified by either the 'key' or '-a' argument */
    LinkedList_t *segsSpecified = NULL; /* the verified seg columns to print, specified by either the 'seg' or '-A' argument*/
    LinkedList_t *linksSpecified = NULL; /* the verified link columns to print, specified by the '-K' argument */

    Hash_Table_t keysHT;
    Hash_Table_t validKeysHT;
    Hash_Table_t segsHT;
    Hash_Table_t linksHT;

  int show_keys;
  int show_segs;
  int jsd_list;
  int list_keys;
    int retrieveLinksIn = 1;
    int retrieveLinks = 1;
  int show_all;
  int show_all_segs;
  int autobang = 0;
  int show_all_links;
  int show_recordspec;
  int show_stats;
  int show_types;
      int parseRS;
  int verbose;
  int max_recs;
      int cursoredQ;
  int quiet;
  int show_retention;
  int show_archive;
  int show_online;
  int disableTO;
  int show_recnum;
  int show_sunum;
  int show_tapeinfo;
  int show_size;
  int show_session;
  int keyword_list;
  int want_count;
  int want_path;
  int want_path_noret;
  int want_dims;
  int dorecs;
  int64_t *given_sunum = NULL; /* array of 64-bit sunums provided in the'sunum=...' argument. */
  int nsunum; /* number of sunums provided in the 'sunum=...' argument. */
  int requireSUMinfo;
  char *sunum_rs_query = NULL;
  char *autobangstr = NULL;
  char *finalin = NULL;
  char seriesnameforheader[DRMS_MAXSERIESNAMELEN]; /* show_info will not work if the record-set string specifies more than one series. */
  HContainer_t *suinfo = NULL;
  LinkedList_t *parsedrs = NULL;
  int iset;
  int err = 0;
  int drmsstat;
  DRMS_Record_t *templrec = NULL;
  char *filter = NULL;
  int64_t *bogus = NULL;
  int nBogus = 0;
    LinkedList_t *bogusList = NULL;
    char *segs[1024] = {0};
    int nsegs = 0;
    char *keys[1024];
    int nkeys = 0;
    char *links[1024];
    int nlinks = 0;
    ListNode_t *node = NULL;
    SIParts_t *parts = NULL;
    int iKey;
    int iSeg;

    int max_precision = 0;
    int binary = 0;

  // Include this code segment to allow operating show_info as a cgi-bin program.
  // It will preceed any output to stdout with the content-type info for text.
  // I.e. if the param QUERY_STRING is present it will assume it was called
  // via a web GET call.  No other special formatting will be done.
  // Note: this code could work in most programs that print to stdout.
  // The variable "from_web" is made just in case some use of the fact might be made.
  int from_web;
  char *web_query;
  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = strcmp (web_query, "Not Specified") != 0;

    int postorget = 0;
    char *webarglist = NULL;
    size_t webarglistsz;

    if (getenv("REQUEST_METHOD"))
    {
        postorget = (strcasecmp(getenv("REQUEST_METHOD"), "POST") == 0 || strcasecmp(getenv("REQUEST_METHOD"), "GET") == 0);
    }

    if (from_web)
    {
        Q_ENTRY *req = NULL;

        /* If we are here then one of three things is true (implied by the existence of QUERY_STRING):
         *   1. We are processing an HTTP GET. The webserver will put the arguments in the
         *      QUERY_STRING environment variable.
         *   2. We are processing an HTTP POST. The webserver will NOT put the arguments in the
         *      QUERY_STRING environment variable. Instead the arguments will be passed to show_info
         *      via stdin. QUERY_STRING should not be set, but it looks like it might be. In any
         *      case qDecoder will ignore it.
         *   3. show_info was invoked via the cmd-line, and the caller provided the QUERY_STRING
         *      argument. The caller is trying to emulate an HTTP request - they want to invoke
         *      the web-processing code, most likely to develop or debug a problem.
         *
         *   If we are in case 3, then we need to make sure that the QUERY_STRING environment variable
         *   is set since qDecoder will be called, and to process a GET, QUERY_STRING must be set.
         */
        if (!getenv("QUERY_STRING"))
        {
            /* Either case 2 or 3. Definitely not case 1. */
            if (!postorget)
            {
                /* Case 3 - set QUERY_STRING from cmd-line arg. */
                setenv("QUERY_STRING", web_query, 1);

                /* REQUEST_METHOD is not set - set it to GET. */
                setenv("REQUEST_METHOD", "GET", 1);
            }
        }

        /* Use qDecoder to parse HTTP POST requests. qDecoder actually handles
         * HTTP GET requests as well.
         * See http://www.qdecoder.org
         */

        webarglistsz = 2048;
        webarglist = (char *)calloc(1, webarglistsz);

        req = qCgiRequestParseQueries(NULL, NULL);
        if (req)
        {
            /* Accept only known key-value pairs - ignore the rest. */
            if (SetWebArg(req, "ds", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "key", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "seg", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "a", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "A", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "b", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "B", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "c", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "C", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "d", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "e", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "h", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "i", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "I", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "j", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "k", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "K", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "l", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "M", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "n", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "o", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "O", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "p", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "P", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "q", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "r", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "R", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "s", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "S", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "t", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "T", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "v", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "x", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "z", &webarglist, &webarglistsz)) show_info_return(1);
            if (SetWebArg(req, "sunum", &webarglist, &webarglistsz)) show_info_return(1);

            qEntryFree(req);
        }

        free(webarglist);
        webarglist = NULL;

        printf("Content-type: text/plain\n\n");
    }

    if (web_query)
    {
        free(web_query);
        web_query = NULL;
    }
  // end of web support stuff

  if (nice_intro ()) return (0);

    in = cmdparams_get_str (&cmdparams, "ds", NULL);
    lenKeysArrIn = cmdparams_get_strarr(&cmdparams, "key", &keysArrIn, NULL);
    lenSegsArrIn = cmdparams_get_strarr(&cmdparams, "seg", &segsArrIn, NULL);
    show_keys = (lenKeysArrIn > 0);
    show_segs = (lenSegsArrIn > 0);

  max_recs =  cmdparams_get_int (&cmdparams, "n", NULL);
  nsunum = cmdparams_get_int64arr(&cmdparams, "sunum", &given_sunum, &status);

  if (status != CMDPARAMS_SUCCESS)
  {
     fprintf(stderr, "Invalid argument 'sunum=%s'.\n", cmdparams_get_str(&cmdparams, "sunum", NULL));
     return 1;
  }

  show_all = cmdparams_get_int (&cmdparams, "a", NULL) != 0;
  show_all_segs = cmdparams_get_int (&cmdparams, "A", NULL) != 0;
  binary = cmdparams_isflagset(&cmdparams, "B");
  autobang = cmdparams_isflagset(&cmdparams, "b");
  want_count = cmdparams_get_int (&cmdparams, "c", NULL) != 0;
  retrieveLinksIn = !cmdparams_isflagset(&cmdparams, "C");
  want_dims = cmdparams_get_int (&cmdparams, "d", NULL) != 0;
  show_recordspec = cmdparams_get_int (&cmdparams, "i", NULL) != 0;
  show_session = cmdparams_get_int (&cmdparams, "I", NULL) != 0;
  jsd_list = cmdparams_get_int (&cmdparams, "j", NULL) != 0;
  keyword_list =  cmdparams_get_int(&cmdparams, "k", NULL) != 0;
  show_all_links = cmdparams_get_int (&cmdparams, "K", NULL) != 0;
  list_keys = cmdparams_get_int (&cmdparams, "l", NULL) != 0;
  max_precision = cmdparams_isflagset(&cmdparams, "M");
  show_stats = cmdparams_get_int (&cmdparams, "s", NULL) != 0;
  show_online = cmdparams_get_int (&cmdparams, "o", NULL) != 0;
  disableTO = cmdparams_isflagset(&cmdparams, "O");
  want_path = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
  want_path_noret = cmdparams_get_int (&cmdparams, "P", NULL) != 0;
  quiet = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  show_recnum =  cmdparams_get_int(&cmdparams, "r", NULL) != 0;
  show_retention = cmdparams_get_int (&cmdparams, "R", NULL) != 0;
  show_sunum =  cmdparams_get_int(&cmdparams, "S", NULL) != 0;
  show_tapeinfo =  cmdparams_get_int(&cmdparams, "T", NULL) != 0;
  show_archive = cmdparams_get_int (&cmdparams, "x", NULL) != 0;
  show_size =  cmdparams_get_int(&cmdparams, "z", NULL) != 0;
  show_types =  cmdparams_get_int(&cmdparams, "t", NULL) != 0;
  parseRS = cmdparams_isflagset(&cmdparams, kArgParseRS);
  verbose = cmdparams_isflagset(&cmdparams, "v");

  dorecs = (show_all || show_all_segs || want_dims || show_recordspec || want_path || want_path_noret || show_keys || show_segs || show_all_links || show_recnum || show_online || show_retention || show_archive || show_tapeinfo || show_size || show_session || show_types);

      /* If autobang is enabled, then set the string that will be used in all recordset specifications. */
      if (autobang)
      {
          autobangstr = "[! 1=1 !]";
      }

      // Set a 10-minute database statement time-out. This code can be disabled by providing the
      // -O flag on the command-line. THIS CAN ONLY BE DONE IN DIRECT-CONNECT CODE! We don't want
      // to have a socket-connect module affect the time-out for all drms_server clients.
#ifndef DRMS_CLIENT
      if (!disableTO && drms_env->dbtimeout == INT_MIN)
      {
          if (db_settimeout(drms_env->session->db_handle, 600000))
          {
              fprintf(stderr, "Failed to modify db-statement time-out to %d.\n", 600000);
          }
      }
#endif

  if(want_path_noret) want_path = 1;	/* also set this flag */

  requireSUMinfo = show_online || show_retention || show_archive || show_tapeinfo || show_size || show_session;

  /* At least seriesname or sunum must be specified */
      /* THIS CODE DOES NOT FOLLOW LINKS TO TARGET SEGMENTS. */
  if (given_sunum && given_sunum[0] >= 0)
  {
     /* use sunum to get seriesname instead of ds= or stand-along param */
     size_t querylen;
     SUM_info_t *onesuinfo = NULL;
     SUM_info_t *lastsuinfo = NULL;
     SUM_info_t **ponesuinfo = NULL;
     char intstr[256];
     int firstone;
     char key[128];
     int isunum;
     int newSeries;

     /* The whole point of this code block is to form a record-set query from the
      * list of sunums provided, and the owning series for those sunums. To get the
      * owning series, we need to call SUM_infoEx(). And we need
      * a record-set query if we will be requesting DRMS info about the records
      * that those sunums identify.
      *
      * Create a container to hold the results of the SUM_infoEx() call.
      * If the user has specified 'show_archive', 'show_size', etc., then we'll
      * need the SUM_info_t information later on.
      *
      * The record-set query created from the sunums and the owning-series strings
      * will not necessarily result in records that are ordered by the original
      * sunum order.
      */
     if ((status = GetSUMinfo(drms_env, &suinfo, given_sunum, nsunum)) != DRMS_SUCCESS)
     {
        /* Either an error, or a pending asynchronous remotesums request. */
        show_info_return(status);
     }

     /* Make the sunum_rs_query string by iterating through the suinfo container,
      * and sorting the SUNUMs by owning series. */
     querylen = sizeof(char) * DRMS_MAXQUERYLEN;
     sunum_rs_query = malloc(querylen);
     *sunum_rs_query = '\0';
     newSeries = 0;

     for (isunum = 0; isunum < nsunum; isunum++)
     {
        snprintf(key, sizeof(key), "%llu", (unsigned long long)given_sunum[isunum]);
        ponesuinfo = hcon_lookup(suinfo, key);

        if (!ponesuinfo)
        {
           /* bad sunum - something went wrong. */
           printf("### show_info: Unexpected sunum '%s'.\n", key);
           break;
        }

        onesuinfo = *ponesuinfo;

         /* It is possible that onesuinfo is NULL. Lower-level code could return a NULL info-struct pointer
          * if the sunum requested was -1. */
         if (!onesuinfo)
         {
             /* skip this sunum - there is no info struct for it. */
             continue;
         }
         else if (*onesuinfo->online_loc == '\0')
         {
            /* The sunum was invalid. Add a item to the list of bogus SUs. */
            if (!bogusList)
            {
                bogusList = list_llcreate(sizeof(int64_t), NULL);
                if (!bogusList)
                {
                    fprintf(stderr, "Out of memory.\n");
                    show_info_return(1);
                }
            }

            list_llinserttail(bogusList, &(given_sunum[isunum]));
            continue;
         }

        if (!lastsuinfo || strcasecmp(onesuinfo->owning_series, lastsuinfo->owning_series) != 0)
        {
           /* Got a new series (so start a new subquery). */
           if (lastsuinfo)
           {
              sunum_rs_query = base_strcatalloc(sunum_rs_query, " !],", &querylen);
              newSeries = 0;
           }

           snprintf(intstr, sizeof(intstr), "%s[! sunum=", onesuinfo->owning_series);
           sunum_rs_query = base_strcatalloc(sunum_rs_query, intstr, &querylen);
           lastsuinfo = onesuinfo;
           firstone = 1;
           newSeries = 1;
        }

        /* append an sunum */
        if (!firstone)
        {
           sunum_rs_query = base_strcatalloc(sunum_rs_query, "OR sunum=", &querylen);
        }
        else
        {
           firstone = 0;
        }

        snprintf(intstr, sizeof(intstr), "%llu", (unsigned long long)onesuinfo->sunum);
        sunum_rs_query = base_strcatalloc(sunum_rs_query, intstr, &querylen);
     } /* SU loop */

    if (newSeries == 1)
    {
         /* Need to end the current subquery. */
         sunum_rs_query = base_strcatalloc(sunum_rs_query, " !]", &querylen);
         newSeries = 0;
    }

     if (strlen(sunum_rs_query) == 0 && bogusList == NULL)
     {
        printf("### show_info: given sunum=%s invalid, must quit\n", cmdparams_get_str(&cmdparams, "sunum", NULL));
        free(sunum_rs_query);
        sunum_rs_query = NULL;
        show_info_return(1);
     }

     if (sunum_rs_query && strlen(sunum_rs_query) > 0)
     {
        in = sunum_rs_query;
        /* free sunum_rs_query before exiting. */
         /* Don't modify sunum_rs_query if -b flag is set - sunum_rs_query has prime-key logic disabled
          * already. */
     }
     else
     {
        in = "";
     }
  }
  else if (strcmp(in, "Not Specified") == 0)
    {
       /* ds arg was not provided */
    if (cmdparams_numargs(&cmdparams) < 1 || !(in=cmdparams_getarg (&cmdparams, 1)))
      {
      printf("### show_info: ds=<record_query> parameter is required, must quit\n");
      show_info_return(1);
      }
    }
  if (verbose)
     {
     /* Print something to identify what series is being accessed */
     printf("show_info() query is %s.\n", in);
     }

      /* The variable "in" will have been finalized by this point. If the -b flag has been
       * specified, then append the autobangstr string. */

      /* Parse "in" to isolate the record-set filter.
       *
       * Not only do we need to isolate the filter, we need the parsed fields to decide, later, whether
       * show_info should continue (there must either be a n=XX arg, or a record-set filter).
       * BTW, checking for an @file argument isn't sufficient - the file could be empty, or
       * it could contain seriesnames with no filters. The parsing below will make sure a filter
       * is found somewhere, even if it is inside the @file. */
       if (in && strlen(in) > 0)
      {
          char *allvers = NULL; /* If 'y', then don't do a 'group by' on the primekey value.
                                 * The rationale for this is to allow users to get all versions
                                 * of the requested DRMS records */
          char **sets = NULL;
          DRMS_RecordSetType_t *settypes = NULL; /* a maximum doesn't make sense */
          char **snames = NULL;
          char **filts = NULL;
          int nsets = 0;

          DRMS_RecQueryInfo_t rsinfo; /* Filled in by parser as it encounters elements. */
          SIParts_t apart;

          if (drms_record_parserecsetspec(in, &allvers, &sets, &settypes, &snames, &filts, &nsets, &rsinfo) != DRMS_SUCCESS)
          {
              show_info_return(2);
          }

          /* HELLO! snames contains information only if the settype is kRecordSetType_DRMS. For DSDS ds's, for example,
           * snames[i] will be NULL. The series name for DSDS ds's is accessible at DRMS_Record_t::seriesinfo->name.
           *
           * show_info DOES NOT SUPPORT MULTIPLE RECORD-SET SUB-SETS. This is why we look at just the first record-set subset
           * (there should be only one subset).
           */
          if (nsets > 0)
          {
              if (settypes[0] == kRecordSetType_DRMS)
              {
                  snprintf(seriesnameforheader, sizeof(seriesnameforheader), "%s", snames[0]);
              }
              else
              {
                  *seriesnameforheader = '\0';
              }
          }
          else
          {
              snprintf(seriesnameforheader, sizeof(seriesnameforheader), "%s", "somecrazyname");
          }

          inqry = ((rsinfo & kFilters) != 0);
          atfile = ((rsinfo & kAtFile) != 0);

          if (autobangstr && !sunum_rs_query)
          {
              /* Replace filter with filter + autobangstr appended. */
              char *filterbuf = NULL;
              size_t fbsz = 128;
              char *intermed = NULL;

              filterbuf = malloc(fbsz);
              finalin = strdup(in);
              if (filterbuf && finalin)
              {
                  memset(filterbuf, 0, sizeof(filterbuf));
              }
              else
              {
                  show_info_return(3);
              }

              for (iset = 0; iset < nsets; iset++)
              {
                  if (settypes[iset] == kRecordSetType_DSDSPort || settypes[iset] == kRecordSetType_DRMS)
                  {
                      templrec = drms_template_record(drms_env, snames[iset], &drmsstat);
                      if (DRMS_ERROR_UNKNOWNSERIES == drmsstat)
                      {
                          fprintf(stderr, "Unable to open template record for series '%s'; this series does not exist.\n", snames[iset]);
                          err = 1;
                          break;
                      }
                      else
                      {
                          filter = drms_recordset_extractfilter(templrec, sets[iset], &err);

                          if (!err)
                          {
                              *filterbuf = '\0';
                              if (filter)
                              {
                                  filterbuf = base_strcatalloc(filterbuf, filter, &fbsz);
                                  filterbuf = base_strcatalloc(filterbuf, autobangstr, &fbsz);

                                  /* Replace filter with filterbuf. */
                                  intermed = base_strreplace(finalin, filter, filterbuf);
                                  free(finalin);
                                  finalin = intermed;
                              }
                              else
                              {
                                  filterbuf = base_strcatalloc(filterbuf, snames[iset], &fbsz);
                                  filterbuf = base_strcatalloc(filterbuf, autobangstr, &fbsz);

                                  /* Replace series name with filterbuf. */
                                  intermed = base_strreplace(finalin, snames[iset], filterbuf);
                                  free(finalin);
                                  finalin = intermed;
                              }

                              if (!parsedrs)
                              {
                                  parsedrs = list_llcreate(sizeof(SIParts_t), (ListFreeFn_t)FreeParts);
                              }

                              apart.series = strdup(snames[iset]);
                              if (filter)
                              {
                                  size_t sz = strlen(filter) + 1;

                                  apart.filter = strdup(filter);
                                  apart.filter = base_strcatalloc(apart.filter, autobangstr, &sz);
                              }
                              else
                              {
                                  apart.filter = NULL;
                              }

                              list_llinserttail(parsedrs, &apart);
                          }

                          free(filter);

                          if (err)
                          {
                              break;
                          }
                      }
                  }
              } /* loop over sets */

              if (!err)
              {
                  in = finalin;
              }

              free(filterbuf);
              filterbuf = NULL;
          } /* autobang */
          else
          {
              for (iset = 0; iset < nsets; iset++)
              {
                  if (settypes[iset] == kRecordSetType_DSDSPort || settypes[iset] == kRecordSetType_DRMS)
                  {
                      templrec = drms_template_record(drms_env, snames[iset], &drmsstat);
                      if (DRMS_ERROR_UNKNOWNSERIES == drmsstat)
                      {
                          fprintf(stderr, "unable to open template record for series '%s'; this series does not exist\n", snames[iset]);
                          err = 1;
                          break;
                      }
                      else
                      {
                          filter = drms_recordset_extractfilter(templrec, sets[iset], &err);

                          if (!err)
                          {
                              if (!parsedrs)
                              {
                                  parsedrs = list_llcreate(sizeof(SIParts_t), (ListFreeFn_t)FreeParts);
                              }

                              apart.series = strdup(templrec->seriesinfo->seriesname);
                              if (filter)
                              {
                                  apart.filter = strdup(filter);
                              }
                              else
                              {
                                  apart.filter = NULL;
                              }

                              list_llinserttail(parsedrs, &apart);
                          }

                          free(filter);

                          if (err)
                          {
                              break;
                          }
                      }
                  }
              } /* loop over sets */
          }

          drms_record_freerecsetspecarr(&allvers, &sets, &settypes, &snames, &filts, nsets);
      }

      if (parseRS && parsedrs && !dorecs)
      {
          /* No exit from this block. The needed info has already been extracted from the input record-set specification */
          if (!quiet)
          {
              printf("SERIES\tFILTER\n");
          }

          list_llreset(parsedrs);

          while ((node = list_llnext(parsedrs)) != NULL)
          {
              parts = (SIParts_t *)(node->data);
              if (parts->filter)
              {
                  printf("%s\t%s\n", parts->series, parts->filter);
              }
              else
              {
                  printf("%s\t%s\n", parts->series, "NONE");
              }
          }

          list_llfree(&parsedrs);
          show_info_return(0);
      }
  /*  if -j, -l or -s is set, just do the short function and exit */
  else if (list_keys || jsd_list || show_stats)
  {
    /* There is no return from this block! */
    char *p, *seriesname;
    int is_drms_series = 1;

    /* If the caller provided one or more sunums instead of a record-set query, use the
     * drms_record_getinfo() call to find the series names that go with the sunums
     * so that the DRMS records can be opened. */

    /* Only want keyword info so get only the template record for drms series or first record for other data */

    /* ART - This won't work if the input recordset query has multiple sub-recordset queries. */
    seriesname = strdup (in);
    if ((p = index(seriesname,'['))) *p = '\0';
    rec = drms_template_record (drms_env, seriesname, &status);
        if (status)
        {
            /* either it is not a drms series (e.g., a dir name that can be interpreted as a DSDS dataset)
             * or not any recognizable series; try for non-drms series before quitting (drms_open_records()
             * handles a few different types of series specifiers; do not open links */
            recordset = drms_open_records2(drms_env, in, NULL, 0, 0, 0, &status);

            if (status == DRMS_ERROR_QUERYFAILED)
            {
                /* Check for error message. */
                const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);

                if (emsg)
                {
                    fprintf(stderr, "DB error message: %s\n", emsg);
                }
            }

            if (!recordset)
            {
                fprintf(stderr,"### show_info: series %s not found.\n",seriesname);
                if (seriesname)
                free (seriesname);
                show_info_return(1);
            }

            if (recordset->n < 1)
            {
                fprintf(stderr, "### show_info: non-drms series '%s' found but is empty\n", seriesname);

                if (seriesname)
                {
                    free (seriesname);
                }

                if (recordset)
                {
                    drms_close_records(recordset, DRMS_FREE_RECORD);
                    recordset = NULL;
                }

                show_info_return(1);
            }

            rec = recordset->records[0];
            is_drms_series = 0;
        }

    if (seriesname)
      free (seriesname);

    if (list_keys)
    {
       list_series_info(rec);
       if (recordset)
       {
          drms_close_records(recordset, DRMS_FREE_RECORD);
          recordset = NULL;
       }
       show_info_return(0);
    }
    else if (jsd_list)
    {
       drms_jsd_print(drms_env, rec->seriesinfo->seriesname);
       if (recordset)
       {
          drms_close_records(recordset, DRMS_FREE_RECORD);
          recordset = NULL;
       }
       show_info_return(0);
    }
    if (show_stats)
    {
       if (is_drms_series)
       {
          DRMS_RecordSet_t *rs;

          rs = drms_find_rec_first(rec, 1, 0);
          if (!rs || rs->n < 1)
            printf("No records Present\n");
          else
          {
             printf("First Record: ");
             drms_print_rec_query(rs->records[0]);
             if (rs->n > 1) printf(" is first of %d records matching first keyword", rs->n);
             printf(", Recnum = %lld\n", rs->records[0]->recnum);
             drms_free_records(rs);

             rs = drms_find_rec_last(rec, 1, 0);
             printf("Last Record:  ");
             drms_print_rec_query(rs->records[0]);
             if (rs->n > 1) printf(" is first of %d records matching first keyword", rs->n);
             printf(", Recnum = %lld\n", rs->records[0]->recnum);
             drms_free_records(rs);

             rs = drms_find_rec_last(rec, 0, 0);
             printf("Last Recnum:  %lld", rs->records[0]->recnum);
             printf("\n");
          }

           /* Print shadow-table status. */
           int shadowStat;
           int hasShadow = drms_series_shadowexists(drms_env, rec->seriesinfo->seriesname, &shadowStat);

           if (shadowStat)
           {
               printf("Has shadow table: ?");
           }
           else
           {
               printf("Has shadow table: %s", hasShadow ? "yes" : "no");
           }

           printf("\n");

          show_info_return(0);
       }
       else
       {
          printf("### Can not use '-s' flag for non-drms series. Sorry.\n");
          if (recordset)
          {
             drms_close_records(recordset, DRMS_FREE_RECORD);
             recordset = NULL;
          }
          show_info_return(1);
       }
    }
    /* ART - This fflush is not reachable. */
    fflush(stdout);
  }
      /* I think recordset == NULL at this point. */

  /* get count if -c flag set */
  if (want_count)
    {
    int count = drms_count_records(drms_env, (char *)in, &status);
    if (status)
      {
      fprintf(stderr,"### show_info: series %s not found.\n",in);
      show_info_return(1);
      }
    printf("%d", count);
    if (!quiet)
      printf(" records match the query");
    printf("\n");
    show_info_return(0);
    }

  /* NOW in mode to list per-record information.  Get recordset */

  /* check for poor usage of no query and no n=record_count */

  if (in && strlen(in) > 0 && !inqry && max_recs == 0 && !atfile)
    {
    fprintf(stderr, "### show_info - the query must contain a record-filter, or the n=num_records or @file argument must be present.\n");
    show_info_return(1);
    }

  /* Open record_set(s) */
    retrieveLinks = retrieveLinksIn;
    if (in && strlen(in) > 0)
    {
        cursoredQ = (max_recs == 0);

        if (cursoredQ)
        {
            /* set chunk size to something bigger than that of the SUM_infoEx() call; code
             * in drms_storageunit.c will subchunk this into the chunk size used by SUM_infoEx()
             */
            if (drms_recordset_setchunksize(4096) != DRMS_SUCCESS)
            {
                show_info_return(99);
            }
        }

        if (!show_all && !show_keys && !show_all_segs && !show_segs && !want_path && !show_all_links)
        {
            /* the user does not want anything to do with keywords or segments */
            retrieveLinks = 0;
        }
        else
        {
            DRMS_Record_t *thisTemplRec = NULL;
            DRMS_Record_t *templRec = NULL;
            LinkedList_t *allRecs = NULL;
            HIterator_t *last = NULL;
            DRMS_Keyword_t *oneKey = NULL;
            DRMS_Keyword_t *oneLinkedKey = NULL;
            DRMS_Segment_t *oneSeg = NULL;
            DRMS_Segment_t *oneLinkedSeg = NULL;
            DRMS_Link_t *oneLink = NULL;
            int validKey = 0;
            int validSeg = 0;

            if (retrieveLinksIn)
            {
                retrieveLinks = 0;
            }

            /* LOL - this if statement MUST be true (since the other half of the enclosing if statement if false) */
            if (show_all || show_keys || show_all_segs || show_segs || want_path || show_all_links)
            {
                /* only need to run this block if keys or segments are specified in some way (other
                 * than as part of the record-set specification - the code to open records will
                 * handle that case);
                 *
                 * want_path is essentially the same thing as printing segment information - if the user
                 * has specified -P/-p, but has provided no list of segments, then we should assume that
                 * the user wants info for ALL segments; just like with show_all_segs and show_segs, there
                 * could be a conflict between the DRMS segment list specified in {}, and the list of ALL
                 * segments;  */
                list_llreset(parsedrs);
                while ((node = list_llnext(parsedrs)) != NULL)
                {
                    parts = (SIParts_t *)(node->data);

                    thisTemplRec = drms_template_record(drms_env, parts->series, &status);
                    if (!thisTemplRec)
                    {
                        fprintf(stderr, "unable to locate series %s\n", parts->series);
                    }
                    else
                    {
                        /* open child template records so we can ensure that each specified child keyword/segment
                         * struct does actually exist in the child */
                        if (OpenAllTemplateRecords(drms_env, parts->series, &allRecs))
                        {
                            break;
                        }

                        iKey = 0;
                        while (1)
                        {
                            if (show_all)
                            {
                                oneKey = drms_record_nextkey(thisTemplRec, &last, 0);
                                if (!oneKey)
                                {
                                    break;
                                }
                            }
                            else if (show_keys)
                            {
                                if (iKey >= lenKeysArrIn)
                                {
                                    break;
                                }

                                oneKey = drms_keyword_lookup(thisTemplRec, keysArrIn[iKey], 0);
                            }
                            else
                            {
                                break;
                            }

                            validKey = 0;

                            if (oneKey)
                            {
                                if (!oneKey->info->islink)
                                {
                                    if (!drms_keyword_getimplicit(oneKey))
                                    {
                                        /* create keysArr from all non-implicit keywords in template record */
                                        validKey = 1;
                                    }
                                }
                                else if (!retrieveLinksIn)
                                {
                                    /* skip this key if not retrieving links; validKey will remain 0 */
                                }
                                else
                                {
                                    oneLinkedKey = oneKey;
                                    templRec = thisTemplRec;

                                    while (oneLinkedKey->info->islink)
                                    {
                                        /* get child key's template record */
                                        oneLink = (DRMS_Link_t *)hcon_lookup_lower(&templRec->links, oneLinkedKey->info->linkname);
                                        if (oneLink)
                                        {
                                            templRec = (DRMS_Record_t *)hcon_lookup_lower(&drms_env->series_cache, oneLink->info->target_series);
                                        }

                                        if (templRec)
                                        {
                                            /* get linked record template's key */
                                            oneLinkedKey = drms_keyword_lookup(templRec, oneLinkedKey->info->target_key, 0);

                                            if (oneLinkedKey)
                                            {
                                                if (!oneLinkedKey->info->islink)
                                                {
                                                    if (!drms_keyword_getimplicit(oneLinkedKey))
                                                    {
                                                        /* create keysArr from all non-implicit keywords in template record */

                                                        /* a valid key in a child series */
                                                        if (retrieveLinksIn)
                                                        {
                                                            retrieveLinks = 1;
                                                        }

                                                        validKey = 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (show_all)
                            {
                                /* work with valid keys only */
                                if (validKey)
                                {
                                    if (!keysSpecified)
                                    {
                                        keysSpecified = list_llcreate(DRMS_MAXKEYNAMELEN, NULL);

                                        if (!keysSpecified)
                                        {
                                            fprintf(stderr, "out of memory\n");
                                            break;
                                        }

                                        hash_init(&keysHT, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                                    }

                                    if (!hash_member(&keysHT, oneKey->info->name))
                                    {
                                        list_llinserttail(keysSpecified, oneKey->info->name);
                                        hash_insert(&keysHT, oneKey->info->name, "T");
                                    }

                                    if (!validKeysSpecified)
                                    {
                                        validKeysSpecified = list_llcreate(DRMS_MAXKEYNAMELEN, NULL);
                                        if (!validKeysSpecified)
                                        {
                                            fprintf(stderr, "out of memory\n");
                                            break;
                                        }

                                        hash_init(&validKeysHT, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                                    }

                                    if (!hash_member(&validKeysHT, oneKey->info->name))
                                    {
                                        list_llinserttail(validKeysSpecified, oneKey->info->name); /* to drms_open_records() - must be valid keys */
                                        hash_insert(&validKeysHT, oneKey->info->name, "T");
                                    }
                                }
                            }
                            else if (show_keys)
                            {
                                /* work with ALL keys, valid or otherwise, specified on command line */
                                if (!keysSpecified)
                                {
                                    keysSpecified = list_llcreate(DRMS_MAXKEYNAMELEN, NULL);

                                    if (!keysSpecified)
                                    {
                                        fprintf(stderr, "out of memory\n");
                                        break;
                                    }

                                    hash_init(&keysHT, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                                }

                                if (!hash_member(&keysHT, keysArrIn[iKey]))
                                {
                                    list_llinserttail(keysSpecified, keysArrIn[iKey]);
                                    hash_insert(&keysHT, keysArrIn[iKey], "T");
                                }

                                if (validKey)
                                {
                                    if (!validKeysSpecified)
                                    {
                                        validKeysSpecified = list_llcreate(DRMS_MAXKEYNAMELEN, NULL);
                                        if (!validKeysSpecified)
                                        {
                                            fprintf(stderr, "out of memory\n");
                                            break;
                                        }

                                        hash_init(&validKeysHT, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                                    }

                                    if (!hash_member(&validKeysHT, oneKey->info->name))
                                    {
                                        list_llinserttail(validKeysSpecified, oneKey->info->name); /* to drms_open_records() - must be valid keys */
                                        hash_insert(&validKeysHT, oneKey->info->name, "T");
                                    }
                                }
                            }

                            iKey++;
                        }

                        if (last)
                        {
                            hiter_destroy(&last);
                        }

                        iSeg = 0;
                        while (1)
                        {
                            if (show_all_segs || (want_path && !show_segs))
                            {
                                oneSeg = drms_record_nextseg(thisTemplRec, &last, 0);
                                if (!oneSeg)
                                {
                                    break;
                                }
                            }
                            else if (show_segs)
                            {
                                if (iSeg >= lenSegsArrIn)
                                {
                                    break;
                                }

                                oneSeg = (DRMS_Segment_t *)hcon_lookup_lower(&thisTemplRec->segments, segsArrIn[iSeg]);
                            }
                            else
                            {
                                break;
                            }

                            validSeg = 0;

                            if (oneSeg)
                            {
                                if (!oneSeg->info->islink)
                                {
                                    validSeg = 1;
                                }
                                else
                                {
                                    oneLinkedSeg = oneSeg;
                                    templRec = thisTemplRec;

                                    while (oneLinkedSeg->info->islink)
                                    {
                                        /* get child key's template record */
                                        oneLink = (DRMS_Link_t *)hcon_lookup_lower(&templRec->links, oneLinkedSeg->info->linkname);
                                        if (oneLink)
                                        {
                                            templRec = (DRMS_Record_t *)hcon_lookup_lower(&drms_env->series_cache, oneLink->info->target_series);
                                        }

                                        if (templRec)
                                        {
                                            /* get linked record template's key */
                                            oneLinkedSeg = (DRMS_Segment_t *)hcon_lookup_lower(&templRec->segments, oneLinkedSeg->info->target_seg);

                                            if (oneLinkedSeg)
                                            {
                                                if (!oneLinkedSeg->info->islink)
                                                {
                                                    /* a valid seg in a child series */
                                                    if (retrieveLinksIn)
                                                    {
                                                        retrieveLinks = 1;
                                                    }

                                                    validSeg = 1;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (show_all_segs || (want_path && !show_segs))
                            {
                                /* work with valid keys only */
                                if (validSeg)
                                {
                                    if (!segsSpecified)
                                    {
                                        segsSpecified = list_llcreate(DRMS_MAXSEGNAMELEN, NULL);
                                        hash_init(&segsHT, 13, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                                    }

                                    if (!hash_member(&segsHT, oneSeg->info->name))
                                    {
                                        list_llinserttail(segsSpecified, oneSeg->info->name);
                                        hash_insert(&segsHT, oneSeg->info->name, "T");
                                    }
                                }
                            }
                            else if (show_segs)
                            {
                                if (!segsSpecified)
                                {
                                    segsSpecified = list_llcreate(DRMS_MAXSEGNAMELEN, NULL);
                                    hash_init(&segsHT, 13, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                                }

                                /* work with ALL keys specified on command line */
                                if (!hash_member(&segsHT, segsArrIn[iSeg]))
                                {
                                    list_llinserttail(segsSpecified, segsArrIn[iSeg]);
                                    hash_insert(&segsHT, segsArrIn[iSeg], "T");
                                }
                            }

                            iSeg++;
                        }

                        if (last)
                        {
                            hiter_destroy(&last);
                        }

                        while (1)
                        {
                            if (show_all_links)
                            {
                                oneLink = drms_record_nextlink(thisTemplRec, &last);
                                if (!oneLink)
                                {
                                    break;
                                }

                                /* the user provided the -K flag (show all links) and at least one link does exist */
                                retrieveLinks = 1;
                            }
                            else
                            {
                                break;
                            }

                            if (!linksSpecified)
                            {
                                linksSpecified = list_llcreate(DRMS_MAXLINKNAMELEN, NULL);
                                hash_init(&linksHT, 13, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                            }

                            if (!hash_member(&linksHT, oneLink->info->name))
                            {
                                list_llinserttail(linksSpecified, oneLink->info->name);
                                hash_insert(&linksHT, oneLink->info->name, "T");
                            }
                        }

                        if (last)
                        {
                            hiter_destroy(&last);
                        }
                    }
                }
            }

            if (allRecs)
            {
                list_llfree(&allRecs);
            }
        }

        /* ART - at the lib DRMS level, the user can specify a set of segments with the {seglist} record-set filter; drms_open_records2()
         * will be able to determine if that seglist contains a segment from a linked record, and if so, it will set the
         * openlinks flag before calling drms_open_records_internal(), provided we set the openLinks argument here to 1
         */

        /* max_recs == 0 --> use 'cursor'
         * max_recs != 0 --> call drms_open_nrecords() internally
         */
        recordset = drms_open_records2(drms_env, in, validKeysSpecified, max_recs == 0, max_recs, retrieveLinks, &status);

        if (validKeysSpecified)
        {
            hash_free(&validKeysHT);
            list_llfree(&validKeysSpecified);
        }

        if (status == DRMS_ERROR_QUERYFAILED)
        {
            /* Check for error message. */
            const char *emsg = DB_GetErrmsg(drms_env->session->db_handle);

            if (emsg)
            {
              fprintf(stderr, "DB error message: %s\n", emsg);
            }

            show_info_return(1);
        }

        if (!recordset)
        {
            if (status == DRMS_ERROR_UNKNOWNSERIES)
            {
                fprintf(stderr,"### show_info: series %s not found.\n",in);
            }

            show_info_return(1);
        }
    }

    if (parsedrs)
    {
        list_llfree(&parsedrs);
    }

/* recordset now points to a struct with  count of records found ("n"), and a pointer to an
 * array of record pointers ("records");
 * it may be a chunked recordset (max_recs==0) or a limited size (max_recs!=0).
 */

    if (recordset)
    {
        if (!cursoredQ)
        {
            if (recordset->n == 0)
            {
                if (!quiet && !drms_env->print_sql_only)
                {
                    printf ("** No records in selected data set, query was %s **\n",in);
                }

                if (recordset)
                {
                    drms_close_records(recordset, DRMS_FREE_RECORD);
                    recordset = NULL;
                }

                show_info_return(0);
            }
        }
        else
        {
          /* nrecs == -1 : we don't know how many records, if any, exist. So, we can't really
           * reject any queries at this point. */
        }
    }

  /* stage records if the user has requested the path (regardless if the user has requested
   * segment information -- -A or seg=XXX).
   *
   * At this point, recordset is either a full record set, or a chunked one.  If max_recs == 0,
   * then it is a full record set, otherwise it is a chunked record set.
   */

  /* If we call drms_records_getinfo() on the recordset
   * BEFORE the first call to drms_recordset_fetchnext(), then this sets a flag in the
   * record-chunk cursor that causes drms_recordset_fetchnext() to automatically call SUM_infoEx()
   * on the chunk. The results are stored in the record's suinfo field. If we call
   * drms_stage_records() on the recordset BEFORE
   * the first call to drms_recordset_fetchnext(), then this sets a flag
   * in the record-chunk cursor that causes drms_recordset_fetchnext() to automatically
   * stage the record chunk.*/

    if (recordset)
    {
        if (requireSUMinfo)
        {
             if ((!given_sunum || given_sunum[0] < 0))
             {
                /* If the caller didn't provide a sunum list, but the caller requested items that requre SUM_info,
                 * make the getinfo call now. */
                drms_record_getinfo(recordset);
             }
        }
    }

  /* At this point, we may or may not have a container of SUM_info_t's available - if we're going to stage
   * records by first sorting on tapeid, filenumber, then we should use that array of SUM_info_t structs,
   * instead of calling SUMS yet again for those structs.
   *
   * drms_sortandstage_records() will not actually fetch SUs if the record-set was created
   * with the drms_open_recordset() call. The fetching will happen during the drms_recordset_fetchnext()
   * call [this function will call drms_sortandstage_records() on a completely new set of records -
   * one for each record in the current chunk - that will become owned by the record-set.]. In this case
   * the record-set has NULL pointers for each record in the set. We should pass the container of
   * SUM_info_t's to drms_sortandstage_records(). drms_sortandstage_records() should use this
   * container if the records in the record-set do not already have an SUM_info_t attached.
   */

    if (recordset)
    {
        /* MUST check want_path_noret (-P) before want_path (-p) because if you run with -P, then want_path gets set too */
        if (want_path_noret && retrieveLinks)
        {
             /* -P - don't retrieve but wait for SUMS to give dir info */
             drms_stage_records(recordset, 0, 0);
        }
        else if (want_path_noret && !retrieveLinks)
        {
            /* -P - don't retrieve but wait for SUMS to give dir info */
            drms_stage_records_dontretrievelinks(recordset, 0);
        }
        else if (want_path && retrieveLinks)
        {
            /* -p - retrieve and wait for retrieval */
            drms_sortandstage_records(recordset, 1, 0, &suinfo);
        }
        else if (want_path && !retrieveLinks)
        {
            /* -p - retrieve and wait for retrieval */
            drms_sortandstage_records_dontretrievelinks(recordset, 1, &suinfo);
        }
    }

  /* check for multiple sub-sets */

// NEED to add stuff to loop over subsets

    /* MAIN loop over set of selected records */
    if (recordset)
    {
        /* We need to know if there are any records to print at all. It used to be the case that this was determined in
         * the record loop. However, we need to know before we go into the loop so we can determine what kind of header
         * to print, if any at all. If we have a mixture of good and bad SUNUMs, then as we go through the record loop,
         * we might have to print a header, but not have an actual record from which to draw information.
         *
         * In other words, simplify the logic. */

        if (cursoredQ)
        {
            int count = drms_count_records(drms_env, in, &status);

            if (!status)
            {
                if (count > 0)
                {
                    PrintHeader(drms_env, seriesnameforheader, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, want_path, show_types, keysSpecified, segsSpecified, linksSpecified);
                    if (drms_env->verbose)
                    {
                        TIME(status = RecordLoopCursor(drms_env, in, recordset, bogusList, requireSUMinfo, given_sunum, suinfo, want_path, want_path_noret, seriesnameforheader, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, show_types, sunum_rs_query, keysSpecified, segsSpecified, linksSpecified, max_precision, binary));
                    }
                    else
                    {
                        status = RecordLoopCursor(drms_env, in, recordset, bogusList, requireSUMinfo, given_sunum, suinfo, want_path, want_path_noret, seriesnameforheader, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, show_types, sunum_rs_query, keysSpecified, segsSpecified, linksSpecified, max_precision, binary);
                    }
                }
            }
        }
        else
        {
            int count = recordset->n;

            if (count > 0)
            {
                PrintHeader(drms_env, seriesnameforheader, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, want_path, show_types, keysSpecified, segsSpecified, linksSpecified);
                status = RecordLoopNoCursor(drms_env, recordset, bogusList, requireSUMinfo, given_sunum, suinfo, want_path, want_path_noret, seriesnameforheader, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, show_types, sunum_rs_query, keysSpecified, segsSpecified, linksSpecified, max_precision, binary);
            }
        }
    }
    else
    {
        /* We have not loaded any DRMS records at all. There was a sunum=XX argument provided, but
         * no ds=XX argument. In this case, either function will work, because neither will actually
         * iterate through any records. */

        int count = list_llgetnitems(bogusList);

        if (count > 0)
        {
            PrintHeader(drms_env, NULL, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, want_path, show_types, NULL, NULL, NULL);
            status = RecordLoopNoCursor(drms_env, NULL, bogusList, requireSUMinfo, given_sunum, suinfo, want_path, want_path_noret, NULL, show_all, show_keys, show_all_segs, show_segs, show_all_links, quiet, keyword_list, show_recnum, show_sunum, show_recordspec, parseRS, show_online, show_retention, show_archive, show_tapeinfo, show_size, show_session, want_dims, show_types, sunum_rs_query, NULL, NULL, NULL, max_precision, binary);
        }
    }

    if (linksSpecified)
    {
        hash_free(&linksHT);
        list_llfree(&linksSpecified);
    }

    if (segsSpecified)
    {
        hash_free(&segsHT);
        list_llfree(&segsSpecified);
    }

    if (validKeysSpecified)
    {
        hash_free(&validKeysHT);
        list_llfree(&validKeysSpecified);
    }

    if (keysSpecified)
    {
        hash_free(&keysHT);
        list_llfree(&keysSpecified);
    }

    if (bogusList)
    {
        list_llfree(&bogusList);
        bogusList = NULL;
    }

  if (finalin)
  {
     free(finalin);
     finalin = NULL;
  }

  drms_close_records(recordset, DRMS_FREE_RECORD);
  fflush(stdout);
  show_info_return(status);
  }
