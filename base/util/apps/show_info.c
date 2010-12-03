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
show_info [-aAiLopPrRSTz] [-dkqt] {ds=}<record_set>|sunum=<sunum> [n=<count>] [key=<keylist>] [seg=<seglist>]
show_info_sock {same options as above}
\endcode

\details

\b Show_info shows various kinds of information about a data series.
This can be the series structure, the "jsoc series definition" for
the series, all or some of the keyword and segment values for a range of records,
the full path to the SUMS storage for the data segment, etc. 
Exactly what information gets printed is
controlled by command-line flags (see below).

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
\li \c  -x: archive - show archived flag for the storage unit. 
\li \c  -K: Select all links and display their targets for the chosen records
\li \c  -i: print record query, for each record, will be before any keywwords or segment data
\li \c  -I: print session information including host, sessionid, runtime, jsoc_version, and logdir
\li \c  -o: list the record's online status 
\li \c  -p: list the record's storage_unit path, waits for retrieval if offline
\li \c  -P: list the record\'s storage_unit path but no retrieve
\li \c  -r: recnum - show record number as first keyword
\li \c  -R: retention - show the online expire date for the segment data
\li \c  -S: SUNUM - show the sunum for the record
\li \c  -T: Tape - show the archive tape name for the record
\li \c  -v: verbose - print extra, helpful information
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

\sa
http://jsoc.stanford.edu/ajax/lookdata.html drms_query describe_series jsoc_info create_series

*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "cmdparams.h"
#include "printk.h"


ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRING, "key", "Not Specified", "<comma delimited keyword list>"},
  {ARG_STRING, "seg", "Not Specified", "<comma delimited segment list>"},
  {ARG_FLAG, "a", "0", "Show info for all keywords"},
  {ARG_FLAG, "A", "0", "Show info for all segments"},
  {ARG_FLAG, "c", "0", "Show count of records in query"},
  {ARG_FLAG, "d", "0", "Show dimensions of segment files with selected segs"},
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_FLAG, "i", "0", "print record query, for each record, will be before any keywwords or segment data"},
  {ARG_FLAG, "I", "0", "print session information for record creation, host, sessionid, runtime, jsoc_version, and logdir"},
  {ARG_FLAG, "j", "0", "list series info in jsd format"},
  {ARG_FLAG, "k", "0", "keyword list one per line"},
  {ARG_FLAG, "l", "0", "just list series keywords with descriptions"},
  {ARG_FLAG, "K", "0", "Show info for all links"},
  {ARG_INT,  "n", "0", "number of records to show, +from first, -from last"},
  {ARG_FLAG, "o", "0", "list the record\'s storage_unit online status"},
  {ARG_FLAG, "p", "0", "list the record\'s storage_unit path"},
  {ARG_FLAG, "P", "0", "list the record\'s storage_unit path but no retrieve"},
  {ARG_FLAG, "q", "0", "quiet - skip header of chosen keywords"},
  {ARG_FLAG, "r", "0", "recnum - show record number as first keyword"},
  {ARG_FLAG, "R", "0", "show the online retention date, i.e. expire date"},
  {ARG_FLAG, "s", "0", "stats - show some statistics about the series"},
  {ARG_FLAG, "S", "0", "SUNUM - show the sunum for the record"},
  {ARG_FLAG, "t", "0", "types - show types and print formats for keyword values"},
  {ARG_FLAG, "T", "0", "tapeinfo - show archive tapename and file number, or NA if not archived"},
  {ARG_FLAG, "v", NULL, "verbosity"},
  {ARG_FLAG, "x", "0", "archive - show archive status for storage unit"},
  {ARG_FLAG, "z", "0", "size - show size of storage unit containing record's segments"},
  {ARG_INTS, "sunum", "-1", "A list of comma-separated SUNUMs, find matching records"},
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
  	"  -d: Show dimensions of segment files with selected segs\n"
	"  -i: query- show the record query that matches the current record\n"
        "  -I: print session information for record creation, host, sessionid, runtime, jsoc_version, and logdir\n"
	"  -K: show information for all links\n"
	"  -o: online - tell the online state\n"
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
DRMS_RecordSet_t *drms_find_rec_first(DRMS_Record_t *rec, int wantprime)
  {
  int nprime;
  int status;
  DRMS_RecordSet_t *rs;
  char query[DRMS_MAXQUERYLEN];
  strcpy(query, rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  if (wantprime && nprime > 0) 
    // only first prime key is used for now
     // for (iprime = 0; iprime < nprime; iprime++)
      strcat(query, "[#^]");
  else
    strcat(query, "[:#^]");
  rs = drms_open_nrecords(rec->env, query, 1, &status);
  return(rs);
  }

/* find last record in series that owns the given record */
DRMS_RecordSet_t *drms_find_rec_last(DRMS_Record_t *rec, int wantprime)
  {
  int nprime;
  int status;
  DRMS_RecordSet_t *rs;
  char query[DRMS_MAXQUERYLEN];
  strcpy(query, rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  if (wantprime && nprime > 0) 
    // only first prime key is used for now
     // for (iprime = 0; iprime < nprime; iprime++)
      strcat(query, "[#$]");
  else
    strcat(query, "[:#$]");
  rs = drms_open_nrecords(rec->env, query, -1, &status);
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

// these next 2 are needed for the QUERY_STRING reading
static char x2c (char *what) {
  register char digit;

  digit = (what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit += (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0'));
  return (digit);
}

static void CGI_unescape_url (char *url) {
  register int x, y;

  for (x = 0, y = 0; url[y]; ++x, ++y) {
    if ((url[x] = url[y]) == '%') {
      url[x] = x2c (&url[y+1]);
      y += 2;
    }
  }
  url[x] = '\0';
}
// end of web enabling functions

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
      infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * nsunums);

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
               if (*(infostructs[iinfo]->online_loc) == '\0')
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

/* Module main function. */
int DoIt(void)
  {
  int need_header_printed=1;
  int status = 0;
  DRMS_RecChunking_t cstat = kRecChunking_None;
  DRMS_RecordSet_t *recordset = NULL;
  DRMS_Record_t *rec;
  int first_rec, last_rec, nrecs, irec;
// these next 3 chould be change to mallocs based on number requested.
  char *keys[1000];
  char *segs[1000];
  char *links[1000];
  int ikey, nkeys = 0;
  int iseg, nsegs, linked_segs = 0;
  int ilink, nlinks = 0;
  char *inqry;
						/* Get command line arguments */
  const char *in;
  char *keylist;
  char *seglist;
  int show_keys;
  int show_segs;
  int jsd_list;
  int list_keys;
  int show_all;
  int show_all_segs;
  int show_all_links;
  int show_recordspec;
  int show_stats;
  int show_types;
  int verbose;
  int max_recs;
  int quiet;
  int show_retention;
  int show_archive;
  int show_online;
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
  int64_t *given_sunum = NULL; /* array of 64-bit sunums provided in the'sunum=...' argument. */
  int nsunum; /* number of sunums provided in the 'sunum=...' argument. */
  int i_set, n_sets;
  int requireSUMinfo;
  char *sunum_rs_query = NULL;

  HContainer_t *suinfo = NULL;

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

  if (from_web)
    {
    char *getstring, *p;
    CGI_unescape_url(web_query);
    getstring = strdup (web_query);
    for (p=strtok(getstring,"&"); p; p=strtok(NULL, "&"))
      {
      char *key=p, *val=index(p,'=');
      if (!val)
         {
	 fprintf(stderr,"Bad QUERY_STRING: %s\n",web_query);
         return(1);
	 }
      *val++ = '\0';
      cmdparams_set(&cmdparams, key, val);
      }
    // Force JSON for now
    free(getstring);
    printf("Content-type: text/plain\n\n");
    }
  if (web_query)
    free(web_query);
  // end of web support stuff

  if (nice_intro ()) return (0);

  in = cmdparams_get_str (&cmdparams, "ds", NULL);
  keylist = strdup (cmdparams_get_str (&cmdparams, "key", NULL));
  seglist = strdup (cmdparams_get_str (&cmdparams, "seg", NULL));
  show_keys = strcmp (keylist, "Not Specified");
  show_segs = strcmp (seglist, "Not Specified");

  max_recs =  cmdparams_get_int (&cmdparams, "n", NULL);
  nsunum = cmdparams_get_int64arr(&cmdparams, "sunum", &given_sunum, &status);

  if (status != CMDPARAMS_SUCCESS)
  {
     fprintf(stderr, "Invalid argument 'sunum=%s'.\n", cmdparams_get_str(&cmdparams, "sunum", NULL));
     return 1;
  }

  show_all = cmdparams_get_int (&cmdparams, "a", NULL) != 0;
  show_all_segs = cmdparams_get_int (&cmdparams, "A", NULL) != 0;
  want_count = cmdparams_get_int (&cmdparams, "c", NULL) != 0;
  want_dims = cmdparams_get_int (&cmdparams, "d", NULL) != 0;
  show_recordspec = cmdparams_get_int (&cmdparams, "i", NULL) != 0;
  show_session = cmdparams_get_int (&cmdparams, "I", NULL) != 0;
  jsd_list = cmdparams_get_int (&cmdparams, "j", NULL) != 0;
  keyword_list =  cmdparams_get_int(&cmdparams, "k", NULL) != 0;
  list_keys = cmdparams_get_int (&cmdparams, "l", NULL) != 0;
  show_all_links = cmdparams_get_int (&cmdparams, "K", NULL) != 0;
  show_stats = cmdparams_get_int (&cmdparams, "s", NULL) != 0;
  show_online = cmdparams_get_int (&cmdparams, "o", NULL) != 0;
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
  verbose = cmdparams_isflagset(&cmdparams, "v");

  if(want_path_noret) want_path = 1;	/* also set this flag */

  requireSUMinfo = show_online || show_retention || show_archive || show_tapeinfo || show_size || show_session;

  /* At least seriesname or sunum must be specified */
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
        
        if (!lastsuinfo || strcasecmp(onesuinfo->owning_series, lastsuinfo->owning_series) != 0)
        {
           /* Got a new series (so start a new subquery). */
           if (lastsuinfo)
           {
              sunum_rs_query = base_strcatalloc(sunum_rs_query, " !],", &querylen);
           }

           snprintf(intstr, sizeof(intstr), "%s[! sunum=", onesuinfo->owning_series);
           sunum_rs_query = base_strcatalloc(sunum_rs_query, intstr, &querylen);
           lastsuinfo = onesuinfo;
           firstone = 1;
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
     }

     /* Need to end the current subquery. */
     sunum_rs_query = base_strcatalloc(sunum_rs_query, " !]", &querylen);

     if (strlen(sunum_rs_query) == 0)
     {
        printf("### show_info: given sunum=%s invalid, must quit\n", cmdparams_get_str(&cmdparams, "sunum", NULL));
        free(sunum_rs_query);
        sunum_rs_query = NULL;
        show_info_return(1);
     }

     if (sunum_rs_query)
     {
        in = sunum_rs_query;
        /* free sunum_rs_query before exiting. */
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

  /*  if -j, -l or -s is set, just do the short function and exit */
  if (list_keys || jsd_list || show_stats) 
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
       * or not any recognizable series.  Try for non-drms series before quitting (drms_open_records()
       * handles a few different types of series specifiers. */
      recordset = drms_open_records (drms_env, in, &status);
      if (!recordset) 
        {
        fprintf(stderr,"### show_info: series %s not found.\n",seriesname);
	if (seriesname)
	  free (seriesname);
        show_info_return(1);
        }
      if (recordset->n < 1)
        {
        fprintf(stderr,"### show_info: non-drms series '%s' found but is empty.\n",seriesname);
	if (seriesname)
	  free (seriesname);
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

          rs = drms_find_rec_first(rec, 1);
          if (!rs || rs->n < 1)
            printf("No records Present\n");
          else
          {
             printf("First Record: ");
             drms_print_rec_query(rs->records[0]);
             if (rs->n > 1) printf(" is first of %d records matching first keyword", rs->n);
             printf(", Recnum = %lld\n", rs->records[0]->recnum);
             drms_free_records(rs);
  
             rs = drms_find_rec_last(rec, 1);
             printf("Last Record:  ");
             drms_print_rec_query(rs->records[0]);
             if (rs->n > 1) printf(" is first of %d records matching first keyword", rs->n);
             printf(", Recnum = %lld\n", rs->records[0]->recnum);
             drms_free_records(rs);
  
             rs = drms_find_rec_last(rec, 0);
             printf("Last Recnum:  %lld", rs->records[0]->recnum);
             printf("\n");
          }
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
  inqry = index(in, '[');
  if (!inqry && max_recs == 0)
    {
    fprintf(stderr, "### show_info query must have n=recs or record query specified\n");
    show_info_return(1);
    }

  /* Open record_set(s) */
  if (max_recs == 0)
    {
       /* Set chunk size to something bigger than that of the SUM_infoEx() call. 
        * Code in drms_storageunit.c will subchunk this into the chunk size used by
        * SUM_infoEx(). */
       if (drms_recordset_setchunksize(4 * MAXSUMREQCNT) != DRMS_SUCCESS)
       {
          show_info_return(99);
       }

       recordset = drms_open_recordset (drms_env, in, &status);
    }
  else // max_recs specified via "n=" parameter.
    {
    recordset = drms_open_nrecords (drms_env, in, max_recs, &status);
    }

  if (!recordset) 
    {
    fprintf(stderr,"### show_info: series %s not found.\n",in);
    show_info_return(1);
    }

/* recordset now points to a struct with  count of records found ("n"), and a pointer to an
 * array of record pointers ("records");
 * it may be a chunked recordset (max_recs==0) or a limited size (max_recs!=0).
 */

  nrecs = recordset->n;
  if (nrecs == 0)
    {
    if (!quiet)
      printf ("** No records in selected data set, query was %s **\n",in);
    if (recordset)
    {
       drms_close_records(recordset, DRMS_FREE_RECORD);
       recordset = NULL;
    }
    show_info_return(0);
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

  if (requireSUMinfo)
  {
     if ((!given_sunum || given_sunum[0] < 0))
     {    
        /* If the caller didn't provide a sunum list, but the caller requested items that requre SUM_info, 
         * make the getinfo call now. */
        drms_record_getinfo(recordset);
     }  
  }

  if (want_path_noret)
  {
     /* -P - don't retrieve but wait for SUMS to give dir info */
     drms_stage_records(recordset, 0, 0); 
  }
  else if (want_path) 
  {
     /* -p - retrieve and wait for retrieval */
     drms_stage_records(recordset, 1, 0); 
  }

  /* check for multiple sub-sets */
  n_sets = recordset->ss_n;
  i_set = 0;
// NEED to add stuff to loop over subsets

  /* if max_recs != 0 recordset will have max_recs records (unless the record limit was
   * exceeded), and the first record will be appropriate for the value of max_recs, 
   * even if max_recs < 0 */
  last_rec = nrecs - 1;
  first_rec = 0;

  int newchunk;
  SUM_info_t **ponesuinfo = NULL;

  /* MAIN loop over set of selected records */
  for (irec = first_rec; irec <= last_rec; irec++) 
    {
    int col;

    if (max_recs == 0)
      {
         char key[128];

         rec = drms_recordset_fetchnext(drms_env, recordset, &status, &cstat, &newchunk);

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

         if (want_path && status == DRMS_REMOTESUMS_TRYLATER)
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
            show_info_return(0);
         }

         if (status < 0)
           status = 0;
      }
    else
      {
      char key[128];

      rec = recordset->records[irec];  /* pointer to current record */

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

      status = DRMS_SUCCESS;
      }

    if (need_header_printed)  /* print header line if not quiet and in table mode */
      {
      // At this point the first record is in hand but nothing has been printed yet.
      /* get list of keywords to print for each record */
      nkeys = 0;
      if (show_all) 
        { /* if wanted get list of all keywords */
        DRMS_Keyword_t *key;
        HIterator_t *last = NULL;

        while ((key = drms_record_nextkey(rec, &last, 0)))
        {
           if (!drms_keyword_getimplicit(key))
           {
              keys[nkeys++] = strdup (key->info->name);
           }
        }

        if (last)
        {
           hiter_destroy(&last);
        }

        }
      else if (show_keys)
        { /* get specified list */
        char *thiskey;
        for (thiskey=strtok(keylist, ","); thiskey; thiskey=strtok(NULL,","))
	    keys[nkeys++] = strdup(thiskey);
        }
      free (keylist);
    
      /* get list of segments to show for each record */
      // NEED to also check for {seglist} notation at end of each ss query 
      nsegs = 0;
      if (show_all_segs) 
        { /* if wanted get list of all segments */
        DRMS_Segment_t *seg;
        HIterator_t *last = NULL;

        while ((seg = drms_record_nextseg(rec, &last, 0)))
          segs[nsegs++] = strdup (seg->info->name);

        if (last)
        {
           hiter_destroy(&last);
        }
        }
      else if (show_segs) 
        { /* get specified segment list */
        char *thisseg;
        for (thisseg=strtok(seglist, ","); thisseg; thisseg=strtok(NULL,","))
	    {
	    segs[nsegs++] = strdup(thisseg);
            }
        }
      free (seglist);
      for (iseg = 0; iseg<nsegs; iseg++)
        {
        DRMS_Segment_t *seg = hcon_lookup_lower(&rec->segments, segs[iseg]);
        if (seg->info->islink)
          linked_segs++;
        }
    
      /* get list of links to print for each record */
      /* no way to choose a subset of links at this time */
      nlinks = 0;
      if (show_all_links) 
        { /* if wanted get list of all links */
        DRMS_Link_t *link;
        HIterator_t *last = NULL;
        
        while ((link = drms_record_nextlink(rec, &last)))
          links[nlinks++] = strdup (link->info->name);

        if (last)
        {
           hiter_destroy(&last);
        }

        }

      need_header_printed=0;
      if (!quiet && !keyword_list) 
        {			/* print keyword and segment name header line */
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
        for (ikey=0 ; ikey<nkeys; ikey++)
          printf ("%s%s", (col++ ? "\t" : ""), keys[ikey]); 
        for (iseg = 0; iseg<nsegs; iseg++)
          {
          printf ("%s%s", (col++ ? "\t" : ""), segs[iseg]); 
          if (want_dims)
	    printf("\t%s_info",segs[iseg]);
          }
        if (nsegs==0 && want_path)
          printf("%sSUDIR", (col++ ? "\t" : ""));
        for (ilink=0 ; ilink<nlinks; ilink++)
          printf ("%s%s", (col++ ? "\t" : ""), links[ilink]); 
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
          for (ikey=0 ; ikey<nkeys; ikey++)
            {
            DRMS_Keyword_t *rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1);
            if (rec_key_ikey)
	      printf ("%s%s", (col++ ? "\t" : ""),  drms_type_names[rec_key_ikey->info->type]);
            else
	      printf ("%s%s", (col++ ? "\t" : ""),  "TBD");
	    }
          for (iseg = 0; iseg<nsegs; iseg++)
            {
            DRMS_Segment_t *rec_seg_iseg = drms_segment_lookup (rec, segs[iseg]); 
	    printf ("%s%s", (col++ ? "\t" : ""),  drms_prot2str(rec_seg_iseg->info->protocol));
            if (want_dims)
              printf ("\tstring");
            }
          if (nsegs==0 && want_path)
            printf ("%sstring", (col++ ? "\t" : ""));
          for (ilink=0 ; ilink<nlinks; ilink++)
            {
            DRMS_Link_t *rec_link = hcon_lookup_lower(&rec->links,links[ilink]);
	    printf ("%s%s", (col++ ? "\t" : ""),  rec_link->info->type == DYNAMIC_LINK ? "dynamic" : "static");
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
          for (ikey=0 ; ikey<nkeys; ikey++)
            {
            DRMS_Keyword_t *rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1);
            if (rec_key_ikey)
              {
	      if (rec_key_ikey->info->type == DRMS_TYPE_TIME)
                printf ("%s%%s", (col++ ? "\t" : ""));
	      else
	        printf ("%s%s", (col++ ? "\t" : ""),  rec_key_ikey->info->format);
              }
            else
	      printf ("%s%s", (col++ ? "\t" : ""),  "TBD");
	    }
          for (iseg = 0; iseg<nsegs; iseg++)
            {
            printf ("%s%%s", (col++ ? "\t" : ""));
            if (want_dims)
              printf ("%s%%s", (col++ ? "\t" : ""));
            }
          if (nsegs==0 && want_path)
            printf ("%s%%s", (col++ ? "\t" : ""));
          for (ilink = 0; ilink<nlinks; ilink++)
            {
            printf ("%s%%s", (col++ ? "\t" : ""));
            }
          printf ("\n");
	  }
        }
      }

    /* now do the work for each record */
    /* record number goes first if wanted */

    col=0;
    if (keyword_list) /* if not in table mode, i.e. value per line mode then show record query for each rec */
      {
      if (irec)
        printf("\n");
      printf("# ");
      drms_print_rec_query(rec);
      printf("\n");
      }

    if (show_recnum)
      {
      if (keyword_list)
	printf("## recnum=%lld\n",rec->recnum);
      else
        printf ("%s%6lld", (col++ ? "\t" : ""), rec->recnum);
        
      }

    if (show_sunum)
      {
      if (keyword_list)
	printf("## sunum=%lld\n",rec->sunum);
      else
        printf ("%s%6lld", (col++ ? "\t" : ""), rec->sunum);
      }

    if (!keyword_list && show_recordspec)
      {
      if (col++)
        printf("\t");
      drms_print_rec_query(rec);
      }

    if (show_online)
      {
       /* rec has the suinfo struct already */
      char *msg;
      if (*rec->suinfo->online_loc == '\0')
        msg = "NA";
      else
        msg = rec->suinfo->online_status;
      if (keyword_list)
        printf("## online=%s\n", msg);
      else
        printf("%s%s", (col++ ? "\t" : ""), msg);
      }

    if (show_retention)
      {
       /* rec has the suinfo struct already */
      char retain[20];
      if (*rec->suinfo->online_loc == '\0')
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
      if (keyword_list)
        printf("## retain=%s\n", retain);
      else
        printf("%s%s", (col++ ? "\t" : ""), retain);
      }

    if (show_archive)
      {
      /* rec has the suinfo struct already */
      char *msg;
      if (*rec->suinfo->online_loc == '\0')
        msg = "NA";
      else
        {
        if(rec->suinfo->pa_status == DAAP && rec->suinfo->pa_substatus == DAADP)
          msg = "Pending";
        else
          msg = rec->suinfo->archive_status;
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
      if (*rec->suinfo->arch_tape == '\0')
        {
        msg = "NA";
        fn = -9999;
        }
      else
        {
        msg = rec->suinfo->arch_tape;
        fn = rec->suinfo->arch_tape_fn;
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
      if (*rec->suinfo->online_loc == '\0')
        strcpy(size, "NA");
      else
        sprintf(size, "%.0f", rec->suinfo->bytes);
      if (keyword_list)
        printf("## size=%s\n", size);
      else
        printf("%s%s", (col++ ? "\t" : ""), size);
      }

    if (show_session)
      {  // show host, runtime, jsoc_version, and logdir
      char *runhost, *sessionid, *runtime, *jsoc_vers, *logdir;
      if (get_session_info(rec, &runhost, &runtime, &jsoc_vers, &logdir))
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

    /* now print keyword information */
    for (ikey=0; ikey<nkeys; ikey++) 
      {
      DRMS_Keyword_t *rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1); 
      if (rec_key_ikey)
	{
	if (keyword_list)
	  {
	  printf("%s=", keys[ikey]);
	  if (rec_key_ikey->info->type != DRMS_TYPE_STRING)
	    drms_keyword_printval (rec_key_ikey);
	  else
	    {
	    printf("\"");
	    drms_keyword_printval (rec_key_ikey);
// change here for full precision XXXXXX
	    printf("\"");
	    }
          printf("\n");
	  }
	else
          {
          if (col++)
	    printf ("\t");
	  drms_keyword_printval (rec_key_ikey);
// change here for full precision XXXXXX
          }
	}
      else
        if (!keyword_list)
	  printf ("%sInvalidKeyname", (col++ ? "\t" : ""));
      }

    /* now show desired segments */
    for (iseg=0; iseg<nsegs; iseg++) 
      {
      DRMS_Segment_t *rec_seg_iseg = drms_segment_lookup (rec, segs[iseg]); 
      if (rec_seg_iseg)
        {
        char fname[DRMS_MAXPATHLEN] = {0};
        char path[DRMS_MAXPATHLEN] = {0};

        if (rec_seg_iseg->info->protocol != DRMS_DSDS && rec_seg_iseg->info->protocol != DRMS_LOCAL)
           {
           if (want_path)
             {
             int stat;
             // use segs rec to get linked record's path
             if(want_path_noret) stat=drms_record_directory (rec_seg_iseg->record, path, 0);
             else stat=drms_record_directory (rec_seg_iseg->record, path, 1);
             if (stat) strcpy(path,"**_NO_sudir_**");
             }
           else
             strcpy(path,"");

           strncpy(fname, rec_seg_iseg->filename, DRMS_MAXPATHLEN);
           }
        else
           {
           char *tmp = strdup(rec_seg_iseg->filename);
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
           printf("%s=", segs[iseg]);
        else
          if (col++)
             printf("\t");
        printf("%s%s%s", path, (want_path ? "/" : ""), fname);
        if (keyword_list)
          printf("\n");

        if (want_dims)
	  {
          int iaxis, naxis = rec_seg_iseg->info->naxis;
          if (keyword_list)
            printf("%s_info=",segs[iseg]);
          else
            printf("\t");
          if (rec_seg_iseg->info->islink)
            sprintf("\"link to %s",rec_seg_iseg->info->linkname);
          else
            {
            printf("\"%s, %s, ",  rec_seg_iseg->info->unit, drms_prot2str(rec_seg_iseg->info->protocol));
            for (iaxis=0; iaxis<naxis; iaxis++)
              {
              if (iaxis)
                printf("x");
              printf("%d",rec_seg_iseg->axis[iaxis]);
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
        DRMS_Segment_t *segment = hcon_lookup_lower(&rec->segments, segs[iseg]);
        if (segment && segment->info->islink)
            nosegmsg = "BadSegLink";
        if (!keyword_list)
	  printf ("%s%s", (col++ ? "\t" : ""), nosegmsg);
        else
          printf("%s=%s\n", segs[iseg], nosegmsg);
        }
      }
    if (nsegs==0 && want_path)
      {
      char path[DRMS_MAXPATHLEN] = {0};

      if (drms_record_numsegments(rec) <= 0)
         {
         snprintf(path, 
                sizeof(path), 
                "Record does not contain any segments - no record directory.");
         }
#ifdef REVEALBUG
      else if (nsegs == linked_segs)
         {
         printf("All segments are links - no record directory.");
         }
#endif
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
         int stat;
         if(want_path_noret)
           stat=drms_record_directory (rec, path, 0);
         else
           stat=drms_record_directory (rec, path, 1);
         if (stat)
           strcpy(path,"**_NO_sudir_**");
         }

         if (keyword_list)
           printf("SUDIR=");
         else
           if (col++)
              printf("\t");
         printf("%s", path);
         if (keyword_list)
           printf("\n");
      }
    /* now print link information */
    for (ilink=0; ilink<nlinks; ilink++) 
      {
      DRMS_Link_t *rec_link = hcon_lookup_lower(&rec->links,links[ilink]);
      DRMS_Record_t *linked_rec =  drms_link_follow(rec, links[ilink], &status);
      if (linked_rec)
	{
	if (keyword_list)
	  {
	  printf("%s=", links[ilink]);
	  if (rec_link->info->type == DYNAMIC_LINK)
            {
	    printf("\"");
            drms_print_rec_query(linked_rec);
	    printf("\"");
            }
	  else
	    {
	    printf("\"");
            printf("%s[:#%lld]",linked_rec->seriesinfo->seriesname, linked_rec->recnum);
	    printf("\"");
	    }
          printf("\n");
	  }
	else
          {
          if (col++)
	    printf ("\t");
	  if (rec_link->info->type == DYNAMIC_LINK)
            drms_print_rec_query(linked_rec);
          else
            printf("%s[:#%lld]",linked_rec->seriesinfo->seriesname, linked_rec->recnum);
          }
	}
      else
        if (!keyword_list)
	  printf ("%sInvalidLink", (col++ ? "\t" : ""));
      }
    if (!keyword_list && !col)
      {
      printf("%d records found, no other information requested\n", nrecs);
      break;
      }
    if (!keyword_list && (show_recnum || show_sunum || show_recordspec || show_online || show_session ||
		show_retention || show_archive || show_tapeinfo || show_size || nkeys || nsegs || nlinks || want_path))
      printf ("\n");
    } /* rec loop */

  /* Finished.  Clean up and exit. */
  for (ikey=0; ikey<nkeys; ikey++) 
    free(keys[ikey]);
  drms_close_records(recordset, DRMS_FREE_RECORD);
  fflush(stdout);
  show_info_return(status);
  }
