/*
 *  show_info - prints keyword information and/or file path for given recordset
 *
 *  new version of original show_keys expanded to have more than just keyword info.
 *
 *  Bugs:
 *	Fails (with a segmentation fault) if called with -p flag or with a
 *	  value for file and there are no data segments associated with the
 *	  requested series
 */

/**
\defgroup show_info show_info
@ingroup drms_util

Prints keyword, segment, and other information and/or file path for given recordset.

\par Synopsis:

\code
show_info [-ajklpPqrs] {ds=}<record_set>|sunum=<sunum> [n=<nrecords>] [key=<keylist>] [seg=<seglist>]
\endcode

\par Description:

\ref show_info shows various kinds of information about a data series.
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

\par Flags controling operation to perform:
This group of arguments controls the action of show_info.  The default action is to query for the specified record-set
and perform the requested display of information.
\par
\c  -c: Show count of records in query and exit.  This flag requires a record set query to be specified.
\par
\c  -h: help - print usage info and exit
\par
\c  -j: list series info in jsd format and exit
\par
\c  -l: just list series keyword, segment, and link names with descriptions then exit
\par
\c  -s: stats - show some statistics about the series, presently only first and last record.
\par
\c  QUERY_STRING=<cgi-bin GET format command line args> - string with default "Not Specified" used when show_info is invoked
from http://jsoc.stanford.edu/cgi_bin/ajax/show_info

\par Parameters and Flags controling record subset to examine:
This group of arguments specifies a recordset to examine.  If the "sunum" argument is present it overrides an
explicit recordset specification and returns the record with that sunum as its record directory in SUMS.
For normal recordset queries the "ds=" is optional.  In no "where clauses" (i.e. "[xxx]" clauses) are
present in the recordset query the "n" parameter is required.  Since the "where clauses" are used to
restrict the number or records retrieved, an empty clause (e.g. "[]") will match all records in the series.
\par
\c  ds=<record_set query> - string with default "Not Specified", see below for more information.
\par
\c  n=<count> - Max number of records to show, +from first, -from last, see below.
\par
\c  sunum=<sunum> - integer with default -1
\par

\par Parameters and Flags controling selction of keywords and segments to examine:
This group of arguments specifies the set of keywords, segments, links, or virtual keywords to display.
(link display not yet implemented/tested).

\par
\c  key=<keylist> - string with default "Not Specified", see below.
\par
\c  seg=<seglist> - string with dedfauly "Not Specified", see below.
\par
\c  -a: Select all keywords and display their values for the chosen records
\par
\c  -A: Select all segments and display their values for the chosen records
\par
\c  -i: print record query, for each record, will be before any keywwords or segment data
\par
\c  -p: list the record's storage_unit path, waits for retrieval if offline
\par
\c  -P: list the record\'s storage_unit path but no retrieve
\par
\c  -r: recnum - show record number as first keyword
\par
\c  -S: SUNUM - show the sunum for the record
\par Flags controling how to display values:
\par
\c  -d: Show dimensions of segment files with selected segs
\par
\c  -k: keyword list one per line
\par
\c  -q: quiet - skip header of chosen keywords
\par
\c  -t: types - show types and print formats for keyword values
\par
\par Driver flags: 
\ref jsoc_main

\param record_set
A record_set list is a comma separated list of record_set queries.
Each query is a series name followed by an optional record-set specification (i.e.,
\a seriesname[RecordSet_filter]). Causes selection of a subset of
records in the series. This argument is required, and if no record-set
filter is specified, then \a n=nrecords must be present.
The "ds=" protion of the record_set argument is optional.
<I>NOTE to the csh user, you need to excape the '[' character and
some other characters used in the record set specification.</I>

\param count
\a n=<count> specifies the maximum number of records for which
information is printed.  If \a count < 0, \ref show_info displays
information for the last \a count records in the record set. If
\a count > 0, \ref show_info displays information for the first
\a count records in the record set. If \a record_set contains a
record set filter, then \a count applies to the set of records
matching the filter.

\param keylist
Comma-separated list of keyword names. For each keyword listed,
information will be displayed.  \a keylist is ignored in the case that
the \a -a flag is set.

\param seglist
Comma-separated list of segment names.  \a seglist is ignored
in the case that the \a -A flag is set.  For each segment listed, the
segment's file is displayed.  If the \a -p flag is set the filename will
be prefaced by the full path to the file in SUMS.  If the storage unit
containing the record directory is offline, it will be staged first and
the new online location is reported.  If the \a -P flag is given the path
is displayed only if the data is online.  If offline with a \a -P flag then
only the filename is shown.  If the \a -d flag is set then the dimensions
of the segment array are displayed along with the path/filename.

\b Example:
To show the storage-unit paths for a maximum of 10
records:
\code
  show_info -p ds=su_arta.TestStoreFile n=10
\endcode

\b Example:
To show information, in non-table format, for all keywords,
plus the segment named file_seg, for a maximum of 10 records:
\code
  show_info ds=su_arta.TestStoreFile -akr n=10 seg=file_seg
\endcode

\bug
The program will produce superflous and non-meaningful output if
called with the \a -p flag and \a seglist is provided on the command line.

\sa
retrieve_file drms_query describe_series

@{
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "cmdparams.h"

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
  {ARG_FLAG, "j", "0", "list series info in jsd format"},
  {ARG_FLAG, "k", "0", "keyword list one per line"},
  {ARG_FLAG, "l", "0", "just list series keywords with descriptions"},
  {ARG_INT,  "n", "0", "number of records to show, +from first, -from last"},
  {ARG_FLAG, "p", "0", "list the record\'s storage_unit path"},
  {ARG_FLAG, "P", "0", "list the record\'s storage_unit path but no retrieve"},
  {ARG_FLAG, "q", "0", "quiet - skip header of chosen keywords"},
  {ARG_FLAG, "r", "0", "recnum - show record number as first keyword"},
  {ARG_FLAG, "s", "0", "stats - show some statistics about the series"},
  {ARG_FLAG, "S", "0", "SUNUM - show the sunum for the record"},
  {ARG_FLAG, "t", "0", "types - show types and print formats for keyword values"},
  {ARG_INT, "sunum", "-1", "SUNUM specified, find matching records"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "show_info called as cgi-bin program args here"},
  {ARG_END}
};

char *module_name = "show_info";
/** @}*/
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
	"  -p: list the record's storage_unit path (retrieve if necessary)\n"
	"  -P: list the record's storage_unit path (no retrieve)\n"
	"  -r: recnum - show record number as first keyword\n"
	"  -S: sunum - show sunum number as first keyword (but after recnum)\n"
        " output appearance control flags are:\n"
	"  -k: list keyword names and values, one per line\n"
	"  -t: list keyword types and print formats as 2nd and 3rd lines in table mode\n"
	"  -q: quiet - skip header of chosen keywords\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"n=0 number of records in query to show, +n from start or -n from end\n"
	"  Note:  this does not reduce the number of records fetch, onlt the number shown.\n"
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
  int iprime, nprime;
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
// fprintf(stderr,"test 1 query is %s\n",query);
  rs = drms_open_records(rec->env, query, &status);
// fprintf(stderr,"test 1 status is %d\n",status);
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
  rs = drms_open_records(rec->env, query, &status);
  return(rs);
  }


static void list_series_info(DRMS_Record_t *rec)
  {
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;
  DRMS_Link_t *link;
  HIterator_t hit;
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
  hiter_new (&hit, &rec->keywords);
  while ((key = (DRMS_Keyword_t *)hiter_getnext (&hit)))
    {
    printf ("\t%-10s", key->info->name);
    if (key->info->islink)
        {
        printf("\tlink through %s",seg->info->linkname);
        }
    else
        {
        printf ("\t(%s)", drms_type_names[key->info->type]);
        }
    printf ("\t%s\n", key->info->description);
    }
  
  /* show the segments */
  if (rec->segments.num_total)
    {
    printf("Segments for series %s:\n",rec->seriesinfo->seriesname);
    hiter_new (&hit, &rec->segments);
    while ((seg = (DRMS_Segment_t *)hiter_getnext (&hit)))
        { /* segment name, units, protocol, dims, description */
	char prot[DRMS_MAXNAMELEN];
	int iaxis, naxis = seg->info->naxis;
	strcpy(prot, drms_prot2str(seg->info->protocol));
	if (seg->info->islink)
	    {
	    printf("\tlink through %s",seg->info->linkname);
	    }
	else
	    {
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
    hiter_new (&hit, &rec->links);
    while ((link = (DRMS_Link_t *)hiter_getnext (&hit)))
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
  return;
  }

int drms_count_records(DRMS_Env_t *env, char *recordsetname, int *status)
  {
  int stat, filter, mixed;
  char *query=NULL, *where=NULL, *seriesname=NULL;
  int count = 0;
  DB_Text_Result_t *tres;
  int allvers = 0;

  stat = drms_recordset_query(env, recordsetname, &where, &seriesname, &filter, &mixed, &allvers);
      if (stat)
        goto failure;

  stat = 1;
  query = drms_query_string(env, seriesname, where, filter, mixed, DRMS_QUERY_COUNT, NULL, NULL, allvers);
      if (!query)
        goto failure;

  tres = drms_query_txt(env->session,  query);

  if (tres && tres->num_rows == 1 && tres->num_cols == 1)
    count = atoi(tres->field[0][0]);
  else
    goto failure;

  free(seriesname);
  free(query);
  free(where);
  *status = DRMS_SUCCESS;
  return(count);

  failure:
  if (seriesname) free(seriesname);
  if (query) free(query);
  if (where) free(where);
  *status = stat;
  return(0);
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

/* Module main function. */
int DoIt(void)
  {
  int firstrec=1;
  int status = 0;
  DRMS_RecordSet_t *recordset;
  DRMS_Record_t *rec;
  int first_rec, last_rec, nrecs, irec;
  char *keys[1000];
  char *segs[1000];
  int ikey, nkeys = 0;
  int iseg, nsegs = 0;
  char *inqry;
						/* Get command line arguments */
  char *in;
  char *keylist;
  char *seglist;
  int show_keys;
  int show_segs;
  int jsd_list;
  int list_keys;
  int show_all;
  int show_all_segs;
  int show_recordspec;
  int show_stats;
  int show_types;
  int max_recs;
  int quiet;
  int show_recnum;
  int show_sunum;
  int keyword_list;
  int want_count;
  int want_path;
  int want_path_noret;
  int want_dims;
  int i_set, n_sets;
  long long given_sunum;

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
  show_recordspec = cmdparams_get_int (&cmdparams, "i", NULL) != 0;
  jsd_list = cmdparams_get_int (&cmdparams, "j", NULL) != 0;
  list_keys = cmdparams_get_int (&cmdparams, "l", NULL) != 0;
  show_all = cmdparams_get_int (&cmdparams, "a", NULL) != 0;
  show_all_segs = cmdparams_get_int (&cmdparams, "A", NULL) != 0;
  want_count = cmdparams_get_int (&cmdparams, "c", NULL) != 0;
  want_dims = cmdparams_get_int (&cmdparams, "d", NULL) != 0;
  show_stats = cmdparams_get_int (&cmdparams, "s", NULL) != 0;
  max_recs =  cmdparams_get_int (&cmdparams, "n", NULL);
  quiet = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  show_recnum =  cmdparams_get_int(&cmdparams, "r", NULL) != 0;
  show_sunum =  cmdparams_get_int(&cmdparams, "S", NULL) != 0;
  show_types =  cmdparams_get_int(&cmdparams, "t", NULL) != 0;
  keyword_list =  cmdparams_get_int(&cmdparams, "k", NULL) != 0;
  want_path = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
  want_path_noret = cmdparams_get_int (&cmdparams, "P", NULL) != 0;
  given_sunum = cmdparams_get_int64 (&cmdparams, "sunum", NULL);

  if(want_path_noret) want_path = 1;	/* also set this flag */

  /* At least seriesname or sunum must be specified */
  if (given_sunum >= 0)
    { /* use sunum to get seriesname instead of ds= or stand-along param */
    char sunum_query[1000];
    char sname[DRMS_MAXNAMELEN];
    char sunum_rs_query[DRMS_MAXQUERYLEN];
    FILE *sums;
    sprintf(sunum_query,
      "echo 'select owning_series from sum_main where ds_index=%lld' |"
      " psql -h hmidb -p 5434 jsoc_sums |"
      " head -3 |"
      " tail -1",
      given_sunum);
// fprintf(stderr,"%s\n",sunum_query);
    sums = popen(sunum_query, "r");
    fscanf(sums, "%s", sname);
    fclose(sums);
    sprintf(sunum_rs_query, "%s[? sunum=%lld ?]", sname, given_sunum);
    in = strdup(sunum_rs_query);
    }
  else if (strcmp(in, "Not Specified") == 0)
    {
    if (cmdparams_numargs(&cmdparams) < 1 || !(in=cmdparams_getarg (&cmdparams, 1)))
      {
      printf("### show_info: ds=<record_query> parameter is required, must quit\n");
      return(1);
      }
    }

  /*  if -j, -l or -s is set, just do the short function and exit */
  if (list_keys || jsd_list || show_stats) 
    {
    char *p, *seriesname;
    int is_drms_series = 1;

    /* Only want keyword info so get only the template record for drms series or first record for other data */
    seriesname = strdup (in);
    if ((p = index(seriesname,'['))) *p = '\0';
    rec = drms_template_record (drms_env, seriesname, &status);
    if (status)
      {
      /* either it is not a drms series or not any series.  Try for non-drms series before quitting */
      recordset = drms_open_records (drms_env, in, &status);
      if (!recordset) 
        {
        fprintf(stderr,"### show_info: series %s not found.\n",seriesname);
	if (seriesname)
	  free (seriesname);
        return (1);
        }
      if (recordset->n < 1)
        {
        fprintf(stderr,"### show_info: non-drms series '%s' found but is empty.\n",seriesname);
	if (seriesname)
	  free (seriesname);
        return (1);
        }
      rec = recordset->records[0];
      is_drms_series = 0;
      }

    if (seriesname)
      free (seriesname);

    if (list_keys)
      { 
      list_series_info(rec);
      return(0);
      }
    else if (jsd_list) 
      {
      drms_jsd_print(drms_env, rec->seriesinfo->seriesname);
      return(0);
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
      return(0);
      }
     else printf("### Can not use '-s' flag for non-drms series. Sorry.\n");
     }
    fflush(stdout);
    }

  /* get count if -c flag set */
  if (want_count)
    {
    int count = drms_count_records(drms_env, in, &status);
    if (status)
      {
      fprintf(stderr,"### show_info: series %s not found.\n",in);
      return (1);
      }
    printf("%d", count);
    if (!quiet)
      printf(" records match the query");
    printf("\n");
    return(0);
    }

  /* NOW in mode to list per-record information.  Get recordset */

  /* check for poor usage of no query and no n=record_count */
  inqry = index(in, '[');
  if (!inqry && !max_recs)
    {
    fprintf(stderr, "### show_info query must have n=recs or record query specified\n");
    return(1);
    }

  /* Open record_set(s) */
  if (max_recs != 0)
  {
     recordset = drms_open_nrecords (drms_env, in, max_recs, &status);
  }
  else
  {
     recordset = drms_open_records (drms_env, in, &status);
  }

  if (!recordset) 
    {
    fprintf(stderr,"### show_info: series %s not found.\n",in);
    return (1);
    }

/* recordset now points to a struct with  count of records found ("n"), and a pointer to an
 * array of record pointers ("records");
 */

  nrecs = recordset->n;
  if (nrecs == 0)
    {
    if (!quiet)
      printf ("** No records in selected data set, query was %s **\n",in);
    return (0);
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

  /* get list of keywords to print for each record */
  nkeys = 0;
  if (show_all) 
    { /* if wanted get list of all keywords */
    DRMS_Keyword_t *key;
    HIterator_t hit;
    hiter_new (&hit, &recordset->records[0]->keywords);
    while ((key = (DRMS_Keyword_t *)hiter_getnext (&hit)))
      keys[nkeys++] = strdup (key->info->name);
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
    HIterator_t hit;
    hiter_new (&hit, &recordset->records[0]->segments);
    while ((seg = (DRMS_Segment_t *)hiter_getnext (&hit)))
      segs[nsegs++] = strdup (seg->info->name);
    }
  else if (show_segs) 
    { /* get specified segment list */
    char *thisseg;
    for (thisseg=strtok(seglist, ","); thisseg; thisseg=strtok(NULL,","))
	segs[nsegs++] = strdup(thisseg);
    }
  free (seglist);

  /* stage records if the user has requested the path (regardless if the user has requested 
   * segment information -- -A or seg=XXX).
   */
  if (want_path_noret)
    /* -P - don't retrieve and don't wait for retrieval */
    drms_stage_records(recordset, 0, 1); 
  else if (want_path)
    /* -p - retrieve and wait for retrieval */
    drms_stage_records(recordset, 1, 0); 



  /* loop over set of selected records */
  for (irec = first_rec; irec <= last_rec; irec++) 
    {
    rec = recordset->records[irec];  /* pointer to current record */
    if (firstrec)  /* print header line if not quiet and in table mode */
      {
      firstrec=0;
      if (!quiet && !keyword_list) 
        {			/* print keyword and segment name header line */
        /* first print the name line */
        if (show_recnum)
          printf ("recnum\t");
        if (show_sunum)
          printf ("sunum\t");
        if (show_recordspec)
          printf ("query\t");
        for (ikey=0 ; ikey<nkeys; ikey++)
          printf ("%s\t",keys[ikey]); 
        for (iseg = 0; iseg<nsegs; iseg++)
          {
          printf ("%s\t",segs[iseg]); 
          if (want_dims)
	    printf("%s_info\t",segs[iseg]);
          }
        if (nsegs==0 && want_path)
          printf("SUDIR");
        printf ("\n");
        /* now, if desired, print the type and format lines. */
        if (show_types)
	  {
	  /* types first */
          /* ASSUME all records have same structure - might not be true for mixed queries, fix later */
          if (show_recnum)
            printf ("longlong\t");
          if (show_sunum)
            printf ("longlong\t");
          if (show_recordspec)
            printf ("string\t");
          for (ikey=0 ; ikey<nkeys; ikey++)
            {
            DRMS_Keyword_t *rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1);
	    printf ("%s\t", drms_type_names[rec_key_ikey->info->type]);
	    }
          for (iseg = 0; iseg<nsegs; iseg++)
            {
            DRMS_Segment_t *rec_seg_iseg = drms_segment_lookup (rec, segs[iseg]); 
	    printf ("%s\t", drms_prot2str(rec_seg_iseg->info->protocol));
            if (want_dims)
              printf ("string\t");
            }
          if (nsegs==0 && want_path)
            printf("string");
          printf ("\n");
	  /* now print format */
          /* ASSUME all records have same structure - might not be true for mixed queries, fix later */
          if (show_recnum)
            printf ("%%lld\t");
          if (show_sunum)
            printf ("%%lld\t");
          if (show_recordspec)
            printf ("%%s\t");
          for (ikey=0 ; ikey<nkeys; ikey++)
            {
            DRMS_Keyword_t *rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1);
	    if (rec_key_ikey->info->type == DRMS_TYPE_TIME)
	      printf("%%s\t");
	    else
	      printf ("%s\t", rec_key_ikey->info->format);
	    }
          for (iseg = 0; iseg<nsegs; iseg++)
            {
	    printf ("%%s\t");
            if (want_dims)
              printf ("%%s\t");
            }
          if (nsegs==0 && want_path)
            printf("%%s");
          printf ("\n");
	  }
        }
      }

    /* now do the work for each record */
    /* record number goes first if wanted */

    if (keyword_list) /* if not in table mode, i.e. value per line mode then show record query for each rec */
      {
      printf("# ");
      drms_print_rec_query(rec);
      printf("\n");
      }

    if (show_recnum)
      {
      if (keyword_list)
	printf("recnum=%lld\n",rec->recnum);
      else
        printf ("%6lld\t", rec->recnum);
      }

    if (show_sunum)
      {
      if (keyword_list)
	printf("sunum=%lld\n",rec->sunum);
      else
        printf ("%6lld\t", rec->sunum);
      }

    if (!keyword_list && show_recordspec)
      {
      drms_print_rec_query(rec);
      printf("\t");
      }

    /* now print keyword information */
    for (ikey=0; ikey<nkeys; ikey++) 
      {
      DRMS_Keyword_t *rec_key_ikey = drms_keyword_lookup (rec, keys[ikey], 1); 
      if (ikey)
	printf (keyword_list ? "\n" : "\t");
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
	    printf("\"");
	    }
	  }
	else
	  drms_keyword_printval (rec_key_ikey);
	}
      else if (!keyword_list)
	printf ("InvalidKeyname");
      }
    if(nkeys)
      printf (keyword_list ? "\n" : "\t");

    /* now show desired segments */
    for (iseg=0; iseg<nsegs; iseg++) 
      {
      DRMS_Segment_t *rec_seg_iseg = drms_segment_lookup (rec, segs[iseg]); 
      if(iseg)
        printf (keyword_list ? "\n" : "\t");
      if (rec_seg_iseg)
        {
        char fname[DRMS_MAXPATHLEN] = {0};
        char path[DRMS_MAXPATHLEN] = {0};

        if (rec_seg_iseg->info->protocol != DRMS_DSDS && rec_seg_iseg->info->protocol != DRMS_LOCAL)
        {
           if (want_path)
             {
             int stat;
             if(want_path_noret) stat=drms_record_directory (rec, path, 0);
             else stat=drms_record_directory (rec, path, 1);
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
        printf("%s%s%s", path, (want_path ? "/" : ""), fname);

        if (want_dims)
	  {
          int iaxis, naxis = rec_seg_iseg->info->naxis;
          if (keyword_list)
            printf("\n%s_info=",segs[iseg]);
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
          }
        }
      else if (!keyword_list)
	printf ("InvalidSegName");
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
            if(want_path_noret) stat=drms_record_directory (rec, path, 0);
            else stat=drms_record_directory (rec, path, 1);
            if (stat) strcpy(path,"**_NO_sudir_**");
         }

         if (keyword_list)
           printf("SUDIR=");
         printf("%s", path);
      }
    if (show_recnum || show_sunum || show_recordspec || nkeys || nsegs || want_path)
      printf ("\n");
    }

  /* Finished.  Clean up and exit. */
  for (ikey=0; ikey<nkeys; ikey++) 
    free(keys[ikey]);
  drms_close_records(recordset, DRMS_FREE_RECORD);
  fflush(stdout);
  return status;
  }

