/*
 *  show_keys - prints keyword information and/or file path for given recordset
 *
 *  Bugs:
 *	Fails (with a segmentation fault) if there are no records in the
 *	  requested series
 *	Fails (with a segmentation fault) if called with -p flag or with a
 *	  value for file and there are no data segments associated with the
 *	  requested series
 */

/* removed from doxygen module documentation - superceded by show_info */

/**
@defgroup show_keys show_keys - Examine a dataseries structure or contents

Deprecated -- Prints keyword information and/or file path for given recordset.

Use \ref show_info instead of show_keys. Show_keys is no longer maintained.
The functionality of show_keys is now a subset of show_info.

Show_keys can list the keyword names and values, and the segment
names and file names (full paths) for each record in a record set. It
can also list the full path to the record direcory in SUMS, which
contains the segment files. Exactly what information gets printed is
controlled by command-line flags (see below). The \a -k flag controls
the format of the output.  If it is set, then the output is in table
format, with a header row showing the keyword names.  Otherwise,
keyword name=value pairs are listed one per line.  If the \a -a flag
is set, \ref show_keys lists the names of all series keywords, prime
keywords, and segments, and exits.  Otherwise, it prints keyword and
segment information as specified by the other flags and arguments.  If
the \a -p flag is set and \a seglist is specified, then the full paths
for the segment files will be displayed. If the \a -p flag is set, but
\a seglist is not specified, then only the full path to the record's
storage unit will be displayed.

The number of records for which information will be printed must be
specified, either by supplying a \a record_set string that selects a
subset of records from a series, or by supplying the \a n=nrecords
argument, which indicates the number of records.

\par Synopsis:

\code
show_keys [-aklpqrDRIVER_FLAGS] ds=<record_set> [n=<nrecords>] [key=<keylist>] [seg=<seglist>]
\endcode

\b Example:
To show the storage-unit paths for a maximum of 10
records:
\code
  show_keys -p ds=su_arta.TestStoreFile n=10
\endcode

\b Example:
To show information, in non-table format, for all keywords,
plus the segment named file_seg, for a maximum of 10 records:
\code
  show_keys ds=su_arta.TestStoreFile -akr n=10 seg=file_seg
\endcode

\par Flags:
\c -a: Show all keyword names and values for each  record  specified
by \a record_set  or \a nrecords.  \a -a takes precedence over \a
keylist.  
\par
\c -k: List keyword name=value pairs, one per line. Otherwise print
all keyword values on a single line and print a header line containing
the keyword names (table format).
\par
\c -l: List the names of all series keywords, prime keywords,  and
segments, and exit. Otherwise, print keyword and segment information
as specified by the other flags and arguments. 
\par
\c -p: Include in the output the full storage-unit path for each record
\par
\c -q: Quiet - omit the header line listing keyword names if the -k
flag is set
\par
\c -r:  Include in the output the record number keyword

\par Driver flags: 
\ref jsoc_main

\param record_set
A series name followed by an optional record-set specification (i.e.,
\a seriesname[RecordSet_filter]). Causes selection of a subset of
records in the series. This argument is required, and if no record-set
filter is specified, then \a n=nrecords must be present.

\param nrecords
\a nrecords specifies the maximum number of records for which
information is printed.  If \a nrecords < 0, \ref show_keys displays
information for the last \a nrecords records in the record set. If
\a nrecords > 0, \ref show_keys displays information for the first
\a nrecords records in the record set. If \a record_set contains a
record set filter, then \a nrecords can reduce the total number of
records for which information is displayed.

\param keylist
Comma-separated list of keyword names. For each keyword listed,
information will be displayed.  \a keylist is ignored in the case that
the \a -a flag is set.

\param seglist
Comma-separated list of segment names. For each segment listed, the
full path to the segment's file is displayed
(if the \a -p flag is set) or the file name of the
segment's file name is displayed (if the \a -p flag is unset).

\bug
Deprecated in favor of show_info

\sa
retrieve_file drms_query describe_series

*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRING, "key", "Not Specified", "<comma delimited keyword list>"},
  {ARG_STRING, "seg", "Not Specified", "<comma delimited segment list>"},
  {ARG_FLAG, "a", "0", "Show info for all keywords"},
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_FLAG, "l", "0", "just list series keywords with descriptions"},
  {ARG_INT, "n", "0", "number of records to show, +from first, -from last"},
  {ARG_FLAG, "p", "0", "list the record\'s storage_unit path"},
  {ARG_FLAG, "P", "0", "list the record\'s storage_unit path but no retrieve"},
  {ARG_FLAG, "k", "0", "keyword list one per line"},
  {ARG_FLAG, "q", "0", "quiet - skip header of chosen keywords"},
  {ARG_FLAG, "r", "0", "recnum - show record number as first keyword"},
  {ARG_END}
};

char *module_name = "show_keys";
int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\nshow_keys [-ahklpqr] "
	"ds=<recordset query> {n=0} {key=<keylist>} {seg=<segment_list>}\n"
        "  details are:\n"
	"  -a: show information for all keywords\n"
	"  -h: help - show this message then exit\n"
	"  -k: list keyword names and values, one per line\n"
	"  -l: list all keywords with description, then exit\n"
	"  -p: list the record's storage_unit path (retrieve if necessary)\n"
	"  -P: list the record's storage_unit path (no retrieve)\n"
	"  -q: quiet - skip header of chosen keywords\n"
	"  -r: recnum - show record number as first keyword\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"n=0 number of records in query to show, +n from start or -n from end\n"
	"key=<comma delimited keyword list>, for all use -a flag\n"
	"seg=<comma delimited segment list>\n"
	"The -p or -P flag will show the record directory by itself or as part of the\n"
	"full path to the segment file if seg=<segmentname> is specified.\n"
	"Note that the -p flag will cause the data to be staged if offline.\n");
    return(1);
    }
  return (0);
  }

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
  char *in = cmdparams_get_str (&cmdparams, "ds", NULL);
  char *keylist = strdup (cmdparams_get_str (&cmdparams, "key", NULL));
  char *seglist = strdup (cmdparams_get_str (&cmdparams, "seg", NULL));
  int show_keys = strcmp (keylist, "Not Specified");
  int show_segs = strcmp (seglist, "Not Specified");
  int list_keys = cmdparams_get_int (&cmdparams, "l", NULL) != 0;
  int show_all = cmdparams_get_int (&cmdparams, "a", NULL) != 0;
  int max_recs =  cmdparams_get_int (&cmdparams, "n", NULL);
  int quiet = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  int show_recnum =  cmdparams_get_int(&cmdparams, "r", NULL) != 0;
  int keyword_list =  cmdparams_get_int(&cmdparams, "k", NULL) != 0;
  int want_path = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
  int want_path_noret = cmdparams_get_int (&cmdparams, "P", NULL) != 0;

  if (nice_intro ()) return (0);
		  /*  if -l is set, just get all the keywords and print them  */
  if(want_path_noret) want_path = 1;	/* also set this flag */
  if (strcmp(in, "Not Specified") == 0)
    {
    printf("### show_keys: ds=<record_query> parameter is required, but I will look for a query without the ds=\n");
    if (cmdparams_numargs(&cmdparams) >= 1 && (in = cmdparams_getarg (&cmdparams, 1)))
      {
      printf("### found \"%s\", using it for the record_query.\n",in);
      }
    else 
      {
      printf("### show_keys: Oops, still no query found, quit\n");
      return(1);
      }
    }
  if (list_keys) 
    {
    char *p, *seriesname;
    DRMS_Record_t *rec;
    DRMS_Keyword_t *key, **prime_keys;
    DRMS_Segment_t *seg;
    HIterator_t hit;
    int nprime, iprime;

    /* Only want keyword info so do not need to open any records, just get the template */
    seriesname = strdup (in);
    if ((p = index(seriesname,'['))) *p = '\0';
    rec = drms_template_record (drms_env, seriesname, &status);
    if (status)
      {
      fprintf(stderr,"### show_keys: series %s not found.\n",seriesname);
      return(1);
      }

    /* show the prime index keywords */
    nprime = rec->seriesinfo->pidx_num;
    prime_keys = rec->seriesinfo->pidx_keywords;
    if (nprime > 0) 
      {
      printf("Prime Keys are:\n");
      for (iprime = 0; iprime < nprime; iprime++)
        printf("\t%s\n", prime_keys[iprime]->info->name);
      }

    /* show all keywords */
    printf("All Keywords for series %s:\n",seriesname);
    hiter_new (&hit, &rec->keywords);
    while ((key = (DRMS_Keyword_t *)hiter_getnext (&hit)))
      printf ("\t%-10s\t%s (%s)\n", key->info->name, key->info->description,
          drms_type_names[key->info->type]);

    /* show the segments */
    if (rec->segments.num_total)
      {
      printf("Segments for series %s:\n",seriesname);
      hiter_new (&hit, &rec->segments);
      while ((seg = (DRMS_Segment_t *)hiter_getnext (&hit)))
          printf ("\t%-10s\t%s\n", seg->info->name, seg->info->description);
      }

    if (seriesname)
    {
       free (seriesname);
    }

    return (0);
    }

  /* check for poor usage of no query and no n=record_count */
  inqry = index(in, '[');
  if (!inqry && !max_recs)
    {
    fprintf(stderr, "### show_keys query must have n=recs or record query specified\n");
    return(1);
    }

  /* Open record_set */
  recordset = drms_open_records (drms_env, in, &status);
  if (!recordset) 
    {
    fprintf(stderr,"### show_keys: series %s not found.\n",in);
    return (1);
    }

/* records now points to a struct with  count of records found ("n"), and a pointer to an
 * array of record pointers ("records");
 */

  nrecs = recordset->n;
  if (nrecs == 0)
    {
    if (!quiet)
      printf ("** No records in selected data set, query was %s **\n",in);
    return (0);
    }
  if (max_recs > 0 && max_recs < nrecs)
    nrecs = max_recs;
  last_rec = nrecs - 1;
  if (max_recs < 0 && nrecs+max_recs > 0)
    first_rec = nrecs + max_recs;
  else
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
  nsegs = 0;
  if (show_segs) 
    { /* get specified segment list */
    char *thisseg;
    for (thisseg=strtok(seglist, ","); thisseg; thisseg=strtok(NULL,","))
	segs[nsegs++] = strdup(thisseg);
    }
  free (seglist);

  /* loop over set of selected records */
  for (irec = first_rec; irec <= last_rec; irec++) 
    {
    rec = recordset->records[irec];  /* pointer to current record */
    if (firstrec)  /* print header line if not quiet and in table mode */
      {
      firstrec=0;
      if (!quiet && !keyword_list) 
        {			/* print keyword and segment name header line */
        if (show_recnum)
          printf ("recnum\t");
        for (ikey=0 ; ikey<nkeys; ikey++)
          printf ("%s\t",keys[ikey]); 
        for (iseg = 0; iseg<nsegs; iseg++)
          printf ("%s\t",segs[iseg]); 
        if (nsegs==0 && want_path)
          printf("SUDIR");
        printf ("\n");
        }
      }

    if (keyword_list) /* if not in table mode, i.e. value per line mode then show record query for each rec */
      {
      printf("# ");
      drms_print_rec_query(rec);
      printf("\n");
      }

    /* now do the work for each record */
    /* record number goes first if wanted */

    if (show_recnum)
      {
      if (keyword_list)
	printf("recnum=%lld\n",rec->recnum);
      else
        printf ("%6lld\t", rec->recnum);
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
	printf ("MISSING");
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
        char fname[DRMS_MAXPATHLEN];
        char path[DRMS_MAXPATHLEN];
        if (want_path)
          if(want_path_noret) drms_record_directory (rec, path, 0);
          else drms_record_directory (rec, path, 1);
        else
          strcpy(path,"");
	strncpy(fname, rec_seg_iseg->filename, DRMS_MAXPATHLEN);
        if (keyword_list)
          printf("%s=", segs[iseg]);
        printf("%s%s%s", path, (want_path ? "/" : ""), fname);
        }
      else if (!keyword_list)
	printf ("NO_FILENAME");
      }
    if (nsegs==0 && want_path)
      {
      char path[DRMS_MAXPATHLEN];
      if(want_path_noret) drms_record_directory (rec, path, 0);
      else drms_record_directory (rec, path, 1);
      if (keyword_list)
        printf("SUDIR=");
      printf("%s", path);
      }
    if (show_recnum || nkeys || nsegs || want_path)
      printf ("\n");
    }

/* Finished.  Clean up and exit. */
  for (ikey=0; ikey<nkeys; ikey++) 
    free(keys[ikey]);
  drms_close_records(recordset, DRMS_FREE_RECORD);
  return status;
  }
