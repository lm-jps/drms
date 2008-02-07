/*
 *  show_info - prints keyword information and/or file path for given recordset
 *
 *  new version of original show_keys expanded to have more than just keyword info.
 *
 *  Bugs:
 *	Fails (with a segmentation fault) if there are no records in the
 *	  requested series
 *	Fails (with a segmentation fault) if called with -p flag or with a
 *	  value for file and there are no data segments associated with the
 *	  requested series
 */

/**
\defgroup show_info show_info

Prints keyword, segment, and other information and/or file path for given recordset.

\ref show_info can list the keyword names and values, and the segment
names and file names (full paths) for each record in a record set. It
can also list the full path to the record direcory in SUMS, which
contains the segment files. Exactly what information gets printed is
controlled by command-line flags (see below). The \a -k flag controls
the format of the output.  If it is set, then the output is in table
format, with a header row showing the keyword names.  Otherwise,
keyword name=value pairs are listed one per line.  If the \a -a flag
is set, \ref show_info lists the names of all series keywords, prime
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
show_info [-ajklpqrsDRIVER_FLAGS] ds=<record_set> [n=<nrecords>] [key=<keylist>] [seg=<seglist>]
\endcode

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

\par Flags:
\c -a: Show all keyword names and values for each  record  specified
by \a record_set  or \a nrecords.  \a -a takes precedence over \a
keylist.  
\par
\c -j: List the names of all series keywords, prime keywords, and
segments, and links in jsd format and exit. 
\par
\c -k: List keyword name=value pairs, one per line. Otherwise print
all keyword values on a single line and print a header line containing
the keyword names (table format).
\par
\c -l: List the names of all series keywords, prime keywords,  and
segments, and exit. 
\par
\c -p: Include in the output the full storage-unit path for each record
\par
\c -q: Quiet - omit the header line listing keyword names if the -k
flag is set
\par
\c -r:  Include the record number in the output
\par
\c -s:  Include statistics of series in the output

\par Driver flags: 
\ref jsoc_main

\param record_set
A series name followed by an optional record-set specification (i.e.,
\a seriesname[RecordSet_filter]). Causes selection of a subset of
records in the series. This argument is required, and if no record-set
filter is specified, then \a n=nrecords must be present.

\param nrecords
\a nrecords specifies the maximum number of records for which
information is printed.  If \a nrecords < 0, \ref show_info displays
information for the last \a nrecords records in the record set. If
\a nrecords > 0, \ref show_info displays information for the first
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
The program will produce superflous and non-meaningful output if
called with the \a -p flag and \a seglist is provided on the command line.

\sa
retrieve_file drms_query describe_series

@{
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
  {ARG_FLAG, "j", "0", "list series info in jsd format"},
  {ARG_FLAG, "l", "0", "just list series keywords with descriptions"},
  {ARG_INT, "n", "0", "number of records to show, +from first, -from last"},
  {ARG_FLAG, "p", "0", "list the record\'s storage_unit path"},
  {ARG_FLAG, "P", "0", "list the record\'s storage_unit path but no retrieve"},
  {ARG_FLAG, "k", "0", "keyword list one per line"},
  {ARG_FLAG, "q", "0", "quiet - skip header of chosen keywords"},
  {ARG_FLAG, "r", "0", "recnum - show record number as first keyword"},
  {ARG_FLAG, "s", "0", "stats - show some statistics about the series"},
  {ARG_FLAG, "z", "0", "JSON formatted output - present output in JSON format"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
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
        "  details are:\n"
	"  -a: show information for all keywords\n"
	"  -h: help - show this message then exit\n"
	"  -j: list all series, keyword, segment, and link items in jsd file format, then exit\n"
	"  -k: list keyword names and values, one per line\n"
	"  -l: list all keywords with description, then exit\n"
	"  -p: list the record's storage_unit path (retrieve if necessary)\n"
	"  -P: list the record's storage_unit path (no retrieve)\n"
	"  -q: quiet - skip header of chosen keywords\n"
	"  -r: recnum - show record number as first keyword\n"
	"  -s: stats - show some statistics for how many records, etc.\n"
	"  -z: present the output in JSON format\n"
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

void drms_keyword_print_jsd(DRMS_Keyword_t *key) {
    printf("Keyword:%s",key->info->name);
    if (key->info->islink) {
      printf(", link, %s, %s, %s\n", key->info->linkname, 
	     key->info->target_key,
	     key->info->description);
    } else {
      printf(", %s", drms_type2str(key->info->type));
      int stat;
      const char *rscope = drms_keyword_getrecscopestr(key, &stat);
      fprintf(stdout, ", %s", stat == DRMS_SUCCESS ? rscope : NULL);
      if (key->info->per_segment) 
	printf(", segment");
      else 
	printf(", record");
      printf(", ");
      if (key->info->type == DRMS_TYPE_STRING) {
	char qf[DRMS_MAXFORMATLEN+2];
	sprintf(qf, "\"%s\"", key->info->format);
	printf(qf, key->value.string_val);
      }
      else 
	drms_keyword_printval(key);      
      if (key->info->unit[0] != ' ') {
	printf(", %s, %s, \"%s\"", key->info->format,
	       key->info->unit,
	       key->info->description);
      } else {
	printf(", %s, none, \"%s\"", key->info->format,
	       key->info->description);
      }
    }
    printf("\n");
}

void drms_segment_print_jsd(DRMS_Segment_t *seg) {
  int i;
  printf("Data: %s, ", seg->info->name);
  if (seg->info->islink) {
    printf("link, %s, %s", seg->info->linkname, seg->info->target_seg);
    if (seg->info->naxis) {
      printf(", %d", seg->info->naxis);
      printf(", %d", seg->axis[0]);
      for (i=1; i<seg->info->naxis; i++) {
	printf(", %d", seg->axis[i]);
      }
    }
  } else {
    switch(seg->info->scope)
      {
      case DRMS_CONSTANT:
	printf("constant");
	break;
      case DRMS_VARIABLE:
	printf("variable");
	break;
      case DRMS_VARDIM:
	printf("vardim");
	break;
      default:
	printf("Illegal value: %d", (int)seg->info->scope);
      }
    printf(", %s, %d", drms_type2str(seg->info->type), seg->info->naxis);
    if (seg->info->naxis) {
      printf(", %d", seg->axis[0]);
      for (i=1; i<seg->info->naxis; i++) {
	printf(", %d", seg->axis[i]);
      }
    }
    printf(", %s, ", seg->info->unit);  
    switch(seg->info->protocol)
      {
      case DRMS_GENERIC:
	printf("generic");
	break;
      case DRMS_BINARY:
	printf("binary");
	break;
      case DRMS_BINZIP:
	printf("binzip");
	break;
      case DRMS_FITZ:
	printf("fitz");
	break;
      case DRMS_FITS:
	printf("fits");
	break;
      case DRMS_MSI:
	printf("msi");
	break;
      case DRMS_TAS:
	printf("tas");
	if (seg->info->naxis) {
	  printf(", %d", seg->blocksize[0]);      
	  for (i=1; i<seg->info->naxis; i++)
	    printf(", %d", seg->blocksize[i]);
	}
	break;
      default:
	printf("Illegal value: %d", (int)seg->info->protocol);
      }
  }
  printf(", \"%s\"\n", seg->info->description);
}

void drms_link_print_jsd(DRMS_Link_t *link) {
  printf("Link: %s, %s, ", link->info->name, link->info->target_series);
  if (link->info->type == STATIC_LINK)
    printf("static");
  else
    printf("dynamic");
  printf(", \"%s\"\n", link->info->description);
}


void print_jsd(DRMS_Record_t *rec) {
  const int fwidth=17;
  int i;
  HIterator_t hit;
  DRMS_Link_t *link;
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;

  printf("#=====General Series Information=====\n");
  printf("%-*s\t%s\n",fwidth,"Seriesname:",rec->seriesinfo->seriesname);
  printf("%-*s\t\"%s\"\n",fwidth,"Author:",rec->seriesinfo->author);
  printf("%-*s\t%s\n",fwidth,"Owner:",rec->seriesinfo->owner);
  printf("%-*s\t%d\n",fwidth,"Unitsize:",rec->seriesinfo->unitsize);
  printf("%-*s\t%d\n",fwidth,"Archive:",rec->seriesinfo->archive);
  printf("%-*s\t%d\n",fwidth,"Retention:",rec->seriesinfo->retention);
  printf("%-*s\t%d\n",fwidth,"Tapegroup:",rec->seriesinfo->tapegroup);

  int npkeys = 0;
  char **extpkeys = 
    drms_series_createpkeyarray(rec->env, rec->seriesinfo->seriesname, &npkeys, NULL);
  if (extpkeys && npkeys > 0)
  {
     printf("%-*s\t%s",fwidth,"Index:",extpkeys[0]);
     for (i=1; i<npkeys; i++)
       printf(", %s", extpkeys[i]);
     printf("\n");
  }

  if (extpkeys)
  {
     drms_series_destroypkeyarray(&extpkeys, npkeys);
  }

  printf("%-*s\t%s\n",fwidth,"Description:",rec->seriesinfo->description);
  printf("\n#=====Links=====\n");
  hiter_new(&hit, &rec->links); 
  while( (link = (DRMS_Link_t *)hiter_getnext(&hit)) )
    drms_link_print_jsd(link);

  printf("\n#=====Keywords=====\n");
  hiter_new(&hit, &rec->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&hit)) )
    drms_keyword_print_jsd(key);

  printf("\n#=====Segments=====\n");
  hiter_new(&hit, &rec->segments);
  while( (seg = (DRMS_Segment_t *)hiter_getnext(&hit)) )
    drms_segment_print_jsd(seg);
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

/* print a query that will return the given record */
void drms_print_query_rec(DRMS_Record_t *rec, int want_JSON)
  {
  drms_fprint_query_rec(stdout, rec, want_JSON);
  }

void drms_fprint_query_rec(FILE *fp, DRMS_Record_t *rec, int want_JSON)
  {
  int iprime, nprime;
  DRMS_Keyword_t *rec_key, *key, **prime_keys;
  if (!rec)
    {
    fprintf(fp, "** No Record **");
    return;
    }
  fprintf(fp, "%s",rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  prime_keys = rec->seriesinfo->pidx_keywords;
  if (nprime > 0) 
    {
    for (iprime = 0; iprime < nprime; iprime++)
      {
      key = prime_keys[iprime];
      rec_key = drms_keyword_lookup (rec, key->info->name, 1); 
      fprintf(fp, "[");
      if (key->info->type != DRMS_TYPE_STRING)
        drms_keyword_fprintval (fp, rec_key);
      else
        {
        if (want_JSON)
          {
          fprintf(fp, "\\\"");
          drms_keyword_fprintval (fp, rec_key);
          fprintf(fp, "\\\"");
          }
        else
          {
          fprintf(fp, "\"");
          drms_keyword_fprintval (fp, rec_key);
          fprintf(fp, "\"");
          }
        }
      fprintf(fp, "]");
      }
    }
  else
    fprintf(fp, "[:#%lld]",rec->recnum);
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
  char *in;
  char *keylist;
  char *seglist;
  char *web_query;
  int from_web;
  int show_keys;
  int show_segs;
  int jsd_list;
  int list_keys;
  int show_all;
  int show_stats;
  int max_recs;
  int quiet;
  int show_recnum;
  int keyword_list;
  int want_path;
  int want_path_noret;
  int want_JSON;
  int not_silent;

  if (nice_intro ()) return (0);

  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = strcmp (web_query, "Not Specified") != 0;
  want_JSON = cmdparams_get_int (&cmdparams, "z", NULL) != 0;

  if (from_web)
    {
    want_JSON = 1;
    not_silent = 0;
// parse web_query here to get args setup
// for testing just extract the ds= first keyword
      {
      char *dsraw, *ds, *p;
      dsraw = strdup (web_query);
      ds = strdup (web_query); // make sure there is room
      sscanf(dsraw,"ds=%s",ds);
      if ((p = index(ds,'&'))) *p = '\0';
      cmdparams_set (&cmdparams, "ds", ds);
      cmdparams_set (&cmdparams,"s", "1");
      free(dsraw);
      free(ds);
      }
    }

  in = cmdparams_get_str (&cmdparams, "ds", NULL);
  keylist = strdup (cmdparams_get_str (&cmdparams, "key", NULL));
  seglist = strdup (cmdparams_get_str (&cmdparams, "seg", NULL));
  show_keys = strcmp (keylist, "Not Specified");
  show_segs = strcmp (seglist, "Not Specified");
  jsd_list = cmdparams_get_int (&cmdparams, "j", NULL) != 0;
  list_keys = cmdparams_get_int (&cmdparams, "l", NULL) != 0;
  show_all = cmdparams_get_int (&cmdparams, "a", NULL) != 0;
  show_stats = cmdparams_get_int (&cmdparams, "s", NULL) != 0;
  max_recs =  cmdparams_get_int (&cmdparams, "n", NULL);
  quiet = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  show_recnum =  cmdparams_get_int(&cmdparams, "r", NULL) != 0;
  keyword_list =  cmdparams_get_int(&cmdparams, "k", NULL) != 0;
  want_path = cmdparams_get_int (&cmdparams, "p", NULL) != 0;
  want_path_noret = cmdparams_get_int (&cmdparams, "P", NULL) != 0;

  if(want_path_noret) want_path = 1;	/* also set this flag */

  if (want_JSON)
    printf("Content-type: application/json\n\n");

  /* At least seriesname must be specified */
  if (strcmp(in, "Not Specified") == 0)
    {
    printf("### show_info: ds=<record_query> parameter is required, but I will look for a query without the ds=\n");
    if (cmdparams_numargs(&cmdparams) >= 1 && (in = cmdparams_getarg (&cmdparams, 1)))
      {
      printf("### found \"%s\", using it for the record_query.\n",in);
      }
    else 
      {
      printf("### show_info: Oops, still no query found, quit\n");
      return(1);
      }
    }

  /*  if -l or -j is set, just get all the keywords and print them  */
  if (list_keys || jsd_list || show_stats) 
    {
    char *p, *seriesname;
    DRMS_Keyword_t *key;
    DRMS_Segment_t *seg;
    HIterator_t hit;
    int iprime;
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
        return (1);
        }
      if (recordset->n < 1)
        {
        fprintf(stderr,"### show_info: non-drms series '%s' found but is empty.\n",seriesname);
        return (1);
        }
      rec = recordset->records[0];
      is_drms_series = 0;
      }
    free (seriesname);

    if (list_keys)
      {
      /* show the prime index keywords */
      int npkeys = 0;
      char **extpkeys = 
	drms_series_createpkeyarray(rec->env, rec->seriesinfo->seriesname, &npkeys, NULL);
      if (extpkeys && npkeys > 0)
        {
	printf("Prime Keys are:\n");
	for (iprime = 0; iprime < npkeys; iprime++)
	  printf("\t%s\n", extpkeys[iprime]);
        }

      if (extpkeys)
      {
	 drms_series_destroypkeyarray(&extpkeys, npkeys);
      }

      /* show all keywords */
      printf("All Keywords for series %s:\n",rec->seriesinfo->seriesname);
      hiter_new (&hit, &rec->keywords);
      while ((key = (DRMS_Keyword_t *)hiter_getnext (&hit)))
        printf ("\t%-10s\t%s (%s)\n", key->info->name, key->info->description,
            drms_type_names[key->info->type]);
  
      /* show the segments */
      if (rec->segments.num_total)
        {
        printf("Segments for series %s:\n",rec->seriesinfo->seriesname);
        hiter_new (&hit, &rec->segments);
        while ((seg = (DRMS_Segment_t *)hiter_getnext (&hit)))
            printf ("\t%-10s\t%s\n", seg->info->name, seg->info->description);
        }
      return (0);
      }
    else if (jsd_list) 
      {
      print_jsd(rec);
      return(0);
      }
    if (show_stats)
     {
     if (is_drms_series)
      {
      DRMS_RecordSet_t *rs;

      rs = drms_find_rec_first(rec, 1);
      if (!rs || rs->n < 1)
        {
        if (want_JSON)
          printf("{ \"status\":1}\n");
        else
          printf("No records Present\n");
        }
      else
        {
        if (want_JSON)
          {
          printf("{ \"status\":0,\n");
          printf(" \"FirstRecord\" : \"");
          }
        else
          printf("First Record: ");
        drms_print_query_rec(rs->records[0], want_JSON);
        if (want_JSON)
          printf("\",\n \"FirstRecnum\" : %ld,\n", rs->records[0]->recnum);
        else
          printf(", Recnum = %ld\n", rs->records[0]->recnum);
        drms_free_records(rs);
  
        rs = drms_find_rec_last(rec, 1);
        if (want_JSON)
          printf(" \"LastRecord\" : \"");
        else
          printf("Last Record:  ");
        drms_print_query_rec(rs->records[0], want_JSON);
        if (want_JSON)
          printf("\",\n \"LastRecnum\" : %ld,\n", rs->records[0]->recnum);
        else
          printf(", Recnum = %ld\n", rs->records[0]->recnum);
        drms_free_records(rs);
  
        rs = drms_find_rec_last(rec, 0);
        if (want_JSON)
          printf(" \"MaxRecnum\" : %ld\n", rs->records[0]->recnum);
        else
          printf("Last Recnum:  %ld", rs->records[0]->recnum);
        if (want_JSON)
          printf("}\n");
        else
          printf("\n");
        }
      return(0);
      }
     else printf("### Can not use '-s' flag for non-drms series. Sorry.\n");
     }
    }
  if (want_JSON) return(0);

  /* check for poor usage of no query and no n=record_count */
  inqry = index(in, '[');
  if (!inqry && !max_recs)
    {
    fprintf(stderr, "### show_info query must have n=recs or record query specified\n");
    return(1);
    }

  /* Open record_set */

fprintf(stderr,"test 1 query is %s\n",in);
  recordset = drms_open_records (drms_env, in, &status);
fprintf(stderr,"test 1 query is %d\n",status);
  if (!recordset) 
    {
    fprintf(stderr,"### show_info: series %s not found.\n",in);
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
      drms_print_query_rec(rec, 0);
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

