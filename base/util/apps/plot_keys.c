/*
 *  plot_keys - plots keyword information for two data fields, x & y
 *
 *  Bugs:
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_STRING, "key", "Not Specified", "<comma delimited keyword list>"},
  {ARG_FLAG, "a", "0", "Show info for all keywords"},
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_FLAG, "l", "0", "just list series keywords with descriptions"},
  {ARG_INT, "n", "0", "number of records to show, +from first, -from last"},
  {ARG_FLAG, "r", "0", "recnum - show record number as first keyword"},
  {ARG_END}
};

char *module_name = "plot_keys";

int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\nplot_keys [-ahlr] "
	"ds=<recordset query> {n=0} {key=<keylist>} {seg=<segment_list>}\n"
        "  details are:\n"
	"  -a: show information for all keywords\n"
	"  -h: help - show this message then exit\n"
	"  -l: list all series keywords with description, then exit\n"
	"  -r: recnum - show record number as first keyword\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"n=0 number of records in query to plot, +n from start or -n from end\n"
	"key=<comma delimited keyword list>, for all use -a flag\n");
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
  int ikey, nkeys = 0;
  char *inqry;
						/* Get command line arguments */
  char *in = cmdparams_get_str (&cmdparams, "ds", NULL);
  char *keylist = strdup (cmdparams_get_str (&cmdparams, "key", NULL));
  int plot_keys = strcmp (keylist, "Not Specified");
  int list_keys = cmdparams_get_int (&cmdparams, "l", NULL) != 0;
  int show_all = cmdparams_get_int (&cmdparams, "a", NULL) != 0;
  int max_recs =  cmdparams_get_int (&cmdparams, "n", NULL);
  int show_recnum =  cmdparams_get_int(&cmdparams, "r", NULL) != 0;
  int keyword_list =  cmdparams_get_int(&cmdparams, "k", NULL) != 0;

  if (nice_intro ()) return (0);
		  /*  if -l is set, just get all the keywords and print them  */
  if (strcmp(in, "Not Specified") == 0)
    {
    printf("### plot_keys: ds=<record_query> parameter is required, but I will look for a query without the ds=\n");
    if (cmdparams_numargs(&cmdparams) >= 1 && (in = cmdparams_getarg (&cmdparams, 1)))
      {
      printf("### found \"%s\", using it for the record_query.\n",in);
      }
    else 
      {
      printf("### plot_keys: Oops, still no query found, quit\n");
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
      fprintf(stderr,"### plot_keys: series %s not found.\n",seriesname);
      return(1);
      }
    free (seriesname);

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

  /* check for poor usage of no query and no n=record_count */
  inqry = index(in, '[');
  if (!inqry && !max_recs)
    {
    fprintf(stderr, "### plot_keys query must have n=recs or record query specified\n");
    return(1);
    }

  /* Open record_set */
  recordset = drms_open_records (drms_env, in, &status);
  if (status) 
    {
    printf ("drms_open_records failed, in=%s, status=%d.  Aborting.\n", in,
        status);
    return (1);
    }

/* records now points to a struct with count of records found ("n"), and a pointer to an array of record pointers ("records"); */

  nrecs = recordset->n;
  if (nrecs == 0)
    {
      printf ("** No records in selected data set, query was %s **\n",in);
    }
  if (max_recs > 0 && max_recs < nrecs)
    nrecs = max_recs;
  last_rec = nrecs - 1;
  if (max_recs < 0 && nrecs+max_recs > 0)
    first_rec = nrecs + max_recs;
  else
    first_rec = 0;

  /* get list of keywords to plot for each record */
  nkeys = 0;
  if (show_all) 
    { /* if wanted get list of all keywords */
    DRMS_Keyword_t *key;
    HIterator_t hit;
    hiter_new (&hit, &recordset->records[0]->keywords);
    while ((key = (DRMS_Keyword_t *)hiter_getnext (&hit)))
      keys[nkeys++] = strdup (key->info->name);
    }
  else if (plot_keys)
    { /* get specified list */
    char *thiskey;
    for (thiskey=strtok(keylist, ","); thiskey; thiskey=strtok(NULL,","))
	keys[nkeys++] = strdup(thiskey);
    }
  free (keylist);

  /* loop over set of selected records */
  for (irec = first_rec; irec <= last_rec; irec++) 
    {
    rec = recordset->records[irec];  /* pointer to current record */
    if (firstrec)  /* print header line if not quiet and in table mode */
      {
      firstrec=0;
      if (!keyword_list) 
        {			/* print keyword and segment name header line */
        if (show_recnum)
          printf ("recnum\t");
        for (ikey=0 ; ikey<nkeys; ikey++)
          printf ("%s\t",keys[ikey]); 
        printf ("\n");
        }
      }

    if (keyword_list) /* if not in table mode, i.e. value per line mode then show record query for each rec */
      {
      /*Open file for writing gnuplot data */
      FILE *myfile;
       myfile = fopen("gnuplot_cmd.txt", "w");  /*  open datafile.txt for writing  */       if (myfile == NULL) {
          fprintf(stderr, "Can't open datafile.txt for reading");
           exit(1);
       }

      void drms_fprint_gnuplot_header(FILE *plotfile, DRMS_Record_t *rec);
      void drms_fprint_query_rec(FILE *plotfile, DRMS_Record_t *rec);
      drms_fprint_gnuplot_header(myfile, rec);
      drms_fprint_query_rec(myfile, rec);
      fclose(myfile);
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
    }
/* Finished.  Clean up and exit. */
  for (ikey=0; ikey<nkeys; ikey++) 
    free(keys[ikey]);
  drms_close_records(recordset, DRMS_FREE_RECORD);
  return status;
  }

/* print a gnuplot header to pass to plotting tool */
void drms_fprint_gnuplot_header(FILE *plotfile, DRMS_Record_t *rec)
  {
DRMS_Keyword_t *rec_key, *key, **prime_keys;
        /* start header for gnuplot file */
        fprintf(plotfile,"# set terminal png transparent nocrop enhanced font arial 10 size 400,400\n");
        fprintf(plotfile,"# set output 'plot_keys.png'\n");
        fprintf(plotfile,"set boxwidth 0.75 absolute\n");
        fprintf(plotfile,"set style fill  solid 1.00 border -1\n");
        fprintf(plotfile,"set key outside right top vertical Left reverse enhanced autotitles columnhead nobox\n");
        fprintf(plotfile,"set title `%s`\n",rec->seriesinfo->seriesname);
        /* End header */
  }

/* print a query that will return the given record */
void drms_fprint_query_rec(FILE *plotfile, DRMS_Record_t *rec)
  {
  int iprime, nprime;
  DRMS_Keyword_t *rec_key, *key, **prime_keys;
  fprintf(plotfile,"%s",rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  prime_keys = rec->seriesinfo->pidx_keywords;
  if (nprime > 0) 
    {
    for (iprime = 0; iprime < nprime; iprime++)
      {
      key = prime_keys[iprime];
      rec_key = drms_keyword_lookup (rec, key->info->name, 1); 
      fprintf(plotfile,"[");
      if (key->info->type != DRMS_TYPE_STRING)
        drms_keyword_fprintval (plotfile, rec_key);
      else
        {
        fprintf(plotfile,"\"");
        drms_keyword_fprintval (plotfile, rec_key);
        fprintf(plotfile,"\"");
        }
    fprintf(plotfile,"]");
    }
  }
else
fprintf(plotfile,"[:#%lld]",rec->recnum);
}
