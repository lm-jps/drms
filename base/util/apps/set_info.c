/* set_info - set keyword information and/or file path for given recordset. 
 *  This is really just set_keys modified to handle data segments of
 *  FITS protocol.
 *  --Art
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "drms_cmdparams.h"
/**
\defgroup set_keys set_info - Add or update records with new keyword or segment filename values
@ingroup drms_util

\par Synopsis:

\code
set_info [-h] 
set_info [-ciktv] [JSOC_FLAGS] ds=<seriesname> [<keyword>=<value>]... [<segment>=<filename>]... [<link>=<record-set specification>]...
set_info [-Cimtv] [JSOC_FLAGS] ds=<record_set> [<keyword>=<value>]... [<segment>=<filename>]... [<link>=<record-set specification>]...
set_info_soc {options as above}
\endcode

\details

Sets keyword values for a record in a DRMS dataseries.  \b set_info runs in one of two modes.
\li If a "create" \c -c or \c -k flag is present a new record will be created.  In this case ALL of the prime keys
must be present in \e keyword=value pairs to specify the locator keywords for the new record.  Other keywords
and or segments specifications are usually also present.  If \c -c is set then a single record will be created
and populated from the command line.  If the \c -k flag is set stdin will be read for a set of records in the same
format as produced by show_info -k and a record will be created for each record specified in the stdin file.
\li An existing record_set may be specified and a new version of those record will be made with the specified keywords
given new values. Usually, almost always, only a single record is updated in one call of set_info since
only one set of keyword values can be provided and all records modified will be given the same new
values for those keywords.  If multiple records are to be changed the \c -m flag is required.
\b set_info will then clone the records specified by the record-set filter.
In the update mode, NONE of the prime keys are allowed in keyword=value pairs on the command line (except
of course as a where clause as part of the recordset specification)

For each keyword specified in a \a keyword=value pair on the command line, \b set_info will set the values for all these clones' keywords to \a value.

If a segment in the series is of type \e generic then set_info can be used to store a given file in
the SUMS directory associated with the record.
In this case, for each \c segment=filename parameter the file found at filename will be copied into the
record directory and the segment filename will be saved in the segment descriptor in DRMS.

If a \c segment=<filename> argument specifies the path to a FITS file, then set_info will parse the FITS-file
header. For each header keyword, set_info examines the set of series' DRMS keywords. If the header-keyword name matches the name of such
a DRMS keyword, then set_info assigns the header-keyword's value to the DRMS keyword. If there exists
such a DRMS header keyword, and the DRMS keyword also appears as the name of an argument on the 
command line, then the command-line argument value takes precendence over the value in the FITS-file header. The
header-keyword value is ignored in this case. This
default behavior can be disabled with the \c -i flag. If the \c -i flag is present on the command line,
along with the \c segment=<filename> argument where \c <filename> is a FITS file, then the entire FITS-file header 
is not parsed and is essentially ignored by set_info. 

In no case is a constant DRMS-keyword value updated by set_info. If a constant DRMS keyword is specified on the 
command line, then set_info exits with an error. If a FITS file is specified on the command line, the module simply 
ignores FITS-file header keywords that correspond to constant DRMS keywords. 

In normal usage if NO segments are specified, any segments in the original version of an updated record will
be shared with the new record.  This allows keyword metadata to be updated without making unnecessary copies
of file data in SUMS.  However if any segment file is to be updated, a new record directory must be allocated
and any other segment's files will be copied (cloned) to the new directory and associated with the new record.
If the \a -C "clone" flag is present then a new record directory will be allocated even if none of the
segments are to be updated.  This usually only makes sense in a JSOC session script where the same record
will be incrementally updated by multiple modules and set_info is invoked as set_info_sock.  
For each link argument specified, the argument name must match the name of a link in the series.
The argument value for each such argument must be a record-set specification that identifies a single record in a target series. If these conditions are met, then for each link argument specified, a link between the newly created record and the record specified by the 
record-set specification will be created.

Only one instance of each keyword name or segment name is allowed.

\par Options:

\par Flags:

\li \c -c: Create a new record
\li \c -k: Create a new set of records from stdin as if piped from show_info -k
\li \c -C: Force-copy the storage unit of the original record to the cloned record
\li \c -h: Print usage message and exit
\li \c -i: Ignore the default behavior of parsing the FITS-file header for keyword values
\li \c -m: Modify the keywords of multiple records. The \c -m flag should be
used with caution.  A typo could damage many records. Do not use
\c -m unless you are sure the query will specify ONLY the records
you want to modify.
\li \c -t: Create any needed records as \ref DRMS_TRANSIENT instead of default which is \ref DRMS_PERMANENT
\li \c -v: Verbose - noisy.

The \c -c and \c -m flags cannot be used simultaneously.

\par JSOC flags: 
\ref jsoc_main

\par Usage:

\c seriesname is the name of the JSOC DRMS series to be updated.  If new records are being
created, no \e where clauses are permitted.

\c record_set
A series name followed by a record-set specification
(i.e., \a seriesname[\a filter]).
If \a record_set resolves to more than one record, then the \a -m flag must be
set to prevent simple typos from doing extensive damage.

\param valueN
The new keyword values to be used to create a new or modify an
existing record.

\param filename

If modifying a record(s) the specified \a segment is a generic series segment,
then first copy the segment file to the cloned record(s)' segment storage,
then store the filename with in the segment descriptor.
Any leading pathname directories will be removed from the filename value prior to
updating the segment descriptor.

\par Example to modify a keyword value:
\code
set_info ds=su_arta.TestStoreFile[file=dsds_data.fits][sel=January] note=fred
\endcode

\par Example to create a new record and specify keyword values:
\code
set_info -c ds=su_arta.TestStoreFile file=data.txt sel=February file_seg=/home/arta/febdata.txt
\endcode

\bug 

*/
ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "ds", "Not Specified", "Series name with optional record spec"},
  {ARG_FLAG, "h", "0", "Print usage message and quit"},
  {ARG_FLAG, "c", "0", "Create a new record from command line keywords"},
  {ARG_FLAG, "C", "0", "Force cloning of needed records to be DRMS_COPY_SEGMENT mode"},
  {ARG_FLAG, "i", NULL, "Ignore the default behavior of ingesting the FITS-file header"},
  {ARG_FLAG, "k", "0", "Create new records as specified from stdin"},
  {ARG_FLAG, "m", "0", "allow multiple records to be updated"},
  {ARG_FLAG, "t", "0", "create any needed records as DRMS_TRANSIENT, default is DRMS_PERMANENT"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_END}
};

char *module_name = "set_info";

int verbose = 0;

int nice_intro(int help)
  {
  int usage = cmdparams_get_int(&cmdparams, "h", NULL) != 0;
  verbose = cmdparams_get_int(&cmdparams, "v", NULL) != 0;
  if (usage || help)
    {
    printf("set_info (({-c}|{-k})|{-m}) {-C} {-i} {-t} {-h} {-v}  "
	"ds=<recordset query> {keyword=value} ... \n"
	"  -h: print this message\n"
	"  -c: create - create new record\n"
        "  -i: ignore - ignore the FITS-file header\n"
	"  -k: create_many - create a set of new records\n"
	"  -m: multiple - allow multiple records to be updated\n"
        "  -C: Force cloning of needed records to be DRMS_COPY_SEGMENT mode\n"
        "  -t: create any needed records as DRMS_TRANSIENT, default is DRMS_PERMANENT\n"
	"  -v: verbose\n"
	"ds=<recordset query> as <series>{[record specifier]} - required\n"
	"keyword=value pairs as needed\n"
        "segment=filename pairs \n");
    return(1);
    }
  return(0);
  }

#define DIE(msg) {fprintf(stderr,"$$$$ set_info error: %s\n",msg); return 1;}

DRMS_Type_Value_t cmdparams_get_type(CmdParams_t *cmdparams, char *keyname, DRMS_Type_t keytype, int *status);

/* keys is an array of HContainer_t *, keys are the keys from the current fits file, keylist is the combined
 * list of keywords from all fits files imported so far. */
static int FitsImport(DRMS_Record_t *rec, DRMS_Segment_t *seg, const char *file, HContainer_t **keys, HContainer_t *keylist)
{
   int err = 0;
   int drmsstat = DRMS_SUCCESS;
   DRMS_Array_t *data = NULL;
   HContainer_t *keysint = NULL;
   HIterator_t hit;
   const char *intstr = NULL;
   DRMS_Keyword_t *akey = NULL;
    HContainer_t *keysintFiltered = NULL;
    DRMS_Keyword_t *drmskey = NULL;
    

   /* Read data - don't apply bzero and bzero in fits file (leave israw == true). If
    * the segment requires conversion of the output array, this will happen in 
    * drms_segment_write(), but don't convert when reading, since the writing may
    * have to undo the conversion. */
   data = drms_fitsrw_read(drms_env, file, 1, &keysint, &drmsstat);
   if (data && drmsstat == DRMS_SUCCESS)
   {
      drmsstat = drms_segment_write(seg, data, 0);
      if (drmsstat != DRMS_SUCCESS)
      {
         err = 1;
      }
      else
      {
          /* Since the ultimate DRMS record we are creating might contain multiple segments and, therefore, multiple
           * data files, we have to ensure that for each keyword the data files have in common either 
           * 1. the multiple keyword values match, or 2. the DRMS record belongs to a series that has a 
           * per-segment keyword to accommodate the disparate keyword values. To that end, 
           * store the keywords in a container with <keywordname>:<segnum> as the key and the DRMS_Keyword_t
           * as the value. After the loop containing this function call ends, check all the keyword and
           * resolve the conflicts between keyword values. */
          
          /* Iterate through keys and add unique values to keylist. BUT DON'T ADD KEYS THAT DO NOT 
           * BELONG TO THE SERIES BEING POPULATED. And do not attempt to add values for series' keywords
           * that are constant. keysintFiltered has all the keys in keysint, minus the ones that are
           * constant keywords. */
          keysintFiltered = hcon_create(sizeof(DRMS_Keyword_t), 
                                        DRMS_MAXKEYNAMELEN, 
                                        (void (*)(const void *))drms_free_template_keyword_struct,
                                        NULL, 
                                        NULL,
                                        NULL,
                                        0);
          
          hiter_new_sort(&hit, keysint, drms_keyword_ranksort);
          while ((akey = hiter_extgetnext(&hit, &intstr)) != NULL)
          {
              /* Remove constant keywords from final list of keywords to work with. */
              /* These keywords are NOT part of any drms record, so the only valid data in them is their name, type and value. To
               * determine if they are constant or not, we must find the same-named keyword in the rec passed in. */
              drmskey = drms_keyword_lookup(rec, akey->info->name, 0);
              
              if (drmskey && !drms_keyword_isconstant(drmskey))
              {
                  /* This call copies the record ptr (which is NULL), the info ptr (so it steals the key info), and 
                   * the keyword value from akey to the entry in keysintFiltered. */
                  hcon_insert(keysintFiltered, intstr, akey);
                  
                  if (!hcon_member(keylist, intstr))
                  {
                      hcon_insert(keylist, intstr, (const void *)&akey->info->rank); 
                  }
                  
                  /* Ensure that when keysint is freed, that the info fields are not freed to (since
                   * they are in use by keysintFiltered). */
                  akey->info = NULL;
              }
              
          }
          
          hiter_free(&hit);
          
          keys[seg->info->segnum] = keysintFiltered;
          hcon_destroy(&keysint);
      }
   }
   else
   {
      err = 1;
   }

   if (data)
   {
      drms_free_array(data);
      data = NULL;
   }

   return err;
}

static inline int KeySort(const void *he1, const void *he2)
{
   int *r1 = (int *)hcon_getval(*((HContainerElement_t **)he1));
   int *r2 = (int *)hcon_getval(*((HContainerElement_t **)he2));
   
   XASSERT(r1 && r2);

   return (*r1 < *r2) ? -1 : (*r1 > *r2 ? 1 : 0);
}

/* keylist is a list of all unique keys - it is not necessarily true that every segment file will 
 * have had all these keys. 
 * segfilekeys is an array of key containers - one for each segment file (indexed by segnum). */
static int WriteKeyValues(DRMS_Record_t *rec, int nsegments, HContainer_t *keylist, HContainer_t **segfilekeys)
{
   int conflict;
   int skipkey;
   HIterator_t keyhit;
   const char *kbuf = NULL;
   char persegkey[DRMS_MAXKEYNAMELEN];
   DRMS_Type_Value_t *val;
   DRMS_Type_t *valtype;
   int persegexists = 0;
   DRMS_Keyword_t *segfilekey = NULL;
   HContainer_t *segfilekeyhash = NULL;
   int isegment;
   char query[DRMS_MAXQUERYLEN];
   DB_Text_Result_t *qres = NULL;
   char *ns = NULL;
   int skret;
   int err = 1;

   get_namespace(rec->seriesinfo->seriesname, &ns, NULL);
   if (ns)
   {
      if (hcon_size(keylist) > 0)
      {
         hiter_new_sort(&keyhit, keylist, KeySort);
         while (hiter_extgetnext(&keyhit, &kbuf))
         {
            conflict = 0;
            skipkey = 0;
            val = NULL;
            valtype = NULL;

            /* Determine if there are any keyword conflicts. */
            for (isegment = 0; isegment < nsegments; isegment++)
            {
               segfilekeyhash = segfilekeys[isegment];

               if (segfilekeyhash)
               {
                  /* It isn't necessarily true that all segments are being 'set'. */
                  segfilekey = (DRMS_Keyword_t *)hcon_lookup(segfilekeyhash, kbuf);

                  if (segfilekey)
                  {
                     /* don't assume that all data files had this keyword */
                     if (val)
                     {
                        if (*valtype != segfilekey->info->type)
                        {
                           /* We have a conflict in data types (two keys with the same name, but different types).
                            * Ignore the key and print a warning. */
                           fprintf(stderr, "Conflicting data types for keyword '%s'; no value for this keyword will be written to this record.\n", segfilekey->info->name);
                           skipkey = 1;
                           break;
                        }
                        else if (!drms_equal(*valtype, val, &segfilekey->value))
                        {
                           conflict = 1;
                           break;
                        }
                     }
                     else
                     {
                        val = &segfilekey->value;
                        valtype = &segfilekey->info->type;
                     }
                  }
                  else
                  {
                     /* At least one segment file had a keyword and one did not - conflict that cannot be resolved. */
                     fprintf(stderr, "Keyword '%s' is not present in all segment files;  no value for this keyword will be written to this record.\n", kbuf);
                     skipkey = 1;
                     break;
                  }
               }
            } /* loop segments */

            /* Resolve conflicts and write keyword value. */
            if (!skipkey)
            {
               char *lckeyname = strdup(kbuf);

               strtolower(lckeyname);
               /* Check to see if keyword in output series is per-segment or not (if the series
                * has a per-segment keyword, then you must specify an index in the keyword name). */
               snprintf(query, sizeof(query), "SELECT * FROM %s.drms_keyword WHERE lower(seriesname) = '%s' AND lower(keywordname) = '%s' AND persegment & 1 = 1", ns, rec->seriesinfo->seriesname, lckeyname);
               free(lckeyname);

               if (rec->env->verbose)
               {
                  fprintf(stdout , "Per-segment Keyword-Check Query: %s\n", query);
               }

               if ((qres = drms_query_txt(rec->env->session, query)) != NULL && qres->num_rows != 0) 
               {
                  persegexists = 1;
               }
               else
               {
                  persegexists = 0;
               }

               if (qres)
               {
                  db_free_text_result(qres);
                  qres = NULL;
               }
         
               if (conflict || persegexists)
               {
                  if (persegexists)
                  {
                     /* Write each segment's keyword value to the corresponding segment-specific DRMS keyword. */
                     for (isegment = 0; isegment < nsegments; isegment++)
                     {
                        segfilekeyhash = segfilekeys[isegment];

                        if (segfilekeyhash)
                        {
                           segfilekey = hcon_lookup(segfilekeyhash, kbuf);
                           snprintf(persegkey, sizeof(persegkey), "%s[%d]", kbuf, isegment);

                           if (!drms_keyword_lookup(rec, persegkey, 0))
                           {
                              fprintf(stderr, "The output series '%s' does not contain keyword '%s', skipping and continuing.\n", rec->seriesinfo->seriesname, kbuf);
                           }
                           else
                           {
                              if ((skret = drms_setkey(rec, persegkey, segfilekey->info->type, &segfilekey->value)) == DRMS_ERROR_KEYWORDREADONLY)
                              {
                                 fprintf(stderr, "Unable to write value for read-only keyword '%s'.\n", kbuf);
                              }
                              else if (skret != DRMS_SUCCESS)
                              {
                                 fprintf(stderr, "Error writing value for keyword '%s'; continuing.\n", kbuf);
                              }
                              else
                              {
                                 err = 0;
                              }
                           }
                        }
                     }
                  }
                  else
                  {
                     fprintf(stderr, "Conflicting keyword values for keyword '%s' and no per-segment keyword exists; no value for this keyword will be written to this record.\n", kbuf);
                  }
               }
               else
               {
                  /* Check for the existence of the keyword to be written - if the input file 
                   * contains a keyword that the output series does not contain, then simply 
                   * print a warning, but continue. */
                  if (!drms_keyword_lookup(rec, kbuf, 0))
                  {
                     fprintf(stderr, "The output series '%s' does not contain keyword '%s', skipping and continuing.\n", rec->seriesinfo->seriesname, kbuf);
                  }
                  else
                  {
                     /* Write to record the keyword value. */
                     if ((skret = drms_setkey(rec, kbuf, *valtype, val)) == DRMS_ERROR_KEYWORDREADONLY)
                     {
                        fprintf(stderr, "Unable to write value for read-only keyword '%s'.\n", kbuf);
                     }
                     else if (skret != DRMS_SUCCESS)
                     {
                        fprintf(stderr, "Error writing value for keyword '%s'; continuing.\n", kbuf);
                     }
                     else
                     {
                        err = 0;
                     }
                  }
               }
            }
         } /* loop over master keylist (the list of unique keys) */

         hiter_free(&keyhit);
      } /* keylist not empty */
      else
      {
         /* no keys to add at all */
         err = 0;
      }

      free(ns);
   }

   return err;
}

static int IngestingAFile(DRMS_Record_t * rec)
{
    int rv = 0;
    DRMS_Segment_t *seg = NULL;
    HIterator_t *hit = NULL;

    /* We do NOT want to follow links, so don't use drms_segment_lookup...(). */
    while ((seg = drms_record_nextseg(rec, &hit, 0)) != NULL)
    {        
        if (!seg->info->islink)
        {
            /* Can't ingest a file for a linked segment. */
            
            if (seg->info->protocol == DRMS_GENERIC || seg->info->protocol == DRMS_FITS)
            {
                const char *segname = NULL;
                const char *filename = NULL;
                segname = seg->info->name;
                filename = cmdparams_get_str(&cmdparams, segname, NULL);
                if (filename && *filename)
                {
                    rv = 1;
                    break; /* Stop iterating if we found a segment file. */
                    /* We know that there is a valid segment name on the cmd-line, so we WILL need 
                     * to fetch the SU containing the segment at some point. If we don't reach 
                     * this spot, then we don't want to access SUMS, unless force_copyseg is set. */
                }
            }
        }
    }
    
    if (hit)
    {
        hiter_destroy(&hit);
    }
    
    return rv;
}

static int CreateLinks(DRMS_Record_t *srec, HContainer_t *links)
{
    HIterator_t *lhit = NULL;
    DRMS_Link_t *lnk = NULL;
    int status;
    int rv = 0;
    
    lhit = hiter_create(links);
    
    if (lhit)
    {
        const char *lname = NULL;
        const char *lval = NULL;
        DRMS_RecordSet_t *rs = NULL;
        DRMS_Record_t *rec = NULL;
        
        while ((lnk = (DRMS_Link_t *)hiter_getnext(lhit)) != NULL)
        {
            lname = lnk->info->name;
            
            if (cmdparams_exists(&cmdparams, (char*)lname))
            {
                lval = cmdparams_get_str(&cmdparams, lname, NULL);
                
                /* Verify that lval is a valid record-set specification. */
                rs = drms_open_records(drms_env, lval, &status);
                if (!rs || status != DRMS_SUCCESS)
                {
                    fprintf(stderr, "Invalid record-set specification '%s'.\n", lval);
                    rv = 1;
                    break;
                }
                
                if (rs->n != 1)
                {
                    fprintf(stderr, "Record-set specification '%s' does not identify a single record.\n", lval);
                    rv = 1;
                    break;
                }
                
                rec = drms_recordset_fetchnext(drms_env, rs, &status, NULL, NULL);
                
                if (!rec || status != DRMS_SUCCESS)
                {
                    fprintf(stderr, "Unable to fetch records.\n");
                    rv = 1;
                    break;
                }
                
                if (drms_link_set(lname, srec, rec) != DRMS_SUCCESS)
                {
                    fprintf(stderr, "Failure creating %s link.\n", lname);
                    rv = 1;
                    break;
                }
            }       
        }
        
        if (rs)
        {
            drms_close_records(rs, DRMS_FREE_RECORD);
        }
        
        hiter_destroy(&lhit);
    }
    else
    {
        fprintf(stderr, "Unable to create iterator.\n");
        rv = 1;
    }
    
    return rv;
}

int found_from_stdin = 0;

/* prepare for create_from_stdin by getting next set of keywords and inserting onto command line */
int get_params_from_stdin()
  {
  char buf[4096];
  while (fgets(buf, 4096, stdin))
    {
    char *eq;
    char *p = buf;
    while (*p && isblank(*p))
      p++;
    if (*p == '\n') 
      {
      if (found_from_stdin) // blank line is between records
{
fprintf(stderr,"found_from_stdin=%d\n",found_from_stdin);
        return(1);
}
      else
{
fprintf(stderr,"found blank line, skip\n");
        continue; // skip blank lines ahead of keyword list
}
      }
    if (*p == '#') // skip comment lines
      continue;
    eq = index(p, '=');
    if (!eq)  // skip lines without '='
      continue;
    *eq++ = '\0';
    while (*eq && isblank(*eq))
      eq++;
    found_from_stdin++;
    if (*eq == '"') // strip leading and trailing quotes
      {
      eq++;
      char *rquote = rindex(eq, '"');
      if (rquote)
        *rquote = '\0';
      else
        fprintf(stderr, "Keyword %s has leading but not trailing quote\n", p);
      }
    char *nl = rindex(eq, '\n');
    if (nl)
      *nl = '\0';
    // now p points to keyword, eq points to value
    cmdparams_set(&cmdparams, p, eq);
    if (verbose)
      fprintf(stderr,"added %s = %s\n",p,eq);
    }
  return(0);
  }

/* Module main function. */
int DoIt(void)
{
  int status = 0;
  int multiple = 0;
  int create = 0;
  int create_from_stdin = 0;
  int nrecs, irec;
  int force_transient;
  int force_copyseg;
  int ignoreHeader = 0;
  const char *keyname;
  char prime_names[100][32];
  char **pkeys;
  char *query;
  char *p;
  DRMS_Type_t keytype;
  DRMS_Type_Value_t key_anyval;
  DRMS_Record_t *rec;
  DRMS_RecordSet_t *rs;
  DRMS_Keyword_t *key;
  DRMS_Segment_t *seg;
  int nprime, iprime;
  int isegment;
  int is_new_seg = 0;

  if (nice_intro(0))
    return(0);

/* Get command line arguments */
   query = strdup(cmdparams_get_str(&cmdparams, "ds", NULL));

   force_copyseg = cmdparams_get_int(&cmdparams, "C", NULL) != 0;
   ignoreHeader = cmdparams_isflagset(&cmdparams, "i");
   force_transient = cmdparams_get_int(&cmdparams, "t", NULL) != 0;

   multiple = cmdparams_get_int(&cmdparams, "m", NULL) != 0;
   create = cmdparams_get_int(&cmdparams, "c", NULL) != 0;
   create_from_stdin = cmdparams_get_int(&cmdparams, "k", NULL) != 0;
   if (multiple && (create || create_from_stdin))
   {
      if (query) { free(query); query = NULL; }
      DIE("-c or -k with -m not compatible");
   }
  p = index(query,'[');
  if (!p && !(create || create_from_stdin))
  {
     if (query) { free(query); query = NULL; }
     DIE ("must be in create mode if no record spec given");
  }
  if (p && (create || create_from_stdin))
  {
     if (query) { free(query); query = NULL; }
     DIE("can only create new record, record set spec not allowed");
  }
  /* Now can test on create and multiple for program control */

  if (verbose)
  {
     /* Print something to identify what series is being modified */
     fprintf(stderr, "set_info() %s, query is %s.\n", (create || create_from_stdin) ? "creating record(s)" : "updating record", query);
  }

  if (create || create_from_stdin)
    { /* in case of create_from_stdin, this will be the first record only */
    if (verbose)fprintf(stderr, "Make new record\n");
    rs = drms_create_records(drms_env, 1, query, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT), &status);
    if (status)
    {
        char msgbuf[128];
        snprintf(msgbuf, sizeof(msgbuf), "cant create records in series %s, status %d", query, status);
       if (query) { free(query); query = NULL; }
       DIE(msgbuf);
    }
    nrecs = 1;
    rec = rs->records[0];

    pkeys = drms_series_createpkeyarray(drms_env, 
					rec->seriesinfo->seriesname,
					&nprime, 
					&status);
    if (status)
        {
        if (query) { free(query); query = NULL; }
          DIE("series bad, prime key missing");
        }

    for (iprime = 0; iprime < nprime; iprime++)
        {
        keyname = pkeys[iprime];
	strcpy(prime_names[iprime], keyname);
        key = drms_keyword_lookup(rec, keyname, 1);
        if (key->info->islink || key->info->recscope == kRecScopeType_Constant)
          DIE("Prime keys may not be linked or constant");
	}

    /* now record exists with prime keys set. */
    }
  else
   {
      /* We are not creating a new record, but instead 'updating' an existing one. */
   DRMS_RecordSet_t *ors;
   if (verbose)fprintf(stderr, "Clone record for update\n");
   ors = drms_open_records(drms_env, query, &status);
   if (status)
   {
      if (query) { free(query); query = NULL; }
      DIE("cant open recordset query");
   }
    nrecs = ors->n;
   if (nrecs > 1 && !multiple)
   {
      if (query) { free(query); query = NULL; }
      DIE("multiple records not expected");
   }
    if (nrecs == 0)
	{
	printf("No records found for %s\n", query);
	return 0;
	}
    rec = ors->records[0];

    pkeys = drms_series_createpkeyarray(drms_env, 
					rec->seriesinfo->seriesname, 
					&nprime, 
					&status);

    for (iprime = 0; iprime < nprime; iprime++)
        {
	keyname = pkeys[iprime];
	strcpy(prime_names[iprime], keyname);
        }
    /* now clone with sharing the existing record set.  Someday this may
       be replaced with code to delete the old record in the case where
       this module is in the same session that created the original
       record.
     */
    /* if a segment is present matching the name of a segment=filename then copy instead of share */

    /* find out if the caller is trying to ingest a file - if so, set is_new_seg --> 1 */
    is_new_seg = IngestingAFile(rec);

    if (is_new_seg || force_copyseg)
    {
       rs = drms_clone_records(ors, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT), DRMS_COPY_SEGMENTS , &status);
    }
    else
    {
        rs = drms_clone_records_nosums(ors, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT), DRMS_SHARE_SEGMENTS, &status);
    }

    /* free ors - not needed after this point. */
    drms_close_records(ors, DRMS_FREE_RECORD);

   if (rs->n != nrecs || status)
   {
      if (query) { free(query); query = NULL; }
      DIE("failed to clone records from query");
   }
   } /* end clone record */

  /* at this point the records are ready for new keyword values and/or files */
  /* not inside a loop here. */
  HContainer_t **segfilekeys = NULL;
  HContainer_t *keylist = NULL;
  int noutsegs = 0;
    int nonlnksegs = 0;
    HIterator_t *seghit = NULL;

  for (irec = 0; irec<nrecs; irec++)
  {
      if (create_from_stdin)
        { // get next record params onto command line
        if (get_params_from_stdin() && found_from_stdin == 0)
          {
          drms_free_records(rs);
          rs = NULL;
          continue;
          }
        }
      char recordpath[DRMS_MAXPATHLEN];
      rec = rs->records[irec];
      noutsegs = hcon_size(&rec->segments);
      
      if (noutsegs > 0)
      {
          segfilekeys = malloc(sizeof(HContainer_t *) * noutsegs);
          memset(segfilekeys, 0, sizeof(HContainer_t *) * noutsegs);
          keylist = hcon_create(sizeof(int), sizeof(DRMS_MAXKEYNAMELEN), NULL, NULL, NULL, NULL, 0);
      }
      
      /* make sure record directory is staged and included in the new record IF we are going to write
       * to the record directory (if the user has supplied a filename on the cmd-line of a file to 
       * be ingested, or if the user has specified the -C flag, which means to copy segments files
       * when cloning the original record). */
      if (is_new_seg || force_copyseg)
      {
          drms_record_directory(rec, recordpath, 1);
      }
      
      /* We do NOT want to follow links, so don't use drms_segment_lookup...(). */
      while ((seg = drms_record_nextseg(rec, &seghit, 0)) != NULL)
      {        
          if (!seg->info->islink)
          {
              const char *filename = NULL;
              const char *segname = seg->info->name;
              
              nonlnksegs++;
              segfilekeys[seg->info->segnum] = NULL; /* initialize to NULL - FitsImport
                                                      * may override this. */
              filename = cmdparams_get_str(&cmdparams, segname, NULL);
              
              if (filename && *filename)
              {
                  /* check to see if generic segment(s) present */
                  if (seg->info->protocol == DRMS_GENERIC)
                  {
                      /* filename might contain a comma-separated list of files to import */
                      char *afile = NULL;
                      char *pch = NULL;
                      char *tmp = strdup(filename);
                      
                      if (tmp)
                      {
                          afile = tmp;
                          while ((pch = strchr(afile, ',')) != NULL)
                          {             
                              *pch = '\0';
                              
                              /* For generic protocol, there are no keyword values to import. */
                              if ((status = drms_segment_write_from_file(seg, afile)))
                              {
                                  if (query) { free(query); query = NULL; }
                                  free(tmp);
                                  tmp = NULL;
                                  DIE("segment name matches cmdline arg but file copy failed.\n");
                              }
                              
                              afile = pch + 1;
                          }
                          
                          /* handle last file in list */
                          if ((status = drms_segment_write_from_file(seg, afile)))
                          {
                              if (query) { free(query); query = NULL; }
                              free(tmp);
                              tmp = NULL;
                              DIE("segment name matches cmdline arg but file copy failed.\n");
                          }
                          
                          free(tmp);
                          tmp = NULL;
                      }
                  }
                  else if (seg->info->protocol == DRMS_FITS)
                  {
                      if (!ignoreHeader)
                      {
                          if (FitsImport(rec, seg, filename, segfilekeys, keylist) != 0)
                          {
                              char diebuf[256];
                             
                              snprintf(diebuf, sizeof(diebuf), 
                                       "File '%s' does not contain data compatible with segment '%s'.\n", 
                                       filename, seg->info->name);
                              if (query) { free(query); query = NULL; }
                              DIE(diebuf);
                          }
                      }
                  }
                  else
                  {
                      fprintf(stderr, "Unsupported file protocol '%s'.\n", drms_prot2str(seg->info->protocol));
                  }
              }
          }
      } /* foreach(seg) */
      
      if (seghit)
      {
          hiter_destroy(&seghit);
      }
      
      /* Resolve keyword value conflicts - iterate over keylist */
      if (nonlnksegs > 0)
      {
          WriteKeyValues(rec, noutsegs, keylist, segfilekeys);
      }
      
      /* Free segment files' keylists */
      if (segfilekeys)
      {
          for (isegment = 0; isegment < noutsegs; isegment++)
          { 
              if (segfilekeys[isegment])
              {
                  hcon_destroy(&segfilekeys[isegment]);
              }
          }
          
          free(segfilekeys);
      }
      /* Free keylist of unique keys from all segments. */
      if (keylist)
      {
          hcon_destroy(&keylist);
      }
      
      /* Loop through cmd-line arguments, checking for keyword names. If a keyword is encountered, set the keyword value 
       * in the record with the value from the cmd-line argument. */
      HIterator_t *last = NULL;
      const char *argname = NULL;
      DRMS_Value_t *keyval = NULL;
      CmdParams_Arg_t *arg = NULL;
      
      while ((arg = drms_cmdparams_getnext(&cmdparams, &last, &status)) != NULL)
      {
          if (status)
          {
              DIE("Problem examining cmd-line arguments.");
          }
          
          argname = cmdparams_get_argname(arg);
          
          if (argname)
          {
              key = drms_keyword_lookup(rec, argname, 0);
          }
          else
          {
              /* This is an unamed argument, and isn't relevant. */
              continue;
          }
          
          if (key)
          {
              keyname = argname; /* Use the cmd-line argument name since we'll be searching the arguments container
                                  * for an argument with this name. */
              keytype = drms_keyword_gettype(key);
              
              /* A valid DRMS-keyword name was provided on the cmd-line. */
              if (drms_keyword_isprime(key))
              {
                  /* is prime, so DIE unless just made in create mode */
                  if (!create && !create_from_stdin)
                  {
                      if (query) { free(query); query = NULL; }
                      DIE("Attempt to change prime key - not allowed");
                  }
                  else
                  {
                      keyval = drms_cmdparams_get(&cmdparams, keyname, keytype, &status);
                      
                      if (status != DRMS_SUCCESS)
                      {
                          char msg[128];
                          snprintf(msg, sizeof(msg), "Cannot get cmd-line argument %s.", keyname);
                          DIE(msg);
                      }
                      
                      key_anyval = keyval->value;
                      status = drms_setkey(rec, keyname, keytype, &key_anyval);
                      
                      if (keytype == DRMS_TYPE_STRING)
                      {
                          free(key_anyval.string_val);
                          key_anyval.string_val = NULL;
                      } 
                  }
              }
              else if (drms_keyword_getrecscope(key) == kRecScopeType_Constant)
              { // This is not supposed to be needed according to comments at start, but constant keywords were not ignored.
                  ; 
              }
              else
              {
                  keyval = drms_cmdparams_get(&cmdparams, keyname, keytype, &status);
                  
                  if (status != DRMS_SUCCESS)
                  {
                      char msg[128];
                      snprintf(msg, sizeof(msg), "Cannot get cmd-line argument %s.", keyname);
                      DIE(msg);
                  }
                  
                  if (!keyname || !keytype || !keyval)
                  {
                      char buffer[DRMS_MAXKEYNAMELEN];
                      
                      snprintf(buffer, sizeof(buffer), "Unable to get cmd-line value for keyword %s.", keyname);
                      status = DRMS_ERROR_INVALIDDATA;
                      DIE(buffer);
                  }
                  
                  key_anyval = keyval->value;
                  
                  /* Check for specialness - HISTORY and COMMENT keywords. */
                  if (strcasecmp("history", keyname) == 0)
                  {
                      if (keytype == DRMS_TYPE_STRING)
                      {
                          if (drms_appendhistory(rec, key_anyval.string_val, 1))
                          {
                              DIE("Unable to append to HISTORY keyword.");
                          }
                      }
                      else
                      {
                          DIE("Unable to append to HISTORY keyword - unexpected keyword type.");
                      }
                  }
                  else if (strcasecmp("comment", keyname) == 0)
                  {
                      if (keytype == DRMS_TYPE_STRING)
                      {
                          if (drms_appendcomment(rec, key_anyval.string_val, 1))
                          {
                              DIE("Unable to append to COMMENT keyword.");
                          }
                      }
                      else
                      {
                          DIE("Unable to append to COMMENT keyword - unexpected keyword type.");
                      }
                  }
                  else
                  {
                      status = drms_setkey(rec, keyname, keytype, &key_anyval);
                  }
                  
                  if (keytype == DRMS_TYPE_STRING)
                  {
                      free(key_anyval.string_val);
                      key_anyval.string_val = NULL;
                  }
               
                  free(keyval);
                  keyval = NULL;

                  if (status)
                  {
                      if (query) { free(query); query = NULL; }
                      DIE("keyval bad, cant set  key val with keyname");
                  }
              }
          }
      }

      if (last)
      {
         hiter_destroy(&last);
      }
    
      /* Ensure that all prime keys are set (if the record was created, not cloned). */
      if (create || create_from_stdin)
      {
          keyval = malloc(sizeof(DRMS_Value_t));
          
          if (!keyval)
          {
              free(keyval);
              DIE("Out of memory.");
          }
          
          for (iprime = 0; iprime < nprime; iprime++)
          {    
              *keyval = drms_getkey_p(rec, prime_names[iprime], &status);
              
              if (status != DRMS_SUCCESS)
              {
                  free(keyval);
                  DIE("Problem getting keyword value.");
              }
              
              if (drms_ismissing(keyval))
              {
                  free(keyval);
                  DIE("some prime key not specified on command line");
              }
          }
          
          free(keyval);
      }
      
      /* Set any links for cmd-line link arguments. */
      if (CreateLinks(rec, &rec->links))
      {
          DIE("Unable to create link.");
      }
      
      if (create_from_stdin && found_from_stdin)
        { // close current record and create next one
fprintf(stderr,"closing record\n");
        status = drms_close_records(rs, DRMS_INSERT_RECORD);
        if (status)
          {
          fprintf(stderr, "XXX close error from stdin record, rs=%p, nrecs=%d",rs,(rs ? rs->n : -1));
          DIE("close failure");
          }
        rs = drms_create_records(drms_env, 1, query, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT), &status);
        irec = -1;
        found_from_stdin = 0;
        }
  } /* foreach(rec) */

  if (pkeys)
  {
     drms_series_destroypkeyarray(&pkeys, nprime);
  }

  if (rs)
    status = drms_close_records(rs, DRMS_INSERT_RECORD);
  if (status)
  {
     if (query) { free(query); query = NULL; }
     DIE("close failure");
  }

  if (query) 
  { 
     free(query); 
     query = NULL; 
  }

  return 0;
}

/* cmdparams_get_type - get a keyword value from command line as a specified type */

DRMS_Type_Value_t cmdparams_get_type(CmdParams_t *cmdparams, char *keyname, DRMS_Type_t keytype, int *status)
{
DRMS_Type_Value_t value;
switch (keytype) 
  {
  case DRMS_TYPE_CHAR:
    value.char_val = cmdparams_get_int8(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_SHORT:
    value.short_val = cmdparams_get_int16(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_INT:
    value.int_val = cmdparams_get_int32(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_LONGLONG:
    value.longlong_val = cmdparams_get_int64(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_FLOAT:
    value.float_val = cmdparams_get_float(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_DOUBLE:
    value.double_val = cmdparams_get_double(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_TIME:
    value.time_val = cmdparams_get_time(cmdparams, keyname, status);
    break;
  case DRMS_TYPE_STRING:
    value.string_val = strdup(cmdparams_get_str(cmdparams, keyname, status));
    break;
  default:
    *status=1;
    break;
  }
return value;
}

