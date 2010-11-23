/* set_info - set keyword information and/or file path for given recordset. 
 *  This is really just set_keys modified to handle data segments of
 *  FITS protocol.
 *  --Art
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
/**
\defgroup set_keys set_info - Add or update records with new keyword or segment filename values
@ingroup drms_util

\par Synopsis:

\code
set_info [-h] 
set_info [-ctv] [JSOC_FLAGS] ds=<seriesname> [<keyword>=<value>]... [<segment>=<filename>]...
set_info [-Cmtv] [JSOC_FLAGS] ds=<record_set> [<keyword>=<value>]... [<segment>=<filename>]...
set_info_soc {options as above}
\endcode

\details

Sets keyword values for a record in a DRMS dataseries.  \b set_info runs in one of two modes.
\li If the "create" \c -c flag is present a new record will be created.  In this case ALL of the prime keys
must be present in \e keyword=value pairs to specify the locator keywords for the new record.  Other keywords
and or segments specifications are usually also present.
\li An existing record_set may be specified and a new version of those record will be made with the specified keywords
given new values. Usually, almost always, only a single record is updated in one call of set_info since
only one set of keyword values can be provided and all records modified will be given the same new
values for those keywords.  If multiple records are to be changed the \c -m flag is required.
\b set_info will then clone the records specified by the record-set filter.
In the update mode, NONE of the prime keys are allowed in keyword=value pairs on the command line (except
of course as a where clause as part of the recordset specification)

For each keyword specified in a \a keyword=value pair on the command line, \b set_info will set the values for all these clones' keywords to \a value.

If a segment in the series is if type \e generic then set_info can be used to store a given file in
the SUMS directory associated with the record.
In this case, for each \c segment=filename parameter the file found at filename will be copied into the
record directory and the segment filename will be saved in the segment descriptor in DRMS.

In normal usage if NO segments are specified, any segments in the original version of an updated record will
be shared with the new record.  This allows keyword metadata to be updated without making unnecessary copies
of file data in SUMS.  However if any segment file is to be updated, a new record directory must be allocated
and any other segment's files will be copied (cloned) to the new directory and associated with the new record.
If the \a -C "clone" flag is present then a new record directory will be allocated even if none of the
segments are to be updated.  This usually only makes sense in a JSOC session script where the same record
will be incrementally updated by multiple modules and set_info is invoked as set_info_sock.  

Only one instance of each keyword name or segment name is allowed.

\par Options:

\par Flags:

\li \c -c: Create a new record
\li \c -C: Clone an existing record
\li \c -h: Print usage message and exit
\li \c -l: Indicates that the keyword names in the key=value cmd-line arguments are all lower case
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
  {ARG_FLAG, "c", "0", "Create new record(s) if needed"},
  {ARG_FLAG, "C", "0", "Force cloning of needed records to be DRMS_COPY_SEGMENT mode"},
  {ARG_FLAG, "m", "0", "allow multiple records to be updated"},
  {ARG_FLAG, "t", "0", "create any needed records as DRMS_TRANSIENT, default is DRMS_PERMANENT"},
  {ARG_FLAG, "l", NULL, "keyword names on cmd-line specified in all lower case (and may not match the case of the keyword names stored in DRMS)"},
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
    printf("set_info {-c}|{-m} {-C} {-t} {-h} {-v}  "
	"ds=<recordset query> {keyword=value} ... \n"
	"  -h: print this message\n"
	"  -c: create - allow creation of new record\n"
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

/* keys is an array of HContainer_t * */
static int FitsImport(DRMS_Segment_t *seg, const char *file, HContainer_t **keys, HContainer_t *keylist)
{
   int err = 0;
   int drmsstat = DRMS_SUCCESS;
   DRMS_Array_t *data = NULL;
   HContainer_t *keysint = NULL;
   HIterator_t hit;
   const char *intstr = NULL;
   DRMS_Keyword_t *akey = NULL;

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

         keys[seg->info->segnum] = keysint;

         /* Iterate through keys and add unique values to keylist */
         hiter_new_sort(&hit, keysint, drms_keyword_ranksort);
         while ((akey = hiter_extgetnext(&hit, &intstr)) != NULL)
         {
            if (!hcon_member(keylist, intstr))
            {
               hcon_insert(keylist, intstr, (const void *)&akey->info->rank); 
            }
         }

         hiter_free(&hit);
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
            /* Check to see if keyword in output series is per-segment or not (if the series
             * has a per-segment keyword, then you must specify an index in the keyword name). */
            snprintf(query, sizeof(query), "SELECT * FROM %s.drms_keyword WHERE keywordname ILIKE '%s' AND persegment & 1 = 1", ns, kbuf);

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

      free(ns);
      hiter_free(&keyhit);
   }

   return err;
}

/* Module main function. */
int DoIt(void)
{
  int status = 0;
  int multiple = 0;
  int create = 0;
  int lckeys = 0;
  int nrecs, irec;
  int force_transient;
  int force_copyseg;
  char *keyname;
  char *lckeyname = NULL;
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
  HIterator_t key_hit;
  int nprime, iprime;
  int nsegments, isegment;
  int is_new_seg = 0;

  if (nice_intro(0))
    return(0);

/* Get command line arguments */
   query = strdup(cmdparams_get_str(&cmdparams, "ds", NULL));

   force_copyseg = cmdparams_get_int(&cmdparams, "C", NULL) != 0;
   force_transient = cmdparams_get_int(&cmdparams, "t", NULL) != 0;

   multiple = cmdparams_get_int(&cmdparams, "m", NULL) != 0;
   create = cmdparams_get_int(&cmdparams, "c", NULL) != 0;
   lckeys = cmdparams_isflagset(&cmdparams, "l");
   if (multiple && create)
   {
      if (query) { free(query); query = NULL; }
      DIE("-c and -m not compatible");
   }
  p = index(query,'[');
  if (!p && !create)
  {
     if (query) { free(query); query = NULL; }
     DIE ("must be in create mode if no record spec given");
  }
  if (p && create)
  {
     if (query) { free(query); query = NULL; }
     DIE("can only create new record, record set not allowed");
  }
  /* Now can test on create and multiple for program control */

  if (verbose)
  {
     /* Print something to identify what series is being modified */
     printf("set_info() %s, query is %s.\n", create ? "creating record" : "updating record", query);
  }

  if (create)
    {
    if (verbose)printf("Make new record\n");
    rs = drms_create_records(drms_env, 1, query, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT), &status);
    if (status)
    {
       if (query) { free(query); query = NULL; }
       DIE("cant create records from in given series");
    }
    nrecs = 1;
    rec = rs->records[0];
    pkeys = drms_series_createpkeyarray(drms_env, 
					rec->seriesinfo->seriesname,
					&nprime, 
					&status);

    for (iprime = 0; iprime < nprime; iprime++)
        {
        keyname = pkeys[iprime];
	strcpy(prime_names[iprime], keyname);
        key = drms_keyword_lookup(rec, keyname, 1);
        keytype = key->info->type;
	if (status)
        {
           if (query) { free(query); query = NULL; }
           DIE("series bad, prime key missing");
        }

        if (lckeys)
           {
           lckeyname = strdup(keyname);
           strtolower(lckeyname);
           keyname = lckeyname;
           }

        if (!cmdparams_exists(&cmdparams, keyname))
        {
           if (query) { free(query); query = NULL; }
           DIE("some prime key not specified on command line");
        }
        key_anyval = cmdparams_get_type(&cmdparams, keyname, keytype, &status); 
	status = drms_setkey(rec, keyname, keytype, &key_anyval); 
	if (status)
        {
           if (query) { free(query); query = NULL; }
           DIE("keyval bad, cant set prime key val with keyname");
        }

        if (lckeys && lckeyname)
           {
           free(lckeyname);
           lckeyname = NULL;
           }
	}

    /* now record exists with prime keys set. */
    }
  else
   {
      /* We are not creating a new record, but instead 'updating' an existing one. */
   DRMS_RecordSet_t *ors;
   if (verbose)printf("Clone record for update\n");
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
    /* if a segment is present matching the name of a keyword=filename then copy instead of share */

    /* find out if the caller is trying to ingest a file - if so, set is_new_seg --> 1 */
    is_new_seg = 0;
    nsegments = hcon_size(&rec->segments);
    for (isegment=0; isegment<nsegments; isegment++)
      {
	   seg = drms_segment_lookupnum(rec, isegment);
	   
	   if (seg->info->protocol == DRMS_GENERIC || seg->info->protocol == DRMS_FITS)
	   {
                char *segname = NULL;
                const char *filename = NULL;
		segname = seg->info->name;
		filename = cmdparams_get_str(&cmdparams, segname, NULL);
		if (filename && *filename)
		{
		  is_new_seg = 1;
		  break; /* Stop iterating if we found a segment file. */
		}
	   }
      }
   rs = drms_clone_records(ors, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT),
           ((is_new_seg||force_copyseg) ? DRMS_COPY_SEGMENTS : DRMS_SHARE_SEGMENTS), &status);
   if (rs->n != nrecs || status)
   {
      if (query) { free(query); query = NULL; }
      DIE("failed to clone records from query");
   }
   }

  /* at this point the records are ready for new keyword values and/or files */
  HContainer_t **segfilekeys = malloc(sizeof(HContainer_t *) * nsegments);

  /* not really going to store anything in hash container - just using hash table to 
   * quickly find out if a key has been previously seen. */
  HContainer_t *keylist = hcon_create(sizeof(int), sizeof(DRMS_MAXKEYNAMELEN), NULL, NULL, NULL, NULL, 0);
  memset(keylist, 0, sizeof(HContainer_t *) * nsegments);
  for (irec = 0; irec<nrecs; irec++)
    {
    char recordpath[DRMS_MAXPATHLEN];
    rec = rs->records[irec];

    /* make sure record directory is staged and included in the new record */
    drms_record_directory(rec, recordpath, 1);
    /* insert new generic segment files found on command line */
    nsegments = hcon_size(&rec->segments);
    for (isegment=0; isegment<nsegments; isegment++)
    { 
	 seg = drms_segment_lookupnum(rec, isegment);
	 const char *filename = NULL;
	 char *segname = seg->info->name;
         segfilekeys[seg->info->segnum] = NULL; /* initialize to NULL - FitsImport
                                                 * may override this. */
	 filename = cmdparams_get_str(&cmdparams, segname, NULL);

	 if (filename && *filename)
	 {
	      /* check to see if generic segment(s) present */
	      if (seg->info->protocol == DRMS_GENERIC)
	      {
                 /* For generic protocol, there are no keyword values to import. */
		   if ((status = drms_segment_write_from_file(seg, filename)))
                   {
                      if (query) { free(query); query = NULL; }
                      DIE("segment name matches cmdline arg but file copy failed.\n");
                   }
	      }
              else if (seg->info->protocol == DRMS_FITS)
              {
                 if (FitsImport(seg, filename, segfilekeys, keylist) != 0)
                 {
                    char diebuf[256];

                    snprintf(diebuf, sizeof(diebuf), 
                             "File '%s' does not contain data compatible with segment '%s'.\n", 
                             filename, seg->info->name);
                    if (query) { free(query); query = NULL; }
                    DIE(diebuf);
                 }
              }
              else
              {
                 fprintf(stderr, "Unsupported file protocol '%s'.\n", drms_prot2str(seg->info->protocol));
              }
	 }
    } /* foreach(seg) */

    /* Resolve keyword value conflicts - iterate over keylist */
    WriteKeyValues(rec, nsegments, keylist, segfilekeys);

    /* Free segment files' keylists */
    for (isegment = 0; isegment < nsegments; isegment++)
    { 
       if (segfilekeys[isegment])
       {
          hcon_destroy(&segfilekeys[isegment]);
       }
    }
    
    hiter_new(&key_hit, &rec->keywords);
    while( (key = (DRMS_Keyword_t *)hiter_getnext(&key_hit)) )
      {
      int is_prime = 0;
      keyname = key->info->name;
      keytype = key->info->type;

      if (lckeys)
         {
         lckeyname = strdup(keyname);
         strtolower(lckeyname);
         keyname = lckeyname;
         }

      /* look to see if given on command line */
      if (cmdparams_exists(&cmdparams, keyname))
        {
        /* check to see if prime key */
        for (is_prime=0, iprime = 0; iprime < nprime; iprime++)
          if (strcasecmp(keyname,  prime_names[iprime]) == 0)
            is_prime = 1;
        if (is_prime)
          { /* is prime, so DIE unless just made in create mode */
          if (!create)
          {
             if (query) { free(query); query = NULL; }
             DIE("Attempt to change prime key - not allowed");
          }
          }
        else
          { /* not prime, so update this value */
	  key_anyval = cmdparams_get_type(&cmdparams, keyname, keytype, &status);
	  status = drms_setkey(rec, keyname, keytype, &key_anyval);
           if (status)
           {
              if (query) { free(query); query = NULL; }
              DIE("keyval bad, cant set  key val with keyname");
           }
          }
	}

      if (lckeys && lckeyname)
         {
         free(lckeyname);
         lckeyname = NULL;
         }
      }

    hiter_free(&key_hit);

    } /* foreach(rec) */

  if (pkeys)
  {
     drms_series_destroypkeyarray(&pkeys, nprime);
  }

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

