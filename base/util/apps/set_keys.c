/* set_keys - set keyword information and/or file path for given recordset. */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"

/**
\defgroup set_keys set_keys - for each existing record specified, modifies the keyword values or segment information, or create new records
@ingroup drms_util

Modify the keyword values of a DRMS record or create a new record with
specified keyword values.

set_keys modifies keyword values and/or inserts generic files into a
DRMS record(s).

\par Synopsis:

\code
set_keys [-chmvDRIVER_FLAGS] ds=<record_set> [<keyword1>=<value1>]... [<segment1>=<file1>]...
\endcode

\par Example to modify a keyword value:
\code
set_keys ds=su_arta.TestStoreFile[file=dsds_data.fits][sel=January] note=fred
\endcode

\par Example to create a new record and specify keyword values:
\code
set_keys -c ds=su_arta.TestStoreFile file=data.txt sel=February file_seg=/home/arta/febdata.txt
\endcode

\par Flags:
\c -c: Create a new record
\par
\c -h: Print usage message and exit
\par
\c -m: Modify the keywords of multiple records. The \c -m flag should be
       used with caution.  A typo could damage many records. Do not use
       \c -m unless you are sure the query will specify ONLY the records
       you want to modify.
\par
\c -v: Verbose - noisy.

The \c -c and \c -m flags cannot be used simultaneously.

\par Driver flags: 
\ref jsoc_main

\param record_set

A series name followed by an optional record-set specification
(i.e., \a seriesname[\a filter]). If no record-set filter is
specified, \ref set_keys requires the \a -c flag, and it creates a new
record. All of the prime keywords and values must be specified as \a
keyword=value pairs.  If a record-set filter IS specified, \ref
set_keys requires the \a -c flag to be unset.  If \a record_set
resolves to more than one record, then the \a -m flag must be
set. \ref set_keys will then clone the records specified by the
record-set filter. For each keyword specified in a \a keyword=value
pair on the command line, \ref set_keys will set the values for all
these clones' keywords to \a value.

\param valueN
The new keyword values to be used to create a new or modify an
existing record.

\param fileN

If modifying a record(s) and \a segmentN is a generic series segment,
then first copy the segment file to the cloned record(s)' segment storage,
then replace the copied file with \a fileN. Otherwise, a cloned record
and its progenitor share the original segment file.

\bug 
At  present updates of segment files fail.  Please use only with the \a -c
flag and prime keys  when  inserting  files  into  generic  type  data
segments.

@{
*/
ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "ds", "Not Specified", "Series name with optional record spec"},
  {ARG_FLAG, "h", "0", "Print usage message and quit"},
  {ARG_FLAG, "c", "0", "Create new record(s) if needed"},
  {ARG_FLAG, "C", "0", "Force cloning of needed records to be DRMS_COPY_SEGMENT mode"},
  {ARG_FLAG, "m", "0", "allow multiple records to be updated"},
  {ARG_FLAG, "t", "0", "create any needed records as DRMS_TRANSIENT, default is DRMS_PERMANENT"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_END}
};

char *module_name = "set_keys";

int verbose = 0;

/** @}*/
int nice_intro(int help)
  {
  int usage = cmdparams_get_int(&cmdparams, "h", NULL) != 0;
  verbose = cmdparams_get_int(&cmdparams, "v", NULL) != 0;
  if (usage || help)
    {
    printf("set_keys {-c}|{-m} {-C} {-t} {-h} {-v}  "
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

#define DIE(msg) {fprintf(stderr,"$$$$ set_keys error: %s\n",msg); return 1;}

DRMS_Type_Value_t cmdparams_get_type(CmdParams_t *cmdparams, char *keyname, DRMS_Type_t keytype, int *status);

/* Module main function. */
int DoIt(void)
{
  int status = 0;
  int multiple = 0;
  int create = 0;
  int nrecs, irec;
  int force_transient;
  int force_copyseg;
  char *keyname;
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
   if (multiple && create)
	DIE("-c and -m not compatible");
  p = index(query,'[');
  if (!p && !create)
	DIE ("must be in create mode if no record spec given");
  if (p && create)
	DIE("can only create new record, record set not allowed");
  /* Now can test on create and multiple for program control */

  if (create)
    {
    if (verbose)printf("Make new record\n");
    rs = drms_create_records(drms_env, 1, query, (force_transient ? DRMS_TRANSIENT : DRMS_PERMANENT), &status);
    if (status)
	DIE("cant create records from in given series");
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
		DIE("series bad, prime key missing");
        if (!cmdparams_exists(&cmdparams, keyname))
	  DIE("some prime key not specified on command line");
        key_anyval = cmdparams_get_type(&cmdparams, keyname, keytype, &status); 
	status = drms_setkey(rec, keyname, keytype, &key_anyval); 
	if (status)
		DIE("keyval bad, cant set prime key val with keyname");
	}

    /* now record exists with prime keys set. */
    }
  else
   {
   DRMS_RecordSet_t *ors;
   if (verbose)printf("Clone record for update\n");
   ors = drms_open_records(drms_env, query, &status);
   if (status)
	DIE("cant open recordset query");
    nrecs = ors->n;
   if (nrecs > 1 && !multiple)
	DIE("multiple records not expected");
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
    is_new_seg = 0;
    nsegments = hcon_size(&rec->segments);
    for (isegment=0; isegment<nsegments; isegment++)
      {
	   seg = drms_segment_lookupnum(rec, isegment);
	   
	   if (seg->info->protocol == DRMS_GENERIC)
	   {
		char *segname, *filename;
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
	DIE("failed to clone records from query");
   }

  /* at this point the records are ready for new keyword values and/or files */
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
	 char *filename = NULL;
	 char *segname = seg->info->name;
	 filename = cmdparams_get_str(&cmdparams, segname, NULL);

	 if (filename && *filename)
	 {
	      /* check to see if generic segment(s) present */
	      if (seg->info->protocol == DRMS_GENERIC)
	      {
		   if ((status = drms_segment_write_from_file(seg, filename)))
		     DIE("segment name matches cmdline arg but file copy failed.\n");
	      }
	 }
    } /* foreach(seg) */
    
    hiter_new(&key_hit, &rec->keywords);
    while( (key = (DRMS_Keyword_t *)hiter_getnext(&key_hit)) )
      {
      int is_prime = 0;
      keyname = key->info->name;
      keytype = key->info->type;

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
	    DIE("Attempt to change prime key - not allowed");
          }
        else
          { /* not prime, so update this value */
	  key_anyval = cmdparams_get_type(&cmdparams, keyname, keytype, &status);
	  status = drms_setkey(rec, keyname, keytype, &key_anyval);
           if (status)
             DIE("keyval bad, cant set  key val with keyname");
          }
	}
      }
    } /* foreach(rec) */

  if (pkeys)
  {
     drms_series_destroypkeyarray(&pkeys, nprime);
  }

  status = drms_close_records(rs, DRMS_INSERT_RECORD);
  if (status)
	DIE("close failure");
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

