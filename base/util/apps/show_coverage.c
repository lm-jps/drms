#include "jsoc_main.h"
#include "drms.h"
#include "atoinc.h"

/*
 *
 * show_coverage - Prints data completeness information for selected range of given series
 *
 */

/**
\defgroup show_coverage show_coverage - Examine data completeness of series of certain types
@ingroup drms_util

\par Synopsis:
show_coverage {ds=}<seriesname> [-h]

show_coverage {ds=}<seriesname> [-igqostmv] [low=<starttime>] [high=<stoptime>] [block=<blocklength>] [key=<pkey>] [mask=<badbits>] [ignore=<ignorebits>] [<other>=<value>...]

show_coverage_sock {same options as above}
\endcode

\details

\b Show_coverage finds the record completeness map for a given series over an interval of a single prime-key.
Since it needs a way to know if any given record is expected the program
only works for series with an integer or slotted prime key.
It will fail to be helpful for series that do not expect each index value of
a slotted series to be present (such as lev0 HK data for HMI and AIA.) 

The operation is to scan the series for all possible records between the \a low and \a high
limits or the present first and last records if \a low or \a high are absent.  For each
record found, the "quality" of the data will be assessed.  If a keyword named "QUALITY" is present
its value will be tested and the record will be labeled "MISSING" if QUALITY is negative.  If both
a QUALITY keyword is present in the data and the "mask" argument is present, then that mask will
be "anded" with QUALITY to determine records to be marked as MISSING.  If
no QUALITY keyword is found but a keyword named "DATAVALS" is present then DATAVALS will
be tested and a record with DATAVALS == 0 will be labeled MISSING.  If the record is not
labeled MISSING it will be labeled "OK".  If no record is present for a record slot in the
range \a low to \a to high then that slot will be labeled unknown, or "UNK" for short.

The record completeness summary is ordered by a single prime-key.  If no \a key parameter is present the first prime-key
of integer or slotted type will be used.  If the series is structured with multiple prime-keys and
the completeness of a subset specified by additional prime-keys is desired, then those prime keys
and selected values may be provided as additional keyword=value pairs.

\par Options:

\par Flags:

\li \c -g: no_gone - treat all GONE determinations as UNK
\li \c -h: help - print usage information and exit
\li \c -i: index - print interval start as index values for slotted ordering prime-key
\li \c -q: quiet - do not print header overfiew information
\li \c -s: summary - print totals of each kind, OK, MISS, UNK, GONE
\li \c -t: times - print start and end time of each segment of OK, MISS, UNK, and optionally GONE
\li \c -m: no_miss - Treat all MISS determinations as UNK
\li \c -o: online - check existance of segment files for all OK records
\li \c -v: verbose - print processing status

\par Parameters:

\li \c block=<blocklength> - optional blocking interval for summary table instead of detailed table.
\li \c {ds=}<seriesname> - required parameter specified the series to examine, the "ds=" is optional.
\li \c high=<stoptime> - optional last value of ordering prime key to use, default to last record in series (as [$]).
\li \c key=<pkey> - optional prime key name to use, default is first integer or slotted prime-key
\li \c low=<starttime> - optional first value of ordering prime key to use, default to first record in series (as [^]).
\li \c mask=<badbits> - optional usually Hex number with bits to test against QUALITY to label record as MISS.
\li \c ignore=<ignore_bits> - optional usually Hex number with bits to exclude in tests against QUALITY to label record as MISS.
\li \c other=<value>... - optional additional query clauses to restrict the records used in the survey.

\par JSOC flags:
\ref jsoc_main

\par Usage:

The prime-key specified by \a pkey or the first integer or slotted primekey will be used to order the
completeness survey and will be referred to as the "ordering prime-key".
The ordering prime-key may be of type TIME or other floating slotted keywords, or of an integer type.  In the
discussions here, the word "time" will be used to refer to the ordering keyword even if it is of some
other type. 

If the \a -o flag (for online) is present then each record labeled OK will be tested to verify
that any storage-unit that has been assigned to that record still exists either online or on tape
in SUMS.  If there once was a storage unit but no longer is (due to expired retention time of a non-archived
series) the record will be labeled "GONE". Note that this can be an expensive test to make on large series since it requires
calls to SUMS and is noticably slower than without the -o flag.  Please ues the \a -o flag only on the selected ranges of
a series where needed.  If the -g flag is set, -o will be forced to be set and any records with storage_units not online
will be labeled as UNK.

Records are identified as MISS if QUALITY is < 0 or if the mask parameter is provided, only if one or
more bits set in mask are also set in QUALITY.  If ignore is provided, the if any bits in ignore are
also present in QUALITY the record not used in the comparison with mask. A -m flag will result in the MISS
category being added to the UNK category.  This is used to simplify finding which records might need to
be recomputed.

A summary will be printed if the -s flag is present.

The "other" argument may be used to add where clauses to the query for records.  The "other" clauses
are not used for determining the first and last slots, but only for the existence information.
As such, any records that are excluded by matching the "other" clauses will be counted in the UNK category.

After all specified records have been examined a table of the resulting completeness information is
printed.  There are two formats for this table.  The default format is a list of contiguous segments
with the same record label, OK, MISS, UNK, or GONE.  For each contiguous segment one line of
information will be printed containing the label, the start "time", and the count of records in
the contiguous same-label interval.  The time printed will be the prime key value of the first
record in the interval.  If the \a -t flag is present then the first and last time of each contiguous
segment will be printed as well as the count.

If the \a block parameter is specified, the alternate table format will be used.  In the blocked case
the records are grouped in intervals of length <blocklength> and a summary line is printed for each
block.  The first block is aligned with the first record time (either first record in series or \a low).
The summary printed includes the start time of the block, the number of records in the block labeled
OK, MISS, UNK, or GONE (if \a -o is present).  If the ordering prime-key is of type TIME then the blocking interval,
<blocklength> may have suffixes to
specify time intervals such as s=seconds, d=day, h=hour, etc. as recognized by atoinc(3).

If the \a -i flag is present, the start times in the printed table will be the index number rather
than the slot label.  E.g. if the prime key used for the completeness survey is T_REC then instead
of printing the time T_REC, the value of T_REC_index will be printed.

A header will be printed before the completeness table.  If the \a -q (quiet) flag is present the
header will not be printed.

\par Examples:

\b Example 1:
To show the coverage in the first MDI Dynamics run:
\code
  show_coverage ds=mdi.fd_V_lev18 low=1996.05.23_22_TAI high=1996.07.24_04:17_TAI
\endcode
Shows a lot of little gaps in complete dynamics interval.

\b Example 2:
To show the summary of records in a range of data, such as the above MDI dynamics run:
\code
  show_coverage ds=mdi.fd_V_lev18 low=1996.05.23_22_TAI high=1996.07.24_04:17_TAI block=1000d
\endcode
Here block is set to a large number to gather all the information into a single line.
Simple math then shows the data is actually 96.6% complete.

\par Special Cases:

Some specific code is included to handle HMI and AIA special cases.

\b Bad FSN:
Both HMI and AIA can produce erroneous large values for FSN (Frame Serial Number) due to
hardware issues onboard.  To help in the case where the user does not specify a "high" limit
for analysis, and where the first 7 characters in the seriesname are "hmi.lev" or "aia.lev" then
the query for series high value will add the clause that FSN < 0X1C000000.  There are some
cases with an erroneous FSN lower than this, but no easy test.

\b FSN and times:
Some series, e.g. hmi.cosmic_rays, have both T_OBS and FSN as prime keys but are not slotted
on time.  For this case as well as the general case of lev0 and lev1 data or other series where
a T_OBS keyword exists and is not the selected key for analysis, and the user wishes to limit
the analysis based on time instead of the integer key (e.g. FSN) then "high" and "low" may be
expressed as a time if and only if the time string contains at least one "_" char.
If the seriesname starts "hmi.lev" or "aia.lev" then the appropriate lev0 series will be used
to convert times to FSN.  Note that this will fail if the low time is after the end of the data
or if the high time requested is before the beginning of the mission.

b Example 3:
To show the summary of records in a range of time where the prime key is FSN, such as hmi.lev0a:
\code
  show_coverage hmi.lev0a low=2010.05.01 high=2010.06.01 block=46080 key=FSN
\endcode

\bug
\b Limitation:
Since DRMS is queried using \ref drms_record_getvector the QUALITY keyword must be an integer type
to be used.  Only the single prime key value, either QUALITY or DATAVALS, and possibly sunum are
used so all must be convertible to long long by PostgreSQL.

\b Efficiency:
Although records that are not OK need not be queried in SUMS for online status, they are and
that information is ignored.  Some waste effort here, but for SDO should be very small.

\sa
show_info

*/

#define NOT_SPECIFIED "NOT_SPECIFIED"

#define DATA_OK ('\0')
#define DATA_MISS ('\1')
#define DATA_UNK ('\2')
#define DATA_GONE ('\3')


char primestr[100];

char *primevalstr(TIME prime, DRMS_Type_t type, char *unit, char *format)
  {
  if (type==DRMS_TYPE_TIME)
    sprint_time(primestr, prime, unit, atoi(format));
  else
    sprintf(primestr, (type <= DRMS_TYPE_LONGLONG ? "%.0f" : "%f"), prime);
  return(primestr);
  }

void printprime(FILE *fp, TIME prime, DRMS_Type_t type, char *unit, char *format)
  {
  fprintf(fp, primevalstr(prime, type, unit, format));
  }

ModuleArgs_t module_args[] =
{ 
    {ARG_STRING, "ds", NOT_SPECIFIED,  "Input data series."},
    // {ARG_STRING, "coverage", NOT_SPECIFIED,  "Output coverage report. - not implemented"},
    {ARG_STRING, "block", NOT_SPECIFIED,  "interval for block summaries of the coverage."},
    {ARG_STRING, "low", NOT_SPECIFIED, "Low limit for coverage map."},
    {ARG_STRING, "high", NOT_SPECIFIED, "High limit for coverage map."},
    {ARG_STRING, "key", NOT_SPECIFIED, "Prime key name to use, default is first prime"},
    {ARG_STRING, "other", NOT_SPECIFIED, "additional clauses for queries"},
    {ARG_INT, "mask", "0", "Mask to use for bits in QUALITY that will cause the record to be counted as MISS"},
    {ARG_INT, "ignore", "0", "Mask to use for bits to ignore in QUALITY tests with mask that will cause the record to be counted as MISS"},
    {ARG_INT, "chunk", "10000", "Number of records to request per get_vector query"},
    {ARG_FLAG, "o", "0", "Verify - verify that SU is available for records with data"},
    {ARG_FLAG, "m", "0", "no_miss -treat otherwise MISS records as UNK"},
    {ARG_FLAG, "g", "0", "no_gone -treat otherwise GONE records as UNK and treat otherwise UNK as MISS"},
    {ARG_FLAG, "s", "0", "Summary - Print the total number of records in each category: OK, MISS, UNK, GONE"},
    {ARG_FLAG, "t", "0", "times - Print the first and last time of each segment of OK, MISS, UNK, GONE"},
    {ARG_FLAG, "i", "0", "Index - Print index values instead of prime slot values"},
    {ARG_FLAG, "q", "0", "Quiet - omit series header info"},
    {ARG_FLAG, "v", "0", "Verbose - Print processing status"},
    {ARG_FLAG, "h", "0", "Help - Print usage and exit"},
    {ARG_END}
};

#define DIE(msg) {fprintf(stderr,"%s\n",msg);exit(1);}

char *module_name = "show_coverage";

int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\nshow_coverage [-hiqv] "
        "ds=<seriesname> {key=<pkey>} {low=<starttime>} <high=<stoptime>}\n"
        "      {key=<prime-key>} {block=<blocklength>} {<other>=<pkeylist>}.\n"
        "  -h: help - show this message then exit\n"
        "  -i: list index values vs slot values\n"
        "  -q: quiet, do not print header\n"
        "  -g: no_gone -treat otherwise GONE records as UNK and otherwise UNK records as MISS\n"
        "  -m: no_miss -treat otherwise MISS records as UNK\n"
        "  -s: Summary - Print the total number of records in each category: OK, MISS, UNK, GONE\n"
        "  -t: Summary - Print first and last time of each segment of OK, MISS, UNK, GONE\n"
        "  -o: online - check with SUMS for expired data\n"
        "  -v: verbose - Print processing status\n"
        "ds=<seriesname> - required\n"
        "key=<prime_key> - prime key to use if not the first available\n"
        "block=<blocklength> - interval span for summary data\n"
        "mask=<bad_bit_mask> - mask to be checked in QUALITY\n"
        "ignore=<ignore_bit_mask> - mask for bits to be ignored in QUALITY tests\n"
        "low=<starttime> - start of range for completeness survey, defaults to [^]\n"
        "high=<stoptime> - end of range for completeness summary, defaults to [$]\n"
        "<other>=<value> - optional additional filters to limit the survey\n"
        );
    return(1);
    }
  return (0);
  }

/* Module main function. */
int DoIt(void)
  {
  FILE *out;
  int status = 0;
  int slotted, blocked;
  long long lowslot, highslot, serieslowslot, serieshighslot;
  DRMS_RecordSet_t *rs;
  DRMS_Record_t *rec, *template;
  DRMS_Keyword_t *skey, *pkey;
  DRMS_Type_t ptype;
  char name[DRMS_MAXNAMELEN];
  int npkeys;
  char *pname;
  char *piname;
  char *punit;
  char *pformat;
  char *seriesname;
  TIME step, epoch, blocking;
  TIME series_low, series_high, low, high;
  char in[DRMS_MAXQUERYLEN];
  char *inbracket;
  const char *ds = cmdparams_get_str (&cmdparams, "ds", NULL);
  const char *report = cmdparams_get_str (&cmdparams, "coverage", NULL);
  const char *blockstr = cmdparams_get_str (&cmdparams, "block", NULL);
  const char *lowstr = cmdparams_get_str (&cmdparams, "low", NULL);
  const char *highstr = cmdparams_get_str (&cmdparams, "high", NULL);
  const char *skeyname = cmdparams_get_str (&cmdparams, "key", NULL);
  char *other = strdup((char *)cmdparams_get_str (&cmdparams, "other", NULL));
  int verify = cmdparams_get_int (&cmdparams, "o", NULL) != 0;
  int quiet = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  int verbose = cmdparams_get_int (&cmdparams, "v", NULL) != 0;
  int want_summary = cmdparams_get_int (&cmdparams, "s", NULL) != 0;
  int want_times = cmdparams_get_int (&cmdparams, "t", NULL) != 0;
  int no_miss = cmdparams_get_int (&cmdparams, "m", NULL) != 0;
  int no_gone = cmdparams_get_int (&cmdparams, "g", NULL) != 0;
  int useindex = cmdparams_get_int (&cmdparams, "i", NULL) != 0;
  int chunksize = cmdparams_get_int (&cmdparams, "chunk", NULL);
  uint32_t mask = (uint32_t)cmdparams_get_int64 (&cmdparams, "mask", NULL);
  uint32_t ignore = (uint32_t)cmdparams_get_int64 (&cmdparams, "ignore", NULL);
  char *map;
  long long islot, jslot, nslots, blockstep;
  int total_ok=0, total_miss=0, total_unk=0, total_gone=0;
  char *qualkey;
  int qualkind;
  int ikey;

  if (nice_intro()) return(0);

  /* check for minimum inputs */

  if (strcmp(ds, NOT_SPECIFIED) == 0 )
    {
    // ds arg was not provided, check for isolated arg
    if (cmdparams_numargs(&cmdparams) < 1 || !(ds=cmdparams_getarg (&cmdparams, 1)))
      DIE("### show_coverage: ds=<seriesname> parameter is required, must quit");
    }

  // out = fopen .... report ...
  out = stdout;

  // get optional other query clauses
  if (strcmp(other, NOT_SPECIFIED) == 0)
    other[0] = '\0';


  /* get series info, low and high, and prime key type, etc. */
  strcpy(in,ds);
  inbracket = index(in, '[');
  if (inbracket)
	  *inbracket = '\0';
  else
	  inbracket = in + strlen(in);
  template = drms_template_record (drms_env, in, &status);
  if (!template || status)
	DIE("Series not found or empty");

  npkeys = template->seriesinfo->pidx_num;
  if (npkeys < 1)
    DIE("Series has no prime keys");
  if (strcmp(skeyname, NOT_SPECIFIED) != 0)
	{
	for (ikey=0; ikey < npkeys; ikey++)
		{
		pkey = template->seriesinfo->pidx_keywords[ikey];
		if (pkey->info->recscope > 1)
			{
			skey = drms_keyword_slotfromindex(pkey);
			if (strcasecmp(skeyname, skey->info->name) == 0)
				break;
			}
		else
			if (strcasecmp(skeyname, pkey->info->name) == 0)
				{
				skey = pkey;
				break;
				}
		}
	if (ikey == npkeys)
		DIE("name in key command line arg is not a prime key of this series");
	}
  else
	{
	skey = pkey = template->seriesinfo->pidx_keywords[0];
	}
  if (pkey->info->recscope > 1)
	{ // pkey is actual "_index" internal primekey, skey is user name for keyword
	skey = drms_keyword_slotfromindex(pkey); // Get the indexed keyword
	slotted = 1;
	}
  else
	slotted = 0;
  // now skey contains DRMS_Keyword_t for users prime key, pkey contains DRMS_Keyword_t for index
  ptype = skey->info->type;
  pname = strdup(skey->info->name);
  punit = strdup(skey->info->unit);
  pformat = strdup(skey->info->format);
  piname = strdup(pkey->info->name);
  seriesname = strdup(template->seriesinfo->seriesname);

  // get method to determine if all data in a record is missing
  if (drms_keyword_lookup(template, "QUALITY", 1))
	{
	qualkey = "QUALITY";
	qualkind = 1;
        if (mask != 0)
          {
          qualkind = 3;
          }
	}
  else if (drms_keyword_lookup(template, "DATAVALS", 1))
	{
	qualkey = "DATAVALS";
	qualkind = 2;
	}
  else
	{
	qualkey = NULL;
	qualkind = 0;
	}
  // check prime key type for valid type for this program
  if (slotted == 0 && ( ptype != DRMS_TYPE_SHORT && ptype != DRMS_TYPE_INT && ptype != DRMS_TYPE_LONGLONG))
	DIE("Must be slotted or integer type first prime key");
  if (ptype == DRMS_TYPE_TIME)
	{
	strcpy(name, pname);
	strcat(name, "_epoch");
	epoch = drms_getkey_time(template, name, &status);
	strcpy(name, pname);
	strcat(name, "_step");
	step = drms_getkey_double(template, name, &status);
	}
  else if (slotted)
	{
	strcpy(name, pname);
	strcat(name, "_base");
	epoch = (TIME)drms_getkey_double(template, name, &status);
	strcpy(name, pname);
	strcat(name, "_step");
	step = (TIME)drms_getkey_double(template, name, &status);
	}
  else
	{
	epoch = (TIME)0.0;
	step = (TIME)1.0;
	}

  // Get series low info
  // do special action to skip false low records with prime key containing missing data values.
  // Allow many MISSING time initial records

  if (slotted)
    {
    sprintf(in, "%s[? %s_index>0 ?]", seriesname, pname);
    rs = drms_open_nrecords (drms_env, in, 1000, &status); // first record
    }
  else
    {
    sprintf(in, "%s[%s=^]", seriesname, pname);
    rs = drms_open_records (drms_env, in, &status); // first record
    }
  if (status || !rs || rs->n == 0)
	DIE("Series is empty");
  rec = rs->records[0];
  if (ptype == DRMS_TYPE_TIME)
	series_low = drms_getkey_time(rec, pname, &status);
  else if (slotted)
	series_low = (TIME)drms_getkey_double(rec, pname, &status);
  else
	series_low = (TIME)drms_getkey_longlong(rec, pname, &status);
  if (slotted)
	serieslowslot = drms_getkey_longlong(rec, piname, &status);
  else
	serieslowslot = series_low;
  drms_close_records(rs, DRMS_FREE_RECORD);
  // Now get low for range to be analysed
  if (strcmp(lowstr, NOT_SPECIFIED) == 0)
		low = series_low;
  else
	{
	if (ptype == DRMS_TYPE_TIME)
		low = sscan_time((char *)lowstr);
	else
                {
		// Even if the params should be slot numbers or integer keys, if the low=xxx param is
		// a string with at least one '_'  or ':' or 2 '.' then treat as a time and convert to a slot number.
                // use hmi.lev0a or aia.lev0 for this time to fsn mapping if series starts with e.g. hmi.lev
                char *tp, *index();
                tp = index(lowstr,'.');
                if (index(lowstr, '_') || index(lowstr,':') || (tp && index(tp+1,'.')))
			{
			char fsn_seriesname[DRMS_MAXSERIESNAMELEN];
			TIME tmplow = sscan_time((char *)lowstr);
			if (strncmp(seriesname,"hmi.",4) == 0)
				strcpy(fsn_seriesname, "hmi.lev0a");
			else if (strncmp(seriesname, "aia.", 4) == 0)
				strcpy(fsn_seriesname, "aia.lev0");
			else
				strcpy(fsn_seriesname, seriesname);
			if (slotted) // simple conversion
				{
				low = sscan_time((char *)lowstr);
				sprintf(in, "%s[? %s >= %f ?]", fsn_seriesname, pname, low);
				rs = drms_open_nrecords (drms_env, in, 1, &status); // first record
				if (status || !rs || rs->n == 0)
					DIE("Series is empty");
				rec = rs->records[0];
				low = drms_getkey_time(rec, pname, &status);
				drms_close_records(rs, DRMS_FREE_RECORD);
				}
			else
				{
				low = sscan_time((char *)lowstr);
// fprintf(stderr,"doing low=timestring case for integer not slotted case\n");
				DRMS_Type_t tobstype;
                                drms_getkey(template, "T_OBS", &tobstype, &status);
                                if (status)
                                	DIE("low given as time, series is not slotted and does not contain T_OBS, giveup.");
                                drms_getkey(template, "FSN", &tobstype, &status);
                                if (status)
                                	DIE("low given as time, series is not slotted and does not contain FSN, giveup.");
				sprintf(in, "%s[? T_OBS >= %f ?]", fsn_seriesname, low);
// fprintf(stderr,"query will be %s\n",in);
				rs = drms_open_nrecords (drms_env, in, 1, &status); // first record
				if (status || !rs || rs->n == 0)
					DIE("Series is empty");
				rec = rs->records[0];
				low = drms_getkey_time(rec, "FSN", &status);
// fprintf(stderr,"got %f\n",low);
				drms_close_records(rs, DRMS_FREE_RECORD);
				}
			}
		else
			low = (TIME)atof(lowstr);
                }
	}

  // Now get high limit
  // Do special action for seriesnames beginning "hmi.lev" or "aia.lev" to exclude
  // erroneous monster FSNs which are > 0X1C000000.
  if ((strncmp(seriesname,"hmi.lev",7) == 0 || strncmp(seriesname, "aia.lev", 7) == 0) && strcasecmp(pname, "fsn") == 0)
    {
    sprintf(in, "%s[? FSN < CAST(x'1c000000' AS int) ?]", seriesname);
    rs = drms_open_nrecords (drms_env, in, -1, &status); // last record
    }
  else
    {
    sprintf(in, "%s[%s=$]", seriesname, pname);
    rs = drms_open_records (drms_env, in, &status); // last record
    }
  rec = rs->records[0];
  if (ptype == DRMS_TYPE_TIME)
	series_high = drms_getkey_time(rec, pname, &status);
  else if (slotted)
	series_high = (TIME)drms_getkey_double(rec, pname, &status);
  else
	series_high = (TIME)drms_getkey_longlong(rec, pname, &status);
  if (slotted)
	serieshighslot = drms_getkey_longlong(rec, piname, &status);
  else
	serieshighslot = series_high;
  drms_close_records(rs, DRMS_FREE_RECORD);

  // Now get requested high limit`
  if (strcmp(highstr, NOT_SPECIFIED) == 0)
	high = series_high;
  else
	{
	if (ptype == DRMS_TYPE_TIME)
		high = sscan_time((char *)highstr);
	else
                {
		// Even if the params should be slot numbers or integer keys, if the high=xxx param is
		// a string with at least one '_' then treat as a time and convert to a slot number.
                // use hmi.lev0a or aia.lev0 for this time to fsn mapping if series starts with e.g. hmi.lev
                char *tp, *index();
                tp = index(highstr,'.');
                if (index(highstr, '_') || index(highstr,':') || (tp && index(tp+1,'.')))
			{
			char fsn_seriesname[DRMS_MAXSERIESNAMELEN];
			TIME tmphigh = sscan_time((char *)highstr);
			if (strncmp(seriesname,"hmi.",4) == 0)
				strcpy(fsn_seriesname, "hmi.lev0a");
			else if (strncmp(seriesname, "aia.", 4) == 0)
				strcpy(fsn_seriesname, "aia.lev0");
			else
				strcpy(fsn_seriesname, seriesname);
			if (slotted) // simple conversion
				{
				high = sscan_time((char *)highstr);
				sprintf(in, "%s[? %s > 0 AND %s <= %f ?]", fsn_seriesname, pname, pname, high);
				rs = drms_open_nrecords (drms_env, in, -1, &status); // first record
				if (status || !rs || rs->n == 0)
					DIE("Series is empty");
				rec = rs->records[0];
				high = drms_getkey_time(rec, pname, &status);
				drms_close_records(rs, DRMS_FREE_RECORD);
				}
			else
				{
				high = sscan_time((char *)highstr);
fprintf(stderr,"doing high=timestring case for integer not slotted case\n");
				DRMS_Type_t tobstype;
                                drms_getkey(template, "T_OBS", &tobstype, &status);
                                if (status)
                                	DIE("high given as time, series is not slotted and does not contain T_OBS, giveup.");
                                drms_getkey(template, "FSN", &tobstype, &status);
                                if (status)
                                	DIE("high given as time, series is not slotted and does not contain FSN, giveup.");
				sprintf(in, "%s[? T_OBS > 0 AND T_OBS <= %f ?]", fsn_seriesname, high);

fprintf(stderr,"query will be %s\n",in);
				rs = drms_open_nrecords (drms_env, in, -1, &status); // first record
				if (status || !rs || rs->n == 0)
					DIE("Series is empty");
				rec = rs->records[0];
				high = drms_getkey_time(rec, "FSN", &status);
fprintf(stderr,"got %f\n",high);
				drms_close_records(rs, DRMS_FREE_RECORD);
				}
			}
		else
			high = (TIME)atof(highstr);
                }
	}

  // Now get lowslot and highslot using same code as drms library calls.
  if (slotted)
	{
	DRMS_Value_t indexval;
	DRMS_Value_t inval;
	inval.value.double_val = low;
	inval.type = skey->info->type;
	drms_keyword_slotval2indexval(skey, &inval, &indexval, NULL);
	lowslot = indexval.value.longlong_val;

	inval.value.double_val = high;
	inval.type = pkey->info->type;
	drms_keyword_slotval2indexval(skey, &inval, &indexval, NULL);
	highslot = indexval.value.longlong_val;
	}
  else
	{
	lowslot = low;
	highslot = high;
	}
  blocked = strcmp(blockstr, NOT_SPECIFIED) != 0;
  if (blocked)
        {
        if (ptype == DRMS_TYPE_TIME)
		{
		char blocktemp[1024];
		strncpy(blocktemp, blockstr, 1020);
		if (isdigit(blocktemp[strlen(blocktemp)-1]))
			strcat(blocktemp, "s");
		blocking = (TIME)atoinc(blocktemp);
		}
	else
		blocking = (TIME)atof(blockstr);
        }
  else
	blocking = 0.0;
  blockstep = round(blocking / step);

  // NOW get the record coverage info
  nslots = highslot - lowslot + 1;
  if (nslots <= 0)
    {
    fprintf(stderr,"Found lowslot=%lld, highslot=%lld\n", lowslot, highslot);
    DIE("Error in low or high, found high < low");
    }
  map = (char *)malloc(sizeof(char) * nslots);
  if (!map)
    {
    fprintf(stderr,"lowslot=%lld, highslot=%lld nslots=%lld\n",lowslot,highslot,nslots);
    DIE("malloc failed");
    }
  for (islot=0; islot<nslots; islot++)
	map[islot] = (no_gone ? DATA_MISS : DATA_UNK);
  islot = 0;
  while (islot < nslots)
	{
	DRMS_Array_t *data;
	int nrecs, irec; 
	char query[DRMS_MAXQUERYLEN];
	char keylist[DRMS_MAXQUERYLEN];
	int qualindex=0, verifyindex=0;
        char *online = NULL;
	jslot = islot + chunksize;
	if (jslot >= nslots) jslot = nslots - 1;
	sprintf(query, "%s[%s=#%lld-#%lld]%s", seriesname, pname, lowslot+islot, lowslot+jslot, other);
	strcpy(keylist, piname);
	if (qualkind)
		{
		strcat(keylist, ",");
		strcat(keylist, qualkey);
		qualindex = 1;
		}
	if (verify)
		{
		strcat(keylist, ",");
		strcat(keylist, "sunum");
		verifyindex = qualindex + 1;
		}
        if (verbose)
          {
          printf("# Starting query %s\n", query);
          fflush(stdout);
          }
	data = drms_record_getvector(drms_env, query, keylist, DRMS_TYPE_LONGLONG, 0, &status);
	if (!data || status)
		{
		fprintf(stderr, "getkey_vector failed status=%d\n", status);
		DIE("getkey_vector failure");
		}
	nrecs = data->axis[1];
        if (verify)
          {
          int irec;
          long long *data_sunums = (long long *)data->data + verifyindex*nrecs;
          SUM_info_t **infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * nrecs);
          int infostatus;
          online = (char *)malloc(sizeof(char) * nrecs);
          infostatus = drms_getsuinfo(drms_env, data_sunums, nrecs, infostructs);
          if (infostatus)
              {
              fprintf(stderr,"drms_getsuinfo failed. Status=%d\n", infostatus);
              return(infostatus);
              }
          for (irec=0; irec<nrecs; irec++)
              {
              SUM_info_t *sinfo = infostructs[irec];
              int is_online =  (sinfo && sinfo->sunum > 0);
              if (is_online) is_online = strcmp(sinfo->online_status,"Y") == 0;
              online[irec] = is_online;
              free(infostructs[irec]);
              }
          free(infostructs);
          } // end of verify
	for (irec = 0; irec < nrecs; irec++)
		{
		long long thisslot = *((long long *)data->data + irec);
		uint32_t qualval;
		long long sunum;
		char val = DATA_OK;
                total_ok++;
		if (qualkind)
			{
			qualval = (uint32_t)(0xFFFFFFFF & *((long long *)data->data + qualindex*nrecs + irec));
			if ((qualkind == 1 && (qualval & 0x80000000) ) ||
                            (qualkind == 2 && qualval == 0) ||
                            (qualkind == 3 && (qualval & ignore)==0 && (qualval & mask)!=0 ))
                                {
                                total_ok--;
                                if (no_miss)
                                  val = DATA_UNK;
                                else
                                  {
			          val = DATA_MISS;
                                  total_miss++;
                                  }
                                }
			else if (verify && !online[irec])
                                {
			        val = (no_gone ? DATA_UNK : DATA_GONE);
                                total_ok--;
                                total_gone++;
                                }
			}
		else if (verify && !online[irec])
			{
			val = (no_gone ? DATA_UNK : DATA_GONE);
			total_ok--;
			total_gone++;
			}
		map[thisslot - lowslot] = val;
		}
	islot = jslot + 1;
	drms_free_array(data);
        if (verify && online)
          free(online);
	}
  total_unk = nslots - total_ok - total_miss - total_gone;

  // now have low, high and series_low, series_high, epoch and step, and lowslot and highslot and serieshighslot and serieslowslot.

if (!quiet)
  {
  fprintf(out, "series=%s\n", seriesname);
  fprintf(out, "key=%s\n", pname);
  fprintf(out, "type=%s\n", drms_type2str(ptype)); // ptype as string 
  fprintf(out, "slotted=%s\n", (slotted ? "T" : "F"));
  fprintf(out, "epoch="); printprime(out, epoch, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "step=%f\n", step); // print step as proper format
  fprintf(out, "low="); printprime(out, low, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "high="); printprime(out, high, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "block="); fprint_inc(out,blocking); fprintf(out, "\n"); // print block as proper format
  fprintf(out, "series_low="); printprime(out, series_low, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "series_high="); printprime(out, series_high, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "qualkey=%s\n", qualkey);
  if (qualkind == 3) fprintf(out, "mask=%#08x\nignore=%#08x\n", mask, ignore);
  if (*other) fprintf(out, "other=%s\n", other);
  }
if (want_summary)
  {
  fprintf(out, "total_ok=%d\n", total_ok);
  fprintf(out, "total_miss=%d\n", total_miss);
  fprintf(out, "total_unk=%d\n", total_unk);
  fprintf(out, "total_gone=%d\n", total_gone);
  }
// fprintf(out, "lowslot=%ld, highslot=%ld, serieslowslot=%ld serieshighslot=%ld\n",lowslot, highslot, serieslowslot, serieshighslot);

  if (blocked)
	{
	int iblock;
	int nblocks = (nslots + blockstep - 1)/blockstep;
	if (!quiet) fprintf(out, "%*s      n_OK   n_MISS    n_UNK%s\n",
			(ptype == DRMS_TYPE_TIME ? 23 : 15 ),"primeval", (verify ? "   n_GONE" : ""));
	for (iblock = 0; iblock < nblocks; iblock++)
		{
		char pval[DRMS_MAXQUERYLEN];
		int nOK, nMISS, nUNK, nGONE;
		nOK = nMISS = nUNK = nGONE = 0;
		if (useindex)
			 sprintf(pval, "%lld", lowslot + iblock*blockstep);
		else
			 sprintf(pval, "%s",
				 primevalstr(epoch + (lowslot + iblock*blockstep) * step, ptype, punit, pformat));
		for (islot = iblock*blockstep; islot < nslots && islot < (iblock+1)*blockstep; islot++)
		  {
		  if (map[islot] == DATA_OK) nOK++;
		  if (map[islot] == DATA_MISS) nMISS++;
		  if (map[islot] == DATA_UNK) nUNK++;
		  if (map[islot] == DATA_GONE) nGONE++;
		  }
		fprintf(out, "%*s  %8d %8d %8d",
				(ptype == DRMS_TYPE_TIME ? 22 : 15 ), pval, nOK, nMISS, nUNK);
		if (verify)
			fprintf(out, " %8d\n", nGONE);
		else
			fprintf(out, "\n");
		}
	}
  else
	{
	islot = 0;
	while (islot < nslots)
		{
		char pval[DRMS_MAXQUERYLEN];
		char endval[DRMS_MAXQUERYLEN];
		int nsame = 1;
		// get time of start of contiguous segment
		if (useindex)
			 sprintf(pval, "%lld", lowslot + islot);
		else
			 sprintf(pval, "%s",
				 primevalstr(epoch + (lowslot + islot) * step, ptype, punit, pformat));
		char thisval = map[islot], nval = 0;
		// get length of contiguouse segment of current type
		for (islot += 1; islot < nslots && map[islot] == thisval; islot++)
		  nsame += 1;
		// finish and print
                if (want_times)
			{
			// -t case, print start and end times of contiguous segment
			// get time of end of contiguous segment
			if (useindex)
			 	sprintf(endval, "%lld", lowslot + islot - 1);
			else
			 	sprintf(endval, "%s",
				   primevalstr(epoch + (lowslot + islot - 1) * step, ptype, punit, pformat));
			fprintf(out, "%4s %s %s %d\n",
				(thisval == DATA_OK ? "OK" :
				(thisval == DATA_MISS ? "MISS" :
				(thisval == DATA_GONE ? "GONE" : "UNK"))),
				pval, endval, nsame );
			}
		else
			{
			// default case, print start time and length of contiguous segment
			fprintf(out, "%4s %s %d\n",
				(thisval == DATA_OK ? "OK" :
				(thisval == DATA_MISS ? "MISS" :
				(thisval == DATA_GONE ? "GONE" : "UNK"))),
				pval, nsame );
			}
		}
	}
  return(0);
  }


