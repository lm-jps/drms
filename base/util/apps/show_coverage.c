#include "jsoc_main.h"
#include "drms.h"
// #include "printk.h"

/*
show_coverage

Show_coverage finds the record completeness map for a given series.
Since it needs a way to know if any given record is expected the program
only works for series with integer type or slotted prime key.
It will fail to be helpful for series that do not expect each index value of
a slotted series to be present (such as lev0 HK data for HMI and AIA.) 
The table will only be evaluated running over a single prime key.  Otherrr
prime keys can be used to select a subset of records by providing those keys
and target values on the command line.  If a prime key other than the first
prime key is to be used as the index for this program, that key can be specified
in the "use=<prime>" command line argument.  For slotted keys the 'user' key
should be given, not the one with '_index" appended, however it is actually
the index that will be used by the program.

Command line
  ds={seriesname}
  low=<first prime value to examine>
  high=<last prime value to examine>
     note: low and high should be index values if -v is set.
  block=<blocking>
     range of prime values to group for summary reports.  In prime units
     unless -v, in which case in index units.
  {key=<primekeyname>}
  {<other_primekey=value>}...
  -i  print index values vs prime key values in table
  -q  omit header information
  -v  verify the existance of all segments for records that are present
      with non-all_missing status.
  [coverage=<priormap>] - not yet implemented.

The program operates as follows:

1.  get prime key list and determine name and type of index to use.
1.1 get any other prime keys to use to filter the results.

2.  Look for existing coverage map in "coverage" keyword specified file.
== not implemented yet ==

3.  Read existing file or create new empty list.

4.  Scan target series from low to high limits and categorize each record
as OK, Missing, or unknown.

5.  Write report coverage map, as header and table
first rows of header contain:
series=<seriesname>
key=<primekeyname> - user name, not slotted index name
type=<slotted type, or variable type, e.g. int or short or longlong>
step=<value of keyname_step>
epoch=<epoch>
low=<table first value
high=table high value.
end -- marke end of header

next print coverage table, 3 words per line
kind start length
where kind is:
    OK - data is present for this range
    MISS - data is all known to be permanently missing for this range.
    UNK - data is absent but status unknown
    LOST - record is present and not marked MISSING but the segment is gone.
           This check will only be made if the '-v' == verify/verbose flag is present
           since it may take a very long time to determine for large series.

start is in user units
length is count of recordscontiguous 

Ah, add blocking capability to count the number of good records
in some interval of the prime key.  The interval would be specified
in units of the prime key and would be for an interval STARTING on the 
block separeted slot starting at low.  So for daily totals,
set block=86400 and low=2009.09.15_00 for data from 2009.09.15_00 to
2009.09.15_23:25:59.999

List terminates with line containing
END <first missing time> 0

*/

#define NOT_SPECIFIED "NOT_SPECIFIED"

#define DATA_OK ('\0')
#define DATA_MISS ('\1')
#define DATA_UNK ('\2')
#define DATA_LOST ('\3')

SUM_t *my_sum=NULL;

SUM_info_t *drms_get_suinfo(long long sunum)
  {
  int status;
  if (my_sum && my_sum->sinfo->sunum == sunum)
    return(my_sum->sinfo);
  if (!my_sum)
    {
    if ((my_sum = SUM_open(NULL, NULL, NULL)) == NULL)
	    return(NULL);
    }
  if (status = SUM_info(my_sum, sunum, NULL))
       return(NULL);
  return(my_sum->sinfo);
  }


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
    {ARG_STRING, "coverage", NOT_SPECIFIED,  "Output coverage report."},
    {ARG_STRING, "block", NOT_SPECIFIED,  "interval for block summaries of the coverage."},
    {ARG_STRING, "low", NOT_SPECIFIED, "Low limit for coverage map."},
    {ARG_STRING, "high", NOT_SPECIFIED, "High limit for coverage map."},
    {ARG_STRING, "key", NOT_SPECIFIED, "Prime key name to use, default is first prime"},
    {ARG_FLAG, "v", "0", "Verify - verify that SU is available for records with data"},
    {ARG_FLAG, "i", "0", "Index - Print index values instead of prime slot values"},
    {ARG_FLAG, "q", "0", "Quiet - omit series header info"},
    {ARG_END}
};

#define DIE(msg) {fprintf(stderr,"%s\n",msg);exit(1);}


char *module_name = "show_coverage";

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
  char otherkeys[20*DRMS_MAXQUERYLEN];
  char *ds = cmdparams_get_str (&cmdparams, "ds", NULL);
  char *report = cmdparams_get_str (&cmdparams, "coverage", NULL);
  char *blockstr = cmdparams_get_str (&cmdparams, "block", NULL);
  char *lowstr = cmdparams_get_str (&cmdparams, "low", NULL);
  char *highstr = cmdparams_get_str (&cmdparams, "high", NULL);
  char *skeyname = cmdparams_get_str (&cmdparams, "key", NULL);
  int verify = cmdparams_get_int (&cmdparams, "v", NULL) != 0;
  int quiet = cmdparams_get_int (&cmdparams, "q", NULL) != 0;
  int useindex = cmdparams_get_int (&cmdparams, "i", NULL) != 0;
  char *map;
  long long islot, jslot, nslots, blockstep;
  char *qualkey;
  int qualkind;
  int ikey;

  /* check for minimum inputs */
  // if (strcmp(ds, NOT_SPECIFIED) == 0 || strcmp(report, NOT_SPECIFIED) == 0)
  if (strcmp(ds, NOT_SPECIFIED) == 0 )
    // DIE("No files: at least ds and coverage must be specified");
    DIE("No files: at least ds must be specified");
  // out = fopen .... report ...
  out = stdout;


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
			skey = drms_keyword_slotfromindex(pkey);
		if (strcmp(skeyname, skey->info->name) == 0)
			break;
		}
	if (ikey == template->seriesinfo->pidx_num)
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
  piname = strdup(pkey->info->name);
  punit = strdup(skey->info->unit);
  pformat = strdup(skey->info->format);
  seriesname = strdup(template->seriesinfo->seriesname);
  // get optional other primekeys
  otherkeys[0] = '\0';
  for (ikey=0; ikey < npkeys; ikey++)
	{
	DRMS_Keyword_t *tmppkey = template->seriesinfo->pidx_keywords[ikey];
	if (tmppkey->info->recscope > 1)
		tmppkey = drms_keyword_slotfromindex(pkey);
	if (cmdparams_exists(&cmdparams, tmppkey->info->name))
		{
		char tmp[DRMS_MAXQUERYLEN];
		if (strcmp(tmppkey->info->name, pname) == 0)
			DIE("Can not have main prime key listed explicitly");
		sprintf(tmp, "[%s=%s]", tmppkey->info->name,
				cmdparams_get_str(&cmdparams, tmppkey->info->name, NULL));
		strcat(otherkeys, tmp);
		}
	}

  // get method to determine if all data in a record is missing
  if (drms_keyword_lookup(template, "QUALITY", 1))
	{
	qualkey = "QUALITY";
	qualkind = 1;
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
  sprintf(in, "%s[%s=^]", seriesname, pname);
  // sprintf(in, "%s[%s=^]%s", seriesname, pname, otherkeys);
  rs = drms_open_records (drms_env, in, &status); // first record
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
  if (strcmp(lowstr, NOT_SPECIFIED) == 0)
		low = series_low;
  else
	{
	if (ptype == DRMS_TYPE_TIME)
		low = sscan_time(lowstr);
	else
		low = (TIME)atof(lowstr);
	}

  sprintf(in, "%s[%s=$]", seriesname, pname);
  // sprintf(in, "%s[%s=$]%s", seriesname, pname, otherkeys);
  rs = drms_open_records (drms_env, in, &status); // last record
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
  if (strcmp(highstr, NOT_SPECIFIED) == 0)
	high = series_high;
  else
	{
	if (ptype == DRMS_TYPE_TIME)
		high = sscan_time(highstr);
	else
		high = atof(highstr);
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
	blocking = (TIME)atof(blockstr);
  else
	blocking = 0.0;
  blockstep = round(blocking / step);

  // NOW get the record coverage info
  nslots = highslot - lowslot + 1;
  map = (char *)malloc(sizeof(char) * nslots);
  for (islot=0; islot<nslots; islot++)
	map[islot] = DATA_UNK;
  islot = 0;
  while (islot < nslots)
	{
	DRMS_Array_t *data;
	int nrecs, irec; 
	char query[DRMS_MAXQUERYLEN];
	char keylist[DRMS_MAXQUERYLEN];
	int qualindex=0, verifyindex=0;
	jslot = islot + 1000000;
	if (jslot >= nslots) jslot = nslots - 1;
	sprintf(query, "%s[%s=#%lld-#%lld]%s", seriesname, pname, lowslot+islot, lowslot+jslot,otherkeys);
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
	data = drms_record_getvector(drms_env, query, keylist, DRMS_TYPE_LONGLONG, 0, &status);
	if (!data || status)
		{
		fprintf(stderr, "getkey_vector failed status=%d\n", status);
		DIE("getkey_vector failure");
		}
	nrecs = data->axis[1];
	for (irec = 0; irec < nrecs; irec++)
		{
		long long thisslot = *((long long *)data->data + irec);
		long long qualval, sunum;
		char val = DATA_OK;
		if (qualkind)
			{
			qualval = *((long long *)data->data + qualindex*nrecs + irec);
			if ((qualkind == 1 && qualval < 0) || (qualkind == 2 && qualval == 0))
			     val = DATA_MISS;
			else if (verify)
				{
				SUM_info_t *suminfo;
				sunum = *((long long *)data->data + verifyindex*nrecs + irec);
				suminfo = drms_get_suinfo(sunum);
				if (!suminfo)
					val = DATA_LOST;
				}
			}
		map[thisslot - lowslot] = val;
		}
	islot = jslot + 1;
	drms_free_array(data);
	}

  if (my_sum)				\
    SUM_close(my_sum,NULL);	\

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
  fprintf(out, "block=%.0f\n", blocking); // print block as proper format
  fprintf(out, "series_low="); printprime(out, series_low, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "series_high="); printprime(out, series_high, ptype, punit, pformat); fprintf(out, "\n");
  fprintf(out, "qualkey=%s\n", qualkey);
  }
// fprintf(out, "lowslot=%ld, highslot=%ld, serieslowslot=%ld serieshighslot=%ld\n",lowslot, highslot, serieslowslot, serieshighslot);

  if (blocked)
	{
	int iblock;
	int nblocks = (nslots + blockstep - 1)/blockstep;
	fprintf(out, "%*s      n_OK   n_MISS    n_UNK%s\n",
			(ptype == DRMS_TYPE_TIME ? 23 : 15 ),"primeval", (verify ? "  n_LOST" : ""));
	for (iblock = 0; iblock < nblocks; iblock++)
		{
		char pval[DRMS_MAXQUERYLEN];
		int nOK, nMISS, nUNK, nLOST;
		nOK = nMISS = nUNK = nLOST = 0;
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
		  if (map[islot] == DATA_LOST) nLOST++;
		  }
		fprintf(out, "%*s  %8d %8d %8d",
				(ptype == DRMS_TYPE_TIME ? 22 : 15 ), pval, nOK, nMISS, nUNK);
		if (verify)
			fprintf(out, " %8d\n", nLOST);
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
		int nsame = 1;
		if (useindex)
			 sprintf(pval, "%lld", lowslot + islot);
		else
			 sprintf(pval, "%s",
				 primevalstr(epoch + (lowslot + islot) * step, ptype, punit, pformat));
		char thisval = map[islot], nval = 0;
		for (islot += 1; islot < nslots && map[islot] == thisval; islot++)
		  nsame += 1;
		fprintf(out, "%4s %s %d\n",
			(thisval == DATA_OK ? "OK" :
			(thisval == DATA_MISS ? "MISS" :
			(thisval == DATA_LOST ? "LOST" : "UNK"))),
			pval, nsame );
		}
	}
  return(0);
  }


