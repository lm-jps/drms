#include "jsoc_main.h"
#include "drms.h"

/*
show_coverage

gets record completeness map for a given series.
Only works for series with integer type or slotted prime key,
and only gets map for first prime key.

1.  get prime key list and determine name and type of index to use.

2.  Look for existing coverage map in "coverage" keyword specified file.

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
    {ARG_FLAG, "v", "0", "Verbose - show all records"},
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
  DRMS_Record_t *rec;
  DRMS_Keyword_t *skey, *pkey;
  DRMS_Type_t ptype;
  char name[DRMS_MAXNAMELEN];
  char *pname;
  char *piname;
  char *punit;
  char *pformat;
  char *seriesname;
  TIME step, epoch, blocking;
  TIME series_low, series_high, low, high;
  char in[DRMS_MAXQUERYLEN];
  char *inbracket;
  char *ds = cmdparams_get_str (&cmdparams, "ds", &status);
  char *report = cmdparams_get_str (&cmdparams, "coverage", &status);
  char *blockstr = cmdparams_get_str (&cmdparams, "block", &status);
  char *lowstr = cmdparams_get_str (&cmdparams, "low", &status);
  char *highstr = cmdparams_get_str (&cmdparams, "high", &status);
  char *map;
  long long islot, jslot, nslots, blockstep;
  char *qualkey;
  int qualkind;

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
  if (inbracket) *inbracket = '\0';
  else inbracket = in + strlen(in);
  strcat(in, "[^]");
  rs = drms_open_records (drms_env, in, &status); // first record
  if (status || !rs || rs->n == 0)
    DIE("Series not found or empty");
  rec = rs->records[0];
  if (rec->seriesinfo->pidx_num < 1)
    DIE("Series has no prime keys");
  skey = pkey = rec->seriesinfo->pidx_keywords[0];
  if (pkey->info->recscope > 1)
    { // pkey is actual "_index" internal primekey, skey is user name for keyword
    skey = drms_keyword_slotfromindex(pkey); // Get the indexed keyword
    slotted = 1;
    }
  else
    slotted = 0;
  ptype = skey->info->type;
  pname = strdup(skey->info->name);
  piname = strdup(pkey->info->name);
  punit = strdup(skey->info->unit);
  pformat = strdup(skey->info->format);
  seriesname = strdup(rec->seriesinfo->seriesname);
  if (drms_keyword_lookup(rec, "QUALITY", 1))
    {
    qualkey = "QUALITY";
    qualkind = 1;
    }
  else if (drms_keyword_lookup(rec, "DATAVALS", 1))
    {
    qualkey = "DATAVALS";
    qualkind = 2;
    }
  else
    {
    qualkey = NULL;
    qualkind = 0;
    }
  if (slotted == 0 && ( ptype != DRMS_TYPE_SHORT && ptype != DRMS_TYPE_INT && ptype != DRMS_TYPE_LONGLONG))
    DIE("Must be slotted or integer type first prime key");
  if (ptype == DRMS_TYPE_TIME)
    {
    strcpy(name, pname);
    strcat(name, "_epoch");
    epoch = drms_getkey_time(rec, name, &status);
    strcpy(name, pname);
    strcat(name, "_step");
    step = drms_getkey_double(rec, name, &status);
    series_low = drms_getkey_time(rec, pname, &status);
    }
  else if (slotted)
    {
    strcpy(name, pname);
    strcat(name, "_base");
    epoch = (TIME)drms_getkey_double(rec, name, &status);
    strcpy(name, pname);
    strcat(name, "_step");
    step = (TIME)drms_getkey_double(rec, name, &status);
    series_low = (TIME)drms_getkey_double(rec, pname, &status);
    }
  else
    {
    epoch = (TIME)0.0;
    step = (TIME)1.0;
    series_low = (TIME)drms_getkey_longlong(rec, pname, &status);
    }
  if (slotted)
    {
    strcpy(name, pname);
    strcat(name, "_index");
    serieslowslot = drms_getkey_longlong(rec, name, &status);
    } // name still contains _index name
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
  strcpy(in,ds);
  *inbracket = '\0';
  strcat(in, "[$]");
  rs = drms_open_records (drms_env, in, &status); // first record
  rec = rs->records[0];
  if (ptype == DRMS_TYPE_TIME)
    series_high = drms_getkey_time(rec, pname, &status);
  else if (slotted)
    series_high = (TIME)drms_getkey_double(rec, pname, &status);
  else
    series_high = (TIME)drms_getkey_longlong(rec, pname, &status);
  if (strcmp(highstr, NOT_SPECIFIED) == 0)
    high = series_high;
  else
    {
    if (ptype == DRMS_TYPE_TIME)
      high = sscan_time(highstr);
    else
      high = atof(highstr);
    }
  if (slotted)
    {
    strcpy(name, pname);
    strcat(name, "_index");
    serieshighslot = drms_getkey_longlong(rec, name, &status);
    } // name still contains _index name
  else
    serieshighslot = series_high;
  // Now get lowslot and highslot using same code as drms library calls.
  if (slotted)
    {
    DRMS_Type_Value_t val;
    DRMS_Value_t indexval;
    DRMS_Value_t inval;
    pkey = drms_keyword_lookup(rec, pname, 0);
    val.double_val = low;
    drms_convert(pkey->info->type, &pkey->value, DRMS_TYPE_DOUBLE, &val);
    inval.value = pkey->value;
    inval.type = pkey->info->type;
    drms_keyword_slotval2indexval(pkey, &inval, &indexval, NULL);
    lowslot = indexval.value.longlong_val;

    val.double_val = high;
    drms_convert(pkey->info->type, &pkey->value, DRMS_TYPE_DOUBLE, &val);
    inval.value = pkey->value;
    inval.type = pkey->info->type;
    drms_keyword_slotval2indexval(pkey, &inval, &indexval, NULL);
    highslot = indexval.value.longlong_val;
    }
  else
    {
    lowslot = low;
    highslot = high;
    }
  drms_close_records(rs, DRMS_FREE_RECORD);
  blocked = strcmp(blockstr, NOT_SPECIFIED) != 0;
  if (blocked)
    {
    blocking = (TIME)atof(blockstr);
    }
  else
    blocking = 0.0;
  blockstep = round(blocking / step);

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
    jslot = islot + 1000000;
    if (jslot >= nslots) jslot = nslots - 1;
    sprintf(query, "%s[#%ld-#%ld]", seriesname, lowslot+islot, lowslot+jslot);
    strcpy(keylist, piname);
    if (qualkind)
      {
      strcat(keylist, ",");
      strcat(keylist, qualkey);
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
      if (qualkind)
        {
        long long thisslot = *((long long *)data->data + irec);
        long long qualval = *((long long *)data->data + nrecs + irec);
        char val = DATA_OK;
        if ((qualkind == 1 && qualval < 0) || (qualkind == 2 && qualval == 0))
           val = DATA_MISS;
if (thisslot < lowslot || thisslot > highslot) fprintf(stderr, "XXX slot error thisslot=%ld, irec=%d\n",thisslot,irec);
        map[thisslot - lowslot] = val;
        }
      else
        {
        long long thisslot = *((long long *)data->data + irec);
        char val = DATA_OK;
        map[thisslot - lowslot] = val;
        }
      }
    islot = jslot + 1;
    drms_free_array(data);
    }

  // now have low, high and series_low, series_high, epoch and step, and lowslot and highslot and serieshighslot and serieslowslot.

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
// fprintf(out, "lowslot=%ld, highslot=%ld, serieslowslot=%ld serieshighslot=%ld\n",lowslot, highslot, serieslowslot, serieshighslot);

  if (blocked)
    {
    int iblock;
    int nblocks = (nslots + blockstep - 1)/blockstep;
    fprintf(out, "%*s      n_OK   n_MISS    n_UNK\n", (ptype == DRMS_TYPE_TIME ? 23 : 15 ),"primeval");
    for (iblock = 0; iblock < nblocks; iblock++)
      {
      char *pval = primevalstr(epoch + (lowslot + iblock*blockstep) * step, ptype, punit, pformat);
      int nOK, nMISS, nUNK;
      nOK = nMISS = nUNK = 0;
      for (islot = iblock*blockstep; islot < nslots && islot < (iblock+1)*blockstep; islot++)
        {
        if (map[islot] == DATA_OK) nOK++;
        if (map[islot] == DATA_MISS) nMISS++;
        if (map[islot] == DATA_UNK) nUNK++;
        }
      fprintf(out, "%*s  %8d %8d %8d\n", (ptype == DRMS_TYPE_TIME ? 22 : 15 ),pval, nOK, nMISS, nUNK);
      }
    }
  else
    {
    islot = 0;
    while (islot < nslots)
      {
      int nsame = 1;
      char *pval = primevalstr(epoch + (lowslot + islot) * step, ptype, punit, pformat);
      char thisval = map[islot], nval = 0;
      for (islot += 1; islot < nslots && map[islot] == thisval; islot++)
        nsame += 1;;
      fprintf(out, "%4s %s %d\n", (thisval == DATA_OK ? "OK" : (thisval == DATA_MISS ? "MISS" : "UNK")), pval, nsame );
      }
    }
  return(0);
  }


