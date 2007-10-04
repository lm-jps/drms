//todo
// add flag to disable config

/* MODIFIED from original program to re-read older versions of DRMS lev0 code.
 * the change is in reverting to original keywords values in the cases where they were munged.
 * List of fixes:
 *  if -R flag is set then add 2 to HCAMID if exposure > 0
 */

/* import_egse_lev0 - import lev0 fits files from prog:hmi_ground,level:lev0,series:hmi_egsefsfm[fsn] type files */
/* Expect input keywords to contain HSQFGSN and shutter time HOBITSEC or SHS from the image header time. */
/* Expect prime keys in out series to be "FSN" and "T_OBS" */

/* Expect the target dataseries to have the desired FITS keynames as the first token in each
   keyword description.  
   Allow a mapping of names from the actual level0 FITS names to the target short names.  These
   mappings must be provided in a map file whose path is in the "keymap" parameter.
   Format of keymap file is one line per target keyname, target keyname is first token on line
   and aliases that may be found in the input file are subsequent tokens.
   At most MAX_KEYMAPS tokens may be present on each line.
   At most MAX_KEYS keywords will be considerend for remapping.
 */

/* NOTE that this module uses the dr library cloned from the sds library !! */

/* The program copies a lev0 FITS file into a matching DRMS record, setting the
 * DRMS keywords from the FITS file.  Additional keywords are written into both
 * the DRMS and FITS headers so the lev0 data can be accessed as a stand-along FITS
 * file or via DRMS segment read calls.
 *
 * The program can accept either RAL EGSE generated lev0 FITS files or
 * CIF/DCHRI HS-bus lev0 files produced from the telemetry via the SSIM.
 * The RAL (vs CIF) mode is set with the -r command line arg and saved in the keyword CONFIG
 */

/* Outline of this program 
 * Init: 
 *	Get command line flags and args
 *	Create a new output record into DRMS struct
 *	Read input lev0 FITS file into DR struct.
 *	Set some keywords from command line info
 * Loop through output keywords
 *   begin
 *	Get DRMS name and FITS shortname from rec
 *	if shortname in input file, use it
 *      else if shortname in alias list
 *		if one of aliases in input file, use it
 *		else set default value in FITS file
 *   if keyword is prime key T_OBS
 *      if t_obs command line keyword is present then use its value
 *	else if RAL mode
 *		Use filename for T_OBS
 *	else
 *		look in keyword named in timekey command line arg
 *		but if not found default to SHS keyword.
 *   if keyword is prime key FSN
 *	get FSN from possibly aliased keyword
 *   if keyword is TELEM_T set telem time value from SHS
 *   else
 *	for all other keywords copy value from
 *	FITS to DRMS if present, if not present
 *	set FITS value from DRMS default values from JSD.
 *	if RAL mode then fix the PCU position values.
 *   End of loop 
 * if CIF mode then
 *   begin
 *   if FSN < FSN_TAI_FIXED then add 33 seconds to T_OBS.
 *   get test config info from auxillary file
 *	call external program (set_config_by_time.csh) to update config info in
 *      an ancillary dataseries.  Pass the FSN and T_OBS to this program.
 *	Then open the newly made record in the ancillar series and copy
 *	the configuration keywords into both DRMS and FITS headers.
 *   set mechanism values via index keywords using tables.
 *   end of CIF special code
 * Save new FITS file with all keywords in segment with original filename
 * Close DRMS record 
 * Done
 */

/* Defined constants */

#define MAX_KEYMAPS     10
#define MAX_KEYS        256

#define FSN_CAM_ID_FIXED 50630 /* FSN before this number will have 1 added to HCAMID to fix bug */
#define FSN_HSHIEXP_FIXED 163852 /* FSN before this need to have exposures looked up.  Probably could be a lot earlier */
#define FSN_TAI_FIXED 1000000 /* FSN before this number will have 33 secs added to OBT to fix to TAI */

/* Table directory for mech values when in CIF mode */
#define TABLE_PATH "src/proj/lev0/apps/data/"

/* Program to call to update the config dataseries, takes 3 args: T_OBS, FSN, DRMSSESSION */
#define UPDATE_CONFIG_PROG "src/proj/lev0/scripts/hmi_ground/set_config_by_time.csh"

/* Ancillary series with test config data, prime key FSN */
#define CONFIG_SERIES "hmi_ground.lev0_config"

/* Includes for DRMS */
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"

/* includes code for sds clone library of fits reading, called DR instead of SDS */
/* #include "dr_lib.c" - code in the dr library now */
#include "dr.h"

/* Includes for standard libs */
#include <string.h>
#include <stdlib.h>

#define NOT_SPECIFIED "***Not Specified***"
#define DIE(msg) {fprintf(stderr,"$$$$ %s: %s\n",module_name,msg); return 1;}

/*  Global variables for JSOC running environment */

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "in", NOT_SPECIFIED, "Path to DSDS hmi_ground lev0 file"},
  {ARG_STRING, "out", NOT_SPECIFIED, "drms series for lev0 data"},
  {ARG_STRING, "fsn_key", "HSQFGSN", "Filtergram number keyword name"},
  {ARG_STRING, "time_key", "HOBITSEC", "Filtergram time keyword name"},
  {ARG_STRING, "t_obs", NOT_SPECIFIED, "Filtergram time override"},
  {ARG_STRING, "keymap", NOT_SPECIFIED, "Keyword mapping table"},
  {ARG_STRING, "dsds", NOT_SPECIFIED, "DSDS source dataset name"},
  {ARG_FLAG, "r", "0", "RAL EGSE data expected"},
  {ARG_FLAG, "R", "0", "Re-ingest from DRMS lev0 file"},
  {ARG_FLAG, "h", "0", "Print usage message and quit"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_END}
};

char *module_name = "import_egse_lev0";

/* Support functions */

int verbose = 0;
int nice_intro(int help)
  {
  int usage = cmdparams_get_int(&cmdparams, "h", NULL) != 0;
  verbose = cmdparams_get_int(&cmdparams, "v", NULL) != 0;
  if (usage || help)
    {
    printf("import_egse_lev0 in='fits file' out='jsoc series' {keymap='keymap file'} {dsds='dsds dataset'} {-h} {-r} {-v}  "
	"  -h: print this message\n"
	"  -r: RAL data input, no external config info needed\n"
        "  -R: Re-ingest mode, fixes HCAMID back to egse version\n"
	"  -v: verbose\n"
        "keymap=<key map file> - optional\n"
        "t_obs=<time over-ride value\n"
        "dsds=<dsds dataset name for source file\n"
	"in=<lev0 fits file> - required\n"
        "out=<drms lev0 series> - required\n");
    return(1);
    }
  return(0);
  }

/* Functions defined at end of this file */

TIME SDO_to_DRMS_time(int sdo_s, int sdo_ss);
int set_mech_values(DRMS_Record_t *rec, DR *dr, int fsn);
int init_keymaps(char *keymaps[MAX_KEYS][MAX_KEYMAPS], int *nkeys, char *key_map_file);
void sprint_time_ISO (char *tstring, TIME t);

/* Module main function. */

int DoIt(void)
{
  int status = 0;
  int RALmode = 0;
  int fsn;
  TIME t_obs, t_obs_force;
  char *t_obs_override;
  char *in, *out;
  char *in_filename;
  char tmpfile[1024];
  char tmpdir[1024];
  char *dsdsname;
  char *keymap;
  DRMS_Record_t *rec;
  DRMS_RecordSet_t *rs;
  HIterator_t key_hit;
  DRMS_Keyword_t *key;
  DR *lev0, *tmpdr;
  char *keymaps[MAX_KEYS][MAX_KEYMAPS];
  int keytarg, keytargs, newkey;
  ATTRIBUTES *fits_attr;
  TIME TIME2006Jan = sscan_time("2006.01.01_00:00:00");
  int re_ingest_from_drms;

  if (nice_intro(0))
    return(0);

printf("Import ground test lev0 data:\n");

/* Get command line arguments */
   in = strdup(cmdparams_get_str(&cmdparams, "in", NULL));
   if (strcmp(in,NOT_SPECIFIED)==0)
	DIE("in argument is required");
printf("   in=%s\n", in);

   out = strdup(cmdparams_get_str(&cmdparams, "out", NULL));
   if (strcmp(out,NOT_SPECIFIED)==0)
	DIE("out argument is required");
printf("   out=%s\n", out);

   dsdsname = cmdparams_get_str(&cmdparams, "dsds", NULL);
   if (strcmp(dsdsname,NOT_SPECIFIED)==0)
	dsdsname = "n.a.";
printf("   dsds=%s\n", dsdsname);

   keymap = cmdparams_get_str(&cmdparams, "keymap", NULL);
   if (strcmp(keymap,NOT_SPECIFIED)==0)
	printf("   keymap not used\n");
   else
	printf("   keymap=%s\n", keymap);
   if (init_keymaps(keymaps, &keytargs, keymap))
	DIE("Key Mapping File not found");
   printf("   keymap count=%d\n",keytargs);

   RALmode = cmdparams_get_int(&cmdparams, "r", NULL);
printf("   %s\n", (RALmode ? "RAL mode" : "DCHRI mode"));

   re_ingest_from_drms = cmdparams_get_int(&cmdparams, "R", NULL) != 0;
   if (re_ingest_from_drms)
      printf("   Re-ingest from previous DRMS lev0 file.\n");

   t_obs_override = cmdparams_get_str(&cmdparams, "t_obs", NULL);
   if (strcmp(t_obs_override,NOT_SPECIFIED)==0)
	t_obs_force = 0.0;
   else
	{
	char t_obs_str[1024];
        t_obs_force = sscan_time(t_obs_override);
	sprint_ut(t_obs_str, t_obs_force);
        printf("   ** WARNING ** T_OBS over-ride from command line is %s\n", t_obs_str);
	}

  /* Create new record to contain the lev0 image */

    rs = drms_create_records(drms_env, 1, out, DRMS_PERMANENT, &status);
    if (status)
	DIE("cant create records in output series");
    rec = rs->records[0];

  /* Get input file fits header */

  lev0 = dr_get_fits(in);
  if (!lev0)
	DIE("Failed to read fits header");
  tmpdr = lev0;

  /* Set some keywords not from input files */
  
  drms_setkey_string(rec, "DSDS_SRC", dsdsname);
  dr_setkey_str(tmpdr, "DSDS_SRC", dsdsname);
  drms_setkey_string(rec, "CONFIG", (RALmode ? "RAL" : "CIF"));
  dr_setkey_str(tmpdr, "CONFIG", (RALmode ? "RAL" : "CIF"));

  /* Look for all the drms keywords in the input lev0 and copy to output */

  hiter_new(&key_hit, &rec->keywords);
  while( (key = (DRMS_Keyword_t *)hiter_getnext(&key_hit)) )
    {
    char describe[DRMS_MAXCOMMENTLEN];
    char *shortname;
    char *usename;
    char *keyname = key->info->name;
    strcpy(describe, key->info->description);
    shortname = strtok(describe," \t,:");
    if (!shortname)
	shortname = keyname;
    if (strlen(shortname) > 8)
	DIE("shortname too long\n");
// fprintf(stderr,"longname=%s	shortname=%s",keyname,shortname);
  if (dr_search_attr(lev0, shortname) != NULL)
    {
    usename = shortname;
// fprintf(stderr,": found in lev0\n");
    }
  else
    { /* name not found, so lookup shortname in keymap to see if alternate expected */
    usename = shortname;
// fprintf(stderr,": NOT found in lev0\n");

    for (keytarg = 0; keytarg < keytargs; keytarg++)
      {
      if (strcmp(shortname, keymaps[keytarg][0]) == 0)
        { /* found shortname in key mapping list, now look for each of the aliases, take the first one found. */
        int trykey;
// fprintf(stderr, "	Found shortname in keymap.\n");
        for (trykey=1; trykey < MAX_KEYMAPS && keymaps[keytarg][trykey] != NULL; trykey++)
          if (fits_attr = dr_search_attr(lev0, keymaps[keytarg][trykey]))
	    { /* found substitute keyword in the input FITS file */
	    usename =  keymaps[keytarg][trykey];
// fprintf(stderr, "	new name is %s\n",usename);
            break;
            }
        break;
        }
      }
// fprintf(stderr, "	none of newnames found.\n");
    } /* usename now contains shortname or a keyword present in the input FITS file */
	    
    if (strcmp(keyname, "T_OBS") == 0)		 /* must have t_obs from somewhere */
      { /* this keyword is prime key T_OBS, make sure it gets set */
      char t_obs_str[40];
      int t_obs_s;
      char *time_key = strdup(cmdparams_get_str(&cmdparams, "time_key", NULL));
      if (t_obs_force > 0.0)
        t_obs = t_obs_force;
      else if (RALmode)
	{
	char *filename = DR_getkey_str(lev0, "FILENAME");
        int y,M,d,h,m,s;
        sscanf(filename, "i_%2d%2d%2d_%2d%2d%2d", &y, &M, &d, &h, &m, &s);
	sprintf(t_obs_str, "20%02d.%02d.%02d_%02d:%02d:%02d_UTC", y, M, d, h, m, s);
        t_obs = sscan_time(t_obs_str);
        }
      else
        {
        t_obs_s = dr_getkey_int(lev0, time_key);
        if (is_I_MISSING(t_obs_s) || t_obs_s < TIME2006Jan )			
          {
          t_obs_s = dr_getkey_int(lev0, "SHS"); /* no T_OBS so try SHS */
          if (is_I_MISSING(t_obs_s))
             DIE("Failed to find either time keyword");
          }
        t_obs = SDO_to_DRMS_time(t_obs_s, 0);
        }
      drms_setkey_double(rec, "T_OBS", t_obs);
      dr_setkey_time(tmpdr, "T_OBS", t_obs);
      sprint_ut(t_obs_str, t_obs);
      printf("   T_OBS=%s\n", t_obs_str);
      }
    else if (strcmp(keyname, "FSN")==0)  	/* get FSN, use alternate if given */
      { /* this keyword is prime key T_OBS, make sure it gets set */
      char *fsn_key = strdup(cmdparams_get_str(&cmdparams, "fsn_key", NULL));
      fsn  = dr_getkey_int(lev0, fsn_key);
      if (is_I_MISSING(fsn))
        DIE("Failed to find FSN keyword");
      drms_setkey_int(rec, "FSN", fsn);
      dr_setkey_int(tmpdr, "FSN", fsn);
      printf("   FSN=%d\n", fsn);
      }
    if (strcmp(keyname, "TELEM_T") == 0)	/* Get telemetry time from SHS, SHSS keywords */ 
      /* but note that TELEM_T is no longer a keyword in the .jsd file so this code will not be used... */
      { /* set keyword for telemetry time, this could be omitted */
      int t = dr_getkey_int(lev0, "SHS");
      int tf = dr_getkey_int(lev0, "SHSS");
      if (!is_I_MISSING(t) && !is_I_MISSING(tf))			
        {
        TIME telem_t = SDO_to_DRMS_time(t, tf);
        drms_setkey_double(rec, keyname, telem_t);
        dr_setkey_time(tmpdr, shortname, telem_t);
        }
      }
    else if (usename) /* simply copy other keywords that are found */
      { /* all other keywords, copy values from FITS to DRMS */
      if (fits_attr = dr_search_attr(lev0, usename))
        {
        char *fits_val_str = dr_attrvalue_str(fits_attr);
// fprintf(stderr, "	Found usename=%s value=%s\n",usename,fits_val_str);
        drms_setkey_string(rec, keyname, fits_val_str);
        if (strcmp(usename, shortname) != 0)
		{
		free(fits_attr->name);
		fits_attr->name = strdup(shortname);
		}
        if (RALmode)
	  {
	  if (!strcmp(shortname,"HPCUPOLR") ||
	      !strcmp(shortname,"HPCUPOLM") ||
	      !strcmp(shortname,"HPCURETR") ||
	      !strcmp(shortname,"HPCURETM") )
	    dr_setkey_double(tmpdr, shortname, strtod(fits_val_str, NULL));
	  }
        free(fits_val_str);
        }
      else
        {
	/* set the dr key to the drms default value */
fprintf(stderr, " using default value for %s\n", shortname);
        dr_setkey_drmstype(tmpdr, shortname, key);
        }
      }
    }

  /* Now try for config info */
  if (!RALmode)
    {
    char cmdline[1024], t_obs_str[1024];
    int status;
    char config_ds[1024];
    DRMS_Record_t *config_rec;
    DRMS_RecordSet_t *config_rs;
    HIterator_t config_key_hit;
    DRMS_Keyword_t *config_key;
    /* Correct T_OBS for TAI error on SSIM prior to FSN_TAI_FIXED */
    if (fsn < FSN_TAI_FIXED)
      {
      t_obs += 33.0;
      drms_setkey_double(rec, "T_OBS", t_obs);
      dr_setkey_time(tmpdr, "T_OBS", t_obs);
      sprint_ut(t_obs_str, t_obs);
      printf("   CORRECTED T_OBS=%s\n", t_obs_str);
      }
    /* call external program to update config info */
    sprint_time(t_obs_str, t_obs, "UTC", 0);
    sprintf(cmdline, "%s/%s '%s' %d %s", cmdparams_get_str(&cmdparams, "JSOCROOT", NULL),
	UPDATE_CONFIG_PROG, t_obs_str, fsn, cmdparams_get_str(&cmdparams, "DRMSSESSION", NULL));
    status = system(cmdline);
    if (status)
      {
      fprintf(stderr, "Failed to execute %s\n", cmdline);
      DIE("Failed to get config info\n");
      }
    /* Now get record with config info for this fsn and copy keywords */
    sprintf(config_ds, "%s[%d]", CONFIG_SERIES, fsn);
    config_rs = drms_open_records(drms_env, config_ds, &status);
    if (status == 0)
      {
      config_rec = config_rs->records[0];
      hiter_new(&config_key_hit, &config_rec->keywords);
      while( (config_key = (DRMS_Keyword_t *)hiter_getnext(&config_key_hit)) )
        {
        int stat = drms_setkey(rec, config_key->info->name, config_key->info->type, &config_key->value);
        /* note for these params the DRMS name is an OK FITS name */
        dr_setkey_drmstype(tmpdr, config_key->info->name, config_key);
        }
      drms_close_records(config_rs, DRMS_FREE_RECORD);
      }
    else
      fprintf(stderr,"cant open config records for %s\n",config_ds);

    /* do mech table lookups */
    status = set_mech_values(rec, tmpdr, fsn);
    if (status)
	DIE("Mech table failure");
    }

  /* set curent time in DATE kwyword.  Note EGSE DATE keyword will have been copied into DATEORIG above */
  {
  char tmpstr[100];
  sprint_time_ISO(tmpstr,CURRENT_SYSTEM_TIME);
  drms_setkey_string(rec, "DATE", tmpstr);
  dr_setkey_str(tmpdr, "DATE", tmpstr);
  }

  /* Save new FITS file with all keywords in segment with original filename */
  if (in_filename = strrchr(in,'/'))
    in_filename++;
  else
    in_filename = in;
  status = dr_write_fits_to_drms_segment(tmpdr, in_filename, rec, 0);
  if (status)
	DIE("FITS save in segment failure");
  dr_free(&lev0);
  status = drms_close_records(rs, DRMS_INSERT_RECORD);
  if (status)
	DIE("close failure");
  return 0;
}

/*
 * TIME SDO_to_DRMS_time(int sdo_s, int sdo_ss);
 * Note on time codes.
 * SDO/HMI,AIA keeps time in a 48-bit counter in units of 1/(2^16) seconds.  Thus
 * the top 32 bits is a seconds counter and the bottom 16 bits is a sub-seconds
 * counter.  The epoch is 1958.01.01_00:00:00.
 * Thus to convert HMI,AIA instrument time in two variables, e.g. SHS and SHSS to
 * a DRMS time the conversion is:  t_drms = SDO_EPOCH + SHS + SHSS/65536.0
 * where SDO_EPOCH = sscan_time("1958.01.01_00:00:00");
 * TAI and UTC are same at 1 Jan 1958.
 */
TIME SDO_to_DRMS_time(int sdo_s, int sdo_ss)
{
static int firstcall = 1;
static TIME sdo_epoch;
if (firstcall)
  { /* time_1958 - time_1977_TAI, to be added to SDO time to get DRMS time */
  firstcall = 0;
  sdo_epoch = sscan_time("1958.01.01_00:00:00_TAI");
  }
return(sdo_epoch + (TIME)sdo_s + (TIME)sdo_ss/65536.0);
}

/* Function to set HMI mechanism position keywords */

int set_mech_values(DRMS_Record_t *rec, DR *lev0, int fsn)
{
static int called = 0;

#define MAXROWS 10000
static int pol[MAXROWS*4];
static int tuning[MAXROWS*5];
static int focus[MAXROWS*3];
static int expose[MAXROWS*3];

static char *pol_keys[] = {"HPL1POS", "HPL2POS", "HPL3POS"};
static char *pol_longkeys[] = {"HPL1POS", "HPL2POS", "HPL3POS"};
static char *tuning_keys[] = {"HWL1POS", "HWL2POS", "HWL3POS", "HWL4POS"};
static char *tuning_longkeys[] = {"HWL1POS", "HWL2POS", "HWL3POS", "HWL4POS"};
static char *focus_keys[] =  {"HCF1POS", "HCF2POS"};
static char *focus_longkeys[] =  {"HCF1POS", "HCF2POS"};
static char *expose_keys[] = {"HSHIEXP", "HSHIEXP"};
static char *expose_longkeys[] = {"HMI_FSW_IMG_CMDED_EXPOSURE", "HMI_FSW_IMG_CMDED_EXPOSURE"};

static char *camkey = "HCAMID";
static char *camkey_longname = "HMI_SEQ_ID_EXP_PATH";

typedef struct tabinf
  {
  char *filename;
  char *index;
  char **keys;
  char **longkeys;
  int *table;
  int cols;
  char *longname;
  } TABINFO;
static TABINFO  tabinfo[] = {"in_air_cal3.p", "HPLTID", pol_keys, pol_longkeys, pol, 3,"HMI_SEQ_ID_PST",
                         "in_air_cal3.w", "HWLTID", tuning_keys, tuning_longkeys, tuning, 4,"HMI_SEQ_ID_WLT",
                         "in_air_cal.c", "HCFTID", focus_keys, focus_longkeys, focus, 2,"HMI_SEQ_ID_FOCUS",
                         "in_air_cal.e", "HSQEIDX", expose_keys, expose_longkeys, expose, 2,"HMI_SEQ_EXPOSURE_INDX"};
int tab;
int status;
int camera = dr_getkey_int(lev0, camkey);
int re_ingest_from_drms = cmdparams_get_int(&cmdparams, "R", NULL) != 0;

if (camera < 0 || camera > 3)
  {
  fprintf(stderr,"XX camera=%d outside range, use camera 1 exposures\n",camera);
  camera=1;
  }

if (re_ingest_from_drms)
  {
  int exposure = dr_getkey_int(lev0, expose_keys[0]);
  if (exposure > 0)
    camera += 2;
  }

if (!called)
  {
  called = 1;
  for (tab=0; tab<4; tab++)
    {
    char tablepath[1024];
    int idx, *res, val, vals;
    FILE *fp;
    char line[1024];
    strcpy(tablepath, cmdparams_get_str(&cmdparams, "JSOCROOT", NULL));
    strcat(tablepath, "/");
    strcat(tablepath, TABLE_PATH);
    strcat(tablepath, tabinfo[tab].filename);
    fp = fopen(tablepath, "r");
    if (!fp)
      {
      fprintf(stderr,"Failed to open mech table %s, die.\n",tablepath);
      return(1);
      }
    res = tabinfo[tab].table;
    vals = tabinfo[tab].cols;
    for (idx=0; idx<MAXROWS; idx++)
     for (val=0; val<vals+1; val++)
       res[val + (vals+1)*idx] = -1;
    for (idx=0; fgets(line,1024,fp); )
      {
      if (*line != '#')
        {
        char *e, *p=line;
        int d;
        for (val=0; val<vals+1; val++)
          {
          d = strtod(p, &e);
          if (e == p)
            break;
          else
            {
            p = e;
            res[val + (vals+1)*idx] = d;
            }
          }
        if (res[(vals+1)*idx] >= 0)
	  idx++;
        }
      }
    fclose(fp);
    }
  }

for (tab=0; tab<4; tab++)
  {
  int found_index;
  int row, index;
  int status;
  int val, vals = tabinfo[tab].cols;
  int *res = tabinfo[tab].table;
  char **keys = tabinfo[tab].keys;
  char **longkeys = tabinfo[tab].longkeys;
  found_index = 0;
  index = dr_getkey_int(lev0, tabinfo[tab].index);
  if (tab == 3) /* Fix index value for exposures */
    {
    if (fsn < FSN_CAM_ID_FIXED)
      index += 1; /* e.g. 39 goes to 40.  Ask Jesper. */
    dr_setkey_int(lev0, tabinfo[tab].index, index);
    drms_setkey_int(rec, tabinfo[tab].longname, index);
    }
  if (is_I_MISSING(index))
    {
    fprintf(stderr,"Mech Index %s not found.\n",tabinfo[tab].index);
    continue;
    }
  for (row=0; row<MAXROWS; row++)
    if (index == res[(vals+1)*row])
      { /* found proper row for this image */
      found_index = 1;
      if (tab<3) /* set positiion values for motors */
        {
        for (val=0; val<vals; val++)
	  {
          drms_setkey_int(rec, longkeys[val], res[val+1+(vals+1)*row]);
          dr_setkey_int(lev0, keys[val], res[val+1+(vals+1)*row]);
	  }
        break;
        }
      else /* exposure handled differently */
        {
        int exposure;
        if (camera <= 1)
          exposure = 0;
        else
          {
          camera -= 2;
          if (fsn <= FSN_HSHIEXP_FIXED) /* use lookup table for old ones, use EGSE keyword for later */
	    exposure = res[camera+1+(vals+1)*row];
          else
            exposure = dr_getkey_int(lev0, "HSHIEXP");
          }
        drms_setkey_int(rec, longkeys[0], exposure);
        dr_setkey_int(lev0, keys[0], exposure);
	drms_setkey_int(rec, camkey_longname, camera);
	dr_setkey_int(lev0, camkey, camera);
        break;
        }
      }
    if (!found_index)
      {
      fprintf(stderr,"Mech Index %d not found in mech table %s.\n",index,tabinfo[tab].filename);
      if (index > 0)
	return(1);
      }
  }
return(0);
}

/* function to read keyword aliases */

int init_keymaps(char *keymaps[MAX_KEYS][MAX_KEYMAPS], int *nkeys, char *key_map_file)
{
int key, map;
for (key=0; key<MAX_KEYS; key++)
  for (map=0; map<MAX_KEYMAPS; map++)
    keymaps[key][map] = NULL;
key = 0;
if (strcmp(key_map_file, NOT_SPECIFIED) != 0)
  {
  FILE *km = fopen(key_map_file,"r");
  char line[1024];
  if (!km)
    return(1);
  key=0;
  while (fgets(line, 1024, km))
    {
    char *tok;
    for (map=0, tok = strtok(line, " \t,:\n"); map<MAX_KEYMAPS && tok; map++, tok=strtok(NULL, " \t,\n"))
{
      keymaps[key][map] = strdup(tok);
}
    key++;
    }
  }
*nkeys = key;
return(0);
}

void sprint_time_ISO (char *tstring, TIME t)
{
sprint_at(tstring,t);
tstring[4] = tstring[7] = '-';
tstring[10] = 'T';
tstring[19] = '\0';
}
