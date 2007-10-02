/* fitsIntoDRMS.c
 *
 * Ingests arbitrary FITS files into DRMS records in a series.
 */

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include "jsoc_main.h"
#include "dr.h"

#define kParamUnspec "NOT SPECIFIED"
#define kParamSeries "series"
#define kPrimeKeys "pkeys"
#define kMapFile "kwmap"
#define kSegInfo "segs"
#define kMaxKeys 1024
#define kBZERO "bzero"
#define kBSCALE "bscale"
#define kMAXSEGS 128
#define kMAXFITSNAME 16
#define kMAXDIMSSTR 256

/* 
   Usage:
     fitsIntoDRMS series=<seriesname> pkeys=<keylist> segs=<seginfolist> kwmap=<file>

       where <keylist> = <fitskeyname1>,<fitskeyname2>,...,<fitskeynameN>
             <seginfolist> = [<seginfo1>][<seginfo2>]...[<seginfoN>]
	     <seginfo> = <segname1>,<naxis1>,...,<naxisM>/<path1>,<path2>,...,<pathN>
	     <path> = <filepath> || <directory>
*/

ModuleArgs_t module_args[] =
{
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_STRING, kParamSeries, "", "Series to contain ingested files."},
  {ARG_STRING, kPrimeKeys, "", "Primary key list of series."},
  {ARG_STRING, kMapFile, kParamUnspec, "Mapping from FITS to DRMS keyword name."},
  {ARG_STRING, kSegInfo, "", "Segment information and data files."},
  {ARG_END}
};

char *module_name = "fitsIntoDRMS";

char *gMapFileStr = NULL;
DRMS_KeyMap_t *gKeyMap = NULL;
HContainer_t *gDict = NULL;

typedef struct SegInfo_struct 
{
  char segname[DRMS_MAXNAMELEN];
  int naxis;
  int dims[DRMS_MAXRANK];
  char dimsStr[kMAXDIMSSTR];
  char **files;
  int nfiles;
} SegInfo_t;

typedef struct RecInfo_struct
{
  char **files;
  int nfiles;
} RecInfo_t;

/* SQL identifier syntax
 * ---------------------
 * SQL identifiers and key words must begin with a letter (a-z, but also letters with 
 * diacritical marks and non-Latin letters) or an underscore (_). Subsequent characters
 * in an identifier or key word can be letters, underscores, or digits (0-9).
 * The SQL standard will not define a key word that contains digits or starts or ends 
 * with an underscore, so identifiers of this form are safe against possible conflict 
 * with future extensions of the standard.
 *
 * The system uses no more than NAMEDATALEN-1 characters of an identifier; longer names 
 * can be written in commands, but they will be truncated. By default, NAMEDATALEN is 64 
 * so the maximum identifier length is 63.
 */

/* 
 * FITS keyword field grammar
 * --------------------------
 *  keyword_field :=
 *     [keyword_char...] [space...] 
 *
 *
 *  keyword_char :=
 *      `A'-`Z' | `0'-`9' | `_' | `-' 
 *
 *  where ... means "one or more" of the preceding element
 *
 */

/* Need to map invalid FITS keyword names to valid ones.  From the above, it
 * appears that the only characters that may appear in a FITS keyword name
 * that are invalid PSQL-identifier characters  '-' and '_'.  Also, a FITS keyword may start with 
 * [0-9], and such a keyword would not be a valid PSQL keyword.
 *
 * The solution is to map these FITS keyword name characters that are invalid PQSL ones to 
 * valid PSQL ones.  Should a '-' start a FITS keyword name, that character will
 * be replaced by '_' and the entire keyword name will be prefixed by "mh_" (to indicate 
 * (m)anipulation of an (h)yphen).  Should a '_' start a FITS keyword name, the entire 
 * keyword name will be prefixed by "mu_" (to indicate (m)anipulation of an (u)derscore). 
 * Should a [0-9] start a FITS keyword name, that character will be replaced by
 * '_' and the entire keyword name will be prefixed by "mn_" (to indicate 
 * (m)anipulation of a (n)umeral).  A prefix suggests that a starting character was
 * manipulated.  '-', if it occurs in the middle of a keyword name, shall be 
 * converted to '_', and the entire keyword name will be suffixed with "_mh".
 * A suffix suggests that one or more non-staring characters were manipulated.
 *
 * For export purposes, the FITS keyword name shall be stored
 * in the keyword description field.  The keyword name will be enclosed in '[' and ']'
 * brackets and whitespace will separate this string from the remainder of the description.
 *
 * To summarize, here is the mapping from FITS to PSQL:
 *   FITS               PSQL
 *   ----               ----
 *   XXX-XXX            XXX_XXX_mh
 *   -XXX               mh__XXX
 *   _XXX               mu__XXX
 *   [0-9]XXX           mn_[0-9]XXX
 *  
 */

static const char *GetDRMSNameFromFITSName(const char *fitsName)
{
   const char *ret = NULL;
   FILE *fPtr = NULL;

   if (gKeyMap == NULL)
   {
      if (gMapFileStr)
      {
	 if (!(fPtr = fopen(gMapFileStr, "r"))) 
	 {
	    fprintf (stderr, "  error: unable to open keyword map file %s\n", gMapFileStr);
	 }
	 else
	 {
	    gKeyMap = drms_keymap_create();
	    
	    if (gKeyMap)
	    {
	       drms_keymap_parsefile(gKeyMap, fPtr);
	    }

	    fclose(fPtr);
	 }
      }
   }

   const char *drmsName = drms_keymap_intname(gKeyMap, fitsName);
   if (drmsName != NULL)
   {
      ret = drmsName;
   }
   
   return ret;
}

static int SanitizeKeywordName(const char *kwName, char *sanitizedName, int size)
{
   int error = 0;

   const char *pcIn = kwName;
   const char *drmsName = NULL;

   /* Override mapping with keyword map file. */
   if (gMapFileStr)
   {
      drmsName = GetDRMSNameFromFITSName(kwName);
      if (drmsName)
      {
	 pcIn = drmsName;
      }
   }

   /* But still give default mapping rules a crack at it since
    * the map file may lead to invalid DRMS names, or it might
    * be missing some of the keywords. */
   if (!IsValidDRMSKeyName(pcIn))
   {
      char buf[DRMS_MAXNAMELEN];
      if(GenerateDRMSKeyName(pcIn, buf, sizeof(buf)))
      {
	 snprintf(sanitizedName, size, "%s", buf);
      }
      else
      {
	 *sanitizedName = '\0';
	 error = 1;
      }
   }
   else
   {
      snprintf(sanitizedName, size, "%s", pcIn);
   }
   
   return error;
}

/* Returns the equivalent drms type. */
static DRMS_Type_t DrTypeToDRMSType(int drType)
{
   DRMS_Type_t drmsType;
     
   switch (drType)
   {
      case DR_LOGICAL:
      case DR_BYTE:
	drmsType = DRMS_TYPE_CHAR;
	break;
      case DR_SHORT:
	drmsType = DRMS_TYPE_SHORT;
	break;
      case DR_INT:
	drmsType = DRMS_TYPE_INT;
	break;
      case DR_LONG:
	drmsType = DRMS_TYPE_LONGLONG;
	break;
      case DR_FLOAT:
	drmsType = DRMS_TYPE_FLOAT;
	break;
      case DR_DOUBLE:
	drmsType = DRMS_TYPE_DOUBLE;
	break;
      case DR_TIME:
	drmsType = DRMS_TYPE_TIME;
	break;
      case DR_STRING:
	drmsType = DRMS_TYPE_STRING;
	break;
      default:
	drmsType = DRMS_TYPE_RAW;
   }

   return drmsType;
}

static int ValToDRMSVal(int drmsType, void *val, DRMS_Type_Value_t *value)
{

   int error = 0;
     
   switch (drmsType)
   {
      case DRMS_TYPE_CHAR:
	value->char_val = *(char *)val;
	break;
      case DRMS_TYPE_SHORT:
	value->short_val = *(short *)val;
	break;
      case DRMS_TYPE_INT:
	value->int_val = *(int *)val;
	break;
      case DRMS_TYPE_LONGLONG:
	value->longlong_val = *(long long *)val;
	break;
      case DRMS_TYPE_FLOAT:
	value->float_val = *(float *)val;
	break;
      case DRMS_TYPE_DOUBLE:
	value->double_val = *(double *)val;
	break;
      case DRMS_TYPE_TIME:
	value->time_val = *(double *)val;
	break;
      case DRMS_TYPE_STRING:
	value->string_val = strdup((char *)val);
	break;
      default:
	fprintf(stderr, "Invalid drms type: %d\n", drmsType);
	error = 1;
   }

   return error;
}

static int SetDRMSKey(DRMS_Record_t *rec, DRMS_Type_t drmsType, char *name, void *val)
{
   int error = 0;

   DRMS_Type_Value_t value;

   switch (drmsType)
   {
      case DRMS_TYPE_CHAR:
	value.char_val = *(char *)val;
	break;
      case DRMS_TYPE_SHORT:
	value.short_val = *(short *)val;
	break;
      case DRMS_TYPE_INT:
	value.int_val = *(int *)val;
	break;
      case DRMS_TYPE_LONGLONG:
	value.longlong_val = *(long long *)val;
	break;
      case DRMS_TYPE_FLOAT:
	value.float_val = *(float *)val;
	break;
      case DRMS_TYPE_DOUBLE:
	value.double_val = *(double *)val;
	break;
      case DRMS_TYPE_TIME:
	value.time_val = *(double *)val;
	break;
      case DRMS_TYPE_STRING:
	value.string_val = (char *)val;
	break;
      default:
	fprintf(stderr, "Invalid drms type: %d\n", drmsType);
	error = 1;
   }

   if (!error)
   {
      error = (drms_setkey(rec, name, drmsType, &value) != 0);
   }

   if (error)
   {
      fprintf(stderr, "Failed to set drms key %s\n", name);
   }

   return error;
}

static HContainer_t *CreateKnownTypeDict()
{
   if (gDict == NULL)
   {	
      HContainer_t *d = hcon_create(sizeof(int), 16, NULL, NULL, NULL, NULL, 0);
      int timeType = DR_TIME;

      if (d)
      {
	 hcon_insert(d, "T_OBS", &timeType);
	 hcon_insert(d, "T_START", &timeType);
	 hcon_insert(d, "T_STOP", &timeType);
	 hcon_insert(d, "T_REC", &timeType);
	 hcon_insert(d, "T_ROT", &timeType);
	 hcon_insert(d, "T_EARTH", &timeType);
	 hcon_insert(d, "RUNTIME", &timeType);
      }
      
      gDict = d;
   }
   
   return gDict;
}

/* The input is the fits file name. Returns the dr type. */
static int GetKnownType(const char *drKwName)
{
     int ret = -1;
     static HContainer_t *dict = NULL;

     if (dict == NULL)
     {
	  dict = CreateKnownTypeDict();
     }

     int *drType = hcon_lookup(dict, drKwName);
     if (drType != NULL)
     {
	  /* Recognized type*/

	  char *name[] = {"Void", "SignedByte", "UnsignedByte", "Short",
			  "UnsignedShort", "Integer", "UnsignedInteger", 
			  "Long", "UnsignedLong",
			  "Float", "Double", "Complex", "String", "Time",
			  "Logical", "illegal"};
	  int printDType = *drType;
	  int known = sizeof(name) / sizeof(char *);

	  if (*drType < 0 || *drType >= known) 
	  {
	       printDType = known - 1;
	  }

	  printf("%s is a known keyword of type %s.\n", drKwName, name[printDType]);
	  ret = *drType;
     }
       
     return ret;
}

static void FixComments(char **kwComments,
			char **kwFitsNames,
			char **drmsComments,
			int nKeys)
{
   int iKeys = 0;

   for (; iKeys < nKeys; iKeys++)
   {
      char *comment = kwComments[iKeys];
      char *fitsName = kwFitsNames[iKeys];
      char realComment[DRMS_MAXCOMMENTLEN] = {0};

      if (fitsName != NULL)
      {
	 char *pComm = realComment;
	 
	 if (comment)
	 {
	    snprintf(pComm, DRMS_MAXCOMMENTLEN, "[%s] %s", fitsName, comment);
	 }
	 else
	 {
	    snprintf(pComm, DRMS_MAXCOMMENTLEN, "[%s]", fitsName);
	 }
	 
	 pComm += strlen(pComm);
      }
      else if (comment != NULL)
      {
	 snprintf(realComment, DRMS_MAXCOMMENTLEN, "%s", comment);
      }

      if (strlen(realComment) > 0)
      {
	 drmsComments[iKeys] = strdup(realComment);
      }
   }
}

/* Returns number of keys ingested. */
/* IMPORTANT: When DR reads a fits file, it converts the data to either float or double.  
 * DR returns bzero and bscale values, which are then ADVISORY values.  The actual data
 * has bzero == 0.0, and bscale == 1.0.  These advisory values, in addition to the
 * dr->scaling advisory value (which is a number of bits), are there to 
 * help convert back to raw integer data that requires a scaling to arrive at physical 
 * values.  So, if you want to convert back to raw data, you can start with the 
 * physical values, and then convert back to integer data of dr->scaling bits, with a 
 * scaling factor of bscale, and an offset of bzero.
 *
 * FITS files with floating-point data always contain physical values, which means
 * bzero = 0.0 and bscale = 1.0.
 */
static int ReadAndParseFitsFile(const char *fileName, 
				DRMS_Type_t *keyTypes, /* drms types */
				char **keyNames,       /* drms keyword names */
				char **keyComments,    /* may contain fits keyword names */
				char **keyFitsNames,
				void **keyValues, 
				DRMS_Type_t *drmsDataType,
				int *naxis,
				int *dims,
				double *bzero,
				double *bscale,
				void **data,
				int *error)
{
     *error = 0;
     int status = 0;
     FILE *fPtr = NULL;
     DR *img = NULL;
     int iKey = 0;
     int cleanImg = 0;

     if (!(fPtr = fopen (fileName, "r"))) 
     {
	  fprintf (stderr, "  error: unable to open file %s\n", fileName);
	  *error = 1;
     }

     if (data)
     {
	if (!*error && (!(img = dr_read_fits(fPtr, &status)) || status != 0)) 
	{
	   fprintf (stderr, "  error: unable to read file %s as FITS\n", fileName);
	   fprintf (stderr, "  status = %d\n", status);
	   *error = 1;
	}

	if (img)
	{
	   cleanImg = 1;
	}
     }
     else
     {
	/* Read just the header */
	DR header;
	memset(&header, 0, sizeof(DR));
	if (!*error && ((status = read_fits_head(&header, fPtr)) != 0))
	{
	   fprintf (stderr, "  error: unable to read file %s as FITS\n", fileName);
	   fprintf (stderr, "  status = %d\n", status);
	   *error = 1;
	}

	if (!*error)
	{
	   img = &header;
	}
     }
      
     if (fPtr != NULL)
     {
	  fclose(fPtr);
	  fPtr = NULL;
     }

     int kwIndex = 0;

     if (!*error)
     {
	  int nDataBytes = dr_data_length(img) * dr_numbytes(img);
	  int drDataType = dr_datatype(img);
	  XASSERT(drDataType == DR_FLOAT || drDataType == DR_DOUBLE);
	  *drmsDataType = DrTypeToDRMSType(drDataType);
	  if (data)
	  {
	     *data = (void *)malloc(nDataBytes);
	     memcpy(*data, dr_data(img), nDataBytes);
	  }
	  char *drComments[kMaxKeys];
	  memset(drComments, 0, sizeof(char *) * kMaxKeys);

	  /* Do mandatory keywords first. */
	  *naxis = dr_rank(img);
	  
	  int iDim = 0;
	  for (; iDim < *naxis; iDim++)
	  {
	       dims[iDim] = dr_dim_n(img, iDim);
	  }

	  XASSERT(img->fillval == NULL); /* DR sets to NULL for non-integer data. */

	  *bzero = dr_bzero(img);
	  *bscale = dr_bscale(img);
	  char *kwName = NULL;
	  char *kwComment = NULL;
	  void *kwValue = NULL;
	  int kwType = 0;
	  ATTRIBUTES *attr = NULL;

	  for (attr = img->attrib; attr != NULL; attr = dr_next_attr(attr)) 
	  {
	       kwName = dr_attrname(attr);
	       kwComment = attr->comment;

	       if (kwName != NULL && strlen(kwName) > 0)
	       {
		    kwValue = dr_attrvalue(attr);
		    kwType = attr->datatype; /* Should be a DR API to provide this. */
		    printf("Examining keyword %s, with value %s\n", 
			   kwName, 
			   dr_attrvalue_str(attr));
		    
		    char sanitizedName[DRMS_MAXNAMELEN];
		    *error = SanitizeKeywordName(kwName, sanitizedName, sizeof(sanitizedName));

		    if (!*error)
		    {
		       if (strcmp(sanitizedName, kwName) != 0)
		       {
			  /* Had to convert FITS keyword name to DRMS keyword name.
			   * Save FITS keyword name */
			   keyFitsNames[kwIndex] = strdup(kwName);
		       }

		       int knownType = GetKnownType(kwName);
		       if (knownType == DR_TIME && kwType == DR_STRING)
		       {
			  /* Convert to time type. */
			  TIME t = sscan_time(kwValue);
			  if ( t >= 0 )
			  {
			     printf("Converted string time %s to double %f\n", 
				    (char *)kwValue, 
				    t);
			     keyTypes[kwIndex] = DrTypeToDRMSType(knownType);
			     keyValues[kwIndex] = (void *)malloc(sizeof(TIME));
			     *(TIME *)(keyValues[kwIndex]) = t;

			  }
			  else
			  {
			     *error = 1;
			  }
		       }
		       else
		       {
			  /* Use the existing type parsed by DR. */
			  keyTypes[kwIndex] = DrTypeToDRMSType(kwType);
			  keyValues[kwIndex] = dr_malloc_fillvalue(kwType);
			  if (kwType == DR_STRING)
			  {
			     keyValues[kwIndex] = strdup(kwValue);
			  }
			  else
			  {
			     memcpy(keyValues[kwIndex], kwValue, dr_sizeof(kwType));
			  }
		       }

		       if (!*error)
		       {
			  keyNames[kwIndex] = strdup(sanitizedName);
			  if (kwComment)
			  {
			     drComments[kwIndex] = strdup(kwComment);
			  }

			  kwIndex++;
		       }
		    }
	       }
	  } /* attribute loop */
	  
	  FixComments(drComments, keyFitsNames, keyComments, kwIndex);
	  if (drComments)
	  {
	     for (iKey = 0; iKey < kwIndex; iKey++)
	     {
		if (drComments[iKey])
		{
		   free(drComments[iKey]);
		}
	     }
	  }
     } /* !error */

     if (cleanImg)
     {
	  dr_free(&img);
     }

     return kwIndex;
}

static int GetKWDefVal(char *buf, int size, DRMS_Type_t drmsType)
{   
   int error = 0;
   DRMS_Type_Value_t defValue;

   drms_missing(drmsType, &defValue);

   if (!error)
   {
      char *defValueStr = drms2string(drmsType, &defValue, NULL);
      if (defValueStr)
      {
	 snprintf(buf, size, "%s", defValueStr);
	 free(defValueStr);
      }
      else if (drmsType == DRMS_TYPE_STRING)
      {
	 if (size >= strlen("<empty_default>"))
	 {
	    snprintf(buf, size, "<empty_default>");
	 }
	 else
	 {
	    fprintf(stderr, "Can't get default string value, buffer too small.\n");
	    error = 1;
	 }
      }
   }

   return error;
}

static int GetKWFormat(char *buf, int size, DRMS_Type_t drmsType)
{
   int error = 0;
   char formatStr[64];

   switch (drmsType)
   {
      case DRMS_TYPE_CHAR:
	formatStr[0] = '%';
	formatStr[1] = 'd';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_SHORT:
	formatStr[0] = '%';
	formatStr[1] = 'd';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_INT:
	formatStr[0] = '%';
	formatStr[1] = 'd';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_LONGLONG:
	formatStr[0] = '%';
	formatStr[1] = 'l';
	formatStr[2] = 'd';
	formatStr[3] = '\0';
	break;
      case DRMS_TYPE_FLOAT:
	formatStr[0] = '%';
	formatStr[1] = 'f';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_DOUBLE:
	formatStr[0] = '%';
	formatStr[1] = 'f';
	formatStr[2] = '\0';
	break;
      case DRMS_TYPE_TIME:
	snprintf(formatStr, sizeof(formatStr), "%s", "UTC");
	break;
      case DRMS_TYPE_STRING:
	formatStr[0] = '%';
	formatStr[1] = 's';
	formatStr[2] = '\0';
	break;
      default:
	fprintf(stderr, "Invalid drms type: %d\n", drmsType);
	error = 1;
   }

   if (!error)
   {
      snprintf(buf, size, "%s", formatStr);
   }

   return error;
}

static int GetKWUnit(char *buf, int size, DRMS_Type_t drmsType)
{
   int error = 0;

   /* Eventually, this might map to units of known keywords. */
   snprintf(buf, size, "%s", "<no_unit>");
   
   return error;
}

static int CreateKeywordDesc(char *buf, 
			     int size, 
			     const char *kwName, 
			     const char *kwComment,
			     DRMS_Type_t drmsType,
			     char *segScope)
{
   int error = 0;

   char typeStr[64];
   char defStr[64];
   char formatStr[64];
   char unitStr[64];

   if (drmsType < DRMS_TYPE_CHAR || drmsType > DRMS_TYPE_RAW)
   {
      error = 1;
   }
   else
   {
      snprintf(typeStr, sizeof(typeStr), "%s", drms_type_names[drmsType]);
   }

   if (!error)
   {
      error = GetKWFormat(formatStr, sizeof(formatStr), drmsType);
   }

   if (!error)
   {
      error = GetKWDefVal(defStr, sizeof(defStr), drmsType);
   }

   if (!error)
   {
      error = GetKWUnit(unitStr, sizeof(unitStr), drmsType);
   }
     
   if (!error)
   {
      snprintf(buf, 
	       size, 
	       "Keyword: %s, %s, variable, %s, %s, %s, %s, \"%s\"\n", 
	       kwName, typeStr, segScope, defStr, formatStr, unitStr, kwComment ? kwComment : "" );
   }

   return error;
}

static int CreateSegmentDesc(char *buf, 
			     int size, 
			     const char *segName, 
			     DRMS_Type_t drmsType,
			     const char *dimStr)
{
   int error = 0;

   snprintf(buf, 
	    size, 
	    "Data: %s, variable, %s, %s, \"unit\", \"fits\", \"FITS file with test data\"\n", 
	    segName, 
	    drms_type_names[drmsType],
	    dimStr);

   return error;
}

static int CreateSeries(DRMS_Env_t *drmsEnv, 
			const char *series, 
			const char *pkeys,
			DRMS_Type_t *drmsDataType,
			int nSegs,
			SegInfo_t *segInfo,
			HContainer_t *segSpKW,
			int nKeys, 
			DRMS_Type_t *kwTypes, 
			char **kwNames, 
			char **kwComments)
{
     int error = 0;

     int kwIndex = 0;
     char pkeyBuf[512];
     char kwDesc[4096];
     char segDesc[4096];
     char *kwPtr = kwDesc;
     char *segPtr = segDesc;

     char buf[128 + DRMS_MAXCOMMENTLEN];
   
     /* For segment-specific keys, create only one keyword. */
     CreateKeywordDesc(buf, sizeof(buf), kBZERO, NULL, DRMS_TYPE_DOUBLE, "segment");
     snprintf(kwPtr, sizeof(kwDesc) - (kwPtr - kwDesc), "%s", buf);
     kwPtr += strlen(buf);

     CreateKeywordDesc(buf, sizeof(buf), kBSCALE, NULL, DRMS_TYPE_DOUBLE, "segment");
     snprintf(kwPtr, sizeof(kwDesc) - (kwPtr - kwDesc), "%s", buf);
     kwPtr += strlen(buf);

     /* Create the primary key list.*/
     char *pPkeys = NULL;
     char *pks = strdup(pkeys);

     char *pBuf = pkeyBuf;
     int first = 1;

     for (pPkeys = strtok(pks, ","); pPkeys != NULL; pPkeys = strtok(NULL, ","))
     {
	  if (!first)
	  {
	       snprintf(pBuf, sizeof(pkeyBuf) - (pBuf - pkeyBuf), ", %s", pPkeys);
	       pBuf += 2 + strlen(pPkeys);
	  }
	  else
	  {
	       snprintf(pBuf, sizeof(pkeyBuf) - (pBuf - pkeyBuf), "%s", pPkeys);
	       pBuf += strlen(pPkeys); 
	       first = 0;
	  }
     }

     /* Create the series jsd. */
     char *segScope = NULL;
     kwIndex = 0;
     while (kwIndex < nKeys)
     {
	if (hcon_lookup(segSpKW, kwNames[kwIndex]))
	{
	   segScope = "segment";
	}
	else
	{
	   segScope = "record";
	}
	
	CreateKeywordDesc(buf, 
			  sizeof(buf), 
			  kwNames[kwIndex], 
			  kwComments[kwIndex], 
			  kwTypes[kwIndex],
			  segScope);
	
	snprintf(kwPtr, sizeof(kwDesc) - (kwPtr - kwDesc), "%s", buf);
	kwPtr += strlen(buf);
	kwIndex++;
     } /* while */

     char *dimStr = NULL;
     int iSeg = 0;

     /* The segment order in segMap matches the order in drmsDataTypes. */
     for (iSeg = 0; iSeg < nSegs; iSeg++)
     {
	dimStr = segInfo[iSeg].dimsStr;
	
	CreateSegmentDesc(buf,
			  sizeof(buf),
			  segInfo[iSeg].segname,
			  drmsDataType[iSeg],
			  dimStr);
	snprintf(segPtr, sizeof(segDesc) - (segPtr - segDesc), "%s", buf);
	segPtr += strlen(buf);
     }

     char *user = getenv("USER");

     char jsd[16384];
     snprintf(jsd,
	      sizeof(jsd),
	     "Seriesname:     %s\n"
	     "Author:         %s\n"
	     "Owners:         %s\n"
	     "Unitsize:       1\n"
	     "Archive:        0\n"
	     "Retention:      2\n"
	     "Tapegroup:      1\n"
	     "Index:          %s\n"
	     "Description:    \"Series of FITS files.\"\n"
	     "%s"
	     "%s",
	      series, 
	      user, 
	      user, 
	      pkeyBuf,
	      kwDesc, 
	      segDesc);

     /* Now actually create the series. */
     DRMS_Record_t *template = drms_parse_description(drms_env, jsd);
     
     if (template == NULL)
     {
	  fprintf(stderr, "Failed to parse series description.  JSD was:\n%s\n", jsd);
	  error = 1;
     }
     else
     {
	  printf("Creating series %s with description:\n%s\n", series, jsd);

	  if (drms_create_series_fromprototype(&template, series, 1) != DRMS_SUCCESS)
	  {
	       fprintf(stderr, "Failed to create series %s.\n", series);
	       error = 1;
	  }
     }

     return error;
}

static int FileFilter(const struct dirent *entry)
{
   char *oneFile = entry->d_name;
   struct stat stBuf;

   if (oneFile && !stat(oneFile, &stBuf))
   {
      if (S_ISREG(stBuf.st_mode))
      {
	 return 1;
      }
   }

   return 0;
}

static void ValFree(const void *val)
{
   void **realPtr = (void **)val;
   if (realPtr && *realPtr)
   {
      free(*realPtr);
   }
}

/* Creates and returns by reference segSpKeys. */
static int CreateSeriesFromFits(DRMS_Env_t *drmsEnv, 
				const char *series, 
				const char *pkeys,
				RecInfo_t *recinfo,
				int nRecs,
				int nSegs,
				SegInfo_t *segInfo,
				HContainer_t **segSpKeys)
{
   int error = 0;
   int iRec = 0;
   int iFile = 0; /* iFile <= iSeg, some files might be missing from a record */
   int iKey = 0;

   /* identifies which keywords require kw-specific names */
   *segSpKeys = hcon_create(sizeof(int),
			    sizeof(char) * DRMS_MAXNAMELEN,
			    NULL, NULL, NULL, NULL, 0);

   int nKeys = 0;
   DRMS_Type_t kwTypes[kMaxKeys];
   char *kwNames[kMaxKeys];    /* drms key names */
   char *kwComments[kMaxKeys]; /* drms comments */
   char *kwFitsNames[kMaxKeys];
   void *kwValues[kMaxKeys];   /* raw values */
   DRMS_Type_t *drmsDataType = (DRMS_Type_t *)malloc(sizeof(DRMS_Type_t) * nSegs);
   int naxis;
   int dims[DRMS_MAXRANK];
   double bzero;
   double bscale;

   int nomNKeys;
   DRMS_Type_t nomKWTypes[kMaxKeys];
   char *nomKWNames[kMaxKeys];
   char *nomKWComments[kMaxKeys];

   memset(kwNames, 0, sizeof(char *) * kMaxKeys);
   memset(kwComments, 0, sizeof(char *) * kMaxKeys);
   memset(kwFitsNames, 0, sizeof(char *) * kMaxKeys);
   memset(kwValues, 0, sizeof(void *) * kMaxKeys);

   memset(nomKWNames, 0, sizeof(char *) * kMaxKeys);
   memset(nomKWComments, 0, sizeof(char *) * kMaxKeys);
  
   /* Must check for segment-specific keywords. */
   for (iRec = 0; iRec < nRecs; iRec++)
   {
      RecInfo_t *ri = &(recinfo[iRec]);

      /* raw values of the last file opened */
      HContainer_t *tempVals = hcon_create(sizeof(void *),
					   sizeof(char) * kMAXFITSNAME,
					   ValFree, NULL, NULL, NULL, 0);

      for (iFile = 0; iFile < ri->nfiles; iFile++)
      {
	 /* If only one segment per record, this loop gets executed once. */
	 if (!error)
	 {
	    nKeys = ReadAndParseFitsFile(ri->files[iFile], 
					 kwTypes, 
					 kwNames, 
					 kwComments,
					 kwFitsNames,
					 kwValues,
					 &drmsDataType[iFile],
					 &naxis,
					 dims,
					 &bzero,
					 &bscale,
					 NULL, /* don't read in data */
					 &error);
	 }

	 if (!error)
	 {
	    /* Compare user-provided dim info with the dim info in the file. */
	    if (segInfo[iFile].naxis != naxis || 
		memcmp(segInfo[iFile].dims, dims, sizeof(int) * naxis) != 0)
	    {
	       error = 1;
	       fprintf(stderr, 
		       "Fits file's dimensions don't match dimensions specified for segment %s.\n", 
		       segInfo[iFile].segname);
	    }	    
	 }

	 if (!error && nSegs > 1)
	 {
	    /* Compare keyword values */
	    void **val = NULL;
	    DRMS_Type_Value_t tempDRMSVal;
	    DRMS_Type_Value_t currDRMSVal;
	    int dummy = 1;

	    for (iKey = 0; iKey < nKeys; iKey++)
	    {
	       if (!hcon_lookup(*segSpKeys, kwNames[iKey]))
	       {
		  if ((val = (void **)hcon_lookup(tempVals, kwNames[iKey])) != NULL)
		  {
		     ValToDRMSVal(kwTypes[iKey], *val, &tempDRMSVal);
		     ValToDRMSVal(kwTypes[iKey], kwValues[iKey], &currDRMSVal);

		     /* compare */
		     if (!drms_equal(kwTypes[iKey], &tempDRMSVal, &currDRMSVal))
		     {
			hcon_insert(*segSpKeys, kwNames[iKey], &dummy);
		     }

		     if (kwTypes[iKey] == DRMS_TYPE_STRING)
		     {
			if (tempDRMSVal.string_val)
			{
			   free(tempDRMSVal.string_val);
			}
			if (currDRMSVal.string_val)
			{
			   free(currDRMSVal.string_val);
			}
		     }
		  }
		  else
		  {
		     /* duplicate the value, becuz it's gonna get kilt below. */
		     void *v = NULL;

		     if (kwTypes[iKey] == DRMS_TYPE_STRING)
		     {
			v = strdup(kwValues[iKey]);
		     }
		     else
		     {
			v = malloc(drms_sizeof(kwTypes[iKey]));
			memcpy(v, kwValues[iKey], drms_sizeof(kwTypes[iKey]));
		     }

		     hcon_insert(tempVals, kwNames[iKey], &v);
		  }
	       }
	    }
	 }

	 if (iFile == 0)
	 {
	    nomNKeys = nKeys;
	    memcpy(nomKWTypes, kwTypes, sizeof(DRMS_Type_t) * kMaxKeys);
	    for (iKey = 0; iKey < nKeys; iKey++)
	    {
	       if (kwNames[iKey])
	       {
		  nomKWNames[iKey] = kwNames[iKey];
	       }

	       if (kwComments[iKey])
	       {
		  nomKWComments[iKey] = kwComments[iKey];
	       }
	    }
	 }
	
	 /* clean up */
	 for (iKey = 0; iKey < nKeys; iKey++)
	 {
	    if (iFile != 0)
	    {
	       if (kwNames[iKey])
	       {
		  free(kwNames[iKey]);
	       }

	       if (kwComments[iKey])
	       {
		  free(kwComments[iKey]);
	       }
	    }
	      
	    if (kwFitsNames[iKey])
	    {
	       free(kwFitsNames[iKey]);
	    }

	    if (kwValues[iKey])
	    {
	       free(kwValues[iKey]);
	    }
	 }

      } /* iFile */

      hcon_destroy(&tempVals);

      if (nSegs == 1)
      {
	 /* No need to check further for segment-specific keywords */
	 break;
      }
   } /* iRec */

   /* shouldn't be any pkeys that are segment-specific */
   if (!error)
   {
      if (nSegs > 1)
      {
	 char *cPkeys = strdup(pkeys);
	 char *lasts = NULL;

	 if (cPkeys)
	 {
	    char *aPkeyName = strtok_r(cPkeys, ",", &lasts);

	    for (; !error && aPkeyName; aPkeyName = strtok_r(NULL, ",", &lasts))
	    {
	       if (hcon_lookup(*segSpKeys, aPkeyName))
	       {
		  error = 1;
		  fprintf(stderr, "The prime-key values of segements within a record are not allowed to differ - cannot create series %s.\n", series);
		  break;
	       }
	    }

	    free(cPkeys);
	 }
      }
   }
   
   /* segSpKeys contains all the kw that require segment-specific names*/
   if (!error)
   {
      error = CreateSeries(drmsEnv,
			   series,
			   pkeys,
			   drmsDataType,
			   nSegs,
			   segInfo,
			   *segSpKeys,
			   nomNKeys,
			   nomKWTypes,
			   nomKWNames,
			   nomKWComments);
   }

   /* clean up */
   if (drmsDataType)
   {
      free(drmsDataType);
   }

   for (iKey = 0; iKey < kMaxKeys; iKey++)
   {
      if (nomKWNames[iKey])
      {
	 free(nomKWNames[iKey]);
      }

      if (nomKWComments[iKey])
      {
	 free(nomKWComments[iKey]);
      }
   }

   return error;
}

static int InsertRecord(DRMS_Record_t **record,
			DRMS_Env_t *drmsEnv,
			const char *series,
			int nKeys,
			DRMS_Type_t *kwTypes,
			char **kwNames,
			void ***kwValues,
			HContainer_t *segSpKeys,
			int nSegs,
			SegInfo_t *segInfo,
			DRMS_Type_t *drmsDataType,
			double *bzero,
			double *bscale,
			void **data)
{
   int error = 0;
   int status = 0;
   char *seriesName = strdup(series); /* Ack! */
   DRMS_Record_t *rec = NULL;
   int iSeg;

   if (seriesName)
   {
      rec = drms_create_record(drms_env, seriesName, DRMS_PERMANENT, &status);
      free(seriesName);
   }

   if (!rec || status != 0)
   {
      error = 1;
   }
     
   if (!error)
   {
      int kwIndex = 0;
      for (; kwIndex < nKeys && !error; kwIndex++)
      {
	 if (!hcon_lookup(segSpKeys, kwNames[kwIndex]))
	 {
	    error = SetDRMSKey(rec, 
			       kwTypes[kwIndex],
			       kwNames[kwIndex],
			       kwValues[0][kwIndex]);
	 }
	 else
	 {
	    char keyname[DRMS_MAXNAMELEN];
	    for (iSeg = 0; iSeg < nSegs; iSeg++)
	    {
	       snprintf(keyname, sizeof(keyname), "%s[%d]", kwNames[kwIndex], iSeg);
	       error =  SetDRMSKey(rec, 
				   kwTypes[kwIndex],
				   keyname,
				   kwValues[iSeg][kwIndex]);
	    }
	 }
      } /* key loop*/
   }
	  
   if (!error)
   {
      /* Create segment. */
      for (iSeg = 0; iSeg < nSegs; iSeg++)
      {
	 DRMS_Segment_t *segment = drms_segment_lookup(rec, segInfo[iSeg].segname);
	 if (segment) 
	 {
	    /* Set bzero and bscale keywords to 0 and 1.  DR always
	     * converts data so that bzero == 0 and bscale == 1.0. 
	     * Later conversions of the segment may change these values.
	     */
	    status = drms_segment_setscaling(segment, 0.0, 1.0);
			
	    if (status != 0)
	    {
	       error = 1;
	       fprintf(stderr, 
		       "Failed to set segment scaling, status %d\n",
		       status);
	    }

	    if (!error)
	    {
	       /* Create data array. */
	       DRMS_Array_t dataArr;
	       dataArr.naxis = segInfo[iSeg].naxis;
	       int iDim = 0;
	       for (; iDim < dataArr.naxis; iDim++)
	       {
		  dataArr.axis[iDim] = (segInfo[iSeg].dims)[iDim];
	       }
	       dataArr.bzero = 0.0;
	       dataArr.bscale = 1.0;
	       dataArr.type = drmsDataType[iSeg];
	       dataArr.israw = 1;
	       dataArr.data = data[iSeg];
			 
	       status = drms_segment_write(segment, &dataArr, 0);

	       if (status) 
	       {
		  error = 1;
		  fprintf(stderr, 
			  "ERROR: drms_segment_write failed with status = %d\n", 
			  status);
	       }
	    }
	 }
      }/* seg loop*/
   } /* !error */

   if (!error)
   {
      *record = rec;
   }
   else
   {
      *record = NULL;
   }
   
   return error;
}

static int ValidatePKeysAndSeries(DRMS_Env_t *env, 
				  const char *seriesName, 
				  const char *pkeys,
				  HContainer_t **segSpKeys)
{
   int status = DRMS_SUCCESS;
   int error = 0;

   /* If the output series exists, its primary keys must match the ones
    * provided in the parameters */

   HContainer_t *info = drms_keyword_createinfocon(env, seriesName, &status);
   if (status == DRMS_SUCCESS)
   {
      char *pks = strdup(pkeys);
      char *aKey = strtok(pks, ",");
	 
      for (; !error && aKey; aKey = strtok(NULL, ","))
      {
	 if (!hcon_lookup(info, aKey))
	 {
	    error = 1;
	    fprintf(stderr, "Primary key parameter not found in series %s.\n", seriesName);
	 }
      }

      *segSpKeys = hcon_create(sizeof(int),
			       sizeof(char) * DRMS_MAXNAMELEN,
			       NULL, NULL, NULL, NULL, 0);

      if (*segSpKeys)
      {
	 HIterator_t *hit = hiter_create(info);
	 if (hit)
	 {
	    DRMS_KeywordInfo_t *ki = NULL;
	    int dummy = 1;

	    while((ki = (DRMS_KeywordInfo_t *)hiter_getnext(hit)) != NULL)
	    {
	       if (ki->per_segment)
	       {
		  hcon_insert(*segSpKeys, ki->name, &dummy);
	       }
	    }

	    hiter_destroy(&hit);
	 }
      }

      drms_keyword_destroyinfocon(&info);
   }
   else
   {
      error = 1;
   }

   return !error;
}

/* validates for one fits file */
static int ValidateDataAndSeries(DRMS_Env_t *env,
				 const char *seriesName,
				 const char *pkeys, 
				 const char *segName,
				 int naxisSpec,
				 int *dimsSpec,
				 int nKeys, 
				 DRMS_Type_t *kwTypes, 
				 char **kwNames, 
				 char **kwFitsNames,
				 DRMS_Type_t drmsDataType,
				 int naxis, 
				 int *dims)
{
   int status = DRMS_SUCCESS;
   int error = 0;
   int iKeys = 0;

   if (naxisSpec == naxis && !memcmp(dimsSpec, dims, sizeof(int) * naxis))
   {
      /* Check keywords - we have the kw name and type only. */
      HContainer_t *kwInfo = drms_keyword_createinfocon(env, seriesName, &status);

      XASSERT(status == DRMS_SUCCESS);
      if (status == DRMS_SUCCESS)
      {
	 for (iKeys = 0; !error && iKeys < nKeys; iKeys++)
	 {
	    DRMS_Type_t type = kwTypes[iKeys]; 
	    char *drmsName = kwNames[iKeys];
	    char *fitsName = kwFitsNames[iKeys];

	    /* For everything else, use existing series' information. */
	    DRMS_KeywordInfo_t *existinfo = hcon_lookup(kwInfo, drmsName);
	    if (kwInfo)
	    {
	       /* There is a key in the existing series with the same name as this one. */
	       /* Check keyword types against existing series. */
	       if (type != existinfo->type)
	       {
		  error = 1;
	       }
	    }
	    else
	    {
	       error = 1;
	       fprintf(stderr, 
		       "Existing series %s has no Fits keyword %s.\n", 
		       seriesName,
		       fitsName);
	    }
	 }

	 drms_keyword_destroyinfocon(&kwInfo);
      }
      else
      {
	 error = 1;
	 fprintf(stderr, "Couldn't obtain keyword info for series %s.\n", seriesName);
      }

      if (!error)
      {
	 /* Check segments - we have the type, name, axis, and dim array only. */
	 HContainer_t *segInfo = drms_segment_createinfocon(env, seriesName, &status);

	 XASSERT(status == DRMS_SUCCESS);
	 if (status == DRMS_SUCCESS)
	 {
	    DRMS_SegmentInfo_t *sInfo = hcon_lookup(segInfo, segName);
	    if (sInfo)
	    {
	       if (drmsDataType != sInfo->type ||
		   strcmp(segName, sInfo->name) != 0 ||
		   naxis != sInfo->naxis)
	       {
		  error = 1;
		  fprintf(stderr, "Fits data incompatible with series %s.\n", seriesName);
	       }
	       else
	       {
		  /* Don't need to check dimensions of series if seg scope is vardim. */
		  if (sInfo->scope != DRMS_VARDIM)
		  {
		     /* Use template record to get segment's dimensions */
		     DRMS_Record_t *template = drms_template_record(env, seriesName, &status);
		     DRMS_SegmentDimInfo_t seriesDimInfo;
		     DRMS_Segment_t *segtemplate = drms_segment_lookup(template, segName);
		     drms_segment_getdims(segtemplate, &seriesDimInfo);
		     const int *seriesDims = &(seriesDimInfo.axis);

		     int i = 0;
		     for (; i < naxis; i++)
		     {
			if (dims[i] != seriesDims[i])
			{
			   error = 1;
			   fprintf(stderr, 
				   "Fits data incompatible with series %s.\n", 
				   seriesName);
			   break;
			}
		     }
		  }
	       }
	    }
	    else
	    {
	       error = 1;
	       fprintf(stderr, 
		       "Existing series %s has no segment named %s.\n", 
		       seriesName, 
		       segName);
	    }

	    drms_segment_destroyinfocon(&segInfo);
	 }
      }
   }
   else
   {
      /* dim info doesn't match */
      error = 1;
      fprintf(stderr, 
	      "Fits file's dimensions don't match dimensions specified for segment %s.\n", 
	      segName);
   }
   
   return !error;
}

static int IngestOneRecord(DRMS_Env_t *env, 
			   const char *seriesName,
			   const char *pkeys,
			   int nSegs,
			   SegInfo_t *segInfo,
			   HContainer_t *segSpKeys,
			   char **files)
{
   int err = 0;
   
   int nKeys;
   DRMS_Type_t kwTypes[kMaxKeys];
   char *kwNames[kMaxKeys];    /* drms key names */
   char *kwComments[kMaxKeys]; /* drms comments */
   char *kwFitsNames[kMaxKeys];
   void ***kwValues = NULL;    /* dr values (fix) */
   DRMS_Type_t *drmsDataTypes = NULL;
   int *naxis = NULL;
   int **dims = NULL;
   double *bzero = NULL;
   double *bscale = NULL;
   void **data = NULL;
   DRMS_Record_t *rec = NULL;
   int iKey = 0;
   int iSeg = 0;

   kwValues = (void ***)malloc(sizeof(void **) * nSegs);
   drmsDataTypes = (DRMS_Type_t *)malloc(sizeof(DRMS_Type_t) * nSegs);
   naxis = (int *)malloc(sizeof(int) * nSegs);
   dims = (int **)malloc(sizeof(int *) * nSegs);
   bzero = (double *)malloc(sizeof(double) * nSegs);
   bscale = (double *)malloc(sizeof(double) * nSegs);
   data = (void **)malloc(sizeof(void *) * nSegs);

   memset(kwNames, 0, sizeof(char *) * kMaxKeys);
   memset(kwComments, 0, sizeof(char *) * kMaxKeys);
   memset(kwFitsNames, 0, sizeof(char *) * kMaxKeys);
   memset(kwValues, 0, sizeof(void **) * nSegs);
   memset(data, 0, sizeof(void *) * nSegs);

   /* Read and validate one file at a time, but ingest one record 
    * at a time. */
   for (iSeg = 0; iSeg < nSegs; iSeg++)
   {
      kwValues[iSeg] = (void **)malloc(sizeof(void *) * kMaxKeys);
      memset(kwValues[iSeg], 0, sizeof(void *) * kMaxKeys);
      dims[iSeg] = (int *)malloc(sizeof(int) * DRMS_MAXRANK);
      nKeys = ReadAndParseFitsFile(files[iSeg], 
				   kwTypes, 
				   kwNames, 
				   kwComments,
				   kwFitsNames,
				   kwValues[iSeg],
				   &(drmsDataTypes[iSeg]),
				   &(naxis[iSeg]),
				   dims[iSeg],
				   &(bzero[iSeg]),
				   &(bscale[iSeg]),
				   &(data[iSeg]),
				   &err);

      if (!err)
      {
	 /* Validate output series, create output series if necessary, 
	  * ensure output series is compatible with data to be written. */
	 err = !ValidateDataAndSeries(env, 
				      seriesName, 
				      pkeys, 
				      segInfo[iSeg].segname,
				      segInfo[iSeg].naxis,
				      segInfo[iSeg].dims,
				      nKeys, 
				      kwTypes, 
				      kwNames, 
				      kwFitsNames,
				      drmsDataTypes[iSeg],
				      naxis[iSeg], 
				      dims[iSeg]);
      }
   } /* seg loop */

   if (!err)
   {
      err = InsertRecord(&rec,
			 env,
			 seriesName,
			 nKeys,
			 kwTypes,
			 kwNames,
			 kwValues,
			 segSpKeys,
			 nSegs,
			 segInfo,
			 drmsDataTypes,
			 bzero,
			 bscale,
			 data);
   }

   if (rec != NULL)
   {
      if (!err)
      {
	 drms_close_record(rec, DRMS_INSERT_RECORD);
      }
      else
      {
	 drms_close_record(rec, DRMS_FREE_RECORD);
      }
   }
  
   /* Free up all the stuff created by IngestFile(). */
   for (iKey = 0; iKey < kMaxKeys; iKey++)
   {
      if (kwNames[iKey])
      {
	 free(kwNames[iKey]);
      }
      if (kwComments[iKey])
      {
	 free(kwComments[iKey]);
      }
      if (kwFitsNames[iKey])
      {
	 free(kwFitsNames[iKey]);
      }
   }

   for (iSeg = 0; iSeg < nSegs; iSeg++)
   {
      if (kwValues)
      {
	 if (kwValues[iSeg])
	 {
	    for (iKey = 0; iKey < kMaxKeys; iKey++)
	    {
	       if (kwValues[iSeg][iKey])
	       {
		  free(kwValues[iSeg][iKey]);
	       }
	    }

	    free(kwValues[iSeg]);
	 }
      }

      if (dims)
      {
	 if (dims[iSeg])
	 {
	    free(dims[iSeg]);
	 }
      }

      if (data)
      {
	 if (data[iSeg])
	 {
	    free(data[iSeg]);
	 }
      }
   }

   if (kwValues)
   {
      free(kwValues);
   }

   if (dims)
   {
      free(dims);
   }

   if (data)
   {
      free(data);
   }

   if (drmsDataTypes)
   {
      free(drmsDataTypes);
   }

   if (naxis)
   {
      free(naxis);
   }

   if (bzero)
   {
      free(bzero);
   }

   if (bscale)
   {
      free(bscale);
   }

   return err;
}

static int ParseFiles(const char *files, char ***fileNArr, int *nFiles)
{
   int error = 0;

   /* Parse file list */
   char *filel = strdup(files);
   char *aFile = NULL;
   int iFile;
   struct stat stBuf;

   /* Count number of files */
   if (filel)
   {
      char *place = NULL;
      aFile = strtok_r(filel, ",", &place);
      for (; !error && aFile; aFile = strtok_r(NULL, ",", &place))
      {
	 (*nFiles)++;
      }

      free(filel);
   }

   *fileNArr = (char **)malloc(sizeof(char *) * *nFiles);
   memset(*fileNArr, 0, sizeof(char *) * *nFiles);

   filel = strdup(files);
   char *lasts = NULL;
   aFile = strtok_r(filel, ",", &lasts);
   for (iFile = 0; !error && aFile; aFile = strtok_r(NULL, ",", &lasts), iFile++)
   {
      /* stat - determine if file or directory */
      if (stat(aFile, &stBuf))
      {
	 fprintf(stderr, "Unable to locate file %s.\n", aFile);
	 continue;
      }

      if (S_ISREG(stBuf.st_mode))
      {
	 (*fileNArr)[iFile] = strdup(aFile);
      }
      else if (S_ISDIR(stBuf.st_mode))
      {
	 struct dirent **fileList = NULL;
	 int nFiles = -1;

	 if ((nFiles = scandir(aFile, &fileList, FileFilter, NULL)) > 0 && fileList != NULL)
	 {
	    int fileIndex = 0;
	    while (fileIndex < nFiles && !error)
	    {
	       struct dirent *entry = fileList[fileIndex];
	       if (entry != NULL )
	       {
		  char *oneFile = entry->d_name;
		  if (oneFile != NULL && *oneFile !=  '\0');
		  {
		     (*fileNArr)[iFile] = strdup(oneFile);
		  }

		  free(entry);
	       }
		 
	       fileIndex++;
	    } /* while */
	      
	    free(fileList);
	 }
      } /* dir */
      else
      {
	 error = 1;
	 fprintf(stderr, "%s is not a file or directory.\n", aFile);
      }
   } /* iFile */

   return error;
}

int DoIt (void) 
{
   char *seriesName = NULL;
   char *pkeys = NULL;
   char *kwMapFile = NULL;
   char *segInfoList = NULL;

   int bCreateSeries = 0;

   int status = 0;
   int error = 0;

   int nSegs = 0;
   int iSeg = 0;
   SegInfo_t *segInfo = NULL;
   RecInfo_t *recInfo = NULL;
   int iRec = 0;

   HContainer_t *segSpKeys = NULL;

   seriesName = cmdparams_get_str(&cmdparams, kParamSeries, NULL);
   pkeys = cmdparams_get_str(&cmdparams, kPrimeKeys, NULL);
   kwMapFile = cmdparams_get_str(&cmdparams, kMapFile, NULL);
   segInfoList = cmdparams_get_str(&cmdparams, kSegInfo, NULL);

   if (!gMapFileStr && kwMapFile && strcmp(kwMapFile, kParamUnspec) != 0 && *kwMapFile != '\0')
   {
      gMapFileStr = strdup(kwMapFile);
   }

   if (!error)
   {
      if (!drms_series_exists(drms_env, seriesName, &status))
      {
	 bCreateSeries = 1;
      }
      else
      {
	 error = !ValidatePKeysAndSeries(drms_env, seriesName, pkeys, &segSpKeys);
	 if (error)
	 {
	    fprintf(stderr, 
		    "Cannot ingest specified files into existing series %s\n", 
		    seriesName);
	 }
      }
   }

   if (!error)
   {
      char *lasts = NULL;
      char *endptr = NULL;
      char dimStr[kMAXDIMSSTR];
      char *anSI = NULL;

      /* Count number of segments */
      char *segInfoStr = strdup(segInfoList);

      if (segInfoStr)
      {
	 char *place = NULL;
	 anSI = strtok_r(segInfoStr, "[]", &place);

	 for (; !error && anSI; anSI = strtok_r(NULL, "[]", &place))
	 {
	    nSegs++;
	 }

	 free(segInfoStr);
      }

      segInfo = (SegInfo_t *)malloc(sizeof(SegInfo_t) * nSegs);
      memset(segInfo, 0, sizeof(SegInfo_t) * nSegs);

      segInfoStr = strdup(segInfoList);
      anSI = strtok_r(segInfoStr, "[]", &lasts);
      for (iSeg = 0; !error && anSI; anSI = strtok_r(NULL, "[]", &lasts), iSeg++)
      {
	 SegInfo_t *si = &(segInfo[iSeg]);
	 char *pAt = strchr(anSI, '@');
	 if (pAt && *(pAt + 1) != '\0')
	 {
	    *pAt ='\0';
	      
	    char *place = NULL;
	    char *theSegName = strtok_r(anSI, ",", &place);
	    char *firstDim = theSegName + strlen(theSegName) + 1;

	    if (theSegName && *firstDim != '\0')
	    {
	       snprintf(si->segname, sizeof(si->segname), "%s", theSegName);
	       snprintf(dimStr, sizeof(dimStr), "%s", firstDim);
	       char *aDim = strtok_r(NULL, ",", &place);

	       for (; !error && aDim && si->naxis < DRMS_MAXRANK; 
		    aDim = strtok_r(NULL, ",", &place))
	       {
		  si->dims[si->naxis] = (int)strtol(aDim, &endptr, 10);
		  if (endptr == aDim)
		  {
		     error = 1;
		     break;
		  }

		  si->naxis++;
	       }

	       /* Parse files */
	       ParseFiles(pAt + 1, &(si->files), &(si->nfiles));		 
	       snprintf(si->dimsStr, kMAXDIMSSTR, "%d,%s", si->naxis, dimStr);
	    }
	    else
	    {
	       error = 1;
	    }
	 }
	 else
	 {
	    error = 1;
	 }

	 if (error)
	 {
	    fprintf(stderr, "Badly formed segment information %s.\n", anSI);
	 }
      } /* segInfo loop */

      /* Ack!  If more than one seg, need to read ALL files' headers 
       * before creating series because might be some segment-specific 
       * keywords.  Ugly. */
      
      int minNFiles = segInfo[0].nfiles;

      for (iSeg = 1; iSeg < nSegs; iSeg++)
      {
	 if (segInfo[iSeg].nfiles < minNFiles)
	 {
	    minNFiles = segInfo[iSeg].nfiles;
	    fprintf(stderr, "Inconsistent number of files per segment.\n");
	 }
      }

      recInfo = (RecInfo_t *)malloc(sizeof(RecInfo_t) * minNFiles);
      if (recInfo)
      {
	 for (iRec = 0; iRec < minNFiles; iRec++)
	 {
	    recInfo[iRec].nfiles = nSegs;
	    recInfo[iRec].files = (char **)malloc(sizeof(char *) * nSegs);
	      
	    for (iSeg = 0; iSeg < nSegs; iSeg++)
	    {
	       recInfo[iRec].files[iSeg] = strdup(segInfo[iSeg].files[iRec]);		 
	    }
	 }
      }

      if (bCreateSeries)
      {
	 error = CreateSeriesFromFits(drms_env, 
				      seriesName,
				      pkeys,
				      recInfo,
				      minNFiles, /* essentially, the number of recs */
				      nSegs,
				      segInfo,
				      &segSpKeys);
      }

      /* Loop through recInfos and ingest each recInfo's files at the same time */
      if (recInfo)
      {
	 for (iRec = 0; iRec < minNFiles; iRec++)
	 {	      
	    error = IngestOneRecord(drms_env, 
				    seriesName, 
				    pkeys, 
				    nSegs,
				    segInfo,
				    segSpKeys,
				    recInfo[iRec].files);
	    /* Keep going if one or more files fails to ingest. */
	 }	   
      }
   }

   if (gDict)
   {
      hcon_destroy(&gDict);
   }

   if (gKeyMap)
   {
      drms_keymap_destroy(&gKeyMap);
   }

   if (segSpKeys)
   {
      hcon_destroy(&segSpKeys);
   }

   if (gMapFileStr)
   {
      free(gMapFileStr);
      gMapFileStr = NULL;
   }
     
   return error;
}
