/* extractFdsStateV.c */

/* Extracts heliocentric state data, for the input date and from the input data series.
 *   Inputs:
 *     Data series in - the fully qualified name of the data series that contains the FDS data
 *     Time range - the range of dates to which the extraction should be restricted
 *     Data series out - the fully qualified name of the data series in which to store 
 *       the extracted data
 */

#include <stdio.h>
#include <stdlib.h>

#include "jsoc_main.h"
#include "cmdparams.h"
#include "drms_env.h"

#define kDefaultSeriesIn "sdo.fds"
#define kDefaultSeriesOut "sdo.fdsStateVectors"
#define kObsTimeKey "OBS_DATE"
#define kFdsDataProductKeyVal "[FDS_DATA_PRODUCT=predHelioOrb]"
#define kSVFileSegName "FILENAME"

#define kSVLineMax 1024

/* State-vector data keys */
#define kSVKeyPrimary "OBS_DATE"
#define kSVKeyX "X"
#define kSVKeyY "Y"
#define kSVKeyZ "Z"
#define kSVKeyVx "Vx"
#define kSVKeyVy "Vy"
#define kSVKeyVz "Vz"


ModuleArgs_t module_args[] =
{
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_STRING, "seriesIn", "NOT SPECIFIED", "name of series containing FDS data"},
  {ARG_STRING, "timeRange", "NOT SPECIFIED", "time ranges separated by commas"},
  {ARG_STRING, "seriesOut", "NOT SPECIFIED", "name of series in which to save extracted data"},
  {ARG_END}
};

char *module_name = "extractFdsStateV";

int nice_intro ()
{
     int usage = cmdparams_get_int(&cmdparams, "h", NULL);
     if (usage)
     {
	  printf ("Usage:\n\textractFdsStateV [-h] "
		  "[seriesIn=<seriesname>] [timeRange=<timerange>] [seriesOut=<seriesname>]\n"
		  "  details are:\n"
		  "  -h: help - show this message then exit\n"
		  "  <seriesname> - fully qualified series name.\n"
		  "  <timerange> - time value range set.\n"
		  "  seriesIn defaults to sdo.fds.\n"
		  "  timeRange defaults to all records in seriesIn.\n"
		  "  seriesOut defaults to sdo.fdsStateVectors.\n"
		  "  example - extractFdsStateV seriesIn=su_arta.TestFDSData timeRange=2006.11.20_22:38:00-2006.11.20_22:45:00,2006.11.20_22:52:00. seriesOut=su_arta.TestFDSHelio\n");
	  return(1);
     }
     return (0);
}

/* Sets all hours, minutes, and seconds to 0 */
typedef enum
{
  kStripTimeStart = 0,
  kStripTimeYear,
  kStripTimeMonth,
  kStripTimeDay,
  kStripTimeHoursOrTimeZone,
  kStripTimeHours,
  kStripTimeMinutes,
  kStripTimeSeconds,
  kStripTimeTimeZone,
  kStripTimeBadFormat
} StripTimeState_t;

static int StripTimeRange(char *fullTR, char **strippedTR)
{
     int error = 0;

     char *p = fullTR;
     char *beginTime = fullTR;
     char *stripped = (char *)malloc(sizeof(char) * strlen(fullTR) + 64);
     char *strippedPtr = stripped;
     int len = strlen(fullTR);

     bzero(stripped, sizeof(char) * strlen(fullTR) + 64);

     int year;
     int month;
     int day;
     int hours;
     int minutes;
     double seconds;
     char timeZone[128];

     char *beginTZ = NULL;

     StripTimeState_t state = kStripTimeStart;

     do
     {
	  switch (state)
	  {
	  case kStripTimeStart:
	    {
		 if (sscanf(p, "%d", &year) == 0)
		 {
		      state = kStripTimeBadFormat;
		 }
		 else
		 {
		      beginTime = p;
		      state = kStripTimeYear;
		 }

		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeYear:
	    {
		 if (*p == '.')
		 {
		      state = kStripTimeMonth;
		 }
		 else if (sscanf(p, "%d", &year) == 0)
		 {
		      printf("Bad time format - parsing year (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }
		 
		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeMonth:
	    {
		 if (*p == '.')
		 {
		      state = kStripTimeDay;
		 }
		 else if (sscanf(p, "%d", &month) == 0)
		 {
		      printf("Bad time format - parsing month (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }
		 
		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeDay:
	    {
		 if (*p == ':')
		 {
		      printf("Bad time format - parsing day (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }
		 else if (*p == '_')
		 { 
		      /* Everything between beginTime and p has the year/month/day - need to find the timezone */
		      strncpy(strippedPtr, beginTime, p - beginTime);
		      strippedPtr += (p - beginTime);
		      
		      state = kStripTimeHoursOrTimeZone;
		      beginTZ = p; /* In case the following is indeed a timezone */
		 }
		 else if (*p == '-' || *p == ',')
		 {
		      /* Everything between beginTime and p has the year/month/day - need to find the timezone */
		      strncpy(strippedPtr, beginTime, p - beginTime);
		      strippedPtr += (p - beginTime);

		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      *strippedPtr = *p;
		      strippedPtr++;
		      state = kStripTimeStart;
		 }
		 else if (*p == '/')
		 {
		      /* Parse duration */
		 }
		 else if (sscanf(p, "%d", &day) == 0)
		 {
		      state = kStripTimeBadFormat;
		 }
		 
		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeHoursOrTimeZone:
	    {
		 if (sscanf(p, "%d", &hours) != 0)
		 {
		      state = kStripTimeHours;
		 }
		 else
		 {
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      state = kStripTimeTimeZone;
		 }
	    }
	    break;

	  case kStripTimeHours:
	    {
		 if (*p == ':')
		 {
		      state = kStripTimeMinutes;
		 }
		 else if (*p == '_')
		 {
		      beginTZ = p; 
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      state = kStripTimeTimeZone;
		 }
		 else if (*p == '-' || *p == '/' || *p == ',')
		 {
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      *strippedPtr = *p;
		      strippedPtr++;
		      state = kStripTimeStart;
		 }
		 else if (sscanf(p, "%d", &hours) == 0)
		 {
		      printf("Bad time format - parsing hours (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }
		 
		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeMinutes:
	    {
		 if (*p == ':')
		 {
		      state = kStripTimeSeconds;
		 }
		 else if (*p == '_')
		 {
		      beginTZ = p; 
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      state = kStripTimeTimeZone;
		 }
		 else if (*p == '-' || *p == '/' || *p == ',')
		 {
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      *strippedPtr = *p;
		      strippedPtr++;
		      state = kStripTimeStart;
		 }
		 else if (sscanf(p, "%d", &minutes) == 0)
		 {
		      printf("Bad time format - parsing minutes (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }

		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeSeconds:
	    {
		 if (*p == '_')
		 {
		      beginTZ = p; 
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      state = kStripTimeTimeZone;
		 }
		 else if (*p == '-' || *p == '/' || *p == ',')
		 {
		      strncpy(strippedPtr, "_00:00", 6);
		      strippedPtr += 6;
		      *strippedPtr = *p;
		      strippedPtr++;
		      state = kStripTimeStart;
		 }
		 else if (sscanf(p, "%lf", &seconds) == 0)
		 {
		      printf("Bad time format - parsing seconds (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }
		 
		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeTimeZone:
	    {
		 if (*p == '-' || *p == '/' || *p == ',')
		 {
		      strncpy(strippedPtr, beginTZ, p - beginTZ);
		      strippedPtr += (p - beginTZ);
		      *strippedPtr = *p;
		      strippedPtr++;
		      state = kStripTimeStart;
		 }
		 else if (sscanf(p, "%s", timeZone) == 0)
		 {
		      printf("Bad time format - parsing time zone (found %s).\n", p);
		      state = kStripTimeBadFormat;
		 }
		 
		 if (state != kStripTimeBadFormat)
		 {
		      p++;
		 }
	    }
	    break;

	  case kStripTimeBadFormat:
	    {
		 error = 1;
		 printf("Bad time format.\n");
	    }
	    break;
	  }

     } while (p - fullTR < len && error == 0);
    
     /* We've run out of input chars - sometimes this leaves a valid time */

     char lastChar[2];
     lastChar[0]= fullTR[strlen(fullTR) - 1];
     lastChar[1] = '\0';
     int testInt;

     if (state == kStripTimeStart || state == kStripTimeYear || state == kStripTimeMonth)
     {
	  printf("Bad time format - missing year or month or day.\n");
	  state = kStripTimeBadFormat;
	  error = 1;	  
     }
     else if (state == kStripTimeDay)
     {
	  if (sscanf(lastChar, "%d", &testInt) == 1)
	  {
	       strncpy(strippedPtr, beginTime, p - beginTime);
	       strippedPtr += (p - beginTime);
	       strncpy(strippedPtr, "_00:00", 6);
	       strippedPtr += 6;
	  }
	  else
	  {
	       printf("Bad time format - invalid day.\n");
	       state = kStripTimeBadFormat;
	  }
     }
     else if (state == kStripTimeHoursOrTimeZone)
     {
	  printf("Bad time format - missing hours:minutes:seconds or timezone.\n");
	  state = kStripTimeBadFormat;
	  error = 1;
     }
     else if (state == kStripTimeHours || state == kStripTimeMinutes || state == kStripTimeSeconds)
     { 
	  if (sscanf(lastChar, "%d", &testInt) == 1)
	  {
	       strncpy(strippedPtr, "_00:00", 6);
	       strippedPtr += 6;
	  }
	  else
	  {
	       printf("Bad time format - invalid minutes or seconds.\n");
	       state = kStripTimeBadFormat;
	  }
     }
     else if (state == kStripTimeTimeZone)
     {
	  strncpy(strippedPtr, beginTZ, p - beginTZ);
	  strippedPtr += (p - beginTZ);
     }

     if (error == 0)
     {
	  *strippedTR = stripped;
     }
     else
     {
	  free(stripped);
     }
     
     return error;
}

/* Caller must clean up records by calling drms_close_records() */
static int FetchRecords(DRMS_Env_t *drmsEnv, char *query, DRMS_RecordSet_t **recordSet, int *nRecs)
{
     int error = 0;

     *recordSet = drms_open_records(drmsEnv, query, &error);

     if (error != 0 || recordSet == NULL) 
     {
	  printf("drms_open_records failed, recSetQuery=%s, error=%d.  Aborting.\n", query, error);
     }
     else
     {
	  *nRecs = (*recordSet)->n;
     }

     return error;
}

static void FreeRecords(DRMS_RecordSet_t *recSet)
{
     drms_close_records(recSet, DRMS_FREE_RECORD);
}

static int FetchSegments(DRMS_RecordSet_t *recSet, int nRecs, char *segName, DRMS_Segment_t ***segs, int *nSegs)
{
     int error = 0;
     *nSegs = 0;

     /* Loop through records in recSet */
     int firstRec = 0;
     int lastRec = nRecs - 1; /* zero-based index */
     int iRec;

     /* Max number of segs is one per record */
     DRMS_Segment_t **newSegs = (DRMS_Segment_t **)malloc(sizeof(DRMS_Segment_t *) * nRecs);

			      
     /* loop over set of selected records */
     int index = 0;
     for (iRec = firstRec; iRec <= lastRec && error == 0; iRec++) 
     {
	  /* pointer to current record */
	  DRMS_Record_t *rec = recSet->records[iRec];  

	  if (rec)
	  {
	       DRMS_Segment_t *recSeg = drms_segment_lookup(rec, segName); 
	       if (recSeg)
	       {
		    newSegs[index++] = recSeg;
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
     }

     *nSegs = index;
     *segs = newSegs;

     return error;
}

static void FreeSegments(DRMS_Segment_t **segments)
{
     free(segments);
}

static int ParseSVRecFields(char *recBuf, char **date, double *xVal, double *yVal, double *zVal, 
			    double *vxVal, double *vyVal, double *vzVal)
{
     int error = 0;
     char *token;
     char *line = strdup(recBuf);

     if (line != NULL)
     {
	  token = strtok(line, " ");

	  if (token != NULL)
	  {
	       /* Must convert date to something that drms recognizes (calendar date) */
	       char year[8];
	       char day[8];
	       char hour[8];
	       char minute[8];

	       strncpy(year, token, 4);
	       year[4] = '\0';
	       strncpy(day, &token[4], 3);
	       day[3] = '\0';
	       strncpy(hour, &token[8], 2);
	       hour[2] = '\0';
	       strncpy(minute, &token[10], 2);
	       minute[2] = '\0';

	       char timeBuf[64];
	       snprintf(timeBuf, sizeof(timeBuf), "%s.01.%s_%s:%s_UT", year, day, hour, minute);

	       *date = strdup(timeBuf);
	  
	       if ((token = strtok(NULL, " ")) != NULL)
	       {
		    sscanf(token, "%lf", xVal);
	       }
	       else
	       {
		    error = 1;
	       }

	       if (error == 0)
	       {
		    if ((token = strtok(NULL, " ")) != NULL)
		    {
			 sscanf(token, "%lf", yVal);
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	       
	       if (error == 0)
	       {
		    if((token = strtok(NULL, " ")) != NULL)
		    {
			 sscanf(token, "%lf", zVal);
		    }
		    else
		    {
			 error = 1;
		    }
	       }

	       if (error == 0)
	       {
		    if ((token = strtok(NULL, " ")) != NULL)
		    {
			 sscanf(token, "%lf", vxVal);
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	       
	       if (error == 0)
	       {
		    if ((token = strtok(NULL, " ")) != NULL)
		    {
			 sscanf(token, "%lf", vyVal);
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	       
	       if (error == 0)
	       {
		    if ((token = strtok(NULL, " ")) != NULL)
		    {
			 sscanf(token, "%lf", vzVal);
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	  }
     }
     else
     {
	  error = 1;
     }

     return error;
}

static int ParseDuration(char *durationStr, double *duration)
{
     int error = 0;

     char *end;
     char *p = durationStr;
     double dval = (int)strtod(p, &end);

     if ((dval == 0 && end==p) || 
	 ((dval == HUGE_VALF || dval == -HUGE_VALF) && errno == ERANGE))
     {
	  fprintf(stderr,"Syntax Error: Expected finite floating point value at "
		  "beginning of time duration, found '%s'.\n", p);
	  error = 1;
     }
     else
     {
	  p = end;
     }

     if (error == 0)
     {
	  switch(*p++)
	  {
	  case 's':
	    *duration = 1.0 * dval;
	    break;
	  case 'm':
	    *duration = 60.0 * dval;
	    break;
	  case 'h':
	    *duration = 3600.0 * dval;
	    break;
	  case 'd':
	    *duration = 86400.0 * dval;
	    break;
	  default:
	    fprintf(stderr,"Syntax Error: Time duration unit must be one of 's', "
		    "'m', 'h', 'd', found '%c', '%s'.\n", *(p-1), p);
	    error = 1;
	  }
     }

     return error;
}

/* If <timeQuery> is within the range specified by <rs>, *bAns is set to 1, otherwise it is set to 0 */
/* The data types in <rs> must be of type DRMS_TYPE_TIME */
/* Adapted from sql_primekey_value_set() */
static int TimeInRange(char *time, ValueRangeSet_t *rs, int *bAns)
{
     int error = 0;
     int bInRange = 0;

     /* Convert time string into drmsTime */
     TIME timeQuery = sscan_time(time) - sscan_time("1977.01.01_00:00_TAI");

     do 
     {
	  if (rs->type == SINGLE_VALUE)
	  {
	       bInRange = (rs->start.time_val == timeQuery) ? 1 : 0;
	       if (bInRange == 1)
	       {
		    break;
	       }
	  }
	  else
	  {
	       if (rs->type == START_END)
	       {
		    /* For the interval case, rs->x is the end of the interval */
		    if (timeQuery >= rs->start.time_val && timeQuery <= rs->x.time_val)
		    {
			 bInRange = 1;
			 break;
		    }
	       }
	       else if (rs->type == START_DURATION)
	       {
		    /* For the duration case, rs->x is the increment from rs->start */
		    TIME endTime = rs->start.time_val + rs->x.time_val;
		    if (timeQuery >= rs->start.time_val && timeQuery < endTime)
		    {
			 bInRange = 1;
			 break;
		    }
	       }

	       if (rs->has_skip)
	       {
		    fprintf(stderr,"(has_skip == 1) not yet supported in value queries.\n");
		    error = 1;
		    break;
	       }
	  }
	  
	  rs = rs->next;
     } while (rs);     
     
     if (error == 0)
     {
	  *bAns = bInRange;
     }
     else
     {
	  *bAns = 0;
     }
     
     return error;
}

static int CreateTimeRange(char *timeRange, ValueRangeSet_t **rs)
{
     int error = 0;

     if (timeRange == NULL)
     {
	  error = 1;
     }
     else
     {
	  int n;
	  char *p = timeRange;
	  ValueRangeSet_t *vr = NULL;
	  ValueRangeSet_t *head = NULL;
	  
	  do 
	  {
	       if (vr)
	       {
		    XASSERT(vr->next = (ValueRangeSet_t *)malloc(sizeof(ValueRangeSet_t)));
		    vr = vr->next;
		    memset(vr, 0, sizeof(ValueRangeSet_t));
	       }
	       else 
	       {
		    XASSERT(vr = (ValueRangeSet_t *)malloc(sizeof(ValueRangeSet_t)));
		    head = vr;
		    memset(vr,0,sizeof( ValueRangeSet_t));
	       }
	       
	       /* Get start */
	       if ((n = drms_sscanf(p, DRMS_TYPE_TIME, &vr->start)) == 0)    
	       {
		    fprintf(stderr,"Syntax Error: Expected start value of type %s in"
			    " value range, found '%s'.\n", drms_type2str(DRMS_TYPE_TIME), p);
		    error = 1;
		    break;
	       }
	       else
	       {
		    p += n;     
	       }
	       
	       if (*p == '-')
	       {
		    vr->type = START_END;
	       }
	       else if (*p == '/')
	       {
		    vr->type = START_DURATION;
	       }
	       else
	       {
		    vr->type = SINGLE_VALUE;
	       }
	       
	       /* Get end or duration "x" */
	       if (vr->type != SINGLE_VALUE)
	       {
		    ++p;
		    
		    if (vr->type == START_END)
		    {
			 if ((n = drms_sscanf(p, DRMS_TYPE_TIME, &vr->x)) == 0)    
			 {
			      fprintf(stderr,"Syntax Error: Expected end value of"
				      " type %s in value range, found '%s'.\n", drms_type2str(DRMS_TYPE_TIME), p);
			      error = 1;
			      break;
			 }
			 else
			 {
			      p += n;
			 }
		    }
		    else
		    {
			 if (ParseDuration(p, &vr->x.time_val))
			 {
			      fprintf(stderr,"Syntax Error: Expected time duration "
				      " in value range, found '%s'.\n", p);
			      error = 1;
			      break;
			 }	  
		    }
		    /* Get skip */
		    if (*p == '@')
		    {
			 ++p;
			 vr->has_skip = 1;
			 if (ParseDuration(p, &vr->skip.time_val))    
			 {
			      fprintf(stderr,"Syntax Error: Expected skip (time duration)"
				      " in value range, found '%s'.\n", p);
			      error = 1;
			      break;
			 }
		    }
		    else
		    {
			 vr->has_skip = 0;
		    }
	       }
	  } while (*p++ == ',');
	  
	  
	  if (error == 0)
	  {
	       *rs = head;
	  }
	  else
	  {
	       *rs = NULL;
	  }
     }

     return error;
}

static void DestroyTimeRange(ValueRangeSet_t *head)
{
     /* Clean up range structures */
     ValueRangeSet_t *vr = NULL;
     
     while (head)
     { 
	  vr = head->next; 
	  free(head); 
	  head = vr; 
     }
}

static int SetSVKey(DRMS_Record_t *rec, char *keyName, DRMS_Type_Value_t value)
{
     int error = 0;

     DRMS_Keyword_t *keyword = drms_keyword_lookup(rec, keyName, 1);

     if (keyword != NULL)
     {
	  if (keyword->info->type == DRMS_TYPE_DOUBLE)
	  {
	       error = drms_setkey(rec, keyName, DRMS_TYPE_DOUBLE, &value);
	       if (error != 0)
	       {
		    printf("Failed to add key %s to record\n", keyName);
	       }
	  }
	  else
	  {
	       error = 1;
	       printf("Key %s is not the correct type\n", keyName);
	  }
     }
     else
     {
	  error = 1;
	  printf("Key %s not found\n", keyName);
     }

     return error;
}

static int ExtractStateVectors(DRMS_Env_t *drmsEnv, char *filePath, char *timeRange, char *outSeries)
{
     int error = 0;
     char *obsDate = NULL;
     double xVal;
     double yVal;
     double zVal;
     double vxVal;
     double vyVal;
     double vzVal;

     int skippedRecs = 0;
     int addedRecs = 0;

     ValueRangeSet_t *tRangeSet = NULL;

     printf("Extracting state vectors from file %s.\n", filePath);

     if (timeRange != NULL)
     {
	  printf("Time range is %s.\n", timeRange);
	  error = CreateTimeRange(timeRange, &tRangeSet);
     }

     /* Read data from filePath one line at a time */
     FILE *datafp = NULL;

     if (error == 0);
     {
	  datafp = fopen(filePath, "r");
     }

     if (datafp != NULL)
     {
	  char lineBuf[kSVLineMax];
	  char *lineIn = NULL;
	  int oneMore = -1;

	  while ((lineIn = fgets(lineBuf, kSVLineMax, datafp)) != NULL)
	  {
	       if (oneMore == -1)
	       { 
		    if (strlen(lineBuf) >= 4)
		    {
			 if (strncmp(lineBuf, "Time", 4) == 0)
			 {
			      oneMore = 1;
			 }
		    }

		    continue;
	       }

	       if (oneMore > 0)
	       {
		    oneMore--;
		    continue;
	       }

	       ParseSVRecFields(lineBuf, &obsDate, &xVal, &yVal, &zVal, &vxVal, &vyVal, &vzVal);
	       
	       /* Strip out records that do not fall within the range of times specified in fdsTRange */
	       int bAns = 0;
	       if (TimeInRange(obsDate, tRangeSet, &bAns) != 0)
	       {
		    error = 1;
	       }
	       else
	       {
		    if (bAns == 0)
		    {
			 skippedRecs++;
			 continue;
		    }
	       }

	       DRMS_RecordSet_t *recSet = drms_create_records(drms_env, 1, outSeries, DRMS_PERMANENT, &error);
	  
	       if (recSet != NULL && error == 0)
	       {
		    DRMS_Record_t *rec = recSet->records[0];
		    int nPrime = rec->seriesinfo->pidx_num;
		    
		    if (nPrime != 1)
		    {
			 error = 1;
			 printf("The specified dataseries %s has the incorrect number of primary keys\n", outSeries);
		    }
		    else
		    {
			 /* Get the one primary keyword - it should match kSVKeyPrimary */
			 DRMS_Keyword_t *primaryKey = (rec->seriesinfo->pidx_keywords)[0];
			 char *actPrimaryKeyName = primaryKey->info->name;
			 DRMS_Type_t actPrimaryKeyType =  primaryKey->info->type;
			 
			 if (strcmp(actPrimaryKeyName, kSVKeyPrimary) != 0)
			 {
			      error = 1;
			      printf("Unexpected primary key in %s: actual - %s, expected - %s\n", 
				     outSeries, actPrimaryKeyName, kSVKeyPrimary);
			 }
			 else
			 {
			      DRMS_Type_Value_t value;
			      
			      /* Add primary key */
			      if (actPrimaryKeyType != DRMS_TYPE_TIME)
			      {
				   error = 1;
				   printf("Primary key in %s has wrong type\n", outSeries);
			      }
			      else
			      {
				   value.time_val = sscan_time(obsDate) - sscan_time("1977.01.01_00:00_TAI");
				   error = drms_setkey(rec, actPrimaryKeyName, actPrimaryKeyType, &value);
				   if (error != 0)
				   {
					printf("Failed to add key %s to record\n", actPrimaryKeyName);
				   }
			      }
			      
			      /* Add state vector keys */
			      value.double_val = xVal;
			      SetSVKey(rec, kSVKeyX, value);
			      
			      value.double_val = yVal;
			      SetSVKey(rec, kSVKeyY, value);
			      
			      value.double_val = zVal;
			      SetSVKey(rec, kSVKeyZ, value);
			      
			      value.double_val = vxVal;
			      SetSVKey(rec, kSVKeyVx, value);
			      
			      value.double_val = vyVal;
			      SetSVKey(rec, kSVKeyVy, value);
			      
			      value.double_val = vzVal;
			      SetSVKey(rec, kSVKeyVz, value);
			 }
		    }    
	       }
	       else
	       {
		    error = 1;
		    printf("The specified output series %s does not exist\n", outSeries);
	       }
	       
	       if (error == 0)
	       {
		    error = drms_close_records(recSet, DRMS_INSERT_RECORD);
		    if (error == 0)
		    {
			 printf("Added obsDate=%s, x=%f, y=%f, z=%f, vx=%f, vy=%f, vz=%f\n", obsDate, xVal, yVal, zVal, vxVal, vyVal, vzVal);
			 addedRecs++;
		    }
	       }
	       else
	       {
		    error = drms_close_records(recSet, DRMS_FREE_RECORD);
		    break;
	       }
	       
	  } /* end while */

	  fclose(datafp);
     }
     else
     {
	  /* Couldn't open file */
	  error = 1;
	  printf("Could not open %s for reading\n", filePath);
     }

     if (error == 0)
     {
	  printf("Added %d recs, skipped %d recs, for a total of %d recs examined in %s.\n", 
		 addedRecs, skippedRecs, addedRecs + skippedRecs, filePath);
     }

     if (tRangeSet)
     {
	  DestroyTimeRange(tRangeSet);
     }

     return error;
}

int DoIt(void)
{

     if (nice_intro())
     {
	  return 0;
     }

     int error = 0;


     if (drms_env == NULL)
     {
	  error = 1;
     }
     else
     {
	  char *fdsSeries = NULL;
	  char *fdsTRange = NULL;
	  char *svSeries = NULL;
	  
	  char *seriesIn = cmdparams_get_str(&cmdparams, "seriesIn", NULL);
	  char *timeRange = cmdparams_get_str(&cmdparams, "timeRange", NULL);
	  char *seriesOut = cmdparams_get_str(&cmdparams, "seriesOut", NULL);
	  
	  if (strcmp(seriesIn, "NOT SPECIFIED") != 0)
	  {
	       fdsSeries = strdup(seriesIn);
	  }
	  else
	  {
	       fdsSeries = strdup(kDefaultSeriesIn);
	  }
	  
	  if (strcmp(seriesOut, "NOT SPECIFIED") != 0)
	  {
	       svSeries = strdup(seriesOut);
	  }
	  else
	  {
	       svSeries = strdup(kDefaultSeriesOut);
	  }
	  
	  XASSERT(fdsSeries && svSeries);
	  
	  if (!fdsSeries || !svSeries)
	  {
	       error = 1;
	  }

	  /* Create the record set query */
	  if (error == 0)
	  {
	       if (strcmp(timeRange, "NOT SPECIFIED") != 0)
	       {
		    size_t len = strlen(kObsTimeKey) + strlen(timeRange) + 3;
		    fdsTRange = malloc(sizeof(char) * len + 1);
		    if (fdsTRange != NULL)
		    {
			 /* Must strip off hours and minutes, since the filenames contain only YYYYDDD */
			 char *strippedTR = NULL;
			 error = StripTimeRange(timeRange, &strippedTR);

			 printf("Time range: %s, stripped time range: %s\n", timeRange, strippedTR);

			 if (error == 0 && strippedTR != NULL)
			 {
			      snprintf(fdsTRange, len + 1, "[%s=%s]", kObsTimeKey, strippedTR);
			      free(strippedTR);
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
	       }
	  }
	  
	  if (error == 0)
	  {
	       char *recSetQuery = NULL;
	       size_t len = strlen(fdsSeries) + strlen(kFdsDataProductKeyVal);
	       
	       if (fdsTRange != NULL)
	       {
		    len += strlen(fdsTRange);
		    recSetQuery = malloc(sizeof(char) * len + 1);
		    if (recSetQuery != NULL)
		    {
			 snprintf(recSetQuery, len + 1, "%s%s%s", fdsSeries, fdsTRange, kFdsDataProductKeyVal);
		    }
		    else
		    {
			 error = 1;
		    }
	       }
	       else
	       {
		    recSetQuery = malloc(sizeof(char) * len + 1);
		    snprintf(recSetQuery, len + 1, "%s%s", fdsSeries, kFdsDataProductKeyVal);
		    if (recSetQuery == NULL)
		    {
			 error = 1;
		    }
	       }

	       printf("Record set query is %s\n", recSetQuery);

	       if (recSetQuery == NULL)
	       {
		    error = 1;
	       }
	       
	       if (error == 0)
	       {
		    /* Query database to locate the record(s) for a given time range */
		    int nRecs = 0;
		    DRMS_RecordSet_t *recordSet = NULL;
		    
		    error = FetchRecords(drms_env, recSetQuery, &recordSet, &nRecs);
		    free(recSetQuery);

		    if (error == 0 && recordSet != NULL)
		    {
			 printf("Number of records fetched is %d\n", nRecs);

			 if (nRecs == 0)
			 {
			      printf ("** No records in selected data set **\n");
			 }
			 else
			 {
			      /* Got the record(s), now fetch the segments for the record(s) */ 
			      int nSegs = 0;
			      DRMS_Segment_t **segments = NULL;

			      error = FetchSegments(recordSet, nRecs, kSVFileSegName, &segments, &nSegs);


			      printf("Number of segments fetched is %d\n", nSegs);

			      if (error == 0)
			      {
				   char fname[DRMS_MAXPATHLEN];
				   char path[DRMS_MAXPATHLEN];
				   int segToProc = 0;
				   
				   while (segToProc < nSegs && error == 0)
				   {
					DRMS_Segment_t *seg = segments[segToProc];

					if (seg != NULL)
					{
					     drms_record_directory(seg->record, path, 1);
					     int len = strlen(path) + strlen(seg->filename) + 1;
					     char *filePath = malloc(sizeof(char) * len + 1);
					     if (filePath)
					     {
						  snprintf(filePath, len + 1, "%s/%s", path, seg->filename);
						  char *tRange = (strcmp(timeRange, "NOT SPECIFIED") == 0) ? NULL : timeRange;
						  error = ExtractStateVectors(drms_env, filePath, tRange, svSeries);
						  free(filePath);
					     }
					     else
					     {
						  error = 1;
					     }
					}
					
					segToProc++;
				   }
			      }

			      FreeSegments(segments);
			 }

			 FreeRecords(recordSet);
		    }
	       }
	  }
	  
	  if (fdsSeries)
	  {
	       free(fdsSeries);
	  }
	  
	  if (fdsTRange)
	  {
	       free(fdsTRange);
	  }
	  
	  if (svSeries)
	  {
	       free(svSeries);
	  }
     }
     
     return error;
}





