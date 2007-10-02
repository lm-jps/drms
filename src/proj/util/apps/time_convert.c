/*
 * time_convert - change TAI time since 1977 into ascii with a given timezone
 * and from an ascii time to seconds.  Takes one argument of time or seconds and an optional
 * flag argument of time zone.  Default is UTC for conversion to ascii.
 */

#include <stdlib.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>
#include "timeio.h"
#include "jsoc.h"
#include "cmdparams.h"

ModuleArgs_t module_args[] =
{
  {ARG_STRING, "s", "NOT SPECIFIED", "<DSDS/JSOC time in seconds>"},
  {ARG_STRING, "sdo", "NOT SPECIFIED", "<SDO time in seconds>"},
  {ARG_STRING, "egse", "NOT SPECIFIED", "<EGSE time in seconds>"},
  {ARG_STRING, "time", "NOT SPECIFIED", "<Time as yyyy.mm...>"},
  {ARG_STRING, "ord", "NOT SPECIFIED", "<Time as yyyy.ddd...>"},
  {ARG_STRING, "zone", "UTC", "<Time zone>"},
  {ARG_STRING, "o", "NOT SPECIFIED", "format of time output"},
  {ARG_FLAG, "h", "0", "help message"},
  {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;

CmdParams_t cmdparams;

#define SEC1970TO2004   1072828800
#define SECSPERDAY         86400.0

typedef enum
{
   kTimeFormat_None = 0,
   kTimeFormat_Internal,
   kTimeFormat_SDO,
   kTimeFormat_EGSE,
   kTimeFormat_Ordinal,
   kTimeFormat_Calendar
} TimeFormat_t;

char *TimeFormatStr[] = 
{
   "None",
   "Internal/JSOC",
   "SDO",
   "EGSE",
   "Ordinal",
   "Calendar"
};

static void PrintTime(TIME t, TimeFormat_t f);
static TIME ZoneAdjustment(TIME t, char *zone);

int nice_intro ()
  {
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  if (usage)
    {
    printf ("Usage:\ntime_convert -h\n"
	"time_convert s=<secondsJSOC>|sdo=<secondsSDO>|egse=<secondsEGSE>|time=<calender time>|ord=<ordinal date> [o=jsoc | o=sdo | o=egse | o=ord | o=cal] [zone=<zone>]\n"
        "<secondsJSOC> = JSOC seconds since 15 seconds before January 1, 1977 UTC \n"
        "<secondsSDO> = seconds since January 1, 1958 TAI, i.e. SDO onboard time\n"
	"<secondsEGSE> = seconds since APPROXIMATELY January 1, 2004 \n"
        "<calender time> = yyyy.mm.dd_hh:mm:ss<zone>\n"
	"<ordinal date> = yyyy.ddd<zone>\n"
        "<zone> = time zone as UT, TAI, PST, etc. - default is UTC\n"
	"h - show usage message\n"
        "o - output time in format specified\n");
    return(1);
    }
  return (0);
  }

int main(int argc, char **argv)
{
int status;
TIME sscan_time(char *s);

char *s, *sdo, *time;
char *ord; /* Users can now supply the time argument as an ordinal date ('day-of-year' format - ISO 8601): YYYY.DDD */
char *egse = NULL;
char *oArg = NULL;

TIME t;

/* Parse command line parameters */
status = cmdparams_parse (&cmdparams, argc, argv);
if (status == CMDPARAMS_QUERYMODE)
  {
  cmdparams_usage (argv[0]);
  return 0;
  }

if (nice_intro ())
  return (0);

/* The are the different types of input. */
s = cmdparams_get_str(&cmdparams, "s", NULL);
sdo = cmdparams_get_str(&cmdparams, "sdo", NULL);
time = cmdparams_get_str(&cmdparams, "time", NULL);
ord = cmdparams_get_str(&cmdparams, "ord", NULL);
egse = cmdparams_get_str(&cmdparams, "egse", NULL);

int err = 0;

TIME timeToPrint;
TimeFormat_t inputFormat = kTimeFormat_None;
TimeFormat_t outputFormat = kTimeFormat_None;
TimeFormat_t outputOverride = kTimeFormat_None;
int mFormats = 0;

/* These are the different types of output. */
oArg = cmdparams_get_str(&cmdparams, "o", NULL);
if (strcmp(oArg, "NOT SPECIFIED") != 0)
{
   if (strcmp(oArg, "jsoc") == 0)
   {
      outputOverride = kTimeFormat_Internal;
   }
   else if (strcmp(oArg, "sdo") == 0)
   {
      outputOverride = kTimeFormat_SDO;
   }
   else if (strcmp(oArg, "egse") == 0)
   {
      outputOverride = kTimeFormat_EGSE;
   }
   else if (strcmp(oArg, "ord") == 0)
   {
      outputOverride = kTimeFormat_Ordinal;
   }
   else if (strcmp(oArg, "cal") == 0)
   {
      outputOverride = kTimeFormat_Calendar;
   }
   else
   {
      fprintf(stderr, "Output format %s is not recognized.  Using default.\n", oArg);
   }
}

if (strcmp(s, "NOT SPECIFIED")!= 0)
{ 
   /* internal seconds was specified */
   if (inputFormat == kTimeFormat_None)
   {
      sscanf(s, "%lf", &t);
      timeToPrint = t;
      outputFormat = kTimeFormat_Calendar;
      inputFormat =  kTimeFormat_Internal;
   }
   else
   {
      mFormats = 1;
   }
}
if (strcmp(sdo, "NOT SPECIFIED")!= 0)
{ 
   /* SDO seconds was specified */
   if (inputFormat == kTimeFormat_None)
   {
      sscanf(sdo, "%lf", &t);
      timeToPrint = t + sscan_time("1958.01.01_00:00:00_TAI");
      outputFormat = kTimeFormat_Calendar;
      inputFormat = kTimeFormat_SDO;
   }
   else
   {
      mFormats = 1;
   }
}
if (strcmp(egse, "NOT SPECIFIED") != 0)
{
   /* Time since ~ 1/1/2004 was provided as input */
   if (inputFormat == kTimeFormat_None)
   {
      sscanf(egse, "%lf", &t);
      timeToPrint = t + SEC1970TO2004 + sscan_time("1970.01.01_00:00_UTC");
      outputFormat = kTimeFormat_Calendar;
      inputFormat = kTimeFormat_EGSE;
   }
   else
   {
      mFormats = 1;
   }
}
if (strcmp(time, "NOT SPECIFIED")!= 0)
{ 
   /* calendar time string was specified */
   if (inputFormat == kTimeFormat_None)
   {
      t = sscan_time(time);
      timeToPrint = t;
      outputFormat = kTimeFormat_Internal;
      inputFormat = kTimeFormat_Calendar;
   }
   else
   {
      mFormats = 1;
   }
}
if (ord != NULL && *ord != NULL && strcmp(ord, "NOT SPECIFIED") != 0)
{
   /* day-of-year was specified - convert to internal representation */
   if (inputFormat == kTimeFormat_None)
   {
      err = 1;
      
      long year = 0;
      int day = 0;
      char *tFormat = NULL;
      
      char *ordDate = strdup(ord);
      int timeStrLen = strlen(ordDate);
      
      if (ordDate != NULL)
      {
	 char *loc = strchr(ordDate, '.');
	 char *loc2 = strchr(ordDate, '_');
	 
	 if (loc != NULL)
	 {
	    *loc = '\0';
	    
	    if (sscanf(ordDate, "%ld", &year) != 0)
	    {
	       if (loc2 != NULL && loc2 > loc)
	       {
		  *loc2 = '\0';
		  sscanf(loc + 1, "%d", &day);
		  
		  if (loc2 + 1 <= &ordDate[timeStrLen - 1])
		  {
		     tFormat = (char *)malloc(sizeof(char) * strlen(loc2 + 1) + 1);
		     if (tFormat)
		     {
			sscanf(loc2 + 1, "%s", tFormat);
		     }
		  }
	       }
	       else if (loc + 1 <= &ordDate[timeStrLen - 1])
	       {
		  sscanf((loc + 1), "%d", &day);
	       }
	       
	       err = (day < 1 || day > 366);
	       
	       if (!err)
	       {
		  /* first convert to calendar date */
		  char timeBuf[64];
		  snprintf(timeBuf, sizeof(timeBuf), "%ld.01.%d_00:00_%s", year, day, tFormat ? tFormat : "UT");
		  /* then use sscan_time to convert to internal time */
		  // t = sscan_time(timeBuf) - sscan_time("1977.01.01_00:00_TAI");
		  // sscan_time("1977.01.01_00:00_TAI") == 0
		  t = sscan_time(timeBuf);
		  timeToPrint = t;
		  outputFormat = kTimeFormat_Internal;
		  inputFormat = kTimeFormat_Ordinal;
	       }
	       else
	       {
		  fprintf(stderr, "invalid day of year\n");
	       }
	    }
	    else
	    {
	       fprintf(stderr, "invalid year\n");
	    }
	 }
	 else
	 {
	    fprintf(stderr, "missing day of year\n");
	 }
	 
	 free(ordDate);
      }
      
      if (tFormat)
      {
	 free(tFormat);
      }
   }
   else
   {
      mFormats = 1;
   }
}

if (inputFormat == kTimeFormat_None)
{
   fprintf(stderr, "No input format specified.\n");
   err = 1;
}
else if (mFormats)
{
   fprintf(stderr, 
	   "Multiple input formats specified, using %s time\n", 
	   TimeFormatStr[inputFormat]);
}

if (outputOverride != kTimeFormat_None)
{
   outputFormat = outputOverride;
}

if (!err)
{
   PrintTime(timeToPrint, outputFormat);
}

if (err)
{
   fprintf(stderr,"time_convert call error\n");
   return(1);
}
else
{
   return(0);
}
}

/* t is time in TAI seconds since 1/1/1977, f is the format in which to print the time string */
void PrintTime(TIME t, TimeFormat_t f)
{
   if (f == kTimeFormat_Calendar)
   {
      char at[128];
      sprint_time(at, t, cmdparams_get_str(&cmdparams, "zone", NULL), 3);
      // broken for precision == 3, causes seg fault, fixed maybe
      // sprint_time(at, t, cmdparams_get_str(&cmdparams, "zone", NULL), 0);
      printf("%s\n", at);
   }
   else if (f == kTimeFormat_SDO)
   {
      /* no zone associated with sdo time, so zone parameter is ignored */
      printf("%12.3f\n", t - sscan_time("1958.01.01_00:00:00_TAI"));
   }
   else if (f == kTimeFormat_EGSE)
   {
      /* no zone associated with egse time, so zone parameter is ignored */
      printf("%12.3f\n", t - SEC1970TO2004 - sscan_time("1970.01.01_00:00_UTC"));
   }
   else if (f == kTimeFormat_Ordinal)
   {
      /* This does NOT provide a fractional day - any fractional part is discarded. */
      int ordTimeDays = 0;
      char zoneStr[32];
      char at[128];
      TIME jan1 = 0;
      TIME secsSinceJan1 = 0;

      char *zone =  cmdparams_get_str(&cmdparams, "zone", NULL);
      
      /* First, get internal time for January 1 of the year we are going to output. */
      sprint_time(at, t, zone, 3);
      char *tz = strrchr(at, '_');

      if (tz)
      {
	 snprintf(zoneStr, sizeof(zoneStr), "_%s", tz + 1);
      }

      char *dot = strchr(at, '.');
      if (dot != NULL)
      {
	 *dot = '\0';
	 char timeStr[64];
	 snprintf(timeStr, sizeof(timeStr), "%s.01.01_00:00:00%s", at, zoneStr);
	 jan1 = sscan_time(timeStr);
      }
      
      secsSinceJan1 = t - jan1;

      /* Now, get zone adjustment for jan1 and t.  Subtract Adj(t) - Adj(jan1) from 
       * secsSinceJan1. */
      TIME adjJan1 = ZoneAdjustment(jan1, zone);
      TIME adjT = ZoneAdjustment(t, zone);
      TIME ordTimeSecs = secsSinceJan1 - (adjT - adjJan1);

      /* Divide by TAI secs per day (86400). */
      ordTimeDays = 1 + ordTimeSecs / SECSPERDAY;      

      /* Output */
      printf("%s.%03d%s\n", at, ordTimeDays, zoneStr);
   }
   else
   {
      if (f != kTimeFormat_Internal)
      {
	 fprintf(stderr, "Invalid output format, defaulting to JSOC time.\n");
      }

      printf("%12.3f\n", t);
   }
}

/* <t> is the TAI-seconds equivalent of a time in the <zone> time zone. */
TIME ZoneAdjustment(TIME t, char *zone)
{
   char timestr[128];
   sprint_time(timestr, t, zone, 3);

   char *tz = strrchr(timestr, '_');
   tz[1] = 'T';
   tz[2] = 'A';
   tz[3] = 'I';
   tz[4] = '\0';
   
   TIME taiDateSecs = sscan_time(timestr);
   return t - taiDateSecs;
}
