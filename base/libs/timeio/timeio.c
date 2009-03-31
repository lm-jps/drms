/*
 *  timeio.c							~CM/src/timeio/
 *
 *  Functions to deal with character representations of times in clock,
 *    calendric, and other forms.
 *
 *  Responsible:
 *    Rick Bogart					RBogart@spd.aas.org
 *
 *  Function and macros:
 *    TIME sscan_time (char *string);
 *    void sprint_time (char *string, TIME t, char *zone, int precision);
 *    sprint_at (char *string, TIME t);
 *    sprint_dt (char *string, TIME t);
 *    sprint_ut (char *string, TIME t);
 *    double tai_adjustment (TIME t, char *zone);
 *    double zone_adjustment (TIME t, char *zone);
 *    int time_is_invalid (TIMT t);
 *
 *  Bugs:
 *    Unrecognized time-zone designations are interpreted as Z
 *    Ephemeris time and "Carrington time" are not supported.
 *    Dates B.C are not handled historically; year 0 is assumed.
 *    The asymmetry between conversions to and from date should be repaired.
 *      The asymmetry of utc_adjustment() and tai_adjustment() is needed
 *	to properly render clock times within the difference between the
 *	two systems of a leap second.
 *    Unlike the old atodate functions, the date_time structure is not
 *      exported, so is unavailable for direct inspection or modification
 *      except through the sprint_time and sscan_time functions; this is
 *      a feature.
 *    Times with +/-signs in the hour or minute fields are subject to
 *	misinterpretation, due to confusion with time-zone designations
 *    The internal dattim structure is not thread safe.
 *
 *  Possible Future Upgrades/Modification:
 *    Ignore leading and trailing blanks in time zone designations
 *    Add support for time zone designations [+-]HH:MM
 *
 *  Revision history is at end of file.
 */

/*  The following special includes may be necessary in part because of
 *    the use of gcc, not because of the architecture; stddef is needed
 *    to define NULL!  strings.h on the sun does not include string.h
 */
#ifdef __sun__
#include <stddef.h>
#endif

#include <ctype.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include "timeio.h"
#include "util.h"

static struct date_time {
    double second;
    double julday;
    double delta;
    int year;
    int month;
    int dofm;
    int dofy;
    int hour;
    int minute;
    int civil;
    int ut_flag;
    char zone[8];
    int isISO;
} dattim;

const int kTIMEIO_MaxTimeEpochStr = 64;

static int molen[] = {31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static double ut_leap_time[] = {
/*
 *  Note: the times and amounts of adjustment prior to 1972.01.01 may be
 *    erroneous (they do not agree with those in the USNO list at
 *    ftp://maia.usno.navy.mil/ser7/tai-utc.dat), but they should not be
 *    changed without due care, as the calculation of utc_adjustment is
 *    based on a count of assumed whole second changes.
 */
-536543999.0,                                                /*  1960.01.01  */
-457747198.0,                                                /*  1962.07.01  */
-394588797.0,                                                /*  1964.07.01  */
-363052796.0,                                                /*  1965.07.01  */
-331516795.0,                                                /*  1966.07.01  */
-284083194.0,                                                /*  1968.01.01  */
-252460793.0,                                                /*  1969.01.01  */
-220924792.0,                                                /*  1970.01.01  */
-189388791.0,                                                /*  1971.01.01  */
-157852790.0,                                                /*  1972.01.01  */
-142127989.0,                                                /*  1972.07.01  */
-126230388.0,                                                /*  1973.01.01  */
 -94694387.0,                                                /*  1974.01.01  */
 -63158386.0,                                                /*  1975.01.01  */
 -31622385.0,                                                /*  1976.01.01  */
        16.0,                                                /*  1977.01.01  */
  31536017.0,                                                /*  1978.01.01  */
  63072018.0,                                                /*  1979.01.01  */
  94608019.0,                                                /*  1980.01.01  */
 141868820.0,                                                /*  1981.07.01  */
 173404821.0,                                                /*  1982.07.01  */
 204940822.0,                                                /*  1983.07.01  */
 268099223.0,                                                /*  1985.07.01  */
 347068824.0,                                                /*  1988.01.01  */
 410227225.0,                                                /*  1990.01.01  */
 441763226.0,                                                /*  1991.01.01  */
 489024027.0,                                                /*  1992.07.01  */
 520560028.0,                                                /*  1993.07.01  */
 552096029.0,                                                /*  1994.07.01  */
 599529630.0,                                                /*  1996.01.01  */
 646790431.0,                                                /*  1997.07.01  */
 694224032.0,                                                /*  1999.01.01  */
 915148833.0,                                                /*  2006.01.01  */
1009843234.0                                                 /*  2009.01.01  */
/*
 * IMPORTANT NOTE ---
 *   When adding a new leap second add time be sure to make changes in  BOTH
 *   this file and its near clone in /CM/src/timeio via your STAGING directory.
 *   
 *   The value to list is the time of the first second after the leap second.
 *   So, before the addition is made, get the seconds of the time in the
 *   comment via e.g. time_index or time_convert  then add 1 to it before
 *   adding it to the table.
 *
*/
/*
 *  ***  NOTE  Please notify mdiops@mdisas.nascom.nasa.gov at EOF
 *       whenever any of these times are updated!   ***
 */
};

static void date_from_epoch_time (TIME t);
static TIME epoch_time_from_julianday ();
static double zone_adjustment_inner (char *zone, int *valid);

static void _clear_date_time () {
/*
 *  Clear the date_time struct to time 0 (epoch 1977.0_TAI)
 */
    dattim.julday = 0.0;
    strcpy (dattim.zone, "TDT");
    date_from_epoch_time (epoch_time_from_julianday ());
    dattim.hour = 11;
    dattim.minute = 59;
    dattim.second = 27.816;
}

static int _parse_error () {
  _clear_date_time ();
  return -1;
}

static void _fracday_2_clock (double fdofm) {
/*
 *  Get clock time from fraction of day
 */
  dattim.dofm = fdofm;
  dattim.second = 86400.0 * (fdofm - dattim.dofm);
  dattim.hour = dattim.second / 3600.0;
  dattim.second -= 3600.0 * dattim.hour;
  dattim.minute = dattim.second / 60.0;
  dattim.second -= 60.0 * dattim.minute;
  dattim.ut_flag = 0;
}

static int _parse_clock_int (char *str, int *hour, int *minute, double *second, int *ut_flag, int *consumed)
  {
  /*
   *  Read time elements from wall-clock string HH:MM:SS.SSS
   */
  int status;
  char *ptr, *endptr;
  int retval;

  if (!str || strlen(str)==0)
     return 0;
  ptr = str;
  *hour = (int)strtol(str, &endptr, 10);
  if (ptr == endptr)
     return 0; // must have at least one digit
  else
     { // got hour
     if (*endptr == 'h' || *endptr == 'H')
        endptr++; // allow h or H to be included in consumed count
     if (!*endptr || *endptr != ':')
        { // do not have min field
        *minute = 0.0;
        *second = 0.0;
        retval = 1;
        }
     else
        { // proceed with minutes
        ptr = endptr+1;
        *minute = (int)strtol(ptr, &endptr, 10);
        if (ptr == endptr)
           return 0; // must have at least one digit
        else
           { // got minute
              if (*endptr == 'm' || *endptr == 'M')
              endptr++; // allow m or M to be included in consumed count
           if (!*endptr || *endptr != ':')
              { // do not have sec field
              *second = 0.0;
              retval = 2;
              }
           else
              { // proceed with seconds
              ptr = endptr+1;
              *second = strtod(ptr, &endptr);
              if (ptr == endptr)
                 return 0; // must have at least one digit
              else
                 { // got seconds
                 if (*endptr == 's' || *endptr == 'S')
                    endptr++; // allow s or S to be included in consumed count
                 retval = 3;
                 }
              }
           }
        }
     }
  if (consumed)
     *consumed = endptr - str;
                              /*  Set a flag in case it is a UT leap second  */
  *ut_flag = (*second >= 60.0);
  return (retval);
}

static int _parse_clock (char *str, int *consumed)
  {
  return  _parse_clock_int (str, &dattim.hour, &dattim.minute, &dattim.second, &dattim.ut_flag, consumed);
  }

/* returns 1 if clock string is a valid time, 0 otherwise. */
static int clock_isvalid(const char *str)
  {
  int status;
  int hr;
  int min;
  double sec;
  int consumed;
  int ut_flag;
  status = _parse_clock_int (str, &hr, &min, &sec, &ut_flag, &consumed);
  if (status)
    status = 1;
  return status;
  }

static int _parse_month_name (char *moname) {
  char *p, monthname[10];
  int month;
  p = monthname;
  while (*moname && isalpha(*moname) && p-monthname < 9)
    *p++ = *moname++;
  *p = '\0';

  if (!strncasecmp (monthname, "JAN", 3) || !strcasecmp (monthname, "I"))
    month = 1;
  else if (!strncasecmp (monthname, "FEB", 3) || !strcasecmp (monthname, "II"))
    month = 2;
  else if (!strncasecmp (monthname, "MAR", 3) || !strcasecmp (monthname, "III"))
    month = 3;
  else if (!strncasecmp (monthname, "APR", 3) || !strcasecmp (monthname, "IV"))
    month = 4;
  else if (!strncasecmp (monthname, "MAY", 3) || !strcasecmp (monthname, "V"))
    month = 5;
  else if (!strncasecmp (monthname, "JUN", 3) || !strcasecmp (monthname, "VI"))
    month = 6;
  else if (!strncasecmp (monthname, "JUL", 3) || !strcasecmp (monthname, "VII"))
    month = 7;
  else if (!strncasecmp (monthname, "AUG", 3) || !strcasecmp (monthname, "VIII"))
    month = 8;
  else if (!strncasecmp (monthname, "SEP", 3) || !strcasecmp (monthname, "IX"))
    month = 9;
  else if (!strncasecmp (monthname, "OCT", 3) || !strcasecmp (monthname, "X"))
    month = 10;
  else if (!strncasecmp (monthname, "NOV", 3) || !strcasecmp (monthname, "XI"))
    month = 11;
  else if (!strncasecmp (monthname, "DEC", 3) || !strcasecmp (monthname, "XII"))
    month = 12;
  else
                                                /*  Unrecognized month name  */
    month = 0;
  return (month);
}

static int _parse_doy (int year, int doy)
  {
  int month;
  if (year%4 == 0)
    molen[2] = 29;
  if ((year%100 == 0) && (year > 1600) && (year%400 != 0))
    molen[2] = 28;
  for (month = 1; month <= 12 && doy > molen[month]; month += 1)
    doy -= molen[month];
  dattim.month = month;
  dattim.dofm = doy;
  return (month < 13);
  }

static int _parse_date (char *strin, int *consumed)
  {
  /*
   *  Read date elements from calendar string YYYY.{MM|nam}.DD[.ddd]
   *  Allow ISO 8601 forms YYYY(-MM{-DD}} or YYYYMMDD or YYYYdoy
   *  Return 3 if OK and no fractional day, 6 if OK with fractional date, else 0
   */
  double fracday;
  int status, dfrac;
  char *ptr, *endptr;
  int len;
  int retval;
  char daystr[32];

  status = 3;  // assume ymd and no day fraction
  dattim.isISO = 0;
  ptr = strin;
  dattim.year = strtol (ptr, &endptr, 10);
  len = endptr - ptr;
  if (len == 8)
    { // ISO concatenated form YYYYMMDD
    if (*endptr == '.' || *endptr == '-')
      return _parse_error ();
    sscanf(ptr, "%4d%2d%2d", &dattim.year, &dattim.month, &dattim.dofm);
    dattim.isISO = 1;
    status = 8;
    }
  else if (len == 7)
    { // ISO concatenated form YYYYddd where ddd is doy
    int doy;
    sscanf(ptr, "%4d%3d", &dattim.year, &doy);
    if (_parse_doy (dattim.year, doy) ==0)
       return _parse_error ();
    dattim.isISO = 1;
    status = 8;
    }
  else if (len > 5)
      return _parse_error ();
  if (status != 8)
    {
    if (*endptr == '.' || *endptr == '-')
      { // second field, start with look for month
      if (*endptr == '-')
          dattim.isISO = 1;
      ptr = endptr +1;
      dattim.month = strtol (ptr, &endptr, 10);
      len = endptr - ptr;
      if (len == 0)
          { /* string month name */
          dattim.month = _parse_month_name (ptr);
          if (dattim.month == 0)
              return _parse_error ();
          // Month name found, now find end of letter string.
          while (*endptr && isalpha(*endptr))
              endptr++;
          }
      else if (len == 3)
          { // must be ordinal doy
          int doy = dattim.month;
          if (_parse_doy (dattim.year, doy) ==0)
             return _parse_error ();
          status = 8;
          }
      else if (dattim.month > 12)
          return _parse_error ();
      // Now look for third field
      if (status != 8) // skip dofm if found doy already
        {
        if (*endptr == '.' || (dattim.isISO && *endptr == '-'))
          { // look for dofm, have leading delim so exptect 1 or 2 digits
            // unless month is Jan, then dofm may be interp as doy
          ptr = endptr +1;
          dattim.dofm = strtol (ptr, &endptr, 10);
          len = endptr - ptr;
          if (len == 0)
              return _parse_error ();
          if (dattim.dofm > 366 && dattim.month > 1)
              return _parse_error ();
          }
        else
          { // No dofm field
          dattim.dofm = 1;
          }
        }
      }
    else
      { // Only first field present, default Jan 01.
      dattim.month = 1;
      dattim.dofm = 1;
      }
    }
  // Now look for day fraction
  if (*endptr == '.')
     { // look for day fraction, expect digits else error
     ptr = endptr +1;
     dfrac = (int)strtol(ptr, &endptr, 10);
     len = endptr - ptr;
     if (len > 0)
         { /*  Day of month is in fractional form  */
         sprintf (daystr, "%d.%d", dattim.dofm, dfrac);
         sscanf (daystr, "%lf", &fracday);
         _fracday_2_clock (fracday);
         status = 6;
         }
      else
        return _parse_error ();
      }
  if (status == 8)
      status = 3;
  if (consumed)
    *consumed = endptr - strin;
  return status;
}

static int _parse_date_time_inner (char *str, 
                                   char **first, 
                                   char **second, 
                                   char **third, 
                                   int *jdout,
                                   int *consumed) {
/*
 *  Parse a date-time string in one of the standard forms specified in
 *    SOI TN 94-116
 *  Returns 0 if string is in calendar-clock form, 1 if it is in Julian day
 *    form, and -1 if it cannot be successfully parsed (in which case the
 *    time is cleared to JD 0.0).
 */
  int status;
  int length;
  char *field0, *field1, *field2;
  char *tmpstr = NULL;
  int earlytz = 0; /* if 1, then missing clock, but time zone present */
  char *endptr = NULL;
  char realzone[64];
  int field1consumed = 0;
  int field0consumed = 0;
  char field0cpy[64] = {'\0'}; /* enough for JD */

  if (first) {
     *first = NULL;
  }

  if (second) {
     *second = NULL;
  }

  if (third) {
     *third = NULL;
  }

  if (jdout) {
     *jdout = 0;
  }

  if (consumed) {
     *consumed = 0;
  }

  length = strlen (str);
  if (!length)
      return _parse_error ();
  field0 = str;
  dattim.isISO = 0;

  /*  First field must either be calendar date or "MJD" or "JD"  */
  strncpy(field0cpy, field0, sizeof(field0cpy)-1);
  strtoupper(field0cpy);
  
  if (strstr(field0cpy, "JD"))
     {
     char *pc = field0cpy;
     while (*pc == ' ') pc++;
     if (*pc == 'M') pc++;
     if (strncmp (pc, "JD", 2))
        return _parse_error();
     pc += 2;
     if (*pc++ != '_')
        return _parse_error();
     field0consumed = pc - field0cpy;

     /* For JD times, the date is contained in field1, not field0 and 
      * there is no clock field.
      */
     field1 = field0 + field0consumed;

     if (first && field1 && strlen(field1) > 0)
        *first = strdup(field1);

     dattim.julday = strtod(field1, &endptr);
     if (endptr == field1)
        return _parse_error ();

     if (consumed)
        *consumed = endptr - field0; 

     if (*endptr == '_')
       {
       field2 = endptr + 1;
       if (consumed)
          *consumed += 1;
       }
     else
       field2 = NULL;

     if (field2)
        {
        if (parse_zone(field2, realzone, sizeof(realzone)))
          {
          return _parse_error ();
          }

        snprintf(dattim.zone, sizeof(dattim.zone), "%s", realzone);

        if (consumed)
          *consumed += strlen(realzone) ; 

        if (third && strlen(realzone) > 0)
          {
          *third = strdup(realzone);
          }
       }
     else
       strcpy (dattim.zone, "TDT"); /*  Default for Julian day notation is TDT  */

     if (field0cpy[0] == 'M')
              /*  Modified Julian date (starts at midnight) : add 2400000.5  */
       dattim.julday += 2400000.5;

     if (jdout)
       *jdout = 1;
     return 1;
     }

/*  First field is calendar date with optional day fraction  */
   dattim.julday = 0.0;

   if (first && field0 && strlen(field0) > 0) 
     *first = strdup(field0);

   status = _parse_date (field0, &field0consumed);
   if (consumed)
     *consumed = field0consumed;

  // now find field1, should be at end of field0 if present
   field1 = field0 + field0consumed;
   if (*field1 == '_' || (dattim.isISO && (*field1 == 'T' || *field1 == ' ')))
     {
     field1 += 1;
     field2 = NULL;
     if (consumed)
        *consumed += 1;
     }
   else
     {
     field2 = field1 = NULL;
     field1consumed = 0;
     earlytz = status == 3;
     }

  if (field1)
   {
   if (status == 3)
     {
     /* normal date with no fraction in field0 so expect clock time in field 1 */
     status = _parse_clock (field1, &field1consumed);
     if (!status)
       { // is not OK time
       /* Add support for  YYYY.MM.DD_TZ. */
       /* field1 exists - may be a 'time zone' */
       /* So, only two fields, and the second field was not a valid clock - 
        * assume it is the 'time zone' (bad time zones are okay in timeio) */
       field1consumed = 0;
       field2 = field1;
       field1 = NULL;
       earlytz = 1;
       }
     else
       { // clock is fine
       if (second && field1 && strlen(field1) > 0)
          *second = strdup(field1);
       if (field1 && (*(field1 + field1consumed) == '+' || *(field1 + field1consumed) == '-'))
          {
          /* could be the case that there is an offset to the clock, like 12:45:10+800. 
           * the +800 is really a time zone from timeio's perspective */
          field2 = field1 + field1consumed;
          }
       if (field1 && *(field1 + field1consumed) == '_')
          { // then TZ may follow
          if (consumed)
             *consumed += 1; // for the '_'
          field2 = field1 + field1consumed + 1;
          }
        else
          { // should stop here
          field2 = NULL;
          }
        }
     }
   else
     {
    /* Fractional day exists - but field1 may contain time zone */
    if (!clock_isvalid(field1))
        {
        /* assume time zone*/
        field1consumed = 0;
        field2 = field1;
        // earlytz = 1;
        }
     }
   }
  /* add the number of chars consumed in field1 */
  if (consumed)
    *consumed += field1consumed;

  /* now all that can be left is time zone in field2 */

  if (earlytz) {
     /* field1 is a time zone, and no field2 
      * Make field1 00:00 and field2 be the time zone */
     // field2 = field1; done above
     dattim.hour = 0;
     dattim.minute = 0;
     dattim.second = 0.0;
     if (second)
       *second = NULL;
  }

  if (field2)
    {
    if (third && field2 && strlen(field2) > 0)
        *third = strdup(field2);

    if (parse_zone(field2, realzone, sizeof(realzone)))
       {
        return _parse_error ();
       }
    snprintf(dattim.zone, sizeof(dattim.zone), "%s", realzone);
    if (consumed)
        *consumed += strlen(realzone);
    }
 else
   strcpy (dattim.zone, "UTC");

  if (tmpstr)
    free(tmpstr);
  return 0;
}

static int _parse_date_time (char *str)
{
   return _parse_date_time_inner (str, NULL, NULL, NULL, NULL, NULL);
}

#define JD_EPOCH        (2443144.5)
#define EPOCH_2000_01_01        ( 725760000.0)
#define EPOCH_1601_01_01        (-11865398400.0)
#define EPOCH_1600_01_01        (-11897020800.0)
#define EPOCH_1582_10_15        (-12440217600.0)
#define EPOCH_1581_01_01        (-12495686400.0)
#define SEC_DAY         (86400.0)                             /*       1 d  */
#define SEC_YEAR        (31536000.0)                          /*     365 d  */
#define SEC_BSYR        (31622400.0)                          /*     366 d  */
#define SEC_YEAR4       (126144000.0)                         /*    1460 d  */
#define SEC_4YRS        (126230400.0)                         /*    1461 d  */
#define SEC_GCNT        (3155673600.0)                        /*   36524 d  */
#define SEC_JCNT        (3155760000.0)                        /*   36525 d  */
#define SEC_GR4C        (12622780800.0)                       /*  146097 d  */
#define SEC_JL4C        (12623040000.0)                       /*  146100 d  */

static void date_from_epoch_time (TIME t) {
  double century, four_year, one_year;
  int year, month, day;

  if (t < EPOCH_1582_10_15) {
                           /*  time < 1582.10.15_00:00: use Julian calendar  */
    year = 1585;
    t -= EPOCH_1581_01_01 + SEC_4YRS;
    while (t < -SEC_JL4C) {
      t += SEC_JL4C;
      year -= 400;
    }
    while (t < -SEC_JCNT) {
      t += SEC_JCNT;
      year -= 100;
    }
    while (t < -SEC_4YRS) {
      t += SEC_4YRS;
      year -= 4;
    }
    one_year = SEC_BSYR;
    while (t < 0.0) {
      t += one_year;
      year -= 1;
      one_year = SEC_YEAR;
    }
  } else {
    year = 1600;
    t -= EPOCH_1600_01_01;
    while (t < -SEC_4YRS) {
      t += SEC_4YRS;
      year -= 4;
    }
    one_year = SEC_YEAR;
    while (t < 0.0) {
      t += one_year;
      year -= 1;
      one_year = (year % 4 == 1) ? SEC_BSYR : SEC_YEAR;
    }
  }

  century = SEC_JCNT;
  while (t >= century) {
    t -= century;
    year += 100;
    century = (year % 400) ? SEC_GCNT : SEC_JCNT;
  }
  four_year = (year % 100) ? SEC_4YRS : (year % 400) ? SEC_YEAR4 : SEC_4YRS;
  while (t >= four_year) {
    t -= four_year;
    year += 4;
    four_year = SEC_4YRS;
  }
  one_year = (year % 4) ? SEC_YEAR : (year % 100) ? SEC_BSYR : (year % 400) ?
      SEC_YEAR : SEC_BSYR;
  while (t >= one_year) {
    t -= one_year;
    year += 1;
    one_year = SEC_YEAR;
  }

  dattim.year = year;
  if (year%4 == 0)
    molen[2] = 29;
  if ((year%100 == 0) && (year > 1600) && (year%400 != 0))
    molen[2] = 28;
  month = 1;
  day = t / SEC_DAY;
  while (day >= molen[month]) {
    day -= molen[month];
    t -= SEC_DAY * molen[month];
    month++;
  }
  molen[2] = 28;
  dattim.month = month;
  dattim.dofm = t / SEC_DAY;
  t -= SEC_DAY * dattim.dofm;
  dattim.dofm++;
  dattim.hour = t / 3600.0;
  t -= 3600.0 * dattim.hour;
  dattim.minute = t / 60.0;
  t -= 60.0 * dattim.minute;
  dattim.second = t;
}

static TIME epoch_time_from_date () {
  TIME t;
  int mon, yr1601;

  t = dattim.second + 60.0 * (dattim.minute + 60.0 * dattim.hour);
  t += SEC_DAY * (dattim.dofm - 1);
  while (dattim.month < 1) {
    dattim.year--;
    dattim.month += 12;
  }
  while (dattim.month > 12) {
    dattim.year++;
    dattim.month -= 12;
  }
  yr1601 = dattim.year - 1601;
  if (yr1601 < 0) {
    if (dattim.year%4 ==0)
      molen[2] = 29;
    while (yr1601 < 1) {
      t -= SEC_JL4C;
      yr1601 += 400;
    }
    while (yr1601 > 99) {
      t += SEC_JCNT;
      yr1601 -= 100;
    }
  }
  else {
    if (dattim.year%400 == 0 || (dattim.year%4 == 0 && dattim.year%100 != 0))
      molen[2] = 29;
    while (yr1601 > 399) {
      t += SEC_GR4C;
      yr1601 -= 400;
    }
    while (yr1601 > 99) {
      t += SEC_GCNT;
      yr1601 -= 100;
    }
  }
  for (mon=1; mon<dattim.month; mon++) {
    t += SEC_DAY * molen[mon];
  }
  molen[2] = 28;
  while (yr1601 > 3) {
    t += SEC_4YRS;
    yr1601 -= 4;
  }
  while (yr1601 > 0) {
    t += SEC_YEAR;
    yr1601 -= 1;
  }
  t +=  EPOCH_1601_01_01;
  if (t < EPOCH_1582_10_15)
                           /*  Correct for adjustment to Gregorian calendar  */
    t += 10 * SEC_DAY;
  return (t);
}

static void julianday_from_epoch_time (TIME t) {
  dattim.julday = t / SEC_DAY + JD_EPOCH;
}

static TIME epoch_time_from_julianday () {
  TIME t;

  t = SEC_DAY * (dattim.julday - JD_EPOCH);
  return (t);
}

static double utc_adjustment (TIME t, char *zone) {
  TIME tt = t;
  double dt;
  int leapsecs, ct;

  dattim.civil = 0;
  if (!strcasecmp (zone, "TAI")) return 0.0;
  if (!strcasecmp (zone, "TDT") || !strcasecmp (zone, "TT")) return 32.184;
  if (!strcasecmp (zone, "GPS")) return -19.0;
       /*  All others civil time, so use universal time coordination offset  */
  dattim.civil = 1;
  leapsecs = sizeof (ut_leap_time) / sizeof (TIME);
  dt = 0.0;
  if (tt >= ut_leap_time[0]) {
    tt += 1.0;
    for (ct = 0; ct < leapsecs && tt >= ut_leap_time[ct]; ct++) {
      tt += 1.0;
      dt -= 1.0;
    }
    if (dattim.ut_flag) dt += 1.0;
  }
  return (dt + zone_adjustment (zone));
}

TIME sscan_time (char *s) {
  TIME t, tt;
  double dt;
  int status;
  char ls[256];

  strncpy (ls, s, 255);
  ls[255] = '\0';
  status = _parse_date_time (ls);
  if (status) t = epoch_time_from_julianday ();
  else t = epoch_time_from_date ();
  dt = utc_adjustment (t, dattim.zone);
  tt = t - dt;
  return (tt);
}

int sscan_time_ext(char *s, TIME *out)
{
   TIME t, tt;
   double dt;
   int status;
   char ls[256];
   int consumed = -1;

   strncpy (ls, s, 255);
   ls[255] = '\0';
   status = _parse_date_time_inner (ls, NULL, NULL, NULL, NULL, &consumed);
   if (status) t = epoch_time_from_julianday ();
   else t = epoch_time_from_date ();
   dt = utc_adjustment (t, dattim.zone);
   tt = t - dt;

   if (out)
   {
      *out = tt;
   }

   return consumed;
}

static void _raise_case (char *s) {
/*
 *  Convert from lower-case to UPPER-CASE
 */
  while (*s) {
    if (*s >= 'a' && *s <= 'z')
      *s += 'A' - 'a';
    s++;
  }
}

void sprint_time (char *out, TIME t, char *zone, int precision) {
  int nozone = 0, concat_zone = 0;
  char format[64], pzone[6];

  if (!out) return;
  if (!zone) {
    nozone = 1;
    zone = "Z";
  }
  if (strlen (zone) < 1) {
    nozone = 1;
    zone = "Z";
  } else {
    if (strlen (zone) == 1) concat_zone = 1;
    else if (zone[0] == '+' || zone[0] == '-') concat_zone = 1;
    strncpy (pzone, zone, 5);
    _raise_case (pzone);
  }
  if (isnan (t) || isinf (t)) t = JULIAN_DAY_ZERO;
  if (fabs (t) > 6.776e+16) zone = "JD";

  if (!strcasecmp (zone, "JD") || !strcasecmp (zone, "MJD")) {
    t += tai_adjustment (t, "TDT");
    julianday_from_epoch_time (t);
    if (strcasecmp (zone, "JD")) {
      dattim.julday -= 2400000.5;
      zone = "MJD";
    } else zone = "JD";
    if (precision >= 0) sprintf (format, "%%s_%%.%df", precision);
    else sprintf (format, "%%s_%%.0f");
    sprintf (out, format, zone, dattim.julday);
    return;
  }
  if (!strcasecmp (zone, "ISO")) {
    t += tai_adjustment (t, "UTC");
    date_from_epoch_time (t);
    if (dattim.year < 1583 || dattim.year > 9999) {
      sprintf (pzone, "Z");
      concat_zone = 1;
    } else {
      if (dattim.ut_flag) dattim.second += 1.0;
      if (precision > 0) {
	sprintf (format, "%s%02d.%dfZ", "%04d-%02d-%02dT%02d:%02d:%",
	    precision+3, precision);
	sprintf (out, format, dattim.year, dattim.month, dattim.dofm,
            dattim.hour, dattim.minute, dattim.second);
      } else if (precision == 0) {
	sprintf (out, "%04d-%02d-%02dT%02d:%02d:%02.0fZ",
	    dattim.year, dattim.month, dattim.dofm,
            dattim.hour, dattim.minute, dattim.second);
      } else switch (precision) {
	case (-1) :
	  sprintf (out, "%04d-%02d-%02dT%02d:%02dZ",
	      dattim.year, dattim.month, dattim.dofm, dattim.hour,
	      dattim.minute);
          break;
	case (-2) :
	  sprintf (out, "%04d-%02d-%02dT%02dZ",
	      dattim.year, dattim.month, dattim.dofm, dattim.hour);
          break;
	case (-3) :
	  sprintf (out, "%04d-%02d-%02d",
	       dattim.year, dattim.month, dattim.dofm);
          break;
	case (-4) :
	  sprintf (out, "%04d-%02d", dattim.year, dattim.month);
          break;
	default :
	  sprintf (out, "%04d", dattim.year);
      }
      return;
    }
  }

  t += tai_adjustment (t, zone);
  date_from_epoch_time (t);
  if (dattim.ut_flag) dattim.second += 1.0;
  if (precision > 0) {
    if (nozone) {
      sprintf (format, "%s%02d.%df", "%04d.%02d.%02d_%02d:%02d:%",
	  precision+3, precision);
      sprintf (out, format, dattim.year, dattim.month, dattim.dofm,
        dattim.hour, dattim.minute, dattim.second);
    } else if (concat_zone) {
      sprintf (format, "%s%02d.%df%%s",
	"%04d.%02d.%02d_%02d:%02d:%", precision+3, precision);
      sprintf (out, format, dattim.year, dattim.month, dattim.dofm,
	  dattim.hour, dattim.minute, dattim.second, pzone);
    } else {
      sprintf (format, "%s%02d.%df_%%s", "%04d.%02d.%02d_%02d:%02d:%",
	precision+3, precision);
      sprintf (out, format, dattim.year, dattim.month, dattim.dofm,
	  dattim.hour, dattim.minute, dattim.second, pzone);
    }
  } else if (precision == 0)
    if (nozone) sprintf (out, "%04d.%02d.%02d_%02d:%02d:%02.0f",
	dattim.year, dattim.month, dattim.dofm,
        dattim.hour, dattim.minute, dattim.second);
    else if (concat_zone) sprintf (out, "%04d.%02d.%02d_%02d:%02d:%02.0f%s",
	dattim.year, dattim.month, dattim.dofm,
        dattim.hour, dattim.minute, dattim.second, pzone);
    else sprintf (out, "%04d.%02d.%02d_%02d:%02d:%02.0f_%s",
	dattim.year, dattim.month, dattim.dofm,
        dattim.hour, dattim.minute, dattim.second, pzone);
  else {
/*
    while (dattim.second >= 30.0) {
      dattim.minute++;
      dattim.second -= 60.0;
      if (dattim.minute == 60) {
	dattim.minute = 0;
	dattim.hour++;
	if (dattim.hour == 24) {
	  dattim.hour = 0;
	  dattim.dofm++;
	  if (dattim.dofm > molen[dattim.month]) {
	    if (dattim.month == 2) {
	      if ((dattim.year < 1601) && ((dattim.year % 4) == 0)) break;
	      if (((dattim.year % 4) == 0) && (dattim.year % 100)) break;
	      if ((dattim.year % 400) == 0) break;
	    } else {
	      dattim.dofm = 1;
	      dattim.month++;
	      if (dattim.month == 13) {
		dattim.month = 1;
		dattim.year++;
	      }
	    }
	  }
	}
      }
    }
*/
    switch (precision) {
      case (-1) :
	if (nozone) sprintf (out, "%04d.%02d.%02d_%02d:%02d",
	    dattim.year, dattim.month, dattim.dofm, dattim.hour, dattim.minute);
	else if (concat_zone) sprintf (out, "%04d.%02d.%02d_%02d:%02d%s",
	    dattim.year, dattim.month, dattim.dofm,
	    dattim.hour, dattim.minute, pzone);
	else sprintf (out, "%04d.%02d.%02d_%02d:%02d_%s",
	    dattim.year, dattim.month, dattim.dofm,
	    dattim.hour, dattim.minute, pzone);
        break;
      case (-2) :
	if (nozone) sprintf (out, "%04d.%02d.%02d_%02d",
	    dattim.year, dattim.month, dattim.dofm, dattim.hour);
	else if (concat_zone) sprintf (out, "%04d.%02d.%02d_%02d%s",
	    dattim.year, dattim.month, dattim.dofm, dattim.hour, pzone);
	else sprintf (out, "%04d.%02d.%02d_%02d_%s",
	    dattim.year, dattim.month, dattim.dofm, dattim.hour, pzone);
        break;
      case (-3) :
	if (concat_zone) sprintf (out, "%04d.%02d.%02d",
	     dattim.year, dattim.month, dattim.dofm);
	else sprintf (out, "%04d.%02d.%02d_%s",
	      dattim.year, dattim.month, dattim.dofm, pzone);
        break;
      case (-4) :
	if (concat_zone) sprintf (out, "%04d.%02d", dattim.year, dattim.month);
	else sprintf (out, "%04d.%02d_%s", dattim.year, dattim.month, pzone);
        break;
      default :
	if (concat_zone) sprintf (out, "%04d", dattim.year);
	else sprintf (out, "%04d_%s", dattim.year, pzone);
    }
  }
}

                                  /*  UTC (and zone) / TAI time corrections  */
/*
  This function takes a scanned time from a zone-designated string and
  returns the difference (TAI - Zone) for that time; it is intended to be
  used to adjust to the proper TAI time from e.g. UTC. It i snot symmetric
  with tai_adjustment because the change in adjustment must be made between
  the time of the leap second and the accumulated difference, rather than
  merely at the time of the leap second.
*/
double tai_adjustment (TIME t, char *zone) {
  TIME dt;
  int leapsecs, ct;

  dattim.ut_flag = 0;
  dattim.civil = 0;
  if (!strcasecmp (zone, "TAI")) return 0.0;
  if (!strcasecmp (zone, "TDT") || !strcasecmp (zone, "TT")) return 32.184;
  if (!strcasecmp (zone, "GPS")) return -19.0;
       /*  All others civil time, so use universal time coordination offset  */
  dattim.civil = 1;
  leapsecs = sizeof (ut_leap_time) / sizeof (TIME);
  dt = 0.0;
  for (ct=0; ct<leapsecs; ct++) {
    if (t < (ut_leap_time[ct] - 1.0)) break;
    dt -= 1.0;
    if (t < ut_leap_time[ct]) dattim.ut_flag = 1;
  }
  return (dt + zone_adjustment (zone));
}

double zone_adjustment_inner (char *zone, int *valid) {
  TIME dt;
  int status, offset, hours, minutes;

  if (valid)
    *valid = 1;
 
  dt = 0.0;
  hours = minutes = 0;
  status = sscanf (zone, "%5d", &offset);
  if (status) {
    hours = offset / 100;
    minutes = offset % 100;
    dt += 60.0 * (minutes + 60.0 * hours);
    return dt;
  }
  if (strlen (zone) == 1) {
    hours = zone[0] - 'A' + 1;
    if (zone[0] > 'I')
      hours--;
    if (zone[0] > 'M') {
      hours = 'M' - zone[0];
      if (zone[0] == 'Z')
      {
        hours = 0;
      }
    }
    dt += 3600.0 * hours;
    return dt;
  }

  /* "BST" has to be a bug - it appears twice (so it can never set hours to -11) */
  if (!strcmp (zone, "PST") || !strcmp (zone, "YDT"))
    hours = -8;
  else if (!strcmp (zone, "MST") || !strcmp (zone, "PDT"))
    hours = -7;
  else if (!strcmp (zone, "CST") || !strcmp (zone, "MDT"))
    hours = -6;
  else if (!strcmp (zone, "EST") || !strcmp (zone, "CDT"))
    hours = -5;
  else if (!strcmp (zone, "AST") || !strcmp (zone, "EDT"))
    hours = -4;
  else if (!strcmp (zone, "ADT"))
    hours = -3;
  else if (!strcmp (zone, "GMT") || !strcmp (zone, "WET"))
    hours = 0;
  else if (!strcmp (zone, "CET") || !strcmp (zone, "BST"))
    hours = 1;
  else if (!strcmp (zone, "EET"))
    hours = 2;
  else if (!strcmp (zone, "SST") || !strcmp (zone, "WST"))
    hours = 8;
  else if (!strcmp (zone, "JST"))
    hours = 9;
  else if (!strcmp (zone, "JDT"))
    hours = 10;
  else if (!strcmp (zone, "NZST"))
    hours = 12;
  else if (!strcmp (zone, "BST"))
    hours = -11;
  else if (!strcmp (zone, "HST") || !strcmp (zone, "BDT"))
    hours = -10;
  else if (!strcmp (zone, "YST") || !strcmp (zone, "HDT"))
    hours = -9;
  else if (!strcmp (zone, "NZDT"))
    hours = 13;
  else {
     /* This function didn't originally recognize all 'time zones', such as TDT, TAI, UTC, etc.
      * These time systems are comingled with the time zones in the parsing code, so include
      * them here insofar as this is the only place in timeio where the 'time zones' are
      * listed. */
     if (strcasecmp(zone, "TDT") &&
	 strcasecmp(zone, "TAI") &&
	 strcasecmp(zone, "TT") &&
	 strcasecmp(zone, "GPS") &&
	 strcasecmp(zone, "UT") &&
	 strcasecmp(zone, "UTC") &&
	 strcasecmp(zone, "ISO") &&
	 strcasecmp(zone, "TAI") &&
	 strcasecmp(zone, "JD") &&
	 strcasecmp(zone, "MJD")) {
	/* Invalid time zone */
	if (valid)
	  *valid = 0;
     }
  }
    
  dt += 3600.0 * hours;
  return dt;
}

                                                  /*  Zone time corrections  */
double zone_adjustment (char *zone) {
   return zone_adjustment_inner(zone, NULL);
}

int zone_isvalid (char *zone) {
   int valid = 0;
   zone_adjustment_inner(zone, &valid);
   return valid;
}

/* will parse zone out of a possibly larger string */
int parse_zone(const char *zonestr, char *out, int size)
{
   int err = 0;
   char *zone = strdup(zonestr);
   strtoupper(zone);

   if (strlen(zone) == 1)
   {
      /* single char abbreviation */
      char abb = zone[0];
      if (abb == 'A' || abb == 'B' || abb == 'C' || abb == 'D' || abb == 'E' ||
          abb == 'F' || abb == 'G' || abb == 'H' || abb == 'I' || abb == 'K' ||
          abb == 'L' || abb == 'M' || abb == 'N' || abb == 'O' || abb == 'P' ||
          abb == 'Q' || abb == 'R' || abb == 'S' || abb == 'T' || abb == 'U' ||
          abb == 'V' || abb == 'W' || abb == 'X' || abb == 'Y' || abb == 'Z')
      {
         if (size >= 2)
         {
            out[0] = abb;
            out[1] = '\0';
         }
      }
      else
      {
         err = 1;
      }
   }
   else if (zone[0] == '+' || zone[1] == '-')
   {
      /* time zones or the format +XXX or -XXX */
      char *endptr = NULL;
      int tzlen = 0;

      strtol(zone, &endptr, 10);
      tzlen = endptr - zone;

      if (size < tzlen + 1)
      {
         fprintf(stderr, "parse_zone() buff size too small.\n");
         err = 1;
      }
      else
      {
         snprintf(out, tzlen + 1, "%s", zone);
      }
   }
   else
   {
      char *pc = zone;
      char *pout = out;
      int state;

      /* 'Z' - valid time zone */
      /* 'X' - invalid time zone */
      /*  0  - initial state */

      state = 0;

      while (state != 'Z' && state != 'X' && size >= 8)
      {
         if (state == 0)
         {
            state = 'X';

            if (*pc == 'A' || *pc == 'B' || *pc == 'C' || *pc == 'E' || *pc == 'G' || 
                *pc == 'H' || *pc == 'J' || *pc == 'M' || *pc == 'N' || *pc == 'P' ||
                *pc == 'S' || *pc == 'T' || *pc == 'U' || *pc == 'Y' || *pc == 'W')
            {
               state = *pc;
               *pout++ = *pc++;
            }
         }
         else if (state == 'T')
         {
            state = 'X';

            if (*pc)
            {
               if (*pc == 'A')
               {
                  *pout++ = *pc++;
                  if (*pc && *pc == 'I')
                  {
                     /* TAI */                        
                     state = 'Z';
                     *pout++ = *pc++;
                  }
               }
               else if (*pc == 'D')
               {
                  *pout++ = *pc++;
                  if (*pc && *pc == 'T')
                  {
                     /* TDT */
                     state = 'Z';
                     *pout++ = *pc++;
                  }
               }
               else if (*pc == 'T')
               {
                  /* TT */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'U')
         {
            state = 'X';
            
            if (*pc && *pc == 'T')
            {
               state = 'Z';
               *pout++ = *pc++;

               if (*pc && *pc == 'C')
               {
                  /* UTC */
                  *pout++ = *pc++;
               }
               else 
               {
                  /* UT */
               }
            }
         }
         else if (state == 'A')
         {
            state = 'X';

            if (*pc && (*pc == 'S' || *pc == 'D'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* ADT or AST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'B')
         {
            state = 'X';

            if (*pc && (*pc == 'S' || *pc == 'D'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* BDT or BST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'C')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'E' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* CDT, CET, or CST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'E')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'E' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* EDT, EET, or EST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'G')
         {
            state = 'X';

            if (*pc && *pc == 'M')
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* GMT */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'H')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* HDT or HST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'J')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* JDT or JST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'M')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* MDT or MST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'N')
         {
            state = 'X';

            if (*pc && (*pc == 'Z'))
            {
               *pout++ = *pc++;
               if (*pc && (*pc == 'D' || *pc == 'S'))
               {
                  *pout++ = *pc++;
                  if (*pc && *pc == 'T')
                  {
                     /* NZDT or NZST */
                     state = 'Z';
                     *pout++ = *pc++;
                  }
               }
            }
         }
         else if (state == 'P')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* PDT or PST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'S')
         {
            state = 'X';

            if (*pc && *pc == 'S')
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* SST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'Y')
         {
            state = 'X';

            if (*pc && (*pc == 'D' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* YDT or YST */
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
         else if (state == 'W')
         {
            state = 'X';

            if (*pc && (*pc == 'E' || *pc == 'S'))
            {
               *pout++ = *pc++;
               if (*pc && *pc == 'T')
               {
                  /* WET or WST*/
                  state = 'Z';
                  *pout++ = *pc++;
               }
            }
         }
      }

      if (size < 8)
      {
         fprintf(stderr, "parse_zone() buff size too small.\n");
         err = 1;
      }
      else if (state == 'Z')
      {
         *pout = '\0';
      }
      else if (state == 'X')
      {
         fprintf(stderr, "Invalid time zone string '%s'.\n", zonestr);
         err = 1;
      }
   }

   if (zone)
   {
      free(zone);
   }

   return err;
}

int time_is_invalid (TIME t) {
  return (isnan (t) || (t < JULIAN_DAY_ZERO + 10.0e-5 && t > JULIAN_DAY_ZERO - 10.0e-5));
}

/* Returns 1 is the time string is a valid time. */
/* Didn't include dattim.ut_flag since is isn't really something that 
 * is parsed from a timestr.  It is deduced from the clock field. 
 * Didn't include dattim.civil because it is also isn't something that 
 * is parsed from a time string.  Also, it is never used in timeio (it
 * is set, but not used).
 */
int parsetimestr (const char *timestr,
                  int **year,
                  int **month,
                  int **dofm,
                  int **dofy,
                  int **hour,
                  int **minute,
                  double **second,
                  char **zone,
                  double **juliday,
                  int *consumedout) {
   int ret = 0;
   int status;
   char ls[256];

   /* These 3 fields contain the original raw strings from which the date, 
    * clock and time zone were determined. If there were only 2 fields, and
    * the second field contained a time zone, then f2 == NULL and f3 = the time 
    * zone. */
   char *f1 = NULL; /* date */
   char *f2 = NULL; /* clock time */
   char *f3 = NULL; /* time zone */
   int isjd = 0;
   int consumed = 0;

   strncpy (ls, timestr, 255);
   ls[255] = '\0';
   status = _parse_date_time_inner (ls, &f1, &f2, &f3, &isjd, &consumed);
   if (status != -1) {
      if (consumedout)
      {
         *consumedout = consumed;
      }

      if (!isjd)
      {
         /* YYYY.MM.DD is required in a time string (or juliday) */
         if (year) 
         {
            *year = malloc(sizeof(int));
            **year = dattim.year;
         }

         if (month)
         {
            *month = malloc(sizeof(int));
            **month = dattim.month;
         }

         if (dofm) 
         {
            *dofm = malloc(sizeof(int));
            **dofm = dattim.dofm;
         }

         if (juliday)
         {
            *juliday = NULL;
         }
      }
      else
      {
         if (year)
         {
            *year = NULL;
         }

         if (month)
         {
            *month = NULL;
         }

         if (dofm)
         {
            *dofm = NULL;
         }

         if (juliday)
         {
            *juliday = malloc(sizeof(double));
            **juliday = dattim.julday;
         }
      }

      if (f2)
      {
         if (hour) 
         {
            *hour = malloc(sizeof(int));
            **hour = dattim.hour;
         }
         if (minute) 
         {
            *minute = malloc(sizeof(int));
            **minute = dattim.minute;
         }
         if (second) 
         {
            *second = malloc(sizeof(double));
            **second = dattim.second;
         }
      }
      else
      {
         if (hour) *hour = NULL;
         if (minute) *minute = NULL;
         if (second) *second = NULL;
      }

      /* Don't use the value of dattim.zone - it is not what was parsed. 
       * Under the hood the value passed in might be changed to UTC. 
       * If f3 is not NULL, then the timezone passed in was valid
       */
      if (zone)
      {
         if (f3)
         {
            *zone = strdup(f3);         
         }
         else
         {
            *zone = NULL;
         }
      }

      if (dofy)
      {
         /* timeio doesn't ever set or use dattim.dofy - so always return NULL */
         *dofy = NULL;
      }

      ret = 1;
   }

   if (f1)
   {
      free(f1);
   }

   if (f2)
   {
      free(f2);
   }

   if (f3)
   {
      free(f3);
   }

   return ret;
}

/*
 *  Revision History (all mods by Rick Bogart unless otherwise noted)
 *  V 1.0  07.05.04	created this file, based on timerep.c
 *		in ~CM/src/libast.d, q.v. for earlier revision history
 *    Changes in this version:
 *	removed setting of soi_errno
 *	better checking for null, empty, or erroneous zone strings in
 *	  sprint_time()
 *	single-letter zone designations printed without intervening "_"
 *	parse errors in sscan_time cause return of time JD_0.0 rather than
 *	  JD_0.0_TAI (= JD_0.0 + 32.184 sec), with consistent clock time
 *	removed BST as known designation
 *	added option for negative precision values < -1 in sprint_time
 *	added support for output in JD and MJD formats
 *	with precision < 0, times are truncated rather than rounded to nearest
 *	  minute
 *	fixed up reading of dates without appended clock times
 *  07.05.16	added support for certain ISO 8601 representations on
 *	output, timezone offsets with colon, standard Australian time
 *	zones and a few others
 *  07.05.25	removed undocumented special treatment of 2-digit years
 *	added function time_is_invalid
 *  07.06.21	minor fix to guarantee null-terminated strings in sscan_time
 *  07.06.26	additional fix to guarantee null-terminated strings in
 *	memory element dattim.zone
 *  07.10.16	plugged memory leak from strdup
 *  08.02.21	in sprint_time, put in checks for NaN's or Inf's, forcing
 *	printing of JD_0.0, and for times that would result in years out of
 *	signed int range, force JD format
 */
