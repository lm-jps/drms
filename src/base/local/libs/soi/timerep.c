/* COPIED from SOI's libast.d, then modified.  This wouldn't compile as is with gcc.
 * Specifically, utc_adjustment() was not declared before being used.
 *
 * arta 5/30/2007
 */


/*
 *  timerep.c                                       ~soi/(rel)/src/libast.d
 *
 *  Functions to deal with character representations of times in clock,
 *    calendric, and other forms.
 *
 *  Responsible:
 *    Rick Bogart                               RBogart@solar.Stanford.EDU
 *
 *  Function and macros:
 *    TIME sscan_time (char *string);
 *    void sprint_time (char *string, TIME t, char *zone, int precision);
 *    sprint_at (char *string, TIME t);
 *    sprint_dt (char *string, TIME t);
 *    sprint_ut (char *string, TIME t);
 *    double tai_adjustment (TIME t, char *zone);
 *    double utc_adjustment (TIME t, char *zone);
 *    double zone_adjustment (TIME t, char *zone);
 *
 *  Bugs:
 *    There is no function to print times in Julian day format.
 *    Ephemeris time and "Carrington time" are not supported.
 *    Dates B.C are not handled historically; year 0 is assumed.
 *    The asymmetry between tai_adjustment and utc_adjustment should be
 *      repaired; likewise between conversions to and from date.
 *    The strtok() function used extensively is not reliably consistent on
 *      different platforms; in particular it is known not to work on the
 *      NeXT.  So far testing has been restricted to SGI and DEC RISC.
 *    Indecipherable strings do not produce a unique time, but rather one
 *      that is within clock time (normally 24 hours) of the Julian day
 *      epoch, 1.Jan.4713 BC.
 *    Unlike the old atodate functions, the date_time structure is not
 *      exported, so is unavailable for direct inspection or modification
 *      except through the sprint_time and sscan_time functions; this is
 *      a feature.
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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <soi_error.h>
#include <timeio.h>

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
} dattim;

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
 915148833.0                                                 /*  2006.01.01  */
/*
 *
 *  The following were predicted dates of UTC adjustments only, but the
 *    offset remains constant at 32 seconds, as adjustments ceased to be made
 *    and will likely never be made, UTC having been frozen to a fixed offset
 *    from TAI as of Jan. 1 1999.  However, the calculation of tai_adjustment
 *    only counts the number, it doesn't look at the actual value, so these
 *    were fully commented out on 21 July 2003.
 *
 741484832.0,                                                    2000.07.01
 788918432.0,                                                    2002.01.01
 820454432.0,                                                    2003.01.01
 851990432.0,                                                    2004.01.01
 883612832.0,                                                    2005.01.01
 899251232.0,                                                    2005.07.01
 930787232.0,                                                    2006.07.01
 962323232.0,                                                    2007.07.01
 993945632.0,                                                    2008.07.01
1025481632.0                                                     2009.07.01
*/
/*
 *  ***  NOTE  Please notify Roger Chevalier at EOF
 *       whenever any of these times are updated!   ***
 */
};

static void date_from_epoch_time (TIME t);
static TIME epoch_time_from_date ();
static TIME epoch_time_from_julianday ();
double utc_adjustment (TIME t, char *zone);

static void _clear_date_time () {
/*
 *  Clear the date_time struct to time 0 (epoch 1977.0_TAI)
 */
    dattim.julday = 0.0;
    strcpy (dattim.zone, "TAI");
    date_from_epoch_time (epoch_time_from_julianday ());
}

static int _parse_error () {
  _clear_date_time ();
  soi_errno = ILLEGAL_TIME_FORMAT;
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

static int _parse_clock (char *str) {
/*
 *  Read time elements from wall-clock string HH:MM:SS.SSS
 */
  int status;
  char *field0, *field1, *field2;

  field0 = strtok (str, ":");
  status = sscanf (field0, "%d", &dattim.hour);
  if (status != 1) return _parse_error ();
  field1 = strtok (NULL, ":");
  if (!field1) return _parse_error ();
  status = sscanf (field1, "%d", &dattim.minute);
  if (status != 1) return _parse_error ();
  field2 = strtok (NULL, ":");
  if (!field2) {
                         /*  Optional seconds field missing: default is 0.0  */
    dattim.second = 0.0;
    return (2);
  }
  status = sscanf (field2, "%lf", &dattim.second);
  if (status != 1) return _parse_error ();
                              /*  Set a flag in case it is a UT leap second  */
  dattim.ut_flag = (dattim.second >= 60.0);
  return (3);
}

static int _parse_month_name (char *moname) {
  int month;

  if (!strncmp (moname, "JAN", 3) || !strcmp (moname, "I"))
    month = 1;
  else if (!strncmp (moname, "FEB", 3) || !strcmp (moname, "II"))
    month = 2;
  else if (!strncmp (moname, "MAR", 3) || !strcmp (moname, "III"))
    month = 3;
  else if (!strncmp (moname, "APR", 3) || !strcmp (moname, "IV"))
    month = 4;
  else if (!strncmp (moname, "MAY", 3) || !strcmp (moname, "V"))
    month = 5;
  else if (!strncmp (moname, "JUN", 3) || !strcmp (moname, "VI"))
    month = 6;
  else if (!strncmp (moname, "JUL", 3) || !strcmp (moname, "VII"))
    month = 7;
  else if (!strncmp (moname, "AUG", 3) || !strcmp (moname, "VIII"))
    month = 8;
  else if (!strncmp (moname, "SEP", 3) || !strcmp (moname, "IX"))
    month = 9;
  else if (!strncmp (moname, "OCT", 3) || !strcmp (moname, "X"))
    month = 10;
  else if (!strncmp (moname, "NOV", 3) || !strcmp (moname, "XI"))
    month = 11;
  else if (!strncmp (moname, "DEC", 3) || !strcmp (moname, "XII"))
    month = 12;
  else
                                                /*  Unrecognized month name  */
    month = 0;
  return (month);
}

static int _parse_date (char *str) {
/*
 *  Read date elements from calendar string YYYY.{MM|nam}.DD[.ddd]
 */
  double fracday;
  int status, dfrac;
  char *field0, *field1, *field2, *field3;
  char daystr[32];

  field0 = strtok (str, ".");
  field1 = strtok (NULL, ".");
  field2 = strtok (NULL, ".");
  field3 = strtok (NULL, ".");
  status = sscanf (field0, "%d", &dattim.year);
  if (status != 1) return _parse_error ();
  if (strlen (field0) == 2) {
    /*  default for 2-digit year strings is that they are base 1900 or 2000  */
    if (dattim.year < 10) dattim.year += 100;
    dattim.year += 1900;
  }
  if (!field1) return _parse_error ();
  status = sscanf (field1, "%d", &dattim.month);
  if (status != 1) {
    dattim.month = _parse_month_name (field1);
    if (dattim.month == 0) return _parse_error ();
  }
  if (!field1) return _parse_error ();
  status = sscanf (field2, "%d", &dattim.dofm);
  if (status != 1) return _parse_error ();
  if (field3) {
    status = sscanf (field3, "%d", &dfrac);
    if (status) {
                                     /*  Day of month is in fractional form  */
      sprintf (daystr, "%d.%d", dattim.dofm, dfrac);
      sscanf (daystr, "%lf", &fracday);
      _fracday_2_clock (fracday);
      status = 6;
    }
  }
  else status = 3;
  return status;
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

static int _parse_date_time (char *str) {
/*
 *  Parse a date-time string in one of the standard forms specified in
 *    SOI TN 94-116
 *  Returns 0 if string is in calendar-clock form, 1 if it is in Julian day
 *    form, and -1 if it cannot be successfully parsed (in which case the
 *    time is cleared to JD 0.0).
 */
  double fdofm;
  int status;
  int length;
  char *field0, *field1, *field2;

  _raise_case (str);
  length = strlen (str);
  if (!length) return _parse_error ();
  field0 = strtok (str, "_");
  if ((strlen (field0)) == length) {
     /*  No "_" separators: field (if valid) must be of type YYYY.MM.dd.ddd  */
                                                         /*  Default is UTC  */
    status = sscanf (str, "%d.%d.%lf", &dattim.year, &dattim.month, &fdofm);
    if (status != 3) return _parse_error ();
    _fracday_2_clock (fdofm);
    strcpy (dattim.zone, "UTC");
    return 0;
  }
              /*  First field must either be calendar date or "MJD" or "JD"  */
  field1 = strtok (NULL, "_");
  if (!(strcmp (field0, "MJD")) || !(strcmp (field0, "JD"))) {
    status = sscanf (field1, "%lf", &dattim.julday);
    if (status != 1) return _parse_error ();
    field2 = strtok (NULL, "_");
    if (field2)
      strcpy (dattim.zone, field2);
    else
                                 /*  Default for Julian day notation is TDT  */
      strcpy (dattim.zone, "TDT");
    if (field0[0] == 'M')
              /*  Modified Julian date (starts at midnight) : add 2400000.5  */
      dattim.julday += 2400000.5;
    return 1;
  }
                /*  First field is calendar date with optional day fraction  */
  dattim.julday = 0.0;
  field2 = strtok (NULL, "_");
  status = _parse_date (field0);
  if (status == 3) {
    status = _parse_clock (field1);
    if (!status) return _parse_error ();
  }
  else if (status == 6)
    field2 = field1;
  if (field2)
    strcpy (dattim.zone, field2);
  else
                             /*  Default for calendar-clock notation is UTC  */
    strcpy (dattim.zone, "UTC");
  return 0;
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
  }
  else {
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

  t = dattim.second + 60.0 * (dattim.minute + 60.0 * (dattim.hour));
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

static TIME epoch_time_from_julianday () {
  TIME t;

  t = SEC_DAY * (dattim.julday - JD_EPOCH);
  return (t);
}

TIME sscan_time (char *s) {
  TIME t;
  int status;
  char ls[256];

  strcpy (ls, s);
  status = _parse_date_time (ls);
  if (status)
    t = epoch_time_from_julianday ();
  else
    t = epoch_time_from_date ();
  t -= tai_adjustment (t, dattim.zone);
  return (t);
}

void sprint_time (char *out, TIME t, char *zone, int precision) {
  char format[64];

  t += utc_adjustment (t, zone);
  date_from_epoch_time (t);
  if (dattim.ut_flag) {
    dattim.second += 1.0;
  }
  if (precision > 0) {
    sprintf (format, "%s%02d.%df_%%s", "%04d.%02d.%02d_%02d:%02d:%",
      precision+3, precision);
    sprintf (out, format, dattim.year, dattim.month, dattim.dofm,
      dattim.hour, dattim.minute, dattim.second, zone);
  } else if (precision == 0)
    sprintf (out, "%04d.%02d.%02d_%02d:%02d:%02.0f_%s",
      dattim.year, dattim.month, dattim.dofm,
      dattim.hour, dattim.minute, dattim.second, zone);
  else {
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
    sprintf (out, "%04d.%02d.%02d_%02d:%02d_%s",
      dattim.year, dattim.month, dattim.dofm,
      dattim.hour, dattim.minute, zone);
  }
}

double tai_adjustment (TIME t, char *zone) {
  TIME dt;
  int leapsecs, ct;

  _raise_case (zone);
  if (!strcmp (zone, "TAI")) {
    dattim.civil = 0;
    return 0.0;
  }
  if (!strcmp (zone, "TDT") || !strcmp (zone, "TT")) {
    dattim.civil = 0;
    return 32.184;
  }
  if (!strcmp (zone, "GPS")) {
    dattim.civil = 0;
    return -19.0;
  }
       /*  All others civil time, so use universal time coordination offset  */
  dattim.civil = 1;
  leapsecs = sizeof (ut_leap_time) / sizeof (TIME);
  dt = 0.0;
  if (t >= ut_leap_time[0]) {
    t += 1.0;
    for (ct=0; ct<leapsecs && t>=ut_leap_time[ct]; ct++) {
      t += 1.0;
      dt -= 1.0;
    }
    if (dattim.ut_flag) dt += 1.0;
  }
  return (dt + zone_adjustment (zone));
}
                                                  /*  Zone time corrections  */
double utc_adjustment (TIME t, char *zone) {
  TIME dt;
  int leapsecs, ct;

  dattim.ut_flag = 0;
  _raise_case (zone);
  if (!strcmp (zone, "TAI")) {
    dattim.civil = 0;
    return 0.0;
  }
  if (!strcmp (zone, "TDT") || !strcmp (zone, "TT")) {
    dattim.civil = 0;
    return 32.184;
  }
  if (!strcmp (zone, "GPS")) {
    dattim.civil = 0;
    return -19.0;
  }
       /*  All others civil time, so use universal time coordination offset  */
  dattim.civil = 1;
  leapsecs = sizeof (ut_leap_time) / sizeof (TIME);
  dt = 0.0;
  for (ct=0; ct<leapsecs; ct++) {
    if (t < (ut_leap_time[ct] - 1.0))
      break;
    dt -= 1.0;
    if (t < ut_leap_time[ct])
      dattim.ut_flag = 1;
/*
    else
      t -= 1.0;
*/
  }
  return (dt + zone_adjustment (zone));
}
                                                  /*  Zone time corrections  */
double zone_adjustment (char *zone) {
  TIME dt;
  int status, offset, hours, minutes;
  
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
        hours = 0;
    }
    dt += 3600.0 * hours;
    return dt;
  }
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
  dt += 3600.0 * hours;
  return dt;
}

/*
 *  Revision History
 *  V 1.0   95.01.30    Rick Bogart     created this file
 *          95.02.01    R Bogart        changed names from sscant and sprintt
 *              to sscan_time and sprint_time
 *          95.02.03    R Bogart        added precision argument to sprint_time
 *          95.02.22    R Bogart        added sun- (gcc-) specific include
 *          95.06.01    R Bogart        removed unused variables; added stdio
 *              to prototype floating-point arguments to varargs
 *  v 2.1   96.12.30	R Bogart	fixed bug in date_from_epoch_time()
 *		that caused sprint_time to produce wrong string for last day
 *		of bissextile years from 1980 onward; however, a serious bug
 *		affecting all dates prior to 1897.01.01 remains.
 *	    97.01.07	R Bogart	rewrote date_from_epoch to fix known
 *		bugs
 *  v 2.3   97.04.14	R Bogart	corrected *comments* in ut leap second
 *		section for dates through 1994; *changed* date of predicted
 *		leap second from 1997.01.01 to actual at 1997.07.01 & beyond:
 *		changed dates for 1998 & 1999, added predictions from 7/1/2005
 *		through 7/1/2009
 *  v 2.7   97.12.16	R Bogart	fixed bug in reporting times truncated
 *		to nearest minute (precision argument < 0) during last 30
 *		seconds of hour.
 *  v 4.1  98.11.23	R Bogart	corrected (predicted) leap second time
 *		from 98.07.01 to 99.01.01 and modified future predictions
 *	   99.01.19	R Bogart	added notification comment
 *  v 4.4  99.09.14	R Bogart	removed predicted leap second for
 *		00.00.01, modified future predictions
 *  v 4.8  00.07.25	R Bogart	corrected "predicted" leap second for
 *		00/07/01, modified future predictions to assume that adjustments
 *		will cease
 *  v 4.9  03.07.21	R Bogart	removed time predictions for (non-)
 *		adjustments that led to bug in UTC/TAI correction; added GPS
 *		time (= TAI - 19 sec)
 *  v 5.9  05.09.21	R Bogart	added scheduled leap second for 06.01.01
 *	   05.11.10	R Bogart	made functions static at declaration,
 *		and eliminated superfluous prototypes
 *  v 6.0  07.05.04	R Bogart	changed name of relevant include file;
 *		changed type declarations of adjustment functions from TIME to
 *		double; updated comments
 */

/*
$Id: timerep.c,v 1.1.1.1 2007/10/02 00:12:20 arta Exp $
$Source: /home/akoufos/Development/Testing/jsoc-4-repos-0914/JSOC-mirror/JSOC/src/base/local/libs/soi/Attic/timerep.c,v $
$Author: arta $
 *
 * $Log: timerep.c,v $
 * Revision 1.1.1.1  2007/10/02 00:12:20  arta
 * First new, reorganized JSOC tree
 *
 * Revision 1.1  2007/06/21 19:45:40  arta
 * Rechecking these in.  CVS checked them into su_interal, not su_internal last time.
 *
 * Revision 1.1  2007/06/21 16:42:34  arta
 * SOI library plug-in.  Called from libdsds.so to open DSDS records and read data.  Not to be exported outside of Stanford.
 *
 * Revision 1.23  2007/05/08  22:21:16  rick
 * see above
 *
 * Revision 1.22  2005/11/11 01:04:40  rick
 * see above
 *
 * Revision 1.21  2005/09/21 18:03:05  rick
 * see above
 *
 * Revision 1.20  2003/11/18 22:00:09  jim
 * include stdlib.h
 *
 * Revision 1.19  2003/11/12  22:50:57  jim
 * need to include string.h for linuxia64 for strtok() def
 *
 * Revision 1.18  2003/07/21  19:57:48  rick
 * see above
 *
 * Revision 1.17  2000/07/25 16:45:34  rick
 * see above
 *
 * Revision 1.16  1999/09/14  18:21:06  rick
 * see above
 *
 * Revision 1.15  1999/01/19  19:12:35  rick
 * see above
 *
 * Revision 1.14  1998/11/23  20:07:57  rick
 * see above
 *
 * Revision 1.13  1997/12/15  17:12:55  rick
 * see above
 *
 * Revision 1.12  1997/04/17  21:53:45  rick
 * see above
 *
 * Revision 1.11  1997/01/07  23:58:34  rick
 * see above
 * D
 *
 * Revision 1.10  1996/12/31  06:01:53  rick
 * see above
 *
 * Revision 1.9  1995/06/13  18:48:04  rick
 * see above
 *
 * Revision 1.8  1995/03/17  00:54:51  CM
 * auto rcsfix by CM
 * */
