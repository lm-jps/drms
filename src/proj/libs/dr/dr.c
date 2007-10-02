/*****************************************************************************/
/*  stuff that should be in libraries and includes			     */
/*****************************************************************************/

/* map of contents:
	double	A_Signaling_dNaN () {
	double	A_Quiet_dNaN () {
	float	A_Signaling_fNaN () {
	float	A_Quiet_fNaN () {
	double	dInfinity () {
	float	fInfinity () {
static	struct	date_time {
static	int	molen[] = {31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static	double	ut_leap_time[] = {
static	void	date_from_epoch_time (TIME t);
static	TIME	epoch_time_from_date ();
static	TIME	epoch_time_from_julianday ();
static	void	_clear_date_time () {
static	int	_parse_error () {
static	void	_fracday_2_clock (double fdofm) {
static	int	_parse_clock (char *str) {
static	int	_parse_month_name (char *moname) {
static	int	_parse_date (char *str) {
static	void	_raise_case (char *s) {
static	int	_parse_date_time (char *str) {
static	void	date_from_epoch_time (TIME t) {
static	TIME	epoch_time_from_date () {
static	TIME	epoch_time_from_julianday () {
static	TIME	zone_adjustment (char *zone) {
static	_llist	*dr_LastElement (_llist **l) {
static	_llist	*dr_insert (_llist *ll, _llist* new) {
static	_llist	*dr_GetNthElement (_llist **l, int n) {
static	int	dr_search_attr_mapfn (ATTRIBUTES *attr, char *key)
	char	*dr_attrname (ATTRIBUTES *attr)
	char	*dr_attrvalue_str (ATTRIBUTES *attr)
static	ATTRIBUTES	*dr_search_attr (DR *dr, char *key)
	int	dr_attrtype(ATTRIBUTES *attr)
	int	dr_attribute_type (DR *drptr, char *key)
	char	*dr_search_attrvalue_str (DR *dr, char *key)
	double	dr_search_attrvalue_double (DR *dr, char *key)
	double	dr_bscale (DR *dr)
	double	dr_bzero (DR *dr)
	int	dr_datatype (DR *dr)
	int	dr_numbytes (DR *dr)
	int	dr_rank (DR *dr)
	int	*dr_length (DR *dr)
	int	dr_dim_n (DR *dr, int n)
	long	dr_data_length (DR *dr)
	int	dr_sizeof (int datatype)
static	int	append_internal_str (char **old, char *new)
	int	dr_append_comment (DR *dr, char *str)
	int	dr_append_history (DR *dr, char *str)
	char	*dr_datatypename (DR *dr)
	extern	int dr_set_data (DR *dr, void *data);
	int	*dr_malloc_length (int rank)
	char	*dr_malloc_string (int numchars)
	void	*dr_malloc_fillvalue (int datatype)
	void	*dr_std_fillvalue (int datatype)
static	void	dr_free_attr (ATTRIBUTES *attr)
static	void	dr_free_color (DR *dr)
	char	*dr_malloc_data (long numbytes)
	int	dr_create_data (DR *dr)
	void	dr_free_data (DR *dr)
static	void	dr_free_ptrs (DR *dr)
	void	dr_free (DR **drptr)
	int	dr_set_attrvalue (ATTRIBUTES *attr, void *value, int datatype)
	int	dr_set_attrname (ATTRIBUTES *attr, char *name)
	int	dr_set_comment (ATTRIBUTES *attr, char *comment)
	int	dr_set_attribute (DR *dr, char *key, void *value, int attrtype, char *comment)
	int	dr_set_data (DR *dr, void *data)
	int	dr_set_fillvalue (DR *dr, void *fillvalue)
	int	dr_set_numbytes (DR *dr, int numbytes)
	int	dr_set_datatype (DR *dr, int datatype)
	int	dr_set_rank (DR *dr, int rank)
static	void	dr_free_one_attr (ATTRIBUTES *attr)
static	int	dr_remove_attr_mapfn (ATTRIBUTES *thisattr, char *key)
	int	dr_remove_attribute(DR *dr, char *key)
	char	*ConvertToUpper (char *line) {
	char	*ConvertToLower (char *line) {
static	char	*FirstNonWhite (char *line)
static	char	*LastNonWhite (char *line)
static	int	numType (char *num)
static	int	header_value_type (char *str)
	int	dr_set_scaling (DR *dr, int bits, double scale, double bias)
	int	dr_data_convert (DR *dr, int type_out)
	int	dr_scale_data (DR *dr, double scale, double bias) {
static	int	dr_flip (void *data, long length, int size) {
	int	dr_flip_data (DR *dr) {
static	unsigned	short ReadDataType (char *line) {
static	char	*ReadComment (char *line, int datatype) {
static	ATTRIBUTES	*GetHeaderRecord (DR *dr, FILE *fp) {
static	void	SetDiminfo (DR *dr) {
static	void	ProcessAttrList (DR *dr, int conforming) {
static	int	read_fits_head (DR *dr, FILE *fp) {
static	int	read_fits_data (DR *dr, FILE *fp) {
static	char	*AttrValue (ATTRIBUTES *attr)
static	int	PutHeaderRecord (ATTRIBUTES *attr, FILE *fp) {
static	int	PutAttributes (DR *dr, FILE *fp)
static	int	PutOneMandatory (char *key, void *value, int attrtype,
static	int	PutMandatory (DR *dr, FILE *fp) {
static	int	PutCommentary (DR *dr, FILE *fp) {
static	int	PutRemainBlanks (int n, FILE *fp) {
static	int	write_fits_head (DR *dr, FILE *fp) {
static	int	write_fits_data (DR *dr, FILE *fp) {
	int	dr_write_fits (DR *dr, FILE *out) {
	int	getColorTable (char *name, int *bpp, int *Red, int *Green, int *Blue) {
	char	*dr_getkey_str(DR *dr, char *key)
	char	*DR_getkey_str(DR *dr, char *key)
	double	dr_getkey_double(DR *dr, char *key);
	int	dr_getkey_int(DR *dr, char *key)
	double	dr_getkey_double(DR *dr, char *key)
	int	dr_setkey_int(DR *dr, char *key, int val)
	int	dr_setkey_double(DR *dr, char *key, double val)
	int	dr_setkey_time(DR *dr, char *key, TIME time)
	int	dr_setkey_time_interval(DR *dr, char *key, TIME time)
	
*/

#include "drms.h"
#include "dr.h"

/*****************************************************************************/
// from src/libM.d/NaNs.c
/*****************************************************************************/

double dr_A_Signaling_dNaN () {
  dNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  x.NaN_mask.frac2 = 0;
  return (x.d);
}

double dr_A_Quiet_dNaN () {
  dNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = ~0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  x.NaN_mask.frac2 = 0;
  return (x.d);
}

float dr_A_Signaling_fNaN () {
  fNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  return (x.f);
}

float dr_A_Quiet_fNaN () {
  fNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = ~0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  return (x.f);
}

double dr_dInfinity () {
  dNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = 0;
  x.NaN_mask.frac1 = 0;
  x.NaN_mask.frac2 = 0;
  return (x.d);
}

float dr_fInfinity () {
  fNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = 0;
  x.NaN_mask.frac1 = 0;
  return (x.f);
}


/*
 *  Revision History
 */


#ifdef TIMEREP

/*****************************************************************************/
// from src/libast.d/timerep.c
/*****************************************************************************/

/*
 *  timerep.c                                       ~soi/(rel)/src/libast.d
 *
 *  Functions to deal with character representations of times in clock,
 *    calendric, and other forms.
 *  The primary functions provided are:
 *    TIME sscan_time (char *string);
 *    void sprint_time (char *string, TIME t, char *zone);
 *
 *  Responsible:
 *    Rick Bogart                               RBogart@solar.Stanford.EDU
 *
 *  Function and macros:
 *    TIME sscan_time (char *string);
 *    void sprint_time (char *string, TIME t, char *zone);
 *    sprint_at (char *string, TIME t);
 *    sprint_dt (char *string, TIME t);
 *    sprint_ut (char *string, TIME t);
 *
 *  Bugs:
 *    There is no way to specify precision in sprint_time; seconds are
 *      unconditionally displayed to 3 decimal digits (1 msec).
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
/*
#ifdef __sun__
#include <stddef.h>
#endif

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
*/
#include <string.h>
#include <stdio.h>

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
                                                  /*  Zone time corrections  */
static TIME zone_adjustment (char *zone) {
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

TIME _tai_adjustment (TIME t, char *zone) {
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

TIME _utc_adjustment (TIME t, char *zone) {
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
/*
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
*/
void _sprint_time (char *out, TIME t, char *zone, int precision) {
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

#endif
/*
 *  Revision History
 */

/*****************************************************************************/

// from libsds.d/sds_llist.c
/*****************************************************************************/

static _llist *dr_LastElement (_llist **l) {
  _llist *result, *ll = *l;

  if (ll == NULL) return NULL; 
  while (ll) {
    result = ll;
    ll = ll->next;
  }
  return result;
}

static _llist *dr_insert (_llist *ll, _llist* new) {
  _llist *temp;

  if ((ll == NULL) || (new == NULL)) return NULL;
  temp = dr_LastElement (&new);
  temp->next = ll->next;
  ll->next = new;
  return new;
}

void dr_AddList (_llist **l, _llist *new) {
  _llist *ll = *l;
  if (new == NULL) return;
  if (ll == NULL) {
    *l = new;
    (*l)->next = NULL;
  } else dr_insert (dr_LastElement (l), new);
}

static _llist *dr_GetNthElement (_llist **l, int n) { 
  int count = 0;
  _llist *ll = *l;

  while (ll) {
    count++;
    if (count == n) return ll;
    ll = ll->next;
  }
  return NULL;
}

_llist *dr_RemoveNthElement (_llist **l, int n) {
  _llist *result, *ll = *l;
  
  if  (ll == NULL) return NULL;
  if (n == 1) {
    result = ll;
    *l = ll->next;
    return (result);
  }
  ll = dr_GetNthElement (l, n-1);
  if (ll && ll->next) {
    result = ll->next;
    ll->next = ll->next->next;
    return (result);
  }
  else return NULL;
}

_llist *dr_MapList (_llist **l, _llistFn fn, void *obj) {
  _llist *hold, *ll = *l;

  while (ll) {
    hold = ll->next;
    if ((*fn)(ll, obj)) break;
    ll = hold;
  }
  return ll;
}

/*****************************************************************************/
// from libsds.d/sds_attr.c
/*****************************************************************************/

#include <string.h>


static int dr_search_attr_mapfn (ATTRIBUTES *attr, char *key)
  {
  if (attr->name == NULL) return 0; 
  return (strcmp (attr->name, key) == 0);
  }

char *dr_attrname (ATTRIBUTES *attr)
  {
  return (attr) ? (attr->name) : (char *)attr;
  }

void *dr_attrvalue (ATTRIBUTES *attr)
  {
  return (attr) ? (attr->value) : attr;
  }

char *dr_attrvalue_str (ATTRIBUTES *attr)
  {
  char *str = NULL;
  static char line[DR_MAXSTRING+1];
  
  if (dr_attrvalue (attr) == NULL) return NULL;
  line[0] = '\0';		/*  just in case  */

  switch(attr->datatype)
    {
    case DR_LOGICAL:
      sprintf (line, "%c", *(char *)attr->value);
      break;
    case DR_BYTE:
      sprintf (line, "%d", *(signed char *)attr->value);
      break;
    case DR_SHORT:
      sprintf (line, "%d", *(short*)attr->value);
      break;
    case DR_INT:
      sprintf (line, "%d", *(int*)attr->value);
      break;
    case DR_LONG:	/*  max 64-bit long int is 19 characters wide;
  			   FITS allows 20. */
      sprintf (line, "%ld", *(long *)attr->value);
      break;
    case DR_FLOAT:
      sprintf (line, "%0.8E", *(float *)attr->value);
      break;
    case DR_DOUBLE:       /* prints with a field of 20 since that's the max
			    allowed by FITS */
      sprintf (line, "%0.12E", *(double *)attr->value);
      break;
    case DR_TIME:
      sprint_time (line, *(double *)attr->value, "UT", 0);
      break;
    case DR_STRING:
      sprintf (line, "%0.*s", DR_MAXSTRING, (char *)attr->value);
      break;
    case DR_VOID:         /*  implies no value (or NULL)  */
      return NULL;
    case DR_COMPLEX:
			/*  NOT_IMPLEMENTED  */
      return NULL;
    default:
			/*  INVALID_DATATYPE  */
      return NULL;
    }
  str = strdup (line);
  return str;
  }

ATTRIBUTES *dr_next_attr (ATTRIBUTES *attr)
  {
  if (attr == NULL) return (NULL);
  else return (attr->next);
  }

ATTRIBUTES *dr_search_attr (DR *dr, char *key)
  {
  ATTRIBUTES *attr;
  if (!dr) return NULL;
  if (!key) return NULL;
  if (!(attr = (ATTRIBUTES *)dr_MapList ((_llist **) &(dr->attrib), 
      (_llistFn) dr_search_attr_mapfn, (void *) key))) 
    return NULL; /* status = ATTRIBUTE_NOT_FOUND; */
  return (attr);
  }

/* return the datatype of the attribute */
int dr_attrtype(ATTRIBUTES *attr)
  {
  if (attr==NULL)
    { /* soi_errno = ATTRIBUTE_PTR_NULL; */
    return 0;
    }
  return ((int)attr->datatype);
  }

/*  return the datatype of the attribute given the drptr and the keyword  */
int  dr_attribute_type (DR *drptr, char *key)
  {
  ATTRIBUTES *attrptr;
  int datatype;

  attrptr = dr_search_attr (drptr, key);
  datatype = dr_attrtype (attrptr);
  return (datatype);
  }

/*
 *  Search for an attribute VALUE given the keyword 
 *    Return the pointer to the value. NULL if none found or error.
 */
void *dr_search_attrvalue (DR *dr, char *key)
  {
  ATTRIBUTES *attr;
  attr = dr_search_attr (dr, key);
  return (dr_attrvalue (attr));
  }

/*
 *  Same as dr_search_attrvalue except that it returns value as a string.
 *    Automatically alloc memory for the string.
 */
char *dr_search_attrvalue_str (DR *dr, char *key)
  {
  ATTRIBUTES *attr;
  attr = dr_search_attr (dr, key);
  return (dr_attrvalue_str (attr));
  }

/* 
 *  Same as dr_search_attrvalue except that it returns value as a double.
 *    NaN is returned if the attrvalue is not a number or a string that can
 *    be interpreted as number
 */

double dr_search_attrvalue_double (DR *dr, char *key)
  {
  ATTRIBUTES *attr;
  double dnum;
  char *endptr[1];

  attr = dr_search_attr (dr, key);
  if (attr)
    {
     switch(attr->datatype)
      {
       case DR_BYTE:
          dnum = *(signed char *)attr->value;
          break;
       case DR_SHORT:
          dnum = (double) *(short *)attr->value;
          break;
       case DR_INT:
          dnum = (double) *(int *)attr->value;
          break;
       case DR_LONG:
          dnum = (double) *(long *)attr->value;
          break;
       case DR_FLOAT:
          dnum = (double) *(float *)attr->value;
          break;
       case DR_DOUBLE:
       case DR_TIME:
          dnum = *(double *)(attr->value);
          break;
       case DR_STRING:
          dnum = strtod (attr->value, endptr);
          if (attr->value == *endptr)
	     {      /* no conversion took place  */
             return (D_MISSING);
             }
          if (**endptr != '\0')
             {                /* not simply a number */
             return (D_MISSING);
             }
          break;
       case DR_COMPLEX:
       case DR_VOID:
       default:
          return (D_MISSING);
     }
     return (dnum);
   }
  else
     return (D_MISSING);
  }


/*****************************************************************************/
// from libsds.d/sds_query.c
/*****************************************************************************/

double dr_bscale (DR *dr)
  {
  return (dr) ? dr->bscale : dr_A_Signaling_dNaN ();
  }

double dr_bzero (DR *dr)
  {
  return (dr) ? dr->bzero : dr_A_Signaling_dNaN ();
  }

void *dr_data (DR *dr)
  {
  return (dr) ? dr->data : NULL;
  }
						    /*  returns -1 on error  */
int dr_datatype (DR *dr)
  {
  return (dr) ? (int)dr->datatype : -1;
  }

int dr_numbytes (DR *dr)
  {
  return (dr) ? (int)dr->datumsize : -1;
  }
						    /*  returns -1 on error  */
int dr_rank (DR *dr)
  {
  return (dr) ? dr->rank : -1;
  }

int *dr_length (DR *dr)
  {
  return (dr) ? dr->length : NULL; 
  }

int dr_dim_n (DR *dr, int n)
  {
  int rank, *length;

  if ((rank = dr_rank (dr)) < 0) return -1;

  if (n < 0 || n > rank) return -1;
  if (rank == 0) return 0;			/*  this is no error  */

  if (!(length = dr_length (dr))) return -1;		/*  but this is  */
  return length[n];
  }

long dr_data_length (DR *dr)
  {
  int rank, i;
  long dlength = 1;

  if ((rank = dr_rank (dr)) < 0) return -1;
  if (rank == 0 || dr->length == NULL) return 0; /* no op */

  for (i=0; i<rank; i++)
    dlength *= dr->length[i];
  return dlength;
  }

int dr_sizeof (int datatype)
  {
  switch (datatype)
    {
    case DR_VOID:
      return 0;
    case DR_BYTE:
    case DR_LOGICAL:
      return sizeof (char);
    case DR_SHORT:
      return sizeof (short);
    case DR_INT:
      return sizeof (int);
    case DR_LONG:
      return sizeof (long);
    case DR_FLOAT:
      return sizeof (float);
    case DR_DOUBLE:
    case DR_TIME:				 /**  treat it as a double  **/
      return sizeof (double);
    case DR_COMPLEX:
      return 2 * sizeof (float);
    case DR_STRING:
      return sizeof (char *);
    default:
      return DR_INVALID_DATATYPE;
    }
  }

/*****************************************************************************/
// from libsds.d/sds_utility.c
/*****************************************************************************/

#include <stdlib.h>
static int append_internal_str (char **old, char *new)
  {
  int length;

  if (!new) return NO_ERROR;
  if (!old) return ATTR_POINTER_NULL;
  if (!*old)
    {
    *old = (char *)malloc (strlen (new) + 1);
    if (!*old) return MALLOC_FAILURE;
    strcpy (*old, new);
    }
  else
    {
    length = strlen (*old) + strlen (new) + 1;
    *old = (char *)realloc (*old, length);
    if (!*old) return MALLOC_FAILURE;
    strcat (*old, new);
    }
  return NO_ERROR;
  }

int dr_append_comment (DR *dr, char *str)
  {
  return append_internal_str (&(dr->comment), str);
  }

int dr_append_history (DR *dr, char *str)
  {
  return append_internal_str (&dr->history, str);
  }

char *dr_datatypename (DR *dr)
  {
  static char *name[] = {"Void", "SignedByte", "UnsignedByte", "Short",
      "UnsignedShort", "Integer", "UnsignedInteger", "Long", "UnsignedLong",
      "Float", "Double", "Complex", "String", "Time",
      "Logical", "illegal"};
  int datatype = dr_datatype (dr);
  int known = sizeof (name) / sizeof (char *);
  if (datatype < 0 || datatype >= known) datatype = known - 1;
  return (name[datatype]);
  }

/*****************************************************************************/
// from libsds.d/sds_malloc.c
/*****************************************************************************/

#include <stdlib.h>

extern int dr_set_data (DR *dr, void *data);

int *dr_malloc_length (int rank)
  {
  int *length;
  if (rank == 0) return NULL;
  length =  (int *)malloc (rank * sizeof(int));
  if (!length) return NULL;
  while (rank--) length[rank] = 1;
  return length;
  }

char *dr_malloc_string (int numchars)
  {
  char *str;
  if (str = (char *)malloc ((1 + numchars) * sizeof(char)))
    {
    str[numchars] = '\0';
    }
  return (str);
  }

ATTRIBUTES *dr_malloc_attribute (void)
  {
		     /*  malloc's and initializes a new attribute structure  */
  ATTRIBUTES *new;

  if (!(new = (ATTRIBUTES *) malloc (sizeof (ATTRIBUTES)))) 
    return NULL;
  new->next = NULL;
  new->name = NULL; 
  new->value = NULL; 
  new->format = NULL; 
  new->comment = NULL; 
  new->datatype = DR_VOID; 

  return new;
  }

void *dr_malloc_fillvalue (int datatype)
  {
  int size;
  void *fillvalue;

  if (!(size = dr_sizeof (datatype))) return NULL;
  if (!(fillvalue = malloc (size))) return NULL;
  return fillvalue;
  }

/*
 *  create a malloced pointer to a default fillvalue declared in dr_missing.h
 */
void *dr_std_fillvalue (int datatype)
  {
  void *fillvalue;

  if (fillvalue = dr_malloc_fillvalue (datatype))
    switch (datatype)
       {
       case DR_BYTE:
         *(char *)fillvalue = B_MISSING;
         break;
       case DR_SHORT:
         *(short *)fillvalue = S_MISSING;
         break;
       case DR_INT:
       case DR_LONG:
         *(int *)fillvalue = I_MISSING;
         break;
       case DR_FLOAT:
         *(float *)fillvalue = F_MISSING;
         break;
       case DR_DOUBLE:
         *(double *)fillvalue = D_MISSING;
         break;
       case DR_TIME:
         *(double *)fillvalue = T_MISSING;
         break;
       case DR_COMPLEX:
         ((fcomplex *)fillvalue)->r = F_MISSING;
         ((fcomplex *)fillvalue)->i = F_MISSING;
         break;
       case DR_STRING:
         *(char**)fillvalue = NULL;
         break;
       case DR_VOID:
       default:
         free (fillvalue); fillvalue = NULL;
         break;
       }
  return (fillvalue);
  }

static void dr_free_attr (ATTRIBUTES *attr)
  {
  if (attr == NULL) return;
  if (attr->name)
    {
    free(attr->name);
    attr->name = NULL;
    }
  if (attr->value)
    {
    free(attr->value);
    attr->value = NULL;
    }
  if (attr->comment)
    {
    free(attr->comment);
    attr->comment = NULL;
    }
  dr_free_attr (attr->next);
  free (attr);
  }

static void dr_free_color (DR *dr)
  {
     /*  frees the color_table pointer of a dr and leaves this pointer NULL  */
  int *tmp;
  if (!dr) return;
  if (!dr->color_table) return;
  if (tmp = dr->color_table->Red) free (tmp);
  if (tmp = dr->color_table->Blue) free (tmp);
  if (tmp = dr->color_table->Green) free (tmp);
  dr->color_table = NULL;
  }

char *dr_malloc_data (long numbytes)
  {
  char *str = NULL;
  if (numbytes <= 0) return str; 
  else if (!(str = (char *)malloc ((numbytes)*sizeof(char)))) ;
/*  MALLOC_FAILURE;  */
  return (str);
  }

/*
 *  malloc a number of bytes consistent with the dimensions and datatype
 *    of the dr and set the data pointer of the dr to it.
 *    any previous data in the dr is freed.
 */
int dr_create_data (DR *dr)
  {
  void *dataptr;
  long elements;
  int bytes_per_element;

  if ((elements = dr_data_length (dr)) < 0) return DR_POINTER_NULL;
  bytes_per_element = dr_sizeof (dr->datatype);
  /* if (soi_errno) return soi_errno; */
  dataptr = dr_malloc_data (elements * bytes_per_element); 
  return dr_set_data (dr, dataptr);
  }

DR *dr_create () /* create and initialize a new dr structure */
  {
  DR *new = (DR *)malloc (sizeof (DR));

  if (new == NULL) return (new);

  new->rank = 0;
  new->length = NULL;
  new->color_table = NULL;
  new->fillval = NULL;
  new->data = NULL;
  new->data_avail = 0;
  new->datatype = DR_VOID;
  new->datumsize = 0;
  new->attrib = NULL;
  new->bscale = 1.0;
  new->bzero = 0.0;
  new->scaling = 0;
  new->comment = new->history = NULL;

  return (new);
  }

void dr_free_data (DR *dr) /*  frees the data pointer of a dr and leaves the data pointer NULL  */
  {
  if (!dr) return;
  if (dr->data) free (dr->data); 
  dr->data = NULL;
  }

static void dr_free_ptrs (DR *dr)
  {
  if (!dr) return;
  dr_free_data (dr);
  if (dr->fillval)
    {
    free (dr->fillval);
    dr->fillval = NULL;
    }
  if (dr->length)
    {
    free (dr->length);
    dr->length = NULL;
    }
  dr_free_attr (dr->attrib);
  dr->attrib = NULL;
  if (dr->comment)
    {
    free (dr->comment);
    dr->comment = NULL;
    }
  if (dr->history)
    {
    free (dr->history);
    dr->history = NULL;
    }
  dr_free_color (dr);
  }

void dr_free (DR **drptr) /*  frees all parts of a dr and leaves its pointer NULL  */
  {
  DR *dr; 
  if (!drptr || !(dr = *drptr)) return;
  dr_free_ptrs (dr);
  free (dr);
  *drptr = NULL;
  }

/*****************************************************************************/
// from libsds.d/sds_helper.c
/*****************************************************************************/

#include <ctype.h>

char *ConvertToUpper (char *line) {
  char *result;
  int i;

  if (line == NULL) return NULL;
  result = (char *) malloc(strlen(line)+1);
  for (i=0; line[i] != '\0'; i++) result[i] = toupper(line[i]);
  result[i] = '\0';
  return result;
}

char *ConvertToLower (char *line) {
  char *result;
  int i;

  if (line == NULL) return NULL;
  result = (char *) malloc(strlen(line)+1);
  for (i=0; line[i] != '\0'; i++) result[i] = tolower(line[i]);
  result[i] = '\0';
  return result;
}

static char *FirstNonWhite (char *line)
  {
  if (line == NULL) return NULL;
  while (*line)
    {
    if (isspace (*line)) line++;
    else return line;
    }
  return NULL;
  }

static char *LastNonWhite (char *line)
  {
  int n;
  if (line == NULL) return NULL;

  n = strlen (line);
  while(n)
    {
    if (isspace (*(line + n - 1))) n--;
    else return (line + n - 1);
    }
  return NULL;
  }

/*  Checks if the string num represents a valid number format.
 *  Returns 0    not a number;
 *          1    a floating point;
 *          2    a long integer.
 */
static int numType (char *num)
  {
  double dval;
  long ival;
  char c, *ptr, hold;
  
  ptr = LastNonWhite (num);
  if (ptr == NULL) return 0;  /*  Nothing there  */
  ptr++;
  hold = *ptr;
  *ptr = 0;
  if (sscanf(num, "%ld%c", &ival, &c) == 1)
    {
    *ptr = hold;
    return 2;
    }
#ifdef __linux__
  if (num[0] == '0' && (num[1] == 'x' || num[1] == 'X'))
    {
    *ptr = hold;
    return 0;
    }
#endif
  if (sscanf (num, "%lf%c", &dval, &c) == 1)
    {
    *ptr = hold;
    return 1;
    }
  *ptr = hold;
  return 0;
  }

static int header_value_type (char *str)
  {
  int datatype;

  switch (numType (str))
    {
    case 1:
      datatype = DR_DOUBLE;
      break;
    case 2:
      datatype = DR_LONG;
      break;
    default:               /*  string  */
      if ((strlen(str)==1)&& ((str[0] == 'T') || (str[0] == 'F')))
	datatype = DR_LOGICAL;
      else if (str[0] == '/')
	datatype = DR_VOID;
      else
	datatype = DR_STRING;
    }
  return (datatype);
  }

/*****************************************************************************/
// from libsds.d/sds_set.c
/*****************************************************************************/

int dr_set_attrvalue (ATTRIBUTES *attr, void *value, int datatype)
  {
  void *newvalue;
  int i, nbytes;

  if (!attr) return ATTR_POINTER_NULL;
  if (!value && (datatype != DR_VOID)) return ATTRVALUE_NULL; 
  
  if (datatype == DR_STRING) nbytes = strlen ((char *)value)+1; 
  else nbytes = dr_sizeof (datatype);

  attr->datatype = datatype;
  if (attr->value) free (attr->value);
  attr->value = NULL;

  if (nbytes)
    {
    if (!(newvalue = calloc (1, nbytes)))
       return MALLOC_FAILURE; 

    for (i=0; i<nbytes; i++) 
      *((char*)newvalue + i) = *((char*)value + i); 

    if (datatype == DR_LOGICAL) 
       if (*(char *)newvalue == '\000') *(char*)newvalue = 'F';
       else if (*(char *)newvalue == '\001') *(char*)newvalue = 'T';

    attr->value = newvalue;  
    }

  return NO_ERROR;
  }

int dr_set_attrname (ATTRIBUTES *attr, char *name)
  {
  if (attr == NULL) return ATTR_POINTER_NULL;
  if (name == NULL) return ATTRNAME_NULL;
  if(attr->name) free(attr->name);
  attr->name = NULL;
  if (!(attr->name = dr_malloc_string (strlen (name) + 1)))
    return MALLOC_FAILURE;
  strcpy (attr->name, name);
  return NO_ERROR;
  }

int dr_set_comment (ATTRIBUTES *attr, char *comment)
  {
  if (attr == NULL) return ATTR_POINTER_NULL;
  if (attr->comment) free (attr->comment);
  attr->comment = NULL;
  if (comment && (strlen (comment) > 0)) 
    if (!(attr->comment = dr_malloc_string (strlen (comment))))
      {
      return MALLOC_FAILURE;
      }
   else
      {
      strcpy (attr->comment, comment);
      }
  return NO_ERROR;
  }

/*  
 *  Adds an attribute to the attribute list of DR.
 *  Overwrites if it already exists.
 *  Takes a copy of the value, so clients do *NOT* have to alloc 
 *  memory before that.
 *  Returns the soi_errno.  Does not replace COMMENT or HISTORY keyword.
 *  Special treament for COMMENT and HISTORY keyword according to FITS standard 
 */

int dr_set_attribute (DR *dr, char *key, void *value, int attrtype, char *comment)
  {
  ATTRIBUTES *newattr = NULL;
  char *name;                                /*  to store a copy of the key  */
  int status;

  if (dr==NULL)
    return (DR_POINTER_NULL);
  if (key==NULL)
    return (ATTRNAME_NULL);
  if ( (value==NULL) && (attrtype != DR_VOID) )
    return (MAKES_NO_SENSE);

  name = ConvertToUpper (key);    /*  malloc'd copy converted to upper case  */
                          /*  use old one if it exists and is not a comment  */
  if ((strcmp (name, "COMMENT") != 0) && (strcmp (name, "HISTORY")))
    newattr = dr_search_attr (dr, name);
  
  if (newattr == NULL)
    {			      /*  add a new attribute to the attribute list  */
    newattr = dr_malloc_attribute ();
    if (status=dr_set_attrname (newattr, name))
      {
      free (name); free (newattr); return (status);
      }
    dr_AddList ((_llist **) &dr->attrib, (_llist *) newattr);
    }
  free (name);
          /*  set the members.  Old members, if exist, will be overwritten.  */
  if (status=dr_set_attrvalue (newattr, value, attrtype))
    return status;
  if (status=dr_set_comment (newattr, comment))
    return status;
  return NO_ERROR;
  }


/*
 *  sets the data pointer in the dr to a previously malloc'd pointer.
 *    any previous data in the dr is freed.
 *    no check is (or can be) made on the datatype or length
 */
int dr_set_data (DR *dr, void *data)
  {
  if (dr == NULL) return DR_POINTER_NULL;
  dr_free_data (dr);
  dr->data = data;
  return NO_ERROR;
  }

/*
 *  copies the given fillvalue into the dr or, if fillvalue is NULL,
 *  copies a default fillvalue for the given datatype into the dr.
 *  mallocs space if necessary.  previously filled data are changed,
 *  but valid data matching fill value will of course be lost
 */
int dr_set_fillvalue (DR *dr, void *fillvalue)
  {
  int datatype, ntot;
  void *fillptr;
  TIME *td, tb, tm;
  long *ld, lb, lm;
  int *id, ib, im;
  short *sd, sb, sm;
  signed char *cd, cb, cm;

  if ((datatype = dr_datatype (dr)) < 0) return DR_INVALID_DATATYPE;

  if (!fillvalue)
    {
    if (!(fillptr = dr_std_fillvalue (datatype))) return MALLOC_FAILURE;
    }
  else
    {
    if (!(fillptr = dr_malloc_fillvalue (datatype)))
      return MALLOC_FAILURE;

    switch (datatype)
      {
      case DR_BYTE:
        *(signed char *)fillptr = *(signed char*)fillvalue;
        break;
      case DR_SHORT:
        *(short *)fillptr = *(short*)fillvalue;
        break;
      case DR_INT:
        *(int *)fillptr = *(int*)fillvalue;
        break;
      case DR_LONG:
        *(long *)fillptr = *(long*)fillvalue;
        break;
      case DR_FLOAT:
        *(float *)fillptr = *(float*)fillvalue;
        break;
      case DR_DOUBLE:
      case DR_TIME:
        *(double *)fillptr = *(double*)fillvalue;
        break;
      case DR_COMPLEX:
      case DR_STRING:
      case DR_LOGICAL:
      default:
        return DR_INVALID_DATATYPE;
      }
    }
/*
 *  If there is already a fill value, replace missing data with new fill
 *    value before replacing fill value
 */
  if (dr->fillval)
    {
    if (dr->data)
      {
      ntot = dr_data_length (dr);
      switch (datatype)
        {
	case DR_BYTE:
	  cd = (signed char *)dr_data (dr);
	  cm = *(signed char *)dr->fillval;
	  cb = *(signed char *)fillptr;
	  while (ntot--)
            {
	    if (*cd == cm) *cd = cb;
	    cd++;
	    }
	  break;
	case DR_SHORT:
	  sd = (short *)dr_data (dr);
	  sm = *(short *)dr->fillval;
	  sb = *(short *)fillptr;
	  while (ntot--)
            {
	    if (*sd == sm) *sd = sb;
	    sd++;
	    }
	  break;
	case DR_INT:
	  id = (int *)dr_data (dr);
	  im = *(int *)dr->fillval;
	  ib = *(int *)fillptr;
	  while (ntot--)
            {
	    if (*id == im) *id = ib;
	    id++;
	    }
	  break;
	case DR_LONG:
	  ld = (long *)dr_data (dr);
	  lm = *(long *)dr->fillval;
	  lb = *(long *)fillptr;
	  while (ntot--)
            {
	    if (*ld == lm)
               *ld = lb;
	    ld++;
	    }
	  break;
	case DR_TIME:
	  td = (TIME *)dr_data (dr);
	  tm = *(TIME *)dr->fillval;
	  tb = *(TIME *)fillptr;
	  while (ntot--)
            {
	    if (*td == tm)
              *td = tb;
	    td++;
	    }
	  break;
        }
      }
    free (dr->fillval);
    }
  dr->fillval = fillptr;
  return (NO_ERROR);
  }

int dr_set_numbytes (DR *dr, int numbytes) /*  no consistency checks - not recommended  */
  {
  if (!dr) return DR_POINTER_NULL;
  dr->datumsize = numbytes;
  return NO_ERROR;
  }

/*
 *  sets the datatype and numbytes in the dr if it has no data  
 *    and the datatype is valid; if the data_type is void or invalid,
 *    data size is set to 0
 */
int dr_set_datatype (DR *dr, int datatype)
  {
  if (!dr) return DR_POINTER_NULL;
  if (dr->data) return DR_DATA_POINTER_EXISTS;
  dr->datatype = datatype;
  dr_set_numbytes (dr, dr_sizeof (datatype));
  return NO_ERROR;
  }

/*
 *  sets the rank in a DR structure without data.
 *  mallocs a length structure if necessary.
 *  any previous length information is replaced
 */
int dr_set_rank (DR *dr, int rank)
  {
  int i;

  if (!dr) return DR_POINTER_NULL;
  if (dr->data) return DR_DATA_POINTER_EXISTS;
  if ((rank < 0) || (rank > DR_MAXRANK)) return DR_RANK_ERROR;
  
  dr->rank = rank;
  if (dr->length) free (dr->length);
  if (!rank) return NO_ERROR;

  if (!(dr->length = dr_malloc_length (rank))) return MALLOC_FAILURE;

  for (i=0; i<rank; i++)
    dr->length[i] = 0;
  return NO_ERROR;
  }

static void dr_free_one_attr (ATTRIBUTES *attr)
  {
  if (attr == NULL) return;
  if (attr->name) free (attr->name);
  if (attr->value) free (attr->value);
  if (attr->format) free (attr->format);
  if (attr->comment) free (attr->comment);
  free (attr);
  }

/*
   If the next attribute after this attr has a name which matches the key
   it is removed from the list headed by this attr and freed.
   This function is used only as an argument to sds_MapList and as such,
   rightly assumes that it is called with a non-NULL ATTRIBUTES pointer
   it also assumes that each attribute has a name and that key is not NULL
*/
static int dr_remove_attr_mapfn (ATTRIBUTES *thisattr, char *key)
  {
  ATTRIBUTES *nextattr = thisattr->next;
  if (nextattr == NULL) return 0;
  if (strcmp (nextattr->name, key) == 0)
    {
    thisattr->next = nextattr->next;
    dr_free_one_attr (nextattr);
    return 1;  /*  to break from MapList loop  */
    }
  else return 0;
  }

int dr_remove_attribute(DR *dr, char *key)
  {
  if (dr == NULL) return DR_POINTER_NULL;
  if (key == NULL) return ATTRIBUTE_NOT_FOUND;
  if (dr->attrib == NULL) return ATTR_POINTER_NULL;
  if (dr->attrib->name == NULL) return ATTRNAME_NULL;

  if (strcmp (dr->attrib->name, key) == 0)
    {
    dr_free_one_attr ((ATTRIBUTES *)dr_RemoveNthElement
        ((_llist **)(&dr->attrib), 1));
    return NO_ERROR;
    }

  if (dr_MapList ((_llist **)&(dr->attrib),
      (_llistFn) dr_remove_attr_mapfn, (void *) key)) return NO_ERROR;
  return ATTRIBUTE_NOT_FOUND;
  }

/*****************************************************************************/
// from libsds.d/sds_convert.c
/*****************************************************************************/

#include <values.h>

int dr_set_scaling (DR *dr, int bits, double scale, double bias)
  {
  if (!dr) return DR_POINTER_NULL;
  dr->scaling = bits;
  dr->bscale = scale;
  dr->bzero = bias;
  return NO_ERROR;
  }

int dr_data_convert (DR *dr, int type_out)
  {
  double *dpo, *dpn;
  float *fo, *fn;
  signed int *io, *in, im, ib;
  signed short *so, *sn, sm, sb;
  signed char *co, *cn, cm, cb;

  void *old, *new, *newfill, *oldfill;
  
  double dpb = dr_A_Quiet_dNaN();
  float fb = dr_A_Quiet_fNaN();
  int type_in, dsize;
  size_t ndata;
  int checkmiss, countmiss = 0;
  int status = NO_ERROR;

  if (dr == NULL) return DR_POINTER_NULL;
  if (dr->data_avail == 0) return NO_ERROR;	     /*  nothing to convert  */
  if (dr->data == NULL) return DR_DATA_POINTER_NULL;
		      /*  check the input and output data types for support  */
  switch (type_in = dr_datatype (dr))
    {
    case DR_BYTE:
    case DR_SHORT:
    case DR_INT:
    case DR_FLOAT:
    case DR_DOUBLE:
      break;
    default:
      return DR_INVALID_DATATYPE;
    }
  if (type_in == type_out) return NO_ERROR;
  switch (type_out)
    {
    case DR_BYTE:
    case DR_SHORT:
    case DR_INT:
    case DR_FLOAT:
    case DR_DOUBLE:
      break;
    default:
      return DR_INVALID_DATATYPE;
    }
				/*  set up working data array and constants  */
  ndata = (size_t)dr_data_length (dr);
  dsize = dr_sizeof (type_out);
  old = dr_data (dr);
  if (!(new = (void *)malloc (ndata * dsize))) 
    return MALLOC_FAILURE;
  if (!(newfill = (void *)malloc (dsize)))
    return MALLOC_FAILURE;
  checkmiss = (dr->fillval != NULL);
  if (checkmiss) oldfill = (void *)dr->fillval;
						/*  perform data conversion  */
  switch (type_in)
    {
    case DR_BYTE:
      co = (signed char *)old;
      if (checkmiss)
        cm = *(signed char *)oldfill;
      switch (type_out)
        {
	case DR_BYTE:						/* i1 -> i1  */
	  break;
	case DR_SHORT:						/* i1 -> i2  */
	  sn = (signed short *)new;
	  if (checkmiss)
            {
	    sb = (cm == B_MISSING) ? S_MISSING : cm;
	    while (ndata--)
              {
	      *sn++ = (*co == cm) ? sb : *co;
	      co++;
	      }
	    }
          else
            {
	    while (ndata--)
	      *sn++ = *co++;
	    }
	  *(short *)newfill = sb;
	  break;
	case DR_INT:						/* i1 -> i4  */
	  in = (signed int *)new;
	  if (checkmiss)
            {
	    ib = (cm == B_MISSING) ? I_MISSING : cm;
	    while (ndata--)
              {
	      *in++ = (*co == cm) ? ib : *co;
	      co++;
	      }
	    }
          else
            {
	    while (ndata--)
	      *in++ = *co++;
	    }
	  *(int *)newfill = ib;
	  break;
	case DR_FLOAT:						/* i1 -> r4  */
	  fn = (float *)new;
	  if (checkmiss)
            {
	    while (ndata--)
              {
	      if (*co == cm) *fn++ = fb;
	      else *fn++ = *co;
	      co++;
	      }
	    }
          else
            {
	    while (ndata--)
	      *fn++ = *co++;
	    }
	  countmiss = checkmiss = 0;
	  break;
	case DR_DOUBLE:					/* i1 -> r8  */
	  dpn = (double *)new;
	  if (checkmiss)
            {
	    while (ndata--)
              {
	      if (*co == cm) *dpn++ = dpb;
	      else *dpn++ = *co;
	      co++;
	      }
	    }
          else
            {
	    while (ndata--)
	      *dpn++ = *co++;
	    }
	  countmiss = checkmiss = 0;
	  break;
	default:
	  status = DR_INVALID_DATATYPE;
	  break;
        }
      break;
    case DR_SHORT:
      so = (signed short *)old;
      if (checkmiss)
        sm = *(signed short *)oldfill;
      switch (type_out) {
	case DR_BYTE:						/* i2 -> i1  */
	  cn = (signed char *)new;
	  if (checkmiss) {
	    cb = (sm < SCHAR_MIN || sm > SCHAR_MAX) ? B_MISSING : sm;
	    while (ndata--) {
	      if (*so == sm) *cn++ = cb;
	      else if (*so < SCHAR_MIN || *so > SCHAR_MAX) {
	        *cn++ = cb;
		countmiss++;
	      } else *cn++ = *so;
	      so++;
	    }
	  } else {
	    cb = B_MISSING;
	    while (ndata--) {
	      if (*so < SCHAR_MIN || *so > SCHAR_MAX) {
	        *cn++ = cb;
		countmiss++;
	      } else *cn++ = *so;
	      so++;
	    }
	  }
	  *(signed char *)newfill = cb;
	  break;
	case DR_SHORT:						/* i2 -> i2  */
	  break;
	case DR_INT:						/* i2 -> i4  */
	  in = (signed int *)new;
	  if (checkmiss) {
	    ib = (sm == S_MISSING) ? I_MISSING : sm;
	    while (ndata--) {
	      *in++ = (*so == sm) ? ib : *so;
	      so++;
	    }
	  } else {
	    while (ndata--)
	      *in++ = *so++;
	  }
	  *(int *)newfill = ib;
	  break;
	case DR_FLOAT:						/* i2 -> r4  */
	  fn = (float *)new;
	  if (checkmiss) {
	    while (ndata--) {
	      if (*so == sm) *fn++ = fb;
	      else *fn++ = *so;
	      so++;
	    }
	  } else {
	    while (ndata--)
	      *fn++ = *so++;
	  }
	  countmiss = checkmiss = 0;
	  break;
	case DR_DOUBLE:					/* i2 -> r8  */
	  dpn = (double *)new;
	  if (checkmiss) {
	    while (ndata--) {
	      if (*so == sm) *dpn++ = dpb;
	      else *dpn++ = *so;
	      so++;
	    }
	  } else {
	    while (ndata--)
	      *dpn++ = *so++;
	  }
	  countmiss = checkmiss = 0;
	  break;
	default:
	  status = DR_INVALID_DATATYPE;
	  break;
      }
      break;
    case DR_INT:
      io = (int *)old;
      if (checkmiss)
        im = *(signed int *)oldfill;
      switch (type_out) {
	case DR_BYTE:						/* i4 -> i1  */
	  cn = (signed char *)new;
	  if (checkmiss) {
	    cb = (im < SCHAR_MIN || im > SCHAR_MAX) ? B_MISSING : im;
	    while (ndata--) {
	      if (*io == im) *cn++ = cb;
	      else if (*io < SCHAR_MIN || *io > SCHAR_MAX) {
	        *cn++ = cb;
		countmiss++;
	      } else *cn++ = *io;
	      io++;
	    }
	  } else {
	    cb = B_MISSING;
	    while (ndata--) {
	      if (*io < SCHAR_MIN || *io > SCHAR_MAX) {
	        *cn++ = cb;
		countmiss++;
	      } else *cn++ = *io;
	      io++;
	    }
	  }
	  *(signed char *)newfill = cb;
	  break;
	case DR_SHORT:						/* i4 -> i2  */
	  sn = (signed short *)new;
	  if (checkmiss) {
	    sb = (im < SHRT_MIN || im > SHRT_MAX) ? S_MISSING : im;
	    while (ndata--) {
	      if (*io == im) *sn++ = sb;
	      if (*io < SHRT_MIN || *io > SHRT_MAX) {
	        *sn++ = sb;
		countmiss++;
	      } else *sn++ = *io;
	      io++;
	    }
	  } else {
	    sb = S_MISSING;
	    while (ndata--) {
	      if (*io < SHRT_MIN || *io > SHRT_MAX) {
	        *sn++ = sb;
		countmiss++;
	      } else *sn++ = *io;
	      io++;
	    }
	  }
	  *(signed short *)newfill = sb;
	  break;
	case DR_INT:						/* i4 -> i4  */
	  break;
	case DR_FLOAT:						/* i4 -> r4  */
	  fn = (float *)new;
	  if (checkmiss) {
	    while (ndata--) {
	      if (*io == im) *fn++ = fb;
	      else *fn++ = *io;
	      io++;
	    }
	  } else {
	    while (ndata--)
	      *fn++ = *io++;
	  }
	  countmiss = checkmiss = 0;
	  break;
	case DR_DOUBLE:					/* i4 -> r8  */
	  dpn = (double *)new;
	  if (checkmiss) {
	    while (ndata--) {
	      if (*io == im) *dpn++ = dpb;
	      else *dpn++ = *io;
	      io++;
	    }
	  } else {
	    while (ndata--)
	      *dpn++ = *io++;
	  }
	  countmiss = checkmiss = 0;
	  break;
	default:
	  status = DR_INVALID_DATATYPE;
	  break;
      }
      break;
    case DR_FLOAT:
      fo = (float *)old;
      switch (type_out) {
	case DR_BYTE:						/* r4 -> i1  */
	  cn = (signed char *)new;
	  cb = B_MISSING;
	  while (ndata--) {
	    if (IsfNaN (*fo)) {
	      *cn++ = cb;
	      checkmiss = 1;
	    }
	    else if (*fo < SCHAR_MIN || *fo > SCHAR_MAX) {
	      *cn++ = cb;
	      countmiss++;
	    } else *cn++ = (*fo < 0.0) ? *fo - 0.5 : *fo + 0.5;
	    fo++;
	  }
	  *(signed char *)newfill = cb;
	  dr_set_scaling (dr, 0, 1.0, 0.0);
	  break;
	case DR_SHORT:						/* r4 -> i2  */
	  sn = (signed short *)new;
	  sb = S_MISSING;
	  while (ndata--) {
	    if (IsfNaN (*fo)) {
	      *sn++ = sb;
	      checkmiss = 1;
	    }
	    else if (*fo < SHRT_MIN || *fo > SHRT_MAX) {
	      *sn++ = sb;
	      countmiss++;
	    } else *sn++ = (*fo < 0.0) ? *fo - 0.5 : *fo + 0.5;
	    fo++;
	  }
	  *(signed short *)newfill = sb;
	  dr_set_scaling (dr, 0, 1.0, 0.0);
	  break;
	case DR_INT:						/* r4 -> i4  */
	  in = (signed int *)new;
	  ib = I_MISSING;
	  while (ndata--) {
	    if (IsfNaN (*fo)) {
	      *in++ = ib;
	      checkmiss = 1;
	    }
	    else if (*fo < INT_MIN || *fo > INT_MAX) {
	      *in++ = ib;
	      countmiss++;
	    } else *in++ = (*fo < 0.0) ? *fo - 0.5 : *fo + 0.5;
	    fo++;
	  }
	  *(signed int *)newfill = ib;
	  dr_set_scaling (dr, 0, 1.0, 0.0);
	  break;
	case DR_FLOAT:						/* r4 -> r4  */
	  break;
	case DR_DOUBLE:					/* r4 -> r8  */
	  dpn = (double *)new;
	  while (ndata--) {
	    if (IsfNaN (*fo))
	      *dpn++ = dpb;
	    else
	      *dpn++ = *fo;
	    fo++;
	  }
	  countmiss = checkmiss = 0;
	  break;
	default:
	  status = DR_INVALID_DATATYPE;
	  break;
      }
      break;
    case DR_DOUBLE:
      dpo = (double *)old;
      switch (type_out) {
	case DR_BYTE:						/* r8 -> i1  */
	  cn = (signed char *)new;
	  cb = B_MISSING;
	  while (ndata--) {
	    if (IsdNaN (*dpo)) {
	      *cn++ = cb;
	      checkmiss = 1;
	    }
	    else if (*dpo < SCHAR_MIN || *dpo > SCHAR_MAX) {
	      *cn++ = cb;
	      countmiss++;
	    } else *cn++ = (*dpo < 0.0) ? *dpo - 0.5 : *dpo + 0.5;
	    dpo++;
	  }
	  *(signed char *)newfill = cb;
	  break;
	case DR_SHORT:						/* r8 -> i2  */
	  sn = (signed short *)new;
	  sb = S_MISSING;
	  while (ndata--) {
	    if (IsdNaN (*dpo)) {
	      *sn++ = sb;
	      checkmiss = 1;
	    }
	    else if (*dpo < SHRT_MIN || *dpo > SHRT_MAX) {
	      *sn++ = sb;
	      countmiss++;
	    } else *sn++ = (*dpo < 0.0) ? *dpo - 0.5 : *dpo + 0.5;
	    dpo++;
	  }
	  *(signed short *)newfill = sb;
	  break;
	case DR_INT:						/* r8 -> i4  */
	  in = (signed int *)new;
	  ib = I_MISSING;
	  while (ndata--) {
	    if (IsdNaN (*dpo)) {
	      *in++ = ib;
	      checkmiss = 1;
	    }
	    else if (*dpo < INT_MIN || *dpo > INT_MAX) {
	      *in++ = ib;
	      countmiss++;
	    } else *in++ = (*dpo < 0.0) ? *dpo - 0.5 : *dpo + 0.5;
	    dpo++;
	  }
	  *(signed int *)newfill = ib;
	  break;
	case DR_FLOAT:						/* r8 -> r4  */
	  fn = (float *)new;
	  while (ndata--) {
	    if (IsfNaN (*dpo))
	      *fn++ = fb;
	    else if (*dpo > FLT_MAX || *dpo < -FLT_MAX) {
	      *fn++ = fb;
	      countmiss++;
	    } else
	      *fn++ = *dpo;
	    dpo++;
	  }
	  if (countmiss) status = DATA_OUT_OF_RANGE;
	  countmiss = checkmiss = 0;
	  break;
	case DR_DOUBLE:					/* r8 -> r8  */
	  break;
	default:
	  status = DR_INVALID_DATATYPE;
	  break;
      }
      break;
    default:
      status = DR_INVALID_DATATYPE;
      break;
  }
  if (status == DR_INVALID_DATATYPE) {
    free (new);
    free (newfill);
    return status;
  }
							 /*  update the dr  */
  dr_free_data (dr);
  dr_set_datatype (dr, type_out);
  dr_set_data (dr, new);
  if (countmiss || checkmiss)
    dr_set_fillvalue (dr, newfill);
  else {
    free (newfill);
    free (dr->fillval);
    dr->fillval = NULL;
  }
  if (countmiss) status = DATA_OUT_OF_RANGE;
  return status;
}

int dr_scale_data (DR *dr, double scale, double bias) {
  double *dd, datum;
  float *fd;
  unsigned int fill;
  int dtype, ndata, out = 0, setfill = 0;
  int status = NO_ERROR;

  if (scale == 1.0 && bias == 0.0) return status;
  if (dr == NULL) return (status = DR_POINTER_NULL);
  if (dr->data == NULL) return (status = DR_DATA_POINTER_NULL);
					/*  check the data type for support  */
  ndata = dr_data_length (dr);
			       /*  save the fill value for fixed-point data  */
  if (dr->fillval) {
    fill = *(unsigned int *)dr->fillval;
    setfill = 1;
  }

  switch (dtype = dr_datatype (dr)) {
    case DR_BYTE:
    case DR_SHORT:
      dr_data_convert (dr, DR_FLOAT);
      dr_scale_data (dr, scale, bias);
      status = dr_data_convert (dr, dtype);
      if (setfill) dr_set_fillvalue (dr, &fill);
      return (status);
    case DR_INT:
      dr_data_convert (dr, DR_DOUBLE);
      dr_scale_data (dr, scale, bias);
      status = dr_data_convert (dr, dtype);
      if (setfill) dr_set_fillvalue (dr, &fill);
      return (status);
    case DR_FLOAT:
      fd = (float *)dr->data;
      while (ndata--) {
	if (IsfNaN (*fd))
	  fd++;
	else {
	  datum = (*fd * scale) + bias;
	  if (datum > FLT_MAX || datum < -FLT_MAX)
	    out++;
	  *fd++ = datum;
	}
      }
      break;
    case DR_DOUBLE:
      dd = (double *)dr->data;
      while (ndata--) {
	if (IsdNaN (*dd))
	  dd++;
	else {
	  datum = scale * *dd + bias;
	  if (datum > DBL_MAX || datum < -DBL_MAX)
	    out++;
	  *dd++ = datum;
	}
      }
      break;
    default:
      return (status = DR_INVALID_DATATYPE);
  }
				       /*  adjust output scaling parameters  */
  if (dr->scaling) {
    dr->bscale /= scale;
    dr->bzero -= bias * dr->bscale;
  }
  return (out) ? DATA_OUT_OF_RANGE : 0;
}

/*****************************************************************************/
// from libsds.d/sds_flip.c
/*****************************************************************************/

static int dr_flip (void *data, long length, int size) {
  long i,j;
  char *c, t;
  unsigned short *s;
  unsigned int *l;
  
  if (data == NULL) return DR_DATA_POINTER_NULL;

  if (size == 2) {
    s = (unsigned short *)data;
    for (i=0; i<length; i++, s++)
      *s = (*s<<8) | (*s>>8);
  } else if (size == 4) {
    l = (unsigned int *)data;
    for (i=0; i<length; i++, l++)
      *l = (*l<<24) | ((*l&0xff00)<<8) | ((*l&0xff0000)>>8) | (*l>>24);
  } else {
    c = (char *)data;
    for (i=0; i<length; i++, c+=size)
      for (j=0; j<(size/2); j++) {
	t=*(c+j);
	*(c+j)=*(c+size-j-1);
	*(c+size-j-1)=t;
      }
  }
  return 0;
}

int dr_flip_data (DR *dr) {
  if (dr == NULL) return DR_POINTER_NULL;
  return dr_flip (dr->data, dr_data_length (dr), dr->datumsize);
}

/*****************************************************************************/
// from libsds.d/sds_fits.c
/*****************************************************************************/

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>

#define FITS_KWSIZE	 8
#define FITS_VALSIZE	70
#define FITS_CARDSIZE	80
#define FITS_NCARDS	36


static unsigned short ReadDataType (char *line) {
  char str[FITS_VALSIZE + 1];

  return (sscanf (line, "%s", str) == 1) ? header_value_type (str) : DR_VOID;
}

static char *ReadComment (char *line, int datatype) {
  char *end, *str, *pos;
  int len;
  if (datatype == DR_STRING) {
    if (end = strrchr (line, '\''))
      line = end;  				     /*  find last "'" char  */
  } else if  (end = strchr (line, ' '))
      line = end;
    /*  Search for first occurrence of delimiting "/" (not really required)  */
  if (pos = strchr (line, '/')) {
    if (pos = FirstNonWhite (pos + 1)) {	  /*  comment really exists  */
      end = LastNonWhite (pos);
      len = end - pos + 2;
      str = dr_malloc_string (len + 1);
      strncpy (str, pos, len);
      str[len] = 0;
      return str;
    }
  }
  return NULL;
}

static ATTRIBUTES *GetHeaderRecord (DR *dr, FILE *fp) {
  ATTRIBUTES *attr;
  char *c1, *c2, *line;
  int i, pos=0;
  char card[FITS_CARDSIZE+1];

  card[FITS_CARDSIZE] = 0;       /*  delimit string  */

  if (!fread (card, FITS_CARDSIZE, 1, fp)) {
    return NULL;
  }
				  /*  pad all the null characters to spaces  */
  for (i=0; i<FITS_CARDSIZE; i++)
    if (!card[i]) card[i] = ' ';

  attr = dr_malloc_attribute ();			  /*  new attribute  */
  attr->name = dr_malloc_string (FITS_KWSIZE);
  sscanf (card, "%8s", attr->name);

  if (!strcmp (attr->name, "COMMENT")) {
    line = card + FITS_KWSIZE;
      /*  Might want to strip out trailing white space for other interfaces  */
    dr_append_comment (dr, line);
    dr_append_comment (dr, "\n");
    return attr;
  }

  if (!strcmp (attr->name, "HISTORY")) {
    line = card + FITS_KWSIZE;
    dr_append_history (dr, line);
    dr_append_history (dr, "\n");
    return attr;
  }

  line = card + FITS_KWSIZE + 2;  /*  line points to beginning of val field  */
  attr->datatype = ReadDataType (line);		     /*  determine datatype  */

  switch (attr->datatype) {
    case DR_VOID:
      attr->value = NULL;
      break;
    case DR_LOGICAL:
      attr->value = (char*)malloc (sizeof (char) + 1);
      sscanf (line, "%1s", (char *)attr->value);
      break;
    case DR_STRING:
      if ((c1 = strchr (line, '\'')) && (c2 = strrchr (c1 + 1, '\'')))
	c1++;
      else {			  /*  string but no quotes, take first word  */
						    /*  skip leading blanks  */
	for (c1 = line; *c1 == ' '; c1++)
	  ;
					      /*  get trailing blank or EOS  */
	for (c2=c1; *c2 && *c2 != ' '; c2++)
	  ;
      }
      *c2 = '\0';
      pos = c2 + 1 - line;
      while (c2 > c1 && *(c2-1) == ' ')
	*--c2 = '\0';
      attr->value = dr_malloc_string (c2 - c1);
      strcpy (attr->value, c1);
      break;
    case DR_LONG:
      attr->value = (long *)malloc (sizeof (long));
      sscanf (line, "%ld", (long *)attr->value);
      break;
    case DR_DOUBLE:
      attr->value = (double *)malloc (sizeof (double));
      sscanf (line, "%lf", (double *)attr->value);
      break;
    default:
      attr->value = NULL;
/*  signal unkown type; this should not happen
     status = ATTRVALUE_TYPE_UNKNOWN;
*/
  }

  attr->comment = ReadComment (line + pos, attr->datatype);
  return attr;
}

static void SetDiminfo (DR *dr) {
  char naxisn[FITS_KWSIZE + 1];
  int i, naxis, *naxisArr;
  void *ptr;
  
  ptr = dr_search_attrvalue (dr, "NAXIS");
  if (ptr == NULL) return;
  naxis = *(long *)ptr;
  dr_remove_attribute (dr, "NAXIS");
  dr->rank = naxis;
  if (naxis == 0) return;  /*  no data  */
  naxisArr = dr_malloc_length (naxis);
  
  for (i = 0; i < naxis; i++) {
    sprintf (naxisn, "NAXIS%d", i+1);
    ptr = dr_search_attrvalue (dr, naxisn);
    if (ptr == NULL) return;
    naxisArr[i] = *(int*)ptr;
    dr_remove_attribute (dr, naxisn);
  }
  if (dr->length) free (dr->length);
  dr->length = naxisArr;
}

static void ProcessAttrList (DR *dr, int conforming) {
  void *vval;
  long int lmiss;
  int imiss;
  short smiss;
  unsigned char bmiss;

  if (!conforming) {
    if (!(vval = dr_search_attrvalue (dr, "BITPIX"))) {
/*
      errstk ("*** dr_read_fits ERROR: illegal FITS file: no BITPIX\n");
      status = FITS_ERROR;
*/
      return;
    }
    switch (*(long *)vval) {
      case (8):
	dr_set_datatype (dr, DR_BYTE);
	break;
      case (16):
	dr_set_datatype (dr, DR_SHORT);
	break;
      case (32):
	dr_set_datatype (dr, DR_INT);
	break;
      case (64):
	if (sizeof (long) > sizeof (int)) {
	  dr_set_datatype (dr, DR_LONG);
	  break;
	}
/*
	errstk ("*** ERROR dr_read_fits: illegal BITPIX value (%d)\n",
	    *(int *)vval);
        status = FITS_ERROR;
*/
	return;
      case (-32):
	dr_set_datatype (dr, DR_FLOAT);
	break;
      case (-64):
	dr_set_datatype (dr, DR_DOUBLE);
	break;
      default:
/*
	errstk ("*** ERROR dr_read_fits: illegal BITPIX value\n");
        status = FITS_ERROR;
*/
	return;
    }
    dr_remove_attribute (dr, "BITPIX");
    SetDiminfo (dr);
  }

  dr_set_scaling (dr, 0, 1.0, 0.0);
  if (vval = dr_search_attrvalue (dr, "BSCALE")) {
    switch (dr_datatype (dr)) {
      case (DR_BYTE):
        dr_set_scaling (dr, 8, *(double *)vval, dr_bzero (dr));
	break;
      case (DR_SHORT):
        dr_set_scaling (dr, 16, *(double *)vval, dr_bzero (dr));
	break;
      case (DR_INT):
        dr_set_scaling (dr, 32, *(double *)vval, dr_bzero (dr));
	break;
      case (DR_LONG):
        dr_set_scaling (dr, 64, *(double *)vval, dr_bzero (dr));
	break;
    }
    dr_remove_attribute (dr, "BSCALE");
  }
  if (vval = dr_search_attrvalue (dr, "BZERO")) {
    switch (dr_datatype (dr)) {
      case (DR_BYTE):
        dr_set_scaling (dr, 8, dr_bscale (dr), *(double *)vval);
	break;
      case (DR_SHORT):
        dr_set_scaling (dr, 16, dr_bscale (dr), *(double *)vval);
	break;
      case (DR_INT):
        dr_set_scaling (dr, 32, dr_bscale (dr), *(double *)vval);
	break;
      case (DR_LONG):
        dr_set_scaling (dr, 64, dr_bscale (dr), *(double *)vval);
	break;
    }
    dr_remove_attribute (dr, "BZERO");
  }

  if (vval = dr_search_attrvalue (dr, "BLANK")) {
/*  FITS standard requires that this keyword be ignored by readers when data
							 are floating point  */
    lmiss = *(long *)vval;
    switch (dr->datatype) {
      case (DR_INT):
	if (lmiss < INT_MIN || lmiss > INT_MAX) {
	  ;
/*
	  errstk ("*** WARNING dr_read_fits: illegal BLANK value (%d) ignored\n",
	      lmiss);
*/
	} else {
          imiss = lmiss;
          dr_set_fillvalue (dr, &imiss);
	}
	break;
      case (DR_SHORT):
	if (lmiss < SHRT_MIN || lmiss > SHRT_MAX) {
	  ;
/*
	  errstk ("*** WARNING dr_read_fits: illegal BLANK value (%d) ignored\n",
	      lmiss);
*/
	} else {
          smiss = lmiss;
          dr_set_fillvalue (dr, &smiss);
	}
	break;
      case (DR_BYTE):
        bmiss = *(long *)vval;
	if (lmiss < 0 || lmiss > UCHAR_MAX) {
	  ;
/*
	  errstk ("*** WARNING dr_read_fits: illegal BLANK value (%d) ignored\n",
	      lmiss);
*/
	} else {
          bmiss = lmiss;
          dr_set_fillvalue (dr, &bmiss);
	}
	break;
      case (DR_LONG):
        dr_set_fillvalue (dr, &lmiss);
	break;
    }
    dr_remove_attribute (dr, "BLANK");
  }
}

int read_fits_head (DR *dr, FILE *fp) {
  ATTRIBUTES *attr;
  int ival, remain, recs = 0;
  int file_conforms = 1;
  int status = NO_ERROR;
  char naxisn[FITS_KWSIZE + 1];

  if (!dr) return DR_POINTER_NULL;
		 /*  if there are attributes already there - blow them away  */
  dr_free_attr (dr->attrib);
  dr->attrib = NULL;
						/*  rewind to start of file  */
  rewind (fp);
						  /*  Check for "SIMPLE= T"  */
  if (!(attr = GetHeaderRecord (dr, fp))) return (FITS_ERROR);
  if (strcmp (dr_attrname (attr), "SIMPLE")) {
				  /*  dr_read_fits ERROR: illegal FITS file  */
    dr_free_attr (attr);
    return FITS_ERROR;
  }
  if (!dr_attrvalue_str (attr) || strcmp (dr_attrvalue_str (attr), "T")) {
/*
    errstk ("**  dr_read_fits WARNING: non-conforming FITS file\n");
*/
    file_conforms = 0;
  }
  dr_free_attr (attr);
  recs++;
  if (file_conforms) {
						  /*  Check for "BITPIX= n"  */
    if (!(attr = GetHeaderRecord (dr, fp))) return (FITS_ERROR);
    if (strcmp (dr_attrname (attr), "BITPIX")) {
/*
      errstk ("*** dr_read_fits ERROR: illegal FITS file - no BITPIX\n");
*/
      dr_free_attr (attr);
      return FITS_ERROR;
    } else if (!dr_attrvalue (attr)) {
/*
      errstk
          ("*** dr_read_fits ERROR: FITS file missing required key value\n");
*/
      dr_free_attr (attr);
      return FITS_ERROR;
    }
    dr_free_data (dr);   /*  data ptr must be NULL in order to set datatype  */

    ival = *(long *)dr_attrvalue (attr);
    switch (ival) {
      case (8):
	dr_set_datatype (dr, DR_BYTE);
	break;
      case (16):
	dr_set_datatype (dr, DR_SHORT);
	break;
      case (32):
	dr_set_datatype (dr, DR_INT);
	break;
      case (64):
	if (sizeof (long) > sizeof (int)) {
	  dr_set_datatype (dr, DR_LONG);
/*
	  errstk
	    ("**  dr_read_fits WARNING: non-conforming BITPIX value (64)\n");
*/
	  break;
	}
/*
	errstk ("*** dr_read_fits ERROR: illegal BITPIX value (64)\n");
*/
        return FITS_ERROR;
      case (-32):
	dr_set_datatype (dr, DR_FLOAT);
	break;
      case (-64):
	dr_set_datatype (dr, DR_DOUBLE);
	break;
      default:
/*
	errstk ("*** dr_read_fits ERROR: illegal BITPIX value (%d)\n", ival);
*/
        dr_free_attr (attr);
        return FITS_ERROR;
    }
    dr_free_attr (attr);
    recs++;
						  /*   Check for "NAXIS= n"  */
    if (!(attr = GetHeaderRecord (dr, fp))) return FITS_ERROR;
    if (strcmp (dr_attrname (attr), "NAXIS")) {
/*
      errstk ("*** dr_read_fits ERROR: illegal FITS file - no NAXIS\n");
*/
      dr_free_attr (attr);
      return FITS_ERROR;
    } else if (!dr_attrvalue (attr)) {
/*
      errstk
          ("*** dr_read_fits ERROR: FITS file missing required key value\n");
*/
      dr_free_attr (attr);
      return FITS_ERROR;
    }
    ival = *(long *)dr_attrvalue (attr);
    if (ival < 0 || ival > 999) {
/*
      errstk ("*** dr_read_fits ERROR: illegal NAXIS value (%d)\n", ival);
*/
      return FITS_ERROR;
    }
    dr_set_rank (dr, ival);
    if (ival) {
      int *naxisArr = dr_malloc_length (ival);
      int n = 0;

      while (n < ival) {
        sprintf (naxisn, "NAXIS%d", n + 1);
	if (!(attr = GetHeaderRecord (dr, fp))) {
/*
	  errstk ("*** dr_read_fits ERROR: insufficient NAXISn keywords\n");
*/
	  return FITS_ERROR;
	} else if (strcmp (dr_attrname (attr), naxisn)) {
/*
	  errstk ("*** dr_read_fits ERROR: missing required keyword %s\n",
	      naxisn);
*/
	  return FITS_ERROR;
	} else if (!dr_attrvalue (attr)) {
/*
	  errstk
	   ("*** dr_read_fits ERROR: FITS file missing required key value\n");
*/
	  return FITS_ERROR;
	}
	naxisArr[n++] = *(long *)dr_attrvalue (attr);
	recs++;
      }
      dr->length = naxisArr;
    }
    recs++;
  }
  while (!status) {
    attr = GetHeaderRecord (dr, fp);
    if (!attr) {
      status = FITS_ERROR;			/*  error -- No END keyword  */
/*
      errstk ("*** dr_read_fits ERROR: no END keyword\n");
*/
      break;
    }
    recs++;
    if (strcmp (attr->name, "END") != 0) {
      if (!strcmp (attr->name, "COMMENT") ||
          !strcmp (attr->name, "HISTORY"))
	dr_free_attr (attr);
      else
						       /*  note the & sign!  */
        dr_AddList ((_llist **) &(dr->attrib), (_llist *) attr);
    } else {					      /*  "END" encountered  */
      dr_free_attr (attr);
      ProcessAttrList (dr, file_conforms);
						       /*  set file pointer  */
      remain = (FITS_NCARDS - recs) % FITS_NCARDS;
      if (remain < 0) remain += FITS_NCARDS;
      if (remain % FITS_NCARDS)
        if (fseek (fp, remain * FITS_CARDSIZE, SEEK_CUR))
	  status = FITS_ERROR;
      return status;
    }
  }
  return status;
}

static int read_fits_data (DR *dr, FILE *fp) {
  int status = NO_ERROR;
  long nbytes;
  if (status = dr_create_data(dr)) return status;
  if (dr->data == NULL) return NO_ERROR;			  /*  no op  */
  nbytes = dr_data_length (dr) * dr_numbytes (dr);

  if (fread (dr->data, nbytes, 1, fp) == 0) {
    status = READ_FAILURE;
    dr_free_data (dr);
  }
  else
    dr->data_avail = 1;

#ifdef IEEE_EL
  dr_flip_data (dr);
#endif
  return status;
}

DR *dr_read_fits_header (FILE *in, int *status)
{
DR *dr;

if (!in)
    {
    *status = FILE_POINTER_NULL;
    return NULL;
    }
if (!(dr = dr_create ()))
    {
    *status = DR_CREATE_FAILURE;
    return (dr);
    }
if (read_fits_head (dr, in))
    {
    dr_free (&dr);
    return (dr);
    }
*status = NO_ERROR;
return (dr);
}

DR *dr_read_fits (FILE *in, int *status) {
  DR *dr;

  if (!in) {
    *status = FILE_POINTER_NULL;
    return NULL;
  }
  if (!(dr = dr_create ())) {
    *status = DR_CREATE_FAILURE;
    return (dr);
  }
  if (read_fits_head (dr, in)) {
    dr_free (&dr);
    return (dr);
  }
  if (read_fits_data (dr, in)) {
				     /*  set errors, and return partial dr  */
    ;
  }
  if (dr->scaling) {
	  /*  do an internal type conversion, but restore advisory scalings  */
    double scale = dr_bscale (dr);
    double bias = dr_bzero (dr);
    int dtype = (dr->scaling > 16) ? DR_DOUBLE : DR_FLOAT;
    dr_data_convert (dr, dtype); 
    dr_scale_data (dr, scale, bias);
    dr_set_scaling (dr, dr->scaling, scale, bias);
  }
  *status = NO_ERROR;
  return (dr);
}

static char *AttrValue (ATTRIBUTES *attr)
  {
  static char str[FITS_CARDSIZE + 1];
  int len;

  str[0] = '\0';
  if (!attr) return (str);
  if (attr->datatype == DR_VOID) return (str);
  switch (attr->datatype)
    {
    case (DR_LOGICAL):
      sprintf (str, "= %*.*s", 20, 1, dr_attrvalue_str (attr));
      break;
    case DR_BYTE:
    case DR_SHORT:
    case DR_INT:
    case DR_LONG:
    case DR_FLOAT:
    case DR_DOUBLE:
      sprintf (str, "= %s", dr_attrvalue_str (attr));
      break;
    case DR_STRING:
    case DR_TIME:
      len = strlen (dr_attrvalue_str (attr));
      if (len > FITS_VALSIZE - 2) len = FITS_VALSIZE - 2;
//      sprintf (str, "= '%-8.*s'", len, dr_attrvalue_str (attr));
      sprintf (str, "= '%.*s'", len, dr_attrvalue_str (attr));
      break;
    }
  return (str);
  }

static int PutHeaderRecord (ATTRIBUTES *attr, FILE *fp) {
  int status = 0;
  int rem = FITS_CARDSIZE;
  char str[FITS_CARDSIZE + 1];
  static char blank[] =
{"                                                                                "};

  if (!attr) return ATTR_POINTER_NULL;
  if (!attr->name) return ATTRNAME_NULL;

  sprintf (str, "%-*.*s", FITS_KWSIZE, FITS_KWSIZE, attr->name);
  rem -= FITS_KWSIZE;
  strncat (str, AttrValue (attr), rem);
  rem = FITS_CARDSIZE - strlen (str);
  if (attr->comment && (rem > 3)) {
    strcat (str, " / ");
    strncat (str, attr->comment, rem - 3);
  }
  rem = FITS_CARDSIZE - strlen (str);
  strncat (str, blank, rem);
  if (fwrite (str, FITS_CARDSIZE, 1, fp) != 1) {
/*
    errstk ("*** sds_write_fits FAILURE: error writing header\n");
*/
    status = WRITE_FAILURE;
  }
  return status;
}

static int PutAttributes (DR *dr, FILE *fp)
  {
  int numput= 0;
  ATTRIBUTES *attr = dr->attrib;
  
  while (attr)
    {
    if (strcmp (attr->name, "SIMPLE") &&
        strcmp (attr->name, "BITPIX") &&
        strncmp (attr->name, "NAXIS", 5)
	&& strcmp (attr->name, "BSCALE") &&
	strcmp (attr->name, "BZERO") &&
	strcmp (attr->name, "BLANK") &&
	strcmp (attr->name, "END"))
       {
				     /*  should test here if attr has value  */
            PutHeaderRecord (attr, fp);
					/* if (soi_errno == NO_ERROR) */
	    numput++;
       }
/*
      else
      errstk ("**  sds_write_fits WARNING: FITS reserved attribute %s ignored\n",
          attr->name);
*/
    attr = dr_next_attr (attr);
    }
  return numput;
  }

static int PutOneMandatory (char *key, void *value, int attrtype,
    char *comment, FILE *fp) {
  int status;
  ATTRIBUTES *temp;
  
  if (!(temp = dr_malloc_attribute())) return 0;
  dr_set_attrname (temp, key);
  dr_set_attrvalue (temp, value, attrtype);
  if (comment) dr_set_comment (temp, comment);
  else dr_set_comment (temp, "");
  status = PutHeaderRecord (temp, fp);
  dr_free_attr (temp);
  return 1;
}

static int PutMandatory (DR *dr, FILE *fp) {
  int bitpix, datatype, rank, dim, i;
  char simple[FITS_VALSIZE+1], naxis[FITS_KWSIZE+1];
  int numput = 0;
  
  sprintf (simple, "file conforms with FITS standard; JSOC %s", dr_lib_version);
  numput += PutOneMandatory ("SIMPLE", "T", DR_LOGICAL, simple, fp);

  datatype = dr_datatype (dr);
  bitpix = 8 * dr_numbytes (dr);
  if (bitpix == 0 || bitpix % 8) {
    bitpix = 8;
/*
    soi_errno = DR_INVALID_DATATYPE;
    errstk ("**  sds_write_fits WARNING: illegal data type - BITPIX set to 8\n");
*/
  }
  if (datatype == DR_FLOAT || datatype == DR_DOUBLE || datatype == DR_TIME)
    bitpix *= -1;
  numput += PutOneMandatory ("BITPIX", &bitpix, DR_INT, "", fp);
  rank = dr_rank (dr);
  numput += PutOneMandatory ("NAXIS", &rank, DR_INT, "", fp);
  for (i=0; i<rank; i++) { 
    sprintf (naxis, "NAXIS%d", i+1);
    dim = dr_dim_n (dr, i);
    numput += PutOneMandatory (naxis, &dim, DR_INT, "", fp);
  }
  if (dr->scaling) {
    numput += PutOneMandatory ("BSCALE", &dr->bscale, DR_DOUBLE, NULL, fp);
    numput += PutOneMandatory ("BZERO", &dr->bzero, DR_DOUBLE, NULL, fp);
  }

  if (dr->fillval) {
    switch (dr->datatype) {
      case (DR_BYTE):
      case (DR_SHORT):
      case (DR_INT):
      case (DR_LONG):
      case (DR_TIME):
        numput +=
	    PutOneMandatory ("BLANK", dr->fillval, dr->datatype, NULL, fp);
    }
  }
  return numput;
}

static int PutCommentary (DR *dr, FILE *fp) {
  int rlen;
  int numput = 0, rem = 0;
  char *str;
  char c = '\0', sp = '\x20';

  if (str = dr->comment) {
    rlen = strlen (str);
    numput++;
    if (fwrite ("COMMENT ", 8, 1, fp) != 1) {
      ;
/*
      errstk ("*** sds_write_fits FAILURE: error writing header\n");
      soi_errno = IO_ERROR;
*/
    }
    rem = FITS_CARDSIZE - 8;
    while (rlen--) {
      if ((c = *(str++)) == '\n') {
        if (rem < (FITS_CARDSIZE - 8)) {
          while (rem) {
	    fputc (sp, fp);
	    rem--;
	  }
        }
      } else {
	fputc (c, fp);
	rem--;
      }
      if (rem == 0 && rlen && *str != '\n') {
        numput++;
        if (fwrite ("COMMENT ", 8, 1, fp) != 1) {
/*
	  errstk ("*** sds_write_fits FAILURE: error writing header\n");
	  soi_errno = IO_ERROR;
*/
	  ;
	}
        rem = FITS_CARDSIZE - 8;
      }
    }
    while (rem--)
    fputc (sp, fp);
  }

  if (str = dr->history) {
    rlen = strlen (str);
    numput++;
    if (fwrite ("HISTORY ", 8, 1, fp) != 1) {
      ;
/*
      errstk ("*** sds_write_fits FAILURE: error writing header\n");
      soi_errno = IO_ERROR;
*/
    }
    rem = FITS_CARDSIZE - 8;
    while (rlen--) {
      if ((c = *(str++)) == '\n') {
        if (rem < (FITS_CARDSIZE - 8)) {
          while (rem) {
	    fputc (sp, fp);
	    rem--;
	  }
	}
      } else {
	fputc (c, fp);
	rem--;
      }
      if (rem == 0 && rlen && *str != '\n') {
	numput++;
        if (fwrite ("HISTORY ", 8, 1, fp) != 1) {
	  ;
/*
	  errstk ("*** sds_write_fits FAILURE: error writing header\n");
	  soi_errno = IO_ERROR;
*/
	}
        rem = FITS_CARDSIZE - 8;
      }
    }
    while (rem--)
      fputc (sp, fp);
  }

  return numput;
}

static int PutRemainBlanks (int n, FILE *fp) {
  int remain, written; 
  int len = 0;
  
  written = n % FITS_NCARDS;
  remain = FITS_NCARDS - written;
  if (remain % FITS_NCARDS) {
    len = (FITS_CARDSIZE * (FITS_NCARDS - written));
    fprintf (fp, "%*c", len, ' ');
  }
  return len;
}

static int write_fits_head (DR *dr, FILE *fp) {
  int status = NO_ERROR;
  int cards = 0;

  cards += PutMandatory (dr, fp);
/*
  status += soi_errno;
*/
  cards += PutCommentary (dr, fp);
  cards += PutAttributes (dr, fp);
  cards += PutOneMandatory ("END", NULL, DR_VOID, NULL, fp);
  PutRemainBlanks (cards, fp);

  return status;
}

static int write_fits_data (DR *dr, FILE *fp) {
  int status = NO_ERROR;
  long nbytes, len;
  long mapsize = FITS_NCARDS * FITS_CARDSIZE;
  char *zero;

  nbytes = dr_data_length (dr);
  if (nbytes == 0 || dr->data == NULL) return status;
  nbytes *= dr->datumsize;
						      /*  flipping of bytes  */
#ifdef IEEE_EL
  dr_flip_data (dr);
#endif
  if (fwrite (dr->data, 1, nbytes, fp) != nbytes) {
/*
    errstk ("*** sds_write_fits FAILURE: nbytes = %d, fp @ %p, data @ %p\n",
        nbytes, fp, sds->data);
*/
    status = WRITE_FAILURE;
  }
						      /*  flip them back!!!  */
#ifdef IEEE_EL
  dr_flip_data (dr);
#endif
  						 /*  write extra NULL bytes  */
  len = (nbytes) % mapsize;  
  if (len) {
    len = mapsize - len;  
    if (!(zero = (char *) calloc (1, len)))
      return MALLOC_FAILURE;
    if (fwrite (zero, 1, len, fp) != len) {
/*
      errstk ("*** sds_write_fits FAILURE: FITS padding write failed, nbytes = %d fp @ %p\n", len, fp);
*/
      status = WRITE_FAILURE;
    }
    free (zero);
  }
  return (status);
}

int dr_write_fits (DR *dr, FILE *out) {
  double scale, bias;
  unsigned long clngth;
  int status = NO_ERROR, save_status = NO_ERROR;
  int bpp, datatype, savetype, reconvert = 0, rescale = 0;
  unsigned char *old, *new, *new0 = NULL;
  if (!dr) return DR_POINTER_NULL;
  if (dr_data_length (dr) == 0) {
			       /*  Special case for structures without data  */
    if (status = write_fits_head (dr, out)) {
      if (status != DR_INVALID_DATATYPE || dr_data_length (dr)) {
/*
        errstk ("*** sds_write_fits FAILED with status %d\n", status);
*/
        return status;
      }
    }
    return status;
  }

  savetype = datatype = dr_datatype (dr);
  if (bpp = dr->scaling) {
    bpp = (bpp < 9) ? 8 : (bpp < 17) ? 16 : 32;
    scale = dr_bscale (dr);
    bias = dr_bzero (dr);
    if (datatype == DR_FLOAT || datatype == DR_DOUBLE) {
    /*  output scaling of floating-point data to fixed-point representation  */
      clngth = dr_numbytes (dr) * dr_data_length (dr);
      new = new0 = (unsigned char *)malloc (clngth);
      old = (unsigned char *)dr_data (dr);
      while (clngth--) *new++ = *old++;
      if (scale != 1.0 || bias != 0.0)
        dr_scale_data (dr, (1.0 / scale), -(bias / scale));
      datatype = (bpp == 8) ? DR_BYTE : (bpp == 16) ? DR_SHORT : DR_INT;
      if (save_status = dr_data_convert (dr, datatype)) ;
/*
        if (save_status == DATA_OUT_OF_RANGE) ;
          errstk ("**  sds_write_fits WARNING: some data out of range\n");
	else
          errstk ("**  sds_write_fits WARNING: conversion failure: status %d\n",
	      status);
*/
      dr_set_scaling (dr, bpp, scale, bias);
      reconvert = 1;
    } else {
      dr_set_scaling (dr, 0, 1.0,  0.0);
      rescale = 1;
    }
  }
					 /*  Convert unsupported FITS types  */
/*
  if (datatype == DR_UBYTE || datatype == DR_USHORT || datatype == DR_UINT ||
      datatype == DR_ULONG) {
    clngth = sds_numbytes (sds) * sds_data_length (sds);
    new = new0 = (unsigned char *) malloc (clngth);
    old = (unsigned char *)sds_data (sds);
    while (clngth--)
      *new++ = *old++;
    if (datatype == DR_BYTE)
      sds_data_convert (sds, DR_SHORT);
    else if (datatype == DR_USHORT)
      sds_data_convert (sds, DR_INT);
    else
      sds_data_convert (sds, DR_DOUBLE);
    reconvert = 1;
  }
*/
  if (status = write_fits_head (dr, out)) {
    if (status != DR_INVALID_DATATYPE || dr_data_length (dr)) {
/*
      errstk ("*** sds_write_fits FAILED with status %d\n", status);
*/
      return status;
    }
  }
  if (status = write_fits_data (dr, out)) {
/*
    errstk ("*** sds_write_fits FAILED with status %d\n", status);
*/
    return status;
  }

  if (reconvert) {
    dr_free_data (dr);
    dr_set_datatype (dr, savetype);
    dr_set_data (dr, new0);
  } 
  if (rescale)
    dr_set_scaling (dr, bpp, scale, bias);
  if (status != NO_ERROR) return status;
  return save_status;
}

/*****************************************************************************/
// from src/libdraw.d/scaling.c
/*****************************************************************************/

#define MAXCOLORS   (256)

int getColorTable (char *name, int *bpp, int *Red, int *Green, int *Blue) {
  FILE *ctbl;
  int z;

  if (!(ctbl = fopen (name, "r"))) return 0;
  fscanf (ctbl, "%d", bpp);
  for (z=0; z<MAXCOLORS; z++) {
    if (fscanf (ctbl, "%d %d %d", Red+z, Green+z, Blue+z) != 3) {
      fclose (ctbl);
      return z;
    }
  }
  fclose (ctbl);
  return z;
}

/*****************************************************************************/
// from /CM/src/libast.d/str_util.c
/*****************************************************************************/

/* mprefix - extracts multiplier prefix from ascii number */

typedef struct _nmval {
        char *name;
        double value;
        } NameVal_t;

static NameVal_t pre[] = {
        "exa",          1e18,
        "pecta",        1e15,
        "tera",         1e12,
        "giga",         1e9,
        "mega",         1e6,
        "kilo",         1e3,
        "hecto",        1e2,
        "deca",         1e1,
        "deci",         1e-1,
        "centi",        1e-2,
        "milli",        1e-3,
        "micro",        1e-6,
        "mu",           1e-6,
        "nano",         1e-9,
        "pico",         1e-12,
        "femto",        1e-15,
        "atto",         1e-18,
        };

char *mprefix(char *str, double *mult)
{
int i;
char *pc;
char *s = str;
int n_prefixes = sizeof(pre)/sizeof(NameVal_t);

/* strstr replaces sindex */
for (i=0; i < n_prefixes; ++i)
        if (pc = strstr(s, pre[i].name))
                {
                s = pc + strlen(pre[i].name);
                *mult = pre[i].value;
                return(s);
                }
*mult = 1.0;
return(str);
}



/*****************************************************************************/
// from /CM/src/libast.d/atoinc.c
/*****************************************************************************/

/* atoinc - converts ascii time increment to number of seconds */

typedef struct
	{
	char *nm;
	int code;
	} nametable;

static nametable tmunit[] = {
	"hz",		8,
	"hertz",	8,
	"rot",		7,
	"rots",		7,
	"rotation",	7,
	"rotations",	7,
	"deg",		6,
	"degs",		6,
	"degree",	6,
	"degrees",	6,
	"wk",		5,
	"wks",		5,
	"week",		5,
	"weeks",	5,
	"w",		5,
	"d",		4,
	"day",		4,
	"days",		4,
	"h",		3,
	"hr",		3,
	"hrs",		3,
	"hour",		3,
	"hours",	3,
	"m",		2,
	"min",		2,
	"mins",		2,
	"minute",	2,
	"minutes",	2,
	"secs",		1,
	"second",	1,
	"seconds",	1,
	"s",		1,
	"",		0,
	0,		0,
	};
/*
static long secs[] = {
	1,
	5,
	10,
	15,
	30,
	1*60,
	5*60,
	6*60,
	10*60,
	12*60,
	15*60,
	20*60,
	30*60,
	1*3600,
	2*3600,
	3*3600,
	4*3600,
	6*3600,
	8*3600,
	12*3600,
	};
*/
/* strcasecmp replaces Strcmp */
static int lookup(char *n, nametable *t)
{
	while (strcasecmp(n,t->nm) && t->code) ++t;
	return(t->code);
}

TIME atoinc(char *str)
{
double num, mult;
char *units, *base_units;

if (!str) return 0;
num = strtod (str, &units);
if (str == units) num = 1;  /* only the units specified, assume 1 */
if (*units == '_') units++; /* to be backwards compatible with form like 1_minutes */

base_units = mprefix(units, &mult);    /* units may have prefix like giga */
if (*base_units == '_') base_units++;  /* to handle form like giga_hertz */

switch(lookup(base_units,tmunit))
	{
case 1: /* seconds	*/
	return(mult*num);
case 2: /* minutes	*/
	return(mult*num*60.0);
case 3: /* hours	*/
	return(mult*num*3600.0);
case 4: /* days		*/
	return(mult*num*86400);
case 5: /* weeks	*/
	return(num*604800);
case 6: /* carrington degrees	*/
	return(num);
case 7: /* carrington rotations	*/
	return(num*360);
case 8:	/* hertz		*/
	return(mult*num);
default:
	return(0);
	}
}

#define integral(x) ((x)==(long)(x))

char *sprint_inc(char *str, TIME inc)
{
double ii;

if (integral(ii = inc/31556952)) sprintf(str,"%.0fyear",ii);
else if (integral(ii = inc/604800)) sprintf(str,"%.0fweek",ii);
else if (integral(ii = inc/86400)) sprintf(str,"%.0fday",ii);
else if (integral(ii = inc/3600)) sprintf(str,"%.0fhour",ii);
else if (integral(ii = inc/60)) sprintf(str,"%.0fminute",ii);
else sprintf(str,"%.fsecond",ii=inc);
if (ii>1 || ii<-1) strcat(str,"s");
return(str);
}

int fprint_inc(FILE *fp, TIME inc)
{
char str[64];
return(fprintf(fp,"%s",sprint_inc(str,inc)));
}

int print_inc(inc)
TIME inc;
{
return(fprint_inc(stdout,inc));
}

/*****************************************************************************/
// from sds_key.c
/*****************************************************************************/

/*
 *  sds_key.c	       
 *
 *
 *  Common interface routines
 *
 *  This set of functions provides a programming interface to
 *  the set of keyword utility functions of the sds library.
 *  This is to allow easier to remember usage consistent with similar routines 
 *  in the keylist (libast.d) and vds (libvds.d) libraries.  
 *
 *  Routines are provided with forms of:
 *    sds_getkey_xxx
 *    sds_setkey_xxx
 *
 *  where the set of functions, xxx are:
 *
 *    int
 *    double
 *    time
 *    time_interval
 *    str
 *
 *  In the case of _str, an additional function is provided
 *  which has upper-case first words, i.e.:
 *
 *   SDS_getkey_str
 *
 *  which is like the lower case version BUT return a pointer
 *  to a static string.  the return string must be used before
 *  subsequent calls to the same function or the value will
 *  be overwritten.  Also, the returned string must not be
 *  "freed" or otherwise treated as a malloced string.
 *
 *  These functions are designed so they can be used without
 *  explicit testing for failures if the values for missing
 *  responses are acceptable in the use.  The returned missing
 *  values are:
 *
 *     int       I_MISSING ( 1 << 31 )
 *     double    D_MISSING (a quiet d NaN)
 *     time      T_MISSING (-4712.01.01_12:00:00.000_UT)
 *     time_interval     TINT_MISSING (0.0)
 *
 *  if the missing value is the result of a void input pointer
 *  or other such probably unexpected error,
 *  soi_errno will be set as appropriate and an errstk message
 *  will be generated.  If the keyword is present but has
 *  no value present, the missing value will be returned without
 *  comment.
 *
 *  Most of the functionality in these routines exists by different
 *  names.  In those cases, the code is simple.  Macros have not
 *  been used even in those cases to allow different error handling
 *  if needed.  
 *
 *  Contents:
 *
 *  char *sds_getkey_str(SDS *sds, char *key);
 *  char *SDS_getkey_str(SDS *sds, char *key);
 *  int sds_getkey_int(SDS *sds, char *key);
 *  double sds_getkey_double(SDS *sds, char *key);
 *  TIME sds_getkey_time(SDS *sds, char *key);
 *  TIME sds_getkey_time_interval(SDS *sds, char *key);
 *
 *  int sds_setkey_str(SDS *sds, char *key, char *str);
 *  int sds_setkey_int(SDS *sds, char *key, int val);
 *  int sds_setkey_double(SDS *sds, char *key, double val);
 *  int sds_setkey_time(SDS *sds, char *key, TIME time);
 *  int sds_setkey_time_interval(SDS *sds, char *key, TIME time);
 *
 *  Responsible:  Kay Leibrand				KLeibrand@solar
 *
 *  Bugs:
 *
 *  Planned updates:
 *
 *  Revision history is at end.
 */


/* dr_getkey routines */

char *dr_getkey_str(DR *dr, char *key)
{
char *c = dr_search_attrvalue_str(dr,key);
if (c)
        return(c);
return(strdup(""));
}

/* same as dr_getkey_str but returns copy in static string */
char *DR_getkey_str(DR *dr, char *key)
{
static char tmp[DR_MAXSTRING];
char *c;
strcpy(tmp, (c=dr_getkey_str(dr,key)));
free(c);
return(tmp);
}

double dr_getkey_double(DR *dr, char *key)
{
return(dr_search_attrvalue_double(dr,key));
}

int dr_getkey_int(DR *dr, char *key)
{
double dv = dr_search_attrvalue_double(dr,key);
int retval;
if (is_D_MISSING(dv))
        retval = I_MISSING;
else
        retval = dv + (dv >= 0.0 ? 0.5 : -0.5);
return(retval);
}

TIME dr_getkey_time(DR *dr, char *key)
{
TIME retval;
char *tim = dr_getkey_str(dr,key);
if (*tim == '\0')
        retval = T_MISSING;
else
        retval = sscan_time(tim);
free(tim);
return(retval);
}

TIME dr_getkey_time_interval(DR *dr, char *key)
{
return(atoinc(DR_getkey_str(dr,key)));
}

/* dr_setkey routines */

int dr_setkey_str(DR *dr, char *key, char *str)
{
int stat=dr_set_attribute(dr, key, str, DR_STRING, NULL);
if (stat)
        fprintf(stderr,"*** WARNING dr_set_attribute failed, ptr=%p, key=%s\n", dr, key);
return(stat);
}

int dr_setkey_int(DR *dr, char *key, int val)
{
int tmp = val;
int stat=dr_set_attribute(dr, key, &tmp, DR_INT, NULL);
if (stat)
        fprintf(stderr,"*** WARNING dr_set_attribute failed, ptr=%p, key=%s\n", dr, key);
return(stat);
}

int dr_setkey_double(DR *dr, char *key, double val)
{
double tmp = val;
int stat=dr_set_attribute(dr, key, &tmp, DR_DOUBLE, NULL);
if (stat)
        fprintf(stderr,"*** WARNING dr_set_attribute failed, ptr=%p, key=%s\n", dr, key);
return(stat);
}

int dr_setkey_time(DR *dr, char *key, TIME time)
{
char tmp[DR_MAXSTRING];
if (time == T_MISSING)
        tmp[0] = '\0';
else
        sprint_ut(tmp,time);
return(dr_setkey_str(dr, key, tmp));
}

int dr_setkey_time_interval(DR *dr, char *key, TIME time)
{
char tmp[DR_MAXSTRING];
sprint_inc(tmp,time);
return(dr_setkey_str(dr, key, tmp));
}

int dr_put_fits(DR *dr, char*file)
  {
  FILE *fp;
  int status;
  if (!dr)
    return(DR_POINTER_NULL);
  fp = fopen(file, "w");
  if (!fp)
    return(FILE_POINTER_NULL);
  status = dr_write_fits(dr, fp);
  fclose(fp);
  return(status);
  }

/* Read FITS header and data into DR structure then close file */
DR *dr_get_fits(char *filename)
  {
  int status;
  FILE *fp;
  DR *fits;
  fp = fopen(filename, "r");
  if (!fp)
        {
	fprintf(stderr,"Failed to open fits file %s\n",filename);
  	fclose(fp);
        return(NULL);
	}
  fits = dr_read_fits(fp, &status);
  if (!fits || status)
	{
	if (fits)
		dr_free(&fits);
	fprintf(stderr,"Failed to find fits file in %s\n",filename);
  	fclose(fp);
	return(NULL);
	}
  fclose(fp);
  return(fits);
  }

/* Read FITS header only into DR structure then close file */
DR *dr_get_fits_head(char *filename)
  {
  int status;
  FILE *fp;
  DR *fits;
  fp = fopen(filename, "r");
  if (!fp)
        {
	fprintf(stderr,"Failed to open fits file %s\n",filename);
        return(NULL);
	}
  fits = dr_read_fits_header(fp, &status);
  if (!fits || status)
	{
	if (fits)
		dr_free(&fits);
	fprintf(stderr,"Failed to find fits header in %s\n",filename);
  	fclose(fp);
	return(NULL);
	}
  fclose(fp);
  return(fits);
  }

/* Print keywords from DR structure, use for debugging.  Does not print data info */
void dr_print_header(DR *dr)
{
ATTRIBUTES *attr;
if (!dr)
    fprintf(stderr,"$$$ dr_print_header - dr is NULL, return.\n");
else
    for (attr = dr->attrib; attr; attr = attr->next)
        fprintf(stderr,"%s = %s\n",attr->name, dr_attrvalue_str(attr));
}

int dr_setkey_drmstype(DR *dr, char *name, DRMS_Keyword_t *key)
{
if (!dr || !key)
  return(1);
switch (key->info->type)
  {
  case DRMS_TYPE_CHAR:
    dr_setkey_int(dr, name, key->value.char_val);
    break;
  case DRMS_TYPE_SHORT:
    dr_setkey_int(dr, name, key->value.short_val);
    break;
  case DRMS_TYPE_INT:
    dr_setkey_int(dr, name, key->value.int_val);
    break;
  case DRMS_TYPE_LONGLONG:
    dr_setkey_int(dr, name, (int)key->value.longlong_val);
    break;
  case DRMS_TYPE_FLOAT:
    dr_setkey_double(dr, name, key->value.float_val);
    break;
  case DRMS_TYPE_DOUBLE:
    dr_setkey_double(dr, name, key->value.double_val);
    break;
  case DRMS_TYPE_TIME:
    dr_setkey_time(dr, name, key->value.time_val);
    break;
  case DRMS_TYPE_STRING:
    dr_setkey_str(dr, name, key->value.string_val);
  }
return(0);
}

/* Map DRMS data type code into nearest DR type code */
int drms2drtype(DRMS_Type_t type)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR:
    return DR_BYTE;
    break;
  case DRMS_TYPE_SHORT:
    return DR_SHORT;
    break;
  case DRMS_TYPE_INT:
    return DR_INT;
    break;
  case DRMS_TYPE_LONGLONG:
    return DR_LONG;
    break;
  case DRMS_TYPE_FLOAT:
    return DR_FLOAT;
    break;
  case DRMS_TYPE_DOUBLE:
    return DR_DOUBLE;
    break;
  case DRMS_TYPE_TIME:
    return DR_TIME;
    break;
  case DRMS_TYPE_STRING:
    return DR_STRING;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
    return DR_STRING;
    break;
  }
}

/* Map DR data type code into nearest DRMS type code */
DRMS_Type_t dr2drmstype(int type)
{
switch(type)
  {
  case  DR_BYTE:
    return DRMS_TYPE_CHAR;
    break;
  case DR_SHORT:
    return DRMS_TYPE_SHORT;
    break;
  case DR_INT:
    return DRMS_TYPE_INT;
    break;
  case DR_LONG:
    return DRMS_TYPE_LONGLONG;
    break;
  case DR_FLOAT:
    return DRMS_TYPE_FLOAT;
    break;
  case DR_DOUBLE:
    return DRMS_TYPE_DOUBLE;
    break;
  case DR_TIME:
    return DRMS_TYPE_TIME;
    break;
  case DR_STRING:
    return DRMS_TYPE_STRING;
  default:
    fprintf(stderr, "ERROR: Unhandled DR type %d\n",type);
    XASSERT(0);
    return DRMS_TYPE_STRING;
    break;
  }
}

/* Write FITS file into segment.
 * copy this code into drms_segment.c after it works, place into drms_segment_write_from_file at
the proper place.
 */
int drms_segment_write_FITS_from_file(DRMS_Segment_t *seg, char *infile) 
{
  int status;
  int naxis, iaxis;
  DRMS_Protocol_t proto_want;
  DR *header;
  int want_data_type;

  /* Check consistency between segment and FITS header */
  header = dr_get_fits_head(infile);
  if (!header)
        return(DRMS_ERROR_IOERROR);
  want_data_type = drms2drtype(seg->info->type);
  if (header->datatype != want_data_type)
    {
    fprintf(stderr,"Write FITS from file: type miss-match, have=%d, jsd says %d\n",header->datatype,want_data_type);
    dr_free(&header);
    return(FITS_ERROR);
    }
  naxis = header->rank;
  if (naxis != seg->info->naxis)
    {
    fprintf(stderr,"Write FITS from file: naxis miss-match, have=%d, jsd says %d\n",naxis,seg->info->naxis);
    dr_free(&header);
    return(FITS_ERROR);
    }
  for (iaxis=0; iaxis < naxis; iaxis++)
    {
    int dim;
    dim = header->length[iaxis];
    if (seg->info->scope == DRMS_VARDIM)
	seg->axis[iaxis] = dim;
    else
	{
	if (dim != seg->axis[iaxis])
		{
		fprintf(stderr,"Write FITS from file: dimension error, have=%d, jsd says %d\n",dim,seg->axis[iaxis]);
		dr_free(&header);
		return(FITS_ERROR);
		}
	}
    }
  dr_free(&header);

  /* Trick drms_segment_write_from_file to take this file */
  proto_want = seg->info->protocol;
  seg->info->protocol = DRMS_GENERIC;
  status = drms_segment_write_from_file(seg, infile);
  seg->info->protocol = proto_want;
  return(status);


}

int dr_write_fits_to_drms_segment(DR *dr, char *fitsname, DRMS_Record_t *rec, int segno)
  {
  DRMS_Segment_t *seg = drms_segment_lookupnum(rec,segno);
  FILE *fp; 
  char path[1024];
  DR *header;
  int status;
  int naxis, iaxis;
  int want_data_type;

  /* first write the new FITS file */
  fp = drms_record_fopen(rec, fitsname, "w");
  status = dr_write_fits(dr, fp);
  if (status)
    {
    fprintf(stderr, "Failed to write fits file to segment, status = %d\n", status);
    return(FITS_ERROR);
    }
  fclose(fp);
  CHECKSNPRINTF(snprintf(seg->filename, DRMS_MAXSEGFILENAME, "%s", fitsname), DRMS_MAXSEGFILENAME);

  /* Now inspect the new FITS file to make sure it matches the segment spec */
  drms_record_directory(rec, path, 1);
  strcat(path, "/");
  strcat(path, fitsname);
  header = dr_get_fits_head(path);
  if (!header)
        return(DRMS_ERROR_IOERROR);
  want_data_type = drms2drtype(seg->info->type);
  if (header->datatype != want_data_type)
    {
    fprintf(stderr,"Write FITS to segment: type miss-match, dr says %d, jsd says %d\n",header->datatype,want_data_type);
    return(FITS_ERROR);
    }
  naxis = header->rank;
  if (naxis != seg->info->naxis)
    {
    fprintf(stderr,"Write FITS to segment: naxis miss-match, dr says %d, jsd says %d\n",naxis,seg->info->naxis);
    return(FITS_ERROR);
    }
  for (iaxis=0; iaxis < naxis; iaxis++)
    {
    int dim;
    dim = header->length[iaxis];
    if (seg->info->scope == DRMS_VARDIM)
      seg->axis[iaxis] = dim;
    else
      {
      if (dim != seg->axis[iaxis])
              {
              fprintf(stderr,"Write FITS to segment: dimension error, dr says %d, jsd says %d\n",dim,seg->axis[iaxis]);
              return(FITS_ERROR);
              }
      }
    }
  return(0);
  }

