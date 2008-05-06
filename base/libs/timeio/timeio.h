/**
   @file timeio.h

   @brief I/O conversion of date/time representations

   The functions support a range of clock systems including Atomic Time
   (@e TAI, Temps Atomique Internationale),
   Terrestrial Dynamical Time (@e TDT), and Coordinated Universal Time
   (@e UTC), and a bewildering array of civil time zones tied to Universal Time.

   The detailed specifications for the format of date-time representations can
   be found in <tt>JSOC TN 07-001</tt> (http://jsoc.stanford.edu/doc/timerep.html), 
   "Time Data Type and Representations of Dates and Times in the SDO JSOC".
   Basically, three formats are supported: calendar, calender-clock, and
   Julian date:
   @par
   1.  year.month.fracday[_type]
   @par
   2.  year.month.day_hour:minute[:second][_type]
   @par
   3.  {MJD|JD}_julday[_type]

   @a month can either be an integer month number, a string containing at least the first
   three letters of the English month name, or a Roman numeral;
   @a year, @a day, @a hour, and @a minute are integers;
   @a julday, @a fracday, and @a second
   can either be decimal fractions or integers; and
   @a type
   is a character string representing either the clock system, or, in the case
   of civil time, the time zone.  If the @a type field is empty, the defaults
   assumed by ::sscan_time are @e TDT if the notation is in Julian day format,
   @e UTC otherwise. @e TT is an acceptable substitute for @e TDT
   and @e UT for @e UTC.
   Time zone designations can be any of the standard single-letter zone
   designations, signed 4-digit hour/minute offsets from @e UT, or some of
   the common 3- or 4-letter designations, @e e.g. @e PST or @e NZDT.
   (See Table 2 of <tt>JSOC TN 07-001</tt> (http://jsoc.stanford.edu/doc/timerep.html) 
   for a listing of accepted codes.)
   ::sscan_time is case-insensitive.

   For example, the call
   @code
   char name[32];
   sprint_ut (name, sscan_time ("1981.Jul.01_00:00:19.5_TAI"));
   @endcode

   will place in @a name the string "1981.06.30_00:00:60.500_UT".
   The call:
   @code
   char name[32];
   sprint_time (name, sscan_time ("1582.10.15_03:30"),"PDT");
   @endcode

   will place in @a name the string "1582.10.04_20:30:00.000_PDT".

   @sa JSOC TN 07-001 (http://jsoc.stanford.edu/doc/timerep.html)

   @par Diagnostics:
   None.  When an indecipherable string is encountered, ::sscan_time returns
   the  valid  time  ::JULIAN_DAY_ZERO,  represented   by   ::sprint_time   as
   -4712.01.01_12:00:00.000_TT or -4712.01.01_11:59:27.816_UT or JD_0.0.
   
   @bug 
   The definition of the "invalid" time ::JULIAN_DAY_ZERO is not the same
   as the definition of ::DRMS_MISSING_TIME in drms_types.h.

   @par
   Years prior to 1 are written in "astronomical" rather than "historical"
   fashion.  For example, the year 0 corresponds to 1 B.C, -1 to 2 B.C.,
   @e etc.
   The proleptic Julian reckoning of leap years is continued throughout.

   @par
   Times during @e UT leap seconds are represented and assumed represented as
   <tt>xxx_23:59:60.yyy_UT</tt> or the equivalent.  The leap seconds occurring prior to 1972
   are proleptic; those after 1997
   are predictions.  Predicted future @e UT
   leap seconds are not automatically included at the time of announcement,
   and require recompilation of the library. It is assumed that leap seconds
   occur in the final minute of the @e UT day, regardless of the zone time
   designation. @e UT times before 1959.12.31:23:59:60 are identical to @e TAI times.

   @par
   All times corresponding to dates on or before 4.Oct.1582 are
   represented in the Julian calendar, and all times corresponding to
   dates on or after 15.Oct.1582 are represented in the Gregorian calendar.

   @par
   ::tai_adjustment for civil zone times is not well defined for times
   during the @e UT leap second; its value during such seconds is the same as
   that for the times one second later.

   @par
   When clock and date fields are suppressed with negative @a precision
   values in ::sprint_time, the remaining clock and date fields are truncated
   rather than rounded. (Rounding always occurs for times in JD format, as
   there is only a single field.)

   @par
   Single-letter time zone designations are suppressed when only the date
   field or less is printed, @e e.g. 1582.10.04_13:00Z or 1582.10.04_13:00_UT
   but 1582.10.04 or 1582.10.04_UT.

   @par
   All times represented in Julian Day formats JD or MJD are assumed to  be
   in Terrestrial Dynamical Time, which is offset by an assumed constant
   32.184 sec from Atomic Time.

   @par
   There is not complete reciprocity between date-time strings that can be
   produced by ::sprint_time and those that can be properly read by
   ::sscan_time. In particular, there is currently no support for
   scanning of strings in ISO-8601 format, though they can be generated.

   @par
   Because of the use of @c strtok in parsing strings in ::sscan_time,
   repeated separator symbols ([@c .:_]) are treated as single symbols.

   @par
   Strings with a plus or minus symbol in the hours or minutes field are
   subject to misinterpretation by ::sscan_time, due to confusion with
   the ISO time-zone designator.

   @par
   If the time value is a NaN or Infinite, ::sprint_time will print the
   appropriate string for the "invalid" time JULIAN_DAY_ZERO. If the time
   value is so large that the year number in conventional date-time format
   would exceed the range of signed 4-byte integers, the format is automatically
   converted to "JD" regardless of what was requested; this occurs for absolute
   time values greater than 6.776e16.

   @par
   Ephemeris Time and "Carrington Time" are not supported.
*/


/*
 *  timeio.h							~CM)/include
 *
 *  Defines and declarations for date/time parsers sscan_time(), sprint_time(),
 *    and related functions in SOI CM libast.
 *  Additional information is in the following man pages:
 *      sscan_time (3), sprint_time (3)
 *
 *  Responsible:  Rick Bogart				RBogart@spd.aas.org
 *
 *  Bugs:
 *    The function utc_adjustment is undocumented, and should probably not be
 *	exposed as an extern, or possibly eliminated entirely
 *    The TIME typedef declaration conflicts with the same typedef in the old
 *      wso /usr/local functions, and the meaning is different; concurrent use
 *      of both libraries should be avoided
 *
 *  Revision history is at the end of the file.
 */
#ifndef TIMECNVRT_INCL
/****************************************************************************/
/**************************  INCLUDE STATEMENTS  ****************************/
/****************************************************************************/

#include <sys/types.h>
#include <time.h>

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#define TIMECNVRT_INCL   1

/****************************************************************************/
/*******************************  TYPEDEFS  *********************************/
/****************************************************************************/

/** 
    @brief Internal representation of time 

    The number of SI seconds between the represented time and an arbitrary epoch.
*/
typedef double TIME;

/****************************************************************************/
/****************************  MACRO DEFINITIONS  ***************************/
/****************************************************************************/

/** @brief Convert internal representation of time to TAI string (to the nearest second) */
#define sprint_at(A, B) (sprint_time (A, B, "TAI", 0))
/** @brief Convert internal representation of time to TDT string (to the nearest second) */
#define sprint_dt(A, B) (sprint_time (A, B, "TDT", 0))
/** @brief Convert internal representation of time to UT string (to the nearest second) */
#define sprint_ut(A, B) (sprint_time (A, B, "UT", 0))
#define CURRENT_SYSTEM_TIME (time (NULL) + UNIX_EPOCH)
/** @brief Internal time representation of Julian Day Zero */
#define JULIAN_DAY_ZERO (sscan_time ("JD_0.0"))
#define SOHO_EPOCH (sscan_time ("1958.01.01_00:00:00_TAI"))
#define UNIX_EPOCH (sscan_time ("1970.01.01_00:00:00_UTC"))
#define SOHO_LAUNCH (sscan_time ("1995.12.02_08:08:00_UT"))
#define MDI_FIRST_LIGHT (sscan_time ("1995.12.19_17:49:00_UT"))
#define SOI_START_LOGS (sscan_time ("1996.01.01_00:00:00_TAI"))
#define MISSION_EPOCH (sscan_time ("1993.01.01_00:00:00_TAI"))

extern const int kTIMEIO_MaxTimeEpochStr;
#define DRMS_EPOCH_S "1977.01.01_00:00:00_TAI"
#define DRMS_EPOCH_F (sscan_time(DRMS_EPOCH_S))
#define DRMS_EPOCH (0.000)
#define MDI_EPOCH_S "1993.01.01_00:00:00_TAI"
#define MDI_EPOCH_F (sscan_time(MDI_EPOCH_S))
#define MDI_EPOCH (504921600.000)
#define WSO_EPOCH_S "1601.01.01_00:00:00_UT"
#define WSO_EPOCH_F (sscan_time(WSO_EPOCH_S))
#define WSO_EPOCH (-11865398400.000)
#define TAI_EPOCH_S "1958.01.01_00:00:00_TAI"
#define TAI_EPOCH_F (sscan_time(TAI_EPOCH_S))
#define TAI_EPOCH (-599616000.000)
#define MJD_EPOCH_S "1858.11.17_00:00:00_UT"
#define MJD_EPOCH_F (sscan_time(MJD_EPOCH_S))
#define MJD_EPOCH (-3727641600.000)

/****************************************************************************/
/***************************  FUNCTION PROTOTYPES  **************************/
/****************************************************************************/

					  /*  source file: timeio/timeio.c  */
/**
   @brief Converts a string representation of time into an internal
   representation of time.

   Reads from the @a s representing a calendar
   date/clock time in a standard format,
   returning a double-precision floating point number representing the elapsed
   time in @e SI seconds between the represented time and an arbitrary epoch.

   @param s String representing a clock time in a standard format (refer to
   JSOC TN 07-001, http://jsoc.stanford.edu/doc/timerep.html).
   @return DRMS representation of time (seconds since an epoch, see ::TIME).
*/
extern TIME sscan_time (char *s);

/**
   @brief Converts an internal representation of time into a string representation
   of time.

   Writes a string corresponding to the elapsed time @a t into the location
   @a str, making the appropriate conversions for the clock system
   designated by @a zone.
   The precision of the clock seconds field is controlled by the
   @a precision parameter, which determines the number of digits written beyond the
   decimal point.  Negative values of @a precision 
   result in suppression of increasing numbers of clock and date fields, up
   to values of -5. For example, a precision of -1 will suppress the seconds
   field, and a precision of -5 (or less) will cause only the year to be
   written. 

   @param s Return buffer to hold a string representing a clock time 
   in a standard format (refer to JSOC TN 07-001, http://jsoc.stanford.edu/doc/timerep.html).
   @param t DRMS representation of a time (seconds since an epoch, see ::TIME).
   @param zone Time system in which @a s is expressed.
   @param precision Number of digits to the right of the decimal point in the seconds field 
   of @a s.
*/
extern void sprint_time (char *s, TIME t, char *zone, int precision);

/**
   @brief Return time difference between a zone time and TAI time

   Returns the difference in seconds between
   @e TAI and the requested @a zone clock time at the specified internal time
   @a t, in the sense <tt>TAI time - zone time</tt>; for example it returns the
   constant values of 32.184 for zone "TDT", and -19.0 for "GPS".
   It returns a value of -32.0 for "UTC" when the date is between
   1999.01.01 and 2005.12.31.

   @param t DRMS representation of a time (seconds since an epoch, see ::TIME).
   @param zone Time system to which @a t is converted before comparing against the
   @e TAI representation of @a t.
   @return Difference, in seconds, between the @e TAI time and the @a zone time.
*/
extern double tai_adjustment (TIME t, char *zone);

/**
   @brief Return time difference between a specified time system and UTC time

   Returns the difference in seconds between the named zone and @e UTC, for example
   -18000.0 for zone "BEST".

   @param zone Time system that is being compared to @e UTC time.
   @return Difference, in seconds, between the @a zone time system and @e UTC time.
*/
extern double zone_adjustment (char *zone);

int zone_isvalid (char *zone);

/** 
    @brief Checks for the validity of an internal time value

    Returns 1 if the time @a t is either @c NaN
    or if it is equal to ::JULIAN_DAY_ZERO.

    @param t DRMS representation of a time (seconds since an epoch, see ::TIME)
    that will be validated.
    @return 0 if time is valid, 1 otherwise.
*/
extern int time_is_invalid (TIME t);

#endif
/*
 *  Revision History
 *  V 1.0  07.05.04 Rick Bogart		created this file, based on previous
 *		file soi_time.h, renamed, with minor modifications as follows:
 *	changed inclusion define from SOI_TIME_INCL to TIMECNVRT_INCL
 *	remove SOI_TIME_VERSION_NUM definition
 *	removed conditional on TIME typedef
 *	removed extern declaration of utc_adjustment()
 *	updated comments
 *  07.05.25	R Bogart	added prototype for time_is_invalid
 */
