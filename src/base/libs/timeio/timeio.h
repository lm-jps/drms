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
  
typedef double TIME;

/****************************************************************************/
/****************************  MACRO DEFINITIONS  ***************************/
/****************************************************************************/

#define sprint_at(A, B) (sprint_time (A, B, "TAI", 0))
#define sprint_dt(A, B) (sprint_time (A, B, "TDT", 0))
#define sprint_ut(A, B) (sprint_time (A, B, "UT", 0))
#define CURRENT_SYSTEM_TIME (time (NULL) + UNIX_EPOCH)
#define JULIAN_DAY_ZERO (sscan_time ("JD_0.0"))
#define SOHO_EPOCH (sscan_time ("1958.01.01_00:00:00_TAI"))
#define UNIX_EPOCH (sscan_time ("1970.01.01_00:00:00_UTC"))
#define SOHO_LAUNCH (sscan_time ("1995.12.02_08:08:00_UT"))
#define MDI_FIRST_LIGHT (sscan_time ("1995.12.19_17:49:00_UT"))
#define SOI_START_LOGS (sscan_time ("1996.01.01_00:00:00_TAI"))
#define MISSION_EPOCH (sscan_time ("1993.01.01_00:00:00_TAI"))

/****************************************************************************/
/***************************  FUNCTION PROTOTYPES  **************************/
/****************************************************************************/

					  /*  source file: timeio/timeio.c  */
extern TIME sscan_time (char *s);
extern void sprint_time (char *s, TIME t, char *zone, int precision);
extern double tai_adjustment (TIME t, char *zone);
extern double zone_adjustment (char *zone);
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
