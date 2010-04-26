/**
\file drms_names.h
*/
#ifndef __DRMS_NAMES_H
#define __DRMS_NAMES_H

/* Not much to export in DRMS API */


int drms_names_parseduration(char **in, double *duration, double width);

/**
   @addtogroup names_api
   @{
*/

/**
   @fn int drms_names_parseduration(char **in, double *duration)
   Converts a time-duration string (like 1d, 10s, 22.5m) into a double-precision, 
   floating-point value. This output value is a time interval expressed as 
   a number of seconds (with one exception, keep reading). The format of 
   the input string is {num}{unit}, 
   where {num} is a string that represents a floating-point number, 
   and {unit} is a single character that represents the unit by which 
   {num} should be interpreted. For example, "2h" should be interpreted
   as two hours, and given an input of "2h", this function will return a
   7200.0 (for 7200 seconds). Supported values for {unit} are @a s (seconds),
   @a m (minutes), @a h (hours), @a d (days), or @a u (for slotted keywords
   only - unit defined by slotted-keyword unit). If {unit} is @a u, 
   then the unit by which {num} should interpreted is equivalent
   to the unit of the slotted-keyword values. For example, if the unit
   of a slotted-keyword value is arcseconds, then "30u" is equivalent
   to 30 arcseconds.

   @param in Pointer to the address of the input string. This function "consumes" 
   the input string, so that, upon return, *in points to one char past
   the end of the input string.
   @param duration The floating-point number of seconds (except 
   when {unit} is @a u, in which case the unit of the floating-point number
   is determined by the unit of the slotted keyword values) returned by reference.
   @return On error, returns 1, otherwise, returns 0.
*/

/**
   @}
*/

#endif

