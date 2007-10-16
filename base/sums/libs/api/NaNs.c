/*
 *  NaNs.c					~soi/(version)/src/libM.d
 *
 *  These routines return Signaling and Quiet IEEE-754 NaN's (Not a Number)
 *    for IEEE machines in both single and double precision.  The NaN's are
 *    selected to have zeroes in the least significant 16 or 48 bits and
 *    ones in all other fraction bits (except of course for the most
 *    significant bit which must be turned off for Signaling NaN's).  On a
 *    VAX the routines will return numbers with bit patterns matching those
 *    of the selected Floating-Point NaNs, which happen to evaluate to
 *      1.2694127359745947e+38 (SNaN) and
 *      1.6947656946257677e+38 (NaN)
 *  They also return values of single and double format +Infinity corresponding
 *    to the IEEE standard.  (VAX implementations will return values that
 *    correspond bitwise to the single-format IEEE standard, 0 extended if
 *    necessary.)
 *  See man pages "NaN" and "isNaN".
 *
 *  Responsible:  Rick Bogart			RBogart@solar.Stanford.EDU
 *
 *  Usage:
 *
 *  Arguments:
 *
 *  Bugs:
 *
 *  Planned updates:
 *
 *  Revision history is at end of file.
 */

#include <soi_NaN.h>

#define VERSION_NUM	(0.7)

double A_Signaling_dNaN ()
{
  dNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  x.NaN_mask.frac2 = 0;
  return (x.d);
}

double A_Quiet_dNaN ()
{
  dNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = ~0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  x.NaN_mask.frac2 = 0;
  return (x.d);
}

float A_Signaling_fNaN ()
{
  fNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  return (x.f);
}

float A_Quiet_fNaN ()
{
  fNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = ~0;
  x.NaN_mask.frac = ~0;
  x.NaN_mask.frac1 = 0;
  return (x.f);
}

double dInfinity ()
{
  dNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = 0;
  x.NaN_mask.frac1 = 0;
  x.NaN_mask.frac2 = 0;
  return (x.d);
}

float fInfinity ()
{
  fNaN x;
  x.NaN_mask.sign = 0;
  x.NaN_mask.exp = ~0;
  x.NaN_mask.quiet = 0;
  x.NaN_mask.frac = 0;
  x.NaN_mask.frac1 = 0;
  return (x.f);
}
