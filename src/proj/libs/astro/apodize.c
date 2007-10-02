/*
 *  apodize.c                                  ~soi/(level)/src/functions
 *
 *  Description:
 *    Apodizes remapped image. Apodizations is done as a function of
 *    fractional radius on the original image or the fictitious
 *    image that would have been obtained if B0=0.
 *
 *  Responsible:  Kay Leibrand                  KLeibrand@solar.Stanford.EDU
 *  Iresponsible: Jesper Schou                  JSchou@solar.Stanford.EDU
 *
 *  Bugs:
 *    Does not operate on SDS structures, so does not belong in functions
 *	directory.
 *    No checking for type or length validity of input array; this function
 *	really should be a local support function for a sds_apodize().
 *
 *  Planned Updates:
 *    Write a real sds_apodize() function, with arbitrary form of the
 *	apodization function and apodization in multiple dimensions and
 *	proper tests for missing data (if NaN's are used for floating-point
 *	missing, the test is unnecessary).
 *
 *  Revision history is at the end of the file.
 */


#include <stdlib.h>
#include <math.h>
#include "astro.h"

#define VERSION_NUM	(0.9)

#define PI   (M_PI)

/* Function parameters are generally the same as to obs2helio */

int apodize(float *data,		/* input/output data array */
	    double b0,		/* heliographic latitude of disk center */
	    int cols, int rows,	/* width and height of input array */
	    double Lmin,		/* start of longitude range */
	    double Ldelta,		/* increment in longitude pre col */
	    double sinBdelta,	/* increment in sin latitude per row */
	    int apodlevel,		/* type of apodization */
	    /* 0 to do no apodization */
	    /* 1 to apodize in true solar coordinates */
	    /* 2 to apodize in ideal solar coordinates */
	    double apinner,		/* fractional radius to start apodization at */
	    double apwidth,		/* width of apodization */
	    int apel,		/* do elliptical apodization as described by 
				 * apx and apy */
	    double apx,		/* divide the x position by this before applying 
				   apodization */
	    double apy)		/* divide the y position by this before applying 
				   apodization */
{
   int row, col;
   float *dp;
   double sinBbase;
   double sinB0, cosB0;
   double sinB, cosB;
   double L;
   double apouter,apinx,apoutx,ss,cc,sc,cs;
   double x, y, r, cosrho, apx2, apy2;
   double *savecosL, *savesinL;

   if (apodlevel == 0) 
   {
      return kLIBASTRO_Success;
   }

   if (apodlevel > 2) 
   {
      return kLIBASTRO_InconsistentConstraints;
   }

   if (apodlevel == 2) 
   {
      b0 = 0.0; /* pretend that B0=0.0 */
   }

   apouter = apinner + apwidth;

   sinBbase = 0.5 - rows / 2;
   sinB0 = sin(b0);
   cosB0 = cos(b0);
   apinx = sqrt(1. - apinner * apinner);
   apoutx = sqrt(1. -apouter * apouter);
   apx2 = 1.0 / apx / apx;
   apy2 = 1.0 / apy / apy;

   /* Save some time by setting up arrays first */
   savecosL = (double *)(malloc(cols * sizeof(double)));
   savesinL = (double *)(malloc(cols * sizeof(double)));
   for (col = 0; col < cols; col++) 
   {
      L = Ldelta * col + Lmin;
      savecosL[col] = cos(L);
      savesinL[col] = sin(L);
   }

   for (row = 0; row < rows; row++) 
   {
      sinB = sinBdelta * (sinBbase + row);
      cosB = sqrt(1.0 - sinB * sinB);
      ss = sinB0 * sinB;
      cc = cosB0 * cosB;
      sc = sinB0 * cosB;
      cs = cosB0 * sinB;
      dp = data + row * cols;
      if (apel == 0) 
      {
	 for (col = 0; col < cols; col++) 
	 {
	    /*    Old code
		  L=Ldelta*col+Lmin;
		  cosrho=sinB0*sinB+cosB0*cosB*cos(L);
		  r=sqrt(1.0-cosrho*cosrho);
		  if (r < apinner)
		  apod=1;
		  else if (r < apouter)
		  apod=0.5+0.5*cos(PI*(r-apinner)/apwidth);
		  else
		  apod=0.0;
		  dp[col]*=apod;
	    */
	    /*   New code, much faster.
		 All points outside apouter are turned into 0.0, no NaNs */
	    cosrho = ss + cc * savecosL[col];
	    if (cosrho > apinx) 
	    {
	       /* XXX why is this here? */
	    }
	    else if (cosrho > apoutx) 
	    {
	       dp[col] *= (float )
		 (0.5 + 0.5 * cos(PI * (sqrt(1.0 - cosrho * cosrho) - apinner) / apwidth));
	    }
	    else
	    {
	       dp[col] = 0.0;
	    }
	 }
      }
      else 
      {
	 /*  Do elliptical apodization, some old, some new code */
	 for (col = 0; col < cols; col++) 
	 {
	    x = cosB * savesinL[col];
	    y = cs - sc * savecosL[col];
	    r = sqrt(apx2 * x * x + apy2 * y * y);
	    if (r < apinner)
	    {
	       /* XXX why is this here? */
	    }
	    else if (r < apouter)
	    {
	       dp[col] *= (float )(0.5 + 0.5 * cos(PI * (r - apinner) / apwidth));
	    }
	    else
	    {
	       dp[col] = 0.0;
	    }
	 }
      }
   }

   free(savesinL);
   free(savecosL);
   return kLIBASTRO_Success;
}
