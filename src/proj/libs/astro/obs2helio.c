/**** obs2helio ****/

/*
 *  obs2helio.c                        
 *
 *  Purpose:
 *     Interpolate CCD data to estimate value that would
 *     be obtained at specified equal incrememts of heliographic
 *     longitude and sine of latitude.
 *
 *  Description:
 *
 *     Employs bi-linear or cubic convolution interpolation to map a
 *     portion of the solar disk to a rectangular array "cols" by "rows" in
 *     width and height respectively.
 *
 *  Reference:
 *
 *     Green, Robin M. "Spherical Astronomy," Cambridge University
 *     Press, Cambridge, 1985.
 *
 *  Responsible:  Kay Leibrand                   KLeibrand@solar.Stanford.EDU
 *
 *  Restrictions:
 *     The number of output map rows must be even.
 *     The number of output map columns must be less than MAXCOLS.
 *     Assumes orientation is a 4 character string with one of the following
 *     8 legal values: SESW SENE SWSE SWNW NWNE NWSW NENW NESE 
 *
 *  Bugs:
 *     Possible division by 0 
 *
 *  Planned updates:
 *     Optimize.
 *
 *  Revision history is at end of file.
 */

#include <math.h>
#include "astro.h"

const int kMaxCols = 3072;
const double kOmegaCarr = (360 /  27.27527); /* degrees/day - synodic Carrington rotation rate */
const double kRadsPerDegree = M_PI/180.;

static void Ccker(double *u, double s);
static double Ccintd(double *f, int nx, int ny, double x, double y);
static double Linintd(double *f, int nx, int ny, double x, double y);
static void Distort(double x, 
		    double y, 
		    double rsun, 
		    int sign, 
		    double *xd, 
		    double *yd, 
		    const LIBASTRO_Dist_t *distpars);

/* Calculate the interpolation kernel. */
void Ccker(double *u, double s)
{
   double s2, s3;
     
   s2= s * s;
   s3= s2 * s;
   u[0] = s2 - 0.5 * (s3 + s);
   u[1] = 1.5*s3 - 2.5*s2 + 1.0;
   u[2] = -1.5*s3 + 2.0*s2 + 0.5*s;
   u[3] = 0.5 * (s3 - s2);
}

/* Cubic convolution interpolation */
double Ccintd(double *f, int nx, int ny, double x, double y)
{
   double  ux[4], uy[4];
   /* Sum changed to double to speed up things */
   double sum;

   int ix, iy, ix1, iy1, i, j;
     
   if (x < 1. || x >= (double)(nx-2) ||
       y < 1. || y >= (double)(ny-2))
     return 0.0;
     
   ix = (int)x;
   iy = (int)y;
   Ccker(ux,  x - (double)ix);
   Ccker(uy,  y - (double)iy);
     
   ix1 = ix - 1;
   iy1 = iy - 1;
   sum = 0.;
   for (i = 0; i < 4; i++)
     for (j = 0; j < 4; j++)
       sum = sum + f[(iy1+i) * nx + ix1 + j] * uy[i] * ux[j];
   return sum;
}

/*
 *  Bilinear interpolation, based on pipeLNU remap.
 *
 *  Reference: 
 *     Abramowitz, Milton, and Stegun, Irene, "Handbook of Mathematical
 *     Functions," Dover Publications, New York, 1964.
 *
 *  Usage:
 *    double linintd (double *f, int nx, int ny, double x, double y)
 *
 *  Bugs:
 *    Only works for double data.
 */

double Linintd(double *f, int nx, int ny, double x, double y) 
{
   double p0, p1, q0, q1;          /* weights                              */
   int ilow, jlow;   /* selected CCD pixels                  */
   double *fptr;                    /* input array temp                     */
   double u;                        
     
   ilow = (int)floor(x);
   jlow = (int)floor(y);
   if (ilow < 0) ilow = 0;
   if (ilow+1 >= nx) ilow -= 1;
   if (jlow < 0) jlow = 0;
   if (jlow+1 >= ny) jlow -= 1;
   p1 = x - ilow;
   p0 = 1.0 - p1;
   q1 = y - jlow;
   q0 = 1.0 - q1;
     
   /* Bilinear interpolation from four data points. */
   u = 0.0;
   fptr = f + jlow*nx + ilow;
   u += p0 * q0 * *fptr++;
   u += p1 * q0 * *fptr;
   fptr += nx - 1;
   u += p0 * q1 * *fptr++;
   u += p1 * q1 * *fptr;
   return u;
}

/*
 *  Cubic convolution interpolation, based on GONG software, received Apr-88
 *  Interpolation by cubic convolution in 2 dimensions with 32-bit double data
 *
 *  Reference:
 *    R.G. Keys in IEEE Trans. Acoustics, Speech, and Signal Processing, 
 *    Vol. 29, 1981, pp. 1153-1160.
 *
 *  Usage:
 *    double ccintd (double *f, int nx, int ny, double x, double y)
 *
 *  Bugs:
 *    Currently interpolation within one pixel of edge is not
 *      implemented.  If x or y is in this range or off the picture, the
 *      function value returned is zero.
 *    Only works for double data.
 */


/**** SetDistortion - used by v2helio ****/
int SetDistort(CmdParams_t *params, 
	       char *distortP,
	       char *cubicP,
	       char *tiltaP,
	       char *tiltbP,
	       char *tiltfeffP,
	       LIBASTRO_Dist_t *dOut)
{
   int error = 0;

   if (dOut != NULL)
   {
      int status = 0;
      double beta;

      dOut->disttype = cmdparams_get_int(params, distortP, &status);

      /* disttype == 0 is 'no distortion' and is not an error */
      if (dOut->disttype != 0)
      {
	 if (dOut->disttype == 1) 
	 { 
	    /* FD */
	    dOut->xc = 511.5;
	    dOut->yc = 511.5;
	    dOut->scale = 1.0;
	 }
	 else if (dOut->disttype == 2)
	 { 
	    /* vw */
	    dOut->xc = 95.1;
	    dOut->yc = 95.1;
	    dOut->scale = 5.0;
	 }
	 else 
	 {
	    error = 1;
	 }

	 if (!error)
	 {
	    dOut->feff = cmdparams_get_double(params, tiltfeffP, &status);
	    dOut->alpha = cmdparams_get_double(params, tiltaP, &status) * kRadsPerDegree;
	    beta = cmdparams_get_double(params, tiltbP, &status) * kRadsPerDegree;
	    dOut->cosbeta = cos(beta);
	    dOut->sinbeta = sin(beta);
	    dOut->cdist = cmdparams_get_double(params, cubicP, &status);

	    if (status != CMDPARAMS_SUCCESS)
	    {
	       error = 1;
	    }

	 }

      } /* disttype != 0*/
   }
   else
   {
      error = 1;
   }

   return error;
}

void Distort(double x, 
	     double y, 
	     double rsun, 
	     int sign, 
	     double *xd, 
	     double *yd, 
	     const LIBASTRO_Dist_t *distpars)
{
   /*
     For sign=+1 transforms ideal coordinates (x,y) to distorted
     coordinates (xd,yd). rsun needed to correct for scale error.
     For sign=-1 ideal and distorted coordinates are swapped.
     Units are pixels from lower left corner.
     Adapted from /home/schou/calib/dist/distort.pro
     Note that rsun must be passed in FD pixels!
   */
     
   double xx,yy,xxl,yyl,x0,y0,x00,y00,x0l,y0l,epsx,epsy,rdist;
     
   if (distpars->disttype == 0) 
   {
      *xd=x;
      *yd=y;
      return;
   }
   /* Convert fo FD pixel scale*/
   /*
     rsun=rsun*distpars.scale;
   */
   /* Don't do anyway since rsun already corrected. Don't get me started... */
     
     
   /* Shift zero point to nominal image center and convert to FD pixel scale */
     
   x00=distpars->scale*(x-distpars->xc);
   y00=distpars->scale*(y-distpars->yc);
     
   /* Radial distortion */
     
   rdist=distpars->cdist*(x00*x00+y00*y00-rsun*rsun);
     
   x0=x00+rdist*x00;
   y0=y00+rdist*y00;
     
   /* Distortion due to CCD tilt */
     
   /* First rotate coordinate system */
   x0l=x0*distpars->cosbeta+y0*distpars->sinbeta;
   y0l=-x0*distpars->sinbeta+y0*distpars->cosbeta;
     
   /* Apply distortion */
   xxl=x0l*(1.-distpars->alpha*distpars->alpha/4.+y0l/distpars->feff*distpars->alpha);
   yyl=y0l*(1.+distpars->alpha*distpars->alpha/4.+y0l/distpars->feff*distpars->alpha) - 
     rsun*rsun/distpars->feff*distpars->alpha;
     
   /* Rotate coordinates back */
   xx=xxl*distpars->cosbeta-yyl*distpars->sinbeta;
   yy=xxl*distpars->sinbeta+yyl*distpars->cosbeta;
     
   /* Total distortion */
     
   epsx=xx-x00;
   epsy=yy-y00;
     
   /* Return distorted values. Cheating with inverse transform. */
     
   *xd=x+sign*epsx/distpars->scale;
   *yd=y+sign*epsy/distpars->scale;
}

int Obs2helio(
	       float	*V,		/* input velocity array				*/
	       /* assumed declared V[ypixels][xpixels]		*/
	       float	*U,		/* output projected array			*/
	       
	       int	xpixels,	/* x width of input array			*/
	       int	ypixels,	/* y width of input array			*/
	       double   x0, 	   	/* x pixel address of disk center		*/
	       double   y0, 	   	/* y pixel address of disk center		*/
	       double   BZero,		/* heliographic latitude of disk center		*/
	       double   P,		/* angle between CCD y-axis and solar vertical	*/
	       /* positive to the east (CCW)			*/
	       double   S,		/* subtended radius of the sun	radians    	*/
	       double   rsun,		/* pixels */
	       double   Rmax,		/* maximum disk radius to use (e.g. 0.95)	*/
	       
	       int	interpolation,	/* option */
	       int	cols,		/* width of output array			*/
	       int	rows,		/* height of output array			*/
	       double   Lmin,		/* start of longitude range 			*/
	       double   Ldelta,		/* increment of longitude per col	        */
	       double 	Ladjust,        
	       double   sinBdelta,	/* increment of sin latitude per row          */
	       
	       double	smajor,
	       double	sminor,
	       double	sangle,
	       double   xscale,
	       double   yscale,
	       const char *orientation,
	       
	       int	mag_correction,		/* option */
	       int	velocity_correction,	/* option */
	       double   obs_vr,
	       double   obs_vw,
	       double   obs_vn,
	       double   vsign,
	       int	NaN_beyond_rmax,
	       int      carrStretch,    /* adjusts for differential rotation if not zero */
	       const LIBASTRO_Dist_t *distpars,
	       float   diffrotA,
	       float   diffrotB,
	       float   diffrotC,
	       LIBASTRO_RotRate_t *rRates, /* rotation rates passed by reference*/
	       int        size)
{
     
   double xtmp, ytmp;
   float u;			/* output temp				*/
     
   /* Variables for coordinate projection of desired point(L,B) on disk	*/
   double sinBbase;  		/* base value to calculate sinB         */
   double sinB0, cosB0;		/* Latitude center of disk components	*/
   double sinB, cosB;		/* Latitude desired point components	*/
   double sinL, cosL;		/* Longitude desired point components	*/
   double sinP, cosP;		/* Position angle of N wrt CCD y-axis	*/
   double cosrho;		/* Central angle to desired point comps */
   double sinp_sinrho;		/* N-S projection on disk		*/
   double cosp_sinrho;		/* W-E projection on disk		*/
   double rp;			/* projected radial distance correction */
   double x, y;			/* CCD location of desired point	*/
   double Rmax2;			/* Max disk radius to consider - squared*/
     
   /* Variables for "stretch" transformation matrix */
   double sinS, cosS, sinS2, cosS2, sinScosS;
   double stretch_xx, stretch_xy, stretch_yx, stretch_yy;
     
   /* Variables for "orientation" transformation matrix */
   int orient_xx, orient_xy, orient_yx, orient_yy;
   int swap, ewflip, nsflip;
     
   /* Variables for "combined" transformation matrix */
   double combo_xx, combo_xy, combo_yx, combo_yy;
     
   /*  Variables to determine (L,B) of desired point			*/
   int row, col;			/* index into output array		*/
   double L;			/* Longitude of desired point		*/
     
     
   /*  Variables to determine velocity correction				*/
   double mu;
   double v_rotation;
   double v_limbshift;
     
   /*  Constants used in velocity correction				*/
   const double v_lat	       = 0.0;
   const double v_equator         = 1974.1;
   const double rotation_coef_a2  = -0.121;
   const double rotation_coef_a4  = -0.178;
   const double limbshift_coef_a0 = -166.;
   const double limbshift_coef_a1 = -139.;
   const double limbshift_coef_a2 = 700.;
     
   /*  Variables for optimization  */
   double savedDeltaL[kMaxCols]; /* delta (radians) between remapped image column and CM */
   double savedsinL[kMaxCols];
   double savedcosL[kMaxCols];
   double cB0sB, cB0cB, sB0sB, sB0cB;
   double sinB2;
   int rowoffset;
   double omegaLat;
   double deltaLAdj; /* delta (radians) between remapped image column and CM AFTER
		      * differential rotation adjustment. */
     
   long i;
   double *vdp;
   float *vp;
     
   double *VD;
   VD=(double *)(malloc(xpixels*ypixels*sizeof(double)));
   vdp=VD;
   vp=V;
   for (i=0;i<xpixels*ypixels;i++) *vdp++=(double)*vp++;
     
   if (cols > kMaxCols) 
   {
      return kLIBASTRO_DimensionMismatch;
   }

   if (rows%2) return kLIBASTRO_CantDoOddNumLats;
     
   if (interpolation != kLIBASTRO_InterCubic && interpolation != kLIBASTRO_InterBilinear) 
   {     
      return kLIBASTRO_UnsupportedInterp;
   }
     
   if ((mag_correction < kLIBASTRO_MCORLevel0) || (mag_correction > kLIBASTRO_MCORLevel1))
     return kLIBASTRO_UnsupportedMCOR;
     
   if ((velocity_correction < kLIBASTRO_VCORSignOnly) || (velocity_correction > kLIBASTRO_VCORLevel2))
     return kLIBASTRO_UnsupportedVCOR;
     
   /* Calculate quantities that are constant for this image		*/
     
   sinBbase = 0.5-rows/2;
   sinS = sin(sangle); cosS = cos(sangle); 
   sinS2 = sinS*sinS;  cosS2 = cosS*cosS; sinScosS = sinS*cosS;
   stretch_xx = smajor*cosS2 + sminor*sinS2;
   stretch_xy = stretch_yx = (smajor - sminor)*sinScosS;
   stretch_yy = smajor*sinS2 + sminor*cosS2;
     
   swap = (orientation[0]!=orientation[2]);
   ewflip = (orientation[1]=='W'); nsflip = (orientation[0]=='N');
   orient_xx = orient_yy = !swap; orient_xy = orient_yx = swap; 
   if(ewflip) {orient_xx=-orient_xx; orient_yx=-orient_yx;}
   if(nsflip) {orient_xy=-orient_xy; orient_yy=-orient_yy;}
     
   combo_xx = rsun*(orient_xx*stretch_xx+orient_xy*stretch_yx)/xscale;
   combo_xy = rsun*(orient_xx*stretch_xy+orient_xy*stretch_yy)/xscale;
   combo_yx = rsun*(orient_yx*stretch_xx+orient_yy*stretch_yx)/yscale;
   combo_yy = rsun*(orient_yx*stretch_xy+orient_yy*stretch_yy)/yscale;
     
   sinB0 = sin(BZero);
   cosB0 = sqrt(1.0 - sinB0*sinB0);/* |B0| <= 7.25 degrees 		*/
   sinP = sin(P);			/* -PI <= P <= PI			*/
   cosP = cos(P);
   Rmax2 = Rmax * Rmax;
     
   for (col = 0; col < cols; col++) {
      savedDeltaL[col] = (L = Ldelta*col + Lmin - Ladjust); /* delta in radians between col and CM */
      savedsinL[col] = (sinL = sin(L));
      savedcosL[col] = sqrt(1.0 - sinL*sinL);
   }
     
   /* do the "remap" */
     
   for (row = 0; row < rows; row++) {
      rowoffset = row*cols;
      sinB = sinBdelta*(sinBbase + row);
      sinB2 = sinB*sinB;
      cosB = sqrt(1.0 - sinB2);
      sB0sB = sinB0*sinB;
      sB0cB = sinB0*cosB;
      cB0sB = cosB0*sinB;
      cB0cB = cosB0*cosB;
      omegaLat = diffrotA + diffrotB * sinB2 + diffrotC * sinB2 * sinB2;

      /* fill in table of sidereal rotation rate vs. row */
      if (rRates && row < size)
      {
	 rRates[row].lat = asin(sinB); /* radians */
	 rRates[row].r =  14.562 + (diffrotB * sinB2) + (diffrotC * sinB2 * sinB2);
      }

      v_rotation = v_equator*(1.0+sinB2*(rotation_coef_a2+sinB2*rotation_coef_a4));
	  
      for (col = 0; col < cols; col++) {
	       
	 if (carrStretch == 0)
	 {
	    sinL = savedsinL[col]; 
	    cosL = savedcosL[col]; 
	 }
	 else
	 {
	    /* savedDeltaL[col] is longitude delta (from CM) in radians. 
	     * NOTE: This is the ACTUAL (with precision) difference
	     * between CM and the current column. 
	     */

	    /* savedDeltaL[midCol] is always (-Ladjust) */
	    deltaLAdj = savedDeltaL[col] * omegaLat / kOmegaCarr;
	    sinL = sin(deltaLAdj);
	    cosL = cos(deltaLAdj);
	 }
	       
	 sinp_sinrho = sinL*cosB;
	 cosp_sinrho = cB0sB - sB0cB*cosL;
	 cosrho      = sB0sB + cB0cB*cosL;
	 rp = 1.0 / (1.0 - S * cosrho);
	 x =  rp * (sinp_sinrho*cosP - cosp_sinrho*sinP);
	 y =  rp * (cosp_sinrho*cosP + sinp_sinrho*sinP);
	      
	 /* Let caller decide what to do with values outside radius*/
	 if ((x*x + y*y) >= Rmax2) 
	 {
	    *(U + rowoffset + col) = (NaN_beyond_rmax) ? DRMS_MISSING_FLOAT : (float)0.0;
	    continue;
	 }
	       
	 xtmp = combo_xx*x + combo_xy*y;
	 ytmp = combo_yx*x + combo_yy*y;
	 x = xtmp + x0;
	 y = ytmp + y0;
	       

	 Distort(x, y, rsun, +1, &x, &y, distpars);
	       
	 /* While this code is great in theory it is slow
	    u = vsign*interpolate (V,xpixels,ypixels,x,y); 
	 */
	       
	 if (interpolation == kLIBASTRO_InterCubic) 
	 {
	    u = (float)(vsign * Ccintd (VD,xpixels,ypixels,x,y));
	 }
	 else if (interpolation == kLIBASTRO_InterBilinear) 
	 {
	    u = (float)(vsign * Linintd (VD,xpixels,ypixels,x,y));
	 }
	       
	 if (mag_correction == kLIBASTRO_MCORLevel1)	/* line-of-sight correction */
	   u = (float)(u * (sB0sB+cB0cB) / cosrho);
	       
	 if (velocity_correction > kLIBASTRO_VCORSignOnly) 
	 {
	    mu = 1.0 - cosrho;
	    v_limbshift = limbshift_coef_a0 + mu*(limbshift_coef_a1 + mu*limbshift_coef_a2);
		    
	    if (velocity_correction == kLIBASTRO_VCORLevel1) rp=1.0;
	    u = (float)((u + v_limbshift)/rp + v_rotation*cosB*sinL*cosB0 - (sB0cB - cosL*cB0sB)*v_lat
	      + obs_vr/rp - S*(obs_vw*sinp_sinrho + obs_vn*cosp_sinrho));
		    
	    if (velocity_correction == kLIBASTRO_VCORLevel2) u = (float)(cosB*cosL*u/(cosrho-S));
	 }
	       
	 *(U + rowoffset + col) = u;
      }
   }
   free(VD);
   return kLIBASTRO_Success;
}
