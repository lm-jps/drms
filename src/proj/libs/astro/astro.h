/* procFDSData.h */

#ifndef _LIBASTRO_H
#define _LIBASTRO_H

#include "drms.h"
#include "cmdparams.h"

#define kALLDATAMISSING         0

typedef enum 
{
   kLIBASTRO_InterBilinear = 0,
   kLIBASTRO_InterCubic = 1,
} LIBASTRO_Interpolation_t;

typedef enum
{
   kLIBASTRO_Success = 0,
   kLIBASTRO_BadDimensionality,
   kLIBASTRO_BadData,
   kLIBASTRO_CouldntCreateData,
   kLIBASTRO_DimensionMismatch,
   kLIBASTRO_CantDoOddNumLats,
   kLIBASTRO_UnsupportedInterp,
   kLIBASTRO_UnsupportedMCOR,
   kLIBASTRO_UnsupportedVCOR,
   kLIBASTRO_InconsistentConstraints
} LIBASTRO_Error_t;

typedef enum
{
   kLIBASTRO_MCORLevel0,
   kLIBASTRO_MCORLevel1
} LIBASTRO_MCOR_t;

typedef enum
{
   kLIBASTRO_VCORSignOnly,
   kLIBASTRO_VCORLevel1,
   kLIBASTRO_VCORLevel2
} LIBASTRO_VCOR_t;


typedef struct LIBASTRO_Dist_Struct 
{
  int disttype;
  double scale; /* Image scale relative to FD. 5 for vw. */
  double xc, yc; /* Nominal image center in pixels. */
  double cdist; /* Cubic distortion constant for FD images */
  double feff, alpha, cosbeta, sinbeta; /* Tilt constants for FD. */
} LIBASTRO_Dist_t;

typedef struct LIBASTRO_RotRate_struct
{
  float lat;
  float r;
} LIBASTRO_RotRate_t;


int ConvAndInterpFDS(DRMS_Env_t *drmsEnv, char *seriesName, char *dateRange);
int SetDistort(CmdParams_t *params, 
	       char *distortP,
	       char *cubicP,
	       char *tiltaP,
	       char *tiltbP,
	       char *tiltfeffP,
	       LIBASTRO_Dist_t *dOut);
int Obs2helio(
	       float	*V,		/* input velocity array				*/
	       /* assumed declared V[ypixels][xpixels]		*/
	       float	*U,		/* output projected array			*/
	       int	xpixels,	/* x width of input array			*/
	       int	ypixels,	/* y width of input array			*/
	       double   x0, 	   	/* x pixel address of disk center		*/
	       double   y0, 	   	/* y pixel address of disk center		*/ 	  
	       double   BZero,          /* heliographic latitude of disk center         */
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
	       int      carrStretch,
	       const LIBASTRO_Dist_t *distpars,
	       float   diffrotA,
	       float   diffrotB,
	       float   diffrotC,
	       LIBASTRO_RotRate_t *rRates, /* rotation rates passed by reference*/
	       int        size);

float Ccint2(float *f, int nx, int ny, double x, double y);
double Ccint2d(double *f, int nx, int ny, double x, double y);
float Linint2(float *f, int nx, int ny, double x, double y);
double Linint2d(double *f, int nx, int ny, double x, double y);
int Regrid(DRMS_Array_t **dataArr, int *new_length, LIBASTRO_Interpolation_t scheme);
float Imaginterp(DRMS_Segment_t *img, double x, double y);

#endif // _DRMS_LIBASTRO_H

