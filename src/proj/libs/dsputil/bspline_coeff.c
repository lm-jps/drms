/*****************************************************************************
 *      Date: January 29, 2002
 *            January  6, 2004: 
 *            Modified by Rasmus Munk Larsen, rmunk@quake.stanford.edu,
 *            to add a single precision version and to improve speed.                         
 *----------------------------------------------------------------------------
 *      This C program is based on the following three papers:
 *              [1]     M. Unser,
 *                      "Splines: A Perfect Fit for Signal and Image Processing,"
 *                      IEEE Signal Processing Magazine, vol. 16, no. 6, pp. 22-38,
 *                      November 1999.
 *              [2]     M. Unser, A. Aldroubi and M. Eden,
 *                      "B-Spline Signal Processing: Part I--Theory,"
 *                      IEEE Transactions on Signal Processing, vol. 41, no. 2, pp. 821-832,
 *                      February 1993.
 *              [3]     M. Unser, A. Aldroubi and M. Eden,
 *                      "B-Spline Signal Processing: Part II--Efficient Design and Applications,"
 *                      IEEE Transactions on Signal Processing, vol. 41, no. 2, pp. 834-848,
 *                      February 1993.
 *----------------------------------------------------------------------------
 *      EPFL/STI/IOA/BIG
 *      Philippe Thevenaz
 *      Bldg. BM-Ecublens 4.137
 *      CH-1015 Lausanne
 *----------------------------------------------------------------------------
 *      phone (CET):    +41(21)693.51.61
 *      fax:                    +41(21)693.37.01
 *      RFC-822:                philippe.thevenaz@epfl.ch
 *      X-400:                  /C=ch/A=400net/P=switch/O=epfl/S=thevenaz/G=philippe/
 *      URL:                    http://bigwww.epfl.ch/
 *----------------------------------------------------------------------------
 *      This file is best viewed with 4-space tabs (the bars below should be aligned)
 *      |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |
 *  |...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|
 ****************************************************************************/
/*****************************************************************************
 *      System includes
 ****************************************************************************/
#include        <float.h>
#include        <math.h>
#include        <stddef.h>
#include        <stdio.h>
#include        <stdlib.h>
/*****************************************************************************
 *      Other includes
 ****************************************************************************/
#include        "bspline_coeff.h"

/* Size of transposition buffer for single and double precision. */
#define BLKSZ_FLT 18
#define BLKSZ_DBL 8

#define HORIZON (18)
 
/* Single precision version */

/*****************************************************************************
 *      Declaration of static procedures
 ****************************************************************************/
/*--------------------------------------------------------------------------*/
static void             fConvertToInterpolationCoefficients
(
 float  c[],            /* input samples --> output coefficients */
 long   DataLength,     /* number of samples or coefficients */
 float  z[],            /* poles */
 long   NbPoles,        /* number of poles */
 float  Tolerance       /* admissible relative error */
 );
/*--------------------------------------------------------------------------*/
static inline float    fInitialCausalCoefficient
(
 float  c[],            /* coefficients */
 long   DataLength,     /* number of coefficients */
 float  z,                      /* actual pole */
 float  Tolerance       /* admissible relative error */
 );
/*****************************************************************************
 *      Definition of static procedures
 ****************************************************************************/
/*--------------------------------------------------------------------------*/
static void             fConvertToInterpolationCoefficients
(
 float  c[],            /* input samples --> output coefficients */
 long   DataLength,     /* number of samples or coefficients */
 float  z[],            /* poles */
 long   NbPoles,        /* number of poles */
 float  Tolerance       /* admissible relative error */
 )
{ /* begin ConvertToInterpolationCoefficients */
  float Lambda = 1.0f;
  long  n, k;
  /* special case required by mirror boundaries */
  if (DataLength == 1L) {
    return;
  }
  /* compute the overall gain */
  for (k = 0L; k < NbPoles; k++) {
    Lambda = Lambda * (1.0f - z[k]) * (1.0f - 1.0f / z[k]);
  }
  /* apply the gain */
  for (n = 0L; n < DataLength; n++) {
    c[n] *= Lambda;
  }
  
  /* loop over all poles */
  for (k = 0L; k < NbPoles; k++) {
    /* causal initialization */
    c[0] = fInitialCausalCoefficient(c, DataLength, z[k], Tolerance);
    /* causal recursion */
    for (n = 1L; n < DataLength; n++) {
      c[n] += z[k] * c[n - 1L];
    }
    /* anticausal initialization */
    c[DataLength - 1L] = (z[k] / (z[k] * z[k] - 1.0f)) * 
                         (z[k] * c[DataLength - 2L] + c[DataLength - 1L]);
    /* anticausal recursion */
    for (n = DataLength - 2L; 0 <= n; n--) {
      c[n] = z[k] * (c[n + 1L] - c[n]);
    }
  }
} /* end ConvertToInterpolationCoefficients */
/*--------------------------------------------------------------------------*/
static inline float    fInitialCausalCoefficient
(
 float  c[],            /* coefficients */
 long   DataLength,     /* number of coefficients */
 float  z,                      /* actual pole */
 float  Tolerance       /* admissible relative error */
 )
{ /* begin InitialCausalCoefficient */
  float Sum, zn, z2n, iz;
  long  n, Horizon;
  /* this initialization corresponds to mirror boundaries */
  Horizon = DataLength;
  if (Tolerance > 0.0f) {
    Horizon = (long)ceilf(logf(Tolerance) / logf(fabsf(z)));
  }
  //  printf("Datalength = %d, Horizon = %d\n",DataLength, Horizon);
  if (Horizon < DataLength) {
    /* accelerated loop */
    Sum = 0.0f;
    for (n = Horizon-1; n >= 0; n--) 
      Sum = z * Sum + c[n];
    return(Sum);
  }
  else {
    /* full loop */
    zn = z;
    iz = 1.0f / z;
    z2n = powf(z, (float)(DataLength - 1L));
    Sum = c[0] + z2n * c[DataLength - 1L];
    z2n *= z2n * iz;
    for (n = 1L; n <= DataLength - 2L; n++) {
      Sum += (zn + z2n) * c[n];
      zn *= z;
      z2n *= iz;
    }
    return(Sum / (1.0f - zn * zn));
  }
} /* end InitialCausalCoefficient */
/*--------------------------------------------------------------------------*/
/*****************************************************************************
 *      Definition of extern procedures
 ****************************************************************************/
/*--------------------------------------------------------------------------*/
extern int              fSamplesToCoefficients
(
 float  *Image,         /* in-place processing */
 long   Width,          /* width of the image */
 long   Height,         /* height of the image */
 long   SplineDegree/* degree of the spline model */
 )
{ /* begin SamplesToCoefficients */
  float *Line;
  float Pole[2];
  long  NbPoles;
  long  x, y,xb,xmax;
  /* recover the poles from a lookup table */
  switch (SplineDegree) {
  case 2L:
    NbPoles = 1L;
    Pole[0] = sqrtf(8.0f) - 3.0f;
    break;
  case 3L:
    NbPoles = 1L;
    Pole[0] = sqrtf(3.0f) - 2.0f;
    break;
  case 4L:
    NbPoles = 2L;
    Pole[0] = sqrtf(664.0f - sqrtf(438976.0f)) + sqrtf(304.0f) - 19.0f;
    Pole[1] = sqrtf(664.0f + sqrtf(438976.0f)) - sqrtf(304.0f) - 19.0f;
    break;
  case 5L:
    NbPoles = 2L;
    Pole[0] = sqrtf(135.0f / 2.0f - sqrtf(17745.0f / 4.0f)) + 
      sqrtf(105.0f / 4.0f) - 13.0f / 2.0f;
    Pole[1] = sqrtf(135.0f / 2.0f + sqrtf(17745.0f / 4.0f)) - 
      sqrtf(105.0f / 4.0f) - 13.0f / 2.0f;
    break;
  default:
    printf("Invalid spline degree\n");
    return(1);
  }
  /* convert the image samples into interpolation coefficients */
  /* in-place separable process, along x */
  for (y = 0L; y < Height; y++) {
    fConvertToInterpolationCoefficients(&Image[y*Width], Width, Pole, NbPoles, 
                                       FLT_EPSILON);
  }
  /* in-place separable process, along y */

  Line = (float *) malloc((size_t)(BLKSZ_FLT*Height * (long)sizeof(float)));
  if (Line == (float *)NULL) {
    printf("Workspace allocation failed\n");
    return(1);
  }

  for (xb = 0L; xb<Width; xb += BLKSZ_FLT)
  {
    xmax = (xb+BLKSZ_FLT>Width?Width:xb+BLKSZ_FLT);        
    for (x = xb; x < xmax; x++)  
    {
      for (y = 0L; y < Height; y++) 
        Line[y+(x-xb)*Height] = Image[y*Width+x];
      fConvertToInterpolationCoefficients(&Line[(x-xb)*Height], Height, Pole, 
                                         NbPoles, FLT_EPSILON);
    }

    for (y = 0L; y < Height; y++) 
      for (x = xb; x < xmax; x++) 
        Image[y*Width+x] = Line[y+(x-xb)*Height];
  }
  free(Line);
  return(0);
} /* end SamplesToCoefficients */





/* Double precision version */

/*****************************************************************************
 *      Declaration of static procedures
 ****************************************************************************/
/*--------------------------------------------------------------------------*/
static void             dConvertToInterpolationCoefficients
(
 double  c[],            /* input samples --> output coefficients */
 long   DataLength,     /* number of samples or coefficients */
 double  z[],            /* poles */
 long   NbPoles,        /* number of poles */
 double  Tolerance       /* admissible relative error */
 );
/*--------------------------------------------------------------------------*/
static inline double    dInitialCausalCoefficient
(
 double  c[],            /* coefficients */
 long   DataLength,     /* number of coefficients */
 double  z,                      /* actual pole */
 double  Tolerance       /* admissible relative error */
 );
/*--------------------------------------------------------------------------*/
static inline double    dInitialAntiCausalCoefficient
(
 double  c[],            /* coefficients */
 long   DataLength,     /* number of samples or coefficients */
 double  z               /* actual pole */
 );
/*****************************************************************************
 *      Definition of static procedures
 ****************************************************************************/
/*--------------------------------------------------------------------------*/
static void             dConvertToInterpolationCoefficients
(
 double  c[],            /* input samples --> output coefficients */
 long   DataLength,     /* number of samples or coefficients */
 double  z[],            /* poles */
 long   NbPoles,        /* number of poles */
 double  Tolerance       /* admissible relative error */
 )
{ /* begin ConvertToInterpolationCoefficients */
  double Lambda = 1.0;
  long  n, k;
  /* special case required by mirror boundaries */
  if (DataLength == 1L) {
    return;
  }
  /* compute the overall gain */
  for (k = 0L; k < NbPoles; k++) {
    Lambda = Lambda * (1.0 - z[k]) * (1.0 - 1.0 / z[k]);
  }
  /* apply the gain */
  for (n = 0L; n < DataLength; n++) {
    c[n] *= Lambda;
  }
  /* loop over all poles */
  for (k = 0L; k < NbPoles; k++) {
    /* causal initialization */
    c[0] = dInitialCausalCoefficient(c, DataLength, z[k], Tolerance);
    /* causal recursion */
    for (n = 1L; n < DataLength; n++) {
      c[n] += z[k] * c[n - 1L];
    }
    /* anticausal initialization */
    c[DataLength - 1L] = dInitialAntiCausalCoefficient(c, DataLength, z[k]);
    /* anticausal recursion */
    for (n = DataLength - 2L; 0 <= n; n--) {
      c[n] = z[k] * (c[n + 1L] - c[n]);
    }
  }
} /* end ConvertToInterpolationCoefficients */
/*--------------------------------------------------------------------------*/
static double    dInitialCausalCoefficient
(
 double  c[],            /* coefficients */
 long   DataLength,     /* number of coefficients */
 double  z,                      /* actual pole */
 double  Tolerance       /* admissible relative error */
 )
{ /* begin InitialCausalCoefficient */
  double Sum, zn, z2n, iz;
  long  n, Horizon;
  /* this initialization corresponds to mirror boundaries */
  Horizon = DataLength;
  if (Tolerance > 0.0) {
    Horizon = (long)ceil(log(Tolerance) / log(fabs(z)));
  }
  if (Horizon < DataLength) {
    /* accelerated loop */
    Sum = 0.0;
    for (n = Horizon-1; n >= 0; n--) 
      Sum = z * Sum + c[n];
    return(Sum);
  }
  else {
    /* full loop */
    zn = z;
    iz = 1.0 / z;
    z2n = pow(z, (double)(DataLength - 1L));
    Sum = c[0] + z2n * c[DataLength - 1L];
    z2n *= z2n * iz;
    for (n = 1L; n <= DataLength - 2L; n++) {
      Sum += (zn + z2n) * c[n];
      zn *= z;
      z2n *= iz;
    }
    return(Sum / (1.0 - zn * zn));
  }
} /* end InitialCausalCoefficient */
/*--------------------------------------------------------------------------*/
static double    dInitialAntiCausalCoefficient
(
 double  c[],            /* coefficients */
 long   DataLength,     /* number of samples or coefficients */
 double  z               /* actual pole */
 )
{ /* begin InitialAntiCausalCoefficient */
  /* this initialization corresponds to mirror boundaries */
  return((z / (z * z - 1.0)) * (z * c[DataLength - 2L] + c[DataLength - 1L]));
} /* end InitialAntiCausalCoefficient */
/*--------------------------------------------------------------------------*/
/*****************************************************************************
 *      Definition of extern procedures
 ****************************************************************************/
/*--------------------------------------------------------------------------*/
extern int              dSamplesToCoefficients
(
 double  *Image,         /* in-place processing */
 long   Width,          /* width of the image */
 long   Height,         /* height of the image */
 long   SplineDegree/* degree of the spline model */
 )
{ /* begin SamplesToCoefficients */
  double *Line;
  double Pole[2];
  long  NbPoles;
  long  x, y,xb,xmax;
  /* recover the poles from a lookup table */
  switch (SplineDegree) {
  case 2L:
    NbPoles = 1L;
    Pole[0] = sqrt(8.0) - 3.0;
    break;
  case 3L:
    NbPoles = 1L;
    Pole[0] = sqrt(3.0) - 2.0;
    break;
  case 4L:
    NbPoles = 2L;
    Pole[0] = sqrt(664.0 - sqrt(438976.0)) + sqrt(304.0) - 19.0;
    Pole[1] = sqrt(664.0 + sqrt(438976.0)) - sqrt(304.0) - 19.0;
    break;
  case 5L:
    NbPoles = 2L;
    Pole[0] = sqrt(135.0 / 2.0 - sqrt(17745.0 / 4.0)) + 
      sqrt(105.0 / 4.0) - 13.0 / 2.0;
    Pole[1] = sqrt(135.0 / 2.0 + sqrt(17745.0 / 4.0)) - 
      sqrt(105.0 / 4.0) - 13.0 / 2.0;
    break;
  default:
    printf("Invalid spline degree\n");
    return(1);
  }
  /* convert the image samples into interpolation coefficients */
  /* in-place separable process, along x */
  for (y = 0L; y < Height; y++) {
    dConvertToInterpolationCoefficients(&Image[y*Width], Width, Pole, NbPoles, 
                                       DBL_EPSILON);
  }
  /* in-place separable process, along y */

  Line = (double *) malloc((size_t)(BLKSZ_DBL*Height * (long)sizeof(double)));
  if (Line == (double *)NULL) {
    printf("Workspace allocation failed\n");
    return(1);
  }

  for (xb = 0L; xb<Width; xb += BLKSZ_DBL)
  {
    xmax = (xb+BLKSZ_DBL>Width?Width:xb+BLKSZ_DBL);        
    for (x = xb; x < xmax; x++)  
    {
      for (y = 0L; y < Height; y++) 
        Line[y+(x-xb)*Height] = Image[y*Width+x];
      dConvertToInterpolationCoefficients(&Line[(x-xb)*Height], Height, Pole, 
                                         NbPoles, DBL_EPSILON);
    }

    for (y = 0L; y < Height; y++) 
      for (x = xb; x < xmax; x++) 
        Image[y*Width+x] = Line[y+(x-xb)*Height];
  }
  free(Line);
  return(0);
} /* end SamplesToCoefficients */
