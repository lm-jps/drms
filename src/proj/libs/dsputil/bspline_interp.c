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
#include        <math.h>
#include        <stddef.h>
#include        <stdio.h>
#include        <stdlib.h>
/*****************************************************************************
 *      Other includes
 ****************************************************************************/
#include        "bspline_interp.h"
 


/*****************************************************************************
 *      Definition of static procedures
 ****************************************************************************/
static inline void fComputeWeights(long SplineDegree, float w, float *Weight);
static inline void dComputeWeights(long SplineDegree, double w, double *Weight);
#define FLOOR(x) ((long)(x))



/*****************************************************************************
 *      Definition of extern procedures
 ****************************************************************************/
/*-------------------- Single precision version ----------------------------*/

extern float    fInterpolatedValue
(
 float  *Bcoeff,        /* input B-spline array of coefficients */
 long   Width,          /* width of the image */
 long   Height,         /* height of the image */
 float  x,                      /* x coordinate where to interpolate */
 float  y,                      /* y coordinate where to interpolate */
 long   SplineDegree/* degree of the spline model */
 )
{ /* begin InterpolatedValue */
  float *p;
  float xWeight[6], yWeight[6];
  float interpolated, w ;
  long  xIndex[6], yIndex[6];
  long  i, j, k, xi, yi;

  if (SplineDegree & 1L) {
    xi = (long) FLOOR(x);
    yi = (long) FLOOR(y);
  }
  else {
    xi = (long)FLOOR(x+0.5f);
    yi = (long)FLOOR(y+0.5f);
  }

  /* compute the interpolation weights */
  fComputeWeights(SplineDegree, x - (float)xi, xWeight);
  fComputeWeights(SplineDegree, y - (float)yi, yWeight);

  if (xi < SplineDegree || xi > (Width - SplineDegree) ||
      yi < SplineDegree || yi > (Height - SplineDegree) )
  {
    /* This is the slow (rare) branch, which is only used along the domain 
       boundaries, where the mirroring boundry condition must be
       applied. */
    long  Width2 = 2L * Width - 2L, Height2 = 2L * Height - 2L;


    /* compute the interpolation indexes */
    if (SplineDegree & 1L) {
      i = (long)FLOOR(x) - SplineDegree / 2L;
      j = (long)FLOOR(y) - SplineDegree / 2L;
      for (k = 0L; k <= SplineDegree; k++) {
        xIndex[k] = i++;
        yIndex[k] = j++;
      }
    }
    else {
      i = (long)FLOOR(x + 0.5f) - SplineDegree / 2L;
      j = (long)FLOOR(y + 0.5f) - SplineDegree / 2L;
      for (k = 0L; k <= SplineDegree; k++) {
        xIndex[k] = i++;
        yIndex[k] = j++;
      }
    }
    
    /* apply the mirror boundary conditions */
    for (k = 0L; k <= SplineDegree; k++) {
      xIndex[k] = (Width == 1L) ? (0L) : ((xIndex[k] < 0L) ?
                                          (-xIndex[k] - Width2 * ((-xIndex[k]) / Width2))
                                          : (xIndex[k] - Width2 * (xIndex[k] / Width2)));
      if (Width <= xIndex[k]) {
        xIndex[k] = Width2 - xIndex[k];
      }
      yIndex[k] = (Height == 1L) ? (0L) : ((yIndex[k] < 0L) ?
                                           (-yIndex[k] - Height2 * ((-yIndex[k]) / Height2))
                                           : (yIndex[k] - Height2 * (yIndex[k] / Height2)));
      if (Height <= yIndex[k]) {
        yIndex[k] = Height2 - yIndex[k];
      }
    }
    /* perform interpolation */
    interpolated = 0.0f;
    for (j = 0; j <= SplineDegree; j++) {
      p = Bcoeff + (ptrdiff_t)(yIndex[j] * Width);
      w = 0.0f;
      for (i = 0; i <= SplineDegree; i++) {
        w += xWeight[i] * p[xIndex[i]];
      }
      interpolated += yWeight[j] * w;
    }
    return(interpolated);  
  }
  else
  {
    /* This is the fast (frequent) branch, which is used in the interior of the domain. */

    /* perform interpolation */
    p = &Bcoeff[yi*Width + xi];
    switch(SplineDegree)
    {
    case 2L:
      return ((xWeight[0] * p[-Width-1] + xWeight[1] * p[-Width] + 
               xWeight[2] * p[-Width+1])
              * yWeight[0] +
              (xWeight[0] * p[      -1] + xWeight[1] * p[     0] + 
               xWeight[2] * p[       1])
              * yWeight[1] +
              (xWeight[0] * p[ Width-1] + xWeight[1] * p[ Width] + 
               xWeight[2] * p[ Width+1])
              * yWeight[2]);
      break;
    case 3L:
      return ((xWeight[0]*p[ -Width-1] + xWeight[1]*p[ -Width  ] + 
               xWeight[2]*p[ -Width+1] + xWeight[3]*p[ -Width+2])
              * yWeight[0] +
              (xWeight[0]*p[       -1] + xWeight[1]*p[        0] + 
               xWeight[2]*p[       +1] + xWeight[3]*p[       +2])
              * yWeight[1] +
              (xWeight[0]*p[  Width-1] + xWeight[1]*p[  Width  ] + 
               xWeight[2]*p[  Width+1] + xWeight[3]*p[  Width+2])
              * yWeight[2] +
              (xWeight[0]*p[2*Width-1] + xWeight[1]*p[2*Width  ] + 
               xWeight[2]*p[2*Width+1] + xWeight[3]*p[2*Width+2])
              * yWeight[3]);
      break;
    case 4L:
      return ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
               xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
               xWeight[4]*p[-2*Width+2]) 
              * yWeight[0] +
              (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
               xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
               xWeight[4]*p[  -Width+2]) 
              * yWeight[1] +
              (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
               xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
               xWeight[4]*p[         2]) 
              * yWeight[2] +
              (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
               xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
               xWeight[4]*p[   Width+2]) 
              * yWeight[3] +
              (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
               xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
               xWeight[4]*p[ 2*Width+2]) 
              * yWeight[4]);
      break;
    case 5L:
      return ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
               xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
               xWeight[4]*p[-2*Width+2] + xWeight[5]*p[-2*Width+3]) 
              * yWeight[0] +
              (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
               xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
               xWeight[4]*p[  -Width+2] + xWeight[5]*p[  -Width+3]) 
              * yWeight[1] +
              (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
               xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
               xWeight[4]*p[         2] + xWeight[5]*p[         3]) 
              * yWeight[2] +
              (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
               xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
               xWeight[4]*p[   Width+2] + xWeight[5]*p[   Width+3]) 
              * yWeight[3] +
              (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
               xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
               xWeight[4]*p[ 2*Width+2] + xWeight[5]*p[ 2*Width+3]) 
              * yWeight[4] +
              (xWeight[0]*p[ 3*Width-2] + xWeight[1]*p[ 3*Width-1] + 
               xWeight[2]*p[ 3*Width  ] + xWeight[3]*p[ 3*Width+1] + 
               xWeight[4]*p[ 3*Width+2] + xWeight[5]*p[ 3*Width+3]) 
              * yWeight[5]);
      break;
    default:
      return 0.0f;
    }
  }
} /* end InterpolatedValue */




extern void     fAffine
(
 float  *Bcoeff,        /* input B-spline array of coefficients */
 float  *Image,         /* output image */
 long   Width,          /* width of the image */
 long   Height,         /* height of the image */
 float a11,             /* (1,1) element in linear transformation matrix */
 float a12,             /* (1,2) element in linear transformation matrix */
 float a21,             /* (2,1) element in linear transformation matrix */
 float a22,             /* (2,2) element in linear transformation matrix */
 float xShift,          /* Horizontal shift */
 float yShift,          /* Vertical shift */
 long Masking,           /* Whether to mask pixels outside the original image */
 long   SplineDegree/* degree of the spline model */
 )
{ /* begin InterpolatedValue */
  float *p;
  float xWeight[6], yWeight[6];
  float interpolated, w;
  long  xIndex[6], yIndex[6];
  long  Width2 = 2L * Width - 2L, Height2 = 2L * Height - 2L;
  long  i, j, k, xi, yi, x1,y1;
  float x, y, x0,y0;

  if (SplineDegree < 2L || SplineDegree > 5L)
  {
    printf("Invalid spline degree: %ld\n", SplineDegree);
    return;
  }

  x0 = xShift;
  y0 = yShift;
  for (y1 = 0L; y1 < Height; y1++) 
  {
    //    x0 = a12 * (float)y1 + xShift;
    //    y0 = a22 * (float)y1 + yShift;
    x = x0;
    y = y0;
    for (x1 = 0L; x1 < Width; x1++) 
    {
      //      x = x0 + a11 * (float)x1;
      //y = y0 + a21 * (float)x1;
      if (Masking && ((x <= -0.5) || (((double)Width - 0.5) <= x)
                      || (y <= -0.5) || (((double)Height - 0.5) <= y)))
        Image[y1*Width + x1] = 0.0F;
      else
      { 
        if (SplineDegree & 1L) {
          xi = (long) (x);
          yi = (long) (y);
        }
        else {
          xi = (long)(x+0.5f);
          yi = (long)(y+0.5f);
        }

        /* compute the interpolation weights */
        fComputeWeights(SplineDegree, x - (float)xi, xWeight);
        fComputeWeights(SplineDegree, y - (float)yi, yWeight);

        if (xi < SplineDegree || xi > (Width - SplineDegree) ||
            yi < SplineDegree || yi > (Height - SplineDegree) ) {
          /* compute the interpolation indexes */
          if (SplineDegree & 1L) {
            i = (long)(x) - SplineDegree / 2L;
            j = (long)(y) - SplineDegree / 2L;
            for (k = 0L; k <= SplineDegree; k++) {
              xIndex[k] = i++;
              yIndex[k] = j++;
            }
          }
          else {
            i = (long)(x + 0.5f) - SplineDegree / 2L;
            j = (long)(y + 0.5f) - SplineDegree / 2L;
            for (k = 0L; k <= SplineDegree; k++) {
              xIndex[k] = i++;
              yIndex[k] = j++;
            }
          }
    

          /* apply the mirror boundary conditions */
          for (k = 0L; k <= SplineDegree; k++) {
            xIndex[k] = (Width == 1L) ? (0L) : ((xIndex[k] < 0L) ?
                                                (-xIndex[k] - Width2 * ((-xIndex[k]) / Width2))
                                                : (xIndex[k] - Width2 * (xIndex[k] / Width2)));
            if (Width <= xIndex[k]) {
              xIndex[k] = Width2 - xIndex[k];
            }
            yIndex[k] = (Height == 1L) ? (0L) : ((yIndex[k] < 0L) ?
                                                 (-yIndex[k] - Height2 * ((-yIndex[k]) / Height2))
                                                 : (yIndex[k] - Height2 * (yIndex[k] / Height2)));
            if (Height <= yIndex[k]) {
              yIndex[k] = Height2 - yIndex[k];
            }
          }
          /* perform interpolation */
          interpolated = 0.0f;
          for (j = 0; j <= SplineDegree; j++) {
            p = Bcoeff + (ptrdiff_t)(yIndex[j] * Width);
            w = 0.0f;
            for (i = 0; i <= SplineDegree; i++) {
              w += xWeight[i] * p[xIndex[i]];
            }
            interpolated += yWeight[j] * w;
          }
          Image[y1*Width + x1] = interpolated;
        }
        else {
          /* perform interpolation */
          p = &Bcoeff[yi*Width + xi];
          switch(SplineDegree)
          {
          case 2L:
            Image[y1*Width + x1] = ((xWeight[0] * p[-Width-1] + xWeight[1] * p[-Width] + 
                                     xWeight[2] * p[-Width+1])
                                    * yWeight[0] +
                                    (xWeight[0] * p[      -1] + xWeight[1] * p[     0] + 
                                     xWeight[2] * p[       1])
                                    * yWeight[1] +
                                    (xWeight[0] * p[ Width-1] + xWeight[1] * p[ Width] + 
                                     xWeight[2] * p[ Width+1])
                                    * yWeight[2]);
            break;
          case 3L:
            Image[y1*Width + x1] = ((xWeight[0]*p[ -Width-1] + xWeight[1]*p[ -Width  ] + 
                                     xWeight[2]*p[ -Width+1] + xWeight[3]*p[ -Width+2])
                                    * yWeight[0] +
                                    (xWeight[0]*p[       -1] + xWeight[1]*p[        0] + 
                                     xWeight[2]*p[       +1] + xWeight[3]*p[       +2])
                                    * yWeight[1] +
                                    (xWeight[0]*p[  Width-1] + xWeight[1]*p[  Width  ] + 
                                     xWeight[2]*p[  Width+1] + xWeight[3]*p[  Width+2])
                                    * yWeight[2] +
                                    (xWeight[0]*p[2*Width-1] + xWeight[1]*p[2*Width  ] + 
                                     xWeight[2]*p[2*Width+1] + xWeight[3]*p[2*Width+2])
                                    * yWeight[3]);
            break;
          case 4L:
            Image[y1*Width + x1] = ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
                                     xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
                                     xWeight[4]*p[-2*Width+2]) 
                                    * yWeight[0] +
                                    (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
                                     xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
                                     xWeight[4]*p[  -Width+2]) 
                                    * yWeight[1] +
                                    (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
                                     xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
                                     xWeight[4]*p[         2]) 
                                    * yWeight[2] +
                                    (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
                                     xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
                                     xWeight[4]*p[   Width+2]) 
                                    * yWeight[3] +
                                    (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
                                     xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
                                     xWeight[4]*p[ 2*Width+2]) 
                                    * yWeight[4]);
            break;
          case 5L:
            Image[y1*Width + x1] = ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
                                     xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
                                     xWeight[4]*p[-2*Width+2] + xWeight[5]*p[-2*Width+3]) 
                                    * yWeight[0] +
                                    (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
                                     xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
                                     xWeight[4]*p[  -Width+2] + xWeight[5]*p[  -Width+3]) 
                                    * yWeight[1] +
                                    (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
                                     xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
                                     xWeight[4]*p[         2] + xWeight[5]*p[         3]) 
                                    * yWeight[2] +
                                    (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
                                     xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
                                     xWeight[4]*p[   Width+2] + xWeight[5]*p[   Width+3]) 
                                    * yWeight[3] +
                                    (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
                                     xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
                                     xWeight[4]*p[ 2*Width+2] + xWeight[5]*p[ 2*Width+3]) 
                                    * yWeight[4] +
                                    (xWeight[0]*p[ 3*Width-2] + xWeight[1]*p[ 3*Width-1] + 
                                     xWeight[2]*p[ 3*Width  ] + xWeight[3]*p[ 3*Width+1] + 
                                     xWeight[4]*p[ 3*Width+2] + xWeight[5]*p[ 3*Width+3]) 
                                    * yWeight[5]);
            break;
          }
        }
      } /* end InterpolatedValue */
      x += a11;
      y += a21;
    }
    x0 += a12;
    y0 += a22;    
  }
}



static inline void fComputeWeights(long SplineDegree, float w, float *Weight)
{
  float t,t0,t1,w2,w4;

  switch (SplineDegree) {
  case 2L:
    Weight[1] = 3.0f / 4.0f - w * w;
    Weight[2] = (1.0f / 2.0f) * (w - Weight[1] + 1.0f);
    Weight[0] = 1.0f - Weight[1] - Weight[2];
    break;
  case 3L:
    Weight[3] = (1.0f / 6.0f) * w * w * w;
    Weight[0] = (1.0f / 6.0f) + (1.0f / 2.0f) * w * (w - 1.0f) - Weight[3];
    Weight[2] = w + Weight[0] - 2.0f * Weight[3];
    Weight[1] = 1.0f - Weight[0] - Weight[2] - Weight[3];
    break;
  case 4L:
    w2 = w * w;
    t = (1.0f / 6.0f) * w2;
    Weight[0] = 1.0f / 2.0f - w;
    Weight[0] *= Weight[0];
    Weight[0] *= (1.0f / 24.0f) * Weight[0];
    t0 = w * (t - 11.0f / 24.0f);
    t1 = 19.0f / 96.0f + w2 * (1.0f / 4.0f - t);
    Weight[1] = t1 + t0;
    Weight[3] = t1 - t0;
    Weight[4] = Weight[0] + t0 + (1.0f / 2.0f) * w;
    Weight[2] = 1.0f - Weight[0] - Weight[1] - Weight[3] - Weight[4];    
    break;
  case 5L:
    w2 = w * w;
    Weight[5] = (1.0f / 120.0f) * w * w2 * w2;
    w2 -= w;
    w4 = w2 * w2;
    w -= 1.0f / 2.0f;
    t = w2 * (w2 - 3.0f);
    Weight[0] = (1.0f / 24.0f) * (1.0f / 5.0f + w2 + w4) - Weight[5];
    t0 = (1.0f / 24.0f) * (w2 * (w2 - 5.0f) + 46.0f / 5.0f);
    t1 = (-1.0f / 12.0f) * w * (t + 4.0f);
    Weight[2] = t0 + t1;
    Weight[3] = t0 - t1;
    t0 = (1.0f / 16.0f) * (9.0f / 5.0f - t);
    t1 = (1.0f / 24.0f) * w * (w4 - w2 - 5.0f);
    Weight[1] = t0 + t1;
    Weight[4] = t0 - t1;
  }
}    




/*-------------------- Double precision version ----------------------------*/

extern double    dInterpolatedValue
(
 double  *Bcoeff,        /* input B-spline array of coefficients */
 long   Width,          /* width of the image */
 long   Height,         /* height of the image */
 double  x,                      /* x coordinate where to interpolate */
 double  y,                      /* y coordinate where to interpolate */
 long   SplineDegree/* degree of the spline model */
 )
{ /* begin InterpolatedValue */
  double *p;
  double xWeight[6], yWeight[6];
  double interpolated, w ;
  long  xIndex[6], yIndex[6];
  long  i, j, k, xi, yi;

  if (SplineDegree & 1L) {
    xi = (long) FLOOR(x);
    yi = (long) FLOOR(y);
  }
  else {
    xi = (long)FLOOR(x+0.5);
    yi = (long)FLOOR(y+0.5);
  }

  /* compute the interpolation weights */
  dComputeWeights(SplineDegree, x - (double)xi, xWeight);
  dComputeWeights(SplineDegree, y - (double)yi, yWeight);

  if (xi < SplineDegree || xi > (Width - SplineDegree) ||
      yi < SplineDegree || yi > (Height - SplineDegree) )
  {
    long  Width2 = 2L * Width - 2L, Height2 = 2L * Height - 2L;
    /* This is the slow (rare) branch, which is only used along the domain 
       boundaries, where the mirroring boundry condition must be
       applied. */


    /* compute the interpolation indexes */
    if (SplineDegree & 1L) {
      i = (long)FLOOR(x) - SplineDegree / 2L;
      j = (long)FLOOR(y) - SplineDegree / 2L;
      for (k = 0L; k <= SplineDegree; k++) {
        xIndex[k] = i++;
        yIndex[k] = j++;
      }
    }
    else {
      i = (long)FLOOR(x + 0.5) - SplineDegree / 2L;
      j = (long)FLOOR(y + 0.5) - SplineDegree / 2L;
      for (k = 0L; k <= SplineDegree; k++) {
        xIndex[k] = i++;
        yIndex[k] = j++;
      }
    }
    
    /* apply the mirror boundary conditions */
    for (k = 0L; k <= SplineDegree; k++) {
      xIndex[k] = (Width == 1L) ? (0L) : ((xIndex[k] < 0L) ?
                                          (-xIndex[k] - Width2 * ((-xIndex[k]) / Width2))
                                          : (xIndex[k] - Width2 * (xIndex[k] / Width2)));
      if (Width <= xIndex[k]) {
        xIndex[k] = Width2 - xIndex[k];
      }
      yIndex[k] = (Height == 1L) ? (0L) : ((yIndex[k] < 0L) ?
                                           (-yIndex[k] - Height2 * ((-yIndex[k]) / Height2))
                                           : (yIndex[k] - Height2 * (yIndex[k] / Height2)));
      if (Height <= yIndex[k]) {
        yIndex[k] = Height2 - yIndex[k];
      }
    }
    /* perform interpolation */
    interpolated = 0.0;
    for (j = 0; j <= SplineDegree; j++) {
      p = Bcoeff + (ptrdiff_t)(yIndex[j] * Width);
      w = 0.0f;
      for (i = 0; i <= SplineDegree; i++) {
        w += xWeight[i] * p[xIndex[i]];
      }
      interpolated += yWeight[j] * w;
    }
    return(interpolated);
  }
  else
  {
    /* This is the fast (frequent) branch, which is used in the interior of the domain. */

    /* perform interpolation */
    p = &Bcoeff[yi*Width + xi];
    switch(SplineDegree)
    {
    case 2L:
      return ((xWeight[0] * p[-Width-1] + xWeight[1] * p[-Width] + 
               xWeight[2] * p[-Width+1])
              * yWeight[0] +
              (xWeight[0] * p[      -1] + xWeight[1] * p[     0] + 
               xWeight[2] * p[       1])
              * yWeight[1] +
              (xWeight[0] * p[ Width-1] + xWeight[1] * p[ Width] + 
               xWeight[2] * p[ Width+1])
              * yWeight[2]);
      break;
    case 3L:
      return ((xWeight[0]*p[ -Width-1] + xWeight[1]*p[ -Width  ] + 
               xWeight[2]*p[ -Width+1] + xWeight[3]*p[ -Width+2])
              * yWeight[0] +
              (xWeight[0]*p[       -1] + xWeight[1]*p[        0] + 
               xWeight[2]*p[       +1] + xWeight[3]*p[       +2])
              * yWeight[1] +
              (xWeight[0]*p[  Width-1] + xWeight[1]*p[  Width  ] + 
               xWeight[2]*p[  Width+1] + xWeight[3]*p[  Width+2])
              * yWeight[2] +
              (xWeight[0]*p[2*Width-1] + xWeight[1]*p[2*Width  ] + 
               xWeight[2]*p[2*Width+1] + xWeight[3]*p[2*Width+2])
              * yWeight[3]);
      break;
    case 4L:
      return ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
               xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
               xWeight[4]*p[-2*Width+2]) 
              * yWeight[0] +
              (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
               xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
               xWeight[4]*p[  -Width+2]) 
              * yWeight[1] +
              (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
               xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
               xWeight[4]*p[         2]) 
              * yWeight[2] +
              (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
               xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
               xWeight[4]*p[   Width+2]) 
              * yWeight[3] +
              (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
               xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
               xWeight[4]*p[ 2*Width+2]) 
              * yWeight[4]);
      break;
    case 5L:
      return ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
               xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
               xWeight[4]*p[-2*Width+2] + xWeight[5]*p[-2*Width+3]) 
              * yWeight[0] +
              (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
               xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
               xWeight[4]*p[  -Width+2] + xWeight[5]*p[  -Width+3]) 
              * yWeight[1] +
              (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
               xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
               xWeight[4]*p[         2] + xWeight[5]*p[         3]) 
              * yWeight[2] +
              (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
               xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
               xWeight[4]*p[   Width+2] + xWeight[5]*p[   Width+3]) 
              * yWeight[3] +
              (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
               xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
               xWeight[4]*p[ 2*Width+2] + xWeight[5]*p[ 2*Width+3]) 
              * yWeight[4] +
              (xWeight[0]*p[ 3*Width-2] + xWeight[1]*p[ 3*Width-1] + 
               xWeight[2]*p[ 3*Width  ] + xWeight[3]*p[ 3*Width+1] + 
               xWeight[4]*p[ 3*Width+2] + xWeight[5]*p[ 3*Width+3]) 
              * yWeight[5]);
      break;
    default:
      return 0.0;
    }
  }
} /* end InterpolatedValue */




extern void     dAffine
(
 double  *Bcoeff,        /* input B-spline array of coefficients */
 double  *Image,         /* output image */
 long   Width,           /* width of the image */
 long   Height,          /* height of the image */
 double a11,             /* (1,1) element in linear transformation matrix */
 double a12,             /* (1,2) element in linear transformation matrix */
 double a21,             /* (2,1) element in linear transformation matrix */
 double a22,             /* (2,2) element in linear transformation matrix */
 double xShift,          /* Horizontal shift */
 double yShift,          /* Vertical shift */
 long Masking,           /* Whether to mask pixels outside the original image */
 long   SplineDegree     /* degree of the spline model */
 )
{ /* begin InterpolatedValue */
  double *p;
  double xWeight[6], yWeight[6];
  double interpolated, w;
  long  xIndex[6], yIndex[6];
  long  Width2 = 2L * Width - 2L, Height2 = 2L * Height - 2L;
  long  i, j, k, xi, yi, x1,y1;
  double x, y, x0,y0;

  if (SplineDegree < 2L || SplineDegree > 5L)
  {
    printf("Invalid spline degree: %ld\n", SplineDegree);
    return;
  }

  x0 = xShift;
  y0 = yShift;
  for (y1 = 0L; y1 < Height; y1++) 
  {
    x = x0;
    y = y0;
    for (x1 = 0L; x1 < Width; x1++) 
    {
      if (Masking && ((x <= -0.5) || (((double)Width - 0.5) <= x)
                      || (y <= -0.5) || (((double)Height - 0.5) <= y)))
        Image[y1*Width + x1] = 0.0F;
      else
      { 
      
        if (SplineDegree & 1L) {
          xi = FLOOR(x);
          yi = FLOOR(y);
        }
        else {
          xi = FLOOR(x+0.5);
          yi = FLOOR(y+0.5);
        }

        /* compute the interpolation weights */
        dComputeWeights(SplineDegree, x - (double)xi, xWeight);
        dComputeWeights(SplineDegree, y - (double)yi, yWeight);

        if (xi < SplineDegree || xi > (Width - SplineDegree) ||
            yi < SplineDegree || yi > (Height - SplineDegree) ) {
          /* compute the interpolation indexes */
          if (SplineDegree & 1L) {
            i = FLOOR(x) - SplineDegree / 2L;
            j = FLOOR(y) - SplineDegree / 2L;
            for (k = 0L; k <= SplineDegree; k++) {
              xIndex[k] = i++;
              yIndex[k] = j++;
            }
          }
          else {
            i = FLOOR(x + 0.5) - SplineDegree / 2L;
            j = FLOOR(y + 0.5) - SplineDegree / 2L;
            for (k = 0L; k <= SplineDegree; k++) {
              xIndex[k] = i++;
              yIndex[k] = j++;
            }
          }
    

          /* apply the mirror boundary conditions */
          for (k = 0L; k <= SplineDegree; k++) {
            xIndex[k] = (Width == 1L) ? (0L) : ((xIndex[k] < 0L) ?
                                                (-xIndex[k] - Width2 * ((-xIndex[k]) / Width2))
                                                : (xIndex[k] - Width2 * (xIndex[k] / Width2)));
            if (Width <= xIndex[k]) {
              xIndex[k] = Width2 - xIndex[k];
            }
            yIndex[k] = (Height == 1L) ? (0L) : ((yIndex[k] < 0L) ?
                                                 (-yIndex[k] - Height2 * ((-yIndex[k]) / Height2))
                                                 : (yIndex[k] - Height2 * (yIndex[k] / Height2)));
            if (Height <= yIndex[k]) {
              yIndex[k] = Height2 - yIndex[k];
            }
          }
          /* perform interpolation */
          interpolated = 0.0f;
          for (j = 0; j <= SplineDegree; j++) {
            p = Bcoeff + (ptrdiff_t)(yIndex[j] * Width);
            w = 0.0f;
            for (i = 0; i <= SplineDegree; i++) {
              w += xWeight[i] * p[xIndex[i]];
            }
            interpolated += yWeight[j] * w;
          }
          Image[y1*Width + x1] = interpolated;
        }
        else {
          /* perform interpolation */
          p = &Bcoeff[yi*Width + xi];
          switch(SplineDegree)
          {
          case 2L:
            Image[y1*Width + x1] = 
              ((xWeight[0] * p[-Width-1] + xWeight[1] * p[-Width] + 
                xWeight[2] * p[-Width+1])
               * yWeight[0] +
               (xWeight[0] * p[      -1] + xWeight[1] * p[     0] + 
                xWeight[2] * p[       1])
               * yWeight[1] +
               (xWeight[0] * p[ Width-1] + xWeight[1] * p[ Width] + 
                xWeight[2] * p[ Width+1])
               * yWeight[2]);
            break;
          case 3L:
            Image[y1*Width + x1] = 
              ((xWeight[0]*p[ -Width-1] + xWeight[1]*p[ -Width  ] + 
                xWeight[2]*p[ -Width+1] + xWeight[3]*p[ -Width+2])
               * yWeight[0] +
               (xWeight[0]*p[       -1] + xWeight[1]*p[        0] + 
                xWeight[2]*p[       +1] + xWeight[3]*p[       +2])
               * yWeight[1] +
               (xWeight[0]*p[  Width-1] + xWeight[1]*p[  Width  ] + 
                xWeight[2]*p[  Width+1] + xWeight[3]*p[  Width+2])
               * yWeight[2] +
               (xWeight[0]*p[2*Width-1] + xWeight[1]*p[2*Width  ] + 
                xWeight[2]*p[2*Width+1] + xWeight[3]*p[2*Width+2])
               * yWeight[3]);
            break;
          case 4L:
            Image[y1*Width + x1] = 
              ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
                xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
                xWeight[4]*p[-2*Width+2]) 
               * yWeight[0] +
               (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
                xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
                xWeight[4]*p[  -Width+2]) 
               * yWeight[1] +
               (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
                xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
                xWeight[4]*p[         2]) 
               * yWeight[2] +
               (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
                xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
                xWeight[4]*p[   Width+2]) 
               * yWeight[3] +
               (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
                xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
                xWeight[4]*p[ 2*Width+2]) 
               * yWeight[4]);
            break;
          case 5L:
            Image[y1*Width + x1] = 
              ((xWeight[0]*p[-2*Width-2] + xWeight[1]*p[-2*Width-1] + 
                xWeight[2]*p[-2*Width  ] + xWeight[3]*p[-2*Width+1] + 
                xWeight[4]*p[-2*Width+2] + xWeight[5]*p[-2*Width+3]) 
               * yWeight[0] +
               (xWeight[0]*p[  -Width-2] + xWeight[1]*p[  -Width-1] + 
                xWeight[2]*p[  -Width  ] + xWeight[3]*p[  -Width+1] + 
                xWeight[4]*p[  -Width+2] + xWeight[5]*p[  -Width+3]) 
               * yWeight[1] +
               (xWeight[0]*p[        -2] + xWeight[1]*p[        -1] + 
                xWeight[2]*p[         0] + xWeight[3]*p[         1] + 
                xWeight[4]*p[         2] + xWeight[5]*p[         3]) 
               * yWeight[2] +
               (xWeight[0]*p[   Width-2] + xWeight[1]*p[   Width-1] + 
                xWeight[2]*p[   Width  ] + xWeight[3]*p[   Width+1] + 
                xWeight[4]*p[   Width+2] + xWeight[5]*p[   Width+3]) 
               * yWeight[3] +
               (xWeight[0]*p[ 2*Width-2] + xWeight[1]*p[ 2*Width-1] + 
                xWeight[2]*p[ 2*Width  ] + xWeight[3]*p[ 2*Width+1] + 
                xWeight[4]*p[ 2*Width+2] + xWeight[5]*p[ 2*Width+3]) 
               * yWeight[4] +
               (xWeight[0]*p[ 3*Width-2] + xWeight[1]*p[ 3*Width-1] + 
                xWeight[2]*p[ 3*Width  ] + xWeight[3]*p[ 3*Width+1] + 
                xWeight[4]*p[ 3*Width+2] + xWeight[5]*p[ 3*Width+3]) 
               * yWeight[5]);
            break;
          }
        }
      } /* end InterpolatedValue */
      x += a11;
      y += a21;
    }
    x0 += a12;
    y0 += a22;    
  }
}


static inline void dComputeWeights(long SplineDegree, double w, double *Weight)
{
  double t,t0,t1,w2,w4;

  switch (SplineDegree) {
  case 2L:
    Weight[1] = 3.0 / 4.0 - w * w;
    Weight[2] = (1.0 / 2.0) * (w - Weight[1] + 1.0);
    Weight[0] = 1.0 - Weight[1] - Weight[2];
    break;
  case 3L:
    Weight[3] = (1.0 / 6.0) * w * w * w;
    Weight[0] = (1.0 / 6.0) + (1.0 / 2.0) * w * (w - 1.0) - Weight[3];
    Weight[2] = w + Weight[0] - 2.0 * Weight[3];
    Weight[1] = 1.0 - Weight[0] - Weight[2] - Weight[3];
    break;
  case 4L:
    w2 = w * w;
    t = (1.0 / 6.0) * w2;
    Weight[0] = 1.0 / 2.0 - w;
    Weight[0] *= Weight[0];
    Weight[0] *= (1.0 / 24.0) * Weight[0];
    t0 = w * (t - 11.0 / 24.0);
    t1 = 19.0 / 96.0 + w2 * (1.0 / 4.0 - t);
    Weight[1] = t1 + t0;
    Weight[3] = t1 - t0;
    Weight[4] = Weight[0] + t0 + (1.0 / 2.0) * w;
    Weight[2] = 1.0 - Weight[0] - Weight[1] - Weight[3] - Weight[4];    
    break;
  case 5L:
    w2 = w * w;
    Weight[5] = (1.0 / 120.0) * w * w2 * w2;
    w2 -= w;
    w4 = w2 * w2;
    w -= 1.0 / 2.0;
    t = w2 * (w2 - 3.0);
    Weight[0] = (1.0 / 24.0) * (1.0 / 5.0 + w2 + w4) - Weight[5];
    t0 = (1.0 / 24.0) * (w2 * (w2 - 5.0) + 46.0 / 5.0);
    t1 = (-1.0 / 12.0) * w * (t + 4.0);
    Weight[2] = t0 + t1;
    Weight[3] = t0 - t1;
    t0 = (1.0 / 16.0) * (9.0 / 5.0 - t);
    t1 = (1.0 / 24.0) * w * (w4 - w2 - 5.0);
    Weight[1] = t0 + t1;
    Weight[4] = t0 - t1;
  }
}    




