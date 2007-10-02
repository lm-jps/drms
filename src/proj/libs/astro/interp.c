/*
 *  interp.c
 *
 *  Interpolation functions.
 *
 *  Contents:
 *	drms_regrid		regrid a data set to a new set of axis lengths
 *				by interpolation
 *	SDSimaginterp		Interpolate in a solar image
 *
 *  Responsible:  Rick Bogart			RBogart@solar.Stanford.EDU
 *
 *  Usage:
 *    int Regrid (DRMS_Segment_t *seg, int *lengths, LIBASTRO_Interpolation_t scheme)
 *    float Imaginterp (DRMS_Segment_t *seg, double x, double y);
 *
 *  Bugs:
 *    The regridding assumes the pixels are labeled by their centers, but the
 *	interpolation assumes the scale of the image is from edge to edge, so
 *	there are some serious conceptual edge inconsistencies.
 *    The regridding involves interpolation as either floats or doubles,
 *	with conversion back to original type as necessary
 *
 *  Planned updates:
 *    Do some of the weighting and projection
 *
 *  Revision history is at end of file.
 */


#include "drms.h"
#include "serverdefs.h"
#include "drms_types.h"
#include "drms_record.h"
#include "drms_names.h"
#include "drms_env.h"
#include "drms_network.h"
#include "drms_keyword.h"
#include "astro.h"

#define MAX_RANK    (2)

				/*  Calculate the interpolation kernel.  */
static void Ccker(double *u, double s) {
  double s2, s3;

  s2= s * s;
  s3= s2 * s;
  u[0] = s2 - 0.5 * (s3 + s);
  u[1] = 1.5*s3 - 2.5*s2 + 1.0;
  u[2] = -1.5*s3 + 2.0*s2 + 0.5*s;
  u[3] = 0.5 * (s3 - s2);
}

/*  Cubic convolution interpolation  */
float Ccint2(float *f, int nx, int ny, double x, double y) {
  double  ux[4], uy[4];
  double sum;
  int ix, iy, ix1, iy1, i, j;

  if (x < 1. || x >= (float)(nx-2) || y < 1. || y >= (float)(ny-2))
    return F_NAN;

  ix = (int)x;
  Ccker (ux,  x - (double)ix);
  iy = (int)y;
  Ccker (uy,  y - (double)iy);

  ix1 = ix - 1;
  iy1 = iy - 1;
  sum = 0.;
  for (i = 0; i < 4; i++)
    for (j = 0; j < 4; j++)
      sum = sum + f[(iy1+i) * nx + ix1 + j] * uy[i] * ux[j];
  return (float)sum;
}

double Ccint2d(double *f, int nx, int ny, double x, double y) {
  double  ux[4], uy[4];
  double sum;
  int ix, iy, ix1, iy1, i, j;

  if (x < 1. || x >= (float)(nx-2) || y < 1. || y >= (float)(ny-2))
    return D_NAN;

  ix = (int)x;
  Ccker (ux,  x - (double)ix);
  iy = (int)y;
  Ccker (uy,  y - (double)iy);

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
 *    float linint2 (float *f, int nx, int ny, double x, double y)
 *    double linint2d (double *f, int nx, int ny, double x, double y)
 *
 */

float Linint2(float *f, int nx, int ny, double x, double y) {

   double p0, p1, q0, q1;			/* weights		*/
   int ilow, jlow;				/* selected CCD pixels	*/
   float *fptr;					/* input array temp	*/
   double u;                        

   ilow = (int)floor (x);
   jlow = (int)floor (y);
   if (ilow < 0) ilow = 0;
   if (ilow+1 >= nx) ilow -= 1;
   if (jlow < 0) jlow = 0;
   if (jlow+1 >= ny) jlow -= 1;
   p1 = x - ilow;
   p0 = 1.0 - p1;
   q1 = y - jlow;
   q0 = 1.0 - q1;
			/*  Bilinear interpolation from four data points. */
   u = 0.0;
   fptr = f + jlow*nx + ilow;
   u += p0 * q0 * *fptr++;
   u += p1 * q0 * *fptr;
   fptr += nx - 1;
   u += p0 * q1 * *fptr++;
   u += p1 * q1 * *fptr;
   return (float)u;
}

double Linint2d(double *f, int nx, int ny, double x, double y) {

   double p0, p1, q0, q1;			/* weights		*/
   int ilow, jlow;				/* selected CCD pixels	*/
   double *fptr;				/* input array temp	*/
   double u;                        

   ilow = (int)floor (x);
   jlow = (int)floor (y);
   if (ilow < 0) ilow = 0;
   if (ilow+1 >= nx) ilow -= 1;
   if (jlow < 0) jlow = 0;
   if (jlow+1 >= ny) jlow -= 1;
   p1 = x - ilow;
   p0 = 1.0 - p1;
   q1 = y - jlow;
   q0 = 1.0 - q1;
			/*  Bilinear interpolation from four data points. */
   u = 0.0;
   fptr = f + jlow*nx + ilow;
   u += p0 * q0 * *fptr++;
   u += p1 * q0 * *fptr;
   fptr += nx - 1;
   u += p0 * q1 * *fptr++;
   u += p1 * q1 * *fptr;
   return u;
}

int Regrid(DRMS_Array_t **dataArr, int *new_length, LIBASTRO_Interpolation_t scheme) {
/*
 *  Regrid a segment, with each dimension converted to length "size", but with
 *    the same range
 *
 *  Return values:
 *      0   normal
 *	-1  not 2 dimensional, unsupported; data unchanged
 *      -2  invalid segment data array
 *      -3  couldn't create new data array
 */

  LIBASTRO_Error_t error = kLIBASTRO_Success;

  void *new;
  double step[MAX_RANK], start[MAX_RANK];
  double x, y;
  int orange[MAX_RANK], nrange[MAX_RANK];
  int col, row, n;
  int nsize = 1;
  int rank = 0;

  int *dims = NULL;
  DRMS_Type_t saveType;
  DRMS_Array_t *segArrConv = NULL;
  DRMS_Array_t *inputArr = NULL;

  int status = 0;
  int bDataConverted = 0;

  if (*dataArr == NULL)
  {
       error = kLIBASTRO_BadData;
  }
  else
  {
     dims = (*dataArr)->axis;
     rank = (*dataArr)->naxis;
     
     if (rank != 2) return kLIBASTRO_BadDimensionality;
     
     for (n = 0; n < rank; n++) {
	orange[n] = dims[n];
	nrange[n] = new_length[n];
	nsize *= nrange[n];
	step[n] = (double) orange[n] / (double) nrange[n];
	start[n] = -0.5 + 0.5 * step[n];
     }
     
     /*  convert the data to floating point, saving type and fill value  */
     /* NOTE: in drms, the missing/fill type is always saved by drms_fitswrite()
      * so there is no need to figure out what this is now. */
     saveType = (*dataArr)->type;
     
     if (saveType == DRMS_TYPE_INT || saveType == DRMS_TYPE_LONGLONG)
     {
	/* drms_segment_read() converts to the type provided. */
	segArrConv = drms_array_convert(DRMS_TYPE_DOUBLE, 
					(*dataArr)->bzero, 
					(*dataArr)->bscale,
					(*dataArr));

	bDataConverted = 1;
     }
     else if (saveType == DRMS_TYPE_SHORT || saveType == DRMS_TYPE_CHAR)
     {
	segArrConv = drms_array_convert(DRMS_TYPE_FLOAT, 
					(*dataArr)->bzero, 
					(*dataArr)->bscale,
					(*dataArr));
	bDataConverted = 1;
     }

     if (bDataConverted)
     {
	inputArr = segArrConv;
     }
     else
     {
	inputArr = *dataArr;
     }

     if (bDataConverted && segArrConv == NULL)
     {
	error = kLIBASTRO_CouldntCreateData;
     }
     else
     {
	y = start[1];

	if (inputArr->type == DRMS_TYPE_DOUBLE) 
	   new = malloc(nsize * sizeof (double));
	else
	   new = malloc(nsize * sizeof (float));

	for (row = 0, n = 0; row < nrange[1]; row++) {
	   x = start[0];
	   for (col = 0; col < nrange[0]; col++) {
	      if (inputArr->type == DRMS_TYPE_DOUBLE)
	      {
		 if (scheme ==  kLIBASTRO_InterCubic)
		 {
		    ((double *)new)[n] = Ccint2d((double *)inputArr->data, 
						 orange[0], 
						 orange[1], 
						 x, 
						 y);
		 }
		 else
		 {
		    ((double *)new)[n] = Linint2d((double *)inputArr->data, 
						  orange[0], 
						  orange[1], 
						  x, 
						  y);
		 }
	      }
	      else
	      {
		 if (scheme ==  kLIBASTRO_InterCubic)
		 {
		    ((float *)new)[n] = Ccint2((float *)inputArr->data, 
					       orange[0], 
					       orange[1], 
					       x, 
					       y);
		 }
		 else
		 {
		    ((float *)new)[n] = Linint2((float *)inputArr->data, 
						orange[0], 
						orange[1], 
						x, 
						y);
		 }
	      }
	      

	      x += step[0];
	      n++;
	   }
	   y += step[1];
	}
	
	DRMS_Array_t *segArray = drms_array_create(inputArr->type,
						   rank, 
						   new_length, 
						   new,
						   &status);
	
	if (status != DRMS_SUCCESS)
	{
	   error = kLIBASTRO_CouldntCreateData;
	}

	segArray->bzero = inputArr->bzero;
	segArray->bscale = inputArr->bscale;
	
	if (error == kLIBASTRO_Success)
	{
	   /* convert back to original type */
	   /* inputArr has the database's bzero and bscale */
	   if (bDataConverted)
	   {
	      drms_array_convert_inplace(saveType, 
					 inputArr->bzero, 
					 inputArr->bscale, 
					 segArray);
	   }

	   if (*dataArr)
	   {
	      drms_free_array(*dataArr);
	      *dataArr = segArray;
	   }
	}
	
	drms_free_array(segArrConv);
     }
  }
  
  return error;
}

float Imaginterp(DRMS_Segment_t *img, double x, double y) {
/*
 *  Interpolate to an arbitrary grid location {x, y} from a SDS dataset
 *    containing a projected solar image.  The aim of this function is
 *    is to provide an ideal interpolation weighted by foreshortening,
 *    limb darkening, and vector projection, but for now this is simply
 *    a stub function that extracts information from the attributes of
 *    the dataset and calls a simple two-dimensional cubic convolutional
 *    interpolation function.
 *
 *  x and y are in the range [-1,-1] at the "lower left" of the first pixel
 *    to [1,1] at the "upper right" of the last pixel in the image.
 *  (These are converted to the ccint2 conventions, with x and y in
 *    the range [0,0] at the "center" of the first pixel to
 *    [cols-1, rows-1] at the "center" of the last pixel.)
 *  Interpolation near the edges is not allowed.
 *
 *  Bugs:
 *    Interpolation within one pixel of edge is not implemented.  If x or y
 *	is in this range or off the image, the function returns zero.
 *    SDSimaginterp assumes a fixed scale in both directions, so that if one
 *	dimension is larger than another the scale is applied to the larger.
 *    Only floating point data are supported by SDSimaginterp, and there is
 *	not even any testing for validity.
 */
  double xs, ys;
  int cols, rows, mdim;
  int status;

  cols = img->axis[0];
  rows = img->axis[1];

  mdim = (cols > rows) ? cols : rows;
  xs = 0.5 * (x + 1.0) * mdim - 0.5; /* P Giles 24/09/97 subtracted this 0.5 */
  ys = 0.5 * (y + 1.0) * mdim - 0.5; /* P Giles 24/09/97 subtracted this 0.5 */

  /* ccint2 assumes float data. */
  DRMS_Array_t *segArr = drms_segment_read(img, DRMS_TYPE_FLOAT, &status);

  if (status == DRMS_SUCCESS)
  {
       return Ccint2(segArr->data, cols, rows, xs, ys);
  }
  else
  {
       return DRMS_MISSING_FLOAT;
  }
}
