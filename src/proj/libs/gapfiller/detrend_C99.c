#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <complex.h>
#include <math.h>
#include <assert.h>
#include "ctypes.h"
#include "detrend_C99.h"
#include "xmem.h"

#ifdef FLOAT
  #define TYPE FLOAT
  #include "detrend_code_C99.h" 
  #undef TYPE
#endif

#ifdef DOUBLE
  #define TYPE DOUBLE
  #include "detrend_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXFLOAT


//#define DEBUG
static int count2=0;

void cdetrend_discontig( int n, _Complex float *data, int *isgood, 
			 int degree, int length, int skip, 
			 int m, int *sect)
{
  int i,first,last;
  // Seperately detrend m sections of data separated by 
  // discontinuities.
  // The first section begins on data[0], 
  // the m-1 first sections end on data[sect_last[i]], i=0,1,...,m-2,
  // and the last section ends on data[n-1].

  if (m==0)
    cdetrend(n,data,isgood,degree,length,skip);
  else
  {
    for (i=0; i<m; i++)
    {
      first = sect[2*i];
      last = sect[2*i+1];
      printf("Detrending [%d:%d]\n",first,last);
      cdetrend(last-first+1,&data[first],&isgood[first],degree,length,skip);
    }
  }
}




void cdetrend( int n, _Complex float *data, int *isgood, int degree, 
	       int length, int skip)
{
  int i;

  float *real, *imag;
  real = (float *) malloc(n*sizeof(float));
  for (i=0;i<n;i++)
    real[i] = crealf(data[i]);
  sdetrend( n, real, isgood, degree, length, skip);

  imag = (float *) malloc(n*sizeof(float));
  for (i=0;i<n;i++)
    imag[i] = cimagf(data[i]);
  sdetrend( n, imag, isgood, degree, length, skip);
  for (i=0;i<n;i++)
    data[i] = real[i] + _Complex_I*imag[i];

#ifdef DEBUG
 {
   FILE *fh;
   char name[1024];
  printf("writing debug2 file #%d\n",count2);
  sprintf(name,"detrend_debug2_%d.out",count2++);
  fh = fopen(name,"w");
  assert(fh);
  for (i=0; i<n; i++)
    fprintf(fh,"%e %e\n",crealf(data[i]),cimagf(data[i]));
  fclose(fh);
 }
#endif

  free(real);
  free(imag);
}
#endif

#ifdef COMPLEXDOUBLE
void zdetrend_discontig( int n, _Complex double *data, int *isgood, 
			 int degree, int length, int skip, 
			 int m, int *sect)
{
  int i,first,last;
  // Seperately detrend m sections of data separated by 
  // discontinuities.
  // The first section begins on data[0], 
  // the m-1 first sections end on data[sect_last[i]], i=0,1,...,m-2,
  // and the last section ends on data[n-1].

  if (m==0)
    zdetrend(n,data,isgood,degree,length,skip);
  else
  {
    first = 0;
    for (i=0; i<m; i++)
    {
      first = sect[2*i];
      last = sect[2*i+1];
      zdetrend(last-first+1,&data[first],&isgood[first],degree,length,skip);
    }
  }
}


void zdetrend( int n, _Complex double *data, int *isgood, int degree, 
	       int length, int skip)
{
  int i;
  double *real, *imag;

  real = (double *) malloc(n*sizeof(double));
  for (i=0;i<n;i++)
    real[i] = creal(data[i]);
  ddetrend( n, real, isgood, degree, length, skip);

  imag = (double *) malloc(n*sizeof(double));
  for (i=0;i<n;i++)
    imag[i] = cimag(data[i]);
  ddetrend( n, imag, isgood, degree, length, skip);
  for (i=0;i<n;i++)
    data[i] = real[i] + _Complex_I*imag[i];

  free(real);
  free(imag);
}
#endif
