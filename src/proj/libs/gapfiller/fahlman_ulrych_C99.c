#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <complex.h>
#include <float.h>
#include <math.h>
#include <assert.h> 
#include <time.h>

#include "ctypes.h"
#include "fahlman_ulrych_C99.h"
#include "multi_burg_C99.h"
#include "pcg_C99.h"
#include "levinson_C99.h"
#include "xmem.h"


static inline int max(int a, int b) { return (a>b? a : b); }
static inline int min(int a, int b) { return (a<b? a : b); }

typedef struct interval_struct 
{ int first;
  int last; 
} interval;

typedef struct gapped_timeseries_struct
{
  int first_is_good;
  int n_data, m_data;
  int n_gap, m_gap;
  interval *gap_int, *data_int;
} gapped_timeseries;


static int maxorder(gapped_timeseries *ts, int *data_length, 
		    int minpercentage)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int i,j;
  int *len,idx,tmp,order;
 // Sort gaps by length and determine the largest AR model order
  // that includes minpercentage percent of the data to be used
  // in the estimation of the AR coefficients.
  len = malloc(ts->m_data*sizeof(int));
  memcpy(len, data_length, ts->m_data*sizeof(int));
  /*  for (i=0; i<ts->m_data; i++)
    printf("len[%3d] = [%d:%d] = %5d\n",i,ts->data_int[i].first,
    ts->data_int[i].last,len[i]); */
  for (i=0; i<ts->m_data; i++)
  {
    idx = i;  
    for (j=i+1; j<ts->m_data; j++)
      if (len[j]>len[idx])
	idx = j;
    tmp = len[idx];
    len[idx] = len[i];
    len[i] = tmp;
  }
  if (verbose)
  {
    for (i=0; i<ts->m_data; i++)
      printf("%5d ",len[i]);
    printf("\n");
  }
  tmp = 0; i = 0;
  while(i<ts->n_data && tmp<(minpercentage*ts->n_data)/100)
  {
    tmp += len[i++];
  }
  order = len[i-1]-1;
  if (verbose)
  {
    printf("maximal initial order chosen = %d\n",order); 
    printf("using %d out of %d data points\n",tmp,ts->n_data); 
  }
  free(len);
  return order;
}  
  



#ifdef FLOAT
#define TYPE FLOAT
#include "fahlman_ulrych_code_C99.h" 
#undef TYPE
#endif

#ifdef DOUBLE
#define TYPE DOUBLE
#include "fahlman_ulrych_code_C99.h" 
#undef TYPE
#endif

#ifdef COMPLEXFLOAT
#define TYPE COMPLEXFLOAT
#include "fahlman_ulrych_code_C99.h" 
#undef TYPE
#endif

#ifdef COMPLEXDOUBLE
#define TYPE COMPLEXDOUBLE
#include "fahlman_ulrych_code_C99.h" 
#undef TYPE
#endif


