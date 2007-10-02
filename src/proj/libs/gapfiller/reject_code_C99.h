#include "ctypes_def.h"
#if TYPE == FLOAT 
#define REJECT sreject
#elif TYPE == DOUBLE 
#define REJECT dreject
#elif TYPE == COMPLEXFLOAT
#define REJECT creject
#elif TYPE == COMPLEXDOUBLE
#define REJECT zreject
#endif


void REJECT(int n, CTYPE *data, int *isgood, RTYPE factor)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int i,ngood;
  CTYPE sum;
  RTYPE std,sum2;
  
  ngood = 0;
  sum=CZERO;
  sum2=RZERO;
  for (i=0; i<n; i++)
  {
    if (isgood[i])
    {
      sum += data[i];
      sum2 += CONJ(data[i])*data[i];
      ++ngood;
    }
  }
  std = sqrt(fabs((sum2-CONJ(sum)*sum/ngood)/(ngood-1)));
  if (verbose)
    printf("ngood = %d, sum2 = %f, sum^2 = %f, std = %f\n",
	   ngood,sum2,SQR(sum),std);

  
  for (i=0; i<n; i++)
  {
    if (isgood[i] && fabs(data[i])>factor*std)
    {
      data[i] = CZERO;
      isgood[i] = 0;
    }
  }
}

#undef REJECT
#include "ctypes_undef.h"
