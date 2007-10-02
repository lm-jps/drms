#include "ctypes_def.h"
#include "cblas.h"

#if TYPE == FLOAT 
  #define LEVINSON slevinson
#elif TYPE == DOUBLE 
  #define LEVINSON dlevinson
#elif TYPE == COMPLEXFLOAT
  #define LEVINSON clevinson
#elif TYPE == COMPLEXDOUBLE
  #define LEVINSON zlevinson
#endif

#ifdef NOBLAS
void LEVINSON( int n, CTYPE *r, CTYPE *b, CTYPE *x)
{
  int k,i;
  CTYPE alpha, mu;
  CTYPE *b_tmp;
  RTYPE beta;
  
  if (cimag(r[0]) != RZERO || creal(r[0]) <= RZERO )
  {
    fprintf(stderr, "%s: Toeplitz matrix must be Hermitian.\n",__func__);
    abort();
  }
  b_tmp = (CTYPE *)malloc(n*sizeof(CTYPE));
  beta = creal(r[0]);
  x[0] = b[0] / r[0];
  b[0] = -r[1]/beta;
  alpha = b[0];
  for (k=1; k<=n-1; k++)
  {
    beta = (RONE - CONJ(alpha)*alpha)*beta;
    mu = b[k];
    for (i=2; i<=(k+1); i++)
      mu -=  conj(r[i-1])*x[k+1-i];
    mu /= beta;
    for (i=1; i<=k; i++)
      x[i-1] +=  mu*b[k-i];
    x[k] = mu;

    if (k<n-1)
    {
      alpha = -r[k+1];
      for (i=2; i<=(k+1); i++)
	alpha -= r[i-1] * b[k+1-i];
      alpha /= beta;
      memcpy(b_tmp, b, k*sizeof(CTYPE));
      for (i=1; i<=k; i++)
	b[i-1] += alpha * CONJ(b_tmp[k-i]);
      b[k] = alpha;
    }
  }
  free(b_tmp);
}

#else

void LEVINSON( int n, CTYPE *r, CTYPE *b, CTYPE *x)
{
  int k,i;
  CTYPE alpha, mu;
  CTYPE *b_tmp;
  RTYPE beta;
  
  if (cimag(r[0]) != RZERO || creal(r[0]) <= RZERO )
  {
    fprintf(stderr, "%s: Toeplitz matrix must be Hermitian.\n",__func__);
    abort();
  }
  b_tmp = (CTYPE *)malloc(n*sizeof(CTYPE));
  beta = creal(r[0]);
  x[0] = b[0] / r[0];
  b[0] = -r[1]/beta;
  alpha = b[0];
  for (k=1; k<=n-1; k++)
  {
    beta = (RONE - CONJ(alpha)*alpha)*beta;
    mu = b[k] - DOTC(k,&r[1],1,x,-1);
    mu /= beta;
    
    AXPY(k,mu,b,-1,x,1);
    x[k] = mu;

    if (k<n-1)
    {
      alpha = -r[k+1] - DOTU(k,&r[1],1,b,-1);
      alpha /= beta;
      for (i=0; i<k; i++)
	b_tmp[i] = CONJ(b[i]);
      AXPY(k,alpha,b_tmp,-1,b,1);
      b[k] = alpha;
    }
  }
  free(b_tmp);
}

#endif
#undef LEVINSON
#undef AXPY 
#undef DOTU 
#undef DOTC 
#include "ctypes_undef.h"
