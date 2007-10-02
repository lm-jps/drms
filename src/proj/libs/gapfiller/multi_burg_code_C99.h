#include "ctypes_def.h"

// type specific definitions
#if TYPE == FLOAT
#define MULTI_BURG smulti_burg
#define EPSILON (10*FLT_EPSILON)   
#elif TYPE == DOUBLE 
#define MULTI_BURG dmulti_burg
#define EPSILON (10*DBL_EPSILON)
#elif TYPE == COMPLEXFLOAT
#define MULTI_BURG cmulti_burg
#define EPSILON (10*FLT_EPSILON)
#elif TYPE == COMPLEXDOUBLE
#define MULTI_BURG zmulti_burg
#define EPSILON (10*DBL_EPSILON)
#endif


#ifdef NOBLAS

int MULTI_BURG( int n, int *m, CTYPE **x, int order, CTYPE *a, RTYPE *E)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int k, i, j, skip, N, n_total, effective_order;
  CTYPE num, *atmp, *parcor, **ef, **eb, pk, cpk;
  RTYPE den, err, err0, err_old, alpha;

  effective_order = order;
  skip = 0;
  N = 0;
  n_total=0;
  for (i=0; i<n; i++)
  {
    n_total += m[i];
    if ( m[i] < order ) 
      skip++;
    else
      N += m[i];
  }
  if (verbose)
    printf("\nMultiburg using %d out of %d samples.\n",N,n_total);

  if (skip >= n)
  {
    fprintf(stderr,"%s: All data segments are shorter than model order (%d)."
	    " Cannot compute AR model.\n",__func__,order);
    abort();
  }

  parcor = (CTYPE *)malloc((order+1)*sizeof(CTYPE));
  atmp = (CTYPE *)malloc((order+1)*sizeof(CTYPE));
  ef = (CTYPE **)malloc(n*sizeof(CTYPE*));
  eb = (CTYPE **)malloc(n*sizeof(CTYPE*));  
  for (i=0; i<n; i++)
  {
    if ( m[i] >= order ) 
    {
      ef[i] = (CTYPE *)malloc(m[i]*sizeof(CTYPE));
      eb[i] = (CTYPE *)malloc(m[i]*sizeof(CTYPE));
      memcpy(ef[i],x[i],m[i]*sizeof(CTYPE));
      memcpy(eb[i],x[i],m[i]*sizeof(CTYPE));
    }
  }
  
  memset(a,0,(order+1)*sizeof(CTYPE));
  a[0] = CONE;

  // Initial error.
  err = 0;
  for (i=0; i<n; i++)
  {
    if ( m[i] >= order ) 
    {
      for (j=0; j<m[i]; j++)
	err += SQR(x[i][j]); 
    }
  }
  err /= N; 
  err0 = err;

  if (E != NULL)
    E[0] = err;

  for (k=1; k<=order; k++)
  {
    num = CZERO;
    den = RZERO;
    for (i=0; i<n; i++)
    {
      if ( m[i] >= order ) 
      {
	for (j=0; j<m[i]-k; j++)
	{
	  CTYPE f = ef[i][j+k];
	  CTYPE b = eb[i][j];
 	  
	  num -= CONJ(b) * f;
	  den += SQR(f) + SQR(b);
	} 
      }
    }
    if (den == RZERO)
    {
      fprintf(stderr,"Data is exactly modelled by an AR(%d) model\n",k-1);
      effective_order = k-1;
      break;
    }
    parcor[k] = (2*num)/den;
    pk = parcor[k];
    cpk = CONJ(pk);

    for (i=0; i<n; i++)
    {
      if ( m[i] >= order ) 
      {
	for (j=0; j<m[i]-k; j++)
	{
	  CTYPE tmp = ef[i][j+k];	  
	  
	  ef[i][j+k] +=  pk * eb[i][j];
	  eb[i][j] += cpk * tmp; 
	}
      }
    }

    alpha =  SQR(pk);
    /*    if ( alpha < EPSILON )
	  {
	  fprintf(stderr,"Model order set to %d since error reduction factor - 1 was"
	  " %e.\n",k-1,alpha);
	  effective_order = k-1;
	  break;
	  } */
    err_old = err;
    err *= RONE - alpha;

    memcpy(atmp, a, k*sizeof(CTYPE));
    atmp[k] = RZERO;
    for (i=1; i<=k; i++)
      a[i] = atmp[i] + pk * CONJ(atmp[k-i]);

    if (E != NULL)
      E[k] = err;


    if (err < EPSILON*err0 )
    {
      fprintf(stderr,"Model order set to %d since prediction error was"
	      " reduced to %f x variance.\n",k-1,EPSILON);
      effective_order = k;
      break;
    } 
  }

  free(parcor);
  free(atmp);
  for (i=0; i<n; i++)
  {
    if ( m[i] >= order ) 
    {
      free(ef[i]);
      free(eb[i]);
    }
  }
  free(ef);
  free(eb);
  return effective_order;
}

#else

int MULTI_BURG( int n, int *m, CTYPE **x, int order, CTYPE *a, RTYPE *E)
{
#ifndef NDEBUG
  int verbose=1;
#else
  int verbose=0;
#endif
  int k, i, j, skip, N, n_total, effective_order;
  CTYPE num, *atmp, *parcor, **ef, **eb, pk, cpk;
  RTYPE den, err, err0, err_old, alpha;

  effective_order = order;
  skip = 0;
  N = 0;
  n_total=0;
  for (i=0; i<n; i++)
  {
    n_total += m[i];
    if ( m[i] < order ) 
      skip++;
    else
      N += m[i];
  }
  if (verbose)
    printf("\nMultiburg using %d out of %d samples.\n",N,n_total);

  if (skip >= n)
  {
    fprintf(stderr,"%s: All data segments are shorter than model order (%d)."
	    " Cannot compute AR model.\n",__func__,order);
    abort();
  }

  parcor = (CTYPE *)malloc((order+1)*sizeof(CTYPE));
  atmp = (CTYPE *)malloc((order+1)*sizeof(CTYPE));
  ef = (CTYPE **)malloc(n*sizeof(CTYPE*));
  eb = (CTYPE **)malloc(n*sizeof(CTYPE*));  
  for (i=0; i<n; i++)
  {
    if ( m[i] >= order ) 
    {
      ef[i] = (CTYPE *)malloc(m[i]*sizeof(CTYPE));
      eb[i] = (CTYPE *)malloc(m[i]*sizeof(CTYPE));
      memcpy(ef[i],x[i],m[i]*sizeof(CTYPE));
      memcpy(eb[i],x[i],m[i]*sizeof(CTYPE));
    }
  }
  
  memset(a,0,(order+1)*sizeof(CTYPE));
  a[0] = CONE;

  // Initial error.
  err = 0;
  for (i=0; i<n; i++)
  {
    if ( m[i] >= order ) 
      err += DOTC(m[i], x[i], 1, x[i],1);
  }
  err /= N; 
  err0 = err;

  if (E != NULL)
    E[0] = err;

  for (k=1; k<=order; k++)
  {
    num = CZERO;
    den = RZERO;
    for (i=0; i<n; i++)
    {
      if ( m[i] >= order ) 
      {
	num -= DOTC(m[i]-k, eb[i], 1, &(ef[i][k]), 1);
	den += DOTC(m[i]-k, &(ef[i][k]), 1, &(ef[i][k]), 1) + 
	  DOTC(m[i]-k, eb[i], 1, eb[i], 1); 
	
      }
    }
    if (den == RZERO)
    {
      fprintf(stderr,"Data is exactly modelled by an AR(%d) model\n",k-1);
      effective_order = k-1;
      break;
    }
    parcor[k] = (2*num)/den;
    pk = parcor[k];
    cpk = CONJ(pk);

    for (i=0; i<n; i++)
    {
      if ( m[i] >= order ) 
      {
	for (j=0; j<m[i]-k; j++)
	{
	  CTYPE tmp = ef[i][j+k];	  

	  ef[i][j+k] +=  pk * eb[i][j];
	  eb[i][j] += cpk * tmp; 
	}
      }
    }

    alpha =  SQR(pk);
    /*    if ( alpha < EPSILON )
	  {
	  fprintf(stderr,"Model order set to %d since error reduction factor - 1 was"
	  " %e.\n",k-1,alpha);
	  effective_order = k-1;
	  break;
	  } */
    err_old = err;
    err *= RONE - alpha;

    memcpy(atmp, a, k*sizeof(CTYPE));
    atmp[k] = RZERO;
    for (i=1; i<=k; i++)
      a[i] = atmp[i] + pk * CONJ(atmp[k-i]);

    if (E != NULL)
      E[k] = err;


    if (err < EPSILON*err0 )
    {
      fprintf(stderr,"Model order set to %d since prediction error was"
	      " reduced to %f x variance.\n",k-1,EPSILON);
      effective_order = k;
      break;
    } 
  }

  free(parcor);
  free(atmp);
  for (i=0; i<n; i++)
  {
    if ( m[i] >= order ) 
    {
      free(ef[i]);
      free(eb[i]);
    }
  }
  free(ef);
  free(eb);
  return effective_order;
}

#endif /* NOBLAS */

#undef DOTC
#undef MULTI_BURG
#undef EPSILON
#undef SQR
#include "ctypes_undef.h"

