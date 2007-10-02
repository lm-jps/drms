#include "ctypes_def.h"

#if TYPE == FLOAT 
   #define PCG spcg
#elif TYPE == DOUBLE 
   #define PCG dpcg
#elif TYPE == COMPLEXFLOAT
   #define PCG cpcg
#elif TYPE == COMPLEXDOUBLE
   #define PCG zpcg
#endif


int PCG( int n, int maxit, RTYPE tol,
	 void (*amult)(int n, CTYPE *x, CTYPE *y, void **data),
	 void (*msolve)(int n, CTYPE *x, CTYPE *y, void **data),
	 CTYPE *b, CTYPE *x, RTYPE *rnorm1,
	 void **adata, void **mdata)
{
  int k,i;
  RTYPE rnorm, bnorm;
  CTYPE gamma, alpha, beta, beta_old;
  CTYPE *r, *z, *p, *Ap;

  k = 0;
  /* Compute norm of rhs and return if rhs is zero. */
  bnorm = NRM2(n,b,1);
  if (bnorm ==  RZERO)
  {
    memset(x,0,n*sizeof(CTYPE));
    *rnorm1 = RZERO;
    return k;
  }

  r = (CTYPE *)malloc(n*sizeof(CTYPE));
  Ap = (CTYPE *)malloc(n*sizeof(CTYPE));

  /* z = A x  */
  amult(n,x,Ap,adata);

  /* r = b - A x */
  for (i=0;i<n;i++)
    r[i] = b[i] - Ap[i];
  rnorm = NRM2(n,r,1);

  /* Test if initial guess satisfies tolerance */
  if (rnorm < tol*bnorm)
  {
    *rnorm1 = rnorm/bnorm;
    free(r);
    free(Ap);
    return k;
  }

  z = (CTYPE *)malloc(n*sizeof(CTYPE));
  p = (CTYPE *)malloc(n*sizeof(CTYPE));
  /* solve M z = r  */
  if (msolve)
    msolve(n,r,z,mdata);
  else
    memcpy(z, r, n*sizeof(CTYPE));

  /* p = z  */
  memcpy(p,z,n*sizeof(CTYPE));

  /* < r, z >  */
  beta_old = DOTC(n,z,1,r,1);

  for (k=0; k<maxit && rnorm >= tol*bnorm; k++)
  {
    /* Compute A p  */
    amult(n,p,Ap,adata);

    /* < p, A p >   */
    alpha = DOTC(n,Ap,1,p,1);

    /* x = x + a*p; r = r - a*A p */
    gamma = beta_old / alpha;
    AXPY(n,gamma,p,1,x,1);
    AXPY(n,-gamma,Ap,1,r,1);
    rnorm = NRM2(n,r,1);

     /* solve M z = r  */ 
    if (msolve)
      msolve(n,r,z,mdata);
    else
      memcpy(z, r, n*sizeof(CTYPE));

    /* beta = <r, z>   */ 
    beta = DOTC(n,z,1,r,1);
    gamma = beta / beta_old; 
    beta_old = beta; 
    /* p = z + gamma*p   */ 
    for (i=0;i<n;i++) 
      p[i] = z[i] + gamma*p[i]; 
  } 
 
  free(r); 
  free(z); 
  free(p); 
  free(Ap); 

  *rnorm1 = rnorm/bnorm; 
  return k; 
}


#ifdef NOBLAS

int PCG( int n, int maxit, RTYPE tol,
	   void (*amult)(int n, CTYPE *x, CTYPE *y, void **data),
	   void (*msolve)(int n, CTYPE *x, CTYPE *y, void **data),
	   CTYPE *b, CTYPE *x, RTYPE *rnorm1,
	   void **adata, void **mdata)
{
  int k,i;
  RTYPE rnorm, bnorm;
  CTYPE gamma, alpha, beta, beta_old;
  CTYPE *r, *z, *p, *Ap;

  k=0;

  /* Compute norm of rhs and return if rhs is zero. */
  bnorm = RZERO;
  for (i=0;i<n;i++)
    bnorm = CONJ(b[i])*b[i];
  bnorm = sqrt(bnorm);
  if (bnorm ==  RZERO)
  {
    memset(x,0,n*sizeof(CTYPE));
    *rnorm1 = RZERO;
    return k;
  }

  r = (CTYPE *)malloc(n*sizeof(CTYPE));
  Ap = (CTYPE *)malloc(n*sizeof(CTYPE));

  /* z = A x  */
  amult(n,x,Ap,adata);

  /* r = b - A x */
  rnorm = RZERO;
  for (i=0;i<n;i++)
  {
    r[i] = b[i] - Ap[i];
    rnorm += CONJ(r[i])*r[i];
  }
  rnorm = sqrt(rnorm);

  /* Test if initial guess satisfies tolerance */
  if (rnorm < tol*bnorm)
  {
    *rnorm1 = rnorm/bnorm;
    free(r);
    free(Ap);
    return k;
  }

  z = (CTYPE *)malloc(n*sizeof(CTYPE));
  p = (CTYPE *)malloc(n*sizeof(CTYPE));
  /* solve M z = r  */
  if (msolve)
    msolve(n,r,z,mdata);
  else
    memcpy(z, r, n*sizeof(CTYPE));

  /* p = z  */
  memcpy(p,z,n*sizeof(CTYPE));

   /* < r, z >  */
  beta_old = CZERO;
  for (i=0; i<n; i++)
    beta_old += CONJ(z[i]) * r[i];
  
  for (k=0; k<maxit && rnorm >= tol*bnorm; k++)
  {
    /* Compute A p  */
    amult(n,p,Ap,adata);

    /* < p, A p >   */
    alpha = CZERO;
    for (i=0;i<n;i++)
      alpha += CONJ(Ap[i]) * p[i];

    /* x = x + a*p; r = r - a*A p */
    gamma = beta_old / alpha;
    rnorm = RZERO;
    for (i=0;i<n;i++) 
    {
      x[i] += gamma*p[i];
      r[i] -= gamma*Ap[i];
      rnorm += SQR(r[i]);
    } 
    rnorm = sqrt(rnorm);
    
    /* solve M z = r  */ 
    if (msolve)
      msolve(n,r,z,mdata);
    else
      memcpy(z, r, n*sizeof(CTYPE));

    /* beta = <r, z>   */ 
    beta = CZERO;
    for (i=0;i<n;i++) 
      beta += CONJ(z[i]) * r[i]; 
    gamma = beta / beta_old; 
    beta_old = beta; 
    /* p = z + b*p   */ 
    for (i=0;i<n;i++) 
      p[i] = z[i] + gamma*p[i]; 
  } 
 
  free(r); 
  free(z); 
  free(p); 
  free(Ap); 

  *rnorm1 = rnorm/bnorm; 
  return k; 
}
#endif

#undef PCG
#include "ctypes_undef.h"
