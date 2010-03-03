#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <math.h>
#include <mkl.h>

int sign(double v)
{
return v > 0 ? 1 : (v < 0 ? -1 : 0);
}

void dcholesky_down_0(
/*
Given Cholesky decomposion of matrix compute decomposition of matrix
with one row and column removed

call dcholesky_down_0(uplo, n, a, lda, idel, info)
uplo,n,a,lda are the same as for dpotrf
idel is the column/row to be deleted 0<=idel<n
currently only uplo='U' has been implemented
*/
char *uplo,
int *n_in,
double *a,
int *lda_in,
int *idel_in,
int *info
)
{
int n,lda,idel;
int i,j,nrot;
double ax,b,c,s,t,u;

n=*n_in;
lda=*lda_in;
idel=*idel_in;

if (*uplo != 'U') {
  *info=-1;
  return;
}

if (n < 0) {
  *info=-2;
  return;
}

if ((idel < 0) || (idel > (n-1))) {
  *info=-5;
  return;
}


// First remove row
// valgrind says that this causes a ....load of D1 read cache misses
for (i=idel; i<(n-1) ; i++) {
//for (j=0 ; j<n ; j++) a[i*lda+j]=a[(i+1)*lda+j];
  for (j=0 ; j<=(i+1) ; j++) a[i*lda+j]=a[(i+1)*lda+j]; // Only need to copy relevant part of matrix
}

for (i=idel; i<(n-1); i++) {
//Calculate Givens rotation coefficients
  ax=a[i*lda+i];
  b=a[i*lda+i+1];
  if (b == 0) {
    c=sign(ax);
    s=0;
  }
  else if (ax == 0) {
    c=0;
    s=sign(b);
  }
  // The original might be a bug - b and ax get truncated to ints.
  // else if (abs(b) > abs(ax)) {
  else if (fabs(b) > fabs(ax)) {
    t=ax/b;
    u=sign(b)*sqrt(1+t*t);
    s=1/u;
    c=s*t;
  }
  else {
    t=b/ax;
    u=sign(ax)*sqrt(1+t*t);
    c=1/u;
    s=c*t;
  }

//Apply Givens rotation
  nrot=n-i-1;
// This apparently also causes a lot of L1 cache misses
  drot(&nrot,a+i*lda+i,&lda,a+i*lda+i+1,&lda,&c,&s);
//a(i:i+1,i:*)=[[c,-s],[s,c]]#a(i:i+1,i:*)
}

*info=0;

}

extern void dcholesky_down_multi_0(
/*
Given Cholesky decomposion of matrix compute decomposition of matrix
with rows and columns removed

call dcholesky_down_multi_0(uplo, n, a, lda, ndel, wdel, info)
uplo,n,a,lda are the same as for dpotrf
ndel is the number of columns/rows to be deleted
wdel is the list of columns/rows to be deleted
*/
char *uplo,
int *n_in,
double *a,
int *lda,
int *ndel_in,
int *wdel,
int *info
)
{
int i,wi;
int *mask;
int n,ndel;

n=*n_in;
ndel=*ndel_in;
if ((ndel < 0) || (ndel >= n)) {
  *info=-5;
  return;
}
// Set up mask
mask=(int *)MKL_malloc(n*sizeof(int),32);

for (i=0;i<n;i++) mask[i]=0;
for (i=0;i<ndel;i++) {
  wi=wdel[i];
  mask[wi]=mask[wi]+1;
}
// Must delete in backwards order!
for (i=n-1;i>=0;i--) {
  if (mask[i] != 0) {
    if (mask[i] == 1) {
      dcholesky_down_0(uplo, &n, a, lda, &i, info);
      if (*info !=0) { //return info and bail
        MKL_free(mask);
        return;
      }
      n=n-1; // Order is now 1 lower
    }
    else { // Duplicates in delete list
      *info=-6;
      MKL_free(mask);
      return;
    }
  }
}

MKL_free(mask);
*info=0;

}

extern void dcholesky_down_mask(
char *uplo,
int *n_in,
double *a,
int *lda,
int *mask,
int *info
)
{
int i;
int n;

n=*n_in;

for (i=n-1;i>=0;i--) {
  if (mask[i] != 0) {
    dcholesky_down_0(uplo, &n, a, lda, &i, info);
    if (*info !=0) return; //return info and bail
    n=n-1; // Order is now 1 lower
  }
}

*info=0;

}

