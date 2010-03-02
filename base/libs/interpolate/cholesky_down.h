extern void dcholesky_down_0(
/*
Given Cholesky decomposion of matrix compute decomposition of matrix
with one row and column removed

call dcholesky_down_0(uplo, n, a, lda, idel, info)
uplo,n,a,lda are the same as for dpotrf
idel is the column/row to be deleted 0<=idel<n
*/
char *uplo,
int *n,
double *a,
int *lda,
int *idel,
int *info
);

extern void dcholesky_down_multi_0(
/*
Given Cholesky decomposion of matrix compute decomposition of matrix
with one row and column removed

call dcholesky_down_multi_0(uplo, n, a, lda, ndel, wdel, info)
uplo,n,a,lda are the same as for dpotrf
ndel is the number of columns/rows to be deleted
wdel is the list of columns/rows to be deleted
*/
char *uplo,
int *n,
double *a,
int *lda,
int *ndel,
int *wdel,
int *info
);

extern void dcholesky_down_mask(
/*
Given Cholesky decomposion of matrix compute decomposition of matrix
with one row and column removed

call dcholesky_down_mask(uplo, n, a, lda, mask, info)
uplo,n,a,lda are the same as for dpotrf
mask is 0 for elements to retain, !=0 for elements to remove
*/
char *uplo,
int *n_in,
double *a,
int *lda,
int *mask,
int *info
);
