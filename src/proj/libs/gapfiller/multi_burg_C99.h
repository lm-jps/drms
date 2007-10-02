#ifndef MULTI_BURG_C99_H_DEF
#define MULTI_BURG_C99_H_DEF

int smulti_burg( int n, int *m, float **x, int order, float *a, float *E);
int dmulti_burg( int n, int *m, double **x, int order, double *a, double *E);
int cmulti_burg( int n, int *m, _Complex float **x, int order, _Complex float *a, float *E);
int zmulti_burg( int n, int *m, _Complex double **x, int order, _Complex double *a, double *E);

#endif

