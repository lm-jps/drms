#ifndef LEVINSON_C99_H_DEF
#define LEVINSON_C99_H_DEF

extern void slevinson( int n, float *r, float *b, float *x);
extern void dlevinson( int n, double *r, double *b, double *x);
extern void clevinson( int n, _Complex float *r, _Complex float *b, _Complex float *x);
extern void zlevinson( int n, _Complex double *r, _Complex double *b, _Complex double *x);

#endif

