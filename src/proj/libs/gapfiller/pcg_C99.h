#ifndef PCG_C99_H_DEF
#define PCG_C99_H_DEF

extern int spcg(int n, int maxit, float tol, 
		void (*amult)(int n, float *x, float *y, void **data),
		void (*msolve)(int n, float *x, float *y, void **data), 
		float *b, float *x, float *rnorm, void **adata, void **mdata);
extern int dpcg(int n, int maxit, double tol, 
		void (*amult)(int n, double *x, double *y, void **data),
		void (*msolve)(int n, double *x, double *y, void **data), 
		double *b, double *x, double *rnorm, void **adata, void **mdata);
extern int cpcg(int n, int maxit, float tol, 
		void (*amult)(int n, _Complex float *x, _Complex float *y, void **data),
		void (*msolve)(int n, _Complex float *x, _Complex float *y, void **data), 
		_Complex float *b, _Complex float *x, float *rnorm, 
		void **adata, void **mdata);
extern int zpcg(int n, int maxit, double tol, 
		void (*amult)(int n, _Complex double *x, _Complex double *y, void **data),
		void (*msolve)(int n, _Complex double *x, _Complex double *y, void **data), 
		_Complex double *b, _Complex double *x, double *rnorm, 
		void **adata, void **mdata);

#endif
