#ifndef INTERPOLATE_H
#define INTERPOLATE_H

/* Single point interpolation */
float fcint(int order, int nx, int ny, float *f,
	    float x, float y, float fillvalue);
double dcint(int order, int nx, int ny, double *f,
	     double x, double y, double fillvalue);

/* Vectorized interpolation */
void fcint_vect(int order, int nx, int ny, float *f, float *g,
		 int N, float *X, float *Y, float fillvalue);
void dcint_vect(int order, int nx, int ny, double *f, double *g,
		  int N, double *X, double *Y, double fillvalue);
/* Affine transformation */
void daffine(int order, int nx, int ny, double *f, double *g,
	     double a11, double a12, double a21, double a22, 
	     double dx, double dy, double fillvalue);
void faffine(int order, int nx, int ny, float *f, float *g,
	     float a11, float a12, float a21, float a22, 
	     float dx, float dy, float fillvalue);


/* Shift image by (dx,dy). */
void fshift(int order, int nx, int ny, float *f, float *g,
	    float dx, float dy, float fillvalue);
void dshift(int order, int nx, int ny, double *f, double *g,
	    double dx, double dy, double fillvalue);


/* Scaling/resampling transformation */
void fscale(int order, int nx, int ny, float *f,
	    int new_nx, int new_ny, float *g);
void dscale(int order, int nx, int ny, double *f,
	    int new_nx, int new_ny, double *g);

#endif
