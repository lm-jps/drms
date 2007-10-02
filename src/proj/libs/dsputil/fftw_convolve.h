#ifndef FFTW_CONVOLVE
#define FFTW_CONVOLVE

/*
   Shape flags for specifying desired output from 
   2-dimensional convolution (correlation). 
   If shape == FFTW_CONV_SHAPE_FULL then the output
   array contains the full (ma+mb-1)x(na+nb-1) convolution
   (correlation).
   If shape == FFTW_CONV_SHAPE_SAME_AS_A then the output
   array contains the central ma x na part of the
   convolution (correlation).
   If shape == FFTW_CONV_SHAPE_SAME_AS_B then the output
   array contains the central mb x nb part of the
   convolution (correlation).
*/   
#define FFTW_CONV_SHAPE_FULL  0
#define FFTW_CONV_SHAPE_AS_A  1
#define FFTW_CONV_SHAPE_AS_B  2

#ifdef sgi
#define INLINE
#else
#define INLINE inline
#endif


typedef struct fftw_convolve_op_struct {
  int reva;
  int na, nb, N;
  int ma, mb, M;
  double scale;
  double *pada, *padb;
  fftw_complex *cc;  
  int freeb, freec;
  fftw_plan q,s;
} fftw_convolve_op;


/* Double precision C interface */
/* 1-D */
fftw_convolve_op *dfftw_convolve_1d_prepare(int reva, int na, int nb, 
					    double *a, double *padb, 
					    fftw_complex *padc);
void dfftw_convolve_1d_execute(fftw_convolve_op *op, int revb,
			       double *b, double *c);
void dfftw_convolve_1d_free(fftw_convolve_op *op);

void dfftw_convolve_1d(int reva, int na, double *a, int revb, int nb, double *b, 
		       double *c);
static INLINE void dfftw_corr_1d(int na, double *a, int nb, double *b, double *c)
{
  dfftw_convolve_1d(0, na, a, 1, nb, b, c);
}

static INLINE void dfftw_conv_1d(int na, double *a, int nb, double *b, double *c)
{
  dfftw_convolve_1d(0, na, a, 0, nb, b, c);
}


/* 2-D */
fftw_convolve_op *dfftw_convolve_2d_prepare(int ma, int na, 
					    int mb, int nb, double *a, 
					    double *padb, fftw_complex *padc);
void dfftw_convolve_2d_execute(fftw_convolve_op *op, int shape, 
			       int reva, int revb, 
			       double *b, double alpha, double beta, 
			       double *c);
void dfftw_convolve_2d_free(fftw_convolve_op *op);
void dfftw_convolve_2d(int shape, 
		       int reva, int ma, int na, double *a, 
		       int revb, int mb, int nb, double *b, 
		       double alpha, double beta, double *c);
static INLINE void dfftw_corr_2d(int shape, int ma, int na, double *a, int mb, int nb, 
		    double *b, double *c)
{
  dfftw_convolve_2d(shape, 0, ma, na, a, 1, mb, nb, b, 1.0, 0.0, c);
}

static INLINE void dfftw_conv_2d(int shape, int ma, int na, double *a, int mb, int nb, 
		   double *b, double *c)
{
  dfftw_convolve_2d(shape, 0, ma, na, a, 0, mb, nb, b, 1.0, 0.0, c);
}

/* Double precision FORTRAN interface */
/* 1-D */
void dfftw_corr_1d_(int *na, double *a, int *nb, double *b, double *c);
void dfftw_conv_1d_(int *na, double *a, int *nb, double *b, double *c);
void dfftw_convolve_1d_prepare_(fftw_convolve_op **op, int *reva, int *na, 
				int *nb, double *a, double **padb, 
				fftw_complex **padc);
void dfftw_convolve_1d_execute_(fftw_convolve_op **op, int *revb,
			       double *b, double *c);
void dfftw_convolve_1d_free_(fftw_convolve_op **op);

void dfftw_convolve_1d_(int *reva, int *na, double *a, int *revb, int *nb, 
			double *b, double *c);

/* 2-D */
void dfftw_corr_2d_(int *shape, int *ma, int *na, double *a, int *mb, int *nb, 
		    double *b, double *c);
void dfftw_conv_2d_(int *shape, int *ma, int *na, double *a, int *mb, int *nb, 
		    double *b, double *c);

void dfftw_convolve_2d_prepare_(fftw_convolve_op **op,  int *ma, 
				int *na, int *mb, int *nb, double *a, 
				double **padb, fftw_complex **padc);

void dfftw_convolve_2d_execute_(fftw_convolve_op **op, int *shape, 
				int *reva, int *revb, 
				double *b, double *alpha, 
				double *beta, double *c);
void dfftw_convolve_2d_free_(fftw_convolve_op **op);
void dfftw_convolve_2d_(int *shape, 
			int *reva, int *ma, int *na, double *a, 
			int *revb, int *mb, int *nb, double *b, 
			double *alpha, double *beta, double *c);

#endif
