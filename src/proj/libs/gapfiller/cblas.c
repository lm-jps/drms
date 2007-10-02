#include "cblas.h"
#include <complex.h>



// cblas_?asum is a C interface to ?asum.
float cblas_sasum(const int N, const float *X, const int incX)
{  
  return sasum_(&N, X, &incX);
}
double cblas_dasum(const int N, const double *X, const int incX)
{  
  return dasum_(&N, X, &incX);
}

float cblas_scasum(const int N, const _Complex float *X, const int incX)
{  
  return scasum_(&N, X, &incX);
}
double cblas_dzasum(const int N, const _Complex double *X, const int incX)
{  
  return dzasum_(&N, X, &incX);
}

// cblas_?axpy is a C interface to ?axpy.
void cblas_saxpy(const int N, const float alpha, const float *X, const int incX, float *Y, const int incY)
{
  saxpy_(&N, &alpha, X, &incX, Y, &incY);
}
void cblas_daxpy(const int N, const double alpha, const double *X, const int incX, double *Y, const int incY)
{
  daxpy_(&N, &alpha, X, &incX, Y, &incY);
}
void cblas_caxpy(const int N, const _Complex float alpha, const _Complex float *X, const int incX, _Complex float *Y, const int incY)
{
  caxpy_(&N, &alpha, X, &incX, Y, &incY);
}
void cblas_zaxpy(const int N, const _Complex double alpha, const _Complex double *X, const int incX, _Complex double *Y, const int incY)
{
  zaxpy_(&N, &alpha, X, &incX, Y, &incY);
}

// cblas_?copy is a C interface to ?copy.
void cblas_scopy(const int N, const float *X, const int incX, float *Y, const int incY)
{
  scopy_(&N,  X, &incX, Y, &incY);
}
void cblas_dcopy(const int N, const double *X, const int incX, double *Y, const int incY)
{
  dcopy_(&N,  X, &incX, Y, &incY);
}
void cblas_ccopy(const int N, const _Complex float *X, const int incX, _Complex float *Y, const int incY)
{
  ccopy_(&N,  X, &incX, Y, &incY);
}
void cblas_zcopy(const int N, const _Complex double *X, const int incX, _Complex double *Y, const int incY)
{
  zcopy_(&N,  X, &incX, Y, &incY);
}

// cblas_?dot is a C interface to ?dot.
float cblas_sdot(const int N, const float *X, const int incX, const float *Y, const int incY)
{
  return sdot_(&N, X, &incX, Y, &incY);
}
double cblas_ddot(const int N, const double *X, const int incX, const double *Y, const int incY)
{
  return ddot_(&N, X, &incX, Y, &incY);
}

// cblas_?dotc is a C interface to ?dotc.
_Complex float cblas_cdotc(const int N, const _Complex float *X, const int incX, const _Complex float *Y, const int incY)
{
  _Complex float cdotc;
  cdotc_(&cdotc,&N, X, &incX, Y, &incY);
  return cdotc;
}
_Complex double cblas_zdotc(const int N, const _Complex double *X, const int incX, const _Complex double *Y, const int incY)
{
  _Complex double zdotc;
  zdotc_(&zdotc,&N, X, &incX, Y, &incY);
  return zdotc;
}


// cblas_?dotu is a C interface to ?dotu.
_Complex float cblas_cdotu(const int N, const _Complex float *X, const int incX, const _Complex float *Y, const int incY)
{
  _Complex float cdotu;
  cdotu_(&cdotu,&N, X, &incX, Y, &incY);
  return cdotu;
}
_Complex double cblas_zdotu(const int N, const _Complex double *X, const int incX, const _Complex double *Y, const int incY)
{
  _Complex double zdotu;
  zdotu_(&zdotu,&N, X, &incX, Y, &incY);
  return zdotu;
}

// cblas_nrm2 is a C interface to ?nrm2.
float cblas_snrm2(const int N, const float *X, const int incX)
{
  return snrm2_(&N, X, &incX);
}
double cblas_dnrm2(const int N, const double *X, const int incX)
{
  return dnrm2_(&N, X, &incX);

}
float cblas_scnrm2(const int N, const _Complex float *X, const int incX)
{
  return scnrm2_(&N, X, &incX);
}
double cblas_dznrm2(const int N, const _Complex double *X, const int incX)
{
  return dznrm2_(&N, X, &incX);
}

// cblas_?rot is a C interface to ?rot.
void cblas_srot(const int N, float *X, const int incX, float *Y, const int incY, const float c, const float s)
{
  srot_(&N, X, &incX, Y, &incY, &c, &s);
}
void cblas_drot(const int N, double *X, const int incX, double *Y, const int incY, const double c, const double s)
{
  drot_(&N, X, &incX, Y, &incY, &c, &s);
}

// cblas_?rotg is a C interface to ?rotg.
void cblas_srotg(float *a, float *b, float *c, float *s)
{
  srotg_(a,b,c,s);
}
void cblas_drotg(double *a, double *b, double *c, double *s)
{
  drotg_(a,b,c,s);
}

// cblas_?rotm is a C interface to ?rotm.
void cblas_srotm(const int N, float *X, const int incX, float *Y, const int incY, const float *P)
{
  srotm_(&N, X, &incX, Y, &incY, P);
}
void cblas_drotm(const int N, double *X, const int incX, double *Y, const int incY, const double *P)
{
  drotm_(&N, X, &incX, Y, &incY, P);
}

// cblas_?rotmg is a C interface to ?rotmg.
void cblas_srotmg(float *d1, float *d2, float *b1, const float b2, float *P)
{
  srotmg_(d1,d2,b1,&b2,P);
}
void cblas_drotmg(double *d1, double *d2, double *b1, const double b2, double *P)
{
  drotmg_(d1,d2,b1,&b2,P);
}

// cblas_?scal is a C interface to ?scal.
void cblas_sscal(const int N, const float alpha, float *X, const int incX)
{
  sscal_(&N, &alpha, X, &incX);
}
void cblas_dscal(const int N, const double alpha, double *X, const int incX)
{
  dscal_(&N, &alpha, X, &incX);
}
void cblas_cscal(const int N, const _Complex float alpha, _Complex float *X, const int incX)
{
  cscal_(&N, &alpha, X, &incX);
}
void cblas_zscal(const int N, const _Complex double alpha, _Complex double *X, const int incX)
{
  zscal_(&N, &alpha, X, &incX);
}
void cblas_csscal(const int N, const float alpha, _Complex float *X, const int incX)
{
  csscal_(&N, &alpha, X, &incX);
}
void cblas_zdscal(const int N, const double alpha, _Complex double *X, const int incX)
{
  zdscal_(&N, &alpha, X, &incX);
}

// cblas_?swap is a C interface to ?swap.
void cblas_sswap(const int N, float *X, const int incX, float *Y, const int incY)
{
  sswap_(&N, X, &incX, Y, &incY);
}
void cblas_dswap(const int N, double *X, const int incX, double *Y, const int incY)
{
  dswap_(&N, X, &incX, Y, &incY);
}
void cblas_cswap(const int N, _Complex float *X, const int incX, _Complex float *Y, const int incY)
{
  cswap_(&N, X, &incX, Y, &incY);
}
void cblas_zswap(const int N, _Complex double *X, const int incX, _Complex double *Y, const int incY)
{
  zswap_(&N, X, &incX, Y, &incY);
}

// cblas_i?amax is a C interface to i?amax.
int cblas_isamax(const int N, const float *X, const int incX)
{
  return isamax_(&N, X, &incX);
}
int cblas_idamax(const int N, const double *X, const int incX)
{
  return idamax_(&N, X, &incX);
}
int cblas_icamax(const int N, const _Complex float *X, const int incX)
{
  return icamax_(&N, X, &incX);
}
int cblas_izamax(const int N, const _Complex double *X, const int incX)
{
  return izamax_(&N, X, &incX);
}

// cblas_i?amin is a C interface to i?amin.
int cblas_isamin(const int N, const float *X, const int incX)
{
  return isamin_(&N, X, &incX);
}
int cblas_idamin(const int N, const double *X, const int incX)
{
  return idamin_(&N, X, &incX);
}
int cblas_icamin(const int N, const _Complex float *X, const int incX)
{
  return icamin_(&N, X, &incX);
}
int cblas_izamin(const int N, const _Complex double *X, const int incX)
{
  return izamin_(&N, X, &incX);
}

