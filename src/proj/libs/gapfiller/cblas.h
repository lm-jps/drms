// cblas_?asum is a C interface to ?asum.
float cblas_sasum(const int N, const float *X, const int incX);
double cblas_dasum(const int N, const double *X, const int incX);
float cblas_scasum(const int N, const _Complex float *X, const int incX);
double cblas_dzasum(const int N, const _Complex double *X, const int incX);

// cblas_?axpy is a C interface to ?axpy.
void cblas_saxpy(const int N, const float alpha, const float *X, const int incX, float *Y, const int incY);
void cblas_daxpy(const int N, const double alpha, const double *X, const int incX, double *Y, const int incY);
void cblas_caxpy(const int N, const _Complex float alpha, const _Complex float *X, const int incX, _Complex float *Y, const int incY);
void cblas_zaxpy(const int N, const _Complex double alpha, const _Complex double *X, const int incX, _Complex double *Y, const int incY);

// cblas_?copy is a C interface to ?copy.
void cblas_scopy(const int N, const float *X, const int incX, float *Y, const int incY);
void cblas_dcopy(const int N, const double *X, const int incX, double *Y, const int incY);
void cblas_ccopy(const int N, const _Complex float *X, const int incX, _Complex float *Y, const int incY);
void cblas_zcopy(const int N, const _Complex double *X, const int incX, _Complex double *Y, const int incY);

// cblas_?dot is a C interface to ?dot.
float cblas_sdot(const int N, const float *X, const int incX, const float *Y, const int incY);
double cblas_ddot(const int N, const double *X, const int incX, const double *Y, const int incY);

// cblas_?dotc is a C interface to ?dotc.
_Complex float cblas_cdotc(const int N, const _Complex float *X, const int incX, const _Complex float *Y, const int incY);
_Complex double cblas_zdotc(const int N, const _Complex double *X, const int incX, const _Complex double *Y, const int incY);

// cblas_?dotu is a C interface to ?dotu.
_Complex float cblas_cdotu(const int N, const _Complex float *X, const int incX, const _Complex float *Y, const int incY);
_Complex double cblas_zdotu(const int N, const _Complex double *X, const int incX, const _Complex double *Y, const int incY);

// cblas_nrm2 is a C interface to ?nrm2.
float cblas_snrm2(const int N, const float *X, const int incX);
double cblas_dnrm2(const int N, const double *X, const int incX);
float cblas_scnrm2(const int N, const _Complex float *X, const int incX);
double cblas_dznrm2(const int N, const _Complex double *X, const int incX);

// cblas_?rot is a C interface to ?rot.
void cblas_srot(const int N, float *X, const int incX, float *Y, const int incY, const float c, const float s);
void cblas_drot(const int N, double *X, const int incX, double *Y, const int incY, const double c, const double s);

// cblas_?rotg is a C interface to ?rotg.
void cblas_srotg(float *a, float *b, float *c, float *s);
void cblas_drotg(double *a, double *b, double *c, double *s);

// cblas_?rotm is a C interface to ?rotm.
void cblas_srotm(const int N, float *X, const int incX, float *Y, const int incY, const float *P);
void cblas_drotm(const int N, double *X, const int incX, double *Y, const int incY, const double *P);

// cblas_?rotmg is a C interface to ?rotmg.
void cblas_srotmg(float *d1, float *d2, float *b1, const float b2, float *P);
void cblas_drotmg(double *d1, double *d2, double *b1, const double b2, double *P);

// cblas_?scal is a C interface to ?scal.
void cblas_sscal(const int N, const float alpha, float *X, const int incX);
void cblas_dscal(const int N, const double alpha, double *X, const int incX);
void cblas_cscal(const int N, const _Complex float alpha, _Complex float *X, const int incX);
void cblas_zscal(const int N, const _Complex double alpha, _Complex double *X, const int incX);
void cblas_csscal(const int N, const float alpha, _Complex float *X, const int incX);
void cblas_zdscal(const int N, const double alpha, _Complex double *X, const int incX);

// cblas_?swap is a C interface to ?swap.
void cblas_sswap(const int N, float *X, const int incX, float *Y, const int incY);
void cblas_dswap(const int N, double *X, const int incX, double *Y, const int incY);
void cblas_cswap(const int N, _Complex float *X, const int incX, _Complex float *Y, const int incY);
void cblas_zswap(const int N, _Complex double *X, const int incX, _Complex double *Y, const int incY);

// cblas_i?amax is a C interface to i?amax.
int cblas_isamax(const int N, const float *X, const int incX);
int cblas_idamax(const int N, const double *X, const int incX);
int cblas_icamax(const int N, const _Complex float *X, const int incX);
int cblas_izamax(const int N, const _Complex double *X, const int incX);

// cblas_i?amin is a C interface to i?amin.
int cblas_isamin(const int N, const float *X, const int incX);
int cblas_idamin(const int N, const double *X, const int incX);
int cblas_icamin(const int N, const _Complex float *X, const int incX);
int cblas_izamin(const int N, const _Complex double *X, const int incX);


// PROTOTYPES FOR FORTRAN ROUTINES

float sasum_(const int *N, const float *X, const int *incX);
double dasum_(const int *N, const double *X, const int *incX);
float scasum_(const int *N, const _Complex float *X, const int *incX);
double dzasum_(const int *N, const _Complex double *X, const int *incX);

void saxpy_(const int *N, const float *alpha, const float *X, const int *incX, float *Y, const int *incY);
void daxpy_(const int *N, const double *alpha, const double *X, const int *incX, double *Y, const int *incY);
void caxpy_(const int *N, const _Complex float *alpha, const _Complex float *X, const int *incX, _Complex float *Y, const int *incY);
void zaxpy_(const int *N, const _Complex double *alpha, const _Complex double *X, const int *incX, _Complex double *Y, const int *incY);

void scopy_(const int *N, const float *X, const int *incX, float *Y, const int *incY);
void dcopy_(const int *N, const double *X, const int *incX, double *Y, const int *incY);
void ccopy_(const int *N, const _Complex float *X, const int *incX, _Complex float *Y, const int *incY);
void zcopy_(const int *N, const _Complex double *X, const int *incX, _Complex double *Y, const int *incY);

float sdot_(const int *N, const float *X, const int *incX, const float *Y, const int *incY);
double ddot_(const int *N, const double *X, const int *incX, const double *Y, const int *incY);

void cdotc_(_Complex float *cdotc, const int *N, const _Complex float *X, const int *incX, const _Complex float *Y, const int *incY);
void zdotc_(_Complex double *zdotc, const int *N, const _Complex double *X, const int *incX, const _Complex double *Y, const int *incY);

void cdotu_(_Complex float *cdotu, const int *N, const _Complex float *X, const int *incX, const _Complex float *Y, const int *incY);
void zdotu_(_Complex double *zdotu, const int *N, const _Complex double *X, const int *incX, const _Complex double *Y, const int *incY);

float snrm2_(const int *N, const float *X, const int *incX);
double dnrm2_(const int *N, const double *X, const int *incX);
float scnrm2_(const int *N, const _Complex float *X, const int *incX);
double dznrm2_(const int *N, const _Complex double *X, const int *incX);

void srot_(const int *N, float *X, const int *incX, float *Y, const int *incY, const float *c, const float *s);
void drot_(const int *N, double *X, const int *incX, double *Y, const int *incY, const double *c, const double *s);

void srotg_(float *a, float *b, float *c, float *s);
void drotg_(double *a, double *b, double *c, double *s);

void srotm_(const int *N, float *X, const int *incX, float *Y, const int *incY, const float *P);
void drotm_(const int *N, double *X, const int *incX, double *Y, const int *incY, const double *P);

void srotmg_(float *d1, float *d2, float *b1, const float *b2, float *P);
void drotmg_(double *d1, double *d2, double *b1, const double *b2, double *P);

void sscal_(const int *N, const float *alpha, float *X, const int *incX);
void dscal_(const int *N, const double *alpha, double *X, const int *incX);
void cscal_(const int *N, const _Complex float *alpha, _Complex float *X, const int *incX);
void zscal_(const int *N, const _Complex double *alpha, _Complex double *X, const int *incX);
void csscal_(const int *N, const float *alpha, _Complex float *X, const int *incX);
void zdscal_(const int *N, const double *alpha, _Complex double *X, const int *incX);

void sswap_(const int *N, float *X, const int *incX, float *Y, const int *incY);
void dswap_(const int *N, double *X, const int *incX, double *Y, const int *incY);
void cswap_(const int *N, _Complex float *X, const int *incX, _Complex float *Y, const int *incY);
void zswap_(const int *N, _Complex double *X, const int *incX, _Complex double *Y, const int *incY);

int isamax_(const int *N, const float *X, const int *incX);
int idamax_(const int *N, const double *X, const int *incX);
int icamax_(const int *N, const _Complex float *X, const int *incX);
int izamax_(const int *N, const _Complex double *X, const int *incX);

int isamin_(const int *N, const float *X, const int *incX);
int idamin_(const int *N, const double *X, const int *incX);
int icamin_(const int *N, const _Complex float *X, const int *incX);
int izamin_(const int *N, const _Complex double *X, const int *incX);

