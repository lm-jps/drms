#if TYPE == FLOAT 
  #define CTYPE float
  #define RTYPE float
  #define CONJ 
  #define SQR(x) (x)*(x)
  #define REAL(x) (x)
  #define IMAG(x) (0.0f)
#elif TYPE == DOUBLE 
  #define CTYPE double 
  #define RTYPE double
  #define CONJ
  #define SQR(x) (x)*(x)
  #define REAL(x) (x)
  #define IMAG(x) (0.0)
#elif TYPE == COMPLEXFLOAT
  #define CTYPE _Complex float  
  #define RTYPE float
  #define CONJ conjf
  #define SQR(x) (crealf(x)*crealf(x)+cimagf(x)*cimagf(x))
  #define REAL(x) (crealf(x))
  #define IMAG(x) (cimagf(x))
#elif TYPE == COMPLEXDOUBLE
  #define CTYPE _Complex double
  #define RTYPE double
  #define CONJ conj 
  #define SQR(x) (creal(x)*creal(x)+cimag(x)*cimag(x))
  #define REAL(x) (creal(x))
  #define IMAG(x) (cimag(x))
#endif

#define RZERO ((RTYPE) 0)
#define CZERO ((CTYPE) 0)
#define RONE ((RTYPE) 1)
#define CONE ((CTYPE) 1)

#define COMPLEX(a,b) ((CTYPE) ((a) + _Complex_I*(b)))

#include "cblas_def.h"

