#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <complex.h>
#include <math.h>
#include "ctypes.h"
#include "cblas.h"

#ifdef FLOAT
  #define TYPE FLOAT
  #include "levinson_code_C99.h" 
  #undef TYPE
#endif

#ifdef DOUBLE
  #define TYPE DOUBLE
  #include "levinson_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXFLOAT
  #define TYPE COMPLEXFLOAT
  #include "levinson_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXDOUBLE
  #define TYPE COMPLEXDOUBLE
  #include "levinson_code_C99.h" 
  #undef TYPE
#endif
