#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <complex.h>
#include <math.h>
#include "ctypes.h"
#include "pcg_C99.h"
#include "cblas.h"
#include "xmem.h"

#ifdef FLOAT
  #define TYPE FLOAT
  #include "pcg_code_C99.h" 
  #undef TYPE
#endif

#ifdef DOUBLE
  #define TYPE DOUBLE
  #include "pcg_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXFLOAT
  #define TYPE COMPLEXFLOAT
  #include "pcg_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXDOUBLE
  #define TYPE COMPLEXDOUBLE
  #include "pcg_code_C99.h" 
  #undef TYPE
#endif

