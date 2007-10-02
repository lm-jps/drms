#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <complex.h> 
#include <math.h>
#include <float.h>
#include "ctypes.h" 
#include "cblas.h"
#include "xmem.h"
 
#ifdef FLOAT
  #define TYPE FLOAT
  #include "multi_burg_code_C99.h" 
  #undef TYPE
#endif

#ifdef DOUBLE
  #define TYPE DOUBLE
  #include "multi_burg_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXFLOAT
  #define TYPE COMPLEXFLOAT
  #include "multi_burg_code_C99.h" 
  #undef TYPE
#endif

#ifdef COMPLEXDOUBLE
  #define TYPE COMPLEXDOUBLE
  #include "multi_burg_code_C99.h" 
  #undef TYPE
#endif
