#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <complex.h>
#include <float.h>
#include <math.h>
#include <assert.h> 
#include <time.h>
#include "ctypes.h"
#include "reject_C99.h"

#ifdef FLOAT
#define TYPE FLOAT
#include "reject_code_C99.h" 
#undef TYPE
#endif

#ifdef DOUBLE
#define TYPE DOUBLE
#include "reject_code_C99.h" 
#undef TYPE
#endif

#ifdef COMPLEXFLOAT
#define TYPE COMPLEXFLOAT
#include "reject_code_C99.h" 
#undef TYPE
#endif

#ifdef COMPLEXDOUBLE
#define TYPE COMPLEXDOUBLE
#include "reject_code_C99.h" 
#undef TYPE
#endif


