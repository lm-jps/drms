#ifndef _JSOC_H
#define _JSOC_H

// we only support C99 or above
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L 

#include <alloca.h>
#include <assert.h>
#include <complex.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <semaphore.h>
#include "jsoc_version.h"
//#include "xmem.h"

/* Prevent user from directly including png.h.  User should #include "mypng.h" */
#define PNG_H
#define PNGCONF_H

#else

#error C implemetations older than C99 are not supported!

#endif	// __STDC_VERSION__

#endif	// _JSOC_H
