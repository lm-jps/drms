#ifndef _MYPNG_H
#define _MYPNG_H

#ifdef _SETJMP_H
#pragma message "PNG/SETJMP WARNING: If png.h is #included (ingest_lev0 includes it) AND setjmp.h is #included before png.h, then pngconf.h will issue an error and stop compilation.  But this error is really not necessary - as long as pngconf.h includes the same setjmp.h as the file including setjmp.h, then there is no problem.  To work around this, either remove the first #include \"setjmp.h\" (because png.h will indirectly #include \"setjmp.h\"), recompile libpng and your code with the PNG_SETJMP_NOT_SUPPORTED flag (which will cause setjmp.h to not be included by png.h), or make your own custom version of libpng which is exactly the same as the orginal libpng, except that the errors on lines 264 and 265 of pngconf.h are commented out."
#endif

#undef PNG_H
#undef PNGCONF_H
#include <png.h>

#endif
