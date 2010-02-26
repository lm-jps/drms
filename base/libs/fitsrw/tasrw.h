#ifndef _TASRW_H
#define _TASRW_H

#include "fitsio.h"

int fitsrw_readslice(int verbose,
                     const char *filename, 
                     int *fpixel, 
                     int *lpixel, 
                     CFITSIO_IMAGE_INFO** image_info,
                     void** image);

int fitsrw_writeslice(int verbose, const char *filename, int *fpixel, int *lpixel, void *image);

fitsfile *fitsrw_getfptr(int verbose, const char *filename, int writeable, int *status);

int fitsrw_closefptr(int verbose, fitsfile *fptr);

void fitsrw_closefptrs(int verbose);

#endif /* _TASRW_H */


