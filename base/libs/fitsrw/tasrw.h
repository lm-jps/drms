#ifndef _TASRW_H
#define _TASRW_H

#include "fitsio.h"

int fitsrw_readslice(const char *filename, 
                     int *fpixel, 
                     int *lpixel, 
                     CFITSIO_IMAGE_INFO** image_info,
                     void** image);

int fitsrw_writeslice(const char *filename, int *fpixel, int *lpixel, void *image);

void fitsrw_closefptrs();

#endif /* _TASRW_H */


