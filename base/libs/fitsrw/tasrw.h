#ifndef _TASRW_H
#define _TASRW_H

#include "fitsio.h"

typedef CFITSIO_IMAGE_INFO TASRW_FilePtrInfo_t;
#define HUGE_HDU_THRESHOLD 3758096384 /* 3.5 GB */

int fitsrw_readslice(int verbose,
                     const char *filename, 
                     int *fpixel, 
                     int *lpixel, 
                     CFITSIO_IMAGE_INFO** image_info,
                     void** image);

int fitsrw_writeslice(int verbose, const char *filename, int *fpixel, int *lpixel, void *image);

fitsfile *fitsrw_getfptr(int verbose, const char *filename, int writeable, int *status, int *fileCreated);
fitsfile *fitsrw_getfptr_nochksum(int verbose, const char *filename, int writeable, int *status, int *fileCreated);

int fitsrw_closefptr(int verbose, fitsfile *fptr);
int fitsrw_closefptrByName(int verbose, const char *filename);
int fitsrw_closefptrs(int verbose);
int fitsrw_getfpinfo_ext(fitsfile *fptr, CFITSIO_IMAGE_INFO *info);
int fitsrw_setfpinfo_ext(fitsfile *fptr, CFITSIO_IMAGE_INFO *info);
int fitsrw_iscompressed(const char *cparms);
int fitsrw_initializeTAS(int verbose, const char *filename);

#endif /* _TASRW_H */


