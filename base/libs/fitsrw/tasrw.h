#ifndef _TASRW_H
#define _TASRW_H


#undef TASRW_API
#define TASRW_API
#include "image_info_def.h"
#undef TASRW_API

typedef struct cfitsio_image_info TASRW_IMAGE_INFO;
typedef struct cfitsio_image_info TASRW_FILE_PTR_INFO;

/* fitsio forward declarations */

//typedef struct cfitsio_image_info
struct TASRW_ImageInfo_struct;
typedef struct TASRW_ImageInfo_struct *TASRW_ImageInfo_t;

//typedef struct cfitsio_image_info
struct TASRW_FilePtrInfo_struct;
typedef struct TASRW_FilePtrInfo_struct *TASRW_FilePtrInfo_t;

struct CFITSIO_FITSFILE_struct;
typedef struct CFITSIO_FITSFILE_struct *TASRW_FilePtr_t;

#define HUGE_HDU_THRESHOLD 3758096384 /* 3.5 GB */

int fitsrw_readslice(int verbose, const char *filename, int *fpixel, int *lpixel, TASRW_ImageInfo_t* image_info, void** image);
int fitsrw_writeslice(int verbose, const char *filename, int *fpixel, int *lpixel, void *image);

TASRW_FilePtr_t fitsrw_getfptr(int verbose, const char *filename, int writeable, int *status, int *fileCreated);
TASRW_FilePtr_t fitsrw_getfptr_nochksum(int verbose, const char *filename, int writeable, int *status, int *fileCreated);

int fitsrw_closefptr(int verbose, TASRW_FilePtr_t fptr);
int fitsrw_closefptrByName(int verbose, const char *filename);
int fitsrw_closefptrs(int verbose);
int fitsrw_getfpinfo_ext(TASRW_FilePtr_t fptr, TASRW_FilePtrInfo_t info);
int fitsrw_setfpinfo_ext(TASRW_FilePtr_t fptr, TASRW_FilePtrInfo_t info);
int fitsrw_iscompressed(const char *cparms);
int fitsrw_initializeTAS(int verbose, const char *filename);

#endif /* _TASRW_H */
