#ifndef DRMS_FITS_H
#define DRMS_FITS_H

DRMS_Array_t *drms_readfits(const char* file, int readraw, int *headlen, 
			   char **header, int *status);
int drms_writefits(const char* file, int compress, int headlen, char *header, 
		   DRMS_Array_t *arr);

void drms_print_fitsheader(int headlen, char *header);
#endif
