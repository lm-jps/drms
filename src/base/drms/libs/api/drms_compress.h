#ifndef DRMS_COMPRESS_H
#define DRMS_COMPRESS_H
#include "rice.h"

typedef enum {DRMS_COMP_NONE, DRMS_COMP_RICE, DRMS_COMP_GZIP} DRMS_Compress_t;

int drms_compress(DRMS_Compress_t method, DRMS_Type_t type, int nin, void *in, 
		  int maxout, unsigned char *out);
int drms_uncompress(DRMS_Compress_t method, int nin, unsigned char *in, 
		    DRMS_Type_t type, int nout, void *out);

#endif
