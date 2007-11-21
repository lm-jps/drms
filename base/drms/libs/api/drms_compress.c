/*
 *  drms_compress.c						2007.11.21
 *
 *  functions defined:
 *	drms_compress
 *	drms_uncompress
 */
#include "drms.h"
#include <zlib.h>

//#define DEBUG

/* Local function definitions. */

static int drms_compress_rice(DRMS_Type_t type, int nin, void *in, 
			      int maxout, unsigned char *out);
static int drms_compress_gzip(DRMS_Type_t type, int nin, void *in, 
			      int maxout, unsigned char *out);
static int drms_uncompress_rice(int nin, unsigned char *in, 
				DRMS_Type_t type, int maxout, void *out);
static int drms_uncompress_gzip(int nin, unsigned char *in, 
				DRMS_Type_t type, int maxout, void *out);


int drms_compress (DRMS_Compress_t method, DRMS_Type_t type, int nin, void *in,
    int maxout, unsigned char *out) {
  int sz;
  switch(method)
  {   
  case DRMS_COMP_NONE:
    sz = drms_sizeof (type);
    if (sz*nin > maxout )
      return DRMS_ERROR_OUTOFMEMORY;
    memcpy (out,in,sz*nin);
    return sz*nin;
    break;
  case DRMS_COMP_RICE:
    return drms_compress_rice (type, nin, in, maxout, out);
    break;
  case DRMS_COMP_GZIP:
    return drms_compress_gzip (type, nin, in, maxout, out);
    break;
  default:
    return DRMS_ERROR_UNKNOWNCOMPMETH;
  }
}


static int drms_compress_rice(DRMS_Type_t type, int nin, void *in, 
			      int maxout, unsigned char *out)
{
  int stat;
  switch(type)
  {
  case DRMS_TYPE_CHAR: 
    stat = rice_encode1(in, nin, (unsigned char *)out, maxout, 32);
    if (stat < 0)     
      return DRMS_ERROR_COMPRESSFAILED;
    else
      return stat;
    break;
  case DRMS_TYPE_SHORT:
    stat = rice_encode2(in, nin, (unsigned char *)out, maxout, 32);
    if (stat < 0)     
      return DRMS_ERROR_COMPRESSFAILED;
    else
      return stat;
    break;
  case DRMS_TYPE_INT:  
    stat = rice_encode4(in, nin, (unsigned char *)out, maxout, 32);
    if (stat < 0)     
      return DRMS_ERROR_COMPRESSFAILED;
    else
      return stat;
    break;
  default:
    fprintf(stderr, "ERROR in drms_compress: Unhandled DRMS type %d\n",(int)type);
    return DRMS_ERROR_INVALIDTYPE;
    break;
  }
}


static int drms_compress_gzip(DRMS_Type_t type, int nin, void *in, 
			      int maxout, unsigned char *out)
{
  int stat;
  unsigned long zlen = maxout;
  stat = compress (out, &zlen, in, nin*drms_sizeof(type));
  if (stat == Z_OK)
    return zlen;
  else
    return DRMS_ERROR_COMPRESSFAILED;
}
/*
 *
 */
int drms_uncompress (DRMS_Compress_t method, int nin, unsigned char *in,
    DRMS_Type_t type, int nout, void *out) {
  int sz;
  switch (method) {   
  case DRMS_COMP_NONE:
    sz = drms_sizeof (type);
    if (sz*nout > nin )
      return DRMS_ERROR_OUTOFMEMORY;
    memcpy (out, in, sz*nout);
    return nout;
    break;
  case DRMS_COMP_RICE:
    return drms_uncompress_rice (nin, in, type, nout, out);
    break;
  case DRMS_COMP_GZIP:
    return drms_uncompress_gzip (nin, in, type, nout, out);
    break;
  default:
    return DRMS_ERROR_UNKNOWNCOMPMETH;
  }
}


static int drms_uncompress_rice(int nin, unsigned char *in, DRMS_Type_t type,  
				int nout, void *out)
{
  int stat;
  switch(type)
  {
  case DRMS_TYPE_CHAR: 
    stat = rice_decode1(in, nin, (char *)out, nout, 32);
    if (stat < 0)     
      return DRMS_ERROR_COMPRESSFAILED;
    else
      return nout;
    break;
  case DRMS_TYPE_SHORT:
    stat = rice_decode2(in, nin, (int16_t *)out, nout, 32);
    if (stat < 0)     
      return DRMS_ERROR_COMPRESSFAILED;
    else
      return nout;
    break;
  case DRMS_TYPE_INT:  
    stat = rice_decode4(in, nin, (int32_t *)out, nout, 32);
    if (stat < 0)     
      return DRMS_ERROR_COMPRESSFAILED;
    else
      return nout;
    break;
  default:
    fprintf(stderr, "ERROR in drms_compress: Unhandled DRMS type %d\n",(int)type);
    return DRMS_ERROR_INVALIDTYPE;
    break;
  }
}


static int drms_uncompress_gzip(int nin, unsigned char *in, DRMS_Type_t type, 
				int nout, void *out)
{
  int stat, sz=drms_sizeof(type);
  unsigned long len = nout*sz;
  stat = uncompress(out, &len, in, nin);
  if (stat == Z_OK)
    return len/sz;
  else
    return DRMS_ERROR_COMPRESSFAILED;    
}



