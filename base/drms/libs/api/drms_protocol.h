#ifndef _DRMS_PROTOCOL_H
#define _DRMS_PROTOCOL_H

#include "drms_types.h"
						   /*  Recognized protocols  */
typedef enum  {
  DRMS_GENERIC, DRMS_BINARY, DRMS_BINZIP, DRMS_FITZ, DRMS_FITS, DRMS_MSI,
  DRMS_TAS, DRMS_DSDS
} DRMS_Protocol_t;
	/*  N.B. To add a protocol, also add info to the static declartions
							 in drms_protocol.c  */

/*  Protocol specific data kept in file headers  */
/* FIXME: To be completed later: */
							   /*  Generic file  */
typedef struct DRMS_GenericSpecific_struct { 
  int spare;  /* Nothing yet */
} DRMS_GenericSpecific_t ;
							/*  Raw binary file  */
typedef struct DRMS_BinarySpecific_struct { 
  int endian; /* 0=little endian, 1=big endian. */
} DRMS_BinarySpecific_t ;
			     /*  Zipped binary file (compressed using libz)  */
typedef struct DRMS_BinzipSpecific_struct { 
  int endian; /* 0=little endian, 1=big endian. */
} DRMS_BinzipSpecific_t ;
							      /*  FITS file  */
typedef struct DRMS_FitsSpecific_struct {
  int bitpix;
  int bscale, bzero;
} DRMS_FitsSpecific_t ;
					       /*  FITZ file.  (Compressed)  */
typedef struct DRMS_FitzSpecific_struct {
  int bitpix;
  int bscale, bzero;
  int blocksize;
} DRMS_FitzSpecific_t;
			      /*  MultiScale Image (MSI) file. (Compressed)  */
typedef struct DRMS_MSISpecific_struct {
  int quant;
} DRMS_MSISpecific_t;
				 /*  Tiled Array Storage (TAS).(Compressed)  */
typedef struct DRMS_TASSpecific_struct {
  int blksz[DRMS_MAXRANK]; /* Blocking sizes. */
  char *tilebuf;           /* Buffer for holding a tiles worth of 
			      uncompressed data. */
  char *zbuf;              /* Buffer for holding a tiles worth of 
			      compressed data. */
  int quant;
} DRMS_TASSpecific_t;

						    /*  Function prototypes  */
DRMS_Protocol_t drms_str2prot(const char *str);
const char *drms_prot2str(DRMS_Protocol_t prot);
#endif
