/**
   @file drms_protocol.h
   
*/

#ifndef _DRMS_PROTOCOL_H
#define _DRMS_PROTOCOL_H

#include "drms_types.h"

/** 
    @brief DRMS segment protocols
*/
enum  DRMS_Protocol_enum {
   DRMS_PROTOCOL_INVALID = -1,
   /** \brief Arbitrary file format which can vary across records */
   DRMS_GENERIC = 0, 
   /** \brief Binary file format */
   DRMS_BINARY, 
   /** \brief Binary file format which is gzip compressed */
   DRMS_BINZIP, 
   /** \brief Format that uses lib FITSRW to read and write FITS files */
   DRMS_FITZ,
   /** \brief Format that uses lib FITSRW to read and write FITS files */
   DRMS_FITS,
   /** \brief Unsupported */
   DRMS_MSI,
   /** \brief "Tiled Array Storage" file format */
   DRMS_TAS, 
   /** \brief DSDS file format stored in DSDS (read only) */
   DRMS_DSDS, 
   /** \brief DSDS file format stored locally (read only) */
   DRMS_LOCAL,
   /** \brief Simple FITS file format which is gzip compressed (DEPRECATED) */
   DRMS_FITZDEPRECATED,
   /** \brief Simple FITS file format (DEPRECATED) */
   DRMS_FITSDEPRECATED,
   DRMS_PROTOCOL_END
};

typedef enum DRMS_Protocol_enum DRMS_Protocol_t;


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
const char *drms_prot2ext (DRMS_Protocol_t prot);
#endif
