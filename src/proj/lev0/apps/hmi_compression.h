#ifndef __HMI_COMPRESSION_H
#define __HMI_COMPRESSION_H

#include <time.h>
#include <stdint.h>
#include "packets.h"

/************ Built-in lookup and crop tables: ************************

We define the following LUTIDs for simple "builtin" lookup tables:
==========================================================================
| LUTID  |  f(i), i=0,...,2^14-1   |  f_inv(i), i=0,...,2^14-1           |
+------------------------------------------------------------------------+
|    0   |          i              |            i                        |
|    1   | floor(SQRT(128*i)+0.5)  | min(floor((i*i)/128 + 0.5),2^14-1)  |
|    2   | floor(SQRT(1024*i)+0.5) | min(floor((i*i)/1024 + 0.5),2^14-1) |
|  255   | read from file          | read from file                      |
==========================================================================

We define the following CROPIDs for simple "builtin" crop tables:
==============================================================================
| CROPID | width | height | cropping | constraint                            | 
+----------------------------------------------------------------------------+
|    0   |  256  |  256   | square   | TRUE (none)                           |
|    1   | 1024  | 1024   | square   | TRUE (none)                           |
|    2   | 4096  | 4096   | square   | TRUE (none)                           |
|    3   |  256  |  256   | circle   | (x-127.5)^2+(y-127.5)^2 <= 127.5^2    |
|    4   | 1024  | 1024   | circle   | (x-511.5)^2+(y-511.5)^2 <= 511.5^2    |
|    5   | 4096  | 4096   | circle   | (x-2047.5)^2+(y-2047.5)^2 <= 2047.5^2 |
| 4095   | ----  | ----   | custom   | read skip-take table from file        |
==============================================================================
where pixels x- and y-coordinates are numbered from 0 to width-1, 
0 to height-1 respectively.
******************************************************/


/*************** Constants *******************/
#define FIDNAME "HMI_SEQ_FILTERGRAM_ID"
#define FSNNAME "HMI_SEQ_FILTERGRAM_SN"

#define APID_HMI_SCIENCE_1		400
#define APID_HMI_SCIENCE_2		410
#define APID_HMI_TIME_1			405
#define APID_HMI_TIME_2			415
#define APID_HMI_TEST1768_1		407
#define APID_HMI_TEST1768_2		417
#define APID_HMI_TEST256x256_1		408
#define APID_HMI_TEST256x256_2		418
#define APID_HMI_TEST4096x4096_1	409
#define APID_HMI_TEST4096x4096_2	419
#define APID_HMI_IMSTAT_1		445
#define APID_HMI_IMSTAT_2		475

#define APID_AIA_SCIENCE_1		500
#define APID_AIA_SCIENCE_2		510
#define APID_AIA_TIME_1			505
#define APID_AIA_TIME_2			515
#define APID_AIA_TEST1768_1		507
#define APID_AIA_TEST1768_2		517
#define APID_AIA_TEST256x256_1		508
#define APID_AIA_TEST256x256_2		518
#define APID_AIA_TEST4096x4096_1	509
#define APID_AIA_TEST4096x4096_2	519
#define APID_AIA_IMSTAT_1		529
#define APID_AIA_IMSTAT_2		569


/* Completion codes returned by decompress_next_packet */
#define SUCCESS_HKCOMPLETE         (3)
#define SUCCESS_HK                 (2)
#define SUCCESS_IMAGECOMPLETE      (1)
#define SUCCESS_IMAGE              (0)
#define SUCCESS                    (0)
#define ERROR_BADOFFSET           (-1)
#define ERROR_CORRUPTDATA         (-2)
#define ERROR_BADHEADER           (-3)
#define ERROR_CTXOVERFLOW         (-4)
#define ERROR_INVALIDID           (-5)
#define ERROR_TOOMANYPIXELS       (-6)
#define ERROR_NOSUCHIMAGE         (-7)
#define ERROR_PARTIALOVERWRITE    (-8)
#define ERROR_WRONGPACKET         (-9)   
#define ERROR_MISSING_FSN        (-10)
#define ERROR_MISSING_FID        (-11)
#define ERROR_INVALIDCROPID      (-12)
#define ERROR_NODATA             (-13)
#define ERROR_HK_UNKNOWN_APID              (-14)
#define ERROR_HK_CANNOT_FIND_VER_NUM       (-15)
#define ERROR_HK_CANNOT_LOAD_HK_VALUES     (-16)
#define ERROR_HK_CANNOT_LOAD_ENGR_VALUES   (-17)
#define ERROR_HK_INVALID_BITFIELD_LENGTH   (-18)
#define ERROR_HK_UNHANDLED_TYPE            (-19)
#define ERROR_HK_NOSUCHDIR                 (-20)
#define ERROR_HK_CANNOT_LOAD_CONFIG        (-21)
#define ERROR_RAW_MODE_TRAILING_GARBAGE    (-22)


/* Maximum number of images that can be decoded simultaneously. */
#define MAXCONTEXTS (30)

/* Limits for compression parameters. */
#define NMAX      (16)
#define NMIN       (6)
#define RMAX       (8)
#define RMIN       (0)
#define KMAX       (7)
#define KMIN       (0)

/* Maximum number of different crop tables. */
#define MAX_CROPID ((1<<12)-1)

/* Maximum number of different lookup tables. */
#define MAX_LUTID  ((1<<8)-1)

/************* Macros **********************/

#define UNPACK_FSN(H) (((unsigned int)(H)->ccdhead[0]<<16)+((unsigned int)(H)->ccdhead[1]))
#define UNPACK_FID(H) (((unsigned int)(H)->ccdhead[2]<<16)+((unsigned int)(H)->ccdhead[3]))

#define IMAGE_FSN(I) UNPACK_FSN(&(I->firstpacket))
#define IMAGE_FID(I) UNPACK_FID(&(I->firstpacket))

#define ID2FSN(ID) ((uint32_t) (ID) & 0xffffffff)
#define ID2FID(ID) ((uint32_t) ((ID)>>32) & 0xffffffff)
//#define UNIQUEID(FSN,FID)  ((int64_t)(FSN) | ((int64_t)(FID)<<32))
#define UNIQUEID(FSN,FID)  ((int64_t)(FSN))

/* Macros for helping the GCC and ICC branch predictor. */
#define unlikely(a) __builtin_expect((a), 0)
#define likely(a) __builtin_expect((a), 1)


/*************** Type definitions *******************/

/* Decompression context for an image. */
typedef struct Decompress_Context_struct
{
  /* Unique Image ID. */
  int64_t ID;               
  unsigned short *pixelbuf; /* Buffer to hold decoded pixels before 
			       uncropping. */
  struct Image_struct *image;           /* Buffer for final image. */
} Decompress_Context_t;


/* Decompression status for an image. */
typedef struct Decompress_Stat_struct
{
  /* Unique Image ID. */
  int64_t ID;
  time_t starttime;        /* Timestamp indicating when processing of the 
			      first packet for this image began. */
  unsigned short npackets; /* Number of packets received. */
  unsigned int numpix;     /* Number of unique pixels decompressed. */
  unsigned int totalpix;   /* Total number of unique pixels expected for 
			      image. */
  unsigned char backup_occured; /*  At some point the offset counter decreased,
				    possibly due to reprocessing of part of an 
				    image.*/
  unsigned char skip_occured; /*  At some point the offset counter skipped 
				  forward, indicating lost or out of order 
				  packets.*/
} Decompress_Stat_t;


/* Image data structure. */
typedef struct Image_struct
{
  /* Unique Image ID. */
  int64_t ID;

  /* Image data. */
  short width, height;
  short *data;

  /* Decompressor statistics. */
  struct Decompress_Stat_struct stat;
  
  /* Meta data. */
  SciDataPacket_t firstpacket;  
  HK_Keyword_t *keywords;

  /* Linked list pointer in case we need to return 
     multiple completed images at a time. */
  struct Image_struct *next; 
} Image_t;



/* Crop table in skip-take format. */
typedef struct CropTable_struct
{
  unsigned int totalpix;
  unsigned int width, height;
  unsigned short *table;      /* Skip-take table with 2*totalpix entries. */
} CropTable_t;



/*************** Global table definitions *******************/
extern CropTable_t croptables[MAX_CROPID+1];
extern unsigned short *lutables[MAX_LUTID+1];
extern unsigned short *ilutables[MAX_LUTID+1];


/*************** Inline functions *******************/

/* Make the unique 64 bit ID out of FSN + (FID<<32), 
   which are stored in the camera header thus:
   CCDHEAD[0] = upper 16 bits of FSN
   CCDHEAD[1] = lower 16 bits of FSN
   CCDHEAD[2] = upper 16 bits of FID
   CCDHEAD[3] = lower 16 bits of FID.
*/
inline static long long hmicomp_uniqueid(SciDataPacket_t *P)
{
  unsigned int FSN, FID;
  FSN = UNPACK_FSN(P);
  FID = UNPACK_FID(P);
  return UNIQUEID(FSN,FID);
}


#endif



