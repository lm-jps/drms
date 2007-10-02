#ifndef __DECOMPRESS_H
#define __DECOMPRESS_H

#include "hmi_compression.h"
#include "rice.h"
#include "dwt_compress.h"

#define ERRMSG(__msg__) printkerr("ERROR at %s, line %d: " #__msg__"\n",__FILE__,__LINE__);
/*************** Function prototypes *******************/


/* Top level interface routines to decompressor. */
int decompress_next_vcdu(unsigned short telempacket[PACKETWORDS], 
			 Image_t **image, CCSDS_Packet_t **hk_packets);
void decompress_init(int (*history)(char *fmt, ...), 
		     int (*err)(char *fmt, ...));
int decompress_flush_image(unsigned int FSN, unsigned int FID, 
			   Image_t **image);
int decompress_status(unsigned int FSN, unsigned int FID, 
		      Decompress_Stat_t *stat);
int decompress_status_all(Decompress_Stat_t **stat);
void decompress_print_status( Decompress_Stat_t *stat);


/* Routines for handling telemetry headers. */
void decompress_printheader(SciDataPacket_t *H);

/* Routines for applying and undoing the bit select and table 
   lookup transformations. */
void decompress_undotransform(const unsigned int N, const unsigned int R, 
		   const unsigned int ILUTID,
		   const unsigned int numpix, unsigned short *pixels);

/* Routines for cropping and uncropping according to a skip-take table. */
void decompress_uncrop(unsigned CROPID, const unsigned int numpix, 
		       unsigned short *pixels, Image_t *image);

void decompress_read_croptable(const char *filename, const int cropid,
	CropTable_t *C);
void decompress_read_lutable(const char *filename, const int lutid,
	unsigned short *LUT);

void decompress_byteswap_packet(unsigned char *p);


/* A simple FITS writer for outputting the contents of an Image struct. */
int decompress_writefitsimage(const char* file, Image_t *image, int compress);
int decompress_writefitsimage2(const char* file, Image_t *image, const char *dsname, int compress);
void decompress_free_images(Image_t *image);
void decompress_free_hk(CCSDS_Packet_t *p);


#endif



