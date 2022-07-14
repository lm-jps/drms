#ifndef DRMS_TASFILE_H
#define DRMS_TASFILE_H

#define TASFILE_VERSION 1


typedef struct TAS_Index_struct {
  uint64_t offset;
  uint64_t length;
  uint32_t adler32;
} __attribute__ ((packed)) TAS_Index_t;

typedef struct TAS_Header_struct {
  char magic[8];
  int version;
  DRMS_Type_t type;
  DRMS_Compress_t compress;
  uint32_t naxis;
/*
  uint32_t axis[DRMS_MAXRANK];
  uint32_t blksz[DRMS_MAXRANK];
*/
  int axis[DRMS_MAXRANK];
  int blksz[DRMS_MAXRANK];
} TAS_Header_t;

/* Read an entire array from a TAS file. */
int drms_tasfile_read(char *filename,  DRMS_Type_t type, 
		      double bzero, double bscale, DRMS_Array_t *rf);

/* Create a new TAS file and write an entire array to it. */
int drms_tasfile_write(char *filename,  DRMS_Type_t type, double bzero, 
		       double bscale, DRMS_Compress_t compression, 
		       int *blocksize, DRMS_Array_t *array);

/* Read a slice from a TAS file. */
int drms_tasfile_readslice(FILE *fp, DRMS_Type_t type, 
			   double bzero, double bscale,  
			   int naxis, int *start, int *end, DRMS_Array_t *rf);

/* Write slice to an existing TAS file. */
int drms_tasfile_writeslice(FILE *fp, double bzero, double bscale, 
			    int *start, DRMS_Array_t *array);

/* Create an empty TAS file. */
int drms_tasfile_create(char *filename, DRMS_Compress_t compression, 
			DRMS_Type_t type, int naxis, int *axis, int *blocksize,
			TAS_Header_t *header);

/* Defragment an existing TAS file by copying the data blocks 
   consecutively to a fresh file and overwriting the old file
   with the defragmented copy using the "rename" system call. 
   If the datablocks are already consecutive in the file the 
   original file is simply left in place. */
int drms_tasfile_defrag(char *tasfile, char *scratchdir);

#endif
