//****************************************************************************

#ifndef __CFITSIO__
#define __CFITSIO__

#include <fitsio.h>

//****************************************************************************

#define CFITSIO_MAX_DIM		          9

#define CFITSIO_SUCCESS                   0
#define CFITSIO_FAIL                     -1

#define CFITSIO_ERROR_FILE_DOESNT_EXIST  -2
#define CFITSIO_ERROR_OUT_OF_MEMORY      -3
#define CFITSIO_ERROR_LIBRARY            -4
#define CFITSIO_ERROR_FILE_IO            -5
#define CFITSIO_ERROR_ARGS               -6
#define CFITSIO_ERROR_DATA_EMPTY         -7
#define CFITSIO_ERROR_ALREADY_COMPRESSED -8

//****************************************************************************

typedef union cfitsio_value
{
      char      vs[FLEN_VALUE];
      int       vl;		//logical 1: true 0: false
      long      vi;
      double    vf;
      // complex number also stored as char string (vs)
} CFITSIO_KEY_VALUE;



typedef struct cfitsio_keyword
{
      char	key_name[FLEN_KEYWORD]; 
      char	key_type; // C: string, L: logical, I: integer, F: float, X: complex 
      CFITSIO_KEY_VALUE	key_value;
      char	key_comment[FLEN_COMMENT];
      char	key_printf_format[FLEN_KEYWORD];
      void*	next; // For fixed card size, just access as array element and return "nkeys"

} CFITSIO_KEYWORD;


typedef	struct cfitsio_image_info
{
      // Require keys for re-creating image

      int bitpix;
      int naxis;
      long naxes[CFITSIO_MAX_DIM];

      // For drms_segment() validity checking
   
      int simple;
      int extend;
      double bscale;
      double bzero;

} CFITSIO_IMAGE_INFO;


// For now, we just use C_NONE or C_DEFAULT (=C_RICE)
typedef enum cfitsio_compression_type 
{
   C_NONE,		// not compress
   C_DEFAULT,		// rice algorithm, compress row_by_row
   C_RICE,		// 
   C_HCOMPRESS,		// hcompress algorithm, whole image
   C_GZIP,		// gzip algorithm
   C_PLIO,		// plio algorithm
   C_EXTENDED_FILENAME	// user specified using extended filename 
} CFITSIO_COMPRESSION_TYPE;


//****************************************************************************
// Using CFITSIO_KEYWORD (single linked list) to hold keys, values, comments

int cfitsio_read_keys(char* filename, CFITSIO_KEYWORD** list);
int cfitsio_print_keys(CFITSIO_KEYWORD* list);
int cfitsio_free_keys(CFITSIO_KEYWORD** keys);


int cfitsio_read_image(char* filename, void** image);
int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, 
		       long from_row, long to_row, long from_col, long to_col);
int cfitsio_free_image(void** image);

int cfitsio_get_image_info(CFITSIO_KEYWORD* keys, CFITSIO_IMAGE_INFO* info);

int cfitsio_read_file(char* fits_filename, CFITSIO_KEYWORD** list,  void** image);
int cfitsio_read_file_and_info(char* fits_filename, CFITSIO_KEYWORD** keys,  void** image, CFITSIO_IMAGE_INFO* info);

int cfitsio_write_file(char* fits_filename, CFITSIO_KEYWORD* keys,  void* image, 
		       CFITSIO_COMPRESSION_TYPE compression_type);

void cfitsio_free_keysandimg(CFITSIO_KEYWORD** list, void** image);


// For drms_segment()
//int cfitsio_read_image_and_info(char* fits_filename, void** image, CFITSIO_IMAGE_INFO* info);
//int cfitsio_free_image_and_info(void** image, CFITSIO_IMAGE_INFO** info);
//int cfitsio_write_image_and_info(char* fits_filename, void** image, CFITSIO_IMAGE_INFO*info, 
//                                 CFITSIO_COMPRESSION_TYPE compression_type);

void cfitsio_print_error(int status);

int cfitsio_key_to_card(CFITSIO_KEYWORD* key, char* card);

int cfitsio_keylist_to_header(CFITSIO_KEYWORD* keylist, char** header);
int cfitsio_header_to_keylist(char* header, CFITSIO_KEYWORD** keylist);

//****************************************************************************
// Using header (char array) to keep also HISTORY, COMMENT ... cards 

int cfitsio_read_header(char* filename, char** header);
int cfitsio_print_header(char* filename);
int cfitsio_free_header(char** header);


int cfitsio_write_file_using_header(char* fits_filename, char* header,  void* image,
				    CFITSIO_COMPRESSION_TYPE compression_type);
int cfitsio_get_image_info_using_header(char* header, CFITSIO_IMAGE_INFO* info);

int cfitsio_write_compress_using_header(char* fits_filename, char* header , void* image, 
					CFITSIO_COMPRESSION_TYPE compression_type );


//****************************************************************************

#endif

