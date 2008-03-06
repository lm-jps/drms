
#ifndef __CFITSIO__
#define __CFITSIO__

//****************************************************************************

#define CFITSIO_MAX_DIM							9

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
   char	vs[FLEN_VALUE];
   int	vl;		//logical 1: true 0: false
   long  vi;
   double vf;
   // complex number also stored as char string (vs)
} CFITSIO_KEY_VALUE;


typedef struct cfitsio_keyword
{
	char				key_name[FLEN_KEYWORD]; 
	char				key_type; // C: string, L: logical, I: integer, F: float, X: complex 
	CFITSIO_KEY_VALUE	key_value;
	char				key_comment[FLEN_COMMENT];
	char				key_printf_format[FLEN_KEYWORD];
	void*				next; // For fixed card size, just access as array element and return "nkeys"

} CFITSIO_KEYWORD;


typedef	struct cfitsio_image_info
{
	int bitpix;
	long naxis;
	long naxes[CFITSIO_MAX_DIM];
	long size;

} CFITSIO_IMAGE_INFO;


typedef enum cfitsio_compression_type 
	{C_NONE, C_IMAGE, C_ROW, C_EXTENDED_FILENAME} CFITSIO_COMPRESSION_TYPE;

//****************************************************************************

int cfitsio_read_keys(char* filename, CFITSIO_KEYWORD** list);
int cfitsio_read_sum_keys(char* filename, CFITSIO_KEYWORD** list);
int cfitsio_print_keys(CFITSIO_KEYWORD* list);

int cfitsio_read_header(char* filename, char** header);
int cfitsio_print_header(char* filename);

int cfitsio_read_image(char* filename, void** image);
int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, 
							  long from_row, long to_row, long from_col, long to_col);

int cfitsio_write_file(char* fits_filename, char* header,  void* image);
int cfitsio_get_image_info(char* header, CFITSIO_IMAGE_INFO* info);

void cfitsio_print_error(int status);

int cfitsio_write_compress(char* fits_filename, char* header ,
						   void* image, 
						   CFITSIO_COMPRESSION_TYPE compression_type);

//****************************************************************************

#endif

