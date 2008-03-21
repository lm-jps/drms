//****************************************************************************

#ifndef __CFITSIO__
#define __CFITSIO__

/* #include <fitsio.h> Don't include this!  Otherwise, every .c file in DRMS
 * will require the -I compile flag.  There is no need to include this header.
 */ 
//****************************************************************************

#define CFITSIO_MAX_DIM		          9
#define CFITSIO_MAX_BLANK                 32
#define CFITSIO_MAX_STR                   128 /* Use this instead of defines in 
					       * fitsio.h, otherwise all files
					       * that include this file will 
					       * be dependent on fitsio.h */

#define CFITSIO_SUCCESS                   0
#define CFITSIO_FAIL                     -1

#define CFITSIO_ERROR_FILE_DOESNT_EXIST  -2
#define CFITSIO_ERROR_OUT_OF_MEMORY      -3
#define CFITSIO_ERROR_LIBRARY            -4
#define CFITSIO_ERROR_FILE_IO            -5
#define CFITSIO_ERROR_ARGS               -6
#define CFITSIO_ERROR_DATA_EMPTY         -7
#define CFITSIO_ERROR_ALREADY_COMPRESSED -8
#define CFITSIO_ERROR_INVALIDFILE        -9

//****************************************************************************

extern const char kFITSRW_Type_String;
extern const char kFITSRW_Type_Logical;
extern const char kFITSRW_Type_Integer;
extern const char kFITSRW_Type_Float;

typedef union cfitsio_value
{
      char      vs[CFITSIO_MAX_STR];
      int       vl;		//logical 1: true 0: false
      long      vi;
      double    vf;
      // complex number also stored as char string (vs)
} CFITSIO_KEY_VALUE;



typedef struct cfitsio_keyword
{
      char	key_name[CFITSIO_MAX_STR]; 
      char	key_type; // C: string, L: logical, I: integer, F: float, X: complex 
      CFITSIO_KEY_VALUE	key_value;
      char	key_comment[CFITSIO_MAX_STR];
      char	key_printf_format[CFITSIO_MAX_STR];
      struct cfitsio_keyword*	next; // For fixed card size, just access as array element and return "nkeys"

} CFITSIO_KEYWORD;

extern const unsigned int kInfoPresent_SIMPLE;
extern const unsigned int kInfoPresent_EXTEND;
extern const unsigned int kInfoPresent_BLANK;
extern const unsigned int kInfoPresent_BSCALE;
extern const unsigned int kInfoPresent_BZERO;

typedef	struct cfitsio_image_info
{
      // Require keys for re-creating image
      int bitpix;
      int naxis;
      long naxes[CFITSIO_MAX_DIM];
      int nkeys;

      // For drms_segment() validity checking
      unsigned int bitfield; /* describes which of the following are present */
      int simple;            /* bit 0 (least significant bit) */
      int extend;            /* bit 1 */
      long long blank;       /* bit 2 */
      double bscale;         /* bit 3 */
      double bzero;          /* bit 4 */
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
// For drms_segment()

int cfitsio_read_file(char* fits_filename, CFITSIO_IMAGE_INFO** image_info, void** image, 
		      CFITSIO_KEYWORD** keylist);


int cfitsio_write_file(const char* fits_filename, CFITSIO_IMAGE_INFO* info,  void* image, 
		       CFITSIO_COMPRESSION_TYPE compression_type,  
		       CFITSIO_KEYWORD* keylist); //keylist == NULL if not needed

void cfitsio_free_these(CFITSIO_IMAGE_INFO** image_info, void** image, 
			CFITSIO_KEYWORD** keylist);


//****************************************************************************
// Using CFITSIO_KEYWORD (single linked list) to hold keys, values, comments


int cfitsio_read_keys(char* filename, CFITSIO_KEYWORD** list);
int cfitsio_print_keys(CFITSIO_KEYWORD* list);
int cfitsio_free_keys(CFITSIO_KEYWORD** keys);
void cfitsio_free_keylist(CFITSIO_KEYWORD** keylist);
int cfitsio_keys_insert(CFITSIO_KEYWORD** list, 
			const char *name, 
			char type, 
			const char *comment,
			const char *format,
			void *data);

int cfitsio_read_image(char* filename, void** image);
int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, 
		       long from_row, long to_row, long from_col, long to_col);
int cfitsio_free_image(void** image);
int cfitsio_get_image_info(CFITSIO_KEYWORD* keys, CFITSIO_IMAGE_INFO* info);

void cfitsio_print_error(int status);
int cfitsio_key_to_card(CFITSIO_KEYWORD* key, char* card);


//****************************************************************************
// Using header (char array) to keep also HISTORY, COMMENT ... cards 

#ifdef __USING_HEADER__

int cfitsio_read_header(char* filename, char** header);
int cfitsio_print_header(char* filename);
int cfitsio_free_header(char** header);

int cfitsio_keylist_to_header(CFITSIO_KEYWORD* keylist, char** header);
int cfitsio_header_to_keylist(char* header, CFITSIO_KEYWORD** keylist);

int cfitsio_write_file_using_header(char* fits_filename, char* header,  void* image,
				    CFITSIO_COMPRESSION_TYPE compression_type);
int cfitsio_get_image_info_using_header(char* header, CFITSIO_IMAGE_INFO* info);

int cfitsio_write_compress_using_header(char* fits_filename, char* header , void* image, 
					CFITSIO_COMPRESSION_TYPE compression_type );
#endif //__USING_HEADER__

//****************************************************************************

#endif

