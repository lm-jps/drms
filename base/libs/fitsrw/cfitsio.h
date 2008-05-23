//****************************************************************************

#ifndef __CFITSIO__
#define __CFITSIO__


//#define __CFITSIO_DEBUG__


#ifdef  __FOR_LOW_LEVEL_TEST_PROGRAMS__
#include <fitsio.h>
#else
/* #include <fitsio.h> Don't include this!Otherwise, every .c file in DRMS
 * will require the -I compile flag.  There is no need to include this header.
 */
#endif

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
// External contants defined in DRMS

#ifdef __FOR_LOW_LEVEL_TEST_PROGRAMS__

// defined them here for low level test programs like "testcopy.c" "testimages.c"

const char kFITSRW_Type_String;
const char kFITSRW_Type_Logical;
const char kFITSRW_Type_Integer;
const char kFITSRW_Type_Float;

const unsigned int kInfoPresent_SIMPLE;
const unsigned int kInfoPresent_EXTEND;
const unsigned int kInfoPresent_BLANK;
const unsigned int kInfoPresent_BSCALE;
const unsigned int kInfoPresent_BZERO;

#else

// use external const define in DRMS
extern const char kFITSRW_Type_String;
extern const char kFITSRW_Type_Logical;
extern const char kFITSRW_Type_Integer;
extern const char kFITSRW_Type_Float;

extern const unsigned int kInfoPresent_SIMPLE;
extern const unsigned int kInfoPresent_EXTEND;
extern const unsigned int kInfoPresent_BLANK;
extern const unsigned int kInfoPresent_BSCALE;
extern const unsigned int kInfoPresent_BZERO;

#endif

//****************************************************************************

typedef union cfitsio_value
{
      char      vs[CFITSIO_MAX_STR];
      int       vl;		//logical 1: true 0: false
      long long vi;
      double    vf;
      // complex number also stored as char string (vs)
} CFITSIO_KEY_VALUE;



typedef struct cfitsio_keyword
{
      char	key_name[CFITSIO_MAX_STR]; 
      char	key_type; // C: string, L: logical, I: integer, F: float, X: complex 
      CFITSIO_KEY_VALUE	key_value;
      char	key_comment[CFITSIO_MAX_STR];
      struct cfitsio_keyword*	next;

} CFITSIO_KEYWORD;


typedef	struct cfitsio_image_info
{
      // Require keys for re-creating image
      int bitpix;
      int naxis;
      long naxes[CFITSIO_MAX_DIM];

      // For drms_segment() validity checking
      unsigned int bitfield; /* describes which of the following are present */
      int simple;            /* bit 0 (least significant bit) */
      int extend;            /* bit 1 */
      long long blank;       /* bit 2 */
      double bscale;         /* bit 3 */
      double bzero;          /* bit 4 */
} CFITSIO_IMAGE_INFO;


//****************************************************************************
// drms_segment() call only these functions

int cfitsio_read_file(char* fits_filename,
		      CFITSIO_IMAGE_INFO** image_info,
		      void** image, 
		      CFITSIO_KEYWORD** keylist);


int cfitsio_write_file(const char* fits_filename,
		       CFITSIO_IMAGE_INFO* info,
		       void* image,
		       const char* compspecs,
		       CFITSIO_KEYWORD* keylist); //keylist == NULL if not needed

void cfitsio_free_these(CFITSIO_IMAGE_INFO** image_info,
			void** image, 
			CFITSIO_KEYWORD** keylist);


int cfitsio_append_key(CFITSIO_KEYWORD** keylist, 
			char *name, 
			char type, 
			char *comment,
			void *value);


//****************************************************************************
// For internal use and low level testing...

int cfitsio_read_keys(char* filename, CFITSIO_KEYWORD** keylist);
int cfitsio_print_keys(CFITSIO_KEYWORD* keylist);
int cfitsio_free_keys(CFITSIO_KEYWORD** keylist);

int cfitsio_extract_image_info_from_keylist(CFITSIO_KEYWORD* keylist,
					    CFITSIO_IMAGE_INFO** info);

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info);

int cfitsio_read_image(char* filename, void** image);
int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, 
		       long from_row, long to_row, long from_col, long to_col);
int cfitsio_free_image(void** image);
int cfitsio_get_image_info(CFITSIO_KEYWORD* keys, CFITSIO_IMAGE_INFO* info);

void cfitsio_print_error(int status);
int cfitsio_key_to_card(CFITSIO_KEYWORD* key, char* card);

int cfitsio_gen_image(char* fits_filename, int width, int bitpix, char* center_value);

//****************************************************************************

#endif

