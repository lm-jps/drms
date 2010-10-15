//****************************************************************************

#ifndef __CFITSIO__
#define __CFITSIO__


//#define __CFITSIO_DEBUG__

#include <limits.h>

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
#define CFITSIO_MAX_FORMAT                32

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
#define CFITSIO_ERROR_CANT_GET_FILEINFO -10

//****************************************************************************
// External contants defined in DRMS

#define kFITSRW_Type_String 'C' 
#define kFITSRW_Type_Logical 'L' 	 
#define kFITSRW_Type_Integer 'I'
#define kFITSRW_Type_Float 'F'

typedef void *FITSRW_fhandle;

#ifdef __FOR_LOW_LEVEL_TEST_PROGRAMS__
// defined them here for low level test programs like "testcopy.c" "testimages.c"
const unsigned int kInfoPresent_SIMPLE;
const unsigned int kInfoPresent_EXTEND;
const unsigned int kInfoPresent_BLANK;
const unsigned int kInfoPresent_BSCALE;
const unsigned int kInfoPresent_BZERO;
#else
extern const unsigned int kInfoPresent_SIMPLE;
extern const unsigned int kInfoPresent_EXTEND;
extern const unsigned int kInfoPresent_BLANK;
extern const unsigned int kInfoPresent_BSCALE;
extern const unsigned int kInfoPresent_BZERO;
#endif

//****************************************************************************

typedef union cfitsio_value
{
  char      *vs;
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
      char      key_format[CFITSIO_MAX_FORMAT]; /* Used when writing to FITS file only, 
                                                 * not used when reading from file. */
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
      char fhash[PATH_MAX];  /* key to fitsfile ptr stored in gFFPtrInfo */
} CFITSIO_IMAGE_INFO;


//****************************************************************************
// drms_segment() call only these functions

int fitsrw_read_keylist_and_image_info(FITSRW_fhandle fhandle, 
                                       CFITSIO_KEYWORD** keylistout, 
                                       CFITSIO_IMAGE_INFO** image_info);

int fitsrw_readintfile(int verbose,
                       char* fits_filename,
                       CFITSIO_IMAGE_INFO** image_info,
                       void** image, 
                       CFITSIO_KEYWORD** keylist);

int fitsrw_writeintfile(int verbose,
                        const char* fits_filename,
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
                        void *value,
                        const char *format);

int fitsrw_read(int verbose,
                const char *filename, 
                CFITSIO_IMAGE_INFO** image_info,
                void** image,
                CFITSIO_KEYWORD** keylist);

int fitsrw_write(int verbose,
                 const char* filein,
                 CFITSIO_IMAGE_INFO* info,  
                 void* image,
                 const char* cparms,
                 CFITSIO_KEYWORD* keylist);


int cfitsio_free_keys(CFITSIO_KEYWORD** keylist);

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info);

int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, 
		       long from_row, long to_row, long from_col, long to_col);

int cfitsio_key_to_card(CFITSIO_KEYWORD* key, char* card);

//****************************************************************************

#endif

