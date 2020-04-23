//****************************************************************************

#ifndef __CFITSIO__
#define __CFITSIO__


//#define __CFITSIO_DEBUG__

#include <limits.h>
#include "list.h"

#ifdef  __FOR_LOW_LEVEL_TEST_PROGRAMS__
#include <fitsio.h>
#else
/* #include <fitsio.h> Don't include this!Otherwise, every .c file in DRMS
 * will require the -I compile flag.  There is no need to include this header.
 */
#endif

//****************************************************************************

#define CFITSIO_MAX_DIM		                  9
#define CFITSIO_MAX_BLANK                  32
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
#define CFITSIO_ERROR_CANT_COMPRESS     -11
#define CFITSIO_ERROR_INVALID_DATA_TYPE -12

/* define keyword-value data types (which are not defined in fitsio for some reason)*/
#define CFITSIO_KEYWORD_DATATYPE_STRING  'C'
#define CFITSIO_KEYWORD_DATATYPE_LOGICAL 'L'
#define CFITSIO_KEYWORD_DATATYPE_INTEGER 'I'
#define CFITSIO_KEYWORD_DATATYPE_FLOAT   'F'
typedef char cfitsio_keyword_datatype_t;

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
extern const unsigned int kInfoPresent_Dirt;
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
                             /* bit 5 - this bit is the dirty bit; if set then this means that the value of
                              *   naxes[naxis - 1] has changed since the fits file was created; if the value has
                              *   changed, then the NAXISn keyword must be updated when the fits file is closed. */
      char fhash[PATH_MAX];  /* key to fitsfile ptr stored in gFFPtrInfo */
} CFITSIO_IMAGE_INFO;


/* a random fitsfile */
typedef struct cfitsio_file CFITSIO_FILE;

/* a fitsfile that contains just metadata (no image) */
typedef struct cfitsio_file CFITSIO_HEADER;

/* a fitsfile that contains just data (no metadata) */
typedef struct cfitsio_file CFITSIO_DATA;

struct CFITSIO_FITSFILE_struct;

typedef struct CFITSIO_FITSFILE_struct *CFITSIO_FITSFILE;

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

int cfitsio_create_key(const char *name, const char type, const char *comment, const void *value, const char *format, CFITSIO_KEYWORD **keyOut);

int cfitsio_delete_key(CFITSIO_FILE *fitsFile, const char *key);

int cfitsio_delete_headsum(CFITSIO_FILE *fitsFile);

int cfitsio_create_file(CFITSIO_FILE **out_file, const char *file_name, int initialize_fitsfile);

int cfitsio_open_file(const char *path, CFITSIO_FILE **fitsFile, int writeable);

void cfitsio_close_file(CFITSIO_FILE **fitsFile);

void cfitsio_get_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE *fptr);

void cfitsio_set_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE fptr);

void cfitsio_close_header(CFITSIO_HEADER **header);

int cfitsio_copy_file(CFITSIO_FILE *source_in, CFITSIO_FILE *dest_in, int copy_header_only);

int cfitsio_copy_keywords(CFITSIO_FILE *in_file, CFITSIO_FILE *out_file, int simple, CFITSIO_KEYWORD *key_list);

int cfitsio_read_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key);

int cfitsio_update_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key);

int cfitsio_update_keywords(CFITSIO_FILE *file, CFITSIO_HEADER *header, CFITSIO_KEYWORD *key_list);

int cfitsio_flush_buffer(CFITSIO_FILE *fitsFile);

int cfitsio_write_headsum(CFITSIO_FILE *file, const char *headsum);

int cfitsio_append_key(CFITSIO_KEYWORD **keylist, char *name, char type, char *comment, void *value, const char *format);

int cfitsio_generate_checksum(CFITSIO_FILE **fitsFile, CFITSIO_KEYWORD *keyList, char **checksum);

int cfitsio_read_headsum(CFITSIO_FILE *fitsFile, char **headsum);

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

//ISS fly-tar START
#ifndef __export_callback_func_t__
#define __export_callback_func_t__
typedef int (*export_callback_func_t)(char *, ...); //ISS fly-tar
#endif
int fitsrw_write2(int verbose,
                  const char* filein,
                  CFITSIO_IMAGE_INFO* info,
                  void* image,
                  const char* cparms,
                  CFITSIO_KEYWORD* keylist,
                  export_callback_func_t callback); //ISS fly-tar
int fitsrw_write3(int verbose, const char *filein, CFITSIO_IMAGE_INFO *info, void *image, CFITSIO_DATA *fitsData, const char *cparms, CFITSIO_KEYWORD *keylist, CFITSIO_HEADER *fitsHeader, export_callback_func_t callback);
//ISS fly-tar END

int cfitsio_free_keys(CFITSIO_KEYWORD** keylist);

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info);

int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info,
		       long from_row, long to_row, long from_col, long to_col);

int cfitsio_key_to_card(CFITSIO_KEYWORD *kptr, char *card, LinkedList_t **cards);

//****************************************************************************

#endif
