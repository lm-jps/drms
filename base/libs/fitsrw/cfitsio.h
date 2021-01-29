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

#define CFITSIO_MAX_DIM		                 9
#define CFITSIO_MAX_BLANK                 32
#define CFITSIO_MAX_STR                   128 /* Use this instead of defines in fitsio.h, otherwise all files that include this file will be dependent on fitsio.h */
#define CFITSIO_MAX_COMMENT               73
#define CFITSIO_MAX_FORMAT                32
#define CFITSIO_MAX_KEYNAME                8
#define CFITSIO_MAX_TFORM                  3

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

/* define keyword-value data types (which are not defined in fitsio for some reason) */
#define CFITSIO_KEYWORD_DATATYPE_NONE    '\0'
#define CFITSIO_KEYWORD_DATATYPE_STRING  'C'
#define CFITSIO_KEYWORD_DATATYPE_LOGICAL 'L'
#define CFITSIO_KEYWORD_DATATYPE_INTEGER 'I'
#define CFITSIO_KEYWORD_DATATYPE_FLOAT   'F'
typedef char cfitsio_keyword_datatype_t;

#define CFITSIO_KEYWORD_HISTORY          "HISTORY"
#define CFITSIO_KEYWORD_COMMENT          "COMMENT"
#define CFITSIO_KEYWORD_CONTINUE         "CONTINUE"

#define CFITSIO_MAX_BINTABLE_WIDTH    1024

typedef enum
{
		CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY = 0,
		CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT
} cfitsio_special_keyword_t;

typedef enum
{
		CFITSIO_FILE_STATE_EMPTY = 0,     /* no fptr, initial cfitsio_file state */
		CFITSIO_FILE_STATE_UNINITIALIZED,   /* has an fptr, but no header or image or table was created (fits_create_...() not called) */
		CFITSIO_FILE_STATE_INITIALIZED    /* has an fptr, fits_create...() also called */
} cfitsio_file_state_t;

typedef enum
{
		CFITSIO_FILE_TYPE_UNKNOWN = 0,
		CFITSIO_FILE_TYPE_HEADER,
		CFITSIO_FILE_TYPE_IMAGE,
		CFITSIO_FILE_TYPE_ASCIITABLE,
		CFITSIO_FILE_TYPE_BINTABLE
} cfitsio_file_type_t;

/* compression enum */
enum __CFITSIO_COMPRESSION_TYPE_enum__
{
#if 0
		/* the FITSIO documentation is misleading; you cannot have compression undefined; images are uncompressed by default */
		CFITSIO_COMPRESSION_UNSET = -1,
#endif
		CFITSIO_COMPRESSION_NONE = 0,
    CFITSIO_COMPRESSION_RICE = 1,
    CFITSIO_COMPRESSION_GZIP1 = 2,
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 27)
    CFITSIO_COMPRESSION_GZIP2 = 3,
#endif
    CFITSIO_COMPRESSION_PLIO = 4,
    CFITSIO_COMPRESSION_HCOMP = 5
};

typedef enum __CFITSIO_COMPRESSION_TYPE_enum__ CFITSIO_COMPRESSION_TYPE;

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
  char     *vs;  // string type
  int       vl;	 // logical type 1: true 0: false
  long long vi;  // long long integer type
  double    vf;  // double type
  // complex number also stored as char string (vs)
} CFITSIO_KEY_VALUE;

typedef struct cfitsio_keyword
{
      char key_name[CFITSIO_MAX_KEYNAME + 1];
      char key_type; // C: string, L: logical, I: integer, F: float, X: complex
      CFITSIO_KEY_VALUE key_value;
      char key_format[CFITSIO_MAX_FORMAT]; /* Used when writing to FITS file only,
                                                 * not used when reading from file. */
      char key_comment[CFITSIO_MAX_STR];
			char key_unit[CFITSIO_MAX_COMMENT];
			char key_tform[CFITSIO_MAX_TFORM + 1];                   /* the tform data type */
			int is_missing; /* set to 1 if there is no key_value */
			int number_bytes; /* for int and float types */
      struct cfitsio_keyword *next;

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
			CFITSIO_COMPRESSION_TYPE export_compression_type; /* used when creating an image only */
} CFITSIO_IMAGE_INFO;

struct __CFITSIO_BINTABLE_TTYPE_struct__
{
		char ttype[CFITSIO_MAX_KEYNAME + 1];
};
typedef struct __CFITSIO_BINTABLE_TTYPE_struct__ CFITSIO_BINTABLE_TTYPE;

struct __CFITSIO_BINTABLE_TFORM_struct__
{
		char tform[CFITSIO_MAX_TFORM + 1];
};
typedef struct __CFITSIO_BINTABLE_TFORM_struct__ CFITSIO_BINTABLE_TFORM;

struct cfitsio_bintable_info
{
		LinkedList_t *rows;                                            /* a list of CFITSIO_KEYWORD * lists; each list contains the keyword values for a single row */
		int tfields;                                                   /* the number of columns in the table; must be constant across all rows */
		CFITSIO_BINTABLE_TTYPE *ttypes[CFITSIO_MAX_BINTABLE_WIDTH];    /* the array of names of columns in the table; must be constant across all rows */
		CFITSIO_BINTABLE_TFORM *tforms[CFITSIO_MAX_BINTABLE_WIDTH];    /* the array of tform data type of columns in the table; must be constant across all rows */
};
typedef struct cfitsio_bintable_info CFITSIO_BINTABLE_INFO;

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

void cfitsio_free_these(CFITSIO_IMAGE_INFO** image_info, void** image, CFITSIO_KEYWORD** keylist);

int cfitsio_create_header_key(const char *name, cfitsio_keyword_datatype_t type, int number_bytes, const void *value, const char *format, const char *comment, const char *unit, CFITSIO_KEYWORD **key_out);

int cfitsio_delete_header_key(CFITSIO_FILE *fitsFile, const char *key);

int cfitsio_delete_headsum(CFITSIO_FILE *fitsFile);

int cfitsio_get_file_state(CFITSIO_FILE *file, cfitsio_file_state_t *state);

int cfitsio_get_file_type_from_fitsfile(CFITSIO_FITSFILE fits_file, cfitsio_file_type_t *type, int *is_initialized, int *old_hdu_index);

int cfitsio_get_file_type(CFITSIO_FILE *file, cfitsio_file_type_t *type);

int cfitsio_create_header(CFITSIO_FILE *file);

int cfitsio_create_image(CFITSIO_FILE *file, CFITSIO_IMAGE_INFO *image_info);

int cfitsio_create_bintable(CFITSIO_FILE *file, CFITSIO_BINTABLE_INFO *bintable_info);

int cfitsio_create_file(CFITSIO_FILE **out_file, const char *file_name, cfitsio_file_type_t file_type, CFITSIO_IMAGE_INFO *image_info, CFITSIO_BINTABLE_INFO *bintable_info, CFITSIO_COMPRESSION_TYPE *compression_type);

int cfitsio_open_file(const char *path, CFITSIO_FILE **fitsFile, int writeable);

void cfitsio_close_file(CFITSIO_FILE **fits_file);

int cfitsio_stream_and_close_file(CFITSIO_FILE **fits_file);

void cfitsio_get_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE *fptr);

int cfitsio_set_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE fptr, int in_memory, cfitsio_file_state_t *state, cfitsio_file_type_t *type);

int cfitsio_get_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE *cfitsio_type);

int cfitsio_get_export_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE *cfitsio_type);

int cfitsio_set_export_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE cfitsio_type);

int cfitsio_get_size(CFITSIO_FILE *file, long long *size);

void cfitsio_close_header(CFITSIO_HEADER **header);

int cfitsio_copy_file(CFITSIO_FILE *source_in, CFITSIO_FILE *dest_in, int copy_header_only);

int cfitsio_write_keys_to_bintable(CFITSIO_FILE *file_out, long long row_number, LinkedList_t *keyword_data);

int cfitsio_copy_header_keywords(CFITSIO_FILE *in_file, CFITSIO_FILE *out_file, CFITSIO_KEYWORD *key_list);

int cfitsio_read_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key);

int cfitsio_update_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key);

int cfitsio_update_header_keywords(CFITSIO_FILE *file, CFITSIO_HEADER *header, CFITSIO_KEYWORD *key_list);

int cfitsio_write_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key);

int cfitsio_write_header_keys(CFITSIO_FILE *file, char **header, CFITSIO_KEYWORD *key_list, CFITSIO_FILE *fits_header);

int cfitsio_flush_buffer(CFITSIO_FILE *fitsFile);

int cfitsio_write_headsum(CFITSIO_FILE *file, const char *headsum);

int cfitsio_write_chksum(CFITSIO_FILE *file);

int cfitsio_write_longwarn(CFITSIO_FILE *file);

int cfitsio_append_header_key(CFITSIO_KEYWORD** keylist, const char *name, cfitsio_keyword_datatype_t type, int number_bytes, const void *value, const char *format, const char *comment, const char *unit, CFITSIO_KEYWORD **key_out);

int cfitsio_generate_checksum(CFITSIO_FILE **fitsFile, CFITSIO_KEYWORD *keyList, char **checksum);

int cfitsio_read_headsum(CFITSIO_FILE *fitsFile, char **headsum);

int cfitsio_key_value_to_cards(CFITSIO_KEYWORD *key, CFITSIO_FILE **cards);

int cfitsio_key_value_to_string(CFITSIO_KEYWORD *key, char **string_value);

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

int cfitsio_free_key(CFITSIO_KEYWORD **key);

int cfitsio_free_keys(CFITSIO_KEYWORD** keylist);

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info);

int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info,
		       long from_row, long to_row, long from_col, long to_col);

int cfitsio_key_to_cards(CFITSIO_KEYWORD *key, LinkedList_t **cards);

//****************************************************************************

#endif
