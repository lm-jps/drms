/****************************************************************************/
// CFITSIO.C
//
// Functions for read/write FITS file format  using CFITSIO library.
//
//
/****************************************************************************/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <regex.h>
#include "fitsio.h"
#include "cfitsio.h"
#include "jsoc.h"
#include "foundation.h"
#include "tasrw.h"
#include "list.h"
#include "hcontainer.h"
#include "util.h"
#include "xassert.h"

/****************************************************************************/

#ifdef  __CFISTIO_DEBUG__           //Turn on/off DEBUGMSG
#define DEBUGMSG(msg)  printf msg   //ex: DEBUGMSG ((stderr, "Hello World %d\n", 123));
#else
#define DEBUGMSG(msg)  //nop
#endif

#define HEADSUM "HEADSUM"

const unsigned int kInfoPresent_SIMPLE = 0x00000001;
const unsigned int kInfoPresent_EXTEND = 0x00000002;
const unsigned int kInfoPresent_BLANK  = 0x00000004;
const unsigned int kInfoPresent_BSCALE = 0x00000008;
const unsigned int kInfoPresent_BZERO  = 0x00000010;
const unsigned int kInfoPresent_Dirt   = 0x00000020;

#define kMISSPIXBLOCK 1310720 /* 10 MB of long long */

struct cfitsio_file
{
    fitsfile *fptr;
    int in_memory; /* 1 if the file exists in memory only; closing the file will flush it to stdout */
    cfitsio_file_state_t state;
    cfitsio_file_type_t type;
    CFITSIO_COMPRESSION_TYPE compression_type; /* the current compression type */
    CFITSIO_COMPRESSION_TYPE export_compression_type; /* the type to export (write to disk or stream) as */
};

struct CFITSIO_FITSFILE_struct
{
    fitsfile *fptr;
};


/* Half-assed - just enough to get past a bug.  Should really check that the value
 * doesn't fall outside the range of the destination type.
 */
static int cfitsio_keyval2int(const CFITSIO_KEYWORD *kw, int *stat)
{
   int val = 0;
   int err = 1;

   if (kw)
   {
      switch(kw->key_type)
      {
         case kFITSRW_Type_String:
           if (kw->key_value.vs)
           {
              err = (sscanf(kw->key_value.vs, "%d", &val) != 1);
           }
           else
           {
              /* No string to convert - default to 0. */
              err = 0;
           }
           break;
         case kFITSRW_Type_Logical:
           val = (int)(kw->key_value.vl);
           err = 0;
           break;
         case kFITSRW_Type_Integer:
           val = (int)(kw->key_value.vi);
           err = 0;
           break;
         case kFITSRW_Type_Float:
           val = (int)(kw->key_value.vf);
           err = 0;
           break;
         default:
           fprintf(stderr, "Unsupported data type '%d'.", kw->key_type);
      }
   }

   if (stat)
   {
      *stat = err;
   }

   return val;
}

static long long cfitsio_keyval2longlong(const CFITSIO_KEYWORD *kw, int *stat)
{
   long long val = 0;
   int err = 1;

   if (kw)
   {
      switch(kw->key_type)
      {
         case kFITSRW_Type_String:
           if (kw->key_value.vs)
           {
              err = (sscanf(kw->key_value.vs, "%lld", &val) != 1);
           }
           else
           {
              err = 0;
           }
           break;
         case kFITSRW_Type_Logical:
           val = (long long)(kw->key_value.vl);
           err = 0;
           break;
         case kFITSRW_Type_Integer:
           val = (long long)(kw->key_value.vi);
           err = 0;
           break;
         case kFITSRW_Type_Float:
           val = (long long)(kw->key_value.vf);
           err = 0;
           break;
         default:
           fprintf(stderr, "Unsupported data type '%d'.", kw->key_type);
      }
   }

   if (stat)
   {
      *stat = err;
   }

   return val;
}

static double cfitsio_keyval2double(const CFITSIO_KEYWORD *kw, int *stat)
{
   double val = 0;
   int err = 1;

   if (kw)
   {
      switch(kw->key_type)
      {
         case kFITSRW_Type_String:
           if (kw->key_value.vs)
           {
              err = (sscanf(kw->key_value.vs, "%lf", &val) != 1);
           }
           else
           {
              err = 0;
           }
           break;
         case kFITSRW_Type_Logical:
           val = (double)(kw->key_value.vl);
           err = 0;
           break;
         case kFITSRW_Type_Integer:
           val = (double)(kw->key_value.vi);
           err = 0;
           break;
         case kFITSRW_Type_Float:
           val = (double)(kw->key_value.vf);
           err = 0;
           break;
         default:
           fprintf(stderr, "Unsupported data type '%d'.", kw->key_type);
      }
   }

   if (stat)
   {
      *stat = err;
   }

   return val;
}

static void Cf_drop_data_and_close_fits_file(fitsfile *fitsPtr)
{
    long long numBytesFitsFile = fitsPtr->Fptr->logfilesize;
    int savedStdout = -1;
    char fileBuf[4096];
    int num;
    int fiostat = 0;
    int devnull = -1;

    if (numBytesFitsFile > 0)
    {
        /* open /dev/null */
        devnull = open("/dev/null", O_WRONLY);

        if (devnull == -1)
        {
            fprintf(stderr, "unable to open /dev/null for writing\n");
        }
        else
        {
            savedStdout = dup(STDOUT_FILENO);
            if (savedStdout != -1)
            {
                if (dup2(devnull, STDOUT_FILENO) != -1)
                {
                    fiostat = 0;
                    fits_close_file(fitsPtr, &fiostat);
                    if (fiostat)
                    {
                        fprintf(stderr, "unable to close and send FITS file\n");
                    }

                    fflush(stdout);
                }
                else
                {
                    /* can't flush FITSIO internal buffers */
                    fprintf(stderr, "unable to flush FITSIO internal buffers following error\n");
                }

                /* restore stdout */
                dup2(savedStdout, STDOUT_FILENO);
            }
            else
            {
                /* can't flush FITSIO internal buffers */
                fprintf(stderr, "unable to flush FITSIO internal buffers following error\n");
            }

            if (savedStdout != -1)
            {
                close(savedStdout);
                savedStdout = -1;
            }

            close(devnull);
            devnull = -1;
        }
    }
}

static void Cf_close_file(fitsfile **fptr, int drop_content)
{
    CFITSIO_IMAGE_INFO fpinfo;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];


    if (fptr && *fptr)
    {
        /* if fitsfile is cached, remove from cache and close file; cannot currently drop data if the file is cached */
        if (!fitsrw_getfpinfo_ext((TASRW_FilePtr_t)*fptr, (TASRW_FilePtrInfo_t)&fpinfo))
        {
            /* writes FITS keyword data and image checksums */
            fitsrw_closefptr(0, (TASRW_FilePtr_t)*fptr);
        }
        else
        {
            if (drop_content)
            {
                Cf_drop_data_and_close_fits_file(*fptr);
            }
            else
            {
                fits_close_file(*fptr, &cfiostat);

                if (cfiostat)
                {
                    fits_get_errstatus(cfiostat, cfiostat_msg);
                    fprintf(stderr, "[ Cf_close_file() ] %s\n", cfiostat_msg);
                }
            }
        }

        *fptr = NULL;
    }
}

static int Cf_stream_and_close_file(fitsfile **fptr)
{
    CFITSIO_IMAGE_INFO fpinfo;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    if (fptr && *fptr)
    {
        /* if fitsfile is cached, remove from cache and close file; cannot currently drop data if the file is cached */
        if (!fitsrw_getfpinfo_ext((TASRW_FilePtr_t)*fptr, (TASRW_FilePtrInfo_t)&fpinfo))
        {
            /* writes FITS keyword data and image checksums */
            fitsrw_closefptr(0, (TASRW_FilePtr_t)*fptr);
        }
        else
        {
            fits_close_file(*fptr, &cfiostat);

            if (cfiostat)
            {
                fits_get_errstatus(cfiostat, cfiostat_msg);
                fprintf(stderr, "[ Cf_stream_and_close_file() ] %s\n", cfiostat_msg);
                err = CFITSIO_ERROR_LIBRARY;
            }
        }

        *fptr = NULL;
    }

    return err;
}

/* returns 1 if NULL, 0 otherwise */
static int is_keyword_unit_null(const char *keyword_unit)
{
    int answer = -1; // -1 means error
    char *stripped = NULL;

    answer = base_strip_whitespace(keyword_unit, &stripped);
    if (answer == 1)
    {
        answer = -1;
    }

    if (answer != -1)
    {
        answer = (strcasecmp(stripped, CFITSIO_KEYWORD_UNIT_NONE) == 0 || strcasecmp(stripped, CFITSIO_KEYWORD_UNIT_NA) == 0 || strcasecmp(stripped, CFITSIO_KEYWORD_UNIT_NSLASHA) == 0 || *stripped == '\0');
    }

    if (stripped)
    {
        free(stripped);
        stripped = NULL;
    }

    return answer;
}

static int Cf_write_key_null(fitsfile *fptr, const char *keyord_name, const char *keyword_comment, const char *keyword_unit)
{
    char *comment = NULL;
    size_t sz_comment = FLEN_COMMENT + 1;
    int omit_unit = -1;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    comment = calloc(sz_comment, sizeof(char));
    if (!comment)
    {
        err = CFITSIO_ERROR_OUT_OF_MEMORY;
    }

    if (!err)
    {
        /* if unit is 'none' or 'na' or missing, then omit [<unit>] */
        if (!is_keyword_unit_null(keyword_unit))
        {
            comment = base_strcatalloc(comment, "[", &sz_comment);
            comment = base_strcatalloc(comment, keyword_unit, &sz_comment);
            comment = base_strcatalloc(comment, "] ", &sz_comment);
        }

        comment = base_strcatalloc(comment, "(", &sz_comment);
        comment = base_strcatalloc(comment, CFITSIO_KEYWORD_COMMENT_MISSING, &sz_comment);
        comment = base_strcatalloc(comment, ")", &sz_comment);

        if (*keyword_comment != '\0')
        {
            comment = base_strcatalloc(comment, " ", &sz_comment);
            comment = base_strcatalloc(comment, keyword_comment, &sz_comment);
        }

        fits_write_key_null(fptr, keyord_name, comment, &cfiostat);
    }

    if (comment)
    {
        free(comment);
        comment = NULL;
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_write_key_null() ] error writing key %s: %s\n", keyord_name, cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

static int Cf_read_history_or_comment_key(fitsfile *fptr, cfitsio_special_keyword_t special_key_type, char **value_out)
{
    int number_keys = -1;
    char *include_list = NULL;
    char card[FLEN_CARD] = {0};
    char card_value[FLEN_CARD] = {0};
    char card_comment[FLEN_CARD] = {0};
    char *history_or_comment = NULL;
    size_t sz_history_or_comment = FLEN_CARD + 1;
    char comment[CFITSIO_MAX_STR] = {0};
    char unit[FLEN_COMMENT] = {0};
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    if (special_key_type == CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY)
    {
        include_list = CFITSIO_KEYWORD_HISTORY;
    }
    else if (special_key_type == CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT)
    {
        include_list = CFITSIO_KEYWORD_COMMENT;
    }
    else
    {
        fprintf(stderr, "invalid special key type %d\n", (int)special_key_type);
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* make sure we have at least one keyowrd in `file` */
        if (fits_get_hdrspace(fptr, &number_keys, NULL, &cfiostat))
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* go to beginning of header */
        if (fits_read_record(fptr, 0, NULL, &cfiostat))
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    while (err == CFITSIO_SUCCESS)
    {
        if (fits_find_nextkey(fptr, (char **)&include_list, 1, NULL, 0, card, &cfiostat))
        {
            if (cfiostat == KEY_NO_EXIST)
            {
                /* no more HISTORY/COMMENT records - error if we never found any, but we check for this below */
                cfiostat = 0;
                break;
            }
            else
            {
                err = CFITSIO_ERROR_LIBRARY;
            }
        }
        else
        {
            if (history_or_comment == NULL)
            {
                history_or_comment = calloc(1, sz_history_or_comment);
                if (!history_or_comment)
                {
                    err = CFITSIO_ERROR_OUT_OF_MEMORY;
                }
            }
            else
            {
                /* we always delimit HISTORY/COMMENT lines with newline characters so that we can preserve
                 * the allocation of these strings among FITS records  */
                history_or_comment = base_strcatalloc(history_or_comment, "\n", &sz_history_or_comment);
            }

            if (err == CFITSIO_SUCCESS)
            {
                /* the value of a HISTORY/COMMENT keyword is actually in the comment field */
                if (fits_parse_value(card, card_value, card_comment, &cfiostat))
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }
                else
                {
                    history_or_comment = base_strcatalloc(history_or_comment, card_comment, &sz_history_or_comment);
                    if (!history_or_comment)
                    {
                        err = CFITSIO_ERROR_OUT_OF_MEMORY;
                    }
                }
            }
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        if (history_or_comment)
        {
            /* HISTORY/COMMENT found */
            if (value_out)
            {
                *value_out = history_or_comment;
            }

            history_or_comment = NULL; // yoink!
        }
        else
        {
            fprintf(stderr, "HISTORY/COMMENT keyword not found\n");
            err = CFITSIO_ERROR_ARGS;
        }
    }

    if (history_or_comment)
    {
        free(history_or_comment);
        history_or_comment = NULL;
    }

    return err;
}

static int Cf_delete_history_or_comment_record(fitsfile *fptr, cfitsio_special_keyword_t special_key_type)
{
    char *special_keyword = NULL;
    int number_keys = -1;
    int err = CFITSIO_SUCCESS;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];

    if (special_key_type == CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY)
    {
        special_keyword = CFITSIO_KEYWORD_HISTORY;
    }
    else if (special_key_type == CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT)
    {
        special_keyword = CFITSIO_KEYWORD_COMMENT;
    }
    else
    {
        fprintf(stderr, "invalid special key type %d\n", (int)special_key_type);
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* make sure we have at least one keyowrd in `file` */
        if (fits_get_hdrspace(fptr, &number_keys, NULL, &cfiostat))
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* go to beginning of header */
        if (fits_read_record(fptr, 0, NULL, &cfiostat))
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    /* not sure if fits_delete_key() deletes only a single keyword or multiple ones, so call in a loop */
    while (err == CFITSIO_SUCCESS)
    {
        if (fits_delete_key(fptr, special_keyword, &cfiostat))
        {
            if (cfiostat == KEY_NO_EXIST)
            {
                cfiostat = 0;
                break;
            }
            else
            {
                err = CFITSIO_ERROR_LIBRARY;
            }
        }
    }

    if (cfiostat != 0)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_delete_history_or_comment_record() ] unable to read keyword '%s'\n", special_keyword);
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        /* err will be set (if cfiostat != 0 ==> err)*/
    }

    return err;
}

static int Cf_write_history_or_comment_key(fitsfile *fptr, cfitsio_special_keyword_t special_key_type, const char *value)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;
    char *working_value = NULL;
    size_t value_length = 0;
    char *p_newline = NULL;
    char *p_current_char = NULL;

    if (value && *value != '\0')
    {
        value_length = strlen(value);
        working_value = strdup(value);

        if (!working_value)
        {
            err = CFITSIO_ERROR_OUT_OF_MEMORY;
        }

        if (err == CFITSIO_SUCCESS)
        {
            if (special_key_type == CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY)
            {
                /* break on newlines */
                p_current_char = working_value;
                while ((p_newline = strchr(p_current_char, '\n')) != NULL)
                {
                    *p_newline = '\0';
                    if (fits_write_history(fptr, p_current_char, &cfiostat))
                    {
                        err = CFITSIO_ERROR_LIBRARY;
                        break;
                    }

                    p_current_char = p_newline + 1;

                    if (p_current_char >= working_value + value_length)
                    {
                        break;
                    }
                }

                if (err == CFITSIO_SUCCESS)
                {
                    /* do the last one */
                    if (p_current_char < working_value + value_length)
                    {
                        fits_write_history(fptr, p_current_char, &cfiostat);
                    }
                }
            }
            else if (special_key_type == CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT)
            {
                /* break on newlines */
                p_current_char = working_value;
                while ((p_newline = strchr(p_current_char, '\n')) != NULL)
                {
                    *p_newline = '\0';
                    if (fits_write_comment(fptr, p_current_char, &cfiostat))
                    {
                        err = CFITSIO_ERROR_LIBRARY;
                        break;
                    }

                    p_current_char = p_newline + 1;

                    if (p_current_char >= working_value + value_length)
                    {
                        break;
                    }
                }

                if (err == CFITSIO_SUCCESS)
                {
                    /* do the last one */
                    if (p_current_char < working_value + value_length)
                    {
                        fits_write_comment(fptr, p_current_char, &cfiostat);
                    }
                }
            }
            else
            {
                err = CFITSIO_ERROR_ARGS;
            }
        }
    }
    else
    {
        fprintf(stderr, "[ Cf_write_history_or_comment_key() ] no HISTORY value provided\n");
        err = CFITSIO_ERROR_ARGS;
    }

    if (working_value)
    {
        free(working_value);
        working_value = NULL;
    }

    if (cfiostat != 0)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_write_history_or_comment_key() ] unable to read HISTORY/COMMENT keyword\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        /* err will be set (if cfiostat != 0 ==> err)*/
    }

    if (err)
    {
        fprintf(stderr, "[ Cf_write_history_or_comment_key() ] failed to write HISTORY/COMMENT keyword\n");
    }

    return err;
}

static int Cf_write_header_key(fitsfile *fptr, CFITSIO_KEYWORD *key)
{
    int is_history_or_comment = 0;
    int cfiostat = 0;
    char comment[FLEN_COMMENT];
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    /* the keyword comment has this format:
     *   [<unit>] commentary {<corresponding DRMS keyword name>}
    */
    if (key->is_missing)
    {
        /* do not write unit at this time if the value is missing; FITSIO has a bug in fits_write_key_unit() -
         * if the value is missing, then fits_write_key_unit() will cause fits_write_key_null() will omit the
         * "= " in bytes 9 and 10 of the keyword card; to work around this FITSIO bug, manually insert
         * the unit string here into string passed to fits_write_key_null()
         */
        err = Cf_write_key_null(fptr, key->key_name, key->key_comment, key->key_unit);
    }
    else
    {
        if (key->key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
        {
            /* any string could be a long string (> 68 chars) so use fits_read_key_longstr() */
            if (strcmp(key->key_name, CFITSIO_KEYWORD_HISTORY) == 0)
            {
                is_history_or_comment = 1;

                if (*key->key_value.vs != '\0')
                {
                    err = Cf_write_history_or_comment_key(fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY, key->key_value.vs);
                }
            }
            else if (strcmp(key->key_name, CFITSIO_KEYWORD_COMMENT) == 0)
            {
                is_history_or_comment = 1;

                if (*key->key_value.vs != '\0')
                {
                    err = Cf_write_history_or_comment_key(fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT, key->key_value.vs);
                }
            }
            else if (strcmp(key->key_name, CFITSIO_KEYWORD_CONTINUE) == 0)
            {
                /* error - fits_write_key_longstr() will be used to generate CONTINUE keywords */
            }
            else
            {
                fits_write_key_longstr(fptr, key->key_name, key->key_value.vs, key->key_comment ? key->key_comment : NULL, &cfiostat);
            }
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_LOGICAL)
        {
            fits_write_key_log(fptr, key->key_name, key->key_value.vl, key->key_comment ? key->key_comment : NULL, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_INTEGER)
        {
            fits_write_key_lng(fptr, key->key_name, key->key_value.vi, key->key_comment ? key->key_comment : NULL, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_FLOAT)
        {
            /* a NaN value implies a missing value; write a keyword with no value (null) */
            if (key->number_bytes == 4)
            {
                /* prints 9 decimal places, if needed */
                if (fits_write_key_flt(fptr, key->key_name, (float)key->key_value.vf, -9, key->key_comment ? key->key_comment : NULL, &cfiostat))
                {
                    fprintf(stderr, "[ Cf_write_header_key() ] NOTE: invalid value for DRMS floating-point keyword %s; continuing\n", key->key_name);
                    key->is_missing = 1;
                    err = Cf_write_key_null(fptr, key->key_name, key->key_comment, key->key_unit);
                    cfiostat = 0;
                }
            }
            else if (key->number_bytes == 8)
            {
                /* prints 17 decimal places, if needed */
                if (fits_write_key_dbl(fptr, key->key_name, key->key_value.vf, -17, key->key_comment ? key->key_comment : NULL, &cfiostat))
                {
                    fprintf(stderr, "[ Cf_write_header_key() ] NOTE: invalid value for DRMS floating-point keyword %s; continuing\n", key->key_name);
                    key->is_missing = 1;
                    err = Cf_write_key_null(fptr, key->key_name, key->key_comment, key->key_unit);
                    cfiostat = 0;
                }
            }
            else
            {
                fprintf(stderr, "[ Cf_write_header_key() ] NOTE: invalid number of bytes specified for `%c` keyword `%s`; continuing\n", key->key_type, key->key_name);
                key->is_missing = 1;
                err = Cf_write_key_null(fptr, key->key_name, key->key_comment, key->key_unit);
                cfiostat = 0;
            }
        }
        else
        {
            err = CFITSIO_ERROR_INVALID_DATA_TYPE;
        }

        /* write the keyword unit substring, but only if the keyword value is not nul */
        if (err == CFITSIO_SUCCESS)
        {
            if (!is_history_or_comment)
            {
                if (!is_keyword_unit_null(key->key_unit))
                {
                    fits_write_key_unit(fptr, key->key_name, key->key_unit, &cfiostat);
                }
            }
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_write_header_key() ] error writing key %s: %s\n",  key->key_name, cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

/* RE-DO all of this to use FITSIO keyword-writing functions
 *
 * `fptr_out` is the FITS file (on disk or in memory) keys are being written to (can be NULL)
 * `header` is the buffer that the FITSIO header is being written to (if fptr_out is NULL)
 * `key_list` is the linked-list of CFITSIO_KEYWORD structs containing the keyword names/values
 * `fits_header` is alternate way of providing keywords to write to fptr_out */
static int Cf_write_header_keys(fitsfile *fptr_out, char **header, CFITSIO_KEYWORD *key_list, fitsfile *fits_header)
{
    fitsfile *temp_file = NULL;
    fitsfile *fptr = NULL;
    int key_class = -1;
    char card[FLEN_CARD];
    char *pCard = NULL;
    LinkedList_t *cards = NULL;
    ListNode_t *node = NULL;
    CFITSIO_KEYWORD* key_node = NULL;
    size_t header_sz = 2880;
    int num_keys = 0;
    int key_num = 0;
    int err = CFITSIO_SUCCESS;
    int cfiostat = CFITSIO_SUCCESS;
    char cfiostat_msg[FLEN_STATUS];

    /* since we are going to calculate a checksum of the entire header, create an in-memory
     * FITS file and write the header to that file, then generate the checksum on that
     * in-memory file; then copy the header from the in-memory FITS file to the
     * FITS file destined for disk (fptr); then close the in-memory FITS file, first
     * dropping all content so that it does not get flushed to stdout */

    if (!fptr_out)
    {
        XASSERT(header);

        if (fits_create_file(&temp_file, "-", &cfiostat))
        {
            fprintf(stderr, "[ Cf_write_header_keys() ] unable to create FITS file\n");
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (!err)
    {
        fptr = fptr_out ? fptr_out : temp_file;

        if (key_list)
        {
            /* header keywords exist in a CFITSIO_KEYWORD list */
            key_node = key_list;

            while (key_node)
            {
                key_class = fits_get_keyclass(key_node->key_name);
                if (key_class != TYP_STRUC_KEY  && key_class != TYP_CMPRS_KEY && key_class != TYP_CONT_KEY)
                {
                    /* properly handles HISTORY and COMMENT and long strings */
                    if (Cf_write_header_key(fptr, key_node))
                    {
                        fprintf(stderr, "[ Cf_write_header_keys() ] unable to write FITS keyword '%s'\n", key_node->key_name);
                    }
                }

                key_node = key_node->next;
            }
        }
        else if (fits_header)
        {
            /* header keywords exist in an actual fitsfile header */
            *card = '\0';

            fits_get_hdrspace(fits_header, &num_keys, NULL, &cfiostat);

            if (!cfiostat)
            {
                for (key_num = 1; key_num < num_keys; key_num++)
                {
                    fits_read_record(fits_header, key_num, card, &cfiostat);

                    if (!cfiostat)
                    {
                        key_class = fits_get_keyclass(card);

                        /* since we are copying FITS cards directly, do not exclude CONTINUE records */
                        if (key_class == TYP_STRUC_KEY  || key_class == TYP_CMPRS_KEY)
                        {
                            continue;
                        }

                        if (fits_write_record(fptr, card, &cfiostat))
                        {
                            fits_get_errstatus(cfiostat, cfiostat_msg);
                            fprintf(stderr, "CFITSIO error writing card '%s': %s\n", card, cfiostat_msg);
                        }
                    }
                }
            }
        }
    }

    /* if dumping to *header, read cards from temp_file, dumping into *header */
    if (!fptr_out)
    {
        *card = '\0';
        fits_get_hdrspace(temp_file, &num_keys, NULL, &cfiostat);

        if (!cfiostat)
        {
            for (key_num = 1; key_num < num_keys; key_num++)
            {
                fits_read_record(temp_file, key_num, card, &cfiostat);

                if (!cfiostat)
                {
                    if (!*header)
                    {
                        *header = calloc(1, header_sz);
                    }

                    if (!*header)
                    {
                        err = CFITSIO_ERROR_OUT_OF_MEMORY;
                        break;
                    }

                    *header = base_strcatalloc(*header, card, &header_sz);
                }
            }
        }
    }

    if (temp_file)
    {
        Cf_close_file(&temp_file, 1);
    }

    return err;
}

/* fptr_out - the BINTABLE fitsfile to which DRMS keyword data are written
 * keyword_data - the DRMS keyword data (in FITS keyword format) to be written to the BINTABLE; a LinkedList_t of CFITSIO_KEYWORD * lists, one for each record
 * [ header_data ] - an alternative way to supply the DRMS keyword data ; a LinkedList_t of CFITSIO_FILE *
 */

static int Cf_write_keys_to_bintable(fitsfile *fptr_out, long long row_number, LinkedList_t *keyword_data)
{
    ListNode_t *node = NULL;
    CFITSIO_KEYWORD *key_list = NULL;
    CFITSIO_KEYWORD* key = NULL;
    int fits_tfields = -1; /* column */
    int row = -1; /* row */
    int fits_data_type = -1;
    int number_bytes = -1;
    char *data_string = NULL;
    char data_logical = 0;
    char data_byte = 0;
    short data_short = 0;
    int data_int = 0;
    long long data_longlong = 0;
    float data_float = 0;
    double data_double = 0;
    void *data = NULL;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_err_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    /* recnum is one of the keywords */
    if (keyword_data)
    {
        row = row_number;
        list_llreset(keyword_data);
        while ((err == CFITSIO_SUCCESS) && ((node = list_llnext(keyword_data)) != NULL))
        {
            /* iterate over rows; each key_list is for a single DRMS record */
            key_list = *(CFITSIO_KEYWORD **)node->data;
            key = key_list;

            fits_tfields = 0;
            while ((err == CFITSIO_SUCCESS) && key)
            {
                /* iterate over columns */
                data = NULL;
                number_bytes = key->number_bytes;

                if (!key->is_missing)
                {
                    switch (key->key_type)
                    {
                        case CFITSIO_KEYWORD_DATATYPE_STRING:
                            fits_data_type = TSTRING;
                            data_string = key->key_value.vs;
                            data = &data_string;
                            break;
                        case CFITSIO_KEYWORD_DATATYPE_LOGICAL:
                            fits_data_type = TLOGICAL;
                            data_logical = (char)key->key_value.vl;
                            data = &data_logical;
                            break;
                        case CFITSIO_KEYWORD_DATATYPE_INTEGER:
                            if (number_bytes == 1)
                            {
                                fits_data_type = TSBYTE;
                                data_byte = (char)key->key_value.vi;
                                data = &data_byte;
                            }
                            else if (number_bytes == 2)
                            {
                                fits_data_type = TSHORT;
                                data_short = (short)key->key_value.vi;
                                data = &data_short;
                            }
                            else if (number_bytes == 4)
                            {
                                fits_data_type = TINT;
                                data_int = (int)key->key_value.vi;
                                data = &data_int;
                            }
                            else if (number_bytes == 8)
                            {
                                fits_data_type = TLONGLONG;
                                data_longlong = (long long)key->key_value.vi;
                                data = &data_longlong;
                            }
                            else
                            {
                                fprintf(stderr, "[ Cf_write_keys_to_bintable() ] invalid number_bytes value %d\n", number_bytes);
                                err = CFITSIO_ERROR_ARGS;
                            }

                            break;
                        case CFITSIO_KEYWORD_DATATYPE_FLOAT:
                            if (number_bytes == 4)
                            {
                                fits_data_type = TFLOAT;
                                data_float = (float)key->key_value.vf;
                                data = &data_float;
                            }
                            else if (number_bytes == 8)
                            {
                                fits_data_type = TDOUBLE;
                                data_double = (double)key->key_value.vf;
                                data = &data_double;
                            }
                            else
                            {
                                fprintf(stderr, "[ Cf_write_keys_to_bintable() ] invalid number_bytes value %d\n", number_bytes);
                                err = CFITSIO_ERROR_ARGS;
                            }

                            break;
                        default:
                            fprintf(stderr, "invalid CFITSIO keyword data type %d; skipping keyword %s\n", key->key_type, key->key_name);
                    }
                }

                if (data)
                {
                    /* I think that it is OK for a BINTABLE cell (an element in a BINTABLE column) to be undefined;
                     * fits_read_col() will return in the returned array a value equal to the value stored in
                     * the `nuval` argument; so if
                     *   longlong my_nulval = -9223372036854775808, then the first element in the returne array will
                     *   will be -9223372036854775808
                     * */
                    fits_write_col(fptr_out, fits_data_type, fits_tfields + 1, row + 1, 1, 1, data, &cfiostat);

                    if (cfiostat)
                    {
                        fits_get_errstatus(cfiostat, cfiostat_err_msg);
                        fprintf(stderr, "[ Cf_write_keys_to_bintable() ] CFITSIO error '%s'\n", cfiostat_err_msg);
                        err = CFITSIO_ERROR_LIBRARY;
                        break;
                    }
                }

                ++fits_tfields;

                if (fits_tfields > CFITSIO_MAX_BINTABLE_WIDTH)
                {
                    fprintf(stderr, "[ Cf_write_keys_to_bintable() ] too many keywords - the mamimum allowed is %d\n", CFITSIO_MAX_BINTABLE_WIDTH);
                    err = CFITSIO_ERROR_ARGS;
                    break;
                }

                key = key->next; /* next key in row */
            } /* column */

            ++row;
        } /* rows */
    }

    return err;
}

/* file_out - the BINTABLE fitsfile to which DRMS keyword data are written
 * key_list - the DRMS keyword data (in FITS keyword format) to be written to the BINTABLE; a LinkedList_t of CFITSIO_KEYWORD * lists, one for each record
 */
int cfitsio_write_keys_to_bintable(CFITSIO_FILE *file_out, long long row_number, LinkedList_t *keyword_data)
{
    return Cf_write_keys_to_bintable(file_out->fptr, row_number, keyword_data);
}

/*
 * keyHeader - a fitsfile containing only image metadata
 * checksum - return the checksum by reference
 */
static int CfGenerateChecksum(fitsfile *keyHeader, char **checksum)
{
    unsigned long datasum = 0;
    unsigned long hdusum = 0;
    char sumString[32]; /* always 16 characters */
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostatErrmsg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    XASSERT(keyHeader);
    XASSERT(checksum);

    // fits_write_hdu(keyHeader, stdout, &cfiostat);

    if (keyHeader && checksum)
    {
        /* write header checksum into fptr - it contains a checksum of the entire header (minus
         * the HEADSUM keyword itself) */
        fits_get_chksum(keyHeader, &datasum, &hdusum, &cfiostat); /* ignore datasum (for image) */

        if (cfiostat)
        {
            fits_get_errstatus(cfiostat, cfiostatErrmsg);
            fprintf(stderr, "[ CfGenerateChecksum() ] CFITSIO error '%s'\n", cfiostatErrmsg);
            err = CFITSIO_ERROR_LIBRARY;
        }
        else
        {
            /* convert 32-bit number into 16-char string */
            memset(sumString, '\0', sizeof(sumString));
            fits_encode_chksum(hdusum, 1, sumString);
            *checksum = strdup(sumString);
        }
    }

    return err;
}

/*
 * fptr - the file into which the meatadata header AND HEADSUM are written
 * header - the header that will be written to fptr
 */
static int CfWriteHeader(fitsfile *fptr, fitsfile *header)
{
    unsigned long datasum;
    unsigned long hdusum;
    char sumString[32]; /* always 16 characters */
    int cfiostat = CFITSIO_SUCCESS;
    char cfiostatErrmsg[FLEN_STATUS];
    char *checksum = NULL;
    int err = CFITSIO_SUCCESS;

    err = CfGenerateChecksum(header, &checksum);

    if (!err)
    {
        fits_copy_file(header, fptr, 0, 1, 0, &cfiostat);

        if (cfiostat)
        {
            fits_get_errstatus(cfiostat, cfiostatErrmsg);
            fprintf(stderr, "CFITSIO error '%s'\n", cfiostatErrmsg);
            err = CFITSIO_ERROR_LIBRARY;
        }
        else
        {
             /* write string as HEADSUM FITS keyword into internal file on disk */
            fits_update_key_str(fptr, HEADSUM, checksum, "checksum of the header part of the primary HDU", &cfiostat);

            if (cfiostat)
            {
                fits_get_errstatus(cfiostat, cfiostatErrmsg);
                fprintf(stderr, "CFITSIO error '%s'\n", cfiostatErrmsg);
                err = CFITSIO_ERROR_LIBRARY;
            }
        }
    }

    if (checksum)
    {
        free(checksum);
    }

    return err;
}

static int CfSetImageInfo(CFITSIO_IMAGE_INFO *info, fitsfile *fptr)
{
    int iAxis;
    int cfiostat = 0;
    char cfiostatErrmsg[FLEN_STATUS];
    int bitpix;
    int naxis;
    long naxes[CFITSIO_MAX_DIM];
    unsigned int bitfield = 0;
    int simple = 1;
    int extend = 0;
    long long blank = 0;
    double bzero = 0;
    double bscale = 0;
    int err = CFITSIO_SUCCESS;

    /* 16 because FITSIO has issues - not sure why they are asking for this */
    if (fits_get_img_param(fptr, 16, &bitpix, &naxis, naxes, &cfiostat))
    {
        fits_get_errstatus(cfiostat, cfiostatErrmsg);
        fprintf(stderr, "[ CfSetImageInfo() ] CFITSIO error '%s'\n", cfiostatErrmsg);
        cfiostat = 0;
    }

    if (!err)
    {
        if (!fits_read_key(fptr, TLONGLONG, "BLANK", &blank, NULL, &cfiostat))
        {
            bitfield = bitfield | kInfoPresent_BLANK;
        }
        else
        {
            fits_get_errstatus(cfiostat, cfiostatErrmsg);
            fprintf(stderr, "[ CfSetImageInfo() ] CFITSIO error '%s'\n", cfiostatErrmsg);
        }
    }

    if (!err)
    {
        if (!fits_read_key(fptr, TDOUBLE, "BZERO", &bzero, NULL, &cfiostat))
        {
            bitfield = bitfield | kInfoPresent_BZERO;
        }
        else
        {
            fits_get_errstatus(cfiostat, cfiostatErrmsg);
            fprintf(stderr, "[ CfSetImageInfo() ] CFITSIO error '%s'\n", cfiostatErrmsg);
        }
    }

    if (!err)
    {
        if (!fits_read_key(fptr, TDOUBLE, "BSCALE", &bscale, NULL, &cfiostat))
        {
            bitfield = bitfield | kInfoPresent_BSCALE;
        }
        else
        {
            fits_get_errstatus(cfiostat, cfiostatErrmsg);
            fprintf(stderr, "[ CfSetImageInfo() ] CFITSIO error '%s'\n", cfiostatErrmsg);
            cfiostat = 0;
        }
    }

    if (!err)
    {
        info->bitpix = bitpix;
        info->naxis = naxis;

        for (iAxis = 0; iAxis < info->naxis; iAxis++)
        {
            info->naxes[iAxis] = naxes[iAxis];
        }

        info->bitfield = bitfield;
        info->simple = simple;
        info->extend = extend;
        info->blank = blank;
        info->bscale = bscale;
        info->bzero = bzero;
        info->export_compression_type = CFITSIO_COMPRESSION_NONE; /* default */
    }

    return err;
}

/****************************************************************************/
/*********************   Using CFITSIO_KEYWORD  *****************************/
/****************************************************************************/
int cfitsio_free_key(CFITSIO_KEYWORD **key)
{
    int err = CFITSIO_SUCCESS;


    if (*key)
    {
        if ((*key)->key_type == kFITSRW_Type_String && (*key)->key_value.vs)
        {
           free((*key)->key_value.vs);
           (*key)->key_value.vs = NULL;
        }

        free(*key);
        *key = NULL;
    }

    return err;
}

int cfitsio_free_keys(CFITSIO_KEYWORD** key_list)

{
   if (key_list)
   {
      CFITSIO_KEYWORD *kptr = *key_list;
      CFITSIO_KEYWORD *del = NULL;

      while (kptr)
      {
	 del = kptr;
	 kptr = kptr->next;

         if (del->key_type == kFITSRW_Type_String && del->key_value.vs)
         {
            free(del->key_value.vs);
            del->key_value.vs = NULL;
         }

	 free(del);
      }

      *key_list = NULL;
   }
   return CFITSIO_SUCCESS;
}

/* value == NULL -> missing key */
int cfitsio_create_header_key(const char *name, cfitsio_keyword_datatype_t type, int number_bytes, const void *value, const char *format, const char *comment, const char *unit, CFITSIO_KEYWORD **key_out)
{
    int err = CFITSIO_SUCCESS;

    err = cfitsio_append_header_key(NULL, name, type, number_bytes, value, format, comment, unit, key_out);

    return err;
}

static int Cf_map_compression_type(int fits_compression_type, CFITSIO_COMPRESSION_TYPE *cfitsio_compression_type)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    if (cfitsio_compression_type)
    {
        switch (fits_compression_type)
        {
            case 0:
                 /* 0 means no compression; the FITSIO documentation is misleading - there is no way to not
                  * have the compression undefined; it is either off (by default) or on
                 *cfitsio_compression_type = CFITSIO_COMPRESSION_UNSET;
                  */
                *cfitsio_compression_type = CFITSIO_COMPRESSION_NONE;
                break;
            /* Ugh - can't understand FITIO; this may be a bug in the FITSIO documentation; NOCOMPRESS does NOT mean
             * uncompressed, sometimes; sometimes 0 means uncompressed */
            case NOCOMPRESS:
                *cfitsio_compression_type = CFITSIO_COMPRESSION_NONE;
                break;
            case RICE_1:
                *cfitsio_compression_type = CFITSIO_COMPRESSION_RICE;
                break;
            case GZIP_1:
                *cfitsio_compression_type = CFITSIO_COMPRESSION_GZIP1;
                break;
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 27)
            case GZIP_2:
                *cfitsio_compression_type = CFITSIO_COMPRESSION_GZIP2;
                break;
#endif
            case PLIO_1:
                *cfitsio_compression_type = CFITSIO_COMPRESSION_PLIO;
                break;
            case HCOMPRESS_1:
                *cfitsio_compression_type = CFITSIO_COMPRESSION_HCOMP;
                break;
            default:
                err = CFITSIO_ERROR_ARGS;
                fprintf(stderr, "[ Cf_map_compression_type() ] invalid FITSIO compression type %d\n", fits_compression_type);
                break;
        }
    }

    return err;
}

static int cfitsio_map_compression_type(CFITSIO_COMPRESSION_TYPE cfitsio_compression_type, int *fits_compression_type)
{
    int err = CFITSIO_SUCCESS;


    if (fits_compression_type)
    {
        switch (cfitsio_compression_type)
        {
#if 0
            /* the FITSIO documentation is misleading; the compression type cannot be undefined */
            case CFITSIO_COMPRESSION_UNSET:
                *fits_compression_type = 0;
                break;
#endif
            case CFITSIO_COMPRESSION_NONE:
                /* the FITSIO documentation is misleading; 0 means uncompressed, NOCOMPRESS does not`
                 *fits_compression_type = NOCOMPRESS;
                 */
                *fits_compression_type = 0;
                break;
            case CFITSIO_COMPRESSION_RICE:
                *fits_compression_type = RICE_1;
                break;
            case CFITSIO_COMPRESSION_GZIP1:
                *fits_compression_type = GZIP_1;
                break;
            case CFITSIO_COMPRESSION_GZIP2:
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 27)
                *fits_compression_type = GZIP_2;
#else
                fprintf(stderr, "[ cfitsio_map_compression_type() ] FITSIO does not support cfitsio compression type %d\n", (int)cfitsio_compression_type);
                err = CFITSIO_ERROR_LIBRARY;
#endif
                break;
            case CFITSIO_COMPRESSION_PLIO:
                *fits_compression_type = PLIO_1;
                break;
            case CFITSIO_COMPRESSION_HCOMP:
                *fits_compression_type = HCOMPRESS_1;
                break;
            default:
                err = CFITSIO_ERROR_ARGS;
                fprintf(stderr, "[ cfitsio_map_compression_type() ] invalid cfitsio compression type %d\n", (int)cfitsio_compression_type);
                break;
        }
    }

    return err;
}

/* will fail if the CHDU is not an image or if the image is empty */
static int Cf_get_compression_type(fitsfile *fptr, int *fits_type)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;
    int hdu_type = -2;
    int fits_is_compressed = 0;
    int fits_comp_type = 0;

    if (fptr)
    {
        fits_get_hdu_type(fptr, &hdu_type, &cfiostat);
        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }

        if (err == CFITSIO_SUCCESS)
        {
            if (hdu_type != IMAGE_HDU)
            {
                fprintf(stderr, "cannot get compression type of non-image HDU");
                err = CFITSIO_ERROR_LIBRARY;
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            fits_is_compressed = fits_is_compressed_image(fptr, &cfiostat);
        }

        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
        else
        {
            if (fits_is_compressed)
            {
                /* get compression type */
                fits_comp_type = fptr->Fptr->compress_type;
            }
            else
            {
                fits_comp_type = NOCOMPRESS;
            }

            *fits_type = fits_comp_type;
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_get_compression_type() ] unable to get compression type\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_get_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE *cfitsio_type)
{
    int err = CFITSIO_SUCCESS;
    CFITSIO_COMPRESSION_TYPE cfitsio_comp_type = CFITSIO_COMPRESSION_NONE;

    if (file)
    {
        cfitsio_comp_type = file->compression_type;
        *cfitsio_type = cfitsio_comp_type;
    }

    return err;
}

static int Cf_get_export_compression_type(fitsfile *fptr, int *fits_type)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;
    int hdu_type = -1;
    int export_compression_type = 0;

    if (fptr)
    {
        fits_get_hdu_type(fptr, &hdu_type, &cfiostat);
        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }

        if (err == CFITSIO_SUCCESS)
        {
            if (hdu_type != IMAGE_HDU)
            {
                fprintf(stderr, "cannot get compression type of non-image HDU");
                err = CFITSIO_ERROR_LIBRARY;
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            fits_get_compression_type(fptr, &export_compression_type, &cfiostat);
        }

        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
        else
        {
            *fits_type = export_compression_type;
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_get_export_compression_type() ] unable to get compression type\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_get_export_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE *export_compression_type)
{
    int err = CFITSIO_SUCCESS;


    if (file)
    {
        *export_compression_type = file->export_compression_type;
    }

    return err;
}

static int Cf_set_export_compression_type(fitsfile *fptr, int fits_comp_type)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int export_compression_type = 0;
    int err = CFITSIO_SUCCESS;

    err = Cf_get_export_compression_type(fptr, &export_compression_type);

    if (err == CFITSIO_SUCCESS)
    {
        if (fits_comp_type != export_compression_type)
        {
            fits_set_compression_type(fptr, fits_comp_type, &cfiostat);
            if (cfiostat)
            {
                err = CFITSIO_ERROR_LIBRARY;
            }
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_set_export_compression_type() ] unable to set compression type\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_set_export_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE compression_type)
{
    int err = CFITSIO_SUCCESS;
    CFITSIO_COMPRESSION_TYPE export_compression_type = CFITSIO_COMPRESSION_NONE;
    int fits_compression_type = 0;

    if (file && file->fptr)
    {
        export_compression_type = file->export_compression_type;

        if (file->type == CFITSIO_FILE_TYPE_IMAGE)
        {
            /* can only call this if file->fptr has already been initialized with a fits_create_img() call */
            err = cfitsio_map_compression_type(compression_type, &fits_compression_type);
            if (err == CFITSIO_SUCCESS)
            {
                err = Cf_set_export_compression_type(file->fptr, fits_compression_type);
            }

            if (err == CFITSIO_SUCCESS)
            {
                file->export_compression_type = compression_type;
            }
        }
        else
        {
            fprintf(stderr, "[ cfitsio_set_export_compression_type() ] can only set compression type of image CFITSIO file\n");
            err = CFITSIO_ERROR_ARGS;
        }
    }

    return err;
}

static int Cf_create_file_object(fitsfile *fptr, cfitsio_file_type_t file_type, void *data, int create_data_unit)
{
    CFITSIO_IMAGE_INFO *image_info = NULL;
    CFITSIO_BINTABLE_INFO *bintable_info = NULL;
    int fits_compression_type = 0;
    int dim = 0;
    long long num_pixels = 0;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    switch (file_type)
    {
        case CFITSIO_FILE_TYPE_HEADER:
            /* BITPIX = 8, NAXIS = 0, NAXES = NULL - even though there is no image, BITPIX cannot be 0 */
            fits_write_imghdr(fptr, 8, 0, NULL, &cfiostat);
            break;
        case CFITSIO_FILE_TYPE_IMAGE:
            if (data)
            {
                image_info = (CFITSIO_IMAGE_INFO *)data;

                /* set fits export compression type now (if it differs from the type in image_info) */
                err = cfitsio_map_compression_type(image_info->export_compression_type, &fits_compression_type);
                if (err == CFITSIO_SUCCESS)
                {
                    Cf_set_export_compression_type(fptr, fits_compression_type);
                }

#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 35)
                if (err == CFITSIO_SUCCESS)
                {
                    for (dim = 0, num_pixels = 1; dim < image_info->naxis; dim++)
                    {
                         num_pixels *= image_info->naxes[dim];
                    }

                    if ((abs(image_info->bitpix) / 8) * num_pixels > HUGE_HDU_THRESHOLD)
                    {
                        // support 64-bit HDUs
                        if (fits_set_huge_hdu(fptr, 1, &cfiostat))
                        {
                            err = CFITSIO_ERROR_FILE_IO;
                        }
                    }
                }
#endif

                if (err == CFITSIO_SUCCESS)
                {
                    if (create_data_unit)
                    {
                        fits_create_img(fptr, image_info->bitpix, image_info->naxis, image_info->naxes, &cfiostat);
                    }
                }
            }
            break;
        case CFITSIO_FILE_TYPE_BINTABLE:
            if (data)
            {
                bintable_info = (CFITSIO_BINTABLE_INFO *)data;
                /* bintable_info->rows is a list of CFITSIO_KEYWORD * lists; bintable_info->tfields is the number of columns;
                 * bintable_info->ttypes is an array of column names; bintable_info->tforms is an array of tform data types */
                if (bintable_info->tfields > CFITSIO_MAX_BINTABLE_WIDTH)
                {
                    fprintf(stderr, "[ Cf_create_file_object() ] too many keywords - the mamimum allowed is %d\n", CFITSIO_MAX_BINTABLE_WIDTH);
                    err = CFITSIO_ERROR_ARGS;
                }

                if (create_data_unit)
                {
                    fits_create_tbl(fptr, BINARY_TBL, list_llgetnitems(bintable_info->rows), bintable_info->tfields, (char **)bintable_info->ttypes, (char **)bintable_info->tforms, NULL, NULL, &cfiostat);
                }
            }
            break;
        default:
            break;
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_create_file_object() ] unable to create FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

/* file type of current HDU */
static int Cf_get_file_type(fitsfile *fptr, int *type)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;
    int fits_hdu_type = -1;

    fits_get_hdu_type(fptr, &fits_hdu_type, &cfiostat);

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_get_file_type() ] unable to get fits FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (err == CFITSIO_SUCCESS)
    {
        *type = fits_hdu_type;
    }

    return err;
}

int cfitsio_get_file_state(CFITSIO_FILE *file, cfitsio_file_state_t *state)
{
    cfitsio_file_type_t fits_file_type = CFITSIO_FILE_TYPE_UNKNOWN;
    int is_initialized = -1;
    int err = CFITSIO_SUCCESS;

    if (file)
    {
        if (file->state != CFITSIO_FILE_STATE_EMPTY)
        {
            *state = file->state;
        }
        else
        {
            if (file->fptr == NULL)
            {
                *state = CFITSIO_FILE_STATE_EMPTY;
            }
            else
            {
                err = cfitsio_get_file_type_from_fitsfile((CFITSIO_FITSFILE)file->fptr, &fits_file_type, &is_initialized, NULL);
                if (err == CFITSIO_SUCCESS)
                {
                    if (is_initialized)
                    {
                        *state = CFITSIO_FILE_STATE_INITIALIZED;
                    }
                    else
                    {
                        *state = CFITSIO_FILE_STATE_UNINITIALIZED;
                    }
                }
            }
        }
    }
    else
    {
        err = CFITSIO_ERROR_ARGS;
    }

    return err;
}

static int Cf_map_file_type(int fits_file_type, cfitsio_file_type_t *cfitsio_file_type)
{
    int err = CFITSIO_SUCCESS;

    switch (fits_file_type)
    {
        case IMAGE_HDU:
            /* the type will also be IMAGE_HDU if fits_create_img() was never called */
            *cfitsio_file_type = CFITSIO_FILE_TYPE_IMAGE;
            break;
        case ASCII_TBL:
            *cfitsio_file_type = CFITSIO_FILE_TYPE_ASCIITABLE;
            break;
        case BINARY_TBL:
            *cfitsio_file_type = CFITSIO_FILE_TYPE_BINTABLE;
            break;
        default:
            fprintf(stderr, "unsupported FITS file type %d\n", fits_file_type);
            err = CFITSIO_ERROR_ARGS;
            break;
    }

    return err;
}

int cfitsio_get_file_type_from_fitsfile(CFITSIO_FITSFILE fits_file, cfitsio_file_type_t *type, int *is_initialized, int *old_hdu_index)
{
    int number_hdus = 0;
    int current_hdu_index = 0;
    int hdu_type = -2; /* [ -1, 2 ] are reserved */
    int fits_is_initialized = -1;
    int naxis = 0;
    fitsfile *fptr = (fitsfile *)fits_file;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    /* check each HDU - the file type is determined by the first one that
    * is a table, or an IMAGE_HDU with an naxis > 0 */
    if (fits_get_num_hdus(fptr, &number_hdus, &cfiostat))
    {
       err = CFITSIO_ERROR_LIBRARY;
    }

    if (err == CFITSIO_SUCCESS)
    {
        if (number_hdus > 0)
        {
           /* get current HDU */
           fits_get_hdu_num(fptr, &current_hdu_index);

           if (number_hdus == 1 && current_hdu_index == 1)
           {
                /* an initialized fits file will have type IMAGE_HDU + naxis > 0, or
                * ASCII_TBL, or BINARY_TBL */
                if (fits_get_hdu_type(fptr, &hdu_type, &cfiostat))
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }

                if (err == CFITSIO_SUCCESS)
                {
                    switch (hdu_type)
                    {
                        case ASCII_TBL:
                            fits_is_initialized = 1;
                            break;
                        case BINARY_TBL:
                            fits_is_initialized = 1;
                            break;
                        case IMAGE_HDU:
                            if (fits_get_img_dim(fptr, &naxis, &cfiostat))
                            {
                                err = CFITSIO_ERROR_LIBRARY;
                            }

                            if (err == CFITSIO_SUCCESS)
                            {
                                if (naxis > 0)
                                {
                                    fits_is_initialized = 1;
                                }
                                else
                                {
                                    fits_is_initialized = 0;
                                }
                            }
                            break;
                    }
                }
           }
           else
           {
               for (current_hdu_index = 1; current_hdu_index <= number_hdus; current_hdu_index++)
               {
                   if (fits_movabs_hdu(fptr, current_hdu_index, &hdu_type, &cfiostat))
                   {
                       err = CFITSIO_ERROR_LIBRARY;
                       break;
                   }

                   switch (hdu_type)
                   {
                       case ASCII_TBL:
                           fits_is_initialized = 1;
                           break;
                       case BINARY_TBL:
                           fits_is_initialized = 1;
                           break;
                       case IMAGE_HDU:
                           if (fits_get_img_dim(fptr, &naxis, &cfiostat))
                           {
                               err = CFITSIO_ERROR_LIBRARY;
                           }

                           if (err == CFITSIO_SUCCESS)
                           {
                               if (naxis > 0)
                               {
                                   fits_is_initialized = 1;
                                   break;
                               }
                               else
                               {
                                   fits_is_initialized = 0;
                               }
                           }
                           break;
                   }

                   if (fits_is_initialized)
                   {
                        break;
                   }
               }
           }
        }
        else
        {
            fits_is_initialized = 0;

            /* file type is unknown since there are no HDUs */
        }
    }

    if (type)
    {
        if (hdu_type >= 0)
        {
            err = Cf_map_file_type(hdu_type, type);
        }
        else
        {
            *type = CFITSIO_FILE_TYPE_UNKNOWN;
        }
    }

    if (is_initialized)
    {
        *is_initialized = fits_is_initialized;
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_get_file_type_from_fitsfile() ] unable to get CFITSIO file type\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_get_file_type(CFITSIO_FILE *file, cfitsio_file_type_t *type)
{
    int err = CFITSIO_SUCCESS;

    /* first check the `type` field (it could be the case that a file was created, but the type was never set) */
    if (file->type != CFITSIO_FILE_TYPE_UNKNOWN)
    {
        *type = file->type;
    }
    else
    {
        if (file->fptr == NULL)
        {
            *type = CFITSIO_FILE_TYPE_UNKNOWN;
        }
        else
        {
            err = cfitsio_get_file_type_from_fitsfile((CFITSIO_FITSFILE)file->fptr, type, NULL, NULL);
        }
    }

    return err;
}

int cfitsio_create_header(CFITSIO_FILE *file)
{
    int err = CFITSIO_SUCCESS;

    err = Cf_create_file_object(file->fptr, CFITSIO_FILE_TYPE_HEADER, NULL, 1);
    if (err == CFITSIO_SUCCESS)
    {
        file->state = CFITSIO_FILE_STATE_INITIALIZED;
        file->type = CFITSIO_FILE_TYPE_HEADER;
    }

    return err;
}

int cfitsio_create_image(CFITSIO_FILE *file, CFITSIO_IMAGE_INFO *image_info)
{
    int err = CFITSIO_SUCCESS;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];

    err = Cf_create_file_object(file->fptr, CFITSIO_FILE_TYPE_IMAGE, image_info, 1);
    if (err == CFITSIO_SUCCESS)
    {
        file->state = CFITSIO_FILE_STATE_INITIALIZED;
        file->type = CFITSIO_FILE_TYPE_IMAGE;
        file->compression_type = image_info->export_compression_type;
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_create_file_object() ] unable to create FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_create_bintable(CFITSIO_FILE *file, CFITSIO_BINTABLE_INFO *bintable_info)
{
    int err = CFITSIO_SUCCESS;

    err = Cf_create_file_object(file->fptr, CFITSIO_FILE_TYPE_BINTABLE, bintable_info, 1);
    if (err == CFITSIO_SUCCESS)
    {
        file->state = CFITSIO_FILE_STATE_INITIALIZED;
        file->type = CFITSIO_FILE_TYPE_BINTABLE;
    }

    return err;
}

/* create a new CFITSIO_FILE struct; if `initialize_fitsfile`, then allocate a fitsfile and assign to CFITSIO_FILE::fptr
 * by calling  fits_create_file(..., `file_name`, ...) */
 /* file is writeable since we are creating an empty file */
int cfitsio_create_file(CFITSIO_FILE **out_file, const char *file_name, cfitsio_file_type_t file_type, CFITSIO_IMAGE_INFO *image_info, CFITSIO_BINTABLE_INFO *bintable_info, CFITSIO_COMPRESSION_TYPE *export_compression_type)
{
    int file_created = 0;
    int fits_compression_type = 0; /* of the out file */
    CFITSIO_COMPRESSION_TYPE cfitsio_compression_type = CFITSIO_COMPRESSION_NONE; /* of the out file */
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    CFITSIO_FILE existing_file;
    int err = CFITSIO_SUCCESS;

    XASSERT(out_file);
    if (file_type != CFITSIO_FILE_TYPE_UNKNOWN)
    {
        XASSERT(file_name);
    }

    *out_file = calloc(1, sizeof(CFITSIO_FILE));

    if (!*out_file)
    {
        fprintf(stderr, "[ cfitsio_create_file() ] out of memory\n");
        err = CFITSIO_ERROR_OUT_OF_MEMORY;
    }

    if (!err)
    {
        (*out_file)->state = CFITSIO_FILE_STATE_EMPTY; /* no fptr */
        (*out_file)->type = CFITSIO_FILE_TYPE_UNKNOWN; /* header, image, bin_table not define */

        if (file_type != CFITSIO_FILE_TYPE_UNKNOWN)
        {
            (*out_file)->in_memory = (strncmp(file_name, "-", 1) == 0);
            (*out_file)->compression_type = CFITSIO_COMPRESSION_NONE; /* by default */
            (*out_file)->export_compression_type = CFITSIO_COMPRESSION_NONE;

            if ((*out_file)->in_memory)
            {
                if (fits_create_file(&((*out_file)->fptr), file_name, &cfiostat))
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }
                else
                {
                    file_created = 1;
                }
            }
            else
            {
                /* since we are CREATING a file, if the file exists, first delete it; do not call
                 * fitsrw_getfptr() before doing so, otherwise, we will get a fitsfile * to an
                 * existing file, and we would then attempt to write over this file without
                 * first 'emptying' it
                 *
                 * ack - FITSIO 'file names' can have stuff appended to the actual filename, like
                 * the string "[compress Rice]" in myfile.fits[compress Rice]; so we have to use
                 * FITSIO to delete the file (which we cannot do without first opening it!) */
                memset(&existing_file, '\0', sizeof(CFITSIO_FILE));
                if (fits_open_file(&existing_file.fptr, file_name, READONLY, &cfiostat))
                {
                    /* file does not exist - this will set cfiostat != 0 */
                    cfiostat = 0;
                }
                else
                {
                    /* file exists - can we delete it if it was opened for reading-only? */
                    if (fits_delete_file(existing_file.fptr, &cfiostat))
                    {
                        err = CFITSIO_ERROR_LIBRARY;
                    }
                }

                if (!err)
                {
                    (*out_file)->fptr = (fitsfile *)fitsrw_getfptr(0, file_name, 1, &err, &file_created);
                }

                if (!err)
                {
                    if (!(*out_file)->fptr)
                    {
                        err = CFITSIO_ERROR_FILE_IO;
                    }
                }
            }

            if (!err)
            {
                /* has fptr, but the fits_() to make an image or bintable HDU has not been called */
                (*out_file)->state = CFITSIO_FILE_STATE_UNINITIALIZED;

                if (file_created)
                {
                    if (export_compression_type)
                    {
                        if (file_type != CFITSIO_FILE_TYPE_IMAGE)
                        {
                            fprintf(stderr, "[ cfitsio_create_file() ] can only compress image FITS files\n");
                            err = CFITSIO_ERROR_ARGS;
                        }
                    }

                    if (file_type == CFITSIO_FILE_TYPE_HEADER)
                    {
                        /* BITPIX = 8, NAXIS = 0, NAXES = NULL - even though there is no image, BITPIX cannot be 0 */
                        (*out_file)->type = CFITSIO_FILE_TYPE_HEADER;

                        err = cfitsio_create_header(*out_file);
                    }
                    else if (file_type == CFITSIO_FILE_TYPE_IMAGE)
                    {
                        (*out_file)->type = CFITSIO_FILE_TYPE_IMAGE;

                        if ((*out_file)->in_memory)
                        {
                            if (export_compression_type)
                            {
                                /* must set before cfitsio_create_image() */
                                (*out_file)->export_compression_type = *export_compression_type;
                            }
                        }
                        else
                        {
                            /* since we are creating a file on disk, a compressed file will have been created if
                             * `file_name` contained such a fits compression specification string; in this case
                             * we will need to update the compression type and export compression type of */
                            if (!err)
                            {
                                if (export_compression_type)
                                {
                                    /* caller specified the export compression type */
                                    (*out_file)->export_compression_type = *export_compression_type;
                                }
                                else
                                {
                                    /* caller did not specify the export compression type; use the out file's compression type */

                                    /* data compression type */
                                    /* can't call Cf_get_compression_type() because fits_create_img() has not been
                                     * called; but we can call Cf_get_export_compression_type() - apparently, creating
                                     * a fits file with a compression-specification calls fits_set_compression_type() */
                                    err = Cf_get_export_compression_type((*out_file)->fptr, &fits_compression_type);

                                    if (!err)
                                    {
                                        err = Cf_map_compression_type(fits_compression_type, &cfitsio_compression_type);
                                    }

                                    if (!err)
                                    {
                                        (*out_file)->export_compression_type = cfitsio_compression_type;
                                    }
                                }
                            }
                        }

                        if (image_info)
                        {
                            image_info->export_compression_type = (*out_file)->export_compression_type;
                            err = cfitsio_create_image(*out_file, image_info);
                        }
                    }
                    else if (file_type == CFITSIO_FILE_TYPE_BINTABLE)
                    {
                        (*out_file)->type = CFITSIO_FILE_TYPE_BINTABLE;

                        if (bintable_info)
                        {
                            /* bintable_info->rows is a list of CFITSIO_KEYWORD * lists; bintable_info->tfields is the number of columns;
                             * bintable_info->ttypes is an array of column names; bintable_info->tforms is an array of tform data types */
                            err = cfitsio_create_bintable(*out_file, bintable_info);
                        }
                    }
                    else
                    {
                        fprintf(stderr, "[ cfitsio_create_file() ] invalid file type %d\n", (int)file_type);
                    }

                    if (cfiostat)
                    {
                        err = CFITSIO_ERROR_FILE_IO;
                    }
                }
                else
                {
                    /* cfitsio_create_file() must create a file, not open an existing one */
                    fprintf(stderr, "[ cfitsio_create_file() ] attempting to create an existing file\n");
                    err = CFITSIO_ERROR_ARGS;
                }
            }
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_create_file() ] unable to create FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

void cfitsio_close_file(CFITSIO_FILE **fits_file)
{
    CFITSIO_IMAGE_INFO image_info;
    int num_hdus = -1;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];


    if (fits_file && *fits_file)
    {
        if ((*fits_file)->fptr)
        {
            /* FITSIO has this problem that you cannot close a FITS file that has no completed HDUs - seems like a bug;
             * check for that situation by getting the number of finalized/closed HDUs; if 0, then don't call
             * Cf_close_file(); I guess FITSIO will leak, but I'm not sure what to do */
            fits_get_num_hdus((*fits_file)->fptr, &num_hdus, &cfiostat);
            if (!cfiostat && num_hdus > 0)
            {
                /* if the FITS file is not cached, then we should call fits_close_file(), dropping
                 * data if the file was in-memory-only */
                Cf_close_file(&(*fits_file)->fptr, (*fits_file)->in_memory);
            }
        }

        free(*fits_file);
        *fits_file = NULL;
    }
}

/* if `fits_file` is an in-memory file (which is not globally cached), then the file content is dumped to stdout */
int cfitsio_stream_and_close_file(CFITSIO_FILE **fits_file)
{
    CFITSIO_IMAGE_INFO fpinfo;
    int err = CFITSIO_SUCCESS;


    if (fits_file && *fits_file)
    {
        if ((*fits_file)->fptr)
        {
            /* if the FITS file is not cached, then we should call fits_close_file(), dropping
             * data if the file was in-memory-only */
            err = Cf_stream_and_close_file(&(*fits_file)->fptr);
        }

        free(*fits_file);
        *fits_file = NULL;
    }

    return err;
}

void cfitsio_get_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE *fptr)
{
    if (file)
    {
        *fptr = (CFITSIO_FITSFILE)file->fptr;
    }
}

/* if `state`/`type` are provided, then use those to set the state and type of `file`; otherwise, obtain those
 * values from `fptr` */
int cfitsio_set_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE fptr, int in_memory, cfitsio_file_state_t *state, cfitsio_file_type_t *type)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int old_hdu_index = 0;
    int fits_compression_type = 0;
    int fits_export_compression_type = 0;
    int fits_file_state_initialized = -1;
    cfitsio_file_state_t cfitsio_file_state = CFITSIO_FILE_STATE_EMPTY;
    cfitsio_file_type_t cfistio_file_type = CFITSIO_FILE_TYPE_UNKNOWN;
    CFITSIO_COMPRESSION_TYPE cfitsio_compression_type = CFITSIO_COMPRESSION_NONE;
    CFITSIO_COMPRESSION_TYPE cfitsio_export_compression_type = CFITSIO_COMPRESSION_NONE;
    int err = CFITSIO_SUCCESS;

    if (file)
    {
        if (!state || !type)
        {
            /* use the code that iterates through HDUs - it tells us:
             * 1. the file type
             * 2. whether it is initialized or not (we know fptr exists, so `cfitsio_file_state` cannot be empty) */
             err = cfitsio_get_file_type_from_fitsfile(fptr, &cfistio_file_type, &fits_file_state_initialized, &old_hdu_index);
        }

        if (state)
        {
            /* override if state was determined from fptr */
            cfitsio_file_state = *state;
        }
        else
        {
            cfitsio_file_state = fits_file_state_initialized ? CFITSIO_FILE_STATE_INITIALIZED : CFITSIO_FILE_STATE_UNINITIALIZED;
        }

        if (type)
        {
            /* override if type was determined from fptr */
            cfistio_file_type = *type;
        }

        if (file->state != CFITSIO_FILE_STATE_EMPTY)
        {
            /* can only set the fits file of `file` if `file` is CFITSIO_FILE_STATE_EMPTY */
            err = CFITSIO_ERROR_ARGS;
        }

        if (!fptr || cfitsio_file_state == CFITSIO_FILE_STATE_EMPTY)
        {
            /* `fptr` must have been created, which means that state cannot be CFITSIO_FILE_STATE_EMPTY */
            err = CFITSIO_ERROR_ARGS;
        }

        if (err == CFITSIO_SUCCESS)
        {
            file->fptr = (fitsfile *)fptr;
            file->in_memory = in_memory;
            file->state = cfitsio_file_state;
            file->type = cfistio_file_type;

            if (cfistio_file_type == CFITSIO_FILE_TYPE_IMAGE)
            {
                if (cfitsio_file_state == CFITSIO_FILE_STATE_INITIALIZED)
                {
                    /* copy compression type from `fptr` to `file` */
                    err = Cf_get_compression_type((fitsfile *)fptr, &fits_compression_type);
                    if (err == CFITSIO_SUCCESS)
                    {
                        /* convert to cfitsio type */
                        err = Cf_map_compression_type(fits_compression_type, &cfitsio_compression_type);
                    }

                    if (err == CFITSIO_SUCCESS)
                    {
                        file->compression_type = cfitsio_compression_type;
                    }
                }
                else
                {
                    /* default */
                    file->compression_type = CFITSIO_COMPRESSION_NONE;
                }
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            if (cfistio_file_type == CFITSIO_FILE_TYPE_IMAGE)
            {
                /* copy export compression type from `fptr` to `file` */
                err = Cf_get_export_compression_type((fitsfile *)fptr, &fits_export_compression_type);
                if (err == CFITSIO_SUCCESS)
                {
                    /* convert to cfitsio type */
                    err = Cf_map_compression_type(fits_export_compression_type, &cfitsio_export_compression_type);
                }

                if (err == CFITSIO_SUCCESS)
                {
                    file->export_compression_type = cfitsio_export_compression_type;
                }
            }
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_set_fitsfile() ] unable to create FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

static int Cf_get_size(fitsfile *fptr, long long *size)
{
    int err = CFITSIO_SUCCESS;

    if (fptr && size)
    {
        *size = fptr->Fptr->logfilesize;
    }

    return err;
}

int cfitsio_get_size(CFITSIO_FILE *file, long long *size)
{
    return Cf_get_size(file->fptr, size);
}

void cfitsio_close_header(CFITSIO_HEADER **header)
{
    cfitsio_close_file((CFITSIO_FILE **)header);
}

/* copy
   if uncompressing an image, then the result will be a single HDU with an image array; if
   compressing an image, then the results will be the primary HDU plus an extension HDU; if
   the file is a bin table, then the result will be two HDUs (the extension will be a BINTABLE)
 */
static int Cf_copy_file(fitsfile *source, fitsfile *dest, CFITSIO_COMPRESSION_TYPE export_compression_type)
{
    int err = CFITSIO_SUCCESS;
    int old_hdu_index = 0;
    cfitsio_file_type_t file_type_from_fptr = CFITSIO_FILE_TYPE_UNKNOWN;
    int is_initialized = -1;
    CFITSIO_IMAGE_INFO image_info = {0};
    CFITSIO_BINTABLE_INFO *bintable_info = NULL;
    int data_type = 0;
    int dimension = 0;
    int hdu_type = 0;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];

    /* since last arg is provided, this call will change HDU if necessary */
    err = cfitsio_get_file_type_from_fitsfile((CFITSIO_FITSFILE)source, &file_type_from_fptr, &is_initialized, &old_hdu_index);

    if (err == CFITSIO_SUCCESS)
    {
        switch (file_type_from_fptr)
        {
            case CFITSIO_FILE_TYPE_HEADER:
                err = Cf_create_file_object(dest, CFITSIO_FILE_TYPE_HEADER, NULL, 1);
                if (err == CFITSIO_SUCCESS)
                {
                    err = Cf_write_header_keys(dest, NULL, NULL, source);
                }
                break;
            case CFITSIO_FILE_TYPE_IMAGE:
                /* the CHDU will be the HDU with the image in it; if the file was a JSOC file SUMS file (which
                 * means the image resides in an image extension), then the CHDU will be the second HDU (an image extension);
                 * if the file was some external image FITS file, then it could really be any HDU; if the file
                 * is an in-memory file, then the CHDU will likely be the primary HDU;
                 *
                 * basically, we do not know what the CHDU is; but we do know that we should have only a single image
                 * in the HDU; so we should move to the first HDU and iterate through them, looking for the first image HDU;
                 * if in the future we intend to open FITS files with multiple image extensions, then we should create
                 * a new file type, like CFITSIO_FILE_TYPE_MUTLI_IMAGE or CFITSIO_FILE_TYPE_MIXED
                 */
                if (err == CFITSIO_SUCCESS)
                {
                    /* read source image HDU data and write the data to `dest` */

                    /* set up image_info (a partial set-up; not every field is needed) for Cf_create_file_object */
                    if (fits_get_img_param(source, CFITSIO_MAX_DIM, &(image_info.bitpix), &(image_info.naxis), &(image_info.naxes[0]), &cfiostat))
                    {
                        err = CFITSIO_ERROR_LIBRARY;
                    }

                    if (err == CFITSIO_SUCCESS)
                    {
                        switch(image_info.bitpix)
                        {
                            case BYTE_IMG:
                                data_type = TBYTE;
                                break;
                            case SHORT_IMG:
                                data_type = TSHORT;
                                break;
                            case LONG_IMG:
                                data_type = TINT;
                                break;
                            case FLOAT_IMG:
                                data_type = TFLOAT;
                                break;
                            case DOUBLE_IMG:
                                data_type = TDOUBLE;
                                break;
                        }

                        image_info.export_compression_type = export_compression_type;

                        /* Cf_create_file_object calls fits_set_compression_type() if `image_info` specifies compression;
                         * 0 ==> do not create image extension - we will copy the source one in, which will create it  */
                        err = Cf_create_file_object(dest, CFITSIO_FILE_TYPE_IMAGE, (void *)&image_info, 0);
                    }

                    if (err == CFITSIO_SUCCESS)
                    {
                        if (fits_copy_hdu(source, dest, 0, &cfiostat))
                        {
                            err = CFITSIO_ERROR_LIBRARY;
                        }
                    }
                }

                break;
            case CFITSIO_FILE_TYPE_BINTABLE:
                /* no need to read bintable info (keyword data) from source; metadata are in source header keywords */
                /* ART - NOT YET IMPLEMENTED */
                err = Cf_create_file_object(dest, CFITSIO_FILE_TYPE_BINTABLE, (void *)bintable_info, 1);
                if (err == CFITSIO_SUCCESS)
                {
                    /* not sure how to copy bintable at the moment, but fitsexport.c currently does not copy BINTABLEs */
                    //Cf_write_keys_to_bintable
                }

                err = CFITSIO_ERROR_LIBRARY;
                break;
            default:
                fprintf(stderr, "[ Cf_copy_file() ] invalid file type %d\n", (int)file_type_from_fptr);
                err = CFITSIO_ERROR_ARGS;
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_copy_file() ] unable to copy FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_copy_file(CFITSIO_FILE *source_in, CFITSIO_FILE *dest_in, int copy_header_only)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int fits_export_compression_type = 0;
    int err = CFITSIO_SUCCESS;

    if (copy_header_only)
    {
        fits_copy_header(source_in->fptr, dest_in->fptr, &cfiostat); /* if dest_in->in_memory, writes header into memory */
        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }
    else
    {
        /* can only copy source_in if it has been initialized (it has a type) */
        if (source_in->state != CFITSIO_FILE_STATE_INITIALIZED)
        {
            err = CFITSIO_ERROR_ARGS;
        }

        if (err == CFITSIO_SUCCESS)
        {
            /* can only copy if dest_in state is uninitialized (cannot be empty) */
            if (dest_in->state != CFITSIO_FILE_STATE_UNINITIALIZED)
            {
                err = CFITSIO_ERROR_ARGS;
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            /* types must match */
            if (source_in->type != dest_in->type)
            {
                err = CFITSIO_ERROR_ARGS;
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            /* if the source/dest are images, then set compression type of dest */
            if (source_in->type == CFITSIO_FILE_TYPE_IMAGE)
            {
                dest_in->compression_type = source_in->compression_type;
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            /* this sets the export compression type before creating an image HDU */
            err = Cf_copy_file(source_in->fptr, dest_in->fptr, dest_in->export_compression_type);
            if (cfiostat)
            {
                err = CFITSIO_ERROR_LIBRARY;
            }

            if (!err)
            {
                dest_in->compression_type = dest_in->export_compression_type;
            }
        }

        if (err == CFITSIO_SUCCESS)
        {
            /* copy succeeded, so set dest_in state to initialized */
            dest_in->state = CFITSIO_FILE_STATE_INITIALIZED;
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_copy_file() ] unable to copy CFITSIO file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        err = CFITSIO_ERROR_FILE_IO;
    }

    return err;
}

static int Cf_make_hcon(CFITSIO_KEYWORD *key_list, HContainer_t **hcon)
{
    HContainer_t *hash = NULL;
    CFITSIO_KEYWORD *key_node = NULL;
    int cfiostat = 0;
    int err = CFITSIO_SUCCESS;

    hash = hcon_create(sizeof(CFITSIO_KEYWORD *), FLEN_KEYWORD, NULL, NULL, NULL, NULL, 0);

    if (!hash)
    {
        err = CFITSIO_ERROR_OUT_OF_MEMORY;
    }
    else
    {
        key_node = key_list;

        while (key_node)
        {
            if (!hcon_member_lower(hash, key_node->key_name))
            {
                hcon_insert_lower(hash, key_node->key_name, &key_node);
            }

            key_node = key_node->next;
        }

        *hcon = hash;
    }

    return err;
}

int cfitsio_copy_header_keywords(CFITSIO_FILE *in_file, CFITSIO_FILE *out_file, CFITSIO_KEYWORD *key_list)
{
    int nkeys = 0;
    int nrec = 0;
    char card[FLEN_CARD];
    char key_name[FLEN_KEYWORD];
    char key_value[FLEN_VALUE];
    HContainer_t *key_hcon = NULL;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostatErrmsg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    fits_get_hdrspace(in_file->fptr, &nkeys, NULL, &cfiostat);

    if (!cfiostat)
    {
        if (key_list)
        {
            /* make a hash table of the 'acceptable' FITS keyword names */
            err = Cf_make_hcon(key_list, &key_hcon);
        }

        if (!err)
        {
            for (nrec = 1; nrec <= nkeys && !cfiostat; nrec++)
            {
                memset(card, '\0', sizeof(card));
                fits_read_record(in_file->fptr, nrec, card, &cfiostat);

                if (cfiostat)
                {
                    continue;
                }

                if (fits_get_keyclass(card) > TYP_CMPRS_KEY)
                {
                    if (key_hcon)
                    {
                        /* extract keyword name from card */
                        fits_read_keyn(in_file->fptr, nrec, key_name, key_value, NULL, &cfiostat);

                        if (cfiostat || !hcon_member_lower(key_hcon, key_name))
                        {
                            continue;
                        }
                    }

                    fits_write_record(out_file->fptr, card, &cfiostat);
                }
            }
        }

        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }
    else
    {
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (key_hcon)
    {
        hcon_destroy(&key_hcon);
    }

    return err;
}

int cfitsio_read_history_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *keyword_out)
{
    char *value = NULL;
    int err = CFITSIO_SUCCESS;

    err = Cf_read_history_or_comment_key(file->fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY, &value);
    if (err == CFITSIO_SUCCESS)
    {
        keyword_out->key_value.vs = value;
        value = NULL; /* yoink! */
    }

    if (value)
    {
        free(value);
        value = NULL;
    }

    return err;
}

int cfitsio_write_history_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *keyword)
{
    return Cf_write_history_or_comment_key(file->fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY, keyword->key_value.vs);
}

int cfitsio_update_history_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *keyword)
{
    int err = CFITSIO_SUCCESS;

    /* first, delete existing HISTORY cards if they exist (no err if the key does not exist) */
    err = Cf_delete_history_or_comment_record(file->fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_HISTORY);

    if (err == CFITSIO_SUCCESS)
    {
        if (*keyword->key_value.vs != '\0')
        {
            /* then, write HISTORY cards */
            err = cfitsio_write_history_key(file, keyword);
        }
    }

    return err;
}

int cfitsio_read_comment_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *keyword_out)
{
    char *value = NULL;
    int err = CFITSIO_SUCCESS;

    err = Cf_read_history_or_comment_key(file->fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT, &value);
    if (err == CFITSIO_SUCCESS)
    {
        keyword_out->key_value.vs = value;
        value = NULL; /* yoink! */
    }

    if (value)
    {
        free(value);
        value = NULL;
    }

    return err;
}

int cfitsio_write_comment_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *keyword)
{
    return Cf_write_history_or_comment_key(file->fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT, keyword->key_value.vs);
}

int cfitsio_update_comment_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *keyword)
{
    int err = CFITSIO_SUCCESS;

    /* first, delete existing HISTORY cards if they exist (no err if the key does not exist) */
    err = Cf_delete_history_or_comment_record(file->fptr, CFITSIO_KEYWORD_SPECIAL_TYPE_COMMENT);

    if (err == CFITSIO_SUCCESS)
    {
        if (*keyword->key_value.vs != '\0')
        {
            /* then, write COMMENT cards */
            err = cfitsio_write_comment_key(file, keyword);
        }
    }

    return err;
}

static int cfitsio_strip_keyword_unit_and_missing(const char *comment, char *stripped_comment, char *unit, char *missing)
{
    char *working_comment = NULL;
    static regex_t *reg_expression = NULL;
    const char *keyword_comment_pattern = "^[ ]*(\\[([[:print:]]+)\\][ ]+)?(\\(([[:print:]]+)\\)[ ]+)?([[:print:]]*)$"; /* comment with unit (a failure to match means the whole string is a comment) */
    regmatch_t matches[6]; /* index 0 is the entire string */
    int err = CFITSIO_SUCCESS;

    if (!reg_expression)
    {
        /* ART - this does not get freed! */
        reg_expression = calloc(1, sizeof(regex_t));
        if (regcomp(reg_expression, keyword_comment_pattern, REG_EXTENDED) != 0)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        working_comment = strdup(comment);

        if (!working_comment)
        {
            err = CFITSIO_ERROR_OUT_OF_MEMORY;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        if (regexec(reg_expression, working_comment, sizeof(matches) / sizeof(matches[0]), matches, 0) == 0)
        {
            /* matches */
            if (matches[2].rm_so != -1)
            {
                /* has unit */
                memcpy(unit, (void *)&working_comment[matches[2].rm_so], matches[2].rm_eo - matches[2].rm_so);
            }

            if (matches[4].rm_so != -1)
            {
                /* has missing */
                memcpy(missing, (void *)&working_comment[matches[4].rm_so], matches[4].rm_eo - matches[4].rm_so);
            }

            snprintf(stripped_comment, FLEN_COMMENT, "%s", (char *)&working_comment[matches[5].rm_so]); /* copy to end of comment string*/
        }
        else
        {
            /* does not match, has no unit */
            //snprintf(stripped_comment, FLEN_COMMENT, "%s", comment);
            //*unit = '\0';
            err = CFITSIO_ERROR_ARGS;
        }
    }

    if (working_comment)
    {
        free(working_comment);
        working_comment = NULL;
    }

    return err;
}

/* read key `key`->key_name from `file`, and initialize `key` with values read from `file`;
 * always uses 8 bytes for ints and floats;
 */
int cfitsio_read_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key)
{
    char keyword_name[CFITSIO_MAX_KEYNAME + 1];
    cfitsio_keyword_datatype_t keyword_type = '\0';
    int number_keys = -1;
    int is_history_or_comment = 0;
    char *include_list = NULL;
    char card[FLEN_CARD] = {0};
    char card_value[FLEN_CARD] = {0};
    char card_comment[FLEN_CARD] = {0};
    char *history_or_comment = NULL;
    size_t sz_history_or_comment = FLEN_CARD + 1;
    char comment[CFITSIO_MAX_STR] = {0};
    char unit[FLEN_COMMENT] = {0};
    char missing[FLEN_COMMENT] = {0};
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    /* in order to read a keyword from a fits file, the essential information is the keyword name and the keyword data type;
     * save those before overwriting them with a memory wipe */
    snprintf(keyword_name, sizeof(keyword_name), "%s", key->key_name);
    keyword_type = key->key_type;
    memset(key, 0, sizeof(CFITSIO_KEYWORD));
    snprintf(key->key_name, sizeof(key->key_name), "%s", keyword_name);
    key->key_type = keyword_type;

    /* make sure we have at least one keyowrd in `file` */
    if (fits_get_hdrspace(file->fptr, &number_keys, NULL, &cfiostat))
    {
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* go to beginning of header */
        if (fits_read_record(file->fptr, 0, NULL, &cfiostat))
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* must special-case HISTORY/COMMENT; we need to call fits_read_card() repeatedly until FITSIO returns KEY_NO_EXIST */
        if (strcasecmp(keyword_name, CFITSIO_KEYWORD_HISTORY) == 0)
        {
            is_history_or_comment = 1;
            err = cfitsio_read_history_key(file, key); /* no unit to deal with */
        }
        else if ( strcasecmp(keyword_name, CFITSIO_KEYWORD_COMMENT) == 0)
        {
            is_history_or_comment = 1;
            err = cfitsio_read_comment_key(file, key); /* no unit to deal with */
        }
        else if (keyword_type == CFITSIO_KEYWORD_DATATYPE_STRING)
        {
            /* any string could be a long string (> 68 chars) so use fits_read_key_longstr() */
            fits_read_key_longstr(file->fptr, keyword_name, &key->key_value.vs, comment, &cfiostat);
        }
        else if (keyword_type == CFITSIO_KEYWORD_DATATYPE_LOGICAL)
        {
            fits_read_key_log(file->fptr, keyword_name, &key->key_value.vl, comment, &cfiostat);
        }
        else if (keyword_type == CFITSIO_KEYWORD_DATATYPE_INTEGER)
        {
            fits_read_key_lnglng(file->fptr, keyword_name, &key->key_value.vi, comment, &cfiostat);
        }
        else if (keyword_type == CFITSIO_KEYWORD_DATATYPE_FLOAT)
        {
            fits_read_key_dbl(file->fptr, keyword_name, &key->key_value.vf, comment, &cfiostat);
        }
        else
        {
            err = CFITSIO_ERROR_INVALID_DATA_TYPE;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        /* for HISTORY/COMMENT keyword, cfiostat == 0 here */
        if (cfiostat == VALUE_UNDEFINED)
        {
            /* value field of keyword is blank (a missing value) */
            key->is_missing = 1;
            cfiostat = 0;
        }
        else if (cfiostat != 0)
        {
            /* an error, set err */
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (err == CFITSIO_SUCCESS)
    {
        if (!is_history_or_comment)
        {
            /* must deal with unit inside comment field */
            if (*comment != '\0')
            {
                /* strip any unit substring that may exist; the unit string goes in key->key_unit instead */
                err = cfitsio_strip_keyword_unit_and_missing(comment, key->key_comment, key->key_unit, missing);
                if (*missing != '\0')
                {
                    if (strcasecmp(missing, CFITSIO_KEYWORD_COMMENT_MISSING) == 0)
                    {
                        key->is_missing = 1;
                    }
                }
            }
        }
    }

    if (cfiostat != 0)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_read_header_key() ] unable to read keyword '%s'\n", keyword_name);
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        /* err will be set (if cfiostat != 0 ==> err)*/
    }

    if (history_or_comment)
    {
        free(history_or_comment);
        history_or_comment = NULL;
    }

    return err;
}

int cfitsio_write_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key)
{
    return Cf_write_header_key(file->fptr, key);
}

int cfitsio_write_header_keys(CFITSIO_FILE *file, char **header, CFITSIO_KEYWORD *key_list, CFITSIO_FILE *fits_header)
{
    return Cf_write_header_keys(file->fptr, header, key_list, fits_header->fptr);
}

static int Cf_update_key_null(fitsfile *fptr, const char *keyord_name, const char *keyword_comment, const char *keyword_unit)
{
    char *comment = NULL;
    size_t sz_comment = FLEN_COMMENT + 1;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    comment = calloc(sz_comment, sizeof(char));
    if (!comment)
    {
        err = CFITSIO_ERROR_OUT_OF_MEMORY;
    }

    if (!err)
    {
        /* if unit is 'none' or 'na' or missing, then omit [<unit>] */
        if (!is_keyword_unit_null(keyword_unit))
        {
            comment = base_strcatalloc(comment, "[", &sz_comment);
            comment = base_strcatalloc(comment, keyword_unit, &sz_comment);
            comment = base_strcatalloc(comment, "] ", &sz_comment);
        }

        comment = base_strcatalloc(comment, "(", &sz_comment);
        comment = base_strcatalloc(comment, CFITSIO_KEYWORD_COMMENT_MISSING, &sz_comment);
        comment = base_strcatalloc(comment, ")", &sz_comment);

        if (*keyword_comment != '\0')
        {
            comment = base_strcatalloc(comment, " ", &sz_comment);
            comment = base_strcatalloc(comment, keyword_comment, &sz_comment);
        }

        fits_update_key_null(fptr, keyord_name, comment, &cfiostat);
    }

    if (comment)
    {
        free(comment);
        comment = NULL;
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_update_key_null() ] error writing key %s: %s\n", keyord_name, cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

/* use `key` to update a keyword in `file` */
int cfitsio_update_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key)
{
    char *comment = NULL;
    int is_history_or_comment = 0;
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    if (key->is_missing)
    {
        err = Cf_update_key_null(file->fptr, key->key_name, key->key_comment, key->key_unit);
    }
    else
    {
        /* use fits_update_key_*() which will update an existing key, or append a new one */
        if (strcmp(key->key_name,"HISTORY") == 0)
        {
            is_history_or_comment = 1;

            /* ARGH - FITSIO!; fits_write_history() will silently fail if the history keyword value is the empty string;
             * then later code that tries to read the history keyword will fail; so simply delete the history
             * keywords if the keyword value is the empty string */
            err = cfitsio_update_history_key(file, key);
        }
        else if (!strcmp(key->key_name,"COMMENT"))
        {
            is_history_or_comment = 1;

            /* ARGH - FITSIO!; fits_write_comment() will silently fail if the comment keyword value is the empty string;
             * then later code that tries to read the comment keyword will fail; so simply delete the comment
             * keywords if the keyword value is the empty string */
            err = cfitsio_update_comment_key(file, key);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
        {
            /* any string could be a long string (> 68 chars) */
            fits_update_key_longstr(file->fptr, key->key_name, key->key_value.vs, key->key_comment, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_LOGICAL)
        {
            fits_update_key_log(file->fptr, key->key_name, key->key_value.vl, key->key_comment, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_INTEGER)
        {
            fits_update_key_lng(file->fptr, key->key_name, key->key_value.vi, key->key_comment, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_FLOAT)
        {
            /* prints 17 decimal places, if needed */
            fits_update_key_dbl(file->fptr, key->key_name, key->key_value.vf, -17, key->key_comment, &cfiostat);
        }
        else
        {
            err = CFITSIO_ERROR_INVALID_DATA_TYPE;
        }

        if (err == CFITSIO_SUCCESS)
        {
            if (!is_history_or_comment)
            {
                if (!is_keyword_unit_null(key->key_unit))
                {
                    fits_write_key_unit(file->fptr, key->key_name, key->key_unit, &cfiostat);
                }
            }
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_update_header_key() ] CFITSIO error updating key %s: %s\n", key->key_name, cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

/* update keywords in `file` with keywords in `header` that are also in `key_list`;
 * always uses 8 bytes for ints and floats
 */
int cfitsio_update_header_keywords(CFITSIO_FILE *file, CFITSIO_HEADER *header, CFITSIO_KEYWORD *key_list)
{
    int nkeys = 0;
    int nrec = 0;
    char card[FLEN_CARD];
    char key_name[FLEN_KEYWORD];
    char key_value[FLEN_VALUE];
    int key_name_len = 0;
    HContainer_t *key_hcon = NULL;
    CFITSIO_KEYWORD one_key;
    CFITSIO_KEYWORD *key_node = NULL;
    CFITSIO_KEYWORD *cfitsio_key = NULL;
    CFITSIO_KEYWORD **cfitsio_key_ptr = NULL;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostatErrmsg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    XASSERT(file);

    /* ART - I think this key_hcon is not being used */
    if (key_list)
    {
        /* make a hash table of the 'acceptable' FITS keyword names */
        err = Cf_make_hcon(key_list, &key_hcon);
    }

    if (!err)
    {
        fits_get_hdrspace(header->fptr, &nkeys, NULL, &cfiostat);
        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    if (!err)
    {
        if (key_list)
        {
            /* since string keywords might span multiple FITS records, we have to use the fits_update_key_*()
             * functions to locate keywords in file - we cannot iterate through file's records */
            key_node = key_list;

            while (key_node)
            {
                cfitsio_key_ptr = hcon_lookup_lower(key_hcon, key_node->key_name);

                if (!cfitsio_key_ptr)
                {
                    fprintf(stderr, "key %s not in key_hcon\n", key_node->key_name);
                }
                else
                {
                    cfitsio_key = *cfitsio_key_ptr;

                    memset(&one_key, '\0', sizeof(one_key));

                    snprintf(one_key.key_name, sizeof(one_key.key_name), "%s", cfitsio_key->key_name);
                    one_key.key_type = cfitsio_key->key_type;

                    /* fits_update_key_*() will update the keyword if it exists, otherise it will
                     * append the key to the end of the HDU */

                    if (one_key.key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
                    {
                        /* any string could be a long string (> 68 chars) */
                        if (cfitsio_read_header_key(header, &one_key) == CFITSIO_SUCCESS)
                        {
                            if (cfitsio_update_header_key(file, &one_key))
                            {
                                fprintf(stderr, "cannot update key %s\n", key_node->key_name);
                            }
                        }

                        if (one_key.key_value.vs)
                        {
                            fits_free_memory(one_key.key_value.vs, &cfiostat);
                            one_key.key_value.vs = NULL;
                        }
                    }
                    else
                    {
                        if (cfitsio_read_header_key(header, &one_key) == CFITSIO_SUCCESS)
                        {
                            if (cfitsio_update_header_key(file, &one_key))
                            {
                                fprintf(stderr, "cannot update key %s\n", key_node->key_name);
                            }
                        }
                    }
                }

                key_node = key_node->next;
            }
        }
        else
        {
            /* no key_list, so update `file` with ALL `header` records */

            /* iterate through header cards */
            for (nrec = 1; nrec <= nkeys && !cfiostat; nrec++)
            {
                memset(card, '\0', sizeof(card));

                /* read record from `header` */
                fits_read_record(header->fptr, nrec, card, &cfiostat);

                if (cfiostat)
                {
                    continue;
                }

                fits_get_keyname(card, key_name, &key_name_len, &cfiostat);

                if (cfiostat)
                {
                    continue;
                }

                /* write record to `file` */
                fits_modify_card(file->fptr, key_name, card, &cfiostat);

                if (cfiostat)
                {
                    continue;
                }
            }
        }
    }
    else
    {
        err = CFITSIO_ERROR_OUT_OF_MEMORY;
    }

    if (cfiostat)
    {
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (key_hcon)
    {
        hcon_destroy(&key_hcon);
    }

    return err;
}

int cfitsio_flush_buffer(CFITSIO_FILE *fitsFile)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostatErrmsg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

#if 0
    if (fits_flush_buffer(fitsFile->fptr, 0, &cfiostat))
#endif
    if (fits_flush_file(fitsFile->fptr, &cfiostat))
    {
        fits_get_errstatus(cfiostat, cfiostatErrmsg);
        fprintf(stderr, "[ cfitsio_flush_buffer() ] CFITSIO error: %s\n", cfiostatErrmsg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

int cfitsio_delete_header_key(CFITSIO_FILE *fitsFile, const char *key)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    int err = CFITSIO_SUCCESS;

    fits_delete_key(fitsFile->fptr, key, &cfiostat); /* ignore error if key does not exist */

    return err;
}

int cfitsio_delete_headsum(CFITSIO_FILE *fitsFile)
{
    return cfitsio_delete_header_key(fitsFile, HEADSUM);
}

/*
 * file     - the CFITSIO_FILE file that contains keyword values from which a checksum is to be generated; if NULL, then
 *            create a new CFITSIO_FILE that contains a header formed from `key_list`, and also
 *            create the checksum from the values passed in via `key_list`; otherwise use the keyword values in `file`
 *            of the keywords specified in `key_list` to generate the checksum
 * key_list - the list of keywords from which a FITS header and checksum will be generated
 * checksum - the checksum returned by reference; must be freed by caller
 */
int cfitsio_generate_checksum(CFITSIO_HEADER **file, CFITSIO_KEYWORD *key_list, char **checksum)
{
    // Hash_Table_t inclKeysHT;
    CFITSIO_KEYWORD *kptr = NULL;
    char card[FLEN_CARD];
    char *md5Input = NULL;

    CFITSIO_HEADER *file_to_checksum = NULL;
    CFITSIO_HEADER *filtered_keys_file = NULL;
    HContainer_t *key_hcon = NULL;
    int nkeys = 0;
    int nrec = 0;
    char key_name[FLEN_KEYWORD];
    char key_value[FLEN_VALUE];
    int key_name_len = 0;
    unsigned long datasum;
    unsigned long hdusum;
    char sumString[32]; /* always 16 characters */
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostatErrmsg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    XASSERT(checksum);

    if (checksum)
    {
        if (key_list)
        {
            /* use the keyword values in `key_list` for checksum calculation */

            /* need to create a CFITSIO_FILE from key_list */
            if (cfitsio_create_file(&filtered_keys_file, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL, NULL))
            {
                fprintf(stderr, "[ cfitsio_generate_checksum() ] unable to create empty FITS file\n");
                err = CFITSIO_ERROR_OUT_OF_MEMORY;
            }

            if (!err)
            {
                if (*file)
                {
                    /* use the keyword values in `*file` for checksum calculation, filtering-in the keys used
                     * with the list in `key_list`; store resulting keyword values in `filtered_keys_file`;
                     * do not return `filtered_keys_file` to caller */

                    /* make a hash table of the 'acceptable' FITS keyword names */
                    err = Cf_make_hcon(key_list, &key_hcon);

                    if (!err)
                    {
                        fits_get_hdrspace((*file)->fptr, &nkeys, NULL, &cfiostat);

                        if (!cfiostat)
                        {
                            /* iterate through `*file` cards, accepting keys that are in key_hcon */
                            for (nrec = 1; nrec <= nkeys; nrec++)
                            {
                                memset(card, '\0', sizeof(card));
                                fits_read_record((*file)->fptr, nrec, card, &cfiostat);

                                if (cfiostat)
                                {
                                    fprintf(stderr, "unable to read record %d; skipping\n", nrec);
                                    cfiostat = 0;
                                    continue;
                                }

                                if (fits_get_keyclass(card) > TYP_CMPRS_KEY)
                                {
                                    /* extract keyword name from card */
                                    fits_read_keyn((*file)->fptr, nrec, key_name, key_value, NULL, &cfiostat);

                                    if (cfiostat)
                                    {
                                        fprintf(stderr, "unable to read record %d; skipping\n", nrec);
                                        cfiostat = 0;
                                        continue;
                                    }
                                    else if (!hcon_member_lower(key_hcon, key_name))
                                    {
                                        continue;
                                    }

                                    fits_write_record(filtered_keys_file->fptr, card, &cfiostat);
                                }
                            }
                        }
                    }

                    hcon_destroy(&key_hcon);
                }
                else
                {
                    /* use `file` (in_memory == true) to return to caller filtered keywords that were written to
                     * in-memory `filtered_keys_file` */
                    if (!cfiostat)
                    {
                        /* iterate over `key_list`, writing FITS keywords to `fptr`;
                         * will NOT create and write HEADSUM keyword */
                        err = Cf_write_header_keys(filtered_keys_file->fptr, NULL, key_list, NULL);
                    }
                    else
                    {
                        err = CFITSIO_ERROR_LIBRARY;
                        fprintf(stderr, "[ cfitsio_generate_checksum() ] Unable to write SIMPLE/BITPIX/NAXIS keywords\n");
                    }
                }
            }

            file_to_checksum = filtered_keys_file;
        }
        else if (*file)
        {
            /* no `key_list` so use all keywords in `file` to calculate checksum */

            /* use the keyword values in `*file` for checksum calculation */
            file_to_checksum = *file;
        }

        if (!err)
        {
            if (file_to_checksum)
            {
                err = CfGenerateChecksum(file_to_checksum->fptr, checksum);
            }
        }

        if (!err)
        {
            if (filtered_keys_file)
            {
                if (!*file)
                {
                    /* return filtered_keys_file if *file == NULL */
                    *file = filtered_keys_file;
                }
                else
                {
                    cfitsio_close_file(&filtered_keys_file);
                }
            }
        }
    }

    return err;
}

int cfitsio_open_file(const char *path, CFITSIO_FILE **file, int writeable)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    struct stat stbuf;
    fitsfile *fptr = NULL;
    int fileCreated = 0;
    int fits_compression_type = 0;
    CFITSIO_COMPRESSION_TYPE cfitsio_compression_type = CFITSIO_COMPRESSION_NONE;
    int err = CFITSIO_SUCCESS;

    if (stat(path, &stbuf) == -1)
    {
        fprintf(stderr, "[ cfitsio_open_file() ] file not found: %s\n", path);
        err = CFITSIO_ERROR_FILE_DOESNT_EXIST;
    }

    if (!err)
    {
        XASSERT(file);

        if (file)
        {
            if (!*file)
            {
                *file = calloc(1, sizeof(CFITSIO_FILE));
            }

            if (!*file)
            {
                fprintf(stderr, "[ cfitsio_open_file() ] unable to allocate fitsfile structure\n");
                err = CFITSIO_ERROR_OUT_OF_MEMORY;
            }

            if (!err)
            {
                fptr = (fitsfile *)fitsrw_getfptr(0, path, (writeable != 0), &err, &fileCreated);
                XASSERT(!fileCreated);

                if (!fptr)
                {
                    err = CFITSIO_ERROR_FILE_DOESNT_EXIST;
                }
            }

            if (!err)
            {
                (*file)->fptr = fptr;
                (*file)->in_memory = 1;
                (*file)->state = CFITSIO_FILE_STATE_INITIALIZED;
                (*file)->type = CFITSIO_FILE_TYPE_UNKNOWN;
                (*file)->compression_type = CFITSIO_COMPRESSION_NONE;
                (*file)->export_compression_type = CFITSIO_COMPRESSION_NONE;

                /* file type */
                err = cfitsio_get_file_type((*file), &((*file)->type));

                if ((*file)->type == CFITSIO_FILE_TYPE_IMAGE)
                {
                    /* data compression type */
                    /* returns 0 if compression type has never been set (NOCOMPRESS is -1) */
                    err = Cf_get_compression_type(fptr, &fits_compression_type);

                    if (err == CFITSIO_SUCCESS)
                    {
                        err = Cf_map_compression_type(fits_compression_type, &cfitsio_compression_type);
                    }

                    if (err == CFITSIO_SUCCESS)
                    {
                        (*file)->compression_type = cfitsio_compression_type;
                    }

                    /* export compression type */
                    if (err == CFITSIO_SUCCESS)
                    {
                        err = Cf_get_export_compression_type(fptr, &fits_compression_type);
                    }

                    if (err == CFITSIO_SUCCESS)
                    {
                        err = Cf_map_compression_type(fits_compression_type, &cfitsio_compression_type);
                    }

                    if (err == CFITSIO_SUCCESS)
                    {
                        (*file)->export_compression_type = cfitsio_compression_type;
                    }
                }
            }
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_open_file() ] unable to write checksum keywords\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
    }

    return err;
}

int cfitsio_read_headsum(CFITSIO_FILE *file, char **headsum)
{
    char strVal[FLEN_VALUE];
    int cfiostat = 0;
    char cfiostatErrmsg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    XASSERT(headsum);
    if (headsum)
    {
        *headsum = NULL;

        if (fits_read_key(file->fptr, TSTRING, HEADSUM, strVal, NULL, &cfiostat))
        {
            if (cfiostat == KEY_NO_EXIST)
            {
                /* this is OK; not all files will have a HEADSUM yet */
#if 0
                fprintf(stderr, "[ cfitsio_read_headsum() ] NOTE: FITS file %s does not contain a HEADSUM keyword\n", file->fptr->Fptr->filename);
#endif
                cfiostat = 0;
            }
            else
            {
                fits_get_errstatus(cfiostat, cfiostatErrmsg);
                fprintf(stderr, "[ cfitsio_read_headsum() ] CFITSIO error '%s'\n", cfiostatErrmsg);
                err = CFITSIO_ERROR_LIBRARY;
            }
        }

        if (!err)
        {
            if (*strVal != '\0')
            {
                *headsum = strdup(strVal);
            }
        }
    }

    return err;
}

static int Cf_write_headsum(fitsfile *fptr, const char *headsum)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    if (headsum)
    {
        fits_update_key_str(fptr, HEADSUM, headsum, "Keyword checksum", &cfiostat);

        if (cfiostat)
        {
            fits_get_errstatus(cfiostat, cfiostat_msg);
            fprintf(stderr, "[ Cf_write_headsum() ] CFITSIO error '%s'\n", cfiostat_msg);
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    return err;
}

int cfitsio_write_headsum(CFITSIO_FILE *file, const char *headsum)
{
    return Cf_write_headsum(file->fptr, headsum);
}

/* write the DATASUM and CHECKSUM FITS keyword for the current HDU */
static int Cf_write_chksum(fitsfile *fptr)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    if (fptr)
    {
        /* not globally cached */
        fits_write_chksum(fptr, &cfiostat);
        if (cfiostat)
        {
            fits_get_errstatus(cfiostat, cfiostat_msg);
            fprintf(stderr, "[ Cf_write_chksum() ] unable to write checksum keywords\n");
            fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
            err = CFITSIO_ERROR_LIBRARY;
        }
    }

    return err;
}

/* write the DATASUM and CHECKSUM FITS keyword for the current HDU; this only needs to be done for non-cached FITS files, since the caching code
 * in fitsrw_closefptr() will write the checksums when a file is closed */
int cfitsio_write_chksum(CFITSIO_FILE *file)
{
    int err = CFITSIO_SUCCESS;


    if (file && file->fptr)
    {
        err = Cf_write_chksum(file->fptr);
    }

    return err;
}

/* VERY IMPORTANT!! Always write LONGWARN last. There is a bug in FITSIO - anything written after writing
 * LONGWARN will automatically disappear. */
static int Cf_write_longwarn(fitsfile *fptr)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    fits_write_key_longwarn(fptr, &cfiostat);

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_write_longware() ] CFITSIO error '%s'\n", cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

int cfitsio_write_longwarn(CFITSIO_FILE *file)
{
    return Cf_write_longwarn(file->fptr);
}

/****************************************************************************/
/* if `keylist` is NULL, create and return a CFITSIO_KEYWORD, but do not append it to the list */
int cfitsio_append_header_key(CFITSIO_KEYWORD** keylist, const char *name, cfitsio_keyword_datatype_t type, int number_bytes, const void *value, const char *format, const char *comment, const char *unit, CFITSIO_KEYWORD **key_out)
{
    CFITSIO_KEYWORD *node = NULL;
    CFITSIO_KEYWORD *last = NULL;
    int error_code = CFITSIO_SUCCESS;

    if (name && (keylist || key_out))
    {
        node = (CFITSIO_KEYWORD *)calloc(1, sizeof(CFITSIO_KEYWORD));

        if (!node)
        {
            return CFITSIO_ERROR_OUT_OF_MEMORY;
        }
        else
        {
            node->next = NULL;

            if (keylist)
            {
                // append node to the end of list - O(n*n) run time!
                if (*keylist)
                {
                    last = *keylist;
                    while (last->next)
                    {
                        last = last->next;
                    }

                    last->next = node;
                }
                else // first node
                {
                    *keylist = node;
                }
            }

            snprintf(node->key_name, sizeof(node->key_name), "%s", name);
            node->key_type = type;

            if (!value)
            {
                node->is_missing = 1;
            }
            else
            {
                node->is_missing = 0;
            }

            // save value into union
            switch (type)
            {
                case( 'X'):
                case (CFITSIO_KEYWORD_DATATYPE_STRING):
                    /* 68 is the max chars in FITS string keyword, but the HISTORY and COMMENT keywords
                     * can contain values with more than this number of characters, in which case
                     * the fits API key-writing function will split the string across multiple
                     * instances of these special keywords. */
                    if (value)
                    {
                        node->key_value.vs = strdup((char *)value);
                    }

                    snprintf(node->key_tform, sizeof(node->key_tform), "1PA");
                    break;
                case (CFITSIO_KEYWORD_DATATYPE_LOGICAL):
                    if (value)
                    {
                        node->key_value.vl = *((int *)value);
                    }

                    snprintf(node->key_tform, sizeof(node->key_tform), "1L");
                    break;
                case (CFITSIO_KEYWORD_DATATYPE_INTEGER):
                    /* use hack to make this more efficient - do not use 'K' for 1-, 2-, or 4-byte integers:
                     * 1-byte integers ==> "1A"
                     * 2-byte integers ==> "1I"
                     * 4-byte integers ==> "1J"
                     * 8-byte integers ==> "1K"
                     */
                    if (value)
                    {
                        node->key_value.vi = *((long long *)value);
                    }

                    if (number_bytes == 1)
                    {
                        snprintf(node->key_tform, sizeof(node->key_tform), "1A");
                        node->number_bytes = 1;
                    }
                    else if (number_bytes == 2)
                    {
                        snprintf(node->key_tform, sizeof(node->key_tform), "1I");
                        node->number_bytes = 2;
                    }
                    else if (number_bytes == 4)
                    {
                        snprintf(node->key_tform, sizeof(node->key_tform), "1J");
                        node->number_bytes = 4;
                    }
                    else if (number_bytes == 8)
                    {
                        snprintf(node->key_tform, sizeof(node->key_tform), "1K");
                        node->number_bytes = 8;
                    }
                    else
                    {
                        fprintf(stderr, "[ cfitsio_append_header_key() ] invalid number_bytes value %d\n", number_bytes);
                        error_code = CFITSIO_ERROR_ARGS;
                    }

                    break;
                case (CFITSIO_KEYWORD_DATATYPE_FLOAT):
                    /* use hack to make this more efficient - do not use 'D' for 4-byte floats:
                     * 4-byte floats ==> "1E"
                     * 8-byte floats ==> "1D"
                     */
                    if (value)
                    {
                        node->key_value.vf = *((double *)value);
                    }

                    if (number_bytes == 4)
                    {
                        snprintf(node->key_tform, sizeof(node->key_tform), "1E");
                        node->number_bytes = 4;
                    }
                    else if (number_bytes == 8)
                    {
                        snprintf(node->key_tform, sizeof(node->key_tform), "1D");
                        node->number_bytes = 8;
                    }
                    else
                    {
                        fprintf(stderr, "[ cfitsio_append_header_key() ] invalid number_bytes value %d\n", number_bytes);
                        error_code = CFITSIO_ERROR_ARGS;
                    }

                    break;
                default:
                    fprintf(stderr, "invalid cfitsio keyword type '%c'\n", (char)type);
                    error_code = CFITSIO_ERROR_ARGS;
                    break;
            }

            if (format)
            {
                snprintf(node->key_format, sizeof(node->key_format), "%s", format);
            }

            if (comment)
            {
                snprintf(node->key_comment, sizeof(node->key_comment), "%s", comment);
            }

            if (unit)
            {
                snprintf(node->key_unit, sizeof(node->key_unit), "%s", unit);
            }
            else
            {
                snprintf(node->key_unit, sizeof(node->key_unit), "none");
            }

            if (key_out)
            {
                *key_out = node;
            }
        }
    }
    else
    {
        error_code = CFITSIO_ERROR_ARGS;
    }

    return error_code;
}

/****************************************************************************/
// Row ranges from 1 to NAXIS1
// Col ranges from 1 to NAXIS2
// Different image types, more than 2 dimensions, later...

int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, long from_row,
		       long to_row, long from_col, long to_col)
{
   short* iptr;
   long i, j;

   DEBUGMSG((stderr,"In cfitsio_dump_image() => image = %p\n", image));

   if((from_row < 1)||(to_row > info->naxes[1])||(from_col < 1)||
      (to_col > info->naxes[0]))
   {
      DEBUGMSG((stderr,"cfitsio_dump_image() out of range.\n"));
      return CFITSIO_FAIL;
   }

   for(i=from_row;i<=to_row;i++)
   {
      iptr = ((short*) (image)) + ((i-1)* info->naxes[0])+ (from_col-1);
      for(j=from_col;j<=to_col;j++,iptr++)
      {
	 printf("%d ",*iptr);
      }
      printf("\n");
   }

   return CFITSIO_SUCCESS;
}

/****************************************************************************/

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info)
{
   if(*image_info) free(*image_info);
   *image_info = NULL;

   return CFITSIO_SUCCESS;
}

/****************************************************************************/
// image_info returns a subset of keywords used by DRMS, where
// (1) SIMPLE, EXTEND, BLANK, BSCALE, BZERO are extracted from keylist
// (2) NAXIS, NAXES[],BITPIX  are extracted from fits_get_image_param()
// In a compressed image, NAXIS, NAXES[] and BITPIX in the header are actually
// the dimensions of the binary table (not the image)
// image_info always has the correct (uncompressed) dimension of the image buffer.
// We keep geting keylist and image dimensions together here in one function (atomic process)

/* If can return both keylistout and image_info, and there are conflicting values, then
 * must remove conflicting values from keylistout; `keylistout` is used ONLY when reading non-internal, non-SUMS FITS files */

/* ART - keyword parsing will not work for long-string FITS keywords (ones that use CONTINUE); but not used for internal, SUMS FITS files  */
static int cfitsio_read_keylist_and_image_info(fitsfile* fptr, CFITSIO_KEYWORD** keylistout, CFITSIO_IMAGE_INFO** image_info)
{
    char card[FLEN_CARD];
    int key_class = -1;
    int status = 0;
    int nkeys;
    int error_code = CFITSIO_FAIL;
    CFITSIO_KEYWORD* node, *last, *kptr;
    char key_name[FLEN_KEYWORD];
    char key_value[FLEN_VALUE];
    char *long_string = NULL;
    int len, i;
    CFITSIO_KEYWORD *keylist = NULL;
    int currHDU = 0;
    int val = 0;

    //-----------------------------  Keylist ------------------------------------------

    if(keylistout && *keylistout)
    {
        cfitsio_free_keys(keylistout);
        *keylistout = NULL;
    }

    if(fits_get_hdrspace(fptr, &nkeys, NULL, &status))
    {
        error_code = CFITSIO_ERROR_LIBRARY;
        goto error_exit;
    }

    for(i=1; i<=nkeys; i++)
    {
        if (fits_read_record(fptr, i, card, &status))
        {
            error_code = CFITSIO_ERROR_LIBRARY;
            goto error_exit;
        }

        key_class = fits_get_keyclass(card);

        if (key_class == TYP_CMPRS_KEY || key_class == TYP_CONT_KEY)
        {
            /* use fits_read_key_longstr() to deal with `CONTINUE` cards */
            continue;
        }

        if(fits_get_keyname(card, key_name, &len, &status))
        {
            error_code = CFITSIO_ERROR_LIBRARY;
            goto error_exit;
        }

        node = (CFITSIO_KEYWORD *) malloc(sizeof(CFITSIO_KEYWORD));

        if(node == NULL)
        {
            error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
            goto error_exit;
        }

        memset(node, 0, sizeof(CFITSIO_KEYWORD));

        if(keylist==NULL) //first item
        {
            keylist = node;
            node->next = NULL;
            last = node;
        }
        else
        {
            node->next = NULL;
            last->next = node;
            last = node;
        }

        // special key
        if((!strcmp(key_name,"COMMENT")) || (!strcmp(key_name,"HISTORY")))
        {
            strcpy(node->key_name, key_name);
            node->key_type=kFITSRW_Type_String;

            if(fits_parse_value(card, key_value, node->key_comment, &status))
            {
                error_code = CFITSIO_ERROR_LIBRARY;
                goto error_exit;
            }

            // The actual comment (excluding the COMMENT key name) was placed
            // into node->key_comment - copy that into value.vs
            node->key_value.vs = strdup(node->key_comment);
        }
        else //regular key=value
        {
            strcpy(node->key_name, key_name);

            if(fits_parse_value(card, key_value, node->key_comment, &status))
            {
                error_code = CFITSIO_ERROR_LIBRARY;
                goto error_exit;
            }

            if(strlen(key_value))
            {
                fits_get_keytype(key_value, &node->key_type, &status);
            }
            else
            {
                node->is_missing = 1;
                node->key_type = kFITSRW_Type_String;
                node->key_value.vs = NULL;
            }

            if (!node->is_missing)
            {
                switch(node->key_type)
                {
                    case ('X'): //complex number is stored as string, for now.
                    case (kFITSRW_Type_String): //Trip off ' ' around cstring?
                        /* ugh - could be a 'long string'; must use fits_read_key_longstr() to read, or parse
                         * CONTINUE; the index `i` contains the keynum (number of the record) */
                        fits_read_key_longstr(fptr, node->key_name, &long_string, NULL, &status);
                        node->key_value.vs = strdup(long_string);
                        if (long_string)
                        {
                            fits_free_memory(long_string, &status);
                            long_string = NULL;
                        }

                        break;

                    case (kFITSRW_Type_Logical):
                        if (key_value[0]=='0')
                        {
                            node->key_value.vl = 0;
                        }
                        else
                        {
                            node->key_value.vl = 1;
                        }
                        break;

                    case (kFITSRW_Type_Integer):
                        /* ART - FITSIO uses something much more complex (it will convert a string value to a long long, for example) -
                         * uses strtoll() on integer strings */
                        sscanf(key_value,"%lld", &node->key_value.vi);

                        if (node->key_value.vi <= (long long)SCHAR_MAX && node->key_value.vi >= (long long)SCHAR_MIN)
                        {
                            node->number_bytes = 1;
                        }
                        else if (node->key_value.vi <= (long long)SHRT_MAX && node->key_value.vi >= (long long)SHRT_MIN)
                        {
                            node->number_bytes = 2;
                        }
                        else if (node->key_value.vi <= (long long)INT_MAX && node->key_value.vi >= (long long)INT_MIN)
                        {
                            node->number_bytes = 4;
                        }
                        else
                        {
                            node->number_bytes = 8;
                        }
                        break;

                    case (kFITSRW_Type_Float):
                        /* ART - FITSIO uses something much more complex (it will convert a string value to a double, for example) -
                         * uses strtod() on float strings */
                        sscanf(key_value,"%lf", &node->key_value.vf);

                        if (node->key_value.vf <= (double)FLT_MAX && node->key_value.vf >= (double)-FLT_MAX)
                        {
                            node->number_bytes = 4;
                        }
                        else
                        {
                            node->number_bytes = 8;
                        }

                        break;

                    default :
                        DEBUGMSG((stderr,"Key of unknown type detected [%s][%c]?\n", key_value, node->key_type));
                        break;
                }
            }

            fits_read_key_unit(fptr, node->key_name, node->key_unit, &status);
            if (status)
            {
                error_code = CFITSIO_ERROR_LIBRARY;
                fprintf(stderr, "unable to read FITS keyword '%s' unit value\n", node->key_name);
                goto error_exit;
            }
        }
    }

    if (status == END_OF_FILE)
    {
        status = 0; // Reset after normal error
    }

    if(status)
    {
        error_code = CFITSIO_FAIL;
        goto error_exit;
    }


    //---------------------------  Image Info  -------------------------

    // Extract SIMPLE, EXTEND, BLANK, BZERO, BSCALE from keylist

    if(image_info && *image_info)
    {
        cfitsio_free_image_info(image_info);
    }

    *image_info = (CFITSIO_IMAGE_INFO*) malloc(sizeof(CFITSIO_IMAGE_INFO));

    if(*image_info == NULL)
    {
        error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
        goto error_exit;
    }

    memset((void*) *image_info,0, sizeof(CFITSIO_IMAGE_INFO));

    //Set to defaults
    (*image_info)->blank = 0;
    (*image_info)->bscale = 1.0;
    (*image_info)->bzero = 0.0;

    /* You can't just assume the types of the keywords are the exact types
    * required by image_info; you need to convert them. */

    kptr = keylist;
    while (kptr && !status)
    {
        if(!strcmp(kptr->key_name,"SIMPLE"))
        {
            // ART - should check to see if keyword value is missing first
            (*image_info)->simple = cfitsio_keyval2int(kptr, &status);
            if (!status)
            {
                (*image_info)->bitfield |= kInfoPresent_SIMPLE;
            }
            else
            {
                fprintf(stderr, "Invalid value for keyword 'SIMPLE'.\n");
            }
        }
        else if(!strcmp(kptr->key_name,"EXTEND"))
        {
            // ART - should check to see if keyword value is missing first
            (*image_info)->extend = cfitsio_keyval2int(kptr, &status);
            if (!status)
            {
                (*image_info)->bitfield |= kInfoPresent_EXTEND;
            }
            else
            {
                fprintf(stderr, "Invalid value for keyword 'EXTEND'.\n");
            }
        }
        else if(!strcmp(kptr->key_name,"BLANK"))
        {
            // ART - should check to see if keyword value is missing first
            (*image_info)->blank = cfitsio_keyval2longlong(kptr, &status);
            if (!status)
            {
                (*image_info)->bitfield |= kInfoPresent_BLANK;
            }
            else
            {
                fprintf(stderr, "Invalid value for keyword 'BLANK'.\n");
            }
        }
        else if(!strcmp(kptr->key_name,"BSCALE"))
        {
            // ART - should check to see if keyword value is missing first
            (*image_info)->bscale = cfitsio_keyval2double(kptr, &status);
            if (!status)
            {
                (*image_info)->bitfield |= kInfoPresent_BSCALE;
            }
            else
            {
                fprintf(stderr, "Invalid value for keyword 'BSCALE'.\n");
            }
        }
        else if(!strcmp(kptr->key_name,"BZERO"))
        {
            // ART - should check to see if keyword value is missing first
            (*image_info)->bzero = cfitsio_keyval2double(kptr, &status);
            if (!status)
            {
                (*image_info)->bitfield |= kInfoPresent_BZERO;
            }
            else
            {
                fprintf(stderr, "Invalid value for keyword 'BZERO'.\n");
            }
        }

        kptr = kptr->next;
    }

    if (status)
    {
        goto error_exit;
    }

   /* Never use the fits keywords to determine NAXIS, NAXISi, BITPIX - if the image is compressed
    * these will be relevant to the compressed image.  But we need the values for the uncompressed
    * image.
    */

    // Check if this HDU has image
    fits_get_img_dim(fptr, &((*image_info)->naxis), &status);
    if((*image_info)->naxis == 0)
    {
        //"No image in this HDU.";
        error_code = CFITSIO_ERROR_DATA_EMPTY;
        goto error_exit;
    }

    // Always extract NAXIS, NAXES[], BITPIX from actual image
    fits_get_img_param(fptr, CFITSIO_MAX_DIM, &((*image_info)->bitpix), &((*image_info)->naxis), &((*image_info)->naxes[0]), &status);

    /* If this is a compressed image the default HDU is the one containing the compressed image, which
     * doesn't have SIMPLE or EXTEND keywords. So, fetch them from the primary HDU. */
    fits_get_hdu_num(fptr, &currHDU);
    if (currHDU != 1)
    {
        /* current HDU was not primary HDU */
        fits_movabs_hdu(fptr, 1, NULL, &status);

        /* Now get SIMPLE and EXTEND keywords */
        fits_read_key(fptr, TLOGICAL, "SIMPLE", &val, NULL, &status);
        if (!status)
        {
            (*image_info)->simple = val;
            (*image_info)->bitfield |= kInfoPresent_SIMPLE;
        }

        fits_read_key(fptr, TLOGICAL, "EXTEND", &val, NULL, &status);
        if (!status)
        {
            (*image_info)->extend = val;
            (*image_info)->bitfield |= kInfoPresent_EXTEND;
        }

        fits_movabs_hdu(fptr, currHDU, NULL, &status);
    }

    for(i=(*image_info)->naxis;i<(int) CFITSIO_MAX_DIM ;i++)
    {
        (*image_info)->naxes[i]=0;
    }

    (*image_info)->export_compression_type = CFITSIO_COMPRESSION_NONE; /* default */

    if (keylistout)
    {
        *keylistout = keylist;
    }
    else
    {
        cfitsio_free_keys(&keylist);
    }

    return CFITSIO_SUCCESS;


error_exit:

    if(keylist)
    {
        cfitsio_free_keys(&keylist);
    }

    if(image_info && *image_info)
    {
        free(*image_info);
        *image_info = NULL;
    }

    return error_code;
}

int fitsrw_read_keylist_and_image_info(FITSRW_fhandle fhandle,
                                       CFITSIO_KEYWORD** keylistout,
                                       CFITSIO_IMAGE_INFO** image_info)
{
   return cfitsio_read_keylist_and_image_info((fitsfile *)fhandle, keylistout, image_info);
}

/****************************************************************************/

/* `keylist` is never used by any caller (since the up-to-date keyword data is in the DRMS DB for all SUMS files) */
int fitsrw_readintfile(int verbose,
                       char* fits_filename,
                       CFITSIO_IMAGE_INFO** image_info,
                       void** image,
                       CFITSIO_KEYWORD** keylist)
{
   fitsfile *fptr=NULL;

   int status = 0;
   int error_code = CFITSIO_FAIL;

   int  data_type, bytepix, i;
   long	npixels;

   void* pixels = NULL;

   char cfitsiostat[FLEN_STATUS];

   int fileCreated = 0;

   // Move directly to first image
   fptr = (fitsfile *)fitsrw_getfptr(verbose, fits_filename, 0, &status, &fileCreated);

   XASSERT(!fileCreated);

   if (!fptr)
   {
      error_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }

   // Get keylist and image info (a separate function to reduce the function size)
   /* `keylist` is not used when reading a file in SUMS; they fits keys exist as DRMS keys
    * in the DRMS DB. */
   error_code = cfitsio_read_keylist_and_image_info(fptr, keylist, image_info);
   if(error_code != CFITSIO_SUCCESS) goto error_exit;


   switch((*image_info)->bitpix)
   {
      case(BYTE_IMG):    data_type = TBYTE; break; /* When reading, data are an unsigned char array */
      case(SHORT_IMG):   data_type = TSHORT; break;
      case(LONG_IMG):    data_type = TINT; break;
      case(LONGLONG_IMG):data_type = TLONGLONG; break;
      case(FLOAT_IMG):   data_type = TFLOAT; break;
      case(DOUBLE_IMG):  data_type = TDOUBLE; break;
   }

   bytepix = abs((*image_info)->bitpix)/8;

   npixels = 1;
   for(i=0;i<(*image_info)->naxis;i++) npixels *= (*image_info)->naxes[i];

   pixels = (double*) calloc(npixels, bytepix); //get alignment to double
   if(pixels == NULL)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }

   // Always read/write RAW data
   /* Don't let cfitsio apply the scale and blank keywords - scaling done in DRMS.
    * This is why, for the BYTE_IMG bitpix type, we must read as unsigned char, not signed char.
    * If you read signed char without applying the scale keywords, then the data values
    * are unsigned values, half of which (128-255) fall outside of the range of
    * signed chars.  This would cause overflow. */
   fits_set_bscale(fptr, 1.0, 0.0, &status);
   fits_set_imgnull(fptr, 0, &status);

   /* WARNING: IMPLEMENTED A WORK-AROUND - THIS SHOULD CONTINUE TO WORK EVEN IF
    * CFITSIO GETS UPDATED, BUT IT HAS EXTRA CODE THAT WON'T BE NECESSARY WHEN
    * CFITSIO GETS UPDATED.
    *
    * fits_read_img() has 3 known bugs that involve 2 of the parameters to this function:
    * 'nulval' and 'anynul'.
    * 1. If the image to be read is a compressed image and the nulval
    * argument provided is NULL, then cfitsio will convert all blank values to zeros.
    * (See cfitsio source code imcompress.c:imcomp_decompress_tile():3737).  The local
    * nullcheck variable is mostly likely erroneously set to 1, which causes conversion
    * of blanks to happen.  The value that blanks get convereted to is specifed
    * by the local variable nulval, which is set to &dummy, and dummy is set to 0.
    * 2. If the image to be read is an uncompressed image and the nulval
    * argument provided is not NULL, then cfitsio will convert all zero values to blanks.
    * 3. If the nulval argument is provided, but the anynul parameter is not AND
    * cfitsio does convert blanks to the value specified by the nulval argument,
    * cfitsio seg faults.  It is trying to set anynul (which is a boolean that say
    * "I changed at least one of your values"), but since it is NULL, the code crashes.
    */

   /* FITS DETERMINES WHICH VALUES ARE BLANK BY LOOKING AT THE PRIMARY HDU'S BLANK KEYWORD VALUE.
    * So make sure when you write a compressed image that you set BLANK in the primary HDU,
    * not the image HDU.
    */

   if(fits_read_img(fptr, data_type, 1, npixels, NULL, pixels, NULL, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   if (fptr)
   {
       /* We're reading a file, so don't worry about errors. */
       fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr);
   }

   //---------------------------------  BYTE_IMG --------------------------------------------------
   // For BYTE_IMG, DRMS expects signed byte buffer, but FITS always stores BYTE_IMG as unsigned byte
   // Conversion for BYTE_IMG here before returning the buffer to DRMS
   // BZERO and BLANK are set accordingly

   if ((*image_info)->bitpix == BYTE_IMG)
   {
      /* Subtract 128 from data so that it fits into the signed char data type expected
       * by DRMS */
      long ipix;
      unsigned char *pDataIn = (unsigned char *)pixels;
      signed char *pDataOut = (signed char *)pixels;

      for (ipix = 0; ipix < npixels; ipix++)
      {
	 pDataOut[ipix] = (signed char)(pDataIn[ipix] - 128);
      }

      /* D = d * bscale + bzero
       *
       * D - real, physical value
       * d - value returned by cfitsio, range is [0,255]
       * bscale - value associated with d
       * bzero - value associated with d
       *
       * ds = d - 128
       *
       * ds - scaled data so range is [-128,127]
       *
       * So,
       * D = d * bscale + bzero
       * D = (ds + 128) * bscale + bzero
       * D = ds * bscale + 128 * bscale + bzero
       * D = ds * BSCALE + BZERO
       *
       * BSCALE = bscale
       * BZERO = 128 * bscale + bzero
       */
      (*image_info)->bzero = 128.0 * (*image_info)->bscale + (*image_info)->bzero;

      /* The BLANK keyword value must also be shifted by 128. */
      (*image_info)->blank = (*image_info)->blank - 128;
   }
   //--------------------------------------------------------------------------------------------------


   if(keylist==NULL) // if caller function not asking for keylist
   {
      cfitsio_free_keys(keylist);
   }

   *image = pixels;

   if (status == 0) return (CFITSIO_SUCCESS);

error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);

   cfitsio_free_these(image_info, &pixels, keylist);

   if (fptr)
   {
       /* There is some other error, so don't worry about any error having to do with not closing the file pointer. */
       fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr);
   }

   return error_code;
}

/******************************************************************************/


void cfitsio_free_these(CFITSIO_IMAGE_INFO** image_info,
			void** image,
			CFITSIO_KEYWORD** keylist)
{

   if(image_info)
   {
      free(*image_info);
      *image_info = NULL;
   }

   if(image)
   {
      free(*image);
      *image = NULL;
   }


   if (keylist)
   {
      CFITSIO_KEYWORD *kptr = *keylist;
      CFITSIO_KEYWORD *del = NULL;

      while (kptr)
      {
	 del = kptr;
	 kptr = kptr->next;

         if (del->key_type == kFITSRW_Type_String && del->key_value.vs)
         {
            free(del->key_value.vs);
            del->key_value.vs = NULL;
         }

	 free(del);
      }

      *keylist = NULL;
   }

}

/****************************************************************************/

/* Assumes file will live in SUMS - eg, if image is char, then it is signed data, etc. */
int fitsrw_writeintfile(int verbose,
                        const char* fits_filename,
                        CFITSIO_IMAGE_INFO* image_info,
                        void* image,
                        const char* compspec,
                        CFITSIO_KEYWORD* keylist)
{

   fitsfile *fptr = NULL;
   int status=0, error_code = CFITSIO_FAIL;
   char filename[PATH_MAX];
   int  data_type;
   int img_type;
   char cfitsiostat[FLEN_STATUS];
   long long oblank = 0;

   CFITSIO_HEADER *header_for_checksum = NULL;
   char *checksum = NULL;

   long	long npixels;

   int i = 0;

   void *misspix = NULL;
   long long pixleft;
   long long pixtowrt;
   long long pixptr;

   int fileCreated = 0;
   long long imgSize = 0;

   // image_info contain the image dimensions, can not be missing
   if(image_info == NULL) return CFITSIO_ERROR_ARGS;


   for (i = 0, npixels = 1; i < image_info->naxis; i++)
   {
      npixels *= image_info->naxes[i];
   }

   switch(image_info->bitpix)
   {
      case(BYTE_IMG): data_type = TSBYTE; img_type = SBYTE_IMG; break;
      case(SHORT_IMG):        data_type = TSHORT; img_type = SHORT_IMG; break;
      case(LONG_IMG): data_type = TINT; img_type = LONG_IMG; break;
      case(LONGLONG_IMG):     data_type = TLONGLONG; img_type = LONGLONG_IMG; break;
      case(FLOAT_IMG):        data_type = TFLOAT; img_type = FLOAT_IMG; break;
      case(DOUBLE_IMG):       data_type = TDOUBLE; img_type = DOUBLE_IMG; break;
   }

   imgSize = (abs(img_type) / 8) * npixels;

   // Remove the file, if exist
   remove(fits_filename);
   // printf("fits_filename = %s compspec = %s\n",fits_filename, compspec);

   if (compspec && *compspec)
   {
      snprintf(filename, sizeof(filename), "%s[%s]", fits_filename, compspec);
   }
   else
   {
      snprintf(filename, sizeof(filename), "%s", fits_filename);
   }

   status = 0; // first thing!

   fptr = (fitsfile *)fitsrw_getfptr(verbose, filename, 1, &status, &fileCreated);

   if (!fptr)
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }

#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 35)
    if (imgSize > HUGE_HDU_THRESHOLD && fileCreated)
    {
        // We will be writing, which means we will call fits_create_file(). Support 64-bit HDUs.
        if (fits_set_huge_hdu(fptr, 1, &status))
        {
            error_code = CFITSIO_ERROR_FILE_IO;
            goto error_exit;
        }
    }
#endif

   if(fits_create_img(fptr, img_type, image_info->naxis, image_info->naxes, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   if (fits_is_compressed_image(fptr, &status))
   {
      /* Disallow unsupported types of tile-compressed files */
      if (data_type == TLONGLONG)
      {
          fprintf(stderr, "CFITSIO doesn't support compression of 64-bit data.\n");
          error_code = CFITSIO_ERROR_CANT_COMPRESS;
          goto error_exit;
      }
      else if (data_type == TFLOAT || data_type == TDOUBLE)
      {
          fprintf(stderr, "CFITSIO compression of floating-point data is lossy, bailing.\n");
          error_code = CFITSIO_ERROR_CANT_COMPRESS;
          goto error_exit;
      }
   }
   else if (status)
   {
       error_code = CFITSIO_ERROR_LIBRARY;
       goto error_exit;
   }

    // keylist is optional; it is currently used for export and for drms_segment_writewithkeys()
    if (keylist)
    {
        error_code = Cf_write_header_keys(fptr, NULL, keylist, NULL);
        if (error_code != CFITSIO_SUCCESS)
        {
            goto error_exit;
        }

        // calculate checksum in a new temp in-memory file header_for_checksum
        error_code = cfitsio_generate_checksum(&header_for_checksum, keylist, &checksum);
        if (error_code != CFITSIO_SUCCESS)
        {
            goto error_exit;
        }

        error_code = Cf_write_headsum(fptr, checksum);
        if (error_code != CFITSIO_SUCCESS)
        {
            goto error_exit;
        }

        error_code = Cf_write_longwarn(fptr);
        if (error_code != CFITSIO_SUCCESS)
        {
            goto error_exit;
        }
    }

    if (checksum)
    {
        free(checksum);
        checksum = NULL;
    }

    if (header_for_checksum)
    {
        cfitsio_close_file(&header_for_checksum);
    }

   if (image_info->bitfield & kInfoPresent_BLANK)
   {
      oblank = (long long)image_info->blank;

      if (image_info->bitpix == BYTE_IMG)
      {
	 /* Work around the fact that the FITS standard does not support
	  * signed byte data.  You have to make missing a valid unsigned
	  * char - I'm choosing 0.  In DRMS, you have to convert 0 to
	  * signed char -128 */
	 oblank = 0;
      }

      fits_update_key(fptr, TLONGLONG, "BLANK", &oblank, "", &status);
   }
   if (image_info->bitfield & kInfoPresent_BZERO)
   {
      double obzero = image_info->bzero;
      if (image_info->bitpix == BYTE_IMG)
      {
	 obzero = (double) (obzero -(128.0 * image_info->bscale));
      }

      fits_update_key(fptr, TDOUBLE, "BZERO", &obzero, "", &status);
   }
   if (image_info->bitfield & kInfoPresent_BSCALE)
   {
      double obscale = image_info->bscale;
      fits_update_key(fptr, TDOUBLE, "BSCALE", &obscale, "", &status);
   }

   if (image_info->bitpix == BYTE_IMG)
   {
      /* If you don't do this, then setting the 'BZERO' keyword to something
       * other than -128 causes a problem.  cfitsio adds the -BZERO value to
       * the signed char data, and if you choose anything other than -128,
       * you can get an overflow (outside of the range of unsigned char)
       * For example, if you choose -120 as BZERO, all signed char values above
       *
       * then for a */
      fits_set_bscale(fptr, 1.0, -128, &status);
   }
   else
   {
      fits_set_bscale(fptr, 1.0, 0.0, &status); /* Don't scale data when writing file -
						 * if you use values other than 1.0/0.0
						 * then cfitsio will use those values to
						 * scale the data.  If you omit this
						 * function call, then cfitsio will use the
						 * BZERO/BSCALE keywords that you set a few
						 * lines of code above to scale the data. */
   }

   fits_set_imgnull(fptr, 0, &status); /* Don't convert any value to missing -
                                        * just write all values as is.  If you
                                        * put something other than 0 here,
                                        * cfitsio converts whatever you specify
                                        * to whatever you've defined as BLANK with
                                        * the BLANK keyword. If you omit this function
                                        * call, then cfitsio will use the BLANK keyword
                                        * that you set a few lines of code above to
                                        * modify the data. */


   if (image)
   {
      if(fits_write_img(fptr, data_type, 1, npixels, image, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
         goto error_exit;
      }
   }
   else
   {
        /* TAS-file branch */

      /* If you avoid writing an image, then fitsio fills in all unspecified values
       * with zeros, which is not what we want. Instead, we need to allocate
       * blocks of missing values and write them out one at a time (we don't
       * want to allocate ALL values - this could be a huge amount of memory).
       * We need to write out npixels missing values. */

      /* If no image, let fitsio create an "empty" image - ensure that the
       * correct blank value is used (use the one passed into this function). */
      if (image_info->bitpix == FLOAT_IMG)
      {
         /* need to manually use NaN */
         float *pix = 0;
         int ipix;

         misspix = malloc(kMISSPIXBLOCK * sizeof(float));
         pix = misspix;

         for (ipix = 0; ipix < kMISSPIXBLOCK; ipix++)
         {
            *pix++ = F_NAN;
         }
      }
      else if (image_info->bitpix == DOUBLE_IMG)
      {
         /* need to manually use NaN */
         double *pix = 0;
         int ipix;

         misspix = malloc(kMISSPIXBLOCK * sizeof(double));
         pix = misspix;

         for (ipix = 0; ipix < kMISSPIXBLOCK; ipix++)
         {
            *pix++ = D_NAN;
         }
      }
      else if (image_info->bitpix == BYTE_IMG)
      {
         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(char));
         memset(misspix, (char)oblank, kMISSPIXBLOCK);
      }
      else if (image_info->bitpix == SHORT_IMG)
      {
         short *p = NULL;

         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(short));
         p = (short *)misspix + (kMISSPIXBLOCK - 1);
         while (p >= misspix)
         {
            *p = (short)oblank;
            p--;
         }
      }
      else if (image_info->bitpix == LONG_IMG)
      {
         int *p = NULL;

         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(int));
         p = (int *)misspix + (kMISSPIXBLOCK - 1);
         while (p >= misspix)
         {
            *p = (int)oblank;
            p--;
         }
      }
      else if (image_info->bitpix == LONGLONG_IMG)
      {
         long long *p = NULL;

         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(long long));
         p = (long long *)misspix + (kMISSPIXBLOCK - 1);
         while (p >= misspix)
         {
            *p = (long long)oblank;
            p--;
         }
      }

      pixleft = npixels;
      pixtowrt = 0;
      pixptr = 0;

      while(pixleft > 0)
      {
         pixtowrt = pixleft > kMISSPIXBLOCK ? kMISSPIXBLOCK : pixleft;
         if(fits_write_img(fptr,
                           data_type,
                           1 + pixptr, /* 1-based first pixel location in image on file */
                           pixtowrt,
                           misspix,
                           &status))
         {
            error_code = CFITSIO_ERROR_LIBRARY;
            goto error_exit;
         }

         pixleft -= pixtowrt;
         pixptr += pixtowrt;
      }

      if (misspix)
      {
         free(misspix);
      }
   }

   if (fptr)
   {
      /* set the fpinfo struct with the values from image_info (but do not overwrite the fhash field) */
      CFITSIO_IMAGE_INFO fpinfo;
      CFITSIO_IMAGE_INFO fpinfonew;

      if (fitsrw_getfpinfo_ext((TASRW_FilePtr_t)fptr, (TASRW_FilePtrInfo_t)&fpinfo))
      {
         fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
         error_code = CFITSIO_ERROR_FILE_IO;
      }
      else
      {
         fpinfonew = *image_info;
         snprintf(fpinfonew.fhash, sizeof(fpinfonew.fhash), "%s", fpinfo.fhash);

         if (fitsrw_setfpinfo_ext((TASRW_FilePtr_t)fptr, (TASRW_FilePtrInfo_t)&fpinfonew))
         {
            fprintf(stderr, "Unable to update file pointer information.\n");
            error_code = CFITSIO_ERROR_FILE_IO;
         }
         else
         {
            if ((status = fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr)) != 0)
            {
               error_code = CFITSIO_ERROR_FILE_IO;
            }
         }
      }
   }

    if (status == 0)
    {
        return CFITSIO_SUCCESS;
    }

error_exit:
    if (checksum)
    {
        free(checksum);
        checksum = NULL;
    }

    if (header_for_checksum)
    {
        cfitsio_close_file(&header_for_checksum);
    }

    if (status)
    {
        fits_get_errstatus(status, cfitsiostat);
    }

    fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);

    if (fptr)
    {
        /* There was some other error, so dont' worry about any errors having to do with closing the file. */
        fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr);
    }

    return error_code;
}

/****************************************************************************/
// Will find the library call for writting card to replace this, later
//
//Format: name(8), '= '(2), value (20), ' / ', comment (the rest and pending spaces)
//Format: name(8), '= '(2), 'string value need single quotes' (upto FLEN_VALUE=71), ' / ', comment
//Format: END is the last one by itself

/*
  Examples:

  01234567890123456789012345678901234567890123456789012345678901234567890123456789
  SIMPLE  =                    T / Fits standard
  BITPIX  =                  -32 / Bits per pixel
  NAXIS   =                    1 / Number of axes
  NAXIS1  =                 1440 / Axis length
  EXTEND  =                    F / File may contain extensions
  DATAMIN =             77.40008 / Minimum data value
  DATAMAX =             88.58415 / Maximum data value
  ORIGIN  = 'NOAO-IRAF FITS Image Kernel December 2001' / FITS file originator
  DATE    = '2003-09-24T01:25:54' / Date FITS file was generated
  IRAF-TLM= '17:25:54 (23/09/2003)' / Time of last modification
  END
*/

/****************************************************************************/

/* write HISTORY and COMMENT keywords
 */
int cfitsio_write_special_to_cards(const char *specialKeyName, const char *specialKeyValue, LinkedList_t **cards)
{
    char card[FLEN_CARD];
    int len, ii;
    char *upper = NULL;
    int err = CFITSIO_SUCCESS;


    if (cards)
    {
        upper = strdup(specialKeyName);
        if (upper)
        {
            strtoupper(upper);

            *cards = list_llcreate(FLEN_CARD, NULL);
            if (*cards)
            {
                len = strlen(specialKeyValue);
                ii = 0;

                for (; len > 0; len -= 72)
                {
                    snprintf(card, sizeof(card), "%-8.8s", specialKeyName);
                    strncat(card, &specialKeyValue[ii], 72);
                    list_llinserttail(*cards, card);
                    ii += 72;
                }
            }
        }
        else
        {
            err = CFITSIO_ERROR_OUT_OF_MEMORY;
        }
    }

    return err;
}

/* a card always constains a string representation of the keyword value */
int cfitsio_key_value_to_string_internal(CFITSIO_KEYWORD *key, CFITSIO_FILE *cards, char **string_value)
{
    char key_value[FLEN_CARD];
    char key_name[FLEN_KEYWORD];
    char *str_out = NULL;
    size_t sz_str_out = FLEN_CARD;
    CFITSIO_HEADER *file = NULL;
    int num_keys = 0;
    int key_num = 0;
    int cfiostat = 0;
    int err = CFITSIO_SUCCESS;


    /* make temporary in-memory-only FITS file so we can write the CFITSIO_KEYWORD to the header of this file,
     * and then read the keyword back with fits_read_card(); the net effect is to convert a CFITSIO_KEYWORD
     * to a FITSIO card, which is then parsed so that the value and comment are extracted */

    /* to deal with HISTORY and COMMENT keywords (which can span multiple cards), use fits_write_history()/
     * fits_write_comment() to the temporary fits file, and then use multiple fits_read_record() calls
     * to read all cards */

    /* to deal with long-string keywords (which can span multiple CONTINUE cards), use fits_write_key_longstr()
     * to write the keyword to the temporary fits file, and then use multiple fits_read_record() calls
     * to read all cards */
    if (key)
    {
        XASSERT(cards || string_value);

        if (cards)
        {
            file = cards;
        }
        else
        {
            err = cfitsio_create_file(&file, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL, NULL);
        }

        if (!err)
        {
            err = Cf_write_header_key(file->fptr, key);

            if (!err)
            {
                if (!cards)
                {
                    /* creating an output string */
                    if (strncasecmp(key->key_name, CFITSIO_KEYWORD_HISTORY, strlen(CFITSIO_KEYWORD_HISTORY)) == 0 || strncasecmp(key->key_name, CFITSIO_KEYWORD_COMMENT, strlen(CFITSIO_KEYWORD_COMMENT)) == 0)
                    {
                        fits_get_hdrspace(file->fptr, &num_keys, NULL, &cfiostat);

                        if (!cfiostat)
                        {
                            str_out = calloc(1, sz_str_out);

                            for (key_num = 1; key_num <= num_keys; key_num++)
                            {
                                fits_read_keyn(file->fptr, key_num, key_name, key_value, NULL, &cfiostat);

                                if (!cfiostat)
                                {
                                    if (strcasecmp(key_name, key->key_name) == 0)
                                    {
                                        str_out = base_strcatalloc(str_out, key_value, &sz_str_out);
                                    }
                                }
                                else
                                {
                                    err = CFITSIO_ERROR_LIBRARY;
                                }
                            }

                            if (!err)
                            {
                                *string_value = str_out; // yoink!
                            }
                        }
                        else
                        {
                            err = CFITSIO_ERROR_LIBRARY;
                        }
                    }
                    else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
                    {
                        fits_read_key_longstr(file->fptr, key->key_name, &str_out, NULL, &cfiostat);

                        if (cfiostat)
                        {
                            err = CFITSIO_ERROR_LIBRARY;
                        }

                        if (!err)
                        {
                            *string_value = str_out; // yoink!
                        }
                    }
                    else
                    {
                        /* converts value to string */
                        fits_read_keyword(file->fptr, key->key_name, key_value, NULL, &cfiostat);

                        if (cfiostat)
                        {
                            err = CFITSIO_ERROR_LIBRARY;
                        }

                        if (!err)
                        {
                            *string_value = strdup(key_value); // yoink!
                        }
                    }
                }
            }
        }

        if (!cards && file)
        {
            /* will drop data on floor */
            cfitsio_close_file(&file);
        }
    }

    return err;
}

int cfitsio_key_value_to_cards(CFITSIO_KEYWORD *key, CFITSIO_HEADER **cards)
{
    int err = CFITSIO_SUCCESS;


    if (cards)
    {
        if (!*cards)
        {
            err = cfitsio_create_file(cards, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL, NULL);
        }
    }

    if (!err)
    {
        err = cfitsio_key_value_to_string_internal(key, *cards, NULL);
    }

    return err;
}

int cfitsio_key_value_to_string(CFITSIO_KEYWORD *key, char **string_value)
{
    return cfitsio_key_value_to_string_internal(key, NULL, string_value);
}

/****************************************************************************/
/* `keylist` is used by drms_fitsrw_read(), which is used by ingest_from_fits; this is not an internal SUMS file */
int fitsrw_read(int verbose,
                const char *filename,
                CFITSIO_IMAGE_INFO** image_info,
                void** image,
                CFITSIO_KEYWORD** keylist)
{
   fitsfile *fptr = NULL;
   long long npixels;
   int bytepix;
   int data_type;
   void *pixels = NULL;
   int status;
   int error_code = 0;
   char cfitsiostat[FLEN_STATUS];
   int idim;
   char *fnamedup = strdup(filename);
   int datachk;
   int hduchk;
   int fileCreated = 0;

   if (!image_info || *image_info)
   {
      fprintf(stderr, "Invalid image_info argument.\n");
      error_code = CFITSIO_ERROR_ARGS;
   }

   if (!error_code)
   {
      *image_info = (CFITSIO_IMAGE_INFO *)malloc(sizeof(CFITSIO_IMAGE_INFO));

      if(*image_info == NULL)
      {
         error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      }
   }

   if (!error_code)
   {
      memset((void*)(*image_info), 0, sizeof(CFITSIO_IMAGE_INFO));

      /* stupid cfitsio will fail if status is not 0 when the following call
       * is made */
      status = 0;

      fptr = (fitsfile *)fitsrw_getfptr(verbose, fnamedup, 0, &status, &fileCreated);
      XASSERT(!fileCreated);

      if (!fptr)
      {
         error_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      }

      if (!error_code)
      {
          /* `keylist` is used by drms_fitsrw_read(), which is used by ingest_from_fits */
         error_code = cfitsio_read_keylist_and_image_info(fptr, keylist, image_info);
      }

      if (!error_code)
      {
         switch((*image_info)->bitpix)
         {
            case(BYTE_IMG):    data_type = TBYTE; break; /* When reading, data are
                                                            an unsigned char array */
            case(SHORT_IMG):   data_type = TSHORT; break;
            case(LONG_IMG):    data_type = TINT; break;
            case(LONGLONG_IMG):data_type = TLONGLONG; break;
            case(FLOAT_IMG):   data_type = TFLOAT; break;
            case(DOUBLE_IMG):  data_type = TDOUBLE; break;
         }

         bytepix = abs((*image_info)->bitpix) / 8;

         npixels = 1;
         for(idim = 0; idim < (*image_info)->naxis; idim++)
         {
            npixels *= (*image_info)->naxes[idim];
         }

         pixels = calloc(npixels, bytepix);
         if(!pixels)
         {
            error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
         }
      }
   }

   if (!error_code)
   {
      /* Read raw data */
      fits_set_bscale(fptr, 1.0, 0.0, &status);

      if (!status)
      {
         fits_set_imgnull(fptr, 0, &status);
      }

      if (status)
      {
         error_code = CFITSIO_ERROR_LIBRARY;
      }
   }

   if (!error_code)
   {
      if(fits_read_img(fptr, data_type, 1, npixels, NULL, pixels, NULL, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
      }

      /* Check checksum, if it exists */
      if (fits_verify_chksum(fptr, &datachk, &hduchk, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
      }
      else
      {
         if (datachk == -1 || hduchk == -1)
         {
            /* Both checksums were present, and at least one of them failed. */
            fprintf(stderr, "Failed to verify data and/or HDU checksum (file corrupted).\n");
            error_code = CFITSIO_ERROR_FILE_IO;
         }
      }
   }

   if(fptr)
   {
       status = fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr);
       if (status)
       {
           error_code = CFITSIO_ERROR_FILE_IO;
       }
   }

   if (status)
   {
      fits_get_errstatus(status, cfitsiostat);
      fprintf(stderr, "In fitsrw_read(), cfitsio error '%s'.\n", cfitsiostat);
      if(pixels)
      {
         free(pixels);
      }
   }
   else
   {
      *image = pixels;
   }

   if (fnamedup)
   {
      free(fnamedup);
   }

   return error_code;
}

/*
 * filein - the name of the fits file to be written ("-" for stdout; passed into lower-level functions to indicate that a FITS file is being streamed to stdout)
 * info - image essential metadata (e.g., bitpix, naxes, ...); NULL if fitsData provided
 * image - the image data to be written to the fits file; NULL if fitsData provided
 * fitsData - an alternate way to supply image data; a FITS file (fitsfile *) that contains only image data
 * cparms - a FITS file compression string (NULL for stdout streaming)
 * keylist - FITS file metadata
 * header - an alternate way to supply metadata; a FITS file (fitsfile *) that contains only a header
 * callback - a function called in the tar-on-fly workflow (fitsfile * for stdout - writing to this FITS file will cause file data to be streamed to stdout)
 */
int fitsrw_write3(int verbose, const char *filein, CFITSIO_IMAGE_INFO *info, void *image, CFITSIO_DATA *fitsData, const char *cparms, CFITSIO_KEYWORD *keylist, CFITSIO_HEADER *fitsHeader, export_callback_func_t callback)
{
    int err = CFITSIO_SUCCESS;
    int idim;
    long long npixels;
    int datatype;
    int imgtype;
    char filename[PATH_MAX];
    int streaming = 0;
    int copyFits = 0;
    fitsfile *fptr = NULL;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail. */
    char cfiostatErrmsg[FLEN_STATUS];
    int fileCreated = 0;
    long long imgSize =0;

    if (filein && ((info && image) || fitsData))
    {
        streaming = (strcmp(filein, "-") == 0);
        copyFits = (!info || !image);

        if (!copyFits)
        {
            for (idim = 0, npixels = 1; idim < info->naxis; idim++)
            {
                npixels *= info->naxes[idim];
            }

            switch (info->bitpix)
            {
                case(BYTE_IMG):
                {
                    datatype = TSBYTE;
                    imgtype = SBYTE_IMG;
                }
                break;

                case(SHORT_IMG):
                {
                    datatype = TSHORT;
                    imgtype = SHORT_IMG;
                }
                break;

                case(LONG_IMG):
                {
                    datatype = TINT;
                    imgtype = LONG_IMG;
                }
                break;

                case(LONGLONG_IMG):
                {
                    datatype = TLONGLONG;
                    imgtype = LONGLONG_IMG;
                }
                break;

                case(FLOAT_IMG):
                {
                    datatype = TFLOAT;
                    imgtype = FLOAT_IMG;
                }
                break;

                case(DOUBLE_IMG):
                {
                    datatype = TDOUBLE;
                    imgtype = DOUBLE_IMG;
                }
                break;

                default:
                {
                    fprintf(stderr, "fitsrw_write(): Unsupported image data type.\n");
                    err = CFITSIO_ERROR_ARGS;
                }
            }


            if (!err)
            {
                imgSize = (abs(imgtype) / 8) * npixels;

                /* In the future, perhaps we override this method of specifying compression
                * with the CFITSIO API call that specifies it. */
                if (!streaming)
                {
                    /* not stdout */
                    if (cparms && *cparms)
                    {
                        snprintf(filename, sizeof(filename), "%s[%s]", filein, cparms);
                    }
                    else
                    {
                        snprintf(filename, sizeof(filename), "%s", filein);
                    }

                    remove(filein);
                }
            }
        }
        else
        {
            /* copying existing fits file */
            snprintf(filename, sizeof(filename), "%s", filein);
        }

        if (!err)
        {
            //ISS fly-tar START
            if (callback != NULL)
            {
                if (!streaming)
                {
                    /* not stdout */
                    int retVal = 0;

                    (*callback)("create", &fptr, filein, cparms, &cfiostat, &retVal);
                    if (retVal)
                    {
                        err = CFITSIO_ERROR_FILE_IO;
                    }
                }
                else
                {
                    /* we are writing the FITS file to stdout; the FITS file has already been created (in memory);
                     * callback has the fitsfile; this file's CFITSIO_IMAGE_INFO has NOT been stored in the global hash
                     * (in memory files for stdout-streaming are not subject to caching) */
                    fptr = (fitsfile *)callback;
                }
            }
            else
            {
                /* calls fits_create_file(), store the CFITSIO_IMAGE_INFO in the global hash */
                fptr = (fitsfile *)fitsrw_getfptr(verbose, filename, 1, &err, &fileCreated);

                if (!fptr)
                {
                    err = CFITSIO_ERROR_FILE_IO;
                }
                else
                {
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 35)
                    if (imgSize > HUGE_HDU_THRESHOLD && fileCreated)
                    {
                        // We will be writing, which means we will call fits_create_file(). Support 64-bit HDUs.
                        if (fits_set_huge_hdu(fptr, 1, &cfiostat))
                        {
                            err = CFITSIO_ERROR_FILE_IO;
                        }
                    }
#endif
                }
            }
         //ISS fly-tar END
        }

        if (!err)
        {
            if (copyFits)
            {
                /* copy the image from fitsData to fptr (a new, empty FITS file) */
                if (fits_copy_file(fitsData->fptr, fptr, 0, 1, 0, &cfiostat))
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
            else
            {
                if (fits_create_img(fptr, imgtype, info->naxis, info->naxes, &cfiostat))
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
        }

        if (!err)
        {
            if (!copyFits)
            {
                if (fits_is_compressed_image(fptr, &cfiostat))
                {
                    if (datatype == TLONGLONG)
                    {
                        fprintf(stderr, "CFITSIO doesn't support compression of 64-bit data.\n");
                        err = CFITSIO_ERROR_ARGS;
                    }
                    else if (datatype == TFLOAT || datatype == TDOUBLE)
                    {
                        fprintf(stderr, "WARNING: CFITSIO compression of floating-point data is lossy.\n");
                    }
                }
                else if (cfiostat)
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
        }

        if (!err)
        {
            /* Write out FITS keywords that were derived from DRMS keywords. */
            err = Cf_write_header_keys(fptr, NULL, keylist, (keylist != NULL) ? NULL : fitsHeader->fptr);
        }

        if (!err)
        {
            if (!copyFits)
            {
                /* Write out special FITS keywords */
                if (info->bitfield & kInfoPresent_BLANK)
                {
                    long long oblank = (long long)info->blank;
                    fits_update_key(fptr, TLONGLONG, "BLANK", &oblank, "", &cfiostat);
                }

                if (!cfiostat && (info->bitfield & kInfoPresent_BZERO))
                {
                    double obzero = info->bzero;
                    fits_update_key(fptr, TDOUBLE, "BZERO", &obzero, "", &cfiostat);
                }

                if (!cfiostat && (info->bitfield & kInfoPresent_BSCALE))
                {
                    double obscale = info->bscale;
                    fits_update_key(fptr, TDOUBLE, "BSCALE", &obscale, "", &cfiostat);
                }

                /* Must ensure that CFITSIO doesn't modify data - BZERO, BSCALE, and BLANK
                * are written out above and without the following calls, CFITSIO may see
                * those values and attempt to modify the data so that those values are
                * relevant. But the data have already been converted so that the
                * special values are already relevant. */
                if (!cfiostat)
                {
                    fits_set_bscale(fptr, 1.0, 0.0, &cfiostat);
                }

                if (!cfiostat)
                {
                    fits_set_imgnull(fptr, 0, &cfiostat);
                }

                if (cfiostat)
                {
                    fprintf(stderr, "Trouble setting BZERO, BSCALE, or BLANK keywords.\n");
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
        }

        if (!err)
        {
            if (!copyFits)
            {
                if(fits_write_img(fptr, datatype, 1, npixels, image, &cfiostat))
                {
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
        }
   }
   else
   {
      err = CFITSIO_ERROR_ARGS;
   }

        if (err && cfiostat)
        {
            fits_get_errstatus(cfiostat, cfiostatErrmsg);
            fprintf(stderr, "[ fitsrw_write3() ] CFITSIO error '%s'\n", cfiostatErrmsg);
        }

    if (fptr)
    {
        /* set the fpinfo struct with the values from image_info (but do not overwrite the fhash field) */

        //ISS fly-tar
        if (callback != NULL )
        {
            if (!streaming)
            {
                //ISS fly-tar
                (*callback)("stdout", fptr, image, &cfiostat);
                if (cfiostat)
                {
                    fprintf(stderr, "Trouble executing callback [%s]\n", filename);
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
        }
        else
        {
            /* we are not streaming; this means that fptr has a cached CFITSIO_IMAGE_INFO globally */
            CFITSIO_IMAGE_INFO fpinfo;
            CFITSIO_IMAGE_INFO fpinfonew;


            /* this will fetch the hash key for the cached CFITSIO_IMAGE_INFO */
            if (fitsrw_getfpinfo_ext((TASRW_FilePtr_t)fptr, (TASRW_FilePtrInfo_t)&fpinfo))
            {
                fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
                err = CFITSIO_ERROR_FILE_IO;
            }
            else
            {
                /* fpinfo contains the hash key into the global CFITSIO_IMAGE_INFO container */
                if (copyFits)
                {
                    /* info is NULL - we need to create it from the image info in fptr itself; this fptr's
                     * CFITSIO_IMAGE_INFO has already been cached in the global hash () */
                    CfSetImageInfo(&fpinfonew, fptr);
                }
                else
                {
                    /* I guess this is faster since the CFITSIO_IMAGE_INFO already exists in the !copyFits case */
                    fpinfonew = *info;
                }

                snprintf(fpinfonew.fhash, sizeof(fpinfonew.fhash), "%s", fpinfo.fhash);

                if (fitsrw_setfpinfo_ext((TASRW_FilePtr_t)fptr, (TASRW_FilePtrInfo_t)&fpinfonew))
                {
                    fprintf(stderr, "Unable to update file pointer information.\n");
                    err = CFITSIO_ERROR_FILE_IO;
                }
                else
                {
                    if (fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr))
                    {
                        err = CFITSIO_ERROR_FILE_IO;
                    }
                }
            }
        }  //ISS fly-tar
    }

    return err;

}

int fitsrw_write(int verbose, const char *filein, CFITSIO_IMAGE_INFO *info, void *image, const char *cparms, CFITSIO_KEYWORD *keylist)
{
   return fitsrw_write3(verbose, filein, info, image, NULL, cparms, keylist, NULL, (export_callback_func_t) NULL);
}

int fitsrw_write2(int verbose, const char *filein, CFITSIO_IMAGE_INFO *info, void *image, const char *cparms, CFITSIO_KEYWORD *keylist, export_callback_func_t callback) //ISS fly-tar - fitsfile * for stdout
{
    return fitsrw_write3(verbose, filein, info, image, NULL, cparms, keylist, NULL, callback);
}
