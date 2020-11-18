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
    cfitsio_file_type_t type;
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
        if (!fitsrw_getfpinfo_ext(*fptr, &fpinfo))
        {
            /* writes FITS keyword data and image checksums */
            fitsrw_closefptr(0, *fptr);
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
        if (!fitsrw_getfpinfo_ext(*fptr, &fpinfo))
        {
            /* writes FITS keyword data and image checksums */
            fitsrw_closefptr(0, *fptr);
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

static int Cf_write_header_key(fitsfile *fptr, CFITSIO_KEYWORD *key)
{
    int cfiostat = 0;
    char comment[FLEN_COMMENT];
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    /* the keyword comment has this format:
     *   [<unit>] commentary {<corresponding DRMS keyword name>}
    */
    if (key->is_missing)
    {
        if (*key->key_comment != '\0')
        {
            snprintf(comment, sizeof(comment), "(MISSING) %s", key->key_comment);
        }
        else
        {
            snprintf(comment, sizeof(comment), "(MISSING)");
        }

        fits_write_key_null(fptr, key->key_name, comment, &cfiostat);
    }
    else
    {
        if (key->key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
        {
            /* any string could be a long string (> 68 chars) so use fits_read_key_longstr() */
            if (strcmp(key->key_name, CFITSIO_KEYWORD_HISTORY) == 0)
            {
                fits_write_history(fptr, key->key_value.vs, &cfiostat);
            }
            else if (strcmp(key->key_name, CFITSIO_KEYWORD_COMMENT) == 0)
            {
                fits_write_comment(fptr, key->key_value.vs, &cfiostat);
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
            /* prints 15 decimal places, if needed */
            // fits_write_key(fptr, TDOUBLE, key->key_name, &key->key_value.vf, key->key_comment ? key->key_comment : NULL, &cfiostat);

            /* a NaN value implies a missing value; write a keyword with no value (null) */

            /* prints 17 decimal places, if needed */
            fits_write_key_dbl(fptr, key->key_name, key->key_value.vf, -17, key->key_comment ? key->key_comment : NULL, &cfiostat);
        }
        else
        {
            err = CFITSIO_ERROR_INVALID_DATA_TYPE;
        }
    }

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_write_header_key() ] error writing key %s: %s\n",  key->key_name, cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (!err)
    {
        if (key->key_unit)
        {
            fits_write_key_unit(fptr, key->key_name, key->key_unit, &cfiostat);

            if (cfiostat)
            {
                fits_get_errstatus(cfiostat, cfiostat_msg);
                fprintf(stderr, "[ Cf_write_header_key() ] error writing key %s: %s\n",  key->key_name, cfiostat_msg);
            }
        }
    }

    return err;
}

/* RE-DO all of this to use FITSIO keyword-writing functions
 *
 * `fptr_out` is the FITS file (on disk or in memory) keys are being written to (can be NULL)
 * `header` is the buffer that the FITSIO header is being written to (if fptr is NULL)
 * `key_list` is the linked-list of CFITSIO_KEYWORD structs containing the keyword names/values
 * `fits_header` is alternate way of providing keywords to write to fptr */
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

                        if (key_class == TYP_STRUC_KEY  || key_class == TYP_CMPRS_KEY || key_class == TYP_CONT_KEY)
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
    int skip_column = -1;
    int fits_tfields = -1; /* column */
    int row = -1; /* row */
    int fits_data_type = -1;
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

            skip_column = 0;
            fits_tfields = 0;
            while ((err == CFITSIO_SUCCESS) && key)
            {
                /* iterate over columns */
                switch (key->key_type)
                {
                    case CFITSIO_KEYWORD_DATATYPE_STRING:
                        fits_data_type = TSTRING;
                        data = &key->key_value.vs;
                        break;
                    case CFITSIO_KEYWORD_DATATYPE_LOGICAL:
                        fits_data_type = TLOGICAL;
                        data = &key->key_value.vl;
                        break;
                    case CFITSIO_KEYWORD_DATATYPE_INTEGER:
                        fits_data_type = TLONG;
                        data = &key->key_value.vi;
                        break;
                    case CFITSIO_KEYWORD_DATATYPE_FLOAT:
                        fits_data_type = TDOUBLE;
                        data = &key->key_value.vf;
                        break;
                    default:
                        fprintf(stderr, "invalid CFITSIO keyword data type %d; skipping keyword %s\n", key->key_type, key->key_name);
                        skip_column = 1;
                }

                if (!skip_column)
                {
                    fits_write_col(fptr_out, fits_data_type, fits_tfields + 1, row + 1, 1, 1, data, &cfiostat);

                    if (cfiostat)
                    {
                        fits_get_errstatus(cfiostat, cfiostat_err_msg);
                        fprintf(stderr, "[ Cf_write_keys_to_bintable() ] CFITSIO error '%s'\n", cfiostat_err_msg);
                        err = CFITSIO_ERROR_LIBRARY;
                        break;
                    }

                    ++fits_tfields;

                    if (fits_tfields > CFITSIO_MAX_BINTABLE_WIDTH)
                    {
                        fprintf(stderr, "[ Cf_write_keys_to_bintable() ] too many keywords - the mamimum allowed is %d\n", CFITSIO_MAX_BINTABLE_WIDTH);
                        err = CFITSIO_ERROR_ARGS;
                        break;
                    }
                }

                key = key->next;
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
    int err = 0;


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

/* value == NULL -> missing key*/
int cfitsio_create_header_key(const char *name, cfitsio_keyword_datatype_t type, const void *value, const char *format, const char *comment, const char *unit, CFITSIO_KEYWORD **keyOut)
{
    CFITSIO_KEYWORD *rv = NULL;
    int err = CFITSIO_SUCCESS;

    if (name && keyOut)
    {
        rv = (CFITSIO_KEYWORD *)calloc(1, sizeof(CFITSIO_KEYWORD));

        if (!rv)
        {
            fprintf(stderr, "cfitsio_create_header_key(): out of memory\n");
            err = CFITSIO_ERROR_OUT_OF_MEMORY;
        }
        else
        {
            snprintf(rv->key_name, FLEN_KEYWORD, "%s", name);
            rv->key_type = type;

            if (value)
            {
                switch (type)
                {
                    case( 'X'):
                    case (CFITSIO_KEYWORD_DATATYPE_STRING):
                        /* 68 is the max chars in FITS string keyword, but the HISTORY and COMMENT keywords
                         * can contain values with more than this number of characters, in which case
                         * the fits API key-writing function will split the string across multiple
                         * instances of these special keywords. */
                        rv->key_value.vs = strdup((char *)value);
                        break;
                    case (CFITSIO_KEYWORD_DATATYPE_LOGICAL):
                        rv->key_value.vl = *((int *)value);
                        break;
                    case (CFITSIO_KEYWORD_DATATYPE_INTEGER):
                        rv->key_value.vi = *((long long *)value);
                        break;
                    case (CFITSIO_KEYWORD_DATATYPE_FLOAT):
                        rv->key_value.vf = *((double *)value);
                        break;
                    default:
                        fprintf(stderr, "invalid cfitsio keyword type '%c'\n", (char)type);
                        err = CFITSIO_ERROR_ARGS;
                        break;
                }
            }
            else
            {
                /* missing value */
                rv->is_missing = 1;
            }

            if (format)
            {
                snprintf(rv->key_format, sizeof(rv->key_format), "%s", format);
            }

            if (comment)
            {
                snprintf(rv->key_comment, sizeof(rv->key_comment), "%s", comment);
            }

            if (unit)
            {
                snprintf(rv->key_unit, sizeof(rv->key_unit), "%s", unit);
            }
        }
    }
    else
    {
        err = CFITSIO_ERROR_ARGS;
    }

    if (keyOut)
    {
        *keyOut = rv; /* swipe! */
        rv = NULL;
    }

    return err;
}

static int Cf_create_file_object(fitsfile *fptr, cfitsio_file_type_t file_type, void *data)
{
    CFITSIO_IMAGE_INFO *image_info = NULL;
    CFITSIO_BINTABLE_INFO *bintable_info = NULL;
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
                fits_create_img(fptr, image_info->bitpix, image_info->naxis, image_info->naxes, &cfiostat);
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

                fits_create_tbl(fptr, BINARY_TBL, list_llgetnitems(bintable_info->rows), bintable_info->tfields, (char **)bintable_info->ttypes, (char **)bintable_info->tforms, NULL, NULL, &cfiostat);
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
        fprintf(stderr, "[ Cf_create_file_object() ] unable to create FITS file\n");
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

int cfitsio_get_file_type(CFITSIO_FILE *file, cfitsio_file_type_t *type)
{
    int fits_file_type = -1;
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
            *type = CFITSIO_FILE_TYPE_EMPTY;
        }
        else
        {
            err = Cf_get_file_type(file->fptr, &fits_file_type);
            if (!err)
            {
                switch (fits_file_type)
                {
                    case IMAGE_HDU:
                        /* the type will also be IMAGE_HDU if fits_create_img() was never called */
                        *type = CFITSIO_FILE_TYPE_IMAGE;
                        break;
                    case ASCII_TBL:
                        *type = CFITSIO_FILE_TYPE_ASCIITABLE;
                        break;
                    case BINARY_TBL:
                        *type = CFITSIO_FILE_TYPE_BINTABLE;
                        break;
                    default:
                        break;
                }
            }
        }
    }

    return err;
}

int cfitsio_create_header(CFITSIO_FILE *file)
{
    int err = CFITSIO_SUCCESS;


    err = Cf_create_file_object(file->fptr, CFITSIO_FILE_TYPE_HEADER, NULL);
    if (err == CFITSIO_SUCCESS)
    {
        file->type = CFITSIO_FILE_TYPE_HEADER;
    }

    return err;
}

int cfitsio_create_image(CFITSIO_FILE *file, CFITSIO_IMAGE_INFO *image_info)
{
    int err = CFITSIO_SUCCESS;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int dim = 0;
    long long num_pixels = 0;


    err = Cf_create_file_object(file->fptr, CFITSIO_FILE_TYPE_IMAGE, image_info);
    if (err == CFITSIO_SUCCESS)
    {
        file->type = CFITSIO_FILE_TYPE_IMAGE;

#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 35)
        if (image_info)
        {
            for (dim = 0, num_pixels = 1; dim < image_info->naxis; dim++)
            {
                 num_pixels *= image_info->naxes[dim];
            }

            if ((abs(image_info->bitpix) / 8) * num_pixels > HUGE_HDU_THRESHOLD)
            {
                // support 64-bit HDUs
                if (fits_set_huge_hdu(file->fptr, 1, &cfiostat))
                {
                    err = CFITSIO_ERROR_FILE_IO;
                }
            }
        }
#endif
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


    err = Cf_create_file_object(file->fptr, CFITSIO_FILE_TYPE_BINTABLE, bintable_info);
    if (err == CFITSIO_SUCCESS)
    {
        file->type = CFITSIO_FILE_TYPE_BINTABLE;
    }

    return err;
}

/* create a new CFITSIO_FILE struct; if `initialize_fitsfile`, then allocate a fitsfile and assign to CFITSIO_FILE::fptr
 * by calling  fits_create_file(..., `file_name`, ...) */
 /* file is writeable since we are creating an empty file */
int cfitsio_create_file(CFITSIO_FILE **out_file, const char *file_name, cfitsio_file_type_t file_type, CFITSIO_IMAGE_INFO *image_info, CFITSIO_BINTABLE_INFO *bintable_info)
{
    int file_created = 0;
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    XASSERT(out_file);
    if (file_type != CFITSIO_FILE_TYPE_EMPTY)
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
        (*out_file)->type = CFITSIO_FILE_TYPE_EMPTY;

        if (file_type != CFITSIO_FILE_TYPE_EMPTY)
        {
            (*out_file)->type = CFITSIO_FILE_TYPE_UNITIALIZED;
            (*out_file)->in_memory = (strncmp(file_name, "-", 1) == 0);

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
                (*out_file)->fptr = fitsrw_getfptr(0, file_name, 1, &err, &file_created);

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
                if (file_created)
                {
                    if (file_type == CFITSIO_FILE_TYPE_HEADER)
                    {
                        /* BITPIX = 8, NAXIS = 0, NAXES = NULL - even though there is no image, BITPIX cannot be 0 */
                        err = cfitsio_create_header(*out_file);
                    }
                    else if (file_type == CFITSIO_FILE_TYPE_IMAGE)
                    {
                        if (image_info)
                        {
                            err = cfitsio_create_image(*out_file, image_info);
                        }
                    }
                    else if (file_type == CFITSIO_FILE_TYPE_BINTABLE)
                    {
                        if (bintable_info)
                        {
                            /* bintable_info->rows is a list of CFITSIO_KEYWORD * lists; bintable_info->tfields is the number of columns;
                             * bintable_info->ttypes is an array of column names; bintable_info->tforms is an array of tform data types */
                            err = cfitsio_create_bintable(*out_file, bintable_info);
                        }
                    }

                    if (cfiostat)
                    {
                        err = CFITSIO_ERROR_FILE_IO;
                    }
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

void cfitsio_set_fitsfile(CFITSIO_FILE *file, CFITSIO_FITSFILE fptr, int in_memory)
{
    if (file)
    {
        file->fptr = (fitsfile *)fptr;
    }

    file->in_memory = in_memory;
}

static int cfitsio_get_fits_compression_type(CFITSIO_COMPRESSION_TYPE cfitsio_type, int *fits_type)
{
    int err = CFITSIO_SUCCESS;
    int fits_comp_type = -1;


    if (fits_type)
    {
        switch (cfitsio_type)
        {
            case CFITSIO_COMPRESSION_NONE:
                *fits_type = NOCOMPRESS;
                break;
            case CFITSIO_COMPRESSION_RICE:
                *fits_type = RICE_1;
                break;
            case CFITSIO_COMPRESSION_GZIP1:
                *fits_type = GZIP_1;
                break;
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 27)
            case CFITSIO_COMPRESSION_GZIP2:
                *fits_type = GZIP_2;
                break;
#endif
            case CFITSIO_COMPRESSION_PLIO:
                *fits_type = PLIO_1;
                break;
            case CFITSIO_COMPRESSION_HCOMP:
                *fits_type = HCOMPRESS_1;
                break;
            default:
                err = CFITSIO_ERROR_ARGS;
                fprintf(stderr, "[ cfitsio_get_fits_compression_type() ] invalid cfitsio compression type %d\n", (int)cfitsio_type);
                break;
        }
    }

    return err;
}

int cfitsio_set_compression_type(CFITSIO_FILE *file, CFITSIO_COMPRESSION_TYPE cfitsio_type)
{
    int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail */
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;
    int fits_comp_type = -1;

    if (file)
    {
        err = cfitsio_get_fits_compression_type(cfitsio_type, &fits_comp_type);

        if (err == CFITSIO_SUCCESS)
        {
            fits_set_compression_type(file->fptr, fits_comp_type, &cfiostat);
            if (cfiostat)
            {
                fits_get_errstatus(cfiostat, cfiostat_msg);
                fprintf(stderr, "[ cfitsio_set_compression_type() ] unable to set compression type\n");
                fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);
                err = CFITSIO_ERROR_LIBRARY;
            }
        }
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

int cfitsio_copy_file(CFITSIO_FILE *source_in, CFITSIO_FILE *dest_in, int copy_header_only)
{
    int cfiostat = 0;
    int err = CFITSIO_SUCCESS;


    if (copy_header_only)
    {
        fits_copy_header(source_in->fptr, dest_in->fptr, &cfiostat); /* if dest_in->in_memory, writes header into memory */
    }
    else
    {
        /* ART - not sure about these 3 flags; at first I tried 0, 1, 0, and that seemed to work; but so does 1, 1, 1 */
        fits_copy_file(source_in->fptr, dest_in->fptr, 1, 1, 1, &cfiostat);
    }

    if (cfiostat)
    {
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

/* read key `key`->key_name from `file`, and initialize `key` with values read from `file` */
int cfitsio_read_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key)
{
    char comment[CFITSIO_MAX_STR] = {0};
    char unit[FLEN_COMMENT] = {0};
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;


    memset(&key->key_value, '\0', sizeof(CFITSIO_KEY_VALUE));

    if (key->key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
    {
        /* any string could be a long string (> 68 chars) so use fits_read_key_longstr() */
        fits_read_key_longstr(file->fptr, key->key_name, &key->key_value.vs, comment, &cfiostat);
    }
    else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_LOGICAL)
    {
        fits_read_key_log(file->fptr, key->key_name, &key->key_value.vl, comment, &cfiostat);
    }
    else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_INTEGER)
    {
        fits_read_key_lnglng(file->fptr, key->key_name, &key->key_value.vi, comment, &cfiostat);
    }
    else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_FLOAT)
    {
        fits_read_key_dbl(file->fptr, key->key_name, &key->key_value.vf, comment, &cfiostat);
    }
    else
    {
        err = CFITSIO_ERROR_INVALID_DATA_TYPE;
    }

    if (!err)
    {
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

    if (!err)
    {
        if (*comment != '\0')
        {
            snprintf(key->key_comment, sizeof(key->key_comment), "%s", comment);
            fits_read_key_unit(file->fptr, key->key_name, unit, &cfiostat);
        }

        if (cfiostat)
        {
            err = CFITSIO_ERROR_LIBRARY;
        }
        else
        {
            if (*unit != '\0')
            {
                snprintf(key->key_unit, sizeof(key->key_unit), "%s", unit);
            }
        }
    }

    if (cfiostat != 0)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ cfitsio_read_header_key() ] unable to read keyword '%s'\n", key->key_name);
        fprintf(stderr, "CFITSIO error: %s\n", cfiostat_msg);

        /* err will be set (if cfiostat != 0 ==> err)*/
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

/* use `key` to update a keyword in `file` */
int cfitsio_update_header_key(CFITSIO_FILE *file, CFITSIO_KEYWORD *key)
{
    char *comment = NULL;
    int cfiostat = 0;
    int err = CFITSIO_SUCCESS;


    if (*key->key_comment)
    {
        comment = key->key_comment;
    }
    else
    {
        /* '\0' means to not update comment */
        comment = NULL;
    }

    if (key->is_missing)
    {
        fits_update_key_null(file->fptr, key->key_name, comment, &cfiostat);
    }
    else
    {
        /* use fits_update_key_*() which will update an existing key, or append a new one */
        if (strcmp(key->key_name,"HISTORY") == 0)
        {
            if (key->key_value.vs)
            {
                fits_write_history(file->fptr, key->key_value.vs, &cfiostat);
            }
        }
        else if (!strcmp(key->key_name,"COMMENT"))
        {
            if (key->key_value.vs)
            {
                fits_write_comment(file->fptr, key->key_value.vs, &cfiostat);
            }
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_STRING)
        {
            /* any string could be a long string (> 68 chars) */
            fits_update_key_longstr(file->fptr, key->key_name, key->key_value.vs, comment, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_LOGICAL)
        {
            fits_update_key_log(file->fptr, key->key_name, key->key_value.vl, comment, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_INTEGER)
        {
            fits_update_key_lng(file->fptr, key->key_name, key->key_value.vi, comment, &cfiostat);
        }
        else if (key->key_type == CFITSIO_KEYWORD_DATATYPE_FLOAT)
        {
            /* prints 17 decimal places, if needed */
            fits_update_key_dbl(file->fptr, key->key_name, key->key_value.vf, -17, comment, &cfiostat);
        }
        else
        {
            err = CFITSIO_ERROR_INVALID_DATA_TYPE;
        }
    }

    if (cfiostat)
    {
        err = CFITSIO_ERROR_LIBRARY;
    }

    if (!err)
    {
        if (*key->key_unit != '\0')
        {
            fits_write_key_unit(file->fptr, key->key_name, key->key_unit, &cfiostat);

            if (cfiostat)
            {
                err = CFITSIO_ERROR_LIBRARY;
            }

        }
    }

    return err;
}

/* update keywords in `file` with keywords in `header` that are also in `key_list` */
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
            if (cfitsio_create_file(&filtered_keys_file, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL))
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
    struct stat stbuf;
    fitsfile *fptr = NULL;
    int fileCreated = 0;
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
                *file = calloc(1, sizeof(CFITSIO_FILE *));
            }

            if (!*file)
            {
                fprintf(stderr, "[ cfitsio_open_file() ] unable to allocate fitsfile structure\n");
                err = CFITSIO_ERROR_OUT_OF_MEMORY;
            }

            if (!err)
            {
                fptr = fitsrw_getfptr(0, path, (writeable != 0), &err, &fileCreated);
                XASSERT(!fileCreated);

                if (!fptr)
                {
                    err = CFITSIO_ERROR_FILE_DOESNT_EXIST;
                }
            }

            if (!err)
            {
                (*file)->fptr = fptr;
            }
        }
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
                fprintf(stderr, "[ cfitsio_read_headsum() ] NOTE: FITS file %s does not contain a HEADSUM keyword\n", file->fptr->Fptr->filename);
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

static int Cf_write_longwarn(fitsfile *fptr)
{
    int cfiostat = 0;
    char cfiostat_msg[FLEN_STATUS];
    int err = CFITSIO_SUCCESS;

    fits_write_key_longwarn(fptr, &cfiostat);

    if (cfiostat)
    {
        fits_get_errstatus(cfiostat, cfiostat_msg);
        fprintf(stderr, "[ Cf_write_headsum() ] CFITSIO error '%s'\n", cfiostat_msg);
        err = CFITSIO_ERROR_LIBRARY;
    }

    return err;
}

int cfitsio_write_longwarn(CFITSIO_FILE *file)
{
    return Cf_write_longwarn(file->fptr);
}

/****************************************************************************/
// TH: Append to the end of list to keep COMMENT in the right sequence.

int cfitsio_append_header_key(CFITSIO_KEYWORD** keylist, const char *name, cfitsio_keyword_datatype_t type, void *value, const char *format, const char *comment, const char *unit, CFITSIO_KEYWORD **fits_key_out)
{
    CFITSIO_KEYWORD *node = NULL;
    CFITSIO_KEYWORD *last = NULL;
    int error_code = CFITSIO_SUCCESS;


    if (name)
    {
        node = (CFITSIO_KEYWORD *)calloc(1, sizeof(CFITSIO_KEYWORD));

        if (!node)
        {
            return CFITSIO_ERROR_OUT_OF_MEMORY;
        }
        else
        {
            memset(node, 0, sizeof(CFITSIO_KEYWORD));
            node->next = NULL;

            // append node to the end of list
            if (*keylist)
            {
                last = *keylist;
                while(last->next) last = last->next;
                last->next = node;
            }
            else // first node
            {
                *keylist = node;
            }

            snprintf(node->key_name, sizeof(node->key_name), "%s", name);
            node->key_type = type;

            if (value)
            {
                // save value into union
                switch (type)
                {
                    case( 'X'):
                    case (CFITSIO_KEYWORD_DATATYPE_STRING):
                        /* 68 is the max chars in FITS string keyword, but the HISTORY and COMMENT keywords
                         * can contain values with more than this number of characters, in which case
                         * the fits API key-writing function will split the string across multiple
                         * instances of these special keywords. */
                        node->key_value.vs = strdup((char *)value);
                        snprintf(node->key_tform, sizeof(node->key_tform), "1PA");
                        break;
                    case (CFITSIO_KEYWORD_DATATYPE_LOGICAL):
                        node->key_value.vl = *((int *)value);
                        snprintf(node->key_tform, sizeof(node->key_tform), "1L");
                        break;
                    case (CFITSIO_KEYWORD_DATATYPE_INTEGER):
                        node->key_value.vi = *((long long *)value);
                        snprintf(node->key_tform, sizeof(node->key_tform), "1K");
                        break;
                    case (CFITSIO_KEYWORD_DATATYPE_FLOAT):
                        node->key_value.vf = *((double *)value);
                        snprintf(node->key_tform, sizeof(node->key_tform), "1D");
                        break;
                    default:
                        fprintf(stderr, "invalid cfitsio keyword type '%c'\n", (char)type);
                        error_code = CFITSIO_ERROR_ARGS;
                        break;
                }
            }
            else
            {
                node->is_missing = 1;
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

            if (fits_key_out)
            {
                *fits_key_out = node;
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
    int status = 0;
    int nkeys;
    int error_code = CFITSIO_FAIL;


    CFITSIO_KEYWORD* node, *last, *kptr;

    char key_name[FLEN_KEYWORD];
    char key_value[FLEN_VALUE];

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
        if(fits_read_record(fptr, i, card, &status))
        {
            error_code = CFITSIO_ERROR_LIBRARY;
            goto error_exit;
        }

        if (fits_get_keyclass(card) == TYP_CMPRS_KEY)
        {
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
                node->key_type = ' ';
            }

            switch(node->key_type)
            {
                case ('X'): //complex number is stored as string, for now.
                case (kFITSRW_Type_String): //Trip off ' ' around cstring?
                    node->key_value.vs = strdup(key_value);
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
                    break;

                case (kFITSRW_Type_Float):
                    /* ART - FITSIO uses something much more complex (it will convert a string value to a double, for example) -
                     * uses strtod() on float strings */
                    sscanf(key_value,"%lf", &node->key_value.vf);
                    break;

                case (' '): // blank keyword value, set it to NULL string
                    node->key_type = kFITSRW_Type_String;
                    node->key_value.vs = NULL;
                    node->is_missing = 1;
                    break;
                default :
                    DEBUGMSG((stderr,"Key of unknown type detected [%s][%c]?\n", key_value, node->key_type));
                    break;
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
    (*image_info)->blank =0;
    (*image_info)->bscale =1.0;
    (*image_info)->bzero =0.0;

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
   fptr = fitsrw_getfptr(verbose, fits_filename, 0, &status, &fileCreated);

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
       fitsrw_closefptr(verbose, fptr);
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
       fitsrw_closefptr(verbose, fptr);
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

   fptr = fitsrw_getfptr(verbose, filename, 1, &status, &fileCreated);

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

        error_code = Cf_write_longwarn(fptr);
        if (error_code != CFITSIO_SUCCESS)
        {
            goto error_exit;
        }

        error_code = Cf_write_headsum(fptr, checksum);
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

      if (fitsrw_getfpinfo_ext(fptr, &fpinfo))
      {
         fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
         error_code = CFITSIO_ERROR_FILE_IO;
      }
      else
      {
         fpinfonew = *image_info;
         snprintf(fpinfonew.fhash, sizeof(fpinfonew.fhash), "%s", fpinfo.fhash);

         if (fitsrw_setfpinfo_ext(fptr, &fpinfonew))
         {
            fprintf(stderr, "Unable to update file pointer information.\n");
            error_code = CFITSIO_ERROR_FILE_IO;
         }
         else
         {
            if ((status = fitsrw_closefptr(verbose, fptr)) != 0)
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
        fitsrw_closefptr(verbose, fptr);
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
            err = cfitsio_create_file(&file, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL);
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
            err = cfitsio_create_file(cards, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL);
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

      fptr = fitsrw_getfptr(verbose, fnamedup, 0, &status, &fileCreated);
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
       status = fitsrw_closefptr(verbose, fptr);
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
                fptr = fitsrw_getfptr(verbose, filename, 1, &err, &fileCreated);

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
            if (fitsrw_getfpinfo_ext(fptr, &fpinfo))
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

                if (fitsrw_setfpinfo_ext(fptr, &fpinfonew))
                {
                    fprintf(stderr, "Unable to update file pointer information.\n");
                    err = CFITSIO_ERROR_FILE_IO;
                }
                else
                {
                    if (fitsrw_closefptr(verbose, fptr))
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
