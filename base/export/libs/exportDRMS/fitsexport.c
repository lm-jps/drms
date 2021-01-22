/* fitsexport.c */

#include "fitsexport.h"
#include "util.h"
#include "tasrw.h"
#include "atoinc.h"
#include <sys/mman.h>
#include <openssl/md5.h>

#define kFERecnum "RECNUM"
#define kFERecnumFormat "%lld"
#define kFERecnumComment "Recnum - the DRMS data series record number (unique within the series)"
#define kFERecnumCommentShort "Recnum"

#define kFE_DRMS_ID "DRMS_ID"
#define kFE_DRMS_ID_COMMENT "DRMS ID - the globally unique image ID (<SERIES>:<RECNUM>:<SEGMENT>)"
#define kFE_DRMS_ID_COMMENT_SHORT "DRMS ID"
#define kFE_DRMS_ID_FORMAT "%s"

#define kFE_PRIMARY_KEY "PRIMARYK"
#define kFE_PRIMARY_KEY_COMMENT "DRMS primary key - the set of DRMS keywords that uniquely identify a DRMS record"
#define kFE_PRIMARY_KEY_SHORT "DRMS primary key"
#define kFE_PRIMARY_KEY_FORMAT "%s"

/* keyword that can never be placed in a FITS file via DRMS */

#undef A
#undef B
#define A(X,Y,Z) #X,
#define B(X,Y)
char *kFITSRESERVED[] =
{
   #include "reserved.h"
   ""
};
#undef A
#undef B

#define A(X,Y,Z) Y,
#define B(X,Y)
enum FE_ReservedKeys_enum
{
   #include "reserved.h"
   kRKW_NUMKEYWORDS
};
#undef A
#undef B

typedef enum FE_ReservedKeys_enum FE_ReservedKeys_t;
typedef int (*pFn_ExportHandler)(void *key, void **fitskeys, void **key_out, void *nameout, void *extra);

/* Available keyword handlers */
int DateHndlr(void *keyin, void **fitskeys, void **key_out, void *nameout, void *comment_out);

int CommHndlr(void *keyin, void **fitskeys, void **key_out, void *nameout, void *extra);

/* A reserved keyword with no export handler is one that is prohibited from being exported. */
#define A(X,Y,Z) Z,
#define B(X,Y)
pFn_ExportHandler ExportHandlers[] =
{
   #include "reserved.h"
   NULL
};
#undef A
#undef B

/* FITS keywords that should never be imported */
#define A(X,Y,Z)
#define B(X,Y) #X,
char *kDRMSFORBIDDEN[] =
{
   #include "reserved.h"
   ""
};
#undef A
#undef B

#define A(X,Y,Z)
#define B(X,Y) Y,
enum FE_ForbiddenKeys_enum
{
   #include "reserved.h"
   kFKW_NUMKEYWORDS
};
#undef A
#undef B

HContainer_t *gReservedFits = NULL;
HContainer_t *gForbiddenDrms = NULL;

char *FE_Keyword_ExtType_Strings[] =
{
   "NONE",
   "INTEGER",
   "FLOAT",
   "STRING",
   "LOGICAL"
};

#define ISUPPER(X) (X >= 0x41 && X <= 0x5A)
#define ISLOWER(X) (X >= 0x61 && X <= 0x7A)
#define ISDIGIT(X) (X >= 0x30 && X <= 0x39)

typedef enum
{
     kFeKwCharFirst = 0,
     kFeKwCharNew,
     kFeKwCharError
} FeKwCharState_t;

static int FE_print_time_keyword(DRMS_Keyword_t *key, char *time_string, int sz_time_string, char *unit, int sz_unit)
{
    char buf[1024];
    char *endptr = NULL;
    TIME interval = 0; /* the number of seconds in the time interval implied by key->info->unit; e.g., if unit == 'mins' ==> 60 */
    int err = 0;

    interval = atoinc(key->info->unit);
    if (interval > 0)
    {
        /* ART - in general, time intervals in DRMS are double keywords, not TIME keywords; as such, this code will not be
         * used for printing time intervals, and instead interval values will appear as double FITS keywords; but if this code is
         * used, then the time interval needs to be printed as a string */

        /* the key value is a time interval where the interval given in seconds is key->value.time_val; must
         * convert the value in seconds to the value in the keyword unit of the time interval (e.g., if unit is 'mins', then
         * divide by 60); use a format that FITS would normally use for doubles */
        snprintf(buf, sizeof(buf), "%.*G", 17, key->value.time_val / interval);
    }
    else
    {
        /* force ISO for consistency, and use three decimal places for seconds - this is more precision than will ever
         * be needed (the max is 2 in production series) */
        sprint_time(buf, key->value.time_val, key->info->unit, 3);

        /* sprint_time tacks on a 'Z', which is not FITS-standard-compliant; remove it */
        if (buf[strlen(buf) - 1] == 'Z')
        {
            buf[strlen(buf) - 1] = '\0';
        }
    }

    if (time_string)
    {
        snprintf(time_string, sz_time_string, "%s", buf);
    }

    return err;
}

int DateHndlr(void *keyin, void **fitskeys, void **fits_key_out, void *nameout, void *comment_out)
{
   DRMS_Keyword_t *key = (DRMS_Keyword_t *)keyin;
   int err = 0;

   /* Write the implicit DATE keyword, but only if it has a value (if it is non-missing) */
   const DRMS_Type_Value_t *val = drms_keyword_getvalue(key);
   if (val && drms_keyword_gettype(key) == DRMS_TYPE_TIME)
   {
      /* unit (time zone) must be ISO - if not, don't export it */
      char unitbuf[DRMS_MAXUNITLEN];
      char *comment = NULL;
      char *separator = NULL;
      int fitsrwRet = 0;

      snprintf(unitbuf, sizeof(unitbuf), "%s", key->info->unit);
      strtoupper(unitbuf);

      /* If this is the DATE keyword, only export if the value isn't "missing". */
      if (strcmp(kFITSRESERVED[kRKW_date], key->info->name) != 0 || !drms_ismissing_time(val->time_val))
      {
         if (strcmp(unitbuf, "ISO") == 0)
         {
            char tbuf[1024];
            FE_print_time_keyword(key, tbuf, sizeof(tbuf), NULL, 0);

            /* the time string returned ends with the char 'Z' - remove it since it is not FITS-standard-compliant */
            if (tbuf[strlen(tbuf) - 1] == 'Z')
            {
               tbuf[strlen(tbuf) - 1] = '\0';
            }

            if (comment_out)
            {
                comment = strdup((char *)comment_out);
            }
            else if (*key->info->description != '\0')
            {
                comment = strdup(key->info->description);
            }

            /* strip off full description from short description */
            if (comment)
            {
                separator = index(comment, '-');
                if (separator)
                {
                    *separator-- = '\0';

                    /* strip off trailing white space */
                    while (separator >= comment && isspace(*separator))
                    {
                        *separator-- = '\0';
                    }
                }
            }

            if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key((CFITSIO_KEYWORD**)fitskeys, (char *)nameout, kFITSRW_Type_String, (void *)tbuf, *key->info->format != '\0' ?  key->info->format : NULL, comment, unitbuf, (CFITSIO_KEYWORD **)fits_key_out)))
            {
               fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
               err = 2;
            }

            if (comment)
            {
                free(comment);
                comment = NULL;
            }
         }
         else
         {
            /* DATE keyword has wrong time format, skip */
            fprintf(stderr, "Invalid time format for keyword '%s' - must be ISO.\n", key->info->name);
            err = 1;
         }
      }
   }
   else
   {
      fprintf(stderr, "Invalid data type for keyword '%s'.\n", key->info->name);
      err = 1;
   }

   return err;
}

int CommHndlr(void *keyin, void **fitskeys, void **fits_key_out, void *nameout, void *extra)
{
    int err = 0;
    char *sbuf = NULL;
    int rangeout = 0;
    DRMS_Keyword_t *key = (DRMS_Keyword_t *)keyin;
    const DRMS_Type_Value_t *val = drms_keyword_getvalue(key);
    int fitsrwRet = 0;


    if (val && drms_keyword_gettype(key) == DRMS_TYPE_STRING)
    {
        if (*val->string_val == '\0')
        {
            /* the keyword value is the empty string; we still want to export it  */
            if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key((CFITSIO_KEYWORD**)fitskeys, (char *)nameout, kFITSRW_Type_String, (void *)val->string_val, key->info->format, key->info->description, key->info->unit, (CFITSIO_KEYWORD **)fits_key_out)))
            {
                fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
                err = 2;
            }
        }
        else
        {
            char *tmp = strdup(val->string_val);

            if (tmp)
            {
                char *pc = tmp;
                char *pout = NULL;
                int nelem = 0;

                sbuf = malloc(sizeof(char) * strlen(tmp) + 1);
                pout = sbuf;

                while (*pc)
                {
                    if (*pc == '\n')
                    {
                        /* skip newlines - the fits_write_comment() code will take care of multiple lines */
                    }
                    else if (*pc >= 0x20 && *pc <= 0x7E || *pc >= 0xA0 && *pc <= 0xFF)
                    {
                        *pout = *pc;
                        pout++;
                    }
                    else
                    {
                        fprintf(stderr, "bad char '%x' at offset %d in comment '%s'\n", *pc, (int)(pc - tmp), tmp);
                        rangeout = 1;
                    }

                    /* next input character */
                    pc++;
                }

                *pout = '\0';
                if (*sbuf)
                {
                    if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key((CFITSIO_KEYWORD**)fitskeys, (char *)nameout, kFITSRW_Type_String, (void *)sbuf, key->info->format, key->info->description, key->info->unit, (CFITSIO_KEYWORD **)fits_key_out)))
                    {
                        fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
                        err = 2;
                    }
                }

                if (sbuf)
                {
                    free(sbuf);
                    sbuf = NULL;
                }

                free(tmp);
            }
            else
            {
                err = 1;
            }
        }
    }

    if (rangeout)
    {
        fprintf(stderr, "at least one character encoding in DRMS keyword '%s' not a member of Latin-1.\n", key->info->name);
    }

    return err;
}

static void FreeReservedFits(void *data)
{
   if (gReservedFits != (HContainer_t *)data)
   {
      fprintf(stderr, "Unexpected argument to FreeReservedFits(); bailing.\n");
      return;
   }

   hcon_destroy(&gReservedFits);
}

static void FreeForbiddenDrms(void *data)
{
   if (gForbiddenDrms != (HContainer_t *)data)
   {
      fprintf(stderr, "Unexpected argument to FreeForbiddenDrms(); bailing.\n");
      return;
   }

   hcon_destroy(&gForbiddenDrms);
}

/* Returns 0 if fitsName is a valid FITS keyword identifier, and not a reserved FITS keyword name.
 * Returns 1 if fitsName is invalid
 * Returns 2 if fitsName is valid but reserved
 * Returns 3 if fitsName is valid but something that the export code might explicitly create
 *   (and therefore should not be used because otherwise results might be unpredictable).
 */
static int FitsKeyNameValidationStatus(const char *fitsName)
{
   int error = 0;
   FeKwCharState_t state = kFeKwCharNew;
   char *nameC = strdup(fitsName);
   char *pc = nameC;

   if (strlen(fitsName) > 8)
   {
      /* max size is 8 chars */
      error = 1;
   }
   else
   {
      /* Disallow FITS reserved keywords - simple, extend, bzero, bscale, blank, bitpix, naxis, naxisN, comment, history, end */
      if (!gReservedFits)
      {
         int i = 0;

         gReservedFits = hcon_create(sizeof(int), 128, NULL, NULL, NULL, NULL, 0);
         while (*(kFITSRESERVED[i]) != '\0')
         {
            /* Save the enum value for this reserved keyword. */
            hcon_insert_lower(gReservedFits, kFITSRESERVED[i], &i);
            i++;
         }

         /* Register for clean up (also in the misc library) */
         BASE_Cleanup_t cu;
         cu.item = gReservedFits;
         cu.free = FreeReservedFits;
         base_cleanup_register("reservedfitskws", &cu);
      }

      if (gReservedFits)
      {
         char *tmp = strdup(fitsName);
         char *pch = NULL;
         char *endptr = NULL;
         char *naxis = "NAXIS";
         int len = strlen(naxis);
         int theint;

         strtoupper(tmp);

         /* Gotta special-case NAXIS since the entire family of keywords NAXISX are reserved. */
         if (strncmp(tmp, naxis, len) == 0)
         {
            pch = tmp + len;

            if (*pch)
            {
               theint = (int)strtol(pch, &endptr, 10);
               if (endptr == pch + strlen(pch))
               {
                  /* the entire string after NAXIS was an integer */
                  if (theint > 0 && theint <= 999)
                  {
                     /* fitsName is a something we can't export */
                     error = 2;
                  }
               }
            }
         }

         if (hcon_lookup_lower(gReservedFits, tmp))
         {
            error = 2;
         }

         free(tmp);
      }

      /* Check to see if the keyword will conflict with an explicitly written (by export code) keyword */
      if (!error)
      {
         /* For now, there is just one name in this camp, so don't use any fancy hcontainer. */
         if (strcasecmp(fitsName, kFERecnum) == 0)
         {
            error = 3;
         }
      }

      if (!error)
      {
         while (*pc != 0 && !error)
         {
            switch (state)
            {
               case kFeKwCharError:
                 error = 1;
                 break;
               case kFeKwCharNew:
                 if (*pc == '-' ||
                     *pc == '_' ||
                     ISUPPER(*pc) ||
                     ISDIGIT(*pc))
                 {
                    state = kFeKwCharNew;
                    pc++;
                 }
                 else
                 {
                    state = kFeKwCharError;
                 }
                 break;
               default:
                 state = kFeKwCharError;
            }
         }
      }
   }

   if (nameC)
   {
      free(nameC);
   }

   return error;
}

/* Phil's scheme for arbitrary fits names. */
/* XXX This has to change to Phil's default scheme */
int GenerateFitsKeyName(const char *drmsName, char *fitsName, int size)
{
    const char *pC = drmsName;
    int nch = 0;

    memset(fitsName, 0, size);

    if (size >= 9)
    {
        while (*pC && nch < 8)
        {
            if (*pC >= 48 && *pC <= 57)
            {
                /* numerals are permitted according to the FITS standard */
                fitsName[nch] = *pC;
                nch++;
            }
            else if (*pC >= 65 && *pC <= 90)
            {
                fitsName[nch] = *pC;
                nch++;
            }
            else if (*pC >= 97 && *pC <= 122)
            {
                /* convert lower-case DRMS keyword name characters to upper case (lower-case chars are not permitted) */
                fitsName[nch] = (char)toupper(*pC);
                nch++;
            }

            pC++;
        }

        /* Now check that the result, fitsName, is not a reserved FITS key name */
        if (FitsKeyNameValidationStatus(fitsName) == 2)
        {
            /* but it could be reserved because of a suffix issue */
            char *tmp = strdup(fitsName);
            char *pch = NULL;

            if (tmp && (pch = strchr(tmp, '_')) != NULL && hcon_lookup_lower(gReservedFits, pch))
            {
                *pch = '\0';
                snprintf(fitsName, 9, "_%s", tmp); /* FITS names can't have more than 8 chars */
            }
            else
            {
                snprintf(fitsName, 9, "_%s", tmp);
            }

            if (tmp)
            {
                free(tmp);
            }
        }
    }
    else
    {
        return 0;
    }

    return 1;
}

static int DRMSKeyTypeToFITSKeyType(DRMS_Type_t drms_type, cfitsio_keyword_datatype_t *fits_type)
{
    int err = 0;

    switch (drms_type)
    {
        case DRMS_TYPE_CHAR:
        case DRMS_TYPE_SHORT:
        case DRMS_TYPE_INT:
        case DRMS_TYPE_LONGLONG:
            *fits_type = 'I';
            break;
        case DRMS_TYPE_FLOAT:
        case DRMS_TYPE_DOUBLE:
            *fits_type = 'F';
            break;
        case DRMS_TYPE_TIME:
        case DRMS_TYPE_STRING:
            *fits_type = 'C';
            break;
        default:
            fprintf(stderr, "unsupported DRMS type '%d'.\n", drms_type);
            err = 1;
            break;
    }

    return err;
}

static int FE_cast_type_to_fits_key_type(FE_Keyword_ExtType_t cast_type, cfitsio_keyword_datatype_t *fits_type)
{
    int err = 0;

    switch (cast_type)
    {
        case kFE_Keyword_ExtType_Integer:
            *fits_type = CFITSIO_KEYWORD_DATATYPE_INTEGER;
            break;
        case kFE_Keyword_ExtType_Float:
            *fits_type = CFITSIO_KEYWORD_DATATYPE_FLOAT;
            break;
        case kFE_Keyword_ExtType_String:
            *fits_type = CFITSIO_KEYWORD_DATATYPE_STRING;
            break;
        case kFE_Keyword_ExtType_Logical:
            *fits_type = CFITSIO_KEYWORD_DATATYPE_LOGICAL;
            break;
        default:
            fprintf(stderr, "unsupported fits-export cast type '%d'.\n", cast_type);
            err = 1;
            break;
    }

    return err;
}

static int DRMSKeyValToFITSKeyVal(DRMS_Keyword_t *key, cfitsio_keyword_datatype_t *fitstype, void **fitsval, char **format, char **comment, char **unit)
{
   int err = 0;
   char *separator = NULL;
   DRMS_Type_Value_t *valin = &key->value;
   DRMS_Value_t type_and_value;
   char unit_new[CFITSIO_MAX_COMMENT] = {0};
   void *res = NULL;
   int status = DRMS_SUCCESS;
   FE_Keyword_ExtType_t casttype = fitsexport_keyword_getcast(key);

    if (valin && fitstype && format)
    {
        if (format)
        {
            *format = strdup(key->info->format);
        }

        if (comment)
        {
            if (*key->info->description != '\0')
            {
                *comment = strdup(key->info->description);

                /* strip off full description from short description */
                separator = index(*comment, '-');
                if (separator)
                {
                    *separator-- = '\0';

                    /* strip off trailing white space */
                    while (separator >= *comment && isspace(*separator))
                    {
                        *separator-- = '\0';
                    }
                }
            }
            else
            {
                *comment = NULL;
            }
        }

        if (fitstype)
        {
            if (casttype == kFE_Keyword_ExtType_None)
            {
                err = DRMSKeyTypeToFITSKeyType(key->info->type, fitstype);
            }
            else
            {
                err = FE_cast_type_to_fits_key_type(casttype, fitstype);
            }
        }

        if (!err)
        {
            /* if valin is the missing value, then the output value should be NULL */
            type_and_value.type = key->info->type;
            type_and_value.value = *valin;

            if (valin && drms_ismissing(&type_and_value))
            {
                res = NULL;
            }
            else
            {
                /* If the keyword being exported to FITS is a reserved keyword, then
                 * drop into specialized code to handle that reserved keyword. */
                if (casttype != kFE_Keyword_ExtType_None)
                {
                    /* cast specified in key's description field */
                    if (key->info->type != DRMS_TYPE_RAW)
                    {
                        switch (casttype)
                        {
                           case kFE_Keyword_ExtType_Integer:
                             res = malloc(sizeof(long long));
                             *(long long *)res = drms2int(key->info->type, valin, &status);
                             break;
                           case kFE_Keyword_ExtType_Float:
                             res = malloc(sizeof(double));
                             *(double *)res = drms2double(key->info->type, valin, &status);
                             break;
                           case kFE_Keyword_ExtType_String:
                           {
                              char tbuf[1024];
                              drms_keyword_snprintfval(key, tbuf, sizeof(tbuf));
                              res = (void *)strdup(tbuf);
                           }
                           break;
                           case kFE_Keyword_ExtType_Logical:
                             res = malloc(sizeof(long long));

                             if (drms2longlong(key->info->type, valin, &status))
                             {
                                *(long long *)res = 0;
                             }
                             else
                             {
                                *(long long *)res = 1;
                             }
                             break;
                           default:
                             fprintf(stderr, "Unsupported FITS type '%d'.\n", (int)casttype);
                             err = 1;
                             break;
                        }
                    }
                    else
                    {
                        /* This shouldn't happen, unless somebody mucked with key->info->description. */
                        fprintf(stderr, "DRMS_TYPE_RAW is not supported.\n");
                        err = 1;
                    }
                }
                else
                {
                    /* default conversion */
                    switch (key->info->type)
                    {
                      case DRMS_TYPE_CHAR:
                        res = malloc(sizeof(long long));
                        *(long long *)res = (long long)(valin->char_val);
                        break;
                      case DRMS_TYPE_SHORT:
                        res = malloc(sizeof(long long));
                        *(long long *)res = (long long)(valin->short_val);
                        break;
                      case DRMS_TYPE_INT:
                        res = malloc(sizeof(long long));
                        *(long long *)res = (long long)(valin->int_val);
                        break;
                      case DRMS_TYPE_LONGLONG:
                        res = malloc(sizeof(long long));
                        *(long long *)res = valin->longlong_val;
                        break;
                      case DRMS_TYPE_FLOAT:
                        res = malloc(sizeof(double));
                        *(double *)res = (double)(valin->float_val);
                        break;
                      case DRMS_TYPE_DOUBLE:
                        res = malloc(sizeof(double));
                        *(double *)res = valin->double_val;
                        break;
                      case DRMS_TYPE_TIME:
                      {
                         char tbuf[1024];
                         FE_print_time_keyword(key, tbuf, sizeof(tbuf), unit_new, sizeof(unit_new));
                         res = (void *)strdup(tbuf);
                      }
                      break;
                      case DRMS_TYPE_STRING:
                        res = (void *)strdup(valin->string_val);
                        break;
                      default:
                        fprintf(stderr, "unsupported DRMS type '%d'\n", (int)key->info->type);
                        err = 1;
                        break;
                    }
                }
            }

            if (unit)
            {
                if (*unit_new != '\0')
                {
                    *unit = strdup(unit_new);
                }
                else
                {
                    *unit = strdup(key->info->unit);
                }
            }
        }
        else
        {
            fprintf(stderr, "[ DRMSKeyValToFITSKeyVal() ] cannot convert DRMS keyword type '%c' to a FITS keyword type\n", key->info->type);
        }
    }
    else
    {
        fprintf(stderr, "[ DRMSKeyValToFITSKeyVal() ] invalid argument\n");
        err = 1;
    }


    if (!err)
    {
        *fitsval = res;
    }

    return err;
}

/* These two function export to FITS files only. */
int fitsexport_export_tofile(DRMS_Segment_t *seg, const char *cparms, const char *fileout, char **actualfname, unsigned long long *expsize)
{
   return fitsexport_mapexport_tofile(seg, cparms, NULL, NULL, fileout, actualfname, expsize);
}

/* Input seg must be the source segment, not the target segment, if the input seg is a linked segment. */
int fitsexport_mapexport_tofile(DRMS_Segment_t *seg, const char *cparms, const char *clname, const char *mapfile, const char *fileout, char **actualfname, unsigned long long *expsize)
{
   return fitsexport_mapexport_tofile2(seg->record, seg, 0, cparms, clname, mapfile, fileout, actualfname, expsize, (export_callback_func_t) NULL);  //ISS fly-tar
}

int fitsexport_mapexport_tofile2(DRMS_Record_t *rec, DRMS_Segment_t *seg, long long row_number, const char *cparms, /* NULL for stdout */ const char *clname, const char *mapfile, const char *fileout, /* "-" for stdout */ char **actualfname, /* NULL for stdout */ unsigned long long *expsize, /* NULL for stdout */ export_callback_func_t callback) //ISS fly-tar - fitsfile * for stdout
{
    int status = DRMS_SUCCESS;
    CFITSIO_KEYWORD *fitskeys = NULL;
    int num_keys = 0;
    LinkedList_t *ttypes = NULL;
    LinkedList_t *tforms = NULL;
    char *ttype = NULL;
    char *tform = NULL;
    char filename[DRMS_MAXPATHLEN];
    struct stat stbuf;
    DRMS_Segment_t *tgtseg = NULL;
    DRMS_Segment_t *actualSeg = NULL;
    char realfileout[DRMS_MAXPATHLEN];
    CFITSIO_FILE *out_file = NULL; /* exported fitsfile; if streaming, then this is also in-memory-only, otherwise
                                    * when closed, the fitsfile will be written to disk (to realfileout) */
    int close_out_file = 0; /* if we are streaming or using the callback method, then do not close out_file */
    struct stat filestat;
    int streaming = 0;
    int keywords_only = 0;
    int swval;
    CFITSIO_BINTABLE_INFO bintable_info = {0};
    LinkedList_t *bintable_rows = NULL;
    long long column_index = -1;
    ListNode_t *node = NULL;

    streaming = (strcmp(fileout, "-") == 0);
    keywords_only = (seg == NULL);

    if (!keywords_only)
    {
        if (seg->info->islink)
        {
            if ((tgtseg = drms_segment_lookup(seg->record, seg->info->name)) == NULL)
            {
                fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] unable to locate target segment %s file\n", seg->info->name);
                status = DRMS_ERROR_INVALIDFILE;
            }

            actualSeg = tgtseg;
        }
        else
        {
            actualSeg = seg;
        }

        if (status == DRMS_SUCCESS)
        {
            drms_segment_filename(actualSeg, filename); /* full, absolute path to segment file */

            if (*filename == '\0' || stat(filename, &stbuf))
            {
                /* file filename is missing */
                snprintf(seg->filename, sizeof(seg->filename), "%s", filename); /* so caller has access to file name */
                status = DRMS_ERROR_INVALIDFILE;
            }
        }
    }

    if (status == DRMS_SUCCESS)
    {
        snprintf(realfileout, sizeof(realfileout), "%s", fileout);

        if (!keywords_only)
        {
            /* must be source segment if the segment is a linked segment */
            fitskeys = fitsexport_mapkeys(NULL, seg, clname, mapfile, &num_keys, NULL, NULL, &status);

            if (tgtseg)
            {
                swval = tgtseg->info->protocol;
            }
            else
            {
                swval = seg->info->protocol;
            }

            switch (swval)
            {
                case DRMS_TAS:
                {
                    if (!streaming)
                    {
                        /* If we are reading a single record from a TAS file, this means that we're
                         * reading a single slice. fileout will have a .tas extension, since
                         * the output file name is derived from the input file name. We need to
                         * substitute .fits for .tas. */
                        size_t len = strlen(realfileout) + 64;
                        size_t lenstr;
                        char *dup = malloc(len);
                        snprintf(dup, len, "%s", realfileout);

                        if (dup)
                        {
                            lenstr = strlen(dup);
                            if (lenstr > 0 &&
                             (dup[lenstr - 1] == 's' || dup[lenstr - 1] == 'S') &&
                             (dup[lenstr - 2] == 'a' || dup[lenstr - 2] == 'A') &&
                             (dup[lenstr - 3] == 't' || dup[lenstr - 3] == 'T') &&
                              dup[lenstr - 4] == '.')
                            {
                             *(dup + lenstr - 3) = '\0';
                             snprintf(realfileout, sizeof(realfileout), "%sfits", dup);
                            }
                            else
                            {
                             fprintf(stderr, "Unexpected export file name '%s'.\n", dup);
                             free(dup);
                             status = DRMS_ERROR_EXPORT;
                             break;
                            }

                            free(dup);
                        }
                        else
                        {
                            status = DRMS_ERROR_OUTOFMEMORY;
                        }
                     }
                }

                    /* intentional fall-through */
                case DRMS_BINARY:
                    /* intentional fall-through */
                case DRMS_BINZIP:
                    /* intentional fall-through */
                case DRMS_FITZ:
                    /* intentional fall-through */
                case DRMS_FITS:
                    /* intentional fall-through */
                case DRMS_DSDS:
                    /* intentional fall-through */
                case DRMS_LOCAL:
                {
                    /* If the segment file is compressed, and will be exported in compressed
                     * format, don't uncompress it (which is what drms_segment_read() will do).
                     * Instead, use the cfitsio routines to read the image into memory, as is -
                     * so compressed image data will remain compressed in memory. Then
                     * combine the header and image into a new FITS file and write it to
                     * the fileout. Steps:
                     *   1. Use CopyFile() to copy the input segment file to fileout.
                     *   2. Call fits_open_image() to open the file for writing. This does not
                     *      read the image into memory.
                     *   3. Call cfitsio_key_to_card()/fits_write_record() to write keywords.
                     *   4. Call fits_write_img().
                     * It is probably best to use some modified version of fitsrw_write() that
                     * simply replaces keywords - it deletes all existing keywords and
                     * takes a keylist of keys to add to the image.
                     *
                     * Try to use the libfitsrw routines which automatically cache open
                     * fitsfile pointers and calculate checksums, etc. */
                    int file_is_up_to_date = 0;
                    char sums_file[PATH_MAX];
                    int has_longwarn = 0;
                    int has_headsum = 0;
                    char *old_headsum = NULL;
                    char *new_headsum = NULL;
                    void *addr = NULL;
                    DRMS_Segment_t *actualSeg = NULL;
                    CFITSIO_FILE *disk_file = NULL; /* in-memory-only fitsfile of existing file on disk */
                    CFITSIO_HEADER *oldFitsHeader = NULL; /* in-memory-only fitsfile header of existing file on disk (no image) */
                    CFITSIO_HEADER *newFitsHeader = NULL; /* in-memory-only fitsfile header of file formed from fitskeys (no image) */
                    CFITSIO_FILE *updated_file = NULL; /* in-memory-only fitsfile to which disk_file content has been copied and updated */
                    CFITSIO_KEYWORD *headsum_key = NULL;


                    snprintf(sums_file, sizeof(sums_file), "%s", filename);

                    /* this must be open read-only since it is in SUMS */
                    if (cfitsio_open_file(sums_file, &disk_file, 0))
                    {
                        /* if we can't open the file for some reason, do not error out, just pretend the existing file
                         * does not exist */
                        fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] WARNING: unable to open internal FITS file '%s'\n", sums_file);
                        status = DRMS_ERROR_INVALIDFILE;
                    }
                    else
                    {
                        if (cfitsio_read_headsum(disk_file, &old_headsum))
                        {
                            fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] WARNING: unable to read HEADSUM from internal FITS file '%s'\n", sums_file);
                        }
                        else if (old_headsum)
                        {
                            has_headsum = 1;
                        }

                        if (!old_headsum)
                        {
                            /* there was no HEADSUM keyword in the internal FITS file, which is OK since files were not
                             * initially created with HEADSUM keywords; it is not clear if the disk_file header has
                             * a complete set of keywords */


                            /* XXX - I THINK we have to close the in-mem header to flush buffers, then we can capture
                             * the FITS file output on stdout with a pipe to ANOTHER cfitsio_open_file(); SO...
                             * 1. create a pipe
                             * 2. redirect stdout to the pipe write end
                             * 3. redirect stdin to the read end of the pipe
                             */
                            if (cfitsio_create_file((CFITSIO_FILE **)&oldFitsHeader, "-", CFITSIO_FILE_TYPE_HEADER, NULL, NULL, NULL))
                            {
                                fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] unable to create empty FITS file\n");
                                status = DRMS_ERROR_EXPORT;
                            }

                            if (status == DRMS_SUCCESS)
                            {
                                /* copy keywords in fitskeys from disk_file to oldFitsHeader */
                                if (cfitsio_copy_header_keywords(disk_file, (CFITSIO_FILE *)oldFitsHeader, fitskeys))
                                {
                                    fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] unable to copy internal header to empty FITS file\n");
                                    status = DRMS_ERROR_EXPORT;
                                }
                            }

                            if (status == DRMS_SUCCESS)
                            {
                                /* oldFitsHeader has the header of the fits file we are exporting; fitskeys is a list
                                 * of fits keywords that we expect to be in fits file we are exporting; old_headsum
                                 * will contain the checksum of the keys listed in fitskeys that exist in oldFitsHeader */
                                if (cfitsio_generate_checksum(&oldFitsHeader, NULL, &old_headsum))
                                {
                                    fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] unable to calculate header checksum\n");
                                    status = DRMS_ERROR_EXPORT;
                                }
                            }
                        }
                    }

                    /* disk_file might be NULL, in which case we bail below with a DRMS_ERROR_INVALIDFILE status  */

                    if (oldFitsHeader)
                    {
                        /* ART - need to NOT flush to stdout, but oldFitsHeader is an in-memory file */
                        cfitsio_close_header(&oldFitsHeader);
                    }

                    if (status == DRMS_SUCCESS)
                    {
                        /* makes `newFitsHeader` CFITSIO_HEADER that contains metadata contained in `fitskeys`; must free newFitsHeader;
                         * generates checksum from CFITSIO_HEADER, placing it in `new_headsum` */
                        if (cfitsio_generate_checksum(&newFitsHeader, fitskeys, &new_headsum))
                        {
                            fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] unable to calculate header checksum\n");
                            status = DRMS_ERROR_EXPORT;
                        }

                        if (status == DRMS_SUCCESS)
                        {
                            /* the segment SUMS file is present and readable (and in disk_file) */
                            file_is_up_to_date = has_longwarn && has_headsum && old_headsum && new_headsum && strcmp(old_headsum, new_headsum) == 0;

                            if (file_is_up_to_date && !streaming)
                            {
                                /* no need to export - existing header is up-to-date; if we are not streaming to stdout,
                                 * make a link from filenameout to the internal FITS file; if we are streaming to stdout,
                                 * dump to */
                                if (symlink(sums_file, realfileout) == -1)
                                {
                                    status = DRMS_ERROR_INVALIDFILE;
                                }
                            }
                            else
                            {
                                /* must export - existing header is NOT up-to-date; send new FITSIO header instead of
                                 * the fitskeys list (to avoid creating the FITS header a second time), and
                                 * existing image data to ExportFITS3()
                                 */

                                /* we've decided not to actually update any series if this is the case; all we care about is
                                 * fixing the metadata on EXPORT in the case where somebody modified the metadata in
                                 * the database (i.e., they bypassed DRMS and used psql to modify metadata without creating
                                 * new records); the code here will produce consistent exported files; generally, we expect the
                                 * record-generating modules to create new records that contain FITS files with up-to-date
                                 * metadata */

                                /* ExportFITS3 will make a link from fileout to the keyword-updated internal FITS file */

                                /* extract image from existing disk_file (the internal fitsfile which may have metadata) */


                                /* send image data (fitsData) and metadata (fitsHeader) to ExportFITS3, which will either:
                                 * 1. combine them into a new FITS file with a path defined by fileout; this is the non-streaming
                                 *    case;
                                 * 2. combine them into a new in-memory FITS file, and dump them on stdout; this is the
                                 *    streaming case
                                 */

                                /* newFitsHeader is CLOSE to being correct - it has the wrong BITPIX and NAXIS and NAXISn; BUT the correct
                                 * values for those keywords exists in the disk_file; we need to grab the values for fitsKeys from newFitsHeader and
                                 * union them with the keywords in disk_file that has been stripped of the fitsKeys values; we can accomplish this
                                 * by starting with disk_file and then copying/updating the keys in fitsKeys that exist in newFitsHeader */
                                if (status == DRMS_SUCCESS)
                                {
                                    if (file_is_up_to_date)
                                    {
                                        updated_file = disk_file;
                                    }
                                    else
                                    {
                                        /* create the fptr, but do not write any keywords (no bitpipx, naxis, naxes) */
                                        if (cfitsio_create_file(&updated_file, "-", CFITSIO_FILE_TYPE_IMAGE, NULL, NULL, NULL))
                                        {
                                            status = DRMS_ERROR_FITSRW;
                                        }

                                        if (status == DRMS_SUCCESS)
                                        {
                                            /* we need to copy the internal input fitsfile so we can edit the header; copy the image too;
                                             * copy compression type too */
                                            if (cfitsio_copy_file(disk_file, updated_file, 0))
                                            {
                                                status = DRMS_ERROR_FITSRW;
                                            }
                                        }

                                        if (status == DRMS_SUCCESS)
                                        {
                                            if (cfitsio_update_header_keywords(updated_file, newFitsHeader, fitskeys))
                                            {
                                                status = DRMS_ERROR_FITSRW;
                                            }
                                        }

                                        if (status == DRMS_SUCCESS)
                                        {
                                            /* write the LONGSTRN keyword to inform FITS readers that the long string convention may be used;
                                             * no harm if this keyword already exists (it will be written to out_file only once) */
                                            if (cfitsio_write_longwarn(updated_file))
                                            {
                                                status = DRMS_ERROR_FITSRW;
                                            }
                                        }

                                        /* write the HEADSUM keyword; this is a checksum of just the FITS keywords that map to
                                         * the DRMS keywords for this image */
                                        if (status == DRMS_SUCCESS)
                                        {
                                            if (cfitsio_write_headsum(updated_file, new_headsum))
                                            {
                                                status = DRMS_ERROR_FITSRW;
                                            }
                                        }
                                    }
                                }

                                if (status == DRMS_SUCCESS)
                                {
                                    /* figure out what the out file is - a stream, a disk file, that VSO on-the-fly thing; we
                                     * write the final, complete FITS file to out_file */
                                    if (callback != NULL)
                                    {
                                        CFITSIO_FITSFILE fptr = NULL; /* the fitsfile * inside (CFITSIO_FILE *)callback (if streaming), or produced by callback (if not streaming) */
                                        CFITSIO_FILE *callback_cfitsio_file = NULL;
                                        cfitsio_file_state_t callback_file_state = CFITSIO_FILE_STATE_EMPTY;
                                        cfitsio_file_type_t callback_file_type = CFITSIO_FILE_TYPE_UNKNOWN;

                                        /* we are not initializing fptr since we will be using a fitsfile generated by a different
                                         * block of code (streaming --> callback is the fptr; !streaming --> callback will create the fptr) */
                                        if (cfitsio_create_file(&out_file, NULL, CFITSIO_FILE_TYPE_UNKNOWN, NULL, NULL, NULL))
                                        {
                                            status = DRMS_ERROR_FITSRW;
                                        }
                                        /* do not cache this fitsfile; in the streaming case, the fitsfile is in-memory-only, so don't need to cache;
                                         * in the VSO "create" case, the VSO drms_export_cgi.c handles the fitsfile */
                                        close_out_file = 0;

                                        if (streaming)
                                        {
                                            /* callback IS the in-memory-only CFITSIO_FITSFILE that will be eventually streamed */
                                            callback_cfitsio_file = (CFITSIO_FILE *)callback;
                                            cfitsio_get_fitsfile(callback_cfitsio_file, &fptr);
                                            cfitsio_get_file_state(callback_cfitsio_file, &callback_file_state);
                                            cfitsio_get_file_type(callback_cfitsio_file, &callback_file_type);
                                            cfitsio_set_fitsfile(out_file, fptr, 1, callback_file_state, callback_file_type);
                                            /* DO NOT CLOSE THIS FILE */
                                        }
                                        else
                                        {
                                            /* not stdout */
                                            int retVal = 0;
                                            int cfiostat = 0;


                                            /* use ISS callback to create the fitsfile */
                                            /* NOTE - there is no reason to call the "setarrout" callback any more; the DRMS_Array_t is no longer
                                             * used by drms_export_cgi.c; cfitsio_copy_file() will copy the image into out_file->fptr, which is then
                                             * used by drms_export_cgi.c */
                                            /* DO NOT CLOSE THIS FILE */

                                            (*callback)("create", &fptr, realfileout, cparms, &cfiostat, &retVal);
                                            if (cfiostat || retVal != CFITSIO_SUCCESS)
                                            {
                                                status = DRMS_ERROR_FILECREATE;
                                            }
                                            else
                                            {
                                                /* set compression type too */
                                                cfitsio_set_fitsfile(out_file, fptr, 1, CFITSIO_FILE_STATE_UNINITIALIZED, CFITSIO_FILE_TYPE_IMAGE);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        /* create a new file - realfileout is the file to export onto disk (not streaming); the fitsfile
                                         * will be cached */

                                        /* create the fptr, but do not write any keywords (no bitpipx, naxis, naxes) */
                                        if (cfitsio_create_file(&out_file, realfileout, CFITSIO_FILE_TYPE_IMAGE, NULL, NULL, NULL))
                                        {
                                            status = DRMS_ERROR_FITSRW;
                                        }
                                        else
                                        {
                                            close_out_file = 1;
                                        }
                                    }

                                    if (status == DRMS_SUCCESS)
                                    {
                                        /* now, copy the final in-memory file (updated_file) to either disk or stdout (streaming); copy
                                         * compression type too */
                                        if (cfitsio_copy_file(updated_file, out_file, 0))
                                        {
                                            status = DRMS_ERROR_FITSRW;
                                        }

                                        /* updated_file was an in-memory-only file */
                                        /* ART - need to NOT flush to stdout, but updated_file is an in-memory file */
                                        if (updated_file != disk_file)
                                        {
                                            cfitsio_close_file(&updated_file);
                                        }

                                        /* close the original SUMS file */
                                        cfitsio_close_file(&disk_file);
                                    }

                                    if (close_out_file)
                                    {
                                        /* flush to disk or stdout (if streaming, do not flush to stdout) */
                                        cfitsio_close_file(&out_file);
                                    }
                                }
                            }
                        }
                    }

                    if (new_headsum)
                    {
                        free(new_headsum);
                        new_headsum = NULL;
                    }

                    if (newFitsHeader)
                    {
                        cfitsio_close_header(&newFitsHeader);
                    }

                    if (old_headsum)
                    {
                        free(old_headsum);
                        old_headsum = NULL;
                    }
                }
                break;
                case DRMS_GENERIC:
                {
                    int ioerr;

                    /* Simply copy the file from the segment's data-file path
                    * to fileout, no keywords to worry about. */

                    /* filename could be a directory. If that is the case, then copy the entire tree to realfileout. Art made a change
                     * to exputl_mk_expfilename() so that if a generic segment has no seg->filename to use for realfileout, then
                     * one is made from <su dir>/<slot dir>/<seg name>. He also changed CopyFile() to handle tree copies. */
                    if (CopyFile(filename, realfileout, &ioerr) != stbuf.st_size)
                    {
                        if (!S_ISDIR(stbuf.st_mode))
                        {
                            /* For a directory, CopyFile will return the number of bytes of all the files copied within the directory at any level.
                             * This will not match stbuf.st_size, the size of the directory, which is 0.
                             */
                            fprintf(stderr, "Unable to export file '%s' to '%s'.\n", filename, realfileout);
                            status = DRMS_ERROR_FILECOPY;
                        }
                    }
                }
                break;
                default:
                  fprintf(stderr, "data export does not support data segment protocol '%s'\n", drms_prot2str(seg->info->protocol));
            } /* segment protocol switch */
        }
        else
        {
            /* adding a row to a BINTABLE (or making a keyword BINTABLE) */
            CFITSIO_FITSFILE fptr = NULL; /* the fitsfile * inside (CFITSIO_FILE *)callback (if streaming), or produced by callback (if not streaming) */
            int retVal = 0;
            cfitsio_file_state_t callback_file_state = CFITSIO_FILE_TYPE_UNKNOWN;
            int cfiostat = 0;

            /* check to see if bintable has already been created */
            if (callback != NULL)
            {
                close_out_file = 0;

                if (streaming)
                {
                    out_file = (CFITSIO_FILE *)callback;
                }
                else
                {
                    (*callback)("create", &fptr, realfileout, cparms, &cfiostat, &retVal);
                    if (cfiostat || retVal != CFITSIO_SUCCESS)
                    {
                        fprintf(stderr, "[ fitsexport_mapexport_tofile2() ] unable to export file '%s' to '%s'.\n", filename, realfileout);
                        status = DRMS_ERROR_FILECREATE;
                    }
                    else
                    {
                        cfitsio_set_fitsfile(out_file, fptr, 1, CFITSIO_FILE_STATE_UNINITIALIZED, CFITSIO_FILE_TYPE_BINTABLE);
                    }
                }

                if (status == DRMS_SUCCESS)
                {
                    if (cfitsio_get_file_state(out_file, &callback_file_state))
                    {
                        status = DRMS_ERROR_FITSRW;
                    }
                }
            }

            if (status == DRMS_SUCCESS)
            {
                if (callback == NULL || callback_file_state == CFITSIO_FILE_STATE_UNINITIALIZED)
                {
                    /* need to create bintable in unitialized file */
                    ttypes = list_llcreate(sizeof(char *), NULL);
                    tforms = list_llcreate(sizeof(char *), NULL);
                    fitskeys = fitsexport_mapkeys(rec, NULL, clname, mapfile, &num_keys, ttypes, tforms, &status);

                    if (status == DRMS_SUCCESS)
                    {
                        if (num_keys > CFITSIO_MAX_BINTABLE_WIDTH)
                        {
                            fprintf(stderr, "[ fitsexport_mapexport_tofile2() too many columns in bintable (maximum is %d)]\n", CFITSIO_MAX_BINTABLE_WIDTH);
                            status == DRMS_ERROR_FITSRW;
                        }
                    }

                    if (status == DRMS_SUCCESS)
                    {
                        bintable_rows = list_llcreate(sizeof(CFITSIO_KEYWORD *), NULL);
                        list_llinserttail(bintable_rows, &fitskeys);

                        bintable_info.rows = bintable_rows;
                        bintable_info.tfields = num_keys;

                        column_index = 0;
                        list_llreset(ttypes);
                        while ((node = list_llnext(ttypes)) != NULL)
                        {
                            ttype = *(char **)node->data;
                            // snprintf((char *)&bintable_info.ttypes[column_index], sizeof(bintable_info.ttypes[column_index]), "%s", ttype);
                            bintable_info.ttypes[column_index] = (CFITSIO_BINTABLE_TTYPE *)ttype;

                            column_index++;
                        }

                        column_index = 0;
                        list_llreset(tforms);
                        while ((node = list_llnext(tforms)) != NULL)
                        {
                            tform = *(char **)node->data;
                            bintable_info.tforms[column_index] = (CFITSIO_BINTABLE_TFORM *)tform;

                            column_index++;
                        }
                    }
                }
                else
                {
                    /* no need for ttypes or tforms */
                    fitskeys = fitsexport_mapkeys(rec, NULL, clname, mapfile, &num_keys, NULL, NULL, &status);

                    if (status == DRMS_SUCCESS)
                    {
                        if (num_keys > CFITSIO_MAX_BINTABLE_WIDTH)
                        {
                            fprintf(stderr, "[ fitsexport_mapexport_tofile2() too many columns in bintable (maximum is %d)]\n", CFITSIO_MAX_BINTABLE_WIDTH);
                            status == DRMS_ERROR_FITSRW;
                        }
                    }

                    if (status == DRMS_SUCCESS)
                    {
                        bintable_rows = list_llcreate(sizeof(CFITSIO_KEYWORD *), NULL);
                        list_llinserttail(bintable_rows, &fitskeys);
                    }
                }
            }

            if (status == DRMS_SUCCESS)
            {
                if (callback == NULL)
                {
                    if (cfitsio_create_file(&out_file, realfileout, CFITSIO_FILE_TYPE_BINTABLE, NULL, &bintable_info, NULL))
                    {
                        fprintf(stderr, "[ fitsexport_mapexport_tofile2() unable to create FITS bintable\n");
                        status = DRMS_ERROR_FITSRW;
                    }
                    else
                    {
                        close_out_file = 1;
                    }
                }
                else if (callback_file_state == CFITSIO_FILE_STATE_UNINITIALIZED)
                {
                    if (cfitsio_create_bintable(out_file, &bintable_info))
                    {
                        fprintf(stderr, "[ fitsexport_mapexport_tofile2() unable to create FITS bintable\n");
                        status = DRMS_ERROR_FITSRW;
                    }
                }
            }

            /* out_file now exists and is initialized to CFITSIO_FILE_TYPE_BINTABLE type */

            /* need to pass a LinkedList_t that contains `fitskeys` */
            if (status == DRMS_SUCCESS)
            {
                /* no cfitsio_bintable_info needed */
                if (cfitsio_write_keys_to_bintable(out_file, row_number, bintable_rows))
                {
                    fprintf(stderr, "[ fitsexport_mapexport_tofile2() unable to write keys to FITS bintable\n");
                    status = DRMS_ERROR_FITSRW;
                }
            }

            list_llfree(&bintable_rows);

            if (close_out_file)
            {
                /* flush to disk or stdout (if streaming, do not flush to stdout) */
                cfitsio_close_file(&out_file);
            }

            if (ttypes)
            {
                list_llfree(&ttypes);
            }

            if (tforms)
            {
                list_llfree(&tforms);
            }
        }


        // fly-tar ISS
        // Don't test the file if callback is not NULL
        // in that case callback handles everything related
        // to the fits generation
        if (callback == NULL)
        {
            /* Ensure file got created. */
            if (stat(realfileout, &filestat))
            {
                status = DRMS_ERROR_EXPORT;
            }
            else if (expsize)
            {
                *actualfname = strdup(basename(realfileout));
                *expsize = filestat.st_size;
            }
        }

        cfitsio_free_keys(&fitskeys);
    }

    return status;
}

int fitsexport_mapexport_to_cfitsio_file(CFITSIO_FILE *file, DRMS_Segment_t *seg, const char *clname, const char *mapfile)
{
    /* ugh - use "-" to indicate that we are exporting to stdout; when that happens the fitsfile * is stored in
     * the callback argument
     */
    return fitsexport_mapexport_tofile2(seg->record, seg, 0, NULL, clname, mapfile, "-", NULL, NULL, (export_callback_func_t)file);
}

int fitsexport_mapexport_keywords_to_cfitsio_file(CFITSIO_FILE *file, DRMS_Record_t *rec, long long row_number, const char *clname, const char *mapfile)
{
    /* no segment - export FITS file with no image */
    return fitsexport_mapexport_tofile2(rec, NULL, row_number, NULL, clname, mapfile, "-", NULL, NULL, (export_callback_func_t)file);
}

/* Map keys that are specific to a segment to fits keywords.  User must free.
 * Follows keyword links and ensures that per-segment keywords are relevant
 * to this seg's keywords. */

/* Input seg must be the src seg, not the target seg, if the input seg is a linked segment. */
CFITSIO_KEYWORD *fitsexport_mapkeys(DRMS_Record_t *rec, DRMS_Segment_t *seg, const char *clname, const char *mapfile, int *num_keys, LinkedList_t *ttypes, LinkedList_t *tforms, int *status)
{
    CFITSIO_KEYWORD *fitskeys = NULL;
    CFITSIO_KEYWORD *fits_key = NULL;
    int total_keys = 0;
    HIterator_t *last = NULL;
    int statint = DRMS_SUCCESS;
    DRMS_Keyword_t *key = NULL;
    const char *keyname = NULL;
    char segnum[4];
    DRMS_Record_t *recin = (seg ? seg->record : rec);
    Exputl_KeyMap_t *map = NULL;
    FILE *fptr = NULL;
    char drms_id[CFITSIO_MAX_COMMENT];
    char *primary_key = NULL;
    size_t sz_primary_key = 64;
    int npkeys = 0;
    int pkey = -1;
    char **ext_pkeys = NULL;
    int fitsrwRet = 0;
    CFITSIO_BINTABLE_TTYPE *ttype = NULL;
    CFITSIO_BINTABLE_TFORM *tform = NULL;

    total_keys = 0;
    while ((key = drms_record_nextkey(recin, &last, 0)) != NULL)
    {
        keyname = drms_keyword_getname(key);

        if (!drms_keyword_getimplicit(key))
        {
            if (seg)
            {
                /* do not look for per-segment keywords if the record belongs to a series that has no segments */
                if (drms_keyword_getperseg(key))
                {
                    snprintf(segnum, sizeof(segnum), "%03d", seg->info->segnum);

                    /* Ensure that this keyword is relevant to this segment. */
                    if (!strstr(keyname, segnum))
                    {
                        continue;
                    }
                }
            }

            if (mapfile && !map)
            {
                map = exputl_keymap_create();

                /* Allow for mapfile to actually be newline-separated key-value pairs. */
                fptr = fopen(mapfile, "r");
                if (fptr)
                {
                    if (!exputl_keymap_parsefile(map, fptr))
                    {
                        exputl_keymap_destroy(&map);
                    }

                    fclose(fptr);
                }
                else if (!exputl_keymap_parsetable(map, mapfile))
                {
                    /* Bad mapfile or map string - print a warning. */
                    fprintf(stderr, "drms_keyword_mapexport() - warning, keyword map file or string '%s' is invalid.\n", mapfile);
                    exputl_keymap_destroy(&map);
                    mapfile = NULL;
                }
            }

            /* calls cfitsio_append_header_key() */
            if (fitsexport_mapexportkey(key, clname, map, &fitskeys, &fits_key))
            {
                fprintf(stderr, "Couldn't export keyword '%s'.\n", keyname);
                statint = DRMS_ERROR_EXPORT;
            }
            else
            {
                total_keys++;
                if (ttypes)
                {
                    ttype = (CFITSIO_BINTABLE_TTYPE *)(fits_key->key_name);
                    list_llinserttail(ttypes, &ttype);
                }

                if (tforms)
                {
                    tform = (CFITSIO_BINTABLE_TFORM *)(fits_key->key_tform);
                    list_llinserttail(tforms, &tform);
                }
            }
        }
    }

    /* Export recnum to facilitate the association between an exported FITS file and its record of origin. */
    long long recnum = recin->recnum;
    if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key(&fitskeys, kFERecnum, kFITSRW_Type_Integer, (void *)&recnum, kFERecnumFormat, kFERecnumCommentShort, NULL, &fits_key)))
    {
        fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
        statint = DRMS_ERROR_FITSRW;
    }
    else
    {
        total_keys++;
        if (ttypes)
        {
            ttype = (CFITSIO_BINTABLE_TTYPE *)(fits_key->key_name);
            list_llinserttail(ttypes, &ttype);
        }

        if (tforms)
        {
            tform = (CFITSIO_BINTABLE_TFORM *)(fits_key->key_tform);
            list_llinserttail(tforms, &tform);
        }
    }

    /* recnum is nice, but to uniquely ID every image, we need series/recnum/[segment] (segment is optional, since there might not be a segment) */
    snprintf(drms_id, sizeof(drms_id), "%s:%lld:%s", recin->seriesinfo->seriesname, recnum, seg ? seg->info->name : "no_segment");
    if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key(&fitskeys, kFE_DRMS_ID, kFITSRW_Type_String, (void *)drms_id, kFE_DRMS_ID_FORMAT, kFE_DRMS_ID_COMMENT_SHORT, NULL, &fits_key)))
    {
        fprintf(stderr, "FITSRW returned '%d'\n", fitsrwRet);
        statint = DRMS_ERROR_FITSRW;
    }
    else
    {
        total_keys++;
        if (ttypes)
        {
            ttype = (CFITSIO_BINTABLE_TTYPE *)(fits_key->key_name);
            list_llinserttail(ttypes, &ttype);
        }

        if (tforms)
        {
            tform = (CFITSIO_BINTABLE_TFORM *)(fits_key->key_tform);
            list_llinserttail(tforms, &tform);
        }
    }

    /* add series prime-key keywords */
    ext_pkeys = drms_series_createpkeyarray(recin->env, recin->seriesinfo->seriesname, &npkeys, NULL);
    if (ext_pkeys)
    {
        if (npkeys > 0)
        {
            primary_key = calloc(1, sz_primary_key);
            primary_key = base_strcatalloc(primary_key, ext_pkeys[0], &sz_primary_key);

            for (pkey = 1; pkey < npkeys; pkey++)
            {
                primary_key = base_strcatalloc(primary_key, ", ", &sz_primary_key);
                primary_key = base_strcatalloc(primary_key, ext_pkeys[pkey], &sz_primary_key);
            }
        }

        drms_series_destroypkeyarray(&ext_pkeys, npkeys);
    }

    if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key(&fitskeys, kFE_PRIMARY_KEY, kFITSRW_Type_String, (void *)primary_key, kFE_PRIMARY_KEY_FORMAT, kFE_PRIMARY_KEY_SHORT, NULL, &fits_key)))
    {
        fprintf(stderr, "FITSRW returned '%d'\n", fitsrwRet);
        statint = DRMS_ERROR_FITSRW;
    }
    else
    {
        total_keys++;
        if (ttypes)
        {
            ttype = (CFITSIO_BINTABLE_TTYPE *)(fits_key->key_name);
            list_llinserttail(ttypes, &ttype);
        }

        if (tforms)
        {
            tform = (CFITSIO_BINTABLE_TFORM *)(fits_key->key_tform);
            list_llinserttail(tforms, &tform);
        }
    }

    if (primary_key)
    {
        free(primary_key);
        primary_key = NULL;
    }

    if (map)
    {
        exputl_keymap_destroy(&map);
    }

    if (last)
    {
        hiter_destroy(&last);
    }

    if (num_keys)
    {
        *num_keys = total_keys;
    }

    if (status)
    {
        *status = statint;
    }

    return fitskeys;
}

/* Keyword mapping during export */
int fitsexport_exportkey(DRMS_Keyword_t *key, CFITSIO_KEYWORD **fitskeys, CFITSIO_KEYWORD **fits_key)
{
   return fitsexport_mapexportkey(key, NULL, NULL, fitskeys, fits_key);
}

/* For linked series, key is the source keyword (not the target). */
int fitsexport_mapexportkey(DRMS_Keyword_t *key, const char *clname, Exputl_KeyMap_t *map, CFITSIO_KEYWORD **fitskeys, CFITSIO_KEYWORD **fits_key)
{
    int stat = DRMS_SUCCESS;

    if (key && fitskeys)
    {
        char nameout[16];
        char comment_out[DRMS_MAXCOMMENTLEN] = {0};

        if (fitsexport_getmappedextkeyname(key, clname, map, nameout, sizeof(nameout), comment_out, sizeof(comment_out)))
        {
            int fitsrwRet = 0;
            cfitsio_keyword_datatype_t fitskwtype = CFITSIO_KEYWORD_DATATYPE_NONE;
            void *fitskwval = NULL;
            char *format = NULL;
            char *fitskw_comment = NULL;
            char *fitskw_unit = NULL;
            DRMS_Keyword_t *keywval = NULL;
            int rv = 0;

            /* follow link if key is a linked keyword, otherwise, use key. */
            keywval = drms_keyword_lookup(key->record, key->info->name, 1);

            /* It may be the case that the linked record is not found - the dependency
             * could be broken if somebody deleted the target record, for example. */
            if (keywval)
            {
                FE_ReservedKeys_t *ikey = NULL;

                if (gReservedFits && (ikey = (FE_ReservedKeys_t *)hcon_lookup_lower(gReservedFits, keywval->info->name)))
                {
                    if (ExportHandlers[*ikey])
                    {
                        /* A handler exists for this reserved keyword - okay to export it. */
                        rv = (*(ExportHandlers[*ikey]))(keywval, (void **)fitskeys, (void *)fits_key, (void *)nameout, *comment_out != '\0' ? (void *)comment_out : NULL);
                        if (rv == 2)
                        {
                            stat = DRMS_ERROR_FITSRW;
                        }
                        else if (rv == 1)
                        {
                            stat = DRMS_ERROR_INVALIDDATA;
                        }
                    }
                    else
                    {
                        /* No handler - don't export, but continue with other keys. */
                        fprintf(stderr, "Cannot export reserved keyword '%s'.\n", keywval->info->name);
                    }
                }
                else
                {
                    if ((rv = DRMSKeyValToFITSKeyVal(keywval, &fitskwtype, &fitskwval, &format, &fitskw_comment, &fitskw_unit)) == 0)
                    {
                        if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_header_key(fitskeys, nameout, fitskwtype, fitskwval, format, fitskw_comment, fitskw_unit, fits_key)))
                        {
                            fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
                            stat = DRMS_ERROR_FITSRW;
                        }
                    }
                    else
                    {
                        fprintf(stderr,
                                "Could not convert DRMS keyword '%s' to FITS keyword.\n",
                                key->info->name);
                        stat = DRMS_ERROR_INVALIDDATA;
                    }
                }
            }
            else
            {
                /* linked keyword structure not found */
                fprintf(stderr, "Broken link - unable to locate target for linked keyword '%s'.\n", key->info->name);
                stat = DRMS_ERROR_BADLINK;
            }

            if (fitskw_unit)
            {
                free(fitskw_unit);
                fitskw_unit = NULL;
            }

            if (fitskw_comment)
            {
                free(fitskw_comment);
                fitskw_comment = NULL;
            }

            if (fitskwval)
            {
                free(fitskwval);
            }

            if (format)
            {
                free(format);
            }
        }
        else
        {
            fprintf(stderr,
                    "Could not determine external FITS keyword name for DRMS name '%s'.\n",
                    key->info->name);
            stat = DRMS_ERROR_INVALIDDATA;
        }
    }

    return stat;
}

int fitsexport_getextkeyname(DRMS_Keyword_t *key, char *nameOut, int size)
{
   return fitsexport_getmappedextkeyname(key, NULL, NULL, nameOut, size, NULL, 0);
}

/* Same as above, but try a KeyMap first, then a KeyMapClass */
int fitsexport_getmappedextkeyname(DRMS_Keyword_t *key, const char *class, Exputl_KeyMap_t *map, char *nameOut, int size, char *comment_out, int comment_out_sz)
{
   int success = 0;
   const char *potential = NULL;
   int vstat = 0;

   /* 1 - Try KeyMap. */
   if (map != NULL)
   {
      potential = exputl_keymap_extname(map, key->info->name);
      if (potential)
      {
         vstat = fitsexport_fitskeycheck(potential);

         if (vstat != 1)
         {
            snprintf(nameOut, size, "%s", potential);
            success = 1;
         }
      }
   }

   /* 2 - Try KeyMapClass. */
   if (!success && class != NULL)
   {
      potential = exputl_keymap_classextname(class, key->info->name);
      if (potential)
      {
         vstat = fitsexport_fitskeycheck(potential);

         if (vstat != 1)
         {
            snprintf(nameOut, size, "%s", potential);
            success = 1;
         }
      }
   }

    if (!success)
    {
      /* Now try the map- and class-independent schemes. */
      char *pot = NULL;
      char *psep = NULL;
      char *desc = strdup(key->info->description);
      char *pFitsName = NULL;
      *nameOut = '\0';

      /* 1 - Try keyword name in description field. */
      if (desc)
      {
          pFitsName = strtok(desc, " ");

    if (pFitsName)
    {
        int len = strlen(pFitsName);

        if (len > 2 && pFitsName[0] == '[' && pFitsName[len - 1] == ']')
        {
            if (len - 2 < size)
            {
                pot = (char *)malloc(sizeof(char) * size);
                if (pot)
          		  {
            		     memcpy(pot, pFitsName + 1, len - 2);
            		     pot[len - 2] = '\0';

                     if (comment_out)
                     {
                         /* save the part of the comment that does not have the cast */
                         snprintf(comment_out, comment_out_sz, "%s {%s}", &desc[len + 1], pot);
                     }

                     /* The description might contain [X:Y], where
                      * X is the external keyword name, and Y is the
                      * external keyword cast. There could be a ':' */
                     if (fitsexport_keyword_getcast(key) != kFE_Keyword_ExtType_None)
                     {
                        psep = strchr(pot, ':');
                        *psep = '\0';
                     }

                     vstat = fitsexport_fitskeycheck(pot);

                     if (vstat != 1)
                     {
                        snprintf(nameOut, size, "%s", pot);
                        success = 1;
                     }

                    free(pot);
          		  }
            }
	    }
	 }

	 free(desc);
      }

      /* 2 - Try DRMS name (must be upper case). */
      if (!success)
      {
	 char nbuf[DRMS_MAXKEYNAMELEN];

	 snprintf(nbuf, sizeof(nbuf), "%s", key->info->name);
	 strtoupper(nbuf);

	 vstat = fitsexport_fitskeycheck(nbuf);

         if (vstat != 1)
         {
            snprintf(nameOut, size, "%s", nbuf);
            success = 1;
         }
      }

      /* 3 - Use default rule. */
        if (!success)
        {
            char actualKeyName[DRMS_MAXKEYNAMELEN];

            pot = (char *)malloc(sizeof(char) * size);
            if (pot)
            {
                snprintf(actualKeyName, sizeof(actualKeyName), "%s", key->info->name);
                if (drms_keyword_getperseg(key))
                {
                    /* strip off the _XXX from the end of the keyword's name */
                    actualKeyName[strlen(actualKeyName) - 4] = '\0';
                }

                if (GenerateFitsKeyName(actualKeyName, pot, size))
                {
                    vstat = fitsexport_fitskeycheck(pot);

                    if (vstat != 1)
                    {
                        snprintf(nameOut, size, "%s", pot);
                        success = 1;
                    }
                }

                free(pot);
            }
        }
   }

   if (success && vstat == 2)
   {
      /* Got a valid keyword name, but it is reserved and MAY require special processing.
       * Of course we disallow writing some reserved keywords altogether (Like NAXIS).
       * We don't know at this point if the reserved keyword can be exported - that decision
       * will be made in DRMSKeyValToFITSKeyVal(). */
   }

   return success;
}

int fitsexport_fitskeycheck(const char *fitsName)
{
   return FitsKeyNameValidationStatus(fitsName);
}

static int FITSKeyValToDRMSKeyVal(CFITSIO_KEYWORD *fitskey, DRMS_Type_t *type, DRMS_Type_Value_t *value, FE_Keyword_ExtType_t *casttype, char **format)
{
   int err = 0;

   if (fitskey && type &&value &&casttype)
   {
      if (*(fitskey->key_format) != '\0')
      {
         *format = strdup(fitskey->key_format);
      }

      switch (fitskey->key_type)
      {
         case kFITSRW_Type_String:
           {
              /* FITS-file string values will have single quotes and
               * may have leading or trailing spaces; strip those. */

              char *strval = strdup((fitskey->key_value).vs);
              char *pb = strval;
              char *pe = pb + strlen(pb) - 1;

              if (*pb == '\'' && *pe == '\'')
              {
                 *pe = '\0';
                 pe--;
                 pb++;
              }

              while (*pb == ' ')
              {
                 pb++;
              }

              while (*pe == ' ')
              {
                 *pe = '\0';
                 pe--;
              }

              value->string_val = strdup(pb);

              if (strval)
              {
                 free(strval);
              }

              *type = DRMS_TYPE_STRING;
              *casttype = kFE_Keyword_ExtType_String;
           }
           break;
         case kFITSRW_Type_Logical:
           /* Arbitrarily choose DRMS_TYPE_CHAR to store FITS logical type */
           if ((fitskey->key_value).vl == 1)
           {
              value->char_val = 1;
           }
           else
           {
              value->char_val = 0;
           }
           *type = DRMS_TYPE_CHAR;
           *casttype = kFE_Keyword_ExtType_Logical;
           break;
         case kFITSRW_Type_Integer:
           {
              long long intval = (fitskey->key_value).vi;

              if (intval <= (long long)SCHAR_MAX &&
                  intval >= (long long)SCHAR_MIN)
              {
                 value->char_val = (char)intval;
                 *type = DRMS_TYPE_CHAR;
              }
              else if (intval <= (long long)SHRT_MAX &&
                       intval >= (long long)SHRT_MIN)
              {
                 value->short_val = (short)intval;
                 *type = DRMS_TYPE_SHORT;
              }
              else if (intval <= (long long)INT_MAX &&
                       intval >= (long long)INT_MIN)
              {
                 value->int_val = (int)intval;
                 *type = DRMS_TYPE_INT;
              }
              else
              {
                 value->longlong_val = intval;
                 *type = DRMS_TYPE_LONGLONG;
              }

              *casttype = kFE_Keyword_ExtType_Integer;
           }
           break;
         case kFITSRW_Type_Float:
           {
              double floatval = (fitskey->key_value).vf;

              if (floatval <= (double)FLT_MAX &&
                  floatval >= (double)-FLT_MAX)
              {
                 value->float_val = (float)floatval;
                 *type = DRMS_TYPE_FLOAT;
              }
              else
              {
                 value->double_val = floatval;
                 *type = DRMS_TYPE_DOUBLE;
              }

              *casttype = kFE_Keyword_ExtType_Float;
           }
           break;
         default:
           fprintf(stderr, "Unsupported FITS type '%c'.\n", fitskey->key_type);
           break;
      }
   }
   else
   {
      fprintf(stderr, "FITSKeyValToDRMSKeyVal() - Invalid argument.\n");
      err = 1;
   }

   return err;
}

int fitsexport_getintkeyname(const char *keyname, char *nameOut, int size)
{
   int success = 0;
   char *potential = NULL;

   *nameOut = '\0';

   /* 1 - Try FITS name. */
   if (base_drmskeycheck(keyname) == 0)
   {
      strcpy(nameOut, keyname);
      success = 1;
   }

   /* 2 - Use default rule. */
   if (!success)
   {
      potential = (char *)malloc(sizeof(char) * size);
      if (potential)
      {
	 if (GenerateDRMSKeyName(keyname, potential, size))
	 {
	    strcpy(nameOut, potential);
	    success = 1;
	 }

	 free(potential);
      }
   }

   return success;
}

int fitsexport_getmappedintkeyname(const char *keyname,
                                   const char *class,
                                   Exputl_KeyMap_t *map,
                                   char *nameOut,
                                   int size)
{
   int success = 0;
   const char *potential = NULL;
   *nameOut = '\0';

   /* 1 - Try KeyMap */
   if (map != NULL)
   {
      potential = exputl_keymap_intname(map, keyname);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   /* 2 - Try KeyMapClass */
   if (!success && class != NULL)
   {
      potential = exputl_keymap_classintname(class, keyname);
      if (potential)
      {
	 snprintf(nameOut, size, "%s", potential);
	 success = 1;
      }
   }

   if (!success)
   {
      /* Now try the map- and class-independent schemes. */
      char buf[DRMS_MAXKEYNAMELEN];
      success = fitsexport_getintkeyname(keyname, buf, sizeof(buf));
      if (success)
      {
	 strncpy(nameOut, buf, size);
      }
   }

   return success;
}

static int IsForbidden(const char *fitskey)
{
   int disp = 0;

   if (!gForbiddenDrms)
   {
      int i = 0;

      gForbiddenDrms = hcon_create(sizeof(int), 128, NULL, NULL, NULL, NULL, 0);
      while (*(kDRMSFORBIDDEN[i]) != '\0')
      {
         /* Save the enum value for this reserved keyword. */
         hcon_insert_lower(gForbiddenDrms, kDRMSFORBIDDEN[i], &i);
         i++;
      }

      /* Register for clean up (also in the misc library) */
      BASE_Cleanup_t cu;
      cu.item = gForbiddenDrms;
      cu.free = FreeForbiddenDrms;
      base_cleanup_register("forbiddendrmskws", &cu);
   }

   if (gForbiddenDrms)
   {
      char *tmp = strdup(fitskey);
      char *pch = NULL;
      char *endptr = NULL;
      char *naxis = "naxis";
      int len = strlen(naxis);
      int theint;

      strtolower(tmp);

      /* Gotta special-case NAXIS since the entire family of keywords NAXISX are reserved. */
      if (strncmp(tmp, naxis, len) == 0)
      {
         pch = tmp + len;

         if (*pch)
         {
            theint = (int)strtol(pch, &endptr, 10);
            if (endptr == pch + strlen(pch))
            {
               /* the entire string after NAXIS was an integer */
               if (theint > 0 && theint <= 999)
               {
                  /* fitsName is something we can't import */
                  disp = 1;
               }
            }
         }
      }

      if (hcon_lookup_lower(gForbiddenDrms, tmp))
      {
         disp = 1;
      }

      free(tmp);
   }

   return disp;
}

int fitsexport_importkey(CFITSIO_KEYWORD *fitskey, HContainer_t *keys, int verbose)
{
   return fitsexport_mapimportkey(fitskey, NULL, NULL, keys, verbose);
}

/* If verbose, then warn about duplicate keywords discovered upon import;
 * deals with COMMENT and HISTORY FITS keywords by appending to the
 * existing COMMENT or HISTORY entry in the HContainer_t returned
 */
int fitsexport_mapimportkey(CFITSIO_KEYWORD *fitskey,
                            const char *clname,
                            const char *mapfile,
                            HContainer_t *keys,
                            int verbose)
{
   int stat = DRMS_SUCCESS;

   if (fitskey && keys)
   {
      char nameout[DRMS_MAXKEYNAMELEN];
      char *namelower = NULL;
      Exputl_KeyMap_t *map = NULL;
      FILE *fptr = NULL;

      /* It is possible that the FITS keyname is one that should not be
       * imported into DRMS, like BITPIX or NAXIS. The DRMS db has the information
       * contained in these keywords in places other than the series' table
       * keyword columns. */
      if (!IsForbidden(fitskey->key_name))
      {
         if (mapfile)
         {
            fptr = fopen(mapfile, "r");
            if (fptr)
            {
               map = exputl_keymap_create();

               if (!exputl_keymap_parsefile(map, fptr))
               {
                  exputl_keymap_destroy(&map);
               }

               fclose(fptr);
            }
         }

         if (fitsexport_getmappedintkeyname(fitskey->key_name, clname, map, nameout, sizeof(nameout)))
         {
            DRMS_Type_t drmskwtype;
            DRMS_Type_Value_t drmskwval;
            FE_Keyword_ExtType_t cast;
            DRMS_Keyword_t *newkey = NULL;
            char *format = NULL;

            /* If drmskwtype is string, then it is an alloc'd string, and ownership
             * gets passed all the way to the DRMS_Keyword_t in the keys. Caller of
             * drms_keyword_mapimport() must free. */
            if (!FITSKeyValToDRMSKeyVal(fitskey, &drmskwtype, &drmskwval, &cast, &format))
            {
               namelower = strdup(nameout);
               strtolower(namelower);

               if ((newkey = hcon_lookup(keys, namelower)) != NULL)
               {
                  /* Even though the newline char (0x0A) is NOT part of the Latin-1 character set,
                   * and the DRMS database is of Latin-1 encoding,
                   * PostgreSQL will allow you to insert a string with that character. */
                  if (strcmp(namelower, "comment") == 0 || strcmp(namelower, "history") == 0 )
                  {
                     /* If this is a FITS comment or history keyword, then
                      * append to the existing string;separate from previous
                      * values with a newline character. */
                     size_t size = strlen(newkey->value.string_val) + 1;
                     newkey->value.string_val = base_strcatalloc(newkey->value.string_val, "\n", &size);
                     newkey->value.string_val = base_strcatalloc(newkey->value.string_val,
                                                                 drmskwval.string_val,
                                                                 &size);
                  }
                  else if (newkey->record &&
                           newkey->info &&
                           newkey->info->type == drmskwtype)
                  {
                     /* key already exists in container - assume the user is trying to
                      * copy the key value into an existing DRMS_Record_t */
                     memcpy(&(newkey->value), &drmskwval, sizeof(DRMS_Type_Value_t));
                     if (format && *format != '\0')
                     {
                        snprintf(newkey->info->format, sizeof(newkey->info->format), "%s", format);
                     }
                  }
                  else if (verbose)
                  {
                     fprintf(stderr, "WARNING: Keyword '%s' already exists; the DRMS value is the value of the first instance .\n", nameout);
                  }
               }
               else if ((newkey = (DRMS_Keyword_t *)hcon_allocslot(keys, namelower)) != NULL)
               {
                  memset(newkey, 0, sizeof(DRMS_Keyword_t));
                  newkey->info = calloc(1, sizeof(DRMS_KeywordInfo_t));
                  memcpy(&(newkey->value), &drmskwval, sizeof(DRMS_Type_Value_t));

                  snprintf(newkey->info->name, DRMS_MAXKEYNAMELEN, "%s", nameout);
                  newkey->info->islink = 0;
                  newkey->info->type = drmskwtype;

                  /* Only write out the [fitsname:cast] if export will be confused otherwise -
                   * write it out if the fits keyword was of logical type (which is stored
                   * as a DRMS type of CHAR), or if the FITS name was not a legal DRMS name
                   */
                  if (cast == kFE_Keyword_ExtType_Logical || strcmp(nameout, fitskey->key_name) != 0)
                  {
                     snprintf(newkey->info->description,
                              DRMS_MAXCOMMENTLEN,
                              "[%s:%s]",
                              fitskey->key_name,
                              FE_Keyword_ExtType_Strings[cast]);
                  }

                  if (format && *format != '\0')
                  {
                     snprintf(newkey->info->format, sizeof(newkey->info->format), "%s", format);
                  }
                  else
                  {
                     /* guess a format, so the keywords will print with
                      * functions like drms_keyword_printval() */
                     switch (drmskwtype)
                     {
                        case DRMS_TYPE_CHAR:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%hhd");
                          break;
                        case DRMS_TYPE_SHORT:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%hd");
                          break;
                        case DRMS_TYPE_INT:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%d");
                          break;
                        case DRMS_TYPE_LONGLONG:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%lld");
                          break;
                        case DRMS_TYPE_FLOAT:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%f");
                          break;
                        case DRMS_TYPE_DOUBLE:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%lf");
                          break;
                        case DRMS_TYPE_STRING:
                          snprintf(newkey->info->format, DRMS_MAXFORMATLEN, "%%s");
                          break;
                        default:
                          fprintf(stderr, "Unsupported keyword data type '%d'", (int)drmskwtype);
                          stat = DRMS_ERROR_INVALIDDATA;
                          break;
                     }
                  }
               }
               else
               {
                  fprintf(stderr, "Failed to alloc a new slot in keys container.\n");
                  stat = DRMS_ERROR_OUTOFMEMORY;
               }

               if (namelower)
               {
                  free(namelower);
               }

               if (format)
               {
                  free(format);
               }
            }
            else
            {
               fprintf(stderr,
                       "Could not convert FITS keyword '%s' to DRMS keyword.\n",
                       fitskey->key_name);
               stat = DRMS_ERROR_INVALIDDATA;
            }
         }
         else
         {
            fprintf(stderr,
                    "Could not determine internal DRMS keyword name for FITS name '%s'.\n",
                    fitskey->key_name);
            stat = DRMS_ERROR_INVALIDDATA;
         }

         if (map)
         {
            exputl_keymap_destroy(&map);
         }
      }
   }

   return stat;
}

FE_Keyword_ExtType_t fitsexport_keyword_getcast(DRMS_Keyword_t *key)
{
   FE_Keyword_ExtType_t type = kFE_Keyword_ExtType_None;
   char *pcast = NULL;
   int icast;

   if (key && key->info->description)
   {
      pcast = strchr(key->info->description, ':');
      if (pcast)
      {
         pcast++;

         for (icast = 0; icast < (int)kFE_Keyword_ExtType_End; icast++)
         {
            if (strcasecmp(pcast, FE_Keyword_ExtType_Strings[icast]) == 0)
            {
               break;
            }
         }

         if (icast != kFE_Keyword_ExtType_End)
         {
            type = (FE_Keyword_ExtType_t)icast;
         }
      }
   }

   return type;
}

/* state
 *   -1 - error
 *    0 - begin (haven't parsed '[' yet)
 *    1 - '['
 *    2 - ':' (delimiter for cast)
 *    3 - ']'
 *
 * return value
 *    0 - success
 *    1 - error
 */
int fitsexport_getextname(const char *strin, char **extname, char **cast)
{
   int rv;
   const char *pch = NULL;
   int state;
   char buf[16]; /* max cfitsio keyname length is 8 */
   char bufcast[16];
   char *pbuf = NULL;
   char *pbufcast = NULL;

   pch = strin;
   pbuf = buf;
   pbufcast = bufcast;
   rv = 0;
   state = 0;

   if (extname || cast)
   {
      while (*pch && state != -1 && state != 3)
      {
         switch(state)
         {
            case 0:
              if (*pch == ' ')
              {
                 pch++;
              }
              else if (*pch == '[')
              {
                 state = 1;
                 pch++;
              }
              else
              {
                 /* no external name and no cast */
                 state = 3;
                 break;
              }
              break;
            case 1:
              if (*pch == ':')
              {
                 state = 2;
                 pch++;
              }
              else if (*pch == ']')
              {
                 state = 3;
                 break;
              }
              else
              {
                 *pbuf = *pch;
                 pch++;
                 pbuf++;
              }
              break;
            case 2:
              if (*pch == ']')
              {
                 state = 3;
                 break;
              }
              else
              {
                 *pbufcast = *pch;
                 pch++;
                 pbufcast++;
              }
              break;
            default:
              /* can't get here */
              state = -1;
              break;
         }
      }

      if (state == 0)
      {
         /* Never found anything other than spaces - valid description. */
         state = 3;
      }

      if (state != 3)
      {
         rv = 1;
      }
      else
      {
         *pbuf = '\0';
         if (extname)
         {
            *extname = strdup(buf);
         }
         *pbufcast = '\0';
         if (cast)
         {
            *cast = strdup(bufcast);
         }
      }
   } /* while */

   return rv;
}

/*
 * key - DRMS Keyword whose value is to be mapped (input)
 * fitsKwString - FITS keyword value in string format, with all whitespace removed (retuned by reference)
 */
int fitsexport_getmappedextkeyvalue(DRMS_Keyword_t *key, char **fitsKwString)
{
    DRMS_Keyword_t *keyWithVal = NULL;
    CFITSIO_KEYWORD *cfitsioKey = NULL;
    char dummy[] = "EXTVAL25";
    cfitsio_keyword_datatype_t fitsKwType = CFITSIO_KEYWORD_DATATYPE_NONE;
    void *fitsKwVal = NULL;
    char *fitsKwFormat = NULL; /* binary FITS keyword value */
    char *fitskw_comment = NULL;
    char *fitskw_unit = NULL;
    char *key_value = NULL;
    int err = 0;

    /* if key is a linked-keyword, then resolve link */
    keyWithVal = drms_keyword_lookup(key->record, key->info->name, 1);

    if ((DRMSKeyValToFITSKeyVal(keyWithVal, &fitsKwType, &fitsKwVal, &fitsKwFormat, &fitskw_comment, &fitskw_unit)) == 0)
    {
        if (cfitsio_create_header_key(dummy, fitsKwType, fitsKwVal, fitsKwFormat, fitskw_comment, fitskw_unit, &cfitsioKey) == 0)
        {
            if (cfitsio_key_value_to_string(cfitsioKey, &key_value) != CFITSIO_SUCCESS)
            {
                err = 1;
                fprintf(stderr, "unable to print FITS value keyword\n");
            }
            else
            {
                *fitsKwString = key_value; // yoink!
            }

            /* free cfitsioKey */
            cfitsio_free_key(&cfitsioKey);
        }
        else
        {
            /* error */
            err = 1;
            fprintf(stderr, "Unable to create FITS keyword.\n");
        }

        if (fitsKwFormat)
        {
            free(fitsKwFormat);
            fitsKwFormat = NULL;
        }

        if (fitskw_comment)
        {
            free(fitskw_comment);
            fitskw_comment = NULL;
        }

        if (fitsKwVal)
        {
            free(fitsKwVal);
            fitsKwVal = NULL;
        }
    }
    else
    {
        /* error */
        err = 1;
        fprintf(stderr, "Unable to convert DRMS keyword %s to FITS keyword.\n", key->info->name);
    }

    return err;
}
