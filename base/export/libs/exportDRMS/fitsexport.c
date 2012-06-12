/* fitsexport.c */

#include "fitsexport.h"
#include "util.h"

#define kFERecnum "RECNUM"
#define kFERecnumFormat "%lld"

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
typedef int (*pFn_ExportHandler)(void *key, void **fitskeys, void *nameout);

/* Available keyword handlers */
int DateHndlr(void *keyin, void **fitskeys, void *nameout);
int CommHndlr(void *keyin, void **fitskeys, void *nameout);

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


int DateHndlr(void *keyin, void **fitskeys, void *nameout)
{
   DRMS_Keyword_t *key = (DRMS_Keyword_t *)keyin;
   int err = 0;

   /* Write the implicit DATE keyword, but only if it has a value (if it is non-missing) */
   const DRMS_Type_Value_t *val = drms_keyword_getvalue(key);
   if (val && drms_keyword_gettype(key) == DRMS_TYPE_TIME)
   {
      /* unit (time zone) must be ISO - if not, don't export it */
      char unitbuf[DRMS_MAXUNITLEN];
      int fitsrwRet = 0;
           
      snprintf(unitbuf, sizeof(unitbuf), "%s", key->info->unit);
      strtoupper(unitbuf);

      /* If this is the DATE keyword, only export if the value isn't "missing". */
      if (strcmp(kFITSRESERVED[kRKW_date], key->info->name) != 0 || !drms_ismissing_time(val->time_val))
      {
         if (strcmp(unitbuf, "ISO") == 0)
         {
            char tbuf[1024];
            drms_keyword_snprintfval(key, tbuf, sizeof(tbuf));

            /* The time string returned should end with the char 'Z' - remove it since 
             * the FITS standard doesn't like it. */
            if (tbuf[strlen(tbuf) - 1] == 'Z')
            {
               tbuf[strlen(tbuf) - 1] = '\0';
            }

            if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_key((CFITSIO_KEYWORD**)fitskeys, 
                                                                   (char *)nameout, 
                                                                   kFITSRW_Type_String, 
                                                                   NULL,
                                                                   (void *)tbuf,
                                                                   NULL)))
            {
               fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
               err = 2;
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

/* Process a COMMENT or HISTORY FITS keyword. The source DRMS keyword may contain a multi-line string, 
 * which must be split into new strings at newlines. And each resulting string must be at most 70 characters
 * in length. */
int CommHndlr(void *keyin, void **fitskeys, void *nameout)
{
   int err = 0;
   char *sbuf = NULL;
   int rangeout = 0;
   DRMS_Keyword_t *key = (DRMS_Keyword_t *)keyin;
   const DRMS_Type_Value_t *val = drms_keyword_getvalue(key);

   if (val && drms_keyword_gettype(key) == DRMS_TYPE_STRING)
   {
      char *tmp = strdup(val->string_val);

      if (tmp)
      {
         char *pc = tmp;
         char *pout = NULL;
         int nelem = 0;
         int fitsrwRet = 0;

         sbuf = malloc(sizeof(char) * strlen(tmp) + 1);
         pout = sbuf;

         while (*pc)
         {
            if (*pc == '\n')
            {
               /* Split */
               *pout = '\0';

               if (*sbuf)
               {
                  if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_key((CFITSIO_KEYWORD**)fitskeys, 
                                                                         (char *)nameout, 
                                                                         kFITSRW_Type_String, 
                                                                         NULL,
                                                                         (void *)sbuf,
                                                                         NULL)))
                  {
                     fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
                     err = 2;
                  }
               }

               nelem = 0;
               pout = sbuf;
            }
            else if (*pc >= 0x20 && *pc <= 0x7E || *pc >= 0xA0 && *pc <= 0xFF)
            {
               *pout = *pc;
               pout++;
               nelem++;
            }
            else
            {
               fprintf(stderr, "Bad char '%x' at offset %d in comment '%s'.\n", *pc, (int)(pc - tmp), tmp);
               rangeout = 1;
            }
         
            pc++;
         }

         /* sbuf might have text not yet converted into a COMMENT FITS key. */
         if (nelem > 0)
         {
            *pout = '\0';
            if (*sbuf)
            {
               if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_key((CFITSIO_KEYWORD**)fitskeys, 
                                                                      (char *)nameout, 
                                                                      kFITSRW_Type_String, 
                                                                      NULL,
                                                                      (void *)sbuf,
                                                                      NULL)))
               {
                  fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
                  err = 2;
               }
            }

            nelem = 0;
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

   if (rangeout)
   {
      fprintf(stderr, "At least one character encoding in DRMS keyword '%s' not a member of Latin-1.\n", key->info->name);
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
	 if (*pC >= 65 && *pC <= 90)
	 {
	    fitsName[nch] = *pC;
	    nch++; 
	 }
	 else if (*pC >= 97 && *pC <= 122)
	 {
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

/* keys may be NULL, in which case no extra keywords are placed into the FITS file. */
static int ExportFITS(DRMS_Env_t *env,
                      DRMS_Array_t *arrout, 
                      const char *fileout, 
                      const char *cparms, 
                      CFITSIO_KEYWORD *fitskeys)
{
   int stat = DRMS_SUCCESS;

   if (arrout)
   {
      /* Need to manually add required keywords that don't exist in the record's 
       * DRMS keywords. */
      CFITSIO_IMAGE_INFO imginfo;

      /* To deal with CFITSIO not handling signed bytes, must convert DRMS_TYPE_CHAR to 
       * DRMS_TYPE_SHORT */
      if (arrout->type == DRMS_TYPE_CHAR)
      {
	drms_array_convert_inplace(DRMS_TYPE_SHORT, 0, 1, arrout);
	fprintf(stdout, "FITS doesn't support signed char, converting to signed short.\n");
      }
      
      if (!drms_fitsrw_SetImageInfo(arrout, &imginfo))
      {
         /* Not sure if data need to be scaled, or if the original blank value
          * should be resurrected. */
         if (arrout->type == DRMS_TYPE_STRING)
         {
            fprintf(stderr, "Can't save string data into a fits file.\n");
            stat = DRMS_ERROR_EXPORT;
         }
         else
         {
            if (fitsrw_write(env->verbose, fileout, &imginfo, arrout->data, cparms, fitskeys))
            {
               fprintf(stderr, "Can't write fits file '%s'.\n", fileout);
               stat = DRMS_ERROR_EXPORT;
            }
         }
      }
      else
      {
         fprintf(stderr, "Data array being exported is invalid.\n");
         stat = DRMS_ERROR_EXPORT;
      }
   }
   else
   {
      stat = DRMS_ERROR_INVALIDDATA;
   }

   return stat;
}

static int DRMSKeyValToFITSKeyVal(DRMS_Keyword_t *key, 
                                  char *fitstype, 
                                  char **format,
                                  void **fitsval)
{
   int err = 0;
   DRMS_Type_Value_t *valin = &key->value;
   void *res = NULL;
   int status = DRMS_SUCCESS;
   FE_Keyword_ExtType_t casttype = fitsexport_keyword_getcast(key);

   if (valin && fitstype && format)
   {
      *format = strdup(key->info->format);

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
                 *fitstype = kFITSRW_Type_Integer;
                 break;
               case kFE_Keyword_ExtType_Float:
                 res = malloc(sizeof(double));
                 *(double *)res = drms2double(key->info->type, valin, &status);
                 *fitstype = kFITSRW_Type_Float;
                 break;
               case kFE_Keyword_ExtType_String:
               {
                  char tbuf[1024];
                  drms_keyword_snprintfval(key, tbuf, sizeof(tbuf));
                  res = (void *)strdup(tbuf);
                  *fitstype = kFITSRW_Type_String;
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
                 *fitstype = kFITSRW_Type_Logical;
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
              *fitstype = 'I';
              break;
            case DRMS_TYPE_SHORT:
              res = malloc(sizeof(long long)); 
              *(long long *)res = (long long)(valin->short_val);
              *fitstype = 'I';
              break;
            case DRMS_TYPE_INT:
              res = malloc(sizeof(long long)); 
              *(long long *)res = (long long)(valin->int_val);
              *fitstype = 'I';
              break;
            case DRMS_TYPE_LONGLONG:
              res = malloc(sizeof(long long)); 
              *(long long *)res = valin->longlong_val;
              *fitstype = 'I';
              break;
            case DRMS_TYPE_FLOAT:
              res = malloc(sizeof(double));
              *(double *)res = (double)(valin->float_val);
              *fitstype = 'F';
              break;
            case DRMS_TYPE_DOUBLE:
              res = malloc(sizeof(double));
              *(double *)res = valin->double_val;
              *fitstype = 'F';
              break;
            case DRMS_TYPE_TIME:
            {
               char tbuf[1024];
               drms_keyword_snprintfval(key, tbuf, sizeof(tbuf));
               res = (void *)strdup(tbuf);
               *fitstype = 'C';
            }
            break;
            case DRMS_TYPE_STRING:
              res = (void *)strdup(valin->string_val);
              *fitstype = 'C';
              break;
            default:
              fprintf(stderr, "Unsupported DRMS type '%d'.\n", (int)key->info->type);
              err = 1;
              break;
         }
      }  
   }
   else
   {
      fprintf(stderr, "DRMSKeyValToFITSKeyVal() - Invalid argument.\n");
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
int fitsexport_mapexport_tofile(DRMS_Segment_t *seg, 
                                const char *cparms, 
                                const char *clname, 
                                const char *mapfile,
                                const char *fileout,
                                char **actualfname,
                                unsigned long long *expsize)
{
   int status = DRMS_SUCCESS;

   CFITSIO_KEYWORD *fitskeys = NULL;
   char filename[DRMS_MAXPATHLEN]; 
   struct stat stbuf;
   DRMS_Segment_t *tgtseg = NULL;
    char realfileout[DRMS_MAXPATHLEN];
    struct stat filestat;

   if (seg->info->islink)
   {
      if ((tgtseg = drms_segment_lookup(seg->record, seg->info->name)) == NULL)
      {
         fprintf(stderr, "Unable to locate target segment %s.\n", seg->info->name);
         status = DRMS_ERROR_INVALIDFILE;
      }
      else
      {
         drms_segment_filename(tgtseg, filename);
      }
   }
   else
   {
      drms_segment_filename(seg, filename); /* full, absolute path to segment file */
   }

   if (status == DRMS_SUCCESS)
   {
      if (*filename == '\0' || stat(filename, &stbuf))
      {
         /* file filename is missing */
         snprintf(seg->filename, sizeof(seg->filename), "%s", filename); /* so caller has access to file name */
         status = DRMS_ERROR_INVALIDFILE;
      }
      else
      {
         int swval;

         /* Must be source segment if the segment is a linked segment. */
         fitskeys = fitsexport_mapkeys(seg, clname, mapfile, &status);

         if (tgtseg)
         {
            swval = tgtseg->info->protocol;
         }
         else
         {
            swval = seg->info->protocol;
         }
          
          snprintf(realfileout, sizeof(realfileout), "%s", fileout);

         switch (swval)
         {
            case DRMS_TAS:
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
                     strtolower(dup);
                     if (lenstr > 0 && dup[lenstr - 1] == 's' && dup[lenstr - 2] == 'a' && dup[lenstr - 3] == 't' && dup[lenstr - 4] == '.')
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
                 DRMS_Array_t *arrout = NULL;

                 if (seg->info->islink)
                 {
                    arrout = drms_segment_read(tgtseg, DRMS_TYPE_RAW, &status);
                 }
                 else
                 {
                    arrout = drms_segment_read(seg, DRMS_TYPE_RAW, &status);
                 }

                 if (arrout)
                 {
                    status = ExportFITS(seg->record->env, arrout, realfileout, cparms ? cparms : (seg->info->islink ? tgtseg->cparms : seg->cparms), fitskeys);
                    drms_free_array(arrout);	     
                 }
              }
              break;
            case DRMS_GENERIC:
              {
                 int ioerr;

                 /* Simply copy the file from the segment's data-file path
                  * to fileout, no keywords to worry about. */
                 if (CopyFile(filename, realfileout, &ioerr) != stbuf.st_size)
                 {
                    fprintf(stderr, "Unable to export file '%s' to '%s'.\n", filename, realfileout);
                    status = DRMS_ERROR_FILECOPY;
                 }
              }
              break;
            default:
              fprintf(stderr, 
                      "Data export does not support data segment protocol '%s'.\n", 
                      drms_prot2str(seg->info->protocol));
         }
          
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
   
         cfitsio_free_keys(&fitskeys);
      }
   }

   return status;
}

/* Map keys that are specific to a segment to fits keywords.  User must free. 
 * Follows keyword links and ensures that per-segment keywords are relevant
 * to this seg's keywords. */

/* Input seg must be the src seg, not the target seg, if the input seg is a linked segment. */
CFITSIO_KEYWORD *fitsexport_mapkeys(DRMS_Segment_t *seg, 
                                    const char *clname, 
                                    const char *mapfile, 
                                    int *status)
{
   CFITSIO_KEYWORD *fitskeys = NULL;
   HIterator_t *last = NULL;
   int statint = DRMS_SUCCESS;
   DRMS_Keyword_t *key = NULL;
   const char *keyname = NULL;
   char segnum[4];
   DRMS_Record_t *recin = seg->record;
   Exputl_KeyMap_t *map = NULL;
   FILE *fptr = NULL;
   int fitsrwRet = 0;

   while ((key = drms_record_nextkey(recin, &last, 0)) != NULL)
   {
      keyname = drms_keyword_getname(key);

      if (!drms_keyword_getimplicit(key))
      {
         if (drms_keyword_getperseg(key))
         {
            snprintf(segnum, sizeof(segnum), "%03d", seg->info->segnum);

            /* Ensure that this keyword is relevant to this segment. */
            if (!strstr(keyname, segnum))
            {
               continue;
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

         if (fitsexport_mapexportkey(key, clname, map, &fitskeys))
         {
            fprintf(stderr, "Couldn't export keyword '%s'.\n", keyname);
            statint = DRMS_ERROR_EXPORT;
         }
      }
   }

   /* Export recnum to facilitate the association between an exported FITS file and its record of origin. */
   long long recnum = seg->record->recnum;
   if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_key(&fitskeys, 
                                                          kFERecnum, 
                                                          kFITSRW_Type_Integer, 
                                                          NULL,
                                                          (void *)&recnum,
                                                          kFERecnumFormat)))
   {
      fprintf(stderr, "FITSRW returned '%d'.\n", fitsrwRet);
      statint = DRMS_ERROR_FITSRW;
   }   

   if (map)
   {
      exputl_keymap_destroy(&map);
   }

   if (last)
   {
      hiter_destroy(&last);
   }

   if (status)
   {
      *status = statint;
   }

   return fitskeys;
}

/* Keyword mapping during export */
int fitsexport_exportkey(DRMS_Keyword_t *key, CFITSIO_KEYWORD **fitskeys)
{
   return fitsexport_mapexportkey(key, NULL, NULL, fitskeys);
}

/* For linked series, key is the source keyword (not the target). */
int fitsexport_mapexportkey(DRMS_Keyword_t *key,
                            const char *clname, 
                            Exputl_KeyMap_t *map,
                            CFITSIO_KEYWORD **fitskeys)
{
   int stat = DRMS_SUCCESS;

   if (key && fitskeys)
   {
      char nameout[16];

      if (fitsexport_getmappedextkeyname(key, clname, map, nameout, sizeof(nameout)))
      {
	 int fitsrwRet = 0;
         char fitskwtype = '\0';
         void *fitskwval = NULL;
         char *format = NULL;
         DRMS_Keyword_t *keywval = NULL;
         int rv = 0;

         /* follow link if key is a linked keyword, otherwise, use key. */
         keywval = drms_keyword_lookup(key->record, key->info->name, 1);

         /* It may be the case that the linked record is not found - the dependency 
          * could be broken if somebody deleted the target record, for example. */
         if (keywval)
         {
            FE_ReservedKeys_t *ikey = NULL;

            if (gReservedFits && 
                (ikey = (FE_ReservedKeys_t *)hcon_lookup_lower(gReservedFits, keywval->info->name)))
            {
               if (ExportHandlers[*ikey])
               {
                  /* A handler exists for this reserved keyword - okay to export it. */
                  rv = (*(ExportHandlers[*ikey]))(keywval, (void **)fitskeys, (void *)nameout);
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
               if ((rv = DRMSKeyValToFITSKeyVal(keywval, &fitskwtype, &format, &fitskwval)) == 0)
               {
                  if (CFITSIO_SUCCESS != (fitsrwRet = cfitsio_append_key(fitskeys, 
                                                                         nameout, 
                                                                         fitskwtype, 
                                                                         NULL,
                                                                         fitskwval,
                                                                         format)))
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
   return fitsexport_getmappedextkeyname(key, NULL, NULL, nameOut, size);
}

/* Same as above, but try a KeyMap first, then a KeyMapClass */
int fitsexport_getmappedextkeyname(DRMS_Keyword_t *key, 
                                   const char *class, 
                                   Exputl_KeyMap_t *map,
                                   char *nameOut,
                                   int size)
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

	    if (len > 2 &&
		pFitsName[0] == '[' &&
		pFitsName[len - 1] == ']')
	    {
	       if (len - 2 < size)
	       {
		  pot = (char *)malloc(sizeof(char) * size);
		  if (pot)
		  {
		     memcpy(pot, pFitsName + 1, len - 2);
		     pot[len - 2] = '\0';

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
	 pot = (char *)malloc(sizeof(char) * size);
	 if (pot)
	 {
	    if (GenerateFitsKeyName(key->info->name, pot, size))
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

static int FITSKeyValToDRMSKeyVal(CFITSIO_KEYWORD *fitskey, 
                                  DRMS_Type_t *type, 
                                  DRMS_Type_Value_t *value,
                                  FE_Keyword_ExtType_t *casttype,
                                  char **format)
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

int fitsexport_importkey(CFITSIO_KEYWORD *fitskey, HContainer_t *keys)
{
   return fitsexport_mapimportkey(fitskey, NULL, NULL, keys);
}

int fitsexport_mapimportkey(CFITSIO_KEYWORD *fitskey,
                            const char *clname, 
                            const char *mapfile,
                            HContainer_t *keys)
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
                  else
                  {
                     fprintf(stderr, "Keyword '%s' already exists; the DRMS value is the value of the first instance .\n", nameout);
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
