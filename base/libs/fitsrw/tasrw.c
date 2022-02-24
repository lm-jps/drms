#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "cfitsio.h"
#include "tasrw.h"
#include "fitsio.h"
#include "hcontainer.h"
#include "list.h"
#include "timer.h"
#include "xassert.h"

/* both of these use the struct cfitsio_image_info to hold information */
typedef struct cfitsio_image_info TASRW_ImageInfo_struct;
//typedef struct cfitsio_image_info TASRW_IMAGE_INFO;
typedef struct cfitsio_image_info TASRW_FilePtrInfo_struct;
//typedef struct cfitsio_image_info TASRW_FILE_PTR_INFO;

// #define DEBUG

#ifdef DEBUG
#include "timer.h"
#endif

/* Our current set up is 1024 maximum open files per process (/usr/include/linux/limits.h);
 * The maximum number of fitsfiles allowed to be open per CFITSIO lib is 300. */
#define MAXFFILES 300

#ifdef SKIPWS
#undef SKIPWS
#endif

#ifdef ISBLANK
#undef ISBLANK
#endif

#define ISBLANK(c) (c==' ' || c=='\t' || c=='\r')
#define SKIPWS(p) {while(*p && ISBLANK(*p)) { ++p; }}

HContainer_t *gFFiles = NULL;
HContainer_t *gFFPtrInfo = NULL;

static int IsWriteable(const char *fhash)
{
   char *pmode = strrchr(fhash, ':');
   int writeable = -1;

   if (!pmode)
   {
      fprintf(stderr, "Invalid file hash '%s'.\n", fhash);
   }
   else
   {
      if (*(pmode + 1) == 'r')
      {
         writeable = 0;
      }
      else if (*(pmode + 1) == 'w')
      {
         writeable = 1;
      }
   }

   return writeable;
}

/* return 1 if not cached */
static int fitsrw_getfpinfo(fitsfile *fptr, TASRW_FILE_PTR_INFO *info)
{
   int err = 1;
   TASRW_FILE_PTR_INFO *pfpinfo = NULL;
   char fileinfokey[64];

   if (fptr && info && gFFPtrInfo)
   {
      snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
      err = ((pfpinfo = (TASRW_FILE_PTR_INFO *)hcon_lookup(gFFPtrInfo, fileinfokey)) == NULL);
   }

   if (!err)
   {
      if (pfpinfo)
      {
         *info = *pfpinfo;
      }
   }

   return err;
}

static int fitsrw_setfpinfo(fitsfile *fptr, TASRW_FILE_PTR_INFO *info)
{
   int err = 1;
   TASRW_FILE_PTR_INFO *pfpinfo = NULL;
   char fileinfokey[64];

   if (fptr && info)
   {
      snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
      err = ((pfpinfo = (TASRW_FILE_PTR_INFO *)hcon_lookup(gFFPtrInfo, fileinfokey)) == NULL);
   }

   if (!err)
   {
      if (pfpinfo)
      {
         *pfpinfo = *info;
      }
   }

   return err;
}

/* fitsfiles can be opened either READWRITE or READONLY. If a request for a READONLY pointer
 * is made, and the fitsfile has already been opened, then regardless of the fitsfile write mode
 * it is okay to return the opened pointer. But, if the request is for a READWRITE pointer
 * and a READONLY fitsfile pointer exists, then the READONLY pointer must be closed,
 * and the file must be reopened with READWRITE write mode.
 *
 * The hash key to the gFFiles is <filename> ':' ('r' | 'w') where r implies readonly and w implies
 * readwrite.
 *
 * fileCreated is set to 1 if fits_create_file() was called successfully.
 */
TASRW_FilePtr_t fitsrw_getfptr_internal(int verbose, const char *filename, int writeable, int verchksum, int *status, int *fileCreated)
{
   fitsfile *fptr = NULL;
   fitsfile **pfptr = NULL;
   char filehashkey[PATH_MAX + 2];
   char tmpfilehashkey[PATH_MAX + 2];
   char fileinfokey[64];
   int stat = CFITSIO_SUCCESS; /* a FITSRW error code is returned from this function */
   int fiostat = 0;
   int datachk;
   int hduchk;
   int newfile = 0;
   char cfitsiostat[FLEN_STATUS];
   int exit_code = CFITSIO_SUCCESS;

   if (fileCreated)
   {
        *fileCreated = 0;
   }

   if (!gFFiles)
   {
      gFFiles = hcon_create(sizeof(fitsfile *), sizeof(filehashkey), NULL, NULL, NULL, NULL, 0);
   }

   if (!gFFPtrInfo)
   {
      gFFPtrInfo = hcon_create(sizeof(TASRW_FILE_PTR_INFO), sizeof(fileinfokey), NULL, NULL, NULL, NULL, 0);
   }

   snprintf(filehashkey, sizeof(filehashkey), "%s:%s", filename, writeable ? "w" : "r");

   /* The fits file filename might already be open - if so, use that fitsfile *, otherwise open it. */
   pfptr = (fitsfile **)hcon_lookup(gFFiles, filehashkey);
   if (pfptr && !*pfptr)
   {
       hcon_remove(gFFiles, filehashkey);
       pfptr = NULL;
   }

    if (pfptr != NULL)
    {
        fptr = *pfptr;
    }
    else if (!writeable)
    {
        snprintf(tmpfilehashkey, sizeof(tmpfilehashkey), "%s:w", filename);
        pfptr = (fitsfile **)hcon_lookup(gFFiles, tmpfilehashkey);
        if (pfptr && !*pfptr)
        {
            hcon_remove(gFFiles, tmpfilehashkey);
            pfptr = NULL;
        }

        if (pfptr != NULL)
        {
            /* caller requested readonly fitsfile, but the writeable one exists - just return that one */
            fptr = *pfptr;
        }
    }

    if (!fptr)
    {
        if (writeable)
        {
            snprintf(tmpfilehashkey, sizeof(tmpfilehashkey), "%s:r", filename);
            if ((pfptr = (fitsfile **)hcon_lookup(gFFiles, tmpfilehashkey)) != NULL)
            {
                /* caller requested writeable fitsfile, but the readonly one exists - close
                 * readonly one and open a writeable one. */

                if (*pfptr)
                {
                   /* remove fileptr info structure */
                   snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)*pfptr);
                   hcon_remove(gFFPtrInfo, fileinfokey);

                   if (verbose)
                   {
                      fprintf(stdout, "Closing fits file '%s'.\n", filename);
                      PushTimer();
                   }

                   /* we are closing a read-only file here. */
                    fiostat = 0;
                   fits_close_file(*pfptr, &fiostat);

                   if (fiostat != 0)
                   {
                       fprintf(stderr, "FITSIO error: %d.\n", fiostat);
                       fprintf(stdout, "Unable to close fits file '%s'.\n", filename);
                       perror("fitsrw_getfptr_internal() system error");
                       stat = CFITSIO_ERROR_FILE_IO;
                   }

                   if (verbose)
                   {
                      fprintf(stdout, "Time to close fitsfile '%s'= %f sec.\n", filename, PopTimer());
                   }
                }

                hcon_remove(gFFiles, tmpfilehashkey);

                pfptr = NULL;
          }
        }

        if (verbose)
        {
            PushTimer();
        }

        if (!stat)
        {
            fiostat = 0;
            if (fits_open_image(&fptr, filename, writeable ? READWRITE : READONLY, &fiostat))
            {
                /* Couldn't open file - doesn't exist. */
                if (writeable)
                {
                    /* OK - this is a new file, so just create it. */
                    fiostat = 0;

                    /* create new file, but only if writing */
                    newfile = 1;
                    if (fits_create_file(&fptr, filename, &fiostat))
                    {
                        /* Couldn't create new file. */
                        fprintf(stderr, "FITSIO error: %d; ", fiostat);
                        fits_report_error(stderr, fiostat);
                        fprintf(stderr, "\n");
                        stat = CFITSIO_ERROR_FILE_IO;
                    }
                    else if (fileCreated)
                    {
                        *fileCreated = 1;
                    }
                }
                else
                {
                    stat = CFITSIO_ERROR_FILE_DOESNT_EXIST;
                }
            }
        }

        if (verbose)
        {
            fprintf(stdout, "Time to open fitsfile '%s' = %f sec.\n", filehashkey, PopTimer());
        }

        if (!stat)
        {
            if (verbose)
            {
                PushTimer();
            }

            /* Check checksum, if it exists */
            if (verchksum)
            {
                fiostat = 0;
                if (fits_verify_chksum(fptr, &datachk, &hduchk, &fiostat))
                {
                    fprintf(stderr, "FITSIO error: %d.\n", fiostat);
                    fprintf(stderr, "Unable to verify fits-file checksum.\n");
                    stat = CFITSIO_ERROR_LIBRARY;
                }
                else
                {
                    if (datachk == -1 || hduchk == -1)
                    {
                        /* Both checksums were present, and at least one of them failed. */
                        fprintf(stderr, "Failed to verify data and/or HDU checksum (file corrupted).\n");
                        stat = CFITSIO_ERROR_FILE_IO;
                    }
                }
            }

            if (verbose)
            {
            fprintf(stdout, "Time to verify checksum = %f sec.\n", PopTimer());
            }
        }

        exit_code = stat;

        if (!stat && gFFiles->num_total >= MAXFFILES - 1)
        {
            /* Must close some open files */
            int ifile = 0;
            HIterator_t *hit = hiter_create(gFFiles);
            LinkedList_t *llist = list_llcreate(sizeof(char *), NULL);
            const char *fhkey = NULL;
            ListNode_t *node = NULL;
            char *onefile = NULL;
            TASRW_FILE_PTR_INFO finfo;

            if (hit)
            {
                while (ifile < MAXFFILES / 2)
                {
                    stat = 0; /* keep on purging files, even if we fail on one; report the first error encountered */
                    pfptr = (fitsfile **)hiter_extgetnext(hit, &fhkey);

                    if (pfptr)
                    {
                        if (*pfptr)
                        {
                            if (IsWriteable(fhkey))
                            {
                                /* Before removing the file info, get the dirty flag value and the value of NAXISn. */
                                stat = fitsrw_getfpinfo(*pfptr, &finfo);

                                if (stat)
                                {
                                    /* we need info in gFFPtrInfo so we can resize the image */
                                    fprintf(stderr, "missing file info for fits file '%s'\n", fhkey);
                                    stat = CFITSIO_ERROR_CANT_GET_FILEINFO;
                                }
                                else
                                {
                                    /* file is present in both gFFiles and gFFPtrInfo */
                                    if (finfo.bitfield & kInfoPresent_Dirt)
                                    {
                                        /* If this is a writable fits file AND the dirty flag is set (which means that
                                        * since the file was first created, the NAXISn length has changed due to
                                        * slice writing), then update the NAXISn keyword value before closing the
                                        * fits file. */

                                        /* It is not sufficient to simply change the NAXISn keyword value. Although this WILL
                                        * make FITSIO think the image is smaller, the actual image data does not get removed
                                        * from the FITS file. So, we ALSO have to call fits_resize_imgll().
                                        */
                                        int dimlen;
                                        int imgType;
                                        long long *axes = NULL;
                                        int iaxis;

                                        axes = calloc(finfo.naxis, sizeof(long long));

                                        if (!axes)
                                        {
                                            stat = CFITSIO_ERROR_OUT_OF_MEMORY;
                                        }
                                        else
                                        {
                                            for (iaxis = 0; iaxis < finfo.naxis; iaxis++)
                                            {
                                                axes[iaxis] = finfo.naxes[iaxis];
                                            }

                                            switch(finfo.bitpix)
                                            {
                                                case(BYTE_IMG): imgType = SBYTE_IMG; break;
                                                case(SHORT_IMG): imgType = SHORT_IMG; break;
                                                case(LONG_IMG): imgType = LONG_IMG; break;
                                                case(LONGLONG_IMG): imgType = LONGLONG_IMG; break;
                                                case(FLOAT_IMG): imgType = FLOAT_IMG; break;
                                                case(DOUBLE_IMG): imgType = DOUBLE_IMG; break;
                                            }

                                            fiostat = 0;
                                            fits_resize_imgll(*pfptr, imgType, finfo.naxis, axes, &fiostat);

                                            if (fiostat)
                                            {
                                                fprintf(stderr, "FITSIO error: %d\n", fiostat);
                                                fprintf(stderr, "unable to resize image in fits file %s\n", filename);
                                                stat = CFITSIO_ERROR_LIBRARY;
                                            }

                                            /* no need to manually modify NAXISn (the value of the slice dimension);
                                            * fits_resize_imgll() does this
                                            */

                                            free(axes);
                                            axes = NULL;
                                        }
                                    }
                                }

                                if (!stat)
                                {
                                    /* we are closing a file, so write its checksum */
                                    if (verbose)
                                    {
                                        PushTimer();
                                    }

                                    fiostat = 0;
                                    fits_write_chksum(*pfptr, &fiostat);

                                    if (fiostat)
                                    {
                                        fits_get_errstatus(fiostat, cfitsiostat);
                                        fprintf(stderr, "Purging cache: error calculating and writing checksum for fitsfile '%s'.\n", fhkey);
                                        fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                                        stat = CFITSIO_ERROR_LIBRARY;
                                    }

                                    if (verbose)
                                    {
                                        fprintf(stdout, "Time to write checksum on fitsfile '%s' = %f sec.\n", fhkey, PopTimer());
                                    }
                                }
                            }

                            /* remove fileptr from gFFPtrInfo; this needs to be done regardless of write-status  */
                            snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)*pfptr);
                            hcon_remove(gFFPtrInfo, fileinfokey);

                            if (verbose)
                            {
                                PushTimer();
                            }

                            fiostat = 0;
                            fits_close_file(*pfptr, &fiostat);

                            if (fiostat == 0)
                            {
                                if (verbose)
                                {
                                    fprintf(stderr, "Closing fits file '%s'.\n", fhkey);
                                }
                            }
                            else
                            {
                                fits_get_errstatus(fiostat, cfitsiostat);
                                fprintf(stderr, "Purging cache: error closing fitsfile '%s'.\n", fhkey);
                                fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                                perror("fitsrw_getfptr_internal() system error");

                                if (!stat)
                                {
                                    stat = CFITSIO_ERROR_FILE_IO;
                                }
                            }

                            if (verbose)
                            {
                                fprintf(stdout, "Time to close fitsfile '%s' = %f sec.\n", fhkey, PopTimer());
                            }
                        }

                        /* Save the filehashkey for each file being removed from the cache -
                        * must remove from gFFiles AFTER exiting the loop that is iterating
                        * over gFFiles. */
                        list_llinserttail(llist, (void *)&fhkey);
                        ifile++;
                    }
                    else
                    {
                        break;
                    }

                    if (stat && exit_code == CFITSIO_SUCCESS)
                    {
                        exit_code = stat;
                    }

                } /* loop over files being purged */

                hiter_destroy(&hit);
            }

            /* free all the file pointers saved in gFFiles */
            list_llreset(llist);
            while ((node = list_llnext(llist)) != NULL)
            {
                onefile = *((char **)node->data);
                hcon_remove(gFFiles, onefile);
            }

            list_llfree(&llist);
        }

        if (!exit_code)
        {
            /* We just opened a new fits-file. Cache it. */
            stat = 0;
            TASRW_IMAGE_INFO *imginfo = NULL;

            /* Read essential keyword information and cache that for later use */
            if (newfile)
            {
                imginfo = (TASRW_IMAGE_INFO *)malloc(sizeof(TASRW_IMAGE_INFO));
                memset(imginfo, 0, sizeof(TASRW_IMAGE_INFO));
            }
            else
            {
                /* the keylist returned is not used since this is an internal SUMS file, and the up-to-date keyword
                * info resides in the DRMS DB
                */
                if (fitsrw_read_keylist_and_image_info((FITSRW_fhandle)fptr, NULL, &imginfo) != CFITSIO_SUCCESS)
                {
                    fprintf(stderr, "Unable to get file information for %s.\n", filehashkey);
                    stat = CFITSIO_ERROR_CANT_GET_FILEINFO;
                }
            }

            if (!stat)
            {
                snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
                /* Stash away filehashkey in imginfo->fhash; used when closing fitsfile pointers */
                snprintf(imginfo->fhash, sizeof(imginfo->fhash), "%s", filehashkey);
                hcon_insert(gFFPtrInfo, fileinfokey, imginfo);

                hcon_insert(gFFiles, filehashkey, &fptr);
            }

            if (imginfo)
            {
                cfitsio_free_these(&imginfo, NULL, NULL);
            }

            if (verbose)
            {
                fprintf(stdout, "Number fitsfiles opened: %d\n", gFFiles->num_total);
            }

            exit_code = stat;
        }
    }

    if (status)
    {
        *status = exit_code;
    }

    return (TASRW_FilePtr_t)fptr;
}

TASRW_FilePtr_t fitsrw_getfptr(int verbose, const char *filename, int writeable, int *status, int *fileCreated)
{
   return fitsrw_getfptr_internal(verbose, filename, writeable, 1, status, fileCreated);
}

TASRW_FilePtr_t fitsrw_getfptr_nochksum(int verbose, const char *filename, int writeable, int *status, int *fileCreated)
{
   return fitsrw_getfptr_internal(verbose, filename, writeable, 0, status, fileCreated);
}

int fitsrw_readslice(int verbose, const char *filename, int *fpixel, int *lpixel, TASRW_ImageInfo_t* image_info_out, void** image)
{
   fitsfile *fptr=NULL;
    int error_code = CFITSIO_SUCCESS;
    int status; /* FITSIO error codes. */
    int istat; /* Status for other calls. */
   long increments[CFITSIO_MAX_DIM]; /* ffgsv requires long type, although this is dangerous
                                      * as long is a different number of bytes on different
                                      * machines.
                                      */
   long long npixels;
   int bytepix, data_type;
   long lfpixel[CFITSIO_MAX_DIM];
   long llpixel[CFITSIO_MAX_DIM];
   int idim = 0;
   void *pixels = NULL;
   char cfitsiostat[FLEN_STATUS];
   TASRW_FILE_PTR_INFO fpinfo;
   TASRW_IMAGE_INFO *image_info = NULL;

   long i;

   int fileCreated = 0;

   if (!image_info_out)
   {
      fprintf(stderr, "Invalid image_info argument.\n");
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }

    if (*((TASRW_IMAGE_INFO **)image_info_out))
    {
        cfitsio_free_image_info((CFITSIO_IMAGE_INFO **)image_info);
    }

   image_info = (TASRW_IMAGE_INFO *)malloc(sizeof(TASRW_IMAGE_INFO));

   if (image_info == NULL)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }

   memset((void*)(image_info), 0, sizeof(TASRW_IMAGE_INFO));

   status = 0; // first thing!
    istat = 0;

   fptr = (fitsfile *)fitsrw_getfptr_nochksum(verbose, filename, 0, &istat, &fileCreated);
   XASSERT(!fileCreated);

    /* The call fitsrw_getfptr_nochksum() is not clean - it looks like it might return
     * a  fptr, but also return istat != 0. fitsrw_getfptr_nochksum() needs to be cleaned
     * up so that if istat != 0, the fptr == 0. */
   if (!fptr || istat)
   {
       error_code = CFITSIO_ERROR_FILE_IO;
       goto error_exit;
   }

   /* Fetch img parameter info - should have been cached when the file was originally opened */
   if (fitsrw_getfpinfo(fptr, &fpinfo))
   {
       fprintf(stderr, "Unable to get fitsfile image parameters.\n");
       error_code = CFITSIO_ERROR_FILE_IO;
       goto error_exit;
   }

   *image_info = fpinfo;
   *image_info_out = (TASRW_ImageInfo_t)image_info;

   if((image_info)->naxis == 0)
   {
      error_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   switch((image_info)->bitpix)
   {
      case(BYTE_IMG):    data_type = TBYTE; break;
      case(SHORT_IMG):   data_type = TSHORT; break;
      case(LONG_IMG):    data_type = TINT; break;
      case(LONGLONG_IMG):data_type = TLONGLONG; break;
      case(FLOAT_IMG):   data_type = TFLOAT; break;
      case(DOUBLE_IMG):  data_type = TDOUBLE; break;
   }

   bytepix = abs((image_info)->bitpix)/8;

   npixels = 1;
   for(i=0;i<(image_info)->naxis;i++)
   {
      /* override default axis lengths in image_info - a subset will have potentially
       * shorter lengths */
      (image_info)->naxes[i] = (lpixel[i]-fpixel[i]+1);
      npixels *= (image_info)->naxes[i];
      increments[i]=1; // no skipping
   }

   pixels = calloc(npixels, bytepix);
   if(!pixels)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }

   //Read raw data
   fits_set_bscale(fptr, 1.0, 0.0, &status);
   fits_set_imgnull(fptr, 0, &status);
   if(status)
   {
       error_code= CFITSIO_ERROR_LIBRARY;
       goto error_exit;
   }

   //Read stripe, slice, block
   int anynul;
   /* ffgsv requires long type, although this is dangerous
    * as long is a different number of bytes on different
    * machines. */
   memset(lfpixel, 0, sizeof(lfpixel));
   memset(llpixel, 0, sizeof(llpixel));

   for (idim = 0; idim < (image_info)->naxis; idim++)
   {
      /* FITS uses column-major order, and C uses row-major order for its arrays.  So,
       * the C array equivalent of a FITS-file's data array would be arr[NAXISn][NAXISn-1]...
       * [NAXIS1].  BUT, to be confusing, when specifying a pixel to FITSIO routines, don't do
       * this.  For some unknown reason, you specify the indicies of a pixel in the opposite order -
       * lfpixel[0] = NAXIS1 value, fpixel[1] = NAXIS2 value, ..., fpixel[n-1] = NAXISn value.
       * NOTE that the array indices of the pixel are 0-based, but the actual NAXISn values are
       * 1-based.  Again, confusing.
       */

      /* CFITSIO uses 1-based array indices, not 0-based */
      lfpixel[idim] = (long)fpixel[idim] + 1;
      llpixel[idim] = (long)lpixel[idim] + 1;
   }

   if(fits_read_subset(fptr, data_type, lfpixel, llpixel, increments, NULL, pixels, &anynul,
		       &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

    /* If data_type is TBYTE, then data are unsigned. We need to convert to signed data,
     * which is the only type of byte data supported in DRMS. But we don't want
     * to apply bzero/bscale either, because that gets applied in a calling function.
     * So what we'll do is to apply bzero/bscale here, then set bzero and bscale to
     * 0 and 1 so that the calling function doesn't re-apply the bzero/bscale values. */

    /* I don't know how to compare floating-point numbers without generting a compiler
     * warning - the comparisons below are valid. */
    if (data_type == TBYTE)
    {
        long long ipix;
        signed char *opix = NULL;
        unsigned char *val = NULL;
        signed char *oval = NULL;

        if ((image_info)->bscale != 1 || (image_info)->bzero != -128)
        {
            /* We can't support this, at least with char data. What this would mean is
             * that the data are all positive, and potentially floating-point data (stored
             * as unsigned chars). We could promote the data to shorts, then hack the
             * image_info struct so that image_info says that data are shorts with
             * bzero and bscale values. We would put the raw positive byte values into a
             * short array (without applying any bzero/bscale scaling) and it would be
             * as if we read a short image to begin with. But I would have to
             * change a bunch of code at the higher level, so let's not do this unless
             * the need arises. */
            error_code = CFITSIO_ERROR_LIBRARY;
            goto error_exit;
        }

        opix = calloc(npixels, bytepix);

        if (!opix)
        {
            error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
            goto error_exit;
        }

        for (ipix = npixels, val = (unsigned char *)pixels, oval = opix; ipix >= 1; ipix--)
        {
            /* Sheesh. We cannot use casting to convert the unsigned char to a signed char,
             * since casting will subtract UCHAR_MAX + 1 (= 256) from the unsigned value.
             * Since bzero is -128, this implies that the image contains signed char data,
             * as specified in the FITS standard. Convert from unsigned char to signed char
             * by subtracting 128.
             */
            *oval = (signed char)((int)((image_info)->bzero) + *val);
            val++;
            oval++;
        }

        (image_info)->bzero = 0;
        (image_info)->bscale = 1;

        /* We have to convert the blank value back into the DRMS signed char system.
         * drms_fitsrw_ShootBlanks(), called downstream, will compare pixel values
         * to the blank value. At this point, the blank value is 0 - when the fits file
         * was first written, the blank value of -128 was converted to 0 (we made a
         * signed value into an unsigned value). Now that we've read the blank value
         * of 0, an unsigned char, we need to convert this value back to a signe char
         * value. */
        (image_info)->blank = -128;

        *image = opix;
        opix = NULL;

        free(pixels);
        pixels = NULL;
    }
    else
    {
        *image = pixels;
        pixels = NULL;
    }

   return CFITSIO_SUCCESS;

  error_exit:

    if (status != 0)
    {
        fits_get_errstatus(status, cfitsiostat);
        fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);
    }

   if (fptr)
   {
      /* Must call fitsrw_closefptr() to free the readonly fitsfile */
       /* There was some other error, so don't worry about failures having to do with closing the file. */
      fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr);
   }

   if(pixels) free(pixels);

   return error_code;
}

/* fpixel - location in TAS file of 'bottom left corner' of slice.
 * lpixel - location in TAS file of 'top right corner' of slice.
 */
int fitsrw_writeslice(int verbose, const char *filename, int *fpixel, int *lpixel, void *image)
{
   fitsfile *fptr=NULL;
    int error_code = CFITSIO_SUCCESS;
    int status; /* FITSIO error code. */
    int istat; /* non-FITSIO error code. */
   int data_type;
   long lfpixel[CFITSIO_MAX_DIM];
   long llpixel[CFITSIO_MAX_DIM];
   int idim = 0;
   int idim2 = 0;
   char cfitsiostat[FLEN_STATUS];
   long long npix;
   long long firstpix;
   int contiguous;
   long long intermed;
   TASRW_FILE_PTR_INFO fpinfo;
   int dimlen = 0;
   int fileCreated = 0;

   status = 0; // first thing!
    istat = 0;

   /* ART - If the file to write doesn't exist, this call will create a new empty file. This is probably
    * bad since an empty file has no fpinfo (other than the fhash value), which means uses of
    * fpinfo below will fail.
    *
    * There should be a test here. If the file does not already exist, then we need to use the fpixel/lpixel
    * information to initialize the fpinfo. */

    /* The call fitsrw_getfptr() is not clean - it looks like it might return
     * a  fptr, but also return istat != 0. fitsrw_getfptr() needs to be cleaned
     * up so that if istat != 0, the fptr == 0. */
   fptr = (fitsfile *)fitsrw_getfptr(verbose, filename, 1, &istat, &fileCreated);

   /* Can't write a slice without having first created the output file. If this is true, then we
    * do not need to deal with the huge hdu issue. */
   XASSERT(!fileCreated);

   if (!fptr || istat)
   {
       error_code = CFITSIO_ERROR_FILE_IO;
       goto error_exit;
   }

   /* Fetch img parameter info - should have been cached when the file was originally opened */
   if (fitsrw_getfpinfo(fptr, &fpinfo))
   {
       fprintf(stderr, "Unable to get fitsfile image parameters.\n");
       error_code = CFITSIO_ERROR_FILE_IO;
       goto error_exit;
   }

   if(fpinfo.naxis == 0)
   {
      error_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   switch(fpinfo.bitpix)
   {
      case(BYTE_IMG): data_type = TSBYTE; break;
      case(SHORT_IMG): data_type = TSHORT; break;
      case(LONG_IMG): data_type = TINT; break;
      case(LONGLONG_IMG): data_type = TLONGLONG; break;
      case(FLOAT_IMG): data_type = TFLOAT; break;
      case(DOUBLE_IMG): data_type = TDOUBLE; break;
   }

   //Write raw data
   fits_set_bscale(fptr, 1.0, 0.0, &status);
   fits_set_imgnull(fptr, 0, &status);
   if(status)
   {
       error_code = CFITSIO_ERROR_LIBRARY;
       goto error_exit;
   }

   /* ffgsv requires long type, although this is dangerous
    * as long is a different number of bytes on different
    * machines. */
   memset(lfpixel, 0, sizeof(lfpixel));
   memset(llpixel, 0, sizeof(llpixel));
   npix = 1;
   firstpix = 0;
   contiguous = 1;

    /* This section does two things:
     *   1. It sets lfpixel and llpixel. These are the fpixel and lpixel incremented
     *      by one, since cfitsio uses 1-based arrays, but fpixel and lpixel are
     *      0-based arrays.
     *   2. It determines if the slice being written is contiguous in memory. */
   for (idim = 0; idim < fpinfo.naxis; idim++)
   {
      /* FITS uses column-major order, and C uses row-major order for its arrays.  So,
       * the C array equivalent of a FITS-file's data array would be arr[NAXISn][NAXISn-1]...
       * [NAXIS1].  BUT, to be confusing, when specifying a pixel to FITSIO routines, don't do
       * this.  For some unknown reason, you specify the indices of a pixel in the opposite order -
       * fpixel[0] = NAXIS1 value, fpixel[1] = NAXIS2 value, ..., fpixel[n-1] = NAXISn value.
       * NOTE that the array indices of the pixel are 0-based, but the actual NAXISn values are
       * 1-based.  Again, confusing.
       */

      /* CFITSIO uses 1-based array indices, not 0-based */
      lfpixel[idim] = (long)fpixel[idim] + 1;
      llpixel[idim] = (long)lpixel[idim] + 1;
      npix += npix * (lpixel[idim] - fpixel[idim]);
      intermed = 1;

       if (fpinfo.naxis > 1)
       {
           if (idim == fpinfo.naxis - 1)
           {
               /* This calculates the number of pixels in a slice of the final image when
                * the current dimension has a thickness of one pixel. The slice spans
                * the entire final image in all dimensions other than the current dimension.
                * This information is used only when the slice being written to file is
                * contiguous in memory. This calcuation should happen only when
                * idim == fpinfo.naxis - 1 (when we are processing the last dimension). */
               for (idim2 = idim - 1; idim2 >= 0; idim2--)
               {
                   intermed = intermed * fpinfo.naxes[idim2];
               }

               /* This assumes a contiguous image, which means all 0-based
                * dims < fpinfo.naxis - 1 of the slice span the entire final
                * image. */
               firstpix = fpixel[idim - 1] + intermed * fpixel[idim];
           }
       }
       else
       {
           /* Must be contiguous! */
           firstpix = fpixel[idim];
       }

      /* check for contiguousness - basically if in the current dimension
       * the slice has a thickness greater than 1, then all previous dimensions
       * must span the entire fits file, else the slice isn't contiguous */
      if (contiguous &&
          idim > 0 &&
          (lpixel[idim] - fpixel[idim] > 0) &&
          (fpixel[idim - 1] > 0 || lpixel[idim - 1] < fpinfo.naxes[idim - 1] - 1))
      {
         contiguous = 0;
      }
   }

   /* This fits_write_subset mofo does all kinds of seeking back and forth in the
    * file - if the data are contiguous, just use first_write_img() as an optimization.
    * This will often be the case, especially for things like TAS slices. */

#ifdef DEBUG
   StartTimer(26);
   fprintf(stderr, "Calling fits_write_subset() or fits_write_img().\n");
#endif

   if (contiguous)
   {
      /* cfitsio pixel numbers are 1-based */
      firstpix++;

      if (fits_write_img(fptr, data_type, firstpix, npix, image, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
         goto error_exit;
      }
   }
   else
   {
      if(fits_write_subset(fptr, data_type, lfpixel, llpixel, image, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
         goto error_exit;
      }
   }

#if 0
   /* The NAXISn keyword of the last dimension might not match the
    * length of the last dimension in the fpinfo. The file may have
    * been created with a dummy last dimension of 1 if the length of
    * the last dimension was not know at file creation time. Update
    * the NAXISn keyword, using the last pixel dimension length. */

   char naxisname[64];
   snprintf(naxisname, sizeof(naxisname), "NAXIS%d", fpinfo.naxis);
   dimlen = lpixel[fpinfo.naxis - 1] + 1;
   fits_update_key(fptr, TINT, naxisname, &dimlen, NULL, &status);
#endif

   /* Must update the file pointer info's naxis value with dimlen if dimlen is greater than
    * fpinfo.naxis
    *
    * As we write slices, we store the value of the cube's last dimension where the slice resides.
    * We can write slices out of order, so we only modify this value if the slice currently being
    * written increases the size of the cube in this last dimension.
    *
    * When we close the file, the NAXISn keyword will be updated (in fitsrw_closefptr()).
    */
   dimlen = lpixel[fpinfo.naxis - 1] + 1;
   if (dimlen > fpinfo.naxes[fpinfo.naxis - 1])
   {
      TASRW_FILE_PTR_INFO newinfo = fpinfo;
      newinfo.naxes[fpinfo.naxis - 1] = dimlen;
      newinfo.bitfield |= kInfoPresent_Dirt; /* set the dirty bit */
      fitsrw_setfpinfo(fptr, &newinfo);
   }

#ifdef DEBUG
   fprintf(stdout, "Time to write subset: %f\n", StopTimer(26));
#endif

   return CFITSIO_SUCCESS;

 error_exit:

    if (status != 0)
    {
        fits_get_errstatus(status, cfitsiostat);
        fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);
    }

   /* Some error writing the file - don't worry about computing checksum. */
   if(fptr)
   {
       /* Some problem, so close the file. There is already an error_code, so don't report a write failure too. */
       fitsrw_closefptr(verbose, (TASRW_FilePtr_t)fptr);
   }

   return error_code;
}

int fitsrw_closefptr(int verbose, TASRW_FilePtr_t fptr)
{
    char fileinfokey[64];
    int fiostat = 0; /* fitsio status */
    TASRW_FILE_PTR_INFO fpinfo;
    char cfitsiostat[FLEN_STATUS];
    int error_code = CFITSIO_SUCCESS;

    if (fptr)
    {
        /* Before removing the file info, get the dirty flag value and the value of NAXISn. */
        if (fitsrw_getfpinfo((fitsfile *)fptr, &fpinfo))
        {
            fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
            error_code = CFITSIO_ERROR_FILE_IO;
        }
        else if (gFFiles)
        {
            if (hcon_lookup(gFFiles, fpinfo.fhash))
            {
                snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
                hcon_remove(gFFPtrInfo, fileinfokey);

                if (IsWriteable(fpinfo.fhash))
                {
                    if (fpinfo.bitfield & kInfoPresent_Dirt)
                    {
                        /* If this is a writable fits file AND the dirty flag is set (which means that
                         * since the file was first created, the NAXISn length has changed due to
                         * slice writing), then update the NAXISn keyword value before closing the
                         * fits file. */

                        /* It is not sufficient to simply change the NAXISn keyword value. Although this WILL
                         * make FITSIO think the image is smaller, the actual image data does not get removed
                         * from the FITS file. So, we ALSO have to call fits_resize_imgll().
                         */
                        int dimlen;
                        int imgType;
                        long long *axes = NULL;
                        int iaxis;

                        axes = calloc(fpinfo.naxis, sizeof(long long));

                        if (!axes)
                        {
                            error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
                        }
                        else
                        {
                            for (iaxis = 0; iaxis < fpinfo.naxis; iaxis++)
                            {
                                axes[iaxis] = fpinfo.naxes[iaxis];
                            }

                            switch(fpinfo.bitpix)
                            {
                                case(BYTE_IMG): imgType = SBYTE_IMG; break;
                                case(SHORT_IMG): imgType = SHORT_IMG; break;
                                case(LONG_IMG): imgType = LONG_IMG; break;
                                case(LONGLONG_IMG): imgType = LONGLONG_IMG; break;
                                case(FLOAT_IMG): imgType = FLOAT_IMG; break;
                                case(DOUBLE_IMG): imgType = DOUBLE_IMG; break;
                            }

                            fiostat = 0;
                            fits_resize_imgll((fitsfile *)fptr, imgType, fpinfo.naxis, axes, &fiostat);

                            if (fiostat)
                            {
                                fprintf(stderr, "FITSIO error: %d\n", fiostat);
                                fprintf(stderr, "unable to resize image in fits file\n");
                                error_code = CFITSIO_ERROR_LIBRARY;
                            }

                            /* no need to manually modify NAXISn (the value of the slice dimension);
                             * fits_resize_imgll() does this
                             */

                            free(axes);
                            axes = NULL;
                         }
                    }

                    if (!error_code)
                    {
                        if (verbose)
                        {
                            PushTimer();
                        }

                        fiostat = 0;
                        fits_write_chksum((fitsfile *)fptr, &fiostat);

                        if (fiostat)
                        {
                            fits_get_errstatus(fiostat, cfitsiostat);
                            fprintf(stderr, "Closing fitsfile: error calculating and writing checksum for fitsfile '%s'.\n", fpinfo.fhash);
                            fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                            fits_report_error(stderr, fiostat);
                            error_code = CFITSIO_ERROR_LIBRARY;
                        }

                        if (verbose)
                        {
                            fprintf(stdout, "Time to write checksum on fitsfile '%s' = %f sec.\n", fpinfo.fhash, PopTimer());
                        }
                    }
                }

                if (verbose)
                {
                    PushTimer();
                }

                fiostat = 0;
                fits_close_file((fitsfile *)fptr, &fiostat);

                if (fiostat == 0)
                {
                    if (verbose)
                    {
                        fprintf(stdout, "Closing fits file '%s'.\n",  fpinfo.fhash);
                    }
                }
                else
                {
                    fits_get_errstatus(fiostat, cfitsiostat);
                    fprintf(stderr, "Closing fitsfile: error closing fitsfile '%s'.\n", fpinfo.fhash);
                    fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                    perror("fitsrw_closefptr() system error");

                    if (!error_code)
                    {
                        error_code = CFITSIO_ERROR_FILE_IO;
                    }
                }

                if (verbose)
                {
                    fprintf(stdout, "Time to close fitsfile '%s' = %f sec.\n", fpinfo.fhash, PopTimer());
                }

                hcon_remove(gFFiles, fpinfo.fhash);
            }
            else
            {
                fprintf(stderr, "Unknown fitsfile '%s'.\n", fpinfo.fhash);
                error_code = CFITSIO_ERROR_ARGS;
            }
        }
        else
        {
            fprintf(stderr, "Not keeping track of fitsfile pointers!\n");
        }
    }

    return error_code;
}

int fitsrw_closefptrByName(int verbose, const char *filename)
{
    char filehashkeyOnefile[PATH_MAX + 2];
    fitsfile **pfptr = NULL;

    if (gFFiles)
    {
        /* Delete the FITS file identified by filename. We don't know if it is writeable or not, so try both file hashes - ending
         * with w and r. */
        snprintf(filehashkeyOnefile, sizeof(filehashkeyOnefile), "%s:w", filename);
        pfptr = (fitsfile **)hcon_lookup(gFFiles, filehashkeyOnefile);
        if (!pfptr || !*pfptr)
        {
            snprintf(filehashkeyOnefile, sizeof(filehashkeyOnefile), "%s:r", filename);
            pfptr = (fitsfile **)hcon_lookup(gFFiles, filehashkeyOnefile);
        }

        if (pfptr && *pfptr)
        {
            return fitsrw_closefptr(verbose, (TASRW_FilePtr_t)(*pfptr));
        }
    }

    /* Don't error out if the file to be closed cannot be found. */
    return CFITSIO_SUCCESS;
}

int fitsrw_closefptrs(int verbose)
{
    int exit_code = CFITSIO_SUCCESS;

    if (gFFiles)
    {
        if (hcon_size(gFFiles) > 0)
        {
            HIterator_t *hit = NULL;
            fitsfile **pfptr = NULL;
            int stat = 0; /* fitsrw error code. */
            int fiostat = 0; /* fitsio error code. */
            const char *filehashkey = NULL;
            char fileinfokey[64];
            int ifile;
            char cfitsiostat[FLEN_STATUS];
            TASRW_FILE_PTR_INFO fpinfo;
            LinkedList_t *llist = NULL;
            ListNode_t *node = NULL;
            char *onefile = NULL;

            llist = list_llcreate(sizeof(char *), NULL);

            if (llist)
            {
                /* Delete all open FITS files. */
                hit = hiter_create(gFFiles);

                if (hit)
                {
                    if (verbose)
                    {
                        fprintf(stdout, "fitsrw_closefptrs(): Attempting to close %d fitsfile pointers.\n", gFFiles->num_total);
                    }

                    ifile = 0;
                    while ((pfptr = (fitsfile **)hiter_extgetnext(hit, &filehashkey)) != NULL)
                    {
                        /* Don't call hcon_remove(gFFiles, filehashkey) here! Can't remove items from
                         * a hash container if you're iterating through the container. */

                        /* Save the filehashkey for each file being removed from the cache -
                         * must remove from gFFiles AFTER existing the loop that is iterating
                         * over gFFiles. */
                        list_llinserttail(llist, (void *)&filehashkey);
                        ifile++;
                    }

                    hiter_destroy(&hit);
                }

                /* free all the file pointers saved in gFFiles */
                list_llreset(llist);
                while ((node = list_llnext(llist)) != NULL)
                {
                    stat = 0;
                    onefile = *((char **)node->data);
                    pfptr = (fitsfile **)hcon_lookup(gFFiles, onefile);

                    if (pfptr && *pfptr)
                    {
                        /* remove fileptr info structure */
                        snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)*pfptr);

                        hcon_remove(gFFPtrInfo, fileinfokey);

                        if (IsWriteable(onefile))
                        {
                            /* Before removing the file info, get the dirty flag value and the value of NAXISn. */

                            /* it is OK to overwrite stat from previous iterations; the previous iteration
                             * stat value was saved in exit_code */
                            stat = fitsrw_getfpinfo(*pfptr, &fpinfo);

                            if (stat)
                            {
                                fprintf(stderr, "Missing file info for fits file '%s'.\n", onefile);
                                /* keep going - we just skip the removal of the file info from the global container */
                            }
                            else
                            {
                                if (fpinfo.bitfield & kInfoPresent_Dirt)
                                {
                                    /* If this is a writable fits file AND the dirty flag is set (which means that
                                     * since the file was first created, the NAXISn length has changed due to
                                     * slice writing), then update the NAXISn keyword value before closing the
                                     * fits file. */

                                    /* It is not sufficient to simply change the NAXISn keyword value. Although this WILL
                                     * make FITSIO think the image is smaller, the actual image data does not get removed
                                     * from the FITS file. So, we ALSO have to call fits_resize_imgll().
                                     */
                                    int dimlen;
                                    int imgType;
                                    long long *axes = NULL;
                                    int iaxis;

                                    axes = calloc(fpinfo.naxis, sizeof(long long));

                                    if (!axes)
                                    {
                                        stat = CFITSIO_ERROR_OUT_OF_MEMORY;
                                    }
                                    else
                                    {
                                        for (iaxis = 0; iaxis < fpinfo.naxis; iaxis++)
                                        {
                                            axes[iaxis] = fpinfo.naxes[iaxis];
                                        }

                                        switch(fpinfo.bitpix)
                                        {
                                            case(BYTE_IMG): imgType = SBYTE_IMG; break;
                                            case(SHORT_IMG): imgType = SHORT_IMG; break;
                                            case(LONG_IMG): imgType = LONG_IMG; break;
                                            case(LONGLONG_IMG): imgType = LONGLONG_IMG; break;
                                            case(FLOAT_IMG): imgType = FLOAT_IMG; break;
                                            case(DOUBLE_IMG): imgType = DOUBLE_IMG; break;
                                        }

                                        fiostat = 0;
                                        fits_resize_imgll(*pfptr, imgType, fpinfo.naxis, axes, &fiostat);

                                        if (fiostat)
                                        {
                                            fprintf(stderr, "FITSIO error: %d.\n", fiostat);
                                            fprintf(stderr, "unable to resize image in fits file\n");
                                            stat = CFITSIO_ERROR_LIBRARY;
                                        }

                                        /* no need to manually modify NAXISn (the value of the slice dimension);
                                         * fits_resize_imgll() does this
                                         */

                                        free(axes);
                                        axes = NULL;
                                     }
                                }
                            }

                            if (!stat)
                            {
                                if (verbose)
                                {
                                    PushTimer();
                                }

                                fiostat = 0;
                                fits_write_chksum(*pfptr, &fiostat);

                                if (fiostat)
                                {
                                    fits_get_errstatus(fiostat, cfitsiostat);
                                    fprintf(stderr, "Closing all fitsfiles: error calculating and writing checksum for fitsfile '%s'.\n", onefile);
                                    fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                                    stat = CFITSIO_ERROR_LIBRARY;
                                }

                                if (verbose)
                                {
                                    fprintf(stdout, "Time to write checksum on fitsfile '%s' = %f sec.\n", onefile, PopTimer());
                                }
                            }
                        }

                        if (verbose)
                        {
                            PushTimer();
                        }

                        fiostat = 0;
                        fits_close_file(*pfptr, &fiostat);

                        if (fiostat == 0)
                        {
                            if (verbose)
                            {
                                fprintf(stdout, "Closing fits file '%s'.\n", onefile);
                            }
                        }
                        else
                        {
                            fits_get_errstatus(fiostat, cfitsiostat);
                            fprintf(stderr, "Closing all fitsfiles: error closing fitsfile '%s'.\n", onefile);
                            fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                            perror("fitsrw_closefptrs() system error");
                            if (!stat)
                            {
                                stat = CFITSIO_ERROR_FILE_IO;
                            }
                        }

                        if (verbose)
                        {
                            fprintf(stdout, "Time to close fitsfile '%s' = %f sec.\n", onefile, PopTimer());
                        }
                    }
                    else
                    {   if (!stat)
                        {
                            stat = CFITSIO_ERROR_FILE_IO;
                        }

                        fprintf(stderr, "Unknown fitsfile '%s'.\n", onefile);
                    }

                    if (stat)
                    {
                        fprintf(stderr, "Error closing fitsfile '%s'.\n", onefile);
                    }

                    hcon_remove(gFFiles, onefile);
                    if (verbose)
                    {
                        fprintf(stdout, "Number fitsfiles still open: %d\n", gFFiles->num_total);
                    }

                    if (stat && exit_code == CFITSIO_SUCCESS)
                    {
                        exit_code = stat;
                    }
                }

                list_llfree(&llist);
            }
        }

        hcon_destroy(&gFFPtrInfo);
        hcon_destroy(&gFFiles);
    }

    return exit_code;
}

int fitsrw_getfpinfo_ext(TASRW_FilePtr_t fptr, TASRW_FilePtrInfo_t info)
{
   return fitsrw_getfpinfo((fitsfile *)fptr, (TASRW_FILE_PTR_INFO *)info);
}

int fitsrw_setfpinfo_ext(TASRW_FilePtr_t fptr, TASRW_FilePtrInfo_t info)
{
   return fitsrw_setfpinfo((fitsfile *)fptr, (TASRW_FILE_PTR_INFO *)info);
}

int fitsrw_iscompressed(const char *cparms)
{
   int rv = 0;

   if (cparms && *cparms)
   {
      const char *pc = cparms;

      SKIPWS(pc);

      /* Same parsing logic as cfileio.c (in the FITSIO library). */
      if (strncmp(pc, "compress", 8) || strncmp(pc, "COMPRESS", 8))
      {
         pc += 8;
         SKIPWS(pc);

         if (*pc == 'r' || *pc == 'R')
         {
            rv = 1;
         }
      }
   }

   return rv;
}

int fitsrw_initializeTAS(int verbose, const char *filename)
{
    int fitsrwErr = CFITSIO_SUCCESS;
    fitsfile *fptr = (fitsfile *)fitsrw_getfptr(verbose, filename, 1, &fitsrwErr, NULL);

    if (!fptr || fitsrwErr != CFITSIO_SUCCESS)
    {
        fprintf(stderr, "could not locate FITS TAS file '%s'\n", filename);
        fitsrwErr = CFITSIO_ERROR_FILE_IO;
    }
    else
    {
        TASRW_FILE_PTR_INFO fpinfo;

        if (fitsrw_getfpinfo(fptr, &fpinfo))
        {
            fprintf(stderr, "unable to get fitsfile image parameters\n");
            fitsrwErr = CFITSIO_ERROR_CANT_GET_FILEINFO;
        }
        else
        {
            TASRW_FILE_PTR_INFO newinfo = fpinfo;

            newinfo.naxes[fpinfo.naxis - 1] = 0; /* set the value of the slice-dimension length to 0 ==> empty cube with no slices */
            newinfo.bitfield |= kInfoPresent_Dirt; /* set the dirty bit */
            if (fitsrw_setfpinfo(fptr, &newinfo) != 0)
            {
                fprintf(stderr, "could not set FITS TAS-file (%s) information\n", filename);
                fitsrwErr = CFITSIO_ERROR_FILE_IO;
            }
        }
    }

    return fitsrwErr;
}
