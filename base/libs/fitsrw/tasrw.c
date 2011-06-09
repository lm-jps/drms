#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "cfitsio.h"
#include "tasrw.h"
#include "cfitsio.h"
#include "fitsio.h"
#include "hcontainer.h"
#include "list.h"
#include "timer.h"

// #define DEBUG

#ifdef DEBUG
#include "timer.h"
#endif

/* Our current set up is 1024 maximum open files per process (/usr/include/linux/limits.h);
 * The maximum number of fitsfiles allowed to be open per CFITSIO lib is 300. */
#define MAXFFILES 256

typedef CFITSIO_IMAGE_INFO TASRW_FilePtrInfo_t;

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

/* fitsfiles can be opened either READWRITE or READONLY. If a request for a READONLY pointer
 * is made, and the fitsfile has already been opened, then regardless of the fitsfile write mode
 * it is okay to return the opened pointer. But, if the request is for a READWRITE pointer
 * and a READONLY fitsfile pointer exists, then the READONLY pointer must be closed, 
 * and the file must be reopened with READWRITE write mode.
 * 
 * The hash key to the gFFiles is <filename> ':' ('r' | 'w') where r implies readonly and w implies
 * readwrite.
 */
fitsfile *fitsrw_getfptr_internal(int verbose, const char *filename, int writeable, int verchksum, int *status)
{
   fitsfile *fptr = NULL;
   fitsfile **pfptr = NULL;
   char filehashkey[PATH_MAX + 2];
   char tmpfilehashkey[PATH_MAX + 2];
   char fileinfokey[64];
   int stat = 0;
   int datachk;
   int hduchk;
   int newfile = 0;
   char cfitsiostat[FLEN_STATUS];

   if (verbose)
   {
      PushTimer();
   }

   if (!gFFiles)
   {
      gFFiles = hcon_create(sizeof(fitsfile *), sizeof(filehashkey), NULL, NULL, NULL, NULL, 0);
   }

   if (!gFFPtrInfo)
   {
      gFFPtrInfo = hcon_create(sizeof(TASRW_FilePtrInfo_t), sizeof(fileinfokey), NULL, NULL, NULL, NULL, 0);
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
                  PushTimer();
               }

               fits_close_file(*pfptr, &stat);

               if (verbose)
               {
                  if (stat == 0)
                  {
                     fprintf(stdout, "Closing fits file '%s'.\n", tmpfilehashkey);
                  }
                  else
                  {
                     fprintf(stdout, "Unable to close fits file '%s'.\n", tmpfilehashkey);
                  }

                  fprintf(stdout, "Time to close fitsfile '%s'= %f sec.\n", tmpfilehashkey, PopTimer());
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

      if (fits_open_image(&fptr, filename, writeable ? READWRITE : READONLY, &stat)) 
      {
         if (writeable)
         {
            stat = 0;

            /* create new file, but only if writing */
            newfile = 1;
            if (fits_create_file(&fptr, filename, &stat)) 
            {
               stat = 1;

               if (status)
               {
                  *status = CFITSIO_ERROR_FILE_IO;
               }
            }
         }
         else
         {
            stat = 1;

            if (status)
            {
               *status = CFITSIO_ERROR_FILE_DOESNT_EXIST;
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
            if (fits_verify_chksum(fptr, &datachk, &hduchk, &stat))
            {
               stat = 1;

               if (status)
               {
                  *status = CFITSIO_ERROR_LIBRARY;
               }
            }
            else
            {
               if (datachk == -1 || hduchk == -1)
               {
                  stat = 1;

                  /* Both checksums were present, and at least one of them failed. */
                  fprintf(stderr, "Failed to verify data and/or HDU checksum (file corrupted).\n");
                  if (status)
                  {
                     *status = CFITSIO_ERROR_FILE_IO;
                  }
               }
            }
         }

         if (verbose)
         {
            fprintf(stdout, "Time to verify checksum = %f sec.\n", PopTimer());
         }
      }
      
      if (!stat && gFFiles->num_total > MAXFFILES)
      {
         /* Must close some open files */
         int ifile = 0;
         HIterator_t *hit = hiter_create(gFFiles);
         LinkedList_t *llist = list_llcreate(sizeof(char *), NULL);
         const char *fhkey = NULL;
         ListNode_t *node = NULL;
         char *onefile = NULL;

         if (hit)
         {
            while (ifile < MAXFFILES / 2)
            {
               pfptr = (fitsfile **)hiter_extgetnext(hit, &fhkey);

               if (pfptr)
               {
                  if (*pfptr)
                  {
                     /* remove fileptr into structure */
                     snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)*pfptr);                 
                     hcon_remove(gFFPtrInfo, fileinfokey);

                     if (IsWriteable(fhkey))
                     {
                        if (verbose)
                        {
                           PushTimer();
                        }

                        fits_write_chksum(*pfptr, &stat);

                        if (stat)
                        {
                           fits_get_errstatus(stat, cfitsiostat);
                           fprintf(stderr, "Purging cache: error calculating and writing checksum for fitsfile '%s'.\n", fhkey);
                           fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                           stat = 0;
                        }
                        
                        if (verbose)
                        {
                           fprintf(stdout, "Time to write checksum on fitsfile '%s' = %f sec.\n", fhkey, PopTimer());
                        }
                     }

                     if (verbose)
                     {
                        PushTimer();
                     }

                     fits_close_file(*pfptr, &stat);

                     if (stat == 0)
                     {
                        if (verbose)
                        {
                           fprintf(stdout, "Closing fits file '%s'.\n", fhkey);
                        }
                     }
                     else
                     {
                        fits_get_errstatus(stat, cfitsiostat);
                        fprintf(stderr, "Purging cache: error closing fitsfile '%s'.\n", fhkey);
                        fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                     }
                     
                     if (verbose)
                     {
                        fprintf(stdout, "Time to close fitsfile '%s' = %f sec.\n", fhkey, PopTimer());
                     }
                  }

                  /* Save the filehashkey for each file being removed from the cache - 
                   * must remove from gFFiles AFTER existing the loop that is iterating
                   * over gFFiles. */
                  list_llinserttail(llist, (void *)&fhkey);

                  if (stat)
                  {
                     fprintf(stderr, "Error closing fitsfile '%s'.\n", filename);
                  }

                  ifile++;
               }
               else
               {
                  break;
               }
            }

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

      if (!stat)
      {
         /* Read essential keyword information and cache that for later use */
         CFITSIO_IMAGE_INFO *imginfo = NULL;

         if (newfile)
         {
            imginfo = (CFITSIO_IMAGE_INFO *)malloc(sizeof(CFITSIO_IMAGE_INFO));
            memset(imginfo, 0, sizeof(CFITSIO_IMAGE_INFO));
         }
         else
         {
            if (fitsrw_read_keylist_and_image_info((FITSRW_fhandle)fptr, NULL, &imginfo) != CFITSIO_SUCCESS)
            {
               stat = 1;

               if (status)
               {
                  *status = CFITSIO_ERROR_CANT_GET_FILEINFO;
               }
            }
         }

         if (!stat)
         {
            if (gFFPtrInfo)
            {
               snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
               /* Stash away filehashkey in imginfo->fhash; used when closing fitsfile pointers */
               snprintf(imginfo->fhash, sizeof(imginfo->fhash), "%s", filehashkey);
               hcon_insert(gFFPtrInfo, fileinfokey, imginfo);
            }
         }

         if (imginfo)
         {
            cfitsio_free_these(&imginfo, NULL, NULL);
         }

         hcon_insert(gFFiles, filehashkey, &fptr);
         if (verbose)
         {
            fprintf(stdout, "Number fitsfiles opened: %d\n", gFFiles->num_total);
         }
      }
   }

   return fptr;
}

fitsfile *fitsrw_getfptr(int verbose, const char *filename, int writeable, int *status)
{
   return fitsrw_getfptr_internal(verbose, filename, writeable, 1, status);
}

fitsfile *fitsrw_getfptr_nochksum(int verbose, const char *filename, int writeable, int *status)
{
   return fitsrw_getfptr_internal(verbose, filename, writeable, 0, status);
}

static int fitsrw_getfpinfo(fitsfile *fptr, TASRW_FilePtrInfo_t *info)
{
   int err = 1;
   TASRW_FilePtrInfo_t *pfpinfo = NULL;
   char fileinfokey[64];

   if (fptr && info)
   {
      snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
      err = ((pfpinfo = (TASRW_FilePtrInfo_t *)hcon_lookup(gFFPtrInfo, fileinfokey)) == NULL);
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

int fitsrw_readslice(int verbose,
                     const char *filename, 
                     int *fpixel, 
                     int *lpixel, 
                     CFITSIO_IMAGE_INFO** image_info,
                     void** image)
{
   fitsfile *fptr=NULL;     
   int error_code, status=0;
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
   TASRW_FilePtrInfo_t fpinfo;

   long i;

   if (!image_info)
   {
      fprintf(stderr, "Invalid image_info argument.\n");
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }

   if(*image_info) cfitsio_free_image_info(image_info);
   *image_info = (CFITSIO_IMAGE_INFO *)malloc(sizeof(CFITSIO_IMAGE_INFO));

   if(*image_info == NULL)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }

   memset((void*)(*image_info), 0, sizeof(CFITSIO_IMAGE_INFO));

   status = 0; // first thing!

   fptr = fitsrw_getfptr_nochksum(verbose, filename, 0, &status);

   if (!fptr)
   {
      goto error_exit;
   }


   /* Fetch img parameter info - should have been cached when the file was originally opened */
   if (fitsrw_getfpinfo(fptr, &fpinfo))
   {
      fprintf(stderr, "Unable to get fitsfile image parameters.\n");
      goto error_exit;
   }

   **image_info = fpinfo;

   if((*image_info)->naxis == 0)
   {
      error_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   switch((*image_info)->bitpix)
   {
      case(BYTE_IMG):    data_type = TSBYTE; break;
      case(SHORT_IMG):   data_type = TSHORT; break;
      case(LONG_IMG):    data_type = TINT; break; 
      case(LONGLONG_IMG):data_type = TLONGLONG; break;
      case(FLOAT_IMG):   data_type = TFLOAT; break;
      case(DOUBLE_IMG):  data_type = TDOUBLE; break;
   }

   bytepix = abs((*image_info)->bitpix)/8;

   npixels = 1;
   for(i=0;i<(*image_info)->naxis;i++) 
   {
      /* override default axis lengths in image_info - a subset will have potentially 
       * shorter lengths */
      (*image_info)->naxes[i] = (lpixel[i]-fpixel[i]+1);
      npixels *= (*image_info)->naxes[i];
      increments[i]=1; // no skipping
   }

   pixels = calloc(npixels,  bytepix);
   if(!pixels)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }

   //Read raw data
   fits_set_bscale(fptr, 1.0, 0.0, &status);
   fits_set_imgnull(fptr, 0, &status);
   if(status) goto error_exit;

   //Read stripe, slice, block
   int anynul;
   /* ffgsv requires long type, although this is dangerous 
    * as long is a different number of bytes on different 
    * machines. */
   memset(lfpixel, 0, sizeof(lfpixel));
   memset(llpixel, 0, sizeof(llpixel));
   
   for (idim = 0; idim < (*image_info)->naxis; idim++)
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

   *image = pixels;

   /* bzero/bscale are always 0.0/1.0 in a TAS FITS file, thank goodness, so 
    * we don't have to worry about correcting the values when type is TSBYTE.  Otherwise,
    * you'd have to account for the fact that cfitsio doesn't understand that 
    * bzero must be -128 for BYTE_IMG type of images. */

   return CFITSIO_SUCCESS;

  error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);
   if(fptr) 
   {
      /* Must call fitsrw_closefptr() to free the readonly fitsfile */
      fitsrw_closefptr(verbose, fptr);
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
   int error_code, status=0;
   int data_type;
   long lfpixel[CFITSIO_MAX_DIM];
   long llpixel[CFITSIO_MAX_DIM];
   int idim = 0;
   int idim2 = 0;
   char cfitsiostat[FLEN_STATUS];
   long long npix;
   int contiguous;
   long long firstpix;
   long long intermed;
   TASRW_FilePtrInfo_t fpinfo;
   int dimlen = 0;

   status = 0; // first thing!

   fptr = fitsrw_getfptr(verbose, filename, 1, &status);

   if (!fptr)
   {
      goto error_exit;
   }

   /* Fetch img parameter info - should have been cached when the file was originally opened */
   if (fitsrw_getfpinfo(fptr, &fpinfo))
   {
      fprintf(stderr, "Unable to get fitsfile image parameters.\n");
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
   if(status) goto error_exit;

   /* ffgsv requires long type, although this is dangerous 
    * as long is a different number of bytes on different 
    * machines. */
   memset(lfpixel, 0, sizeof(lfpixel));
   memset(llpixel, 0, sizeof(llpixel));
   npix = 1;
   firstpix = 0;
   contiguous = 1;
   
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
      
      for (idim2 = idim - 1; idim2 >= 0; idim2--)
      {
         intermed = intermed * fpinfo.naxes[idim2];
      }

      firstpix += firstpix + intermed * fpixel[idim];

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

   /* The NAXISn keyword of the last dimension might not match the 
    * length of the last dimension in the fpinfo. The file may have
    * been created with a dummy last dimension of 1 if the length of
    * the last dimension was not know at file creation time. Update
    * the NAXISn keyword, using the last pixel dimension length. */
   char naxisname[64];
   snprintf(naxisname, sizeof(naxisname), "NAXIS%d", fpinfo.naxis);
   dimlen = lpixel[fpinfo.naxis - 1] + 1;
   fits_update_key(fptr, TINT, naxisname, &dimlen, NULL, &status);

#ifdef DEBUG
   fprintf(stdout, "Time to write subset: %f\n", StopTimer(26));
#endif

   return CFITSIO_SUCCESS;

 error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);

   /* Some error writing the file - don't worry about computing checksum. */
   if(fptr) 
   {
      /* Must call fitsrw_closefptr() to free the readonly fitsfile */
      fitsrw_closefptr(verbose, fptr);
   }

   return error_code;
}

int fitsrw_closefptr(int verbose, fitsfile *fptr)
{
   char fileinfokey[64];
   int stat = 0;
   TASRW_FilePtrInfo_t fpinfo;
   char cfitsiostat[FLEN_STATUS];

   if (fptr)
   {
      if (fitsrw_getfpinfo(fptr, &fpinfo))
      {
         fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
      }
      else if (gFFiles)
      {
         if (hcon_lookup(gFFiles, fpinfo.fhash))
         {
            snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)fptr);
            hcon_remove(gFFPtrInfo, fileinfokey);

            if (IsWriteable(fpinfo.fhash))
            {
               if (verbose)
               {
                  PushTimer();
               }

               fits_write_chksum(fptr, &stat);

               if (stat)
               {
                  fits_get_errstatus(stat, cfitsiostat);
                  fprintf(stderr, "Closing fitsfile: error calculating and writing checksum for fitsfile '%s'.\n", fpinfo.fhash);
                  fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                  stat = 0;
               }

               if (verbose)
               {
                  fprintf(stdout, "Time to write checksum on fitsfile '%s' = %f sec.\n", fpinfo.fhash, PopTimer());
               }
            }

            if (verbose)
            {
               PushTimer();
            }

            fits_close_file(fptr, &stat);

            if (stat == 0)
            {
               if (verbose)
               {
                  fprintf(stdout, "Closing fits file '%s'.\n",  fpinfo.fhash);
               }
            }
            else
            {
               fits_get_errstatus(stat, cfitsiostat);
               fprintf(stderr, "Closing fitsfile: error closing fitsfile '%s'.\n", fpinfo.fhash);
               fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
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
         }
      }
      else
      {
         fprintf(stderr, "Not keeping track of fitsfile pointers!\n");
      }
   }

   return stat;
}

void fitsrw_closefptrs(int verbose)
{
   if (gFFiles)
   {
      if (hcon_size(gFFiles) > 0)
      {
         HIterator_t *hit = hiter_create(gFFiles);
         fitsfile **pfptr = NULL;
         int stat = 0;
         const char *filehashkey = NULL;
         char fileinfokey[64];
         int ifile;
         char cfitsiostat[FLEN_STATUS];

         if (hit)
         {
            LinkedList_t *llist = list_llcreate(sizeof(char *), NULL);
            ListNode_t *node = NULL;
            char *onefile = NULL;

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

            /* free all the file pointers saved in gFFiles */
            list_llreset(llist);
            while ((node = list_llnext(llist)) != NULL)
            {
               onefile = *((char **)node->data);
               pfptr = (fitsfile **)hcon_lookup(gFFiles, onefile);

               if (pfptr && *pfptr)
               {
                  /* remove fileptr info structure */         
                  snprintf(fileinfokey, sizeof(fileinfokey), "%p", (void *)*pfptr);
                  hcon_remove(gFFPtrInfo, fileinfokey);

                  if (IsWriteable(onefile))
                  {
                     if (verbose)
                     {
                        PushTimer();
                     }

                     fits_write_chksum(*pfptr, &stat);

                     if (stat)
                     {
                        fits_get_errstatus(stat, cfitsiostat);
                        fprintf(stderr, "Closing all fitsfiles: error calculating and writing checksum for fitsfile '%s'.\n", onefile);
                        fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                        stat = 0;
                     }

                     if (verbose)
                     {
                        fprintf(stdout, "Time to write checksum on fitsfile '%s' = %f sec.\n", onefile, PopTimer());
                     }
                  }

                  if (verbose)
                  {
                     PushTimer();
                  }
                  
                  fits_close_file(*pfptr, &stat);

                  if (stat == 0)
                  {
                     if (verbose)
                     {
                        fprintf(stdout, "Closing fits file '%s'.\n", onefile);
                     }
                  }
                  else
                  {
                     fits_get_errstatus(stat, cfitsiostat);
                     fprintf(stderr, "Closing all fitsfiles: error closing fitsfile '%s'.\n", onefile);
                     fprintf(stderr, "CFITSIO error '%s'\n", cfitsiostat);
                  }

                  if (verbose)
                  {
                     fprintf(stdout, "Time to close fitsfile '%s' = %f sec.\n", onefile, PopTimer());
                  }
               }
               else
               {
                  stat = 1;
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
            }

            list_llfree(&llist);
         }
      }

      hcon_destroy(&gFFPtrInfo);
      hcon_destroy(&gFFiles);
   }
}
