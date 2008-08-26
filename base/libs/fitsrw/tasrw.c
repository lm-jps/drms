#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "cfitsio.h"
#include "tasrw.h"
#include "cfitsio.h"
#include "fitsio.h"
#include "hcontainer.h"

/* Our current set up is 1024 maximum open files per process (/usr/include/linux/limits.h) */
#define MAXFFILES 128

HContainer_t *gFFiles = NULL;

/* fitsfiles can be opened either READWRITE or READONLY. If a request for a READONLY pointer
 * is made, and the fitsfile has already been opened, then regardless of the fitsfile write mode
 * it is okay to return the opened pointer. But, if the request is for a READWRITE pointer
 * and a READONLY fitsfile pointer exists, then the READONLY pointer must be closed, 
 * and the file must be reopened with READWRITE write mode.
 * 
 * The hash key to the gFFiles is <filename> ':' ('r' | 'w') where r implies readonly and w implies
 * readwrite.
 */
static fitsfile *fitsrw_getfptr(const char *filename, int writeable, int *status)
{
   fitsfile *fptr = NULL;
   fitsfile **pfptr = NULL;
   char filehashkey[PATH_MAX + 2];
   char tmpfilehashkey[PATH_MAX + 2];
   int stat = 0;

   if (!gFFiles)
   {
      gFFiles = hcon_create(sizeof(fitsfile *), sizeof(filehashkey), NULL, NULL, NULL, NULL, 0);
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
               fits_close_file(*pfptr, &stat);
            }
               
            hcon_remove(gFFiles, tmpfilehashkey);
            pfptr = NULL;
         }
      }
     
      if (fits_open_image(&fptr, filename, writeable ? READWRITE : READONLY, &stat)) 
      {
         if (status)
         {
            *status = CFITSIO_ERROR_FILE_DOESNT_EXIST;
         }
      }

      if (!stat && gFFiles->num_total > MAXFFILES)
      {
         /* Must close some open files */
         int ifile = 0;
         HIterator_t *hit = hiter_create(gFFiles);

         if (hit)
         {
            while (ifile < MAXFFILES / 2)
            {
               pfptr = (fitsfile **)hiter_getnext(hit);

               if (pfptr)
               {
                  if (*pfptr)
                  {
                     fits_close_file(*pfptr, &stat);
                  }

                  hcon_remove(gFFiles, filehashkey);

                  if (stat)
                  {
                     fprintf(stderr, "Error closing fitsfile '%s'.\n", filename);
                     break;
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
      }

      if (!stat)
      {
         hcon_insert(gFFiles, filehashkey, &fptr);
      }
   }

   return fptr;
}

int fitsrw_readslice(const char *filename, 
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

   fptr = fitsrw_getfptr(filename, 0, &status);

   if (!fptr)
   {
      goto error_exit;
   }

   error_code = fitsrw_read_keylist_and_image_info((FITSRW_fhandle)fptr, NULL, image_info);
   if(error_code != CFITSIO_SUCCESS) goto error_exit;

   /* fits_get_img_param(fptr, CFITSIO_MAX_DIM, &((*image_info)->bitpix), &((*image_info)->naxis), 
      &((*image_info)->naxes[0]), &status); */

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
   fits_set_imgnull(fptr, NULL, &status);
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

   /* fits_close_file(fptr, &status); */

   return CFITSIO_SUCCESS;

  error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);
   if(fptr) fits_close_file(fptr, &status);
   if(pixels) free(pixels);

   return error_code;
}

/* fpixel - location in TAS file of 'bottom left corner' of slice.
 * lpixel - location in TAS file of 'top right corner' of slice.
 */
int fitsrw_writeslice(const char *filename, int *fpixel, int *lpixel, void *image)
{
   fitsfile *fptr=NULL;     
   int error_code, status=0;
   int data_type;
   CFITSIO_IMAGE_INFO info;
   long lfpixel[CFITSIO_MAX_DIM];
   long llpixel[CFITSIO_MAX_DIM];
   int idim = 0;
   char cfitsiostat[FLEN_STATUS];

   // Get the image dimensions from file 
   memset((void*) &info,0,sizeof(CFITSIO_IMAGE_INFO));

   status = 0; // first thing!

   fptr = fitsrw_getfptr(filename, 1, &status);

   if (!fptr)
   {
      goto error_exit;
   }
  
   fits_get_img_param(fptr, CFITSIO_MAX_DIM, &info.bitpix, &info.naxis, 
		      &info.naxes[0], &status); 
   if(info.naxis == 0)
   {
      error_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   switch(info.bitpix)
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
   fits_set_imgnull(fptr, NULL, &status);
   if(status) goto error_exit;

   /* ffgsv requires long type, although this is dangerous 
    * as long is a different number of bytes on different 
    * machines. */
   memset(lfpixel, 0, sizeof(lfpixel));
   memset(llpixel, 0, sizeof(llpixel));
   
   for (idim = 0; idim < info.naxis; idim++)
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

   //Set stripe, slice, block
   if(fits_write_subset(fptr, data_type, lfpixel, llpixel, image, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   /* fits_close_file(fptr, &status); */
   //fits_close_file(fptr, &status); 
   return CFITSIO_SUCCESS;

 error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);
   if(fptr) fits_close_file(fptr, &status);

   return error_code;
}

void fitsrw_closefptrs()
{
   if (gFFiles)
   {
      HIterator_t *hit = hiter_create(gFFiles);
      fitsfile **pfptr = NULL;
      int stat = 0;
      const char *filehashkey = NULL;

      if (hit)
      {
         while ((pfptr = (fitsfile **)hiter_extgetnext(hit, &filehashkey)) != NULL)
         {
            if (*pfptr)
            {
               fits_close_file(*pfptr, &stat);
            }

            hcon_remove(gFFiles, filehashkey);

            if (stat)
            {
               fprintf(stderr, "Error closing fitsfile '%s'.\n", filehashkey);
               break;
            }
         }

         hiter_destroy(&hit);
      }

      hcon_destroy(&gFFiles);
   }
}
