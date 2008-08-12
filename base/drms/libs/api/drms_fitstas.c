#include "drms_fitstas.h"
#include "drms_fitsrw.h"
#include "cfitsio.h"
#include "tasrw.h"

int drms_fitstas_create(const char *filename, 
                        const char *comp,
                        DRMS_Type_t type, 
                        int naxis, 
                        int *axis,
                        double bzero,
                        double bscale)
{
   int status = DRMS_SUCCESS;
   CFITSIO_IMAGE_INFO info;
   DRMS_Array_t *arr = NULL;

   if (type != DRMS_TYPE_RAW)
   {
      arr = drms_array_create(type, naxis, axis, NULL, &status);

      if (status == DRMS_SUCCESS)
      {
         arr->bzero = bzero;
         arr->bscale = bscale;

         /* If bzero == 0.0 and bscale == 1.0, then the TAS file has physical units 
          * (data are NOT 'raw'). */
         if (bzero == 0.0 && bscale == 1.0)
         {
            arr->israw = 0;
         }
         else
         {
            arr->israw = 1;
         }

         drms_array2missing(arr);

         if (!drms_fitsrw_SetImageInfo(arr, &info))
         {
            if (cfitsio_write_file(filename, &info, arr->data, comp, NULL) != CFITSIO_SUCCESS)
            {
               fprintf(stderr, "Couldn't create FITS TAS file '%s'.\n", filename); 
               status = DRMS_ERROR_CANTCREATETASFILE;
            }
         }
         else
         {
            fprintf(stderr, "Couldn't create empty FITS TAS file array.\n"); 
            status = DRMS_ERROR_CANTCREATETASFILE;
         }

         drms_free_array(arr);
      }
      else
      {
         fprintf(stderr, "Couldn't create FITS TAS file array.\n"); 
         status = DRMS_ERROR_ARRAYCREATEFAILED;
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDTYPE;
   }

   return status;
}

/* Array may be converted in calling function, but not here */
int drms_fitstas_readslice(const char *filename, 
                           int naxis,
                           int *axis,
                           int slotnum,
                           DRMS_Array_t **arr)
{
   int status = DRMS_SUCCESS;
   CFITSIO_IMAGE_INFO *info = NULL;
   void *image = NULL;
   int fitsrwstat = CFITSIO_SUCCESS;
   int start[DRMS_MAXRANK+1];
   int end[DRMS_MAXRANK+1];
   int i;

   for (i = 0; i < naxis; i++)
   {
      start[i] = 0;
      end[i] = axis[i] - 1;
   }

   start[naxis] = slotnum;
   end[naxis] =  slotnum;

   fitsrwstat = fitsrw_readslice(filename, start, end, &info, &image);

   if (fitsrwstat != CFITSIO_SUCCESS)
   {
      status = DRMS_ERROR_FITSRW;
      fprintf(stderr, "FITSRW error '%d'.\n", fitsrwstat);
   }
   else
   {
      /* image has one extra dimension, but its length is 1 (so the extra dim is superfluous) */
      if (naxis + 1 != info->naxis)
      {
         fprintf(stderr, "TAS file axis mismatch.\n");
         status = DRMS_ERROR_FITSRW;
      }
      else
      {
         info->naxis--;
         info->naxes[naxis] = 0;

         if (drms_fitsrw_CreateDRMSArray(info, image, arr))
         {
            status = DRMS_ERROR_ARRAYCREATEFAILED;
         }
      }

      /* Don't free image - arr has stolen it. */
      cfitsio_free_these(&info, NULL, NULL);
   }

   return status;
}

/* Array may be converted in calling function, but not here */
int drms_fitstas_writeslice(DRMS_Segment_t *seg,
                            const char *filename, 
                            int naxis,
                            int *axis,
                            int slotnum,
                            DRMS_Array_t *arrayout)
{
   int status = DRMS_SUCCESS;
   int fitsrwstat = CFITSIO_SUCCESS;
   int start[DRMS_MAXRANK] = {0};
   int end[DRMS_MAXRANK] = {0};
   int iaxis;

   start[naxis] = slotnum;

   for (iaxis = 0; iaxis < naxis; iaxis++)
   {
      end[iaxis] = start[iaxis] + axis[iaxis] - 1;
   }

   end[naxis] = slotnum;

   fitsrwstat = fitsrw_writeslice(filename, 
                                  start, 
                                  end,
                                  arrayout->data);

   if (fitsrwstat != CFITSIO_SUCCESS)
   {
      status = DRMS_ERROR_FITSRW;
      fprintf(stderr, "FITSRW error '%d'.\n", fitsrwstat);
   }
   else
   {
      /* arrayout->bzero and arrayout->bscale must be saved, if an appropriately named 
       * keyword exists.  The TAS FITS file cannot save record-and-segment-specific
       * keywords.*/
      char kw[DRMS_MAXKEYNAMELEN];
      snprintf(kw, sizeof(kw), "%s_bzero", seg->info->name);
      status = drms_setkey_double(seg->record, kw, arrayout->bzero);

      if (status == DRMS_ERROR_UNKNOWNKEYWORD)
      {
         fprintf(stderr, "%s keyword not defined, cannot save bzero.\n", kw);
      }
      else
      {
         snprintf(kw, sizeof(kw), "%s_bscale", seg->info->name);
         status = drms_setkey_double(seg->record, kw, arrayout->bscale);

         if (status == DRMS_ERROR_UNKNOWNKEYWORD)
         {
            fprintf(stderr, "%s keyword not defined, cannot save bscale.\n", kw);
         }
      }
   }

   return status;
}
