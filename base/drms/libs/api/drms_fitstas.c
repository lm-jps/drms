#include "drms.h"
#include "drms_priv.h"
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
   DRMS_Array_t arr;
   memset(&arr, 0, sizeof(DRMS_Array_t));

   if (type != DRMS_TYPE_RAW)
   {
      arr.type = type;
      arr.naxis = naxis;
      memcpy(arr.axis, axis, sizeof(int) * naxis);
      arr.bzero = bzero;
      arr.bscale = bscale;

      /* If bzero == 0.0 and bscale == 1.0, then the TAS file has physical units 
       * (data are NOT 'raw'). */
      if (bzero == 0.0 && bscale == 1.0)
      {
         arr.israw = 0;
      }
      else
      {
         arr.israw = 1;
      }

      if (!drms_fitsrw_SetImageInfo(&arr, &info))
      {
         if (fitsrw_writeintfile(filename, &info, NULL, comp, NULL) != CFITSIO_SUCCESS)
         {
            fprintf(stderr, "Couldn't create FITS TAS file '%s'.\n", filename); 
            status = DRMS_ERROR_CANTCREATETASFILE;
         }
      }
      else
      {
         fprintf(stderr, "Couldn't set FITS TAS file image info.\n"); 
         status = DRMS_ERROR_CANTCREATETASFILE;
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDTYPE;
   }

   return status;
}

int drms_fitstas_readslice(const char *filename, 
                           int naxis,
                           int *axis,
                           int *lower,
                           int *upper,
                           int slotnum,
                           DRMS_Array_t **arr)
{
   int status = DRMS_SUCCESS;
   int start[DRMS_MAXRANK+1];
   int end[DRMS_MAXRANK+1];
   int i;

   if (lower && upper)
   {
      memcpy(start, lower, naxis * sizeof(int));
      memcpy(end, upper, naxis * sizeof(int));
   }
   else
   {
      for (i = 0; i < naxis; i++)
      {
         start[i] = 0;
         end[i] = axis[i] - 1;
      }
   }

   start[naxis] = slotnum;
   end[naxis] =  slotnum;

   status = drms_fitsrw_readslice(filename, naxis + 1, start, end, arr);

   if (status == DRMS_SUCCESS)
   {
      /* arr has one extra dimension in it, but always of length one */
      ((*arr)->axis)[naxis] = 0;
      (*arr)->naxis--;
   }

   return status;
}

/* Array may be converted in calling function, but not here */
int drms_fitstas_writeslice(DRMS_Segment_t *seg,
                            const char *filename, 
                            int naxis,
                            int *axis,
                            int *lower,
                            int *upper,
                            int slotnum,
                            DRMS_Array_t *arrayout)
{
   int status = DRMS_SUCCESS;
   int start[DRMS_MAXRANK] = {0};
   int end[DRMS_MAXRANK] = {0};
   int iaxis;

   if (lower && upper)
   {
      memcpy(start, lower, naxis * sizeof(int));
      memcpy(end, upper, naxis * sizeof(int));
   }
   else
   {
      for (iaxis = 0; iaxis < naxis; iaxis++)
      {
         end[iaxis] = start[iaxis] + axis[iaxis] - 1;
      }
   }

   start[naxis] = slotnum;
   end[naxis] = slotnum;

   status = drms_fitsrw_writeslice(seg, filename, naxis, start, end, arrayout);

   if (status == DRMS_SUCCESS)
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
