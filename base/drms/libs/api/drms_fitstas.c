#include "drms.h"
#include "drms_priv.h"
#include "cfitsio.h"
#include "tasrw.h"

int drms_fitstas_create(DRMS_Env_t *env,
                        const char *filename, 
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
         if (fitsrw_writeintfile(env->verbose, filename, &info, NULL, comp, NULL) != CFITSIO_SUCCESS)
         {
            fprintf(stderr, "couldn't create FITS TAS file '%s'\n", filename); 
            status = DRMS_ERROR_CANTCREATETASFILE;
         }
         else
         {
            /* axis[naxis] == unitsize, which mean that fitsrw_writeintfile() created
             * a cube with the value of the last dimension (the slice dimension) set
             * to the unitsize. A cube was created with all slices present, all pixel
             * values were set to the empty value, and the slice NAXIS FITS keyword
             * was set to the unitsize.
             *
             * However, we want to pretend that we have not written any slices just yet.
             * As slices are written, we keep track of the index of the slice, saving
             * the highest index. In this way, we know how big the final slice-dimension
             * will be. Then when we close the TAS file, we truncate the 'blank' slices
             * at the end of the cube - the ones that were never written.
             *
             * fitsrw_writeintfile() called fitsrw_setfpinfo_ext() to set the slice NAXIS
             * value to the unitsize. In order to pretend that no slices have been yet written,
             * we need to set this value to 0.
             */
            int fitsrwErr = CFITSIO_SUCCESS;
            
            if (fitsrw_initializeTAS(env->verbose, filename) != CFITSIO_SUCCESS)
            {
                fprintf(stderr, "could not initialize FITS TAS file '%s'\n", filename);
                status = DRMS_ERROR_CANTCREATETASFILE;
            }
         }
      }
      else
      {
         fprintf(stderr, "couldn't set FITS TAS file image info\n"); 
         status = DRMS_ERROR_CANTCREATETASFILE;
      }
   }
   else
   {
      status = DRMS_ERROR_INVALIDTYPE;
   }

   return status;
}

int drms_fitstas_readslice(DRMS_Env_t *env,
                           const char *filename, 
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

   status = drms_fitsrw_readslice(env, filename, naxis + 1, start, end, arr);

   if (status == DRMS_SUCCESS)
   {
      /* arr has one extra dimension in it, but always of length one */
      ((*arr)->axis)[naxis] = 0;
      (*arr)->naxis--;
   }

   return status;
}

/* Array may be converted in calling function, but not here */
int drms_fitstas_writeslice(DRMS_Env_t *env,
                            DRMS_Segment_t *seg,
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

    /* No need to specify final image size. drms_create_records() will have already 
     * created an "empty" image of final dimensions. */
   status = drms_fitsrw_writeslice(env, seg, filename, naxis, start, end, arrayout);

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
