#include "drms.h"
#include "drms_priv.h"
#include "tasrw.h"
#include "fitsexport.h"

const char kSIMPLE[] = "SIMPLE";
const char kBITPIX[] = "BITPIX";
const char kNAXIS[] = "NAXIS";
const char kNAXISn[] = "NAXIS%d";

DRMS_Type_t drms_fitsrw_Bitpix2Type(int bitpix, int *err)
{
   DRMS_Type_t type = DRMS_TYPE_CHAR;
   int error = 0;

   switch(bitpix)
   {
      case 8:
	type = DRMS_TYPE_CHAR; 
	break;
      case 16:
	type = DRMS_TYPE_SHORT;
	break;
      case 32:
	type = DRMS_TYPE_INT;
	break;
      case 64:
	type = DRMS_TYPE_LONGLONG;  
	break;
      case -32:
	type = DRMS_TYPE_FLOAT;
	break;
      case -64:
	type = DRMS_TYPE_DOUBLE; 	
	break;
      default:
	fprintf(stderr, "ERROR: Invalid bitpix value %d\n", bitpix);
	error = 1;
	break;
   }

   if (err)
   {
      *err = error;
   }

   return type;
}

int drms_fitsrw_Type2Bitpix(DRMS_Type_t type, int *err)
{
   int bitpix = 0;
   int error = 0;

   switch(type)
   {
      case DRMS_TYPE_CHAR: 
	bitpix = 8;
	break;
      case DRMS_TYPE_SHORT:
	bitpix = 16;
	break;
      case DRMS_TYPE_INT:  
	bitpix = 32;
	break;
      case DRMS_TYPE_LONGLONG:  
	bitpix = 64;
	break;
      case DRMS_TYPE_FLOAT:
	bitpix = -32;
	break;
      case DRMS_TYPE_DOUBLE: 	
      case DRMS_TYPE_TIME: 
	bitpix = -64;
	break;
      case DRMS_TYPE_STRING: 
      default:
	fprintf(stderr, "ERROR: Unsupported DRMS type %d\n", (int)type);
	error = 1;
	break;
   }

   if (err)
   {
      *err = error;
   }

   return bitpix;
}

void drms_fitsrw_ShootBlanks(DRMS_Array_t *arr, long long blank)
{
   if (arr->type != DRMS_TYPE_FLOAT &&
       arr->type != DRMS_TYPE_DOUBLE &&
       arr->type != DRMS_TYPE_TIME)
   {
      arraylen_t nelem = drms_array_count(arr);
      DRMS_Value_t val;
      long long dataval;

      while (nelem > 0)
      {
	 void *elem = (char *)arr->data + nelem - 1;

	 DRMS_VAL_SET(arr->type, elem, val);
	 dataval = conv2longlong(arr->type, &(val.value), NULL);

	 if (dataval == blank)
	 {
	    drms_missing_vp(arr->type, elem);
	 }

	 nelem--;
      }
   }
}

int drms_fitsrw_CreateDRMSArray(CFITSIO_IMAGE_INFO *info, void *data, DRMS_Array_t **arrout)
{
   DRMS_Array_t *retarr = NULL;
   int err = 0;
   DRMS_Type_t datatype;
   int drmsstatus = DRMS_SUCCESS;
   int naxis = 0;
   int axes[DRMS_MAXRANK] = {0};
   int ia;

   if (info && data && arrout)
   {
      if (!(info->bitfield & kInfoPresent_SIMPLE) || !info->simple)
      {
	 err = 1;
	 fprintf(stderr, "Simple FITS file expected.\n");
      }

      if (!err)
      {
	 datatype = drms_fitsrw_Bitpix2Type(info->bitpix, &err);
	 if (err)
	 {
	    fprintf(stderr, "BITPIX = %d not allowed\n", info->bitpix);
	 }
      }

      if (!err)
      {
	 if (info->naxis >= 1 && info->naxis <= DRMS_MAXRANK)
	 {
	    naxis = info->naxis;
	 }
	 else
	 {
	    err = 1;
	    fprintf(stderr, 
		    "%s = %d outside allowable DRMS range [1-%d].\n", 
		    kNAXIS,
		    info->naxis,
		    DRMS_MAXRANK);
	 }
      }

      if (!err)
      {
	 for (ia = 0; ia < naxis; ia++)
	 {
	    axes[ia] = (int)(info->naxes[ia]);
	 }
      }

      if (!err)
      {
	 /* retarr steals data */
	 retarr = drms_array_create(datatype, naxis, axes, data, &drmsstatus);
      }
      else
      {
	 err = 1;
      }

      retarr->bzero = 0.0;
      retarr->bscale = 1.0;

      if (!err && info->bitpix > 0)
      {
	 /* BLANK isn't stored anywhere in DRMS.  Just use DRMS_MISSING_XXX. 
	  * But need to convert data blanks to missing. */
	 if (info->bitfield & kInfoPresent_BLANK)
	 {
	    drms_fitsrw_ShootBlanks(retarr, info->blank);
	 }

	 if (info->bitfield & kInfoPresent_BZERO)
	 {
	    retarr->bzero = info->bzero;
	 }

	 if (info->bitfield & kInfoPresent_BSCALE)
	 {
	    retarr->bscale = info->bscale;
	 }
      }

      if (!err)
      {
	 retarr->israw = 1; /* FITSRW should never apply BSCALE or BZERO */
	 *arrout = retarr;
      }
   }

   return err;
}

int drms_fitsrw_SetImageInfo(DRMS_Array_t *arr, CFITSIO_IMAGE_INFO *info)
{
    int err = 0;
    int ia;
    
    if (info)
    {
        memset(info, 0, sizeof(CFITSIO_IMAGE_INFO));
        info->bitpix = drms_fitsrw_Type2Bitpix(arr->type, &err);
        
        if (!err)
        {
            if (arr->naxis > 0)
            {
                info->naxis = arr->naxis;
            }
            else
            {
                err = 1;
            }
        }
        
        if (!err)
        {
            for (ia = 0; ia < arr->naxis; ia++)
            {
                info->naxes[ia] = (long)(arr->axis[ia]);
            }
            
            info->simple = 1;
            info->extend = 0; /* baby steps - trying to dupe what happens in FITS protocol. */
            info->bitfield = (info->bitfield | kInfoPresent_SIMPLE);
            
            if (info->bitpix > 0)
            {
                /* An integer type - need to set BLANK, and possibly BZERO and BSCALE. */
                DRMS_Type_Value_t missing;
                drms_missing(arr->type, &missing);
                
                info->blank = conv2longlong(arr->type, &missing, NULL);
                info->bitfield = (info->bitfield | kInfoPresent_BLANK);
                
                if (arr->israw)
                {
                    /* This means that the data COULD BE not real values,
                     * and to get real values they need to be scaled by bzero/bscale. */
#ifdef ICCCOMP
#pragma warning (disable : 1572)
#endif
                    if (arr->bscale != 1.0 || fabs(arr->bzero) != 0.0)
                    {
                        info->bscale = arr->bscale;
                        info->bzero = arr->bzero;
                        
                        info->bitfield = (info->bitfield | kInfoPresent_BSCALE);
                        info->bitfield = (info->bitfield | kInfoPresent_BZERO);
                    }
#ifdef ICCCOMP
#pragma warning (default : 1572)
#endif
                }
            }
        }
    }
    
    return err;
}

int drms_fitsrw_GetSimpleFromInfo(CFITSIO_IMAGE_INFO *info)
{
    if (info->bitfield & kInfoPresent_SIMPLE)
    {
        return info->simple;
    }
    else
    {
        /* If not set, then the assumption is true. */
        return 1;
    }
}

int drms_fitsrw_GetExtendFromInfo(CFITSIO_IMAGE_INFO *info)
{
    if (info->bitfield & kInfoPresent_EXTEND)
    {
        return info->extend;
    }
    else
    {
        /* If not set, then the assumption is false. */
        return 0;
    }
}

long long drms_fitsrw_GetBlankFromInfo(CFITSIO_IMAGE_INFO *info)
{
    if (info->bitfield & kInfoPresent_BLANK)
    {
        return info->blank;
    }
    else
    {
        /* If not set, then the assumption is the DRMS missing value. */
        DRMS_Type_Value_t missing;
        DRMS_Type_t dtype;
        int err;
        
        dtype = drms_fitsrw_Bitpix2Type(info->bitpix, &err);
        if (!err)
        {
            drms_missing(dtype, &missing);
        }
        else
        {
            /* Not sure what to do - we don't know the data type. I guess assume CHAR (the only value that will 'fit' in all data types)
             * and print an error message. */
            fprintf(stderr, "Unable to convert bitpix %d to a DRMS data type.\n", info->bitpix);
            drms_missing(DRMS_TYPE_CHAR, &missing);
        }
        
        return conv2longlong(dtype, &missing, NULL);
    }
}

double drms_fitsrw_GetBscaleFromInfo(CFITSIO_IMAGE_INFO *info)
{
    if (info->bitfield & kInfoPresent_BSCALE)
    {
        return info->bscale;
    }
    else
    {
        /* If not set, then the assumption is 1.0. */
        return 1.0;
    }
}

double drms_fitsrw_GetBzeroFromInfo(CFITSIO_IMAGE_INFO *info)
{
    if (info->bitfield & kInfoPresent_BZERO)
    {
        return info->bzero;
    }
    else
    {
        /* If not set, then the assumption is 0.0. */
        return 0.0;
    }
}

void drms_fitsrw_term(int verbose)
{
   fitsrw_closefptrs(verbose);
}

void drms_fitsrw_close(int verbose, const char *filename)
{
    fitsrw_closefptrByName(verbose, filename);
}

DRMS_Array_t *drms_fitsrw_read(DRMS_Env_t *env,
                               const char *filename,
                               int readraw,
                               HContainer_t **keywords,
                               int *status)
{
   int statusint = DRMS_SUCCESS;
   CFITSIO_IMAGE_INFO *info = NULL;
   void *image = NULL;
   CFITSIO_KEYWORD* keylist = NULL;
   CFITSIO_KEYWORD* fitskey = NULL;
   int fitsrwstat = CFITSIO_SUCCESS;
   DRMS_Array_t *arr = NULL;

   /* fitsrw_read always reads 'RAW' - it does NOT apply bzero/bscale */
   fitsrwstat = fitsrw_read(env->verbose, filename, &info, &image, &keylist);

   if (fitsrwstat != CFITSIO_SUCCESS)
   {
      statusint = DRMS_ERROR_FITSRW;
      fprintf(stderr, "FITSRW error '%d'.\n", fitsrwstat);
   }
   else
   {
      /* sets arr->israw = 1 */
      if (drms_fitsrw_CreateDRMSArray(info, image, &arr))
      {
         statusint = DRMS_ERROR_ARRAYCREATEFAILED;
      }
      else
      {
         /* At this point, we have a RAW array - if readraw == 0, unscale. */
         if (!readraw && (arr->bscale != 1.0 || arr->bzero != 0.0))
         {
            drms_array_convert_inplace(arr->type, arr->bzero, arr->bscale, arr);
            arr->israw = 0;    
         }

         /* Must iterate through keylist and create DRMS keywords */
         if (keywords)
         {
            if (!*keywords)
            {
               /* User is filling up a new container with detached (from rec) keywords. */
               *keywords = hcon_create(sizeof(DRMS_Keyword_t), 
                                       DRMS_MAXKEYNAMELEN, 
                                       (void (*)(const void *)) drms_free_template_keyword_struct,
                                       NULL, 
                                       NULL,
                                       NULL,
                                       0);
            }

            /* else User is filling up an existing container attached to a rec. */

            fitskey = keylist;
            while (fitskey != NULL)
            {
               /* For now, don't use map or class to map fits to drms keywords */
               if (fitsexport_mapimportkey(fitskey, NULL, NULL, *keywords, env->verbose))
               {
                  fprintf(stderr, "Error importing fits keyword '%s'; skipping.\n", fitskey->key_name);
               }

               fitskey = fitskey->next;
            }
         }

         /* Don't free image - arr has stolen it. */
         cfitsio_free_these(&info, NULL, &keylist);
      }
   }
   
   if (status)
   {
      *status = statusint;
   }

   return arr;
}

/* Only call this when keywords are headless (not attached to a record) */
void drms_fitsrw_freekeys(HContainer_t **keywords)
{
   if (keywords)
   {
      DRMS_Keyword_t *key = NULL;
      HIterator_t *hit = hiter_create(*keywords);

      /* Must free key->info, key->value (if it is a string type) */
      while ((key = hiter_getnext(hit)) != NULL)
      {
         if (!key->record && key->info)
         {
            if (key->info->type == DRMS_TYPE_STRING)
            {
               free((key->value).string_val);
            }

            free(key->info);
         }
      }

      hiter_destroy(&hit);

      hcon_destroy(keywords);
   }
}

/* Array may be converted in calling function, but not here */
int drms_fitsrw_readslice(DRMS_Env_t *env,
                          const char *filename, 
                          int naxis,
                          int *start,
                          int *end,
                          DRMS_Array_t **arr)
{
   int status = DRMS_SUCCESS;
   CFITSIO_IMAGE_INFO *info = NULL;
   void *image = NULL;
   int fitsrwstat = CFITSIO_SUCCESS;

   /* Check start and end - end > start, and they should fall into 
    * an acceptable range. */

   /* This call really should take naxis as a parameter so that it knows how many values 
    * are in start and end. */
   fitsrwstat = fitsrw_readslice(env->verbose, filename, start, end, &info, &image);

   if (fitsrwstat != CFITSIO_SUCCESS)
   {
      status = DRMS_ERROR_FITSRW;
      fprintf(stderr, "FITSRW error '%d'.\n", fitsrwstat);
   }
   else
   {
      if (naxis != info->naxis)
      {
         fprintf(stderr, "TAS file axis mismatch.\n");
         status = DRMS_ERROR_FITSRW;
      }
      else
      {
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

int drms_fitsrw_write(DRMS_Env_t *env,
                      const char *filename,
                      const char* cparms,
                      HContainer_t *keywords,
                      DRMS_Array_t *arr)
{
   int status = DRMS_SUCCESS;
   CFITSIO_IMAGE_INFO imginfo;
   void *image = NULL;
   CFITSIO_KEYWORD* keylist = NULL;
   HIterator_t hit;
   DRMS_Keyword_t *key = NULL;
   const char *keyname = NULL;
   int fitsrwstat;

   if (arr && arr->data && keywords)
   {
      /* iterate through keywords and populate keylist */
      hiter_new_sort(&hit, keywords, drms_keyword_ranksort); 
      while ((key = hiter_getnext(&hit)) != NULL)
      {
         if (!drms_keyword_getimplicit(key))
         {
            /* will reject DRMS keywords that collide with FITS reserved 
             * keywords, like SIMPLE */
            if (fitsexport_exportkey(key, &keylist))
            {
               keyname = drms_keyword_getname(key);
               fprintf(stderr, "Couldn't export keyword '%s'.\n", keyname);
            }
         }
      }

      if (keylist)
      {
         cfitsio_free_keys(&keylist);
      }

      /* extract image array */
      image = arr->data;

      /* create image info */
      if (!drms_fitsrw_SetImageInfo(arr, &imginfo))
      {
         if (arr->type == DRMS_TYPE_STRING ||
             arr->type == DRMS_TYPE_TIME ||
             arr->type == DRMS_TYPE_RAW)
         {
            fprintf(stderr, "Type '%s' not supported in FITS.\n", drms_type2str(arr->type));
            status = DRMS_ERROR_FITSRW;
         }
         else
         {
            fitsrwstat = fitsrw_write(env->verbose, filename, &imginfo, image, cparms, keylist);
            if (fitsrwstat != CFITSIO_SUCCESS)
            {
               status = DRMS_ERROR_FITSRW;
               fprintf(stderr, "FITSRW error '%d'.\n", fitsrwstat);
            }     
         }
      }
      else
      {
         fprintf(stderr, "Data array being exported is invalid.\n");
         status = DRMS_ERROR_FITSRW;
      }
   }
   else
   {
      fprintf(stderr, "WARNING: no data to write to FITS.\n");
   }
   
   return status;
}

/* Array may be converted in calling function, but not here */
int drms_fitsrw_writeslice_ext(DRMS_Env_t *env,
                               DRMS_Segment_t *seg,
                               const char *filename, 
                               int naxis,
                               int *start,
                               int *end,
                               int *finaldims,
                               DRMS_Array_t *arrayout)
{
   int status = DRMS_SUCCESS;
   int fitsrwstat = CFITSIO_SUCCESS;

   /* If the file doesn't exist, then must create one with missing data. THIS SHOULD 
    * NEVER BE THE CASE WITH TAS FILES.  They get created when the DRMS record gets
    * created. */
   struct stat stbuf;
   if (stat(filename, &stbuf) == -1)
   {
      CFITSIO_IMAGE_INFO info;
      DRMS_Array_t arr;
      int usearrout;
      int idim;

      memset(&arr, 0, sizeof(DRMS_Array_t));

      /* Write a "blank" file by calling fitsrw_writeintfile() with a NULL pointer for the image */

      if (seg->info->type != DRMS_TYPE_RAW)
      {
         arr.type = seg->info->type;
         arr.naxis = seg->info->naxis;
         memcpy(arr.axis, seg->axis, sizeof(int) * seg->info->naxis);
         arr.bzero = seg->bzero;
         arr.bscale = seg->bscale;

         /* If bzero == 0.0 and bscale == 1.0, then the file has physical units 
          * (data are NOT 'raw'). */
         if (seg->bzero == 0.0 && seg->bscale == 1.0)
         {
            arr.israw = 0;
         }
         else
         {
            arr.israw = 1;
         }

         
         usearrout = 0;

         /* The jsd axis information is only valid if all but the last dimension are non-zero. If the jsd
          * information is invalid, use the output array to obtain the first n - 1 dimensions. If there
          * is only one dimension, then skip this determination - the jsd will automatically be 
          correct. */
          
          if (finaldims)
          {
              /* Override the jsd's axis lengths AND the output array's lengths. Use
               * the values contained in finaldims. Ensure the values are 
               * larger than the output-array-dimension values. */
              idim = 0;
              while (idim < arr.naxis)
              {
                  if (finaldims[idim] < arrayout->axis[idim]) 
                  {
                      status = DRMS_ERROR_INVALIDDIMS;
                      break;
                  }
                  else
                  {
                      arr.axis[idim] = finaldims[idim];
                  }
                  
                  idim++;
              }
          }
          else
          {
              idim = 0;
              while (idim < arr.naxis - 1)
              {
                  if (arr.axis[idim++] == 0)
                  {
                      usearrout = 1;
                      break;
                  }
              }
              
              if (usearrout)
              {
                  idim = 0;
                  while (idim < arr.naxis - 1)
                  {
                      arr.axis[idim] = arrayout->axis[idim];
                      idim++;
                  }
              }
          }

          if (status == DRMS_SUCCESS)
          {
              if (arr.axis[arr.naxis - 1] == 0)
              {
                  /* A last-dimension length of zero implies that the total number of slices in 
                   * the cube is unknown. Although this is typically the case for VARDIM segments,
                   * this scenario isn't restricted to VARDIM. And it may be known at JSD-creation
                   * time, for VARMDIM segments, was the last dimension length is. */
                  
                  /* Write a file with a last dimension of 1. CFITSIO will automatically 
                   * increase the size of the last dimension before it closes the file, 
                   * IF the relevant NAXISn keyword is updated with the appropriate length
                   * before the file is closed. */
                  arr.axis[arr.naxis - 1] = 1;
              }
              
              if (!drms_fitsrw_SetImageInfo(&arr, &info))
              {
                  if (fitsrw_writeintfile(env->verbose, filename, &info, NULL, seg->cparms, NULL) != CFITSIO_SUCCESS)
                  {
                      fprintf(stderr, "Couldn't create FITS file '%s'.\n", filename); 
                      status = DRMS_ERROR_CANTCREATETASFILE;
                  }
                  
                  /* At this point, the first n-1 dimension lengths are set in stone. These lengths originated
                   * from either the output array or the jsd. The nth length is not set and could increase 
                   * as slices are written. As we write slices, we need to check the nth dimension of the 
                   * slice being written. If the slice's largest value of this dimension is greater than 
                   * the existing value stored in memory (there is a TASRW_FilePtrInfo_t that 
                   * holds the lenghts of all dimensions), then we need to increase the value stored in memory
                   * to this largest value. When the file gets closed, we then need to update the nth NAXIS keyword
                   * in the FITS file. To do this we need a dirty flag in the TASRW_FilePtrInfo_t struct. We set the
                   * dirty flag if we have ever increased the value of the nth length in TASRW_FilePtrInfo_t. */
              }
              else
              {
                  fprintf(stderr, "Couldn't set FITS file image info.\n"); 
                  status = DRMS_ERROR_CANTCREATETASFILE;
              }
          }
      }
      else
      {
         status = DRMS_ERROR_INVALIDTYPE;
      }
   } /* file doesn't exist */

   if (status == DRMS_SUCCESS)
   {
      if (naxis != arrayout->naxis)
      {
         fprintf(stderr, "TAS file axis mismatch.\n");
         status = DRMS_ERROR_FITSRW;
      }
      else
      {
         /* This call really should take naxis as a parameter so that it knows how many values 
          * are in start and end. */
         fitsrwstat = fitsrw_writeslice(env->verbose,
                                        filename, 
                                        start, 
                                        end,
                                        arrayout->data);
   

         if (fitsrwstat != CFITSIO_SUCCESS)
         {
            fprintf(stderr, "FITSRW error '%d'.\n", fitsrwstat);
            status = DRMS_ERROR_FITSRW;
         }
      }
   }

   return status;
}

int drms_fitsrw_writeslice(DRMS_Env_t *env,
                           DRMS_Segment_t *seg,
                           const char *filename, 
                           int naxis,
                           int *start,
                           int *end,
                           DRMS_Array_t *arrayout)
{
    return drms_fitsrw_writeslice_ext(env, seg, filename, naxis, start, end, NULL, arrayout);
}
