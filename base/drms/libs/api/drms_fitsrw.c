#include "drms_fitsrw.h"
#include "drms.h"
#include "tasrw.h"

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
      int nelem = drms_array_count(arr);
      DRMS_Value_t val;
      long long dataval;

      while (nelem > 0)
      {
	 void *elem = (char *)arr->data + nelem;

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

void drms_fitsrw_term()
{
   fitsrw_closefptrs();
}
