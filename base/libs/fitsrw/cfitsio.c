/****************************************************************************/
// CFITSIO.C
//
// Functions for read/write FITS file format  using CFITSIO library.
//
//
/****************************************************************************/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "fitsio.h"
#include "cfitsio.h"
#include "jsoc.h"
#include "foundation.h"
#include "tasrw.h"
#include "xassert.h"

/****************************************************************************/

#ifdef  __CFISTIO_DEBUG__           //Turn on/off DEBUGMSG
#define DEBUGMSG(msg)  printf msg   //ex: DEBUGMSG ((stderr, "Hello World %d\n", 123));
#else
#define DEBUGMSG(msg)  //nop
#endif

const unsigned int kInfoPresent_SIMPLE = 0x00000001;
const unsigned int kInfoPresent_EXTEND = 0x00000002;
const unsigned int kInfoPresent_BLANK  = 0x00000004;
const unsigned int kInfoPresent_BSCALE = 0x00000008;
const unsigned int kInfoPresent_BZERO  = 0x00000010;
const unsigned int kInfoPresent_Dirt   = 0x00000020;

#define kMISSPIXBLOCK 1310720 /* 10 MB of long long */

/* Half-assed - just enough to get past a bug.  Should really check that the value
 * doesn't fall outside the range of the destination type.
 */
static int cfitsio_keyval2int(const CFITSIO_KEYWORD *kw, int *stat)
{
   int val = 0;
   int err = 1;

   if (kw)
   {
      switch(kw->key_type)
      {
         case kFITSRW_Type_String:
           if (kw->key_value.vs)
           {
              err = (sscanf(kw->key_value.vs, "%d", &val) != 1);
           }
           else
           {
              /* No string to convert - default to 0. */
              err = 0;
           }
           break;
         case kFITSRW_Type_Logical:
           val = (int)(kw->key_value.vl);
           err = 0;
           break;
         case kFITSRW_Type_Integer:
           val = (int)(kw->key_value.vi);
           err = 0;
           break;
         case kFITSRW_Type_Float:
           val = (int)(kw->key_value.vf);
           err = 0;
           break;
         default:
           fprintf(stderr, "Unsupported data type '%d'.", kw->key_type);
      }
   }

   if (stat)
   {
      *stat = err;
   }

   return val;
}

static long long cfitsio_keyval2longlong(const CFITSIO_KEYWORD *kw, int *stat)
{
   long long val = 0;
   int err = 1;

   if (kw)
   {
      switch(kw->key_type)
      {
         case kFITSRW_Type_String:
           if (kw->key_value.vs)
           {
              err = (sscanf(kw->key_value.vs, "%lld", &val) != 1);
           }
           else
           {
              err = 0;
           }
           break;
         case kFITSRW_Type_Logical:
           val = (long long)(kw->key_value.vl);
           err = 0;
           break;
         case kFITSRW_Type_Integer:
           val = (long long)(kw->key_value.vi);
           err = 0;
           break;
         case kFITSRW_Type_Float:
           val = (long long)(kw->key_value.vf);
           err = 0;
           break;
         default:
           fprintf(stderr, "Unsupported data type '%d'.", kw->key_type);
      }
   }

   if (stat)
   {
      *stat = err;
   }

   return val;
}

static double cfitsio_keyval2double(const CFITSIO_KEYWORD *kw, int *stat)
{
   double val = 0;
   int err = 1;

   if (kw)
   {
      switch(kw->key_type)
      {
         case kFITSRW_Type_String:
           if (kw->key_value.vs)
           {
              err = (sscanf(kw->key_value.vs, "%lf", &val) != 1);
           }
           else
           {
              err = 0;
           }
           break;
         case kFITSRW_Type_Logical:
           val = (double)(kw->key_value.vl);
           err = 0;
           break;
         case kFITSRW_Type_Integer:
           val = (double)(kw->key_value.vi);
           err = 0;
           break;
         case kFITSRW_Type_Float:
           val = (double)(kw->key_value.vf);
           err = 0;
           break;
         default:
           fprintf(stderr, "Unsupported data type '%d'.", kw->key_type);
      }
   }

   if (stat)
   {
      *stat = err;
   }

   return val;
}

static int cfitsio_writekeys(fitsfile *fptr, CFITSIO_KEYWORD *keylist)
{
   char card[FLEN_CARD];
   CFITSIO_KEYWORD* kptr = NULL;
   int err = CFITSIO_SUCCESS;
   int cfiostat = CFITSIO_SUCCESS;

   if (keylist)
   {
      kptr = keylist;

      while (kptr)
      {
         if (!strcmp(kptr->key_name,"HISTORY"))
         {
            if (kptr->key_value.vs)
            {
               fits_write_history(fptr, kptr->key_value.vs, &cfiostat);
            }
         }
         else if (!strcmp(kptr->key_name,"COMMENT"))
         {
            if (kptr->key_value.vs)
            {
               fits_write_comment(fptr, kptr->key_value.vs, &cfiostat);
            }
         }
         else
         {
            if (cfitsio_key_to_card(kptr, card) != CFITSIO_SUCCESS)
            {
               err = CFITSIO_ERROR_ARGS;
               break;
            }

            if (fits_get_keyclass(card) > TYP_CMPRS_KEY)
            {
               if(fits_write_record(fptr, card, &cfiostat))
               {
                  err = CFITSIO_ERROR_LIBRARY;
                  break;
               }
            }
         }

         if (cfiostat != CFITSIO_SUCCESS)
         {
            err = CFITSIO_ERROR_LIBRARY;
            break;
         }

         kptr = kptr->next;
      }
   }

   return err;
}

/****************************************************************************/
/*********************   Using CFITSIO_KEYWORD  *****************************/
/****************************************************************************/

int cfitsio_free_keys(CFITSIO_KEYWORD** keylist)
{
   if (keylist)
   {
      CFITSIO_KEYWORD *kptr = *keylist;
      CFITSIO_KEYWORD *del = NULL;
   
      while (kptr)
      {
	 del = kptr;
	 kptr = kptr->next;

         if (del->key_type == kFITSRW_Type_String && del->key_value.vs)
         {
            free(del->key_value.vs);
            del->key_value.vs = NULL;
         }

	 free(del);
      }

      *keylist = NULL;
   }
   return CFITSIO_SUCCESS;
}

int cfitsio_create_key(const char *name, const char type, const char *comment, const void *value, const char *format, CFITSIO_KEYWORD **keyOut)
{
    CFITSIO_KEYWORD *rv = NULL;
    int err = CFITSIO_SUCCESS;
    
    if (name && value && keyOut)
    {    
        rv = (CFITSIO_KEYWORD *)calloc(1, sizeof(CFITSIO_KEYWORD));
    
        if (!rv)
        {
            fprintf(stderr, "cfitsio_create_key(): out of memory\n");
            err = CFITSIO_ERROR_OUT_OF_MEMORY;
        }
        else
        {
            snprintf(rv->key_name, FLEN_KEYWORD, "%s", name);
            rv->key_type = type;

            switch (type)
            {
                case( 'X'):
                case (kFITSRW_Type_String):
                    /* 68 is the max chars in FITS string keyword, but the HISTORY and COMMENT keywords
                     * can contain values with more than this number of characters, in which case
                     * the fits API key-writing function will split the string across multiple 
                     * instances of these special keywords. */
                    rv->key_value.vs = strdup((char *)value);             
                    break;
                case (kFITSRW_Type_Logical):
                    rv->key_value.vl = *((int *)value);
                    break;
                case (kFITSRW_Type_Integer):
                    rv->key_value.vi = *((long long *)value);
                    break;
                case (kFITSRW_Type_Float):
                    rv->key_value.vf = *((double *)value);
                    break;
                default:
                    fprintf(stderr, "Invalid FITSRW keyword type '%c'.\n", (char)type);
                    err = CFITSIO_ERROR_ARGS;
                    break;
            }
        
            if (comment)
            {
                snprintf(rv->key_comment, FLEN_COMMENT, "%s", comment);
            }
        
            if (format)
            {
                snprintf(rv->key_format, CFITSIO_MAX_FORMAT, "%s", format);
            }
        }
    }
    else
    {
        err = CFITSIO_ERROR_ARGS; 
    }
    
    if (keyOut)
    {
        *keyOut = rv; /* swipe! */
        rv = NULL;
    }
    
    return err;
}

/****************************************************************************/
// TH: Append to the end of list to keep COMMENT in the right sequence.

int cfitsio_append_key(CFITSIO_KEYWORD** keylist, 
		       char *name, 
		       char type, 
		       char *comment,
		       void * value,
                       const char *format)
{
    CFITSIO_KEYWORD *node, *last;
    int error_code = CFITSIO_SUCCESS;
    
    
    if (name && value)
    {
        node = (CFITSIO_KEYWORD *) malloc(sizeof(CFITSIO_KEYWORD));
        
        if (!node) return CFITSIO_ERROR_OUT_OF_MEMORY;
        else
        {
            memset(node, 0, sizeof(CFITSIO_KEYWORD));
            node->next = NULL;
            
            // append node to the end of list
            if (*keylist)
            {
                last = *keylist;
                while(last->next) last = last->next; 
                last->next = node;
            }
            else // first node
            {
                *keylist = node;
            }
            
            snprintf(node->key_name, FLEN_KEYWORD, "%s", name);
            node->key_type = type;
            
            // save value into union
            switch (type)
            {
                case( 'X'):
                case (kFITSRW_Type_String):
                    /* 68 is the max chars in FITS string keyword, but the HISTORY and COMMENT keywords
                     * can contain values with more than this number of characters, in which case
                     * the fits API key-writing function will split the string across multiple 
                     * instances of these special keywords. */
                    node->key_value.vs = strdup((char *)value);             
                    break;
                case (kFITSRW_Type_Logical):
                    node->key_value.vl = *((int *)value);
                    break;
                case (kFITSRW_Type_Integer):
                    node->key_value.vi = *((long long *)value);
                    break;
                case (kFITSRW_Type_Float):
                    node->key_value.vf = *((double *)value);
                    break;
                default:
                    fprintf(stderr, "Invalid FITSRW keyword type '%c'.\n", (char)type);
                    error_code = CFITSIO_ERROR_ARGS;
                    break;
            }
            
            if (comment)
            {
                snprintf(node->key_comment, FLEN_COMMENT, "%s", comment);
            }
            
            if (format)
            {
                snprintf(node->key_format, CFITSIO_MAX_FORMAT, "%s", format);
            }
        }
    }
    else
    {
        error_code = CFITSIO_ERROR_ARGS; 
    }
    
    return error_code;
    
}


/****************************************************************************/
// Row ranges from 1 to NAXIS1
// Col ranges from 1 to NAXIS2
// Different image types, more than 2 dimensions, later...

int cfitsio_dump_image(void* image, CFITSIO_IMAGE_INFO* info, long from_row, 
		       long to_row, long from_col, long to_col)
{
   short* iptr; 
   long i, j;

   DEBUGMSG((stderr,"In cfitsio_dump_image() => image = %p\n", image));

   if((from_row < 1)||(to_row > info->naxes[1])||(from_col < 1)||
      (to_col > info->naxes[0]))
   {
      DEBUGMSG((stderr,"cfitsio_dump_image() out of range.\n"));
      return CFITSIO_FAIL;
   }

   for(i=from_row;i<=to_row;i++) 
   {
      iptr = ((short*) (image)) + ((i-1)* info->naxes[0])+ (from_col-1);
      for(j=from_col;j<=to_col;j++,iptr++) 
      {
	 printf("%d ",*iptr);
      }
      printf("\n");
   }

   return CFITSIO_SUCCESS;
}

/****************************************************************************/

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info)
{
   if(*image_info) free(*image_info);
   *image_info = NULL;

   return CFITSIO_SUCCESS;
}

/****************************************************************************/
// image_info returns a subset of keywords used by DRMS, where
// (1) SIMPLE, EXTEND, BLANK, BSCALE, BZERO are extracted from keylist
// (2) NAXIS, NAXES[],BITPIX  are extracted from fits_get_image_param()
// In a compressed image, NAXIS, NAXES[] and BITPIX in the header are actually
// the dimensions of the binary table (not the image)
// image_info always has the correct (uncompressed) dimension of the image buffer.
// We keep geting keylist and image dimensions together here in one function (atomic process)

/* If can return both keylistout and image_info, and there are conflicting values, then
 * must remove conflicting values from keylistout. */
static int cfitsio_read_keylist_and_image_info(fitsfile* fptr, CFITSIO_KEYWORD** keylistout, CFITSIO_IMAGE_INFO** image_info)
{

   char card[FLEN_CARD];
   int status = 0;			
   int nkeys;
   int error_code = CFITSIO_FAIL;


   CFITSIO_KEYWORD* node, *last, *kptr;

   char key_name[FLEN_KEYWORD];
   char key_value[FLEN_VALUE];

   int len, i;

   CFITSIO_KEYWORD *keylist = NULL;

   int currHDU = 0;
   int val = 0;

   //-----------------------------  Keylist ------------------------------------------
   
   if(keylistout && *keylistout) 
   {
      cfitsio_free_keys(keylistout);
      *keylistout = NULL;
   }

   if(fits_get_hdrspace(fptr, &nkeys, NULL, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   for(i=1; i<=nkeys; i++) 
   {
      if(fits_read_record(fptr, i, card, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;
	 goto error_exit;
      }

      if (fits_get_keyclass(card) == TYP_CMPRS_KEY)
      {
	 continue;
      }

      if(fits_get_keyname(card, key_name, &len, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;
	 goto error_exit;
      }

      node = (CFITSIO_KEYWORD *) malloc(sizeof(CFITSIO_KEYWORD));

      if(node == NULL) 
      {
	 error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
	 goto error_exit;
      }

      memset(node, 0, sizeof(CFITSIO_KEYWORD));

      if(keylist==NULL) //first item
      {
	 keylist = node;
	 node->next = NULL;
	 last = node;
      }
      else 
      {
	 node->next = NULL;
	 last->next = node;
	 last = node;
      }

      // special key 
      if((!strcmp(key_name,"COMMENT")) || (!strcmp(key_name,"HISTORY")))
      {
	 strcpy(node->key_name, key_name);
	 node->key_type=kFITSRW_Type_String;

         if(fits_parse_value(card, key_value, node->key_comment, &status)) 
	 {
	    error_code = CFITSIO_ERROR_LIBRARY;
	    goto error_exit;
	 }

         // The actual comment (excluding the COMMENT key name) was placed 
         // into node->key_comment - copy that into value.vs
	 node->key_value.vs = strdup(node->key_comment);
      }					
      else //regular key=value
      {
      
	 strcpy(node->key_name, key_name);
	 if(fits_parse_value(card, key_value, node->key_comment, &status)) 
	 {
	    error_code = CFITSIO_ERROR_LIBRARY;
	    goto error_exit;
	 }

	 if(strlen(key_value)) fits_get_keytype(key_value, &node->key_type, &status);
	 else	node->key_type = ' '; 

	 switch(node->key_type)
	 {
	    case ('X'): //complex number is stored as string, for now.
	    case (kFITSRW_Type_String): //Trip off ' ' around cstring? 
               node->key_value.vs = strdup(key_value);
	       break;
	    case (kFITSRW_Type_Logical): if (key_value[0]=='0') node->key_value.vl = 0;
	    else node->key_value.vl = 1;
	       break;

	    case (kFITSRW_Type_Integer): sscanf(key_value,"%lld", &node->key_value.vi);
	       break;

	    case (kFITSRW_Type_Float): sscanf(key_value,"%lf", &node->key_value.vf);
	       break;

	    case (' '): //type not found, set it to NULL string
	       node->key_type = kFITSRW_Type_String;
	       node->key_value.vs = strdup("");
	    default :
	       DEBUGMSG((stderr,"Key of unknown type detected [%s][%c]?\n",
			 key_value,node->key_type));
	       break;
	 }
      }
   }

   
   if (status == END_OF_FILE)  status = 0; // Reset after normal error 
   if(status)
   {
      error_code = CFITSIO_FAIL;
      goto error_exit;
   }


   //---------------------------  Image Info  -------------------------

   // Extract SIMPLE, EXTEND, BLANK, BZERO, BSCALE from keylist
     
   if(image_info && *image_info) cfitsio_free_image_info(image_info);
   
   *image_info = (CFITSIO_IMAGE_INFO*) malloc(sizeof(CFITSIO_IMAGE_INFO));
   if(*image_info == NULL)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }  
   
   memset((void*) *image_info,0, sizeof(CFITSIO_IMAGE_INFO));

   //Set to defaults
   (*image_info)->blank =0;
   (*image_info)->bscale =1.0;
   (*image_info)->bzero =0.0;

   /* You can't just assume the types of the keywords are the exact types 
    * required by image_info; you need to convert them. */

   kptr = keylist;
   while (kptr && !status)
   {
      if(!strcmp(kptr->key_name,"SIMPLE")) 
      {
	 (*image_info)->simple = cfitsio_keyval2int(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_SIMPLE;
         }
         else
         {
            fprintf(stderr, "Invalid value for keyword 'SIMPLE'.\n");
         }
      }
      else if(!strcmp(kptr->key_name,"EXTEND")) 
      {
	 (*image_info)->extend = cfitsio_keyval2int(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_EXTEND;
         }
         else
         {
            fprintf(stderr, "Invalid value for keyword 'EXTEND'.\n");
         }
      }
      else if(!strcmp(kptr->key_name,"BLANK"))
      {
	 (*image_info)->blank = cfitsio_keyval2longlong(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_BLANK;
         }
         else
         {
            fprintf(stderr, "Invalid value for keyword 'BLANK'.\n");
         }
      }
      else if(!strcmp(kptr->key_name,"BSCALE"))
      {
	 (*image_info)->bscale = cfitsio_keyval2double(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_BSCALE;
         }
         else
         {
            fprintf(stderr, "Invalid value for keyword 'BSCALE'.\n");
         }
      }
      else if(!strcmp(kptr->key_name,"BZERO")) 
      {
	 (*image_info)->bzero = cfitsio_keyval2double(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_BZERO;
         }
         else
         {
            fprintf(stderr, "Invalid value for keyword 'BZERO'.\n");
         }
      }
      
      kptr = kptr->next;
   }

   if (status)
   {
      goto error_exit;
   }

   /* Never use the fits keywords to determine NAXIS, NAXISi, BITPIX - if the image is compressed
    * these will be relevant to the compressed image.  But we need the values for the uncompressed 
    * image.
    */
   
   // Check if this HDU has image
   fits_get_img_dim(fptr, &((*image_info)->naxis), &status);
   if((*image_info)->naxis == 0)
   {
     //"No image in this HDU.";
      error_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   // Always extract NAXIS, NAXES[], BITPIX from actual image
   fits_get_img_param(fptr, CFITSIO_MAX_DIM, &((*image_info)->bitpix), &((*image_info)->naxis), 
		      &((*image_info)->naxes[0]), &status); 

   /* If this is a compressed image the default HDU is the one containing the compressed image, which 
    * doesn't have SIMPLE or EXTEND keywords. So, fetch them from the primary HDU. */
   fits_get_hdu_num(fptr, &currHDU);
   if (currHDU != 1)
   {
      /* current HDU was not primary HDU */
      fits_movabs_hdu(fptr, 1, NULL, &status);

      /* Now get SIMPLE and EXTEND keywords */
      fits_read_key(fptr, TLOGICAL, "SIMPLE", &val, NULL, &status);
      if (!status)
      {
	 (*image_info)->simple = val;
	 (*image_info)->bitfield |= kInfoPresent_SIMPLE;
      }

      fits_read_key(fptr, TLOGICAL, "EXTEND", &val, NULL, &status);
      if (!status)
      {
	 (*image_info)->extend = val;
	 (*image_info)->bitfield |= kInfoPresent_EXTEND;
      }

      fits_movabs_hdu(fptr, currHDU, NULL, &status);
   }

   for(i=(*image_info)->naxis;i<(int) CFITSIO_MAX_DIM ;i++) (*image_info)->naxes[i]=0;

   if (keylistout)
   {
      *keylistout = keylist;
   }
   else
   {
      cfitsio_free_keys(&keylist);
   }
   
   return CFITSIO_SUCCESS;
   

error_exit:

   if(keylist) cfitsio_free_keys(&keylist);

   if(image_info && *image_info) 
   {
      free(*image_info);
      *image_info = NULL;
   }
   
   return error_code;

}

int fitsrw_read_keylist_and_image_info(FITSRW_fhandle fhandle, 
                                       CFITSIO_KEYWORD** keylistout, 
                                       CFITSIO_IMAGE_INFO** image_info)
{
   return cfitsio_read_keylist_and_image_info((fitsfile *)fhandle, keylistout, image_info);
}

/****************************************************************************/

int fitsrw_readintfile(int verbose,
                       char* fits_filename,
                       CFITSIO_IMAGE_INFO** image_info,
                       void** image, 
                       CFITSIO_KEYWORD** keylist)
{
   fitsfile *fptr=NULL;        

   int status = 0;			
   int error_code = CFITSIO_FAIL;

   int  data_type, bytepix, i; 
   long	npixels;
   
   void* pixels = NULL;

   char cfitsiostat[FLEN_STATUS];
   
   int fileCreated = 0;

   // Move directly to first image
   fptr = fitsrw_getfptr(verbose, fits_filename, 0, &status, &fileCreated);
   
   XASSERT(!fileCreated);

   if (!fptr)
   {
      error_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }

   // Get keylist and image info (a separate function to reduce the function size)
   error_code = cfitsio_read_keylist_and_image_info(fptr, keylist, image_info);
   if(error_code != CFITSIO_SUCCESS) goto error_exit;


   switch((*image_info)->bitpix)
   {
      case(BYTE_IMG):    data_type = TBYTE; break; /* When reading, data are an unsigned char array */
      case(SHORT_IMG):   data_type = TSHORT; break;
      case(LONG_IMG):    data_type = TINT; break; 
      case(LONGLONG_IMG):data_type = TLONGLONG; break;
      case(FLOAT_IMG):   data_type = TFLOAT; break;
      case(DOUBLE_IMG):  data_type = TDOUBLE; break;
   }

   bytepix = abs((*image_info)->bitpix)/8;
	
   npixels = 1;
   for(i=0;i<(*image_info)->naxis;i++) npixels *= (*image_info)->naxes[i];
	
   pixels = (double*) calloc(npixels, bytepix); //get alignment to double
   if(pixels == NULL) 
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY; 
      goto error_exit;
   }

   // Always read/write RAW data   
   /* Don't let cfitsio apply the scale and blank keywords - scaling done in DRMS.
    * This is why, for the BYTE_IMG bitpix type, we must read as unsigned char, not signed char.
    * If you read signed char without applying the scale keywords, then the data values
    * are unsigned values, half of which (128-255) fall outside of the range of
    * signed chars.  This would cause overflow. */
   fits_set_bscale(fptr, 1.0, 0.0, &status);
   fits_set_imgnull(fptr, 0, &status);

   /* WARNING: IMPLEMENTED A WORK-AROUND - THIS SHOULD CONTINUE TO WORK EVEN IF
    * CFITSIO GETS UPDATED, BUT IT HAS EXTRA CODE THAT WON'T BE NECESSARY WHEN
    * CFITSIO GETS UPDATED.
    *
    * fits_read_img() has 3 known bugs that involve 2 of the parameters to this function: 
    * 'nulval' and 'anynul'.  
    * 1. If the image to be read is a compressed image and the nulval
    * argument provided is NULL, then cfitsio will convert all blank values to zeros.
    * (See cfitsio source code imcompress.c:imcomp_decompress_tile():3737).  The local 
    * nullcheck variable is mostly likely erroneously set to 1, which causes conversion 
    * of blanks to happen.  The value that blanks get convereted to is specifed 
    * by the local variable nulval, which is set to &dummy, and dummy is set to 0.  
    * 2. If the image to be read is an uncompressed image and the nulval
    * argument provided is not NULL, then cfitsio will convert all zero values to blanks.
    * 3. If the nulval argument is provided, but the anynul parameter is not AND
    * cfitsio does convert blanks to the value specified by the nulval argument, 
    * cfitsio seg faults.  It is trying to set anynul (which is a boolean that say 
    * "I changed at least one of your values"), but since it is NULL, the code crashes.
    */

   /* FITS DETERMINES WHICH VALUES ARE BLANK BY LOOKING AT THE PRIMARY HDU'S BLANK KEYWORD VALUE.
    * So make sure when you write a compressed image that you set BLANK in the primary HDU, 
    * not the image HDU.
    */

   if(fits_read_img(fptr, data_type, 1, npixels, NULL, pixels, NULL, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY; 
      goto error_exit;
   }
 
   if (fptr) 
   {
       /* We're reading a file, so don't worry about errors. */
       fitsrw_closefptr(verbose, fptr);
   } 

   //---------------------------------  BYTE_IMG --------------------------------------------------
   // For BYTE_IMG, DRMS expects signed byte buffer, but FITS always stores BYTE_IMG as unsigned byte
   // Conversion for BYTE_IMG here before returning the buffer to DRMS
   // BZERO and BLANK are set accordingly
   
   if ((*image_info)->bitpix == BYTE_IMG)
   {
      /* Subtract 128 from data so that it fits into the signed char data type expected
       * by DRMS */
      long ipix;
      unsigned char *pDataIn = (unsigned char *)pixels;
      signed char *pDataOut = (signed char *)pixels;

      for (ipix = 0; ipix < npixels; ipix++)
      {
	 pDataOut[ipix] = (signed char)(pDataIn[ipix] - 128);
      }

      /* D = d * bscale + bzero
       *
       * D - real, physical value
       * d - value returned by cfitsio, range is [0,255]
       * bscale - value associated with d
       * bzero - value associated with d
       *
       * ds = d - 128 
       *
       * ds - scaled data so range is [-128,127]
       * 
       * So, 
       * D = d * bscale + bzero
       * D = (ds + 128) * bscale + bzero
       * D = ds * bscale + 128 * bscale + bzero
       * D = ds * BSCALE + BZERO
       *
       * BSCALE = bscale
       * BZERO = 128 * bscale + bzero
       */
      (*image_info)->bzero = 128.0 * (*image_info)->bscale + (*image_info)->bzero;

      /* The BLANK keyword value must also be shifted by 128. */
      (*image_info)->blank = (*image_info)->blank - 128;
   }   
   //--------------------------------------------------------------------------------------------------
   
   
   if(keylist==NULL) // if caller function not asking for keylist
   {
      cfitsio_free_keys(keylist); 
   }
   
   *image = pixels;
   
   if (status == 0) return (CFITSIO_SUCCESS);

error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);

   cfitsio_free_these(image_info, &pixels, keylist);
   
   if (fptr) 
   {
       /* There is some other error, so don't worry about any error having to do with not closing the file pointer. */
       fitsrw_closefptr(verbose, fptr);
   }

   return error_code;
}

/******************************************************************************/


void cfitsio_free_these(CFITSIO_IMAGE_INFO** image_info,
			void** image,
			CFITSIO_KEYWORD** keylist)
{

   if(image_info)
   {
      free(*image_info);
      *image_info = NULL;
   }

   if(image)
   {
      free(*image);
      *image = NULL;
   }


   if (keylist)
   {
      CFITSIO_KEYWORD *kptr = *keylist;
      CFITSIO_KEYWORD *del = NULL;
   
      while (kptr)
      {
	 del = kptr;
	 kptr = kptr->next;

         if (del->key_type == kFITSRW_Type_String && del->key_value.vs)
         {
            free(del->key_value.vs);
            del->key_value.vs = NULL;
         }

	 free(del);
      }

      *keylist = NULL;
   }   

}

/****************************************************************************/

/* Assumes file will live in SUMS - eg, if image is char, then it is signed data, etc. */
int fitsrw_writeintfile(int verbose,
                        const char* fits_filename,
                        CFITSIO_IMAGE_INFO* image_info,  
                        void* image,
                        const char* compspec,
                        CFITSIO_KEYWORD* keylist) 
{

   fitsfile *fptr=NULL; 
   int status=0, error_code = CFITSIO_FAIL;
   char filename[PATH_MAX];
   int  data_type;
   int img_type;
   char cfitsiostat[FLEN_STATUS];
   long long oblank = 0;

   long	long npixels;

   int i = 0;

   void *misspix = NULL;
   long long pixleft;
   long long pixtowrt;
   long long pixptr;
   
   int fileCreated = 0;
   long long imgSize = 0;

   // image_info contain the image dimensions, can not be missing
   if(image_info == NULL) return CFITSIO_ERROR_ARGS;
   
   
   for (i = 0, npixels = 1; i < image_info->naxis; i++)
   {
      npixels *= image_info->naxes[i];     
   }

   switch(image_info->bitpix)
   {
      case(BYTE_IMG): data_type = TSBYTE; img_type = SBYTE_IMG; break;
      case(SHORT_IMG):        data_type = TSHORT; img_type = SHORT_IMG; break;
      case(LONG_IMG): data_type = TINT; img_type = LONG_IMG; break; 
      case(LONGLONG_IMG):     data_type = TLONGLONG; img_type = LONGLONG_IMG; break;
      case(FLOAT_IMG):        data_type = TFLOAT; img_type = FLOAT_IMG; break;
      case(DOUBLE_IMG):       data_type = TDOUBLE; img_type = DOUBLE_IMG; break;
   }
   
   imgSize = (abs(img_type) / 8) * npixels;

   // Remove the file, if exist
   remove(fits_filename);
   // printf("fits_filename = %s compspec = %s\n",fits_filename, compspec);

   if (compspec && *compspec)
   {
      snprintf(filename, sizeof(filename), "%s[%s]", fits_filename, compspec);
   }
   else
   {
      snprintf(filename, sizeof(filename), "%s", fits_filename);
   }
   
   status = 0; // first thing!

   fptr = fitsrw_getfptr(verbose, filename, 1, &status, &fileCreated);

   if (!fptr)
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }
   
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 35)
    if (imgSize > HUGE_HDU_THRESHOLD && fileCreated)
    {
        // We will be writing, which means we will call fits_create_file(). Support 64-bit HDUs.
        if (fits_set_huge_hdu(fptr, 1, &status))
        {
            error_code = CFITSIO_ERROR_FILE_IO;
            goto error_exit;
        }
    }
#endif

   if(fits_create_img(fptr, img_type, image_info->naxis, image_info->naxes, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   if (fits_is_compressed_image(fptr, &status))
   {
      /* Disallow unsupported types of tile-compressed files */
      if (data_type == TLONGLONG)
      {
          fprintf(stderr, "CFITSIO doesn't support compression of 64-bit data.\n");
          error_code = CFITSIO_ERROR_CANT_COMPRESS;
          goto error_exit;
      }
      else if (data_type == TFLOAT || data_type == TDOUBLE)
      {
          fprintf(stderr, "CFITSIO compression of floating-point data is lossy, bailing.\n");
          error_code = CFITSIO_ERROR_CANT_COMPRESS;
          goto error_exit;
      }
   }
   else if (status)
   {
       error_code = CFITSIO_ERROR_LIBRARY;
       goto error_exit;
   }
   
   // keylist is optional. It is available only for export 
   error_code = cfitsio_writekeys(fptr, keylist);
   if (error_code != CFITSIO_SUCCESS)
   {
      goto error_exit;
   }
   
   /* override keylist special keywords (these may have come from 
    * the keylist to begin with - they should have been removed from
    * the keylist if this is the case, but I don't think they were removed).
    */
   if (image_info->bitfield & kInfoPresent_BLANK)
   {
      oblank = (long long)image_info->blank;

      if (image_info->bitpix == BYTE_IMG)
      {
	 /* Work around the fact that the FITS standard does not support 
	  * signed byte data.  You have to make missing a valid unsigned
	  * char - I'm choosing 0.  In DRMS, you have to convert 0 to 
	  * signed char -128 */
	 oblank = 0;
      }

      fits_update_key(fptr, TLONGLONG, "BLANK", &oblank, "", &status);
   }
   if (image_info->bitfield & kInfoPresent_BZERO)
   {
      double obzero = image_info->bzero;
      if (image_info->bitpix == BYTE_IMG)
      {
	 obzero = (double) (obzero -(128.0 * image_info->bscale));
      }
     
      fits_update_key(fptr, TDOUBLE, "BZERO", &obzero, "", &status);
   }
   if (image_info->bitfield & kInfoPresent_BSCALE)
   {
      double obscale = image_info->bscale;
      fits_update_key(fptr, TDOUBLE, "BSCALE", &obscale, "", &status);
   }

   if (image_info->bitpix == BYTE_IMG)
   {
      /* If you don't do this, then setting the 'BZERO' keyword to something 
       * other than -128 causes a problem.  cfitsio adds the -BZERO value to 
       * the signed char data, and if you choose anything other than -128, 
       * you can get an overflow (outside of the range of unsigned char)
       * For example, if you choose -120 as BZERO, all signed char values above
       * 
       * then for a */
      fits_set_bscale(fptr, 1.0, -128, &status);
   }
   else
   {
      fits_set_bscale(fptr, 1.0, 0.0, &status); /* Don't scale data when writing file - 
						 * if you use values other than 1.0/0.0
						 * then cfitsio will use those values to 
						 * scale the data.  If you omit this 
						 * function call, then cfitsio will use the
						 * BZERO/BSCALE keywords that you set a few
						 * lines of code above to scale the data. */
   }

   fits_set_imgnull(fptr, 0, &status); /* Don't convert any value to missing -
                                        * just write all values as is.  If you
                                        * put something other than 0 here, 
                                        * cfitsio converts whatever you specify 
                                        * to whatever you've defined as BLANK with
                                        * the BLANK keyword. If you omit this function
                                        * call, then cfitsio will use the BLANK keyword
                                        * that you set a few lines of code above to 
                                        * modify the data. */
   
  
   if (image)
   {
      if(fits_write_img(fptr, data_type, 1, npixels, image, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
         goto error_exit;
      }
   }
   else
   {
        /* TAS-file branch */

      /* If you avoid writing an image, then fitsio fills in all unspecified values 
       * with zeros, which is not what we want. Instead, we need to allocate 
       * blocks of missing values and write them out one at a time (we don't
       * want to allocate ALL values - this could be a huge amount of memory). 
       * We need to write out npixels missing values. */

      /* If no image, let fitsio create an "empty" image - ensure that the 
       * correct blank value is used (use the one passed into this function). */
      if (image_info->bitpix == FLOAT_IMG)
      {
         /* need to manually use NaN */
         float *pix = 0;
         int ipix;

         misspix = malloc(kMISSPIXBLOCK * sizeof(float));
         pix = misspix;

         for (ipix = 0; ipix < kMISSPIXBLOCK; ipix++)
         {
            *pix++ = F_NAN;
         }
      }
      else if (image_info->bitpix == DOUBLE_IMG)
      {
         /* need to manually use NaN */
         double *pix = 0;
         int ipix;

         misspix = malloc(kMISSPIXBLOCK * sizeof(double));
         pix = misspix;

         for (ipix = 0; ipix < kMISSPIXBLOCK; ipix++)
         {
            *pix++ = D_NAN;
         }
      }
      else if (image_info->bitpix == BYTE_IMG)
      {
         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(char));
         memset(misspix, (char)oblank, kMISSPIXBLOCK);
      }
      else if (image_info->bitpix == SHORT_IMG)
      {
         short *p = NULL;

         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(short));
         p = (short *)misspix + (kMISSPIXBLOCK - 1);
         while (p >= misspix)
         {
            *p = (short)oblank;
            p--;
         }
      }
      else if (image_info->bitpix == LONG_IMG)
      {
         int *p = NULL;

         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(int));
         p = (int *)misspix + (kMISSPIXBLOCK - 1);
         while (p >= misspix)
         {
            *p = (int)oblank;
            p--;
         }
      }
      else if (image_info->bitpix == LONGLONG_IMG)
      {
         long long *p = NULL;

         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(long long));
         p = (long long *)misspix + (kMISSPIXBLOCK - 1);
         while (p >= misspix)
         {
            *p = (long long)oblank;
            p--;
         }
      }

      pixleft = npixels;
      pixtowrt = 0;
      pixptr = 0;

      while(pixleft > 0)
      {
         pixtowrt = pixleft > kMISSPIXBLOCK ? kMISSPIXBLOCK : pixleft;
         if(fits_write_img(fptr, 
                           data_type, 
                           1 + pixptr, /* 1-based first pixel location in image on file */
                           pixtowrt, 
                           misspix, 
                           &status))
         {
            error_code = CFITSIO_ERROR_LIBRARY;
            goto error_exit;
         }

         pixleft -= pixtowrt;
         pixptr += pixtowrt;
      }

      if (misspix)
      {
         free(misspix);
      }
   }

   if (fptr)
   {
      /* set the fpinfo struct with the values from image_info (but do not overwrite the fhash field) */
      CFITSIO_IMAGE_INFO fpinfo;
      CFITSIO_IMAGE_INFO fpinfonew;

      if (fitsrw_getfpinfo_ext(fptr, &fpinfo))
      {
         fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
         error_code = CFITSIO_ERROR_FILE_IO;
      }
      else
      {
         fpinfonew = *image_info;
         snprintf(fpinfonew.fhash, sizeof(fpinfonew.fhash), "%s", fpinfo.fhash);

         if (fitsrw_setfpinfo_ext(fptr, &fpinfonew))
         {
            fprintf(stderr, "Unable to update file pointer information.\n");
            error_code = CFITSIO_ERROR_FILE_IO;
         }
         else
         {
            if ((status = fitsrw_closefptr(verbose, fptr)) != 0)
            {
               error_code = CFITSIO_ERROR_FILE_IO;         
            }
         }
      }
   }

   if(status == 0) return CFITSIO_SUCCESS;


  error_exit:

   if (status)
   {
      fits_get_errstatus(status, cfitsiostat);
   }

   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);

   if (fptr) 
   {
       /* There was some other error, so dont' worry about any errors having to do with closing the file. */
       fitsrw_closefptr(verbose, fptr);
   }

   return error_code;
}

/****************************************************************************/
// Will find the library call for writting card to replace this, later
//
//Format: name(8), '= '(2), value (20), ' / ', comment (the rest and pending spaces)
//Format: name(8), '= '(2), 'string value need single quotes' (upto FLEN_VALUE=71), ' / ', comment
//Format: END is the last one by itself

/*
  Examples:

  01234567890123456789012345678901234567890123456789012345678901234567890123456789
  SIMPLE  =                    T / Fits standard
  BITPIX  =                  -32 / Bits per pixel
  NAXIS   =                    1 / Number of axes
  NAXIS1  =                 1440 / Axis length
  EXTEND  =                    F / File may contain extensions
  DATAMIN =             77.40008 / Minimum data value
  DATAMAX =             88.58415 / Maximum data value
  ORIGIN  = 'NOAO-IRAF FITS Image Kernel December 2001' / FITS file originator
  DATE    = '2003-09-24T01:25:54' / Date FITS file was generated
  IRAF-TLM= '17:25:54 (23/09/2003)' / Time of last modification
  END
*/

/****************************************************************************/


int cfitsio_key_to_card(CFITSIO_KEYWORD* kptr, char* card)
{
    char temp[FLEN_CARD];
    char buf[128];
    
    memset(card,0,sizeof(FLEN_CARD));
    if(!kptr) return CFITSIO_FAIL;
    
    //TH:  add check for upper case, length limits
    switch(kptr->key_type)
    {
        case(kFITSRW_Type_String):
        case('X'):
        {
            /* 68 is the maximum number of characters allowed in a FITS string keyword, 
             * provided the opening single quote string starts on byte 11. We need
             * to truncate strings that are longer than this. However, the HISTORY and COMMENT keywords, 
             * which are strings, should not be truncated. They are written to the FITS file
             * with special API function calls, and if the length of the values of these keywords
             * is greater than 68 characters, FITSIO will split these strings over multiple
             * instances of the keyword. */
            int isspecial = (strcasecmp(kptr->key_name, "history") == 0 || strcasecmp(kptr->key_name, "comment") == 0);
            char format[128] = {0};
            char formatspecial[128] = {0};
            
            if (strlen(kptr->key_comment) > 0)
            {
                snprintf(format, sizeof(format), "%s / %s", "%-8s= '%.68s'", kptr->key_comment);
                snprintf(formatspecial, sizeof(formatspecial), "%s / %s", "%-8s= '%s'", kptr->key_comment);
            }
            else
            {
                snprintf(format, sizeof(format), "%s", "%-8s= '%.68s'");
                snprintf(formatspecial, sizeof(formatspecial), "%s", "%-8s= '%s'");
            }           
            
            if (isspecial)
            {
                snprintf(temp, 
                         sizeof(temp), 
                         formatspecial,
                         kptr->key_name, 
                         kptr->key_value.vs);
            }
            else
            {
                snprintf(temp, 
                         sizeof(temp), 
                         format,
                         kptr->key_name, 
                         kptr->key_value.vs);
            }
            
        }
            break;
            
        case(kFITSRW_Type_Logical):
            if(strlen(kptr->key_comment) >0)
            {
                if(kptr->key_value.vl)
                    sprintf(temp,"%-8s=                    T / %s", kptr->key_name, kptr->key_comment);
                else
                    sprintf(temp,"%-8s=                    F / %s", kptr->key_name, kptr->key_comment);
            }
            else
            {
                if(kptr->key_value.vl)
                    sprintf(temp,"%-8s=                    T", kptr->key_name);
                else
                    sprintf(temp,"%-8s=                    F", kptr->key_name);
            }
            
            break;
            
        case(kFITSRW_Type_Integer):
            if(strlen(kptr->key_comment) >0)
            {
                sprintf(temp,"%-8s= %20lld / %s", kptr->key_name, kptr->key_value.vi, kptr->key_comment);
            }
            else
            {
                sprintf(temp,"%-8s= %20lld", kptr->key_name, kptr->key_value.vi);
            }
            break;
            
        case(kFITSRW_Type_Float):
        {
            int usedef = 1;
            
            if (*kptr->key_format != '\0')
            {
                double tester = 0;
                snprintf(buf, sizeof(buf), kptr->key_format, kptr->key_value.vf);
                if (sscanf(buf, "%lf", &tester) == 1)
                {
                    snprintf(temp, sizeof(temp), "%-8s= %s", kptr->key_name, buf);
                    usedef = 0;
                }
            }
            
            /* Use default scheme %20.15e */
            if (usedef)
            {
                snprintf(temp, sizeof(temp), "%-8s= %20.15e", kptr->key_name, kptr->key_value.vf);
            }
            
            if(strlen(kptr->key_comment) > 0)
            {
                char *pref = strdup(temp);
                
                if (pref)
                {
                    snprintf(temp, sizeof(temp), "%s / %s", pref, kptr->key_comment);
                    free(pref);
                }
                else
                {
                    return CFITSIO_ERROR_OUT_OF_MEMORY;
                }
            }
        }
            break;
    }
    
    sprintf(card,"%-80s",temp); // append space to the end to 80 chars
    
    //DEBUGMSG((stderr,"[01234567890123456789012345678901234567890123456789012345678901234567890123456789]\n"));
    //DEBUGMSG((stderr,"[%s]\n",card));
    
    return CFITSIO_SUCCESS;
}

/****************************************************************************/

int fitsrw_read(int verbose,
                const char *filename, 
                CFITSIO_IMAGE_INFO** image_info,
                void** image,
                CFITSIO_KEYWORD** keylist)
{
   fitsfile *fptr = NULL;
   long long npixels;
   int bytepix;
   int data_type;
   void *pixels = NULL;
   int status;
   int error_code = 0;
   char cfitsiostat[FLEN_STATUS];
   int idim;
   char *fnamedup = strdup(filename);
   int datachk;
   int hduchk;
   int fileCreated = 0;

   if (!image_info || *image_info)
   {
      fprintf(stderr, "Invalid image_info argument.\n");
      error_code = CFITSIO_ERROR_ARGS;
   }

   if (!error_code)
   {
      *image_info = (CFITSIO_IMAGE_INFO *)malloc(sizeof(CFITSIO_IMAGE_INFO));

      if(*image_info == NULL)
      {
         error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      }
   }

   if (!error_code)
   {
      memset((void*)(*image_info), 0, sizeof(CFITSIO_IMAGE_INFO));

      /* stupid cfitsio will fail if status is not 0 when the following call
       * is made */
      status = 0;

      fptr = fitsrw_getfptr(verbose, fnamedup, 0, &status, &fileCreated);
      XASSERT(!fileCreated);

      if (!fptr)
      {
         error_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      }

      if (!error_code)
      {
         error_code = cfitsio_read_keylist_and_image_info(fptr, keylist, image_info);
      }

      if (!error_code)
      {
         switch((*image_info)->bitpix)
         {
            case(BYTE_IMG):    data_type = TBYTE; break; /* When reading, data are 
                                                            an unsigned char array */
            case(SHORT_IMG):   data_type = TSHORT; break;
            case(LONG_IMG):    data_type = TINT; break; 
            case(LONGLONG_IMG):data_type = TLONGLONG; break;
            case(FLOAT_IMG):   data_type = TFLOAT; break;
            case(DOUBLE_IMG):  data_type = TDOUBLE; break;
         }

         bytepix = abs((*image_info)->bitpix) / 8;

         npixels = 1;
         for(idim = 0; idim < (*image_info)->naxis; idim++) 
         {
            npixels *= (*image_info)->naxes[idim];
         }

         pixels = calloc(npixels, bytepix);
         if(!pixels)
         {
            error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
         }
      }
   }

   if (!error_code)
   {
      /* Read raw data */
      fits_set_bscale(fptr, 1.0, 0.0, &status);

      if (!status)
      {
         fits_set_imgnull(fptr, 0, &status);
      }

      if (status)
      {
         error_code = CFITSIO_ERROR_LIBRARY;
      }
   }

   if (!error_code)
   {
      if(fits_read_img(fptr, data_type, 1, npixels, NULL, pixels, NULL, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY; 
      }

      /* Check checksum, if it exists */
      if (fits_verify_chksum(fptr, &datachk, &hduchk, &status))
      {
         error_code = CFITSIO_ERROR_LIBRARY;
      }
      else
      {
         if (datachk == -1 || hduchk == -1)
         {
            /* Both checksums were present, and at least one of them failed. */
            fprintf(stderr, "Failed to verify data and/or HDU checksum (file corrupted).\n");
            error_code = CFITSIO_ERROR_FILE_IO;
         }
      }
   }

   if(fptr) 
   {
       status = fitsrw_closefptr(verbose, fptr);
       if (status)
       {
           error_code = CFITSIO_ERROR_FILE_IO;
       }
   }
   
   if (status)
   {
      fits_get_errstatus(status, cfitsiostat);
      fprintf(stderr, "In fitsrw_read(), cfitsio error '%s'.\n", cfitsiostat);
      if(pixels) 
      {
         free(pixels);
      }
   }
   else
   {
      *image = pixels;
   }

   if (fnamedup)
   {
      free(fnamedup);
   }

   return error_code;
}

int fitsrw_write(int verbose,
                 const char* filein,
                 CFITSIO_IMAGE_INFO* info,
                 void* image,
                 const char* cparms,
                 CFITSIO_KEYWORD* keylist)
{ //ISS fly-tar
   return fitsrw_write2(verbose, filein, info, image, cparms, keylist, (export_callback_func_t) NULL);
}

int fitsrw_write2(int verbose,
                  const char* filein, /* "-" for stdout */
                  CFITSIO_IMAGE_INFO* info,  
                  void* image,
                  const char* cparms, /* NULL for stdout */
                  CFITSIO_KEYWORD* keylist,
                  export_callback_func_t callback) //ISS fly-tar - fitsfile * for stdout
{
   int err = CFITSIO_SUCCESS;
   int idim;
   long long npixels;
   int datatype;
   int imgtype;
   char filename[PATH_MAX];
   fitsfile *fptr = NULL;
   int cfiostat = 0; /* MUST start with no-error status, else CFITSIO will fail. */
   int fileCreated = 0;
   long long imgSize =0;
   
   if (filein && info && image)
   {
      for (idim = 0, npixels = 1; idim < info->naxis; idim++)
      {
         npixels *= info->naxes[idim];     
      }

      switch (info->bitpix)
      {
         case(BYTE_IMG): 
           {
              datatype = TSBYTE; 
              imgtype = SBYTE_IMG; 
           }
           break;
         case(SHORT_IMG): 
           {
              datatype = TSHORT; 
              imgtype = SHORT_IMG; 
           }
           break;
         case(LONG_IMG): 
           {
              datatype = TINT; 
              imgtype = LONG_IMG; 
           }
           break; 
         case(LONGLONG_IMG): 
           {
              datatype = TLONGLONG; 
              imgtype = LONGLONG_IMG; 
           }
           break;
         case(FLOAT_IMG): 
           {
              datatype = TFLOAT; 
              imgtype = FLOAT_IMG; 
           }
           break;
         case(DOUBLE_IMG):
           {
              datatype = TDOUBLE; 
              imgtype = DOUBLE_IMG; 
           }
           break;
         default:
           fprintf(stderr, "fitsrw_write(): Unsupported image data type.\n");
           err = CFITSIO_ERROR_ARGS;
      }

      if (!err)
      {
         imgSize = (abs(imgtype) / 8) * npixels;
      
         /* In the future, perhaps we override this method of specifying compression 
          * with the CFITSIO API call that specifies it. */
        if (strcmp(filein, "-") != 0)
        {
            /* not stdout */
            if (cparms && *cparms)
            {
                snprintf(filename, sizeof(filename), "%s[%s]", filein, cparms);
            }
            else
            {
                snprintf(filename, sizeof(filename), "%s", filein);
            }

            remove(filein);
         }
         
         //ISS fly-tar START
         if (callback != NULL) 
         {
            if (strcmp(filein, "-") != 0)
            {
                /* not stdout */
                int retVal =0;
            
                (*callback)("create", &fptr, filein, cparms, &cfiostat, &retVal);
                if (retVal)
                {
                   err = CFITSIO_ERROR_FILE_IO;
                }
            }
            else
            {
                /* we are writing the FITS file to stdout; the FITS file has already been created (in memory);
                 * callback has the fitsfile */
                fptr = (fitsfile *)callback;
            }
         } 
         else 
         {
            fptr = fitsrw_getfptr(verbose, filename, 1, &err, &fileCreated);
            
            if (!fptr)
            {
               err = CFITSIO_ERROR_FILE_IO;
            }
            else
            {
#if CFITSIO_MAJOR >= 4 || (CFITSIO_MAJOR == 3 && CFITSIO_MINOR >= 35)
    if (imgSize > HUGE_HDU_THRESHOLD && fileCreated)
    {
        // We will be writing, which means we will call fits_create_file(). Support 64-bit HDUs.
        if (fits_set_huge_hdu(fptr, 1, &cfiostat))
        {
            err = CFITSIO_ERROR_FILE_IO;
        }
    }
#endif
            }      
            
         }
         //ISS fly-tar END
      }

      if (!err)
      {
         if(fits_create_img(fptr, imgtype, info->naxis, info->naxes, &cfiostat))
         {
            err = CFITSIO_ERROR_LIBRARY;
         }
      }

      if (!err)
      {
         if (fits_is_compressed_image(fptr, &cfiostat))
         {
            if (datatype == TLONGLONG)
            {
               fprintf(stderr, "CFITSIO doesn't support compression of 64-bit data.\n");
               err = CFITSIO_ERROR_ARGS;
            }
            else if (datatype == TFLOAT || datatype == TDOUBLE)
            {
               fprintf(stderr, "WARNING: CFITSIO compression of floating-point data is lossy.\n");
            }
         }
         else if (cfiostat)
         {
            err = CFITSIO_ERROR_LIBRARY;
         }
      }

      if (!err)
      {
         /* Write out FITS keywords that were derived from DRMS keywords. */
         err = cfitsio_writekeys(fptr, keylist);
      }

      if (!err)
      {
         /* Write out special FITS keywords */
         if (info->bitfield & kInfoPresent_BLANK)
         {
            long long oblank = (long long)info->blank;
            fits_update_key(fptr, TLONGLONG, "BLANK", &oblank, "", &cfiostat);
         }
       
         if (!cfiostat && (info->bitfield & kInfoPresent_BZERO))
         {
            double obzero = info->bzero;
            fits_update_key(fptr, TDOUBLE, "BZERO", &obzero, "", &cfiostat);
         }

         if (!cfiostat && (info->bitfield & kInfoPresent_BSCALE))
         {
            double obscale = info->bscale;
            fits_update_key(fptr, TDOUBLE, "BSCALE", &obscale, "", &cfiostat);
         }

         /* Must ensure that CFITSIO doesn't modify data - BZERO, BSCALE, and BLANK
          * are written out above and without the following calls, CFITSIO may see
          * those values and attempt to modify the data so that those values are
          * relevant. But the data have already been converted so that the 
          * special values are already relevant. */
         if (!cfiostat)
         {
            fits_set_bscale(fptr, 1.0, 0.0, &cfiostat);
         }

         if (!cfiostat)
         {
            fits_set_imgnull(fptr, 0, &cfiostat);
         }

         if (cfiostat)
         {
            fprintf(stderr, "Trouble setting BZERO, BSCALE, or BLANK keywords.\n");
            err = CFITSIO_ERROR_LIBRARY;
         }
      }

      if (!err)
      {
         if(fits_write_img(fptr, datatype, 1, npixels, image, &cfiostat))
         {
            err = CFITSIO_ERROR_LIBRARY;
         }
      }
   }
   else
   {
      err = CFITSIO_ERROR_ARGS;
   }

   if (err && cfiostat)
   {
      char errmsg[FLEN_STATUS];
      fits_get_errstatus(cfiostat, errmsg);
      fprintf(stderr, "CFITSIO error '%s'.\n", errmsg);
   }

   if (fptr)
   {
      /* set the fpinfo struct with the values from image_info (but do not overwrite the fhash field) */

      //ISS fly-tar
      if (callback != NULL ) 
      {
            if (strcmp(filein, "-") != 0)
            {
                //ISS fly-tar
                (*callback)("stdout", fptr, image, &cfiostat);
                if (cfiostat) 
                {
                    fprintf(stderr, "Trouble executing callback [%s]\n", filename);
                    err = CFITSIO_ERROR_LIBRARY;
                }
            }
      } 
      else 
      {
         CFITSIO_IMAGE_INFO fpinfo;
         CFITSIO_IMAGE_INFO fpinfonew;
         
         if (fitsrw_getfpinfo_ext(fptr, &fpinfo))
         {
            fprintf(stderr, "Invalid fitsfile pointer '%p'.\n", fptr);
            err = CFITSIO_ERROR_FILE_IO;
         }
         else
         {
            fpinfonew = *info;
            snprintf(fpinfonew.fhash, sizeof(fpinfonew.fhash), "%s", fpinfo.fhash);
            
            if (fitsrw_setfpinfo_ext(fptr, &fpinfonew))
            {
               fprintf(stderr, "Unable to update file pointer information.\n");
               err = CFITSIO_ERROR_FILE_IO;
            }
            else
            {
               if (fitsrw_closefptr(verbose, fptr))
               {
                  err = CFITSIO_ERROR_FILE_IO;
               }
            }
         }
      }  //ISS fly-tar
   }
   
   return err;
}


