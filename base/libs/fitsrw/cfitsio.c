//****************************************************************************
// CFITSIO.C
//
// Functions for read/write FITS file format  using CFITSIO library.
//
//
//****************************************************************************

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "fitsio.h"
#include "cfitsio.h"
#include "jsoc.h"
#include "foundation.h"

//****************************************************************************

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
           err = (sscanf(kw->key_value.vs, "%d", &val) != 1);
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
           err = (sscanf(kw->key_value.vs, "%lld", &val) != 1);
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
           err = (sscanf(kw->key_value.vs, "%lf", &val) != 1);
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

//****************************************************************************
//*********************   Using CFITSIO_KEYWORD  *****************************
//****************************************************************************

int cfitsio_read_keys(char* fits_filename, CFITSIO_KEYWORD** keylist)
{
   fitsfile *fptr=NULL;        
   char card[FLEN_CARD];
   int status = 0;			
   int nkeys, i;
   int error_code = CFITSIO_FAIL;

   CFITSIO_KEYWORD* node, *last;

   int len;

   char key_name[FLEN_KEYWORD];
   char key_value[FLEN_VALUE];



   // Move directly to first image
   if (fits_open_image(&fptr, fits_filename, READONLY, &status)) 
   {
      error_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }

   fits_get_hdrspace(fptr, &nkeys, NULL, &status);


   if(*keylist) 
   {
      free(*keylist);
      *keylist = NULL;
   }

		
   for(i=1; i<=nkeys; i++) 
   {
      if(fits_read_record(fptr, i, card, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;
	 goto error_exit;
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

      if(*keylist==NULL) //first item
      {
	 *keylist= node;
	 node->next = NULL;
	 last = node;
      }
      else 
      {
	 node->next = NULL;
	 last->next = node;
	 last = node;
      }

      // special key  (no keyname = )
      if((!strcmp(key_name,"COMMENT")) || (!strcmp(key_name,"HISTORY")))
      {
	 strcpy(node->key_name, key_name);
	 node->key_type=kFITSRW_Type_String;
	 strcpy(node->key_value.vs,card); //save the whole card into value
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
	       strcpy(node->key_value.vs, key_value);
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
	       node->key_value.vs[0]='\0';
	    default :
	       DEBUGMSG((stderr,"Key of unknown type detected [%s][%c]?\n",
			 key_value,node->key_type));
	       break;
	 }
      }
   }

   
   if (status == END_OF_FILE)  status = 0; // Reset after normal error 
   fits_close_file(fptr, &status);

   return (CFITSIO_SUCCESS);

  error_exit:

   if(*keylist) 
   {
      cfitsio_free_keys(keylist);
      *keylist = NULL;
   }
   if(fptr) fits_close_file(fptr, &status);
   return error_code;
}

//****************************************************************************

int cfitsio_print_keys(CFITSIO_KEYWORD* keylist)
{	
   CFITSIO_KEYWORD* kptr;

   printf("\nKeys:\n");

   kptr = keylist;
   while(kptr != NULL)
   {
      if(strcmp(kptr->key_name,"COMMENT") && strcmp(kptr->key_name,"HISTORY"))
	 printf("%-10s= ",kptr->key_name);
	
      switch(kptr->key_type)
      {
	 case(kFITSRW_Type_String): printf("%s",kptr->key_value.vs); break;
	 case(kFITSRW_Type_Logical): printf("%19d",kptr->key_value.vl); break;
	 case(kFITSRW_Type_Integer): printf("%19d",kptr->key_value.vl); break;
	 case(kFITSRW_Type_Float): printf("%19f",kptr->key_value.vf); break;
	 case('X'): printf("%s",kptr->key_value.vs); break;
      }
      
      if (strlen(kptr->key_comment)) printf(" / %s\n",kptr->key_comment);
      else printf("\n");
 
      kptr = (CFITSIO_KEYWORD*) kptr->next;
   }

   return CFITSIO_SUCCESS;

}

//****************************************************************************


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
	 free(del);
      }

      *keylist = NULL;
   }
   return CFITSIO_SUCCESS;
}

//****************************************************************************
// TH: Append to the end of list to keep COMMENT in the right sequence.

int cfitsio_append_key(CFITSIO_KEYWORD** keylist, 
		       char *name, 
		       char type, 
		       char *comment,
		       void * value)
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
	       snprintf(node->key_value.vs, FLEN_VALUE, "%s", (char *)value);
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
      }
   }
   else
   {
      error_code = CFITSIO_ERROR_ARGS; 
   }

   return error_code;

}

//****************************************************************************


int cfitsio_read_image(char* fits_filename, void** image)
{
   fitsfile *fptr=NULL; 
   int status=0, error_code = CFITSIO_FAIL;
	
   long	first_pixel = 1;	// starting point 
   long	null_val = 0;		// don't check for null values in the image 
   double bscale = 1.0;		// over ride BSCALE, if there is 
   double bzero = 0.0;		// over ride BZERO, if there is 
	
   int naxis, data_type, bytepix, i; 

   long	npixels;

   CFITSIO_IMAGE_INFO info;
   double* pixels;
	

   if (*image) 
   {
      free(*image);
      *image = NULL;
   }

   memset((void*) &info,0,sizeof(CFITSIO_IMAGE_INFO));

   status = 0; // first thing!

   DEBUGMSG((stderr,"cfitsio_read_image()=> fits_filename = %s\n",fits_filename));

   // move directly to the first image
   if (fits_open_image(&fptr, fits_filename, READONLY, &status)) 
   {
      error_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }

   fits_get_img_dim(fptr, &naxis, &status);
   if(naxis == 0)
   {
     //"No image in this HDU.";
      error_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   fits_get_img_param(fptr, CFITSIO_MAX_DIM, &info.bitpix, &naxis, 
		      &info.naxes[0], &status); 


   switch(info.bitpix)
   {
      case(BYTE_IMG):    data_type = TBYTE; break;
      case(SHORT_IMG):   data_type = TSHORT; break;
      case(LONG_IMG):    data_type = TINT; break; 
      case(LONGLONG_IMG):data_type = TLONGLONG; break;
      case(FLOAT_IMG):   data_type = TFLOAT; break;
      case(DOUBLE_IMG):  data_type = TDOUBLE; break;
   }

   bytepix = abs(info.bitpix)/8;
	
   npixels = 1;
   for(i=0;i<naxis;i++) npixels *= info.naxes[i];
	
   pixels = (double*) calloc(npixels, bytepix); //get alignment to double
   if(pixels == NULL) 
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY; 
      goto error_exit;
   }

   //Turn off scaling and offset and image_null
   fits_set_bscale(fptr, bscale, bzero, &status);
   fits_set_imgnull(fptr, null_val, &status);

   if(fits_read_img(fptr, data_type, first_pixel, npixels, NULL, pixels,
		    NULL, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY; 
      goto error_exit;
   }

   
   *image = pixels;

   fits_close_file(fptr, &status);
   if(status == 0) return CFITSIO_SUCCESS;


  error_exit:
   cfitsio_print_error(status);
   if(*image) 
   {
      free(*image);
      *image = NULL;
   }
   if(fptr) fits_close_file(fptr, &status);
   return error_code;
}

//****************************************************************************
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

//****************************************************************************

int cfitsio_free_image(void ** image)
{
   if(*image) free(*image);
   *image = NULL;

   return CFITSIO_SUCCESS;
}

//****************************************************************************

int cfitsio_free_image_info(CFITSIO_IMAGE_INFO** image_info)
{
   if(*image_info) free(*image_info);
   *image_info = NULL;

   return CFITSIO_SUCCESS;
}

//****************************************************************************
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
	 strcpy(node->key_value.vs,card); //save the whole card into value .vs
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
	       strcpy(node->key_value.vs, key_value);
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
	       node->key_value.vs[0]='\0';
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
   while(kptr)
   {
      if(!strcmp(kptr->key_name,"SIMPLE")) 
      {
	 (*image_info)->simple = cfitsio_keyval2int(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_SIMPLE;
         }
      }
      else if(!strcmp(kptr->key_name,"EXTEND")) 
      {
	 (*image_info)->extend = cfitsio_keyval2int(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_EXTEND;
         }
      }
      else if(!strcmp(kptr->key_name,"BLANK"))
      {
	 (*image_info)->blank = cfitsio_keyval2longlong(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_BLANK;
         }
      }
      else if(!strcmp(kptr->key_name,"BSCALE"))
      {
	 (*image_info)->bscale = cfitsio_keyval2double(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_BSCALE;
         }
      }
      else if(!strcmp(kptr->key_name,"BZERO")) 
      {
	 (*image_info)->bzero = cfitsio_keyval2double(kptr, &status);
         if (!status)
         {
            (*image_info)->bitfield |= kInfoPresent_BZERO;
         }
      }
      
      kptr = kptr->next;          
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

   if(*image_info) 
   {
      free(*image_info);
      image_info = NULL;
   }
   
   return error_code;

}

int fitsrw_read_keylist_and_image_info(FITSRW_fhandle fhandle, 
                                       CFITSIO_KEYWORD** keylistout, 
                                       CFITSIO_IMAGE_INFO** image_info)
{
   return cfitsio_read_keylist_and_image_info((fitsfile *)fhandle, keylistout, image_info);
}

//****************************************************************************

int cfitsio_read_file(char* fits_filename,
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

   // Move directly to first image
   if (fits_open_image(&fptr, fits_filename, READONLY, &status)) 
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
 
   if(fptr) fits_close_file(fptr, &status);
 

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
   
   if(fptr) fits_close_file(fptr, &status);

   return error_code;

}

//******************************************************************************


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
	 free(del);
      }

      *keylist = NULL;
   }   

}

//****************************************************************************

int cfitsio_write_file(const char* fits_filename,
		       CFITSIO_IMAGE_INFO* image_info,  
		       void* image,
		       const char* compspec,
		       CFITSIO_KEYWORD* keylist) 
{

   fitsfile *fptr=NULL; 
   CFITSIO_KEYWORD* kptr;
   int status=0, error_code = CFITSIO_FAIL;
   char filename[PATH_MAX];
   char card[FLEN_CARD];
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

   if(fits_create_file(&fptr, filename, &status)) // create new FITS file 
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }

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
	 goto error_exit;
      }
      else if (data_type == TFLOAT || data_type == TDOUBLE)
      {
         fprintf(stderr, "CFITSIO compression of floating-point data is lossy, bailing.\n");
         goto error_exit;
      }
   }
   else if (status)
   {
      goto error_exit;
   }
   
   // keylist is optional. It is avaible only for export 
   if (keylist)
   {
      kptr = keylist;
      while(kptr)
      {

	 if( cfitsio_key_to_card(kptr,card) != CFITSIO_SUCCESS)
	 {
	    error_code = CFITSIO_ERROR_ARGS;
	    goto error_exit;
	 }

	 if(fits_get_keyclass(card) > TYP_CMPRS_KEY)
	 {
	   if(fits_write_record(fptr, card, &status))
	   {
	     error_code = CFITSIO_ERROR_LIBRARY;
	     goto error_exit;
	   }
	 }
		
	 kptr = kptr->next;
      }
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
      float obzero = (float)image_info->bzero;
      if (image_info->bitpix == BYTE_IMG)
      {
	 obzero = (float) (obzero -(128.0 * (float)image_info->bscale));
      }
     
      fits_update_key(fptr, TFLOAT, "BZERO", &obzero, "", &status);
   }
   if (image_info->bitfield & kInfoPresent_BSCALE)
   {
      float obscale = (float)image_info->bscale;
      fits_update_key(fptr, TFLOAT, "BSCALE", &obscale, "", &status);
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
         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(short));
         memset(misspix, (short)oblank, kMISSPIXBLOCK);
      }
      else if (image_info->bitpix == LONG_IMG)
      {
         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(int));
         memset(misspix, (int)oblank, kMISSPIXBLOCK);
      }
      else if (image_info->bitpix == LONGLONG_IMG)
      {
         /* use the value of oblank */
         misspix = malloc(kMISSPIXBLOCK * sizeof(long long));
         memset(misspix, (long long)oblank, kMISSPIXBLOCK);
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
	
   fits_close_file(fptr, &status);

   if(status == 0) return CFITSIO_SUCCESS;


  error_exit:

   fits_get_errstatus(status, cfitsiostat);
   fprintf(stderr, "cfitsio error '%s'.\n", cfitsiostat);
   if(fptr) fits_close_file(fptr, &status);
   return error_code;
}

//****************************************************************************
// Extract SIMPLE, EXTEND, BLANK, BZERO, BSCALE, NAXIS, NAXIS#, BITPIX from keylist
// If they are available...
// To add: if exist use ZNAXIS, ZNAXIS# and ZBITPIX instead

int cfitsio_extract_image_info_from_keylist(CFITSIO_KEYWORD* keylist, CFITSIO_IMAGE_INFO** image_info)
{

   int error_code = CFITSIO_SUCCESS;
   CFITSIO_KEYWORD* kptr;
   int axisnum,i;

   
   if((keylist==NULL)||(*image_info!=NULL)) return CFITSIO_ERROR_ARGS;

   
   *image_info = (CFITSIO_IMAGE_INFO*) malloc(sizeof(CFITSIO_IMAGE_INFO));
   if(*image_info == NULL)
   {
      error_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }  
   
   memset((void*) *image_info,0, sizeof(CFITSIO_IMAGE_INFO));
   (*image_info)->simple=1;
   (*image_info)->extend=0; // it might a string in CFITSIO (not logical)
   (*image_info)->blank =0;
   (*image_info)->bscale =1.0;
   (*image_info)->bzero =0.0;
   (*image_info)->naxis=0;
   for(i=0;i<CFITSIO_MAX_DIM;i++)(*image_info)->naxes[i]=0;
   

   kptr = keylist;
   while(kptr)
   {
      if(!strcmp(kptr->key_name,"SIMPLE")) 
      {
	 (*image_info)->simple = kptr->key_value.vl;
	 (*image_info)->bitfield |= kInfoPresent_SIMPLE;
      }
      else if(!strcmp(kptr->key_name,"EXTEND")) 
      {
	 (*image_info)->extend = kptr->key_value.vl;
	 (*image_info)->bitfield |= kInfoPresent_EXTEND;
      }
      else if(!strcmp(kptr->key_name,"BLANK"))
      {
	 (*image_info)->blank = kptr->key_value.vi;
	 (*image_info)->bitfield |= kInfoPresent_BLANK;
      }
      else if(!strcmp(kptr->key_name,"BSCALE"))
      {
	 (*image_info)->bscale = kptr->key_value.vf;
	 (*image_info)->bitfield |= kInfoPresent_BSCALE;
      }
      else if(!strcmp(kptr->key_name,"BZERO")) 
      {
	 (*image_info)->bzero = kptr->key_value.vf;
	 (*image_info)->bitfield |= kInfoPresent_BZERO;
      }
      else if(!strcmp(kptr->key_name,"BITPIX"))
      {
	 (*image_info)->bitpix = kptr->key_value.vi;
      }    
      else if(!strcmp(kptr->key_name,"NAXIS"))
      {
	 (*image_info)->naxis = kptr->key_value.vi;
      }
      else if (sscanf(kptr->key_name, "NAXIS%d", &axisnum) == 1)
      {
	 (*image_info)->naxes[axisnum - 1] =  kptr->key_value.vi;
      }

      kptr = kptr->next;
   }

   if(((*image_info)->bitpix == 0)||((*image_info)->naxis == 0)) error_code = CFITSIO_ERROR_ARGS;
   for(i=0;i<(*image_info)->naxis;i++) if((*image_info)->naxes[i] == 0) error_code = CFITSIO_ERROR_ARGS;

   if( error_code == CFITSIO_SUCCESS) return error_code;
   else goto error_exit;
   

 error_exit:

   cfitsio_free_image_info(image_info);  
   return error_code;

}


//****************************************************************************
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

//****************************************************************************


int cfitsio_key_to_card(CFITSIO_KEYWORD* kptr, char* card)
{
   char temp[FLEN_CARD];

   memset(card,0,sizeof(FLEN_CARD));
   if(!kptr) return CFITSIO_FAIL;

   //TH:  add check for upper case, length limits

   if((!strcmp(kptr->key_name,"HISTORY")||(!strcmp(kptr->key_name,"COMMENT"))))
   {
      strcpy(card,kptr->key_value.vs);
      return CFITSIO_SUCCESS;
   }

   switch(kptr->key_type)
   {
      case(kFITSRW_Type_String):
      case('X'):
	 if(strlen(kptr->key_comment) >0)
	 {
	    snprintf(temp, 
                     sizeof(temp), "%-8s= '%s' / %s",
                     kptr->key_name, 
                     kptr->key_value.vs, 
                     kptr->key_comment);
	 }
	 else
	 {
	    snprintf(temp, 
                     sizeof(temp), 
                     "%-8s= '%s'",
                     kptr->key_name, 
                     kptr->key_value.vs);
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
	 if(strlen(kptr->key_comment) >0)
	 {
	    sprintf(temp,"%-8s= %20G / %s", kptr->key_name, kptr->key_value.vf, kptr->key_comment);
	 }
	 else
	 {
	    sprintf(temp,"%-8s= %20G", kptr->key_name, kptr->key_value.vf);
	 }
	 break;
	 
	 //sprintf(temp,"%-8s= %20lf / %s", kptr->key_name, kptr->key_value.vf, kptr->key_comment); break;

   }

   sprintf(card,"%-80s",temp); // append space to the end to 80 chars
  
   //DEBUGMSG((stderr,"[01234567890123456789012345678901234567890123456789012345678901234567890123456789]\n"));
   //DEBUGMSG((stderr,"[%s]\n",card));

   return CFITSIO_SUCCESS;
}

//****************************************************************************

void cfitsio_print_error(int status)
{
   if (status)
   {
#ifdef __CFITSIO_DEBUG__
      fits_report_error(stderr, status); // print error report, the whole stack
#endif
      //exit( status ); // terminate the program, returning error status 
   }
   return;
}


//****************************************************************************
// To generate test images given a filename, with, bitpix.
// It generate image with 64 squares and 1 center square.
// center_value can be "0", "NaN", "Inf" ... and other special limits

int cfitsio_gen_image(char* fits_filename, int width, int bitpix, char* center_value)
{
   fitsfile *fptr=NULL;     
   CFITSIO_IMAGE_INFO info;
   int error_code, status=0;
   //double bscale=1.0, bzero=0.0;
   long nullval=0;

   int data_type;
   int bytepix, current_square,square,i,j,k;
   long  fpixel[2], lpixel[2], npixels;
   char *pixels, *p_ptr;

   
   union 
   {
	 char c;
	 int  i;
	 long l;
	 float f;	 
	 double d;
	 //longlong ll;
   } pvalue[67];

   
   info.bitpix = bitpix;
   
   info.naxis = 2;
   info.naxes[0]=width;
   info.naxes[1]=width;
   
   
   for(i=info.naxis;i<CFITSIO_MAX_DIM;i++) info.naxes[i]=0;  
   
   switch(info.bitpix)
   {
      case(BYTE_IMG):	data_type = TBYTE;
	 pvalue[0].c = 0;
	 pvalue[1].c = 1;//MAX-MIN /64
	 for(i=2;i<64;i++) pvalue[i].c= (char) (pvalue[i-1].c + pvalue[1].c);
	 pvalue[65].c = (char) atoi(center_value);
	 //for(i=0;i<64;i++) printf("byte pvalue[%d] = %d\n",i,pvalue[i].c);	 
	 break;
	 
      case(SHORT_IMG):	data_type = TSHORT;
	 pvalue[0].i = 0;
	 pvalue[1].i = 1;//MAX-MIN /64
	 for(i=2;i<64;i++) pvalue[i].i= pvalue[i-1].i + pvalue[1].i;
	 pvalue[65].i = atoi(center_value);
	 //for(i=0;i<64;i++) printf("int pvalue[%d] = %d\n",i,pvalue[i].i);	 
	 break;
      case(LONG_IMG):	data_type = TLONG; 
	 pvalue[0].l = 0;
	 pvalue[1].l = 1;//MAX-MIN /64
	 for(i=2;i<64;i++) pvalue[i].l= pvalue[i-1].l + pvalue[1].l;
	 pvalue[65].l = atol(center_value);
	 //for(i=0;i<64;i++) printf("long pvalue[%d] = %ld\n",i,pvalue[i].l);	 
	 break; 

      case(FLOAT_IMG):	data_type = TFLOAT;
	 pvalue[0].f = 0.0;
	 pvalue[1].f = 1.0;//MAX-MIN /64
	 for(i=2;i<64;i++) pvalue[i].f= pvalue[i-1].f + pvalue[1].f;
	 pvalue[65].f = (float) atof(center_value);
	 //for(i=0;i<64;i++) printf("float pvalue[%d] = %f\n",i,pvalue[i].f);	 
	 break; 

      case(DOUBLE_IMG):	data_type = TDOUBLE; 
	 pvalue[0].d = 0;
	 pvalue[1].d = 1;//MAX-MIN /64
	 for(i=2;i<64;i++) pvalue[i].d= pvalue[i-1].d + pvalue[1].d;
 	 pvalue[65].d = atof(center_value); 
	 //for(i=0;i<64;i++) printf("double pvalue[%d] = %lf\n",i,pvalue[i].d);	 
	 break;
	 
   }

   bytepix = abs(info.bitpix)/8;
 
   square = width/8;
   npixels = square*square; //we will get 8x8 or 64 squares
   pixels = (void*) ((double*) calloc(npixels, bytepix)); // align to double

   
   status = 0; 
   remove(fits_filename);     
   if(fits_create_file(&fptr, fits_filename, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   if(fits_create_img(fptr, info.bitpix, info.naxis, info.naxes, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }
 
   //fits_update_key(fptr, TDOUBLE, "BSCALE", &bscale, "BSCALE", &status);
   //fits_update_key(fptr, TDOUBLE, "BZERO", &bzero, "BZERO", &status);

   if(fits_set_imgnull(fptr, nullval, &status))
   {
     error_code = CFITSIO_ERROR_LIBRARY;
     goto error_exit;
   }      
 

   
   current_square =0;
 
   for(i=0;i<8;i++)
   {
      fpixel[1]=i*square+1;
      lpixel[1]=(i+1)*square;
      for(j=0;j<8;j++, current_square++)
      {
	 fpixel[0] = j*square+1;
	 lpixel[0] = (j+1)*square;
	 p_ptr = pixels;
	 for (k = 0; k<npixels; k++, p_ptr+= bytepix) 
	 {
	    switch(info.bitpix)
	    {
	       case(BYTE_IMG):	 *((char*)p_ptr) = pvalue[current_square].c; break;
	       case(SHORT_IMG): *((int*)p_ptr) = pvalue[current_square].i; break;
	       case(LONG_IMG):  *((long*)p_ptr) = pvalue[current_square].l; break; 			
	       case(FLOAT_IMG): *((float*)p_ptr) = pvalue[current_square].f; break;
	       case(DOUBLE_IMG):*((double*)p_ptr) = pvalue[current_square].d; break;
	    }
	 }

	 if (fits_write_subset(fptr, data_type, fpixel, lpixel, pixels, &status))
	 {
	    error_code = CFITSIO_ERROR_LIBRARY;
	    goto error_exit;
	 }

      }
   }
     
   
   // Center square
   fpixel[0]=(long)(3.5*square)+1;  fpixel[1]= fpixel[0];
   lpixel[0]=(long) fpixel[0] + square-1;   lpixel[1]=lpixel[0];
   
   p_ptr = pixels;
   for (k = 0; k<npixels; k++, p_ptr+= bytepix) 
   {
      switch(info.bitpix)
      {
	 case(BYTE_IMG):  *((char*)p_ptr) = pvalue[65].c; break;
	 case(SHORT_IMG): *((int*)p_ptr) = pvalue[65].i; break;
	 case(LONG_IMG):  *((long*)p_ptr) = pvalue[65].l; break; 			
	 case(FLOAT_IMG): *((float*)p_ptr) = pvalue[65].f; break;
	 case(DOUBLE_IMG):*((double*)p_ptr) = pvalue[65].d; break;
      }
   }

   
   if (fits_write_subset(fptr, data_type, fpixel, lpixel, pixels, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }


   fits_close_file(fptr, &status);
   if(pixels) free(pixels);
   if(status == 0) return CFITSIO_SUCCESS;

  error_exit:

   if(status) cfitsio_print_error(status);
   if(fptr) fits_close_file(fptr, &status);
   if(pixels) free(pixels);

   return error_code;
}

//****************************************************************************
//****************************************************************************
