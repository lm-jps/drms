//****************************************************************************
// CFITSIO.C
//
// These are wrapper functions to read/write FITS file format 
// using CFITSIO library
//
//****************************************************************************

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "fitsio.h"
#include "cfitsio.h"


//****************************************************************************

//#define __CFITSIO_DEBUG__	    //Turn on for DEBUGMSG
#ifdef  __CFITSIO_DEBUG__
#define DEBUGMSG(msg)  printf msg   //ex: DEBUGMSG (("Hello World %d\n", 123));
#else
#define DEBUGMSG(msg)  //nop
#endif

//****************************************************************************
//****************************************************************************

int cfitsio_read_header(char* fits_filename, char** header)
{
   fitsfile *fptr=NULL;        
   char *cptr;
   int status=0, nkeys, i;
   int exit_code = CFITSIO_FAIL;
	
   if(*header) free(*header);

   status =0;

   //move directly to first image
   if (fits_open_image(&fptr, fits_filename, READONLY, &status)) 
   {
      exit_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }
   
   fits_get_hdrspace(fptr, &nkeys, NULL, &status);

   // add 1 card to hold "COMMENT CFITSIO_NKEYS=int" on the first card
   *header = (char*) malloc((nkeys+1)* FLEN_CARD);
   if(*header == NULL)
   {
      exit_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }
		
   cptr = *header;
   sprintf(cptr,"COMMENT CFITSIO_HEADER_NKEYS=%d",nkeys);
   cptr = cptr + FLEN_CARD;
   for(i=1;i<=nkeys;i++, cptr = cptr+ FLEN_CARD)
   {
      fits_read_record(fptr, i, cptr, &status);
   }

   if (status) 
   {
      exit_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   return CFITSIO_SUCCESS;

 error_exit:

   if(*header) free(*header);
   *header = NULL;
   if(fptr) fits_close_file(fptr, &status);
   return exit_code;
}

//****************************************************************************

int cfitsio_print_header(char* header)
{
   char* cptr;
   int nkeys, i;

   if((!header)||(!strstr(header,"COMMENT CFITSIO_HEADER_NKEYS="))) return CFITSIO_FAIL;
   sscanf((header+29),"%d",&nkeys);
	
   cptr = header + FLEN_CARD; // skip the first card
   for(i=0;i<nkeys;i++, cptr = cptr+FLEN_CARD)
   {
      printf("%s\n",cptr);
   }

   return CFITSIO_SUCCESS;

}

//****************************************************************************

int cfitsio_read_keys(char* fits_filename, CFITSIO_KEYWORD** list)
{
   fitsfile *fptr=NULL;        
   char card[FLEN_CARD];
   int status = 0;			
   int nkeys, i;
   int exit_code = CFITSIO_FAIL;

   CFITSIO_KEYWORD* kptr;
   int  len, counts;

   char key_name[FLEN_KEYWORD];
   char key_value[FLEN_VALUE];

   if(*list) 
   {
      free(*list);
      *list = NULL;
   }

   //move directly to first image
   if (fits_open_image(&fptr, fits_filename, READONLY, &status)) 
   {
      exit_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }

   fits_get_hdrspace(fptr, &nkeys, NULL, &status);

   *list = (CFITSIO_KEYWORD *) malloc( (long) nkeys *  sizeof(CFITSIO_KEYWORD));
   if(*list == NULL) 
   {
      exit_code = CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }


   counts = 0;
   kptr = NULL;
		
   for(i=1; i<=nkeys; i++) 
   {
      if(fits_read_record(fptr, i, card, &status))
      {
	 exit_code = CFITSIO_ERROR_LIBRARY;
	 goto error_exit;
      }

      if(fits_get_keyname(card, key_name, &len, &status))
      {
	 exit_code = CFITSIO_ERROR_LIBRARY;
	 goto error_exit;
      }

      // Skip the COMMENT and HISTORY records
      if((!strcmp(key_name,"COMMENT")) || (!strcmp(key_name,"HISTORY"))) continue;
					
      // Found a usefuly keyword, move the next node.
      if(kptr==NULL) //first item
      {
	 kptr = *list;
	 kptr->next = NULL;
      }
      else 
      {
	 kptr->next = kptr + 1;
	 kptr = kptr + 1;
	 kptr->next = NULL;
      }

      // Copy data list
      strcpy(kptr->key_name, key_name);
      if(fits_parse_value(card, key_value, kptr->key_comment, &status)) 
      {
	 exit_code = CFITSIO_ERROR_LIBRARY;
	 goto error_exit;
      }

      if(strlen(key_value)) fits_get_keytype(key_value, &kptr->key_type, &status);
      else	kptr->key_type = ' '; 

      switch(kptr->key_type)
      {
      case ('X'): //complex number is stored as string, for now.
      case ('C'): //Trip off ' ' around cstring? 
	 strcpy(kptr->key_value.vs, key_value);
	 break;
      case ('L'): if (key_value[0]=='0') kptr->key_value.vl = 0;
      else kptr->key_value.vl = 1;
	 break;

      case ('I'): sscanf(key_value,"%ld", &kptr->key_value.vi);
	 break;

      case ('F'): sscanf(key_value,"%lf", &kptr->key_value.vf);
	 break;

      case (' '): //type not found, set it to NULL string
	 kptr->key_type = 'C';
	 kptr->key_value.vs[0]='\0';
      default :
	 DEBUGMSG(("Key of unknown type detected [%s][%c]?\n",
		   key_value,kptr->key_type));
	 break;
      }

      DEBUGMSG(("%s = %s (%c)\n", key_name, key_value, kptr->key_type));

      counts++;
   }

   DEBUGMSG(("\nFound %d keys out of %d records in hdu\n",counts, nkeys));

   if (status == END_OF_FILE)  status = 0; // Reset after normal error 
   fits_close_file(fptr, &status);

   return (CFITSIO_SUCCESS);

 error_exit:

   if(*list) 
   {
      free(*list);
      *list = NULL;
   }
   if(fptr) fits_close_file(fptr, &status);
   return exit_code;
}

//****************************************************************************

int cfitsio_print_keys(CFITSIO_KEYWORD* list)
{	
   CFITSIO_KEYWORD* kptr;

   printf("\nKeys:\n");

   kptr = list;
   while(kptr != NULL)
   {
      printf("%10s= ",kptr->key_name);
	
      switch(kptr->key_type)
      {
      case('C'): printf("%s",kptr->key_value.vs); break;
      case('L'): printf("%d",kptr->key_value.vl); break;
      case('I'): printf("%d",kptr->key_value.vl); break;
      case('F'): printf("%f",kptr->key_value.vf); break;
      case('X'): printf("%s",kptr->key_value.vs); break;
      }
      printf(" / %s\n",kptr->key_comment);
      kptr = (CFITSIO_KEYWORD*) kptr->next;
   }

   return CFITSIO_SUCCESS;

}

//****************************************************************************

void cfitsio_free_keys(CFITSIO_KEYWORD** list)
{
   /* TODO: Free the list */

   *list = NULL;
}

//****************************************************************************

int cfitsio_read_image(char* fits_filename, void** image)
{
   fitsfile *fptr=NULL; 
   int status=0, exit_code = CFITSIO_FAIL;
	
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

   DEBUGMSG(("cfitsio_read_image()=> fits_filename = %s\n",fits_filename));

   // move directly to the first image
   if (fits_open_image(&fptr, fits_filename, READONLY, &status)) 
   {
      exit_code = CFITSIO_ERROR_FILE_DOESNT_EXIST;
      goto error_exit;
   }

   fits_get_img_dim(fptr, &naxis, &status);
   if(naxis == 0)
   {
      DEBUGMSG(("No image in this HDU."));
      exit_code = CFITSIO_ERROR_DATA_EMPTY;
      goto error_exit;
   }

   fits_get_img_param(fptr, CFITSIO_MAX_DIM, &info.bitpix, &naxis, 
		      &info.naxes[0], &status); 

   DEBUGMSG(("naxis = %d, bitpix = %d, ", naxis, info.bitpix));
   for(i=0;i<naxis;i++) DEBUGMSG(("axes[%d] = %ld ",i,info.naxes[i]));
   DEBUGMSG(("\n"));

   switch(info.bitpix)
   {
   case(BYTE_IMG):	data_type = TBYTE; break;
   case(SHORT_IMG):	data_type = TSHORT; break;
   case(LONG_IMG):	data_type = TLONG; break;
   case(FLOAT_IMG):	data_type = TFLOAT; break;
   case(DOUBLE_IMG): data_type = TDOUBLE;	break;
   }
   bytepix = abs(info.bitpix)/8;
	
   npixels = 1;
   for(i=0;i<naxis;i++) npixels *= info.naxes[i];
	
   pixels = (double*) calloc(npixels, bytepix); //get alignment double
   if(pixels == NULL) 
   {
      exit_code = CFITSIO_ERROR_OUT_OF_MEMORY; 
      goto error_exit;
   }

   //Turn off scaling and offset and image_null
   fits_set_bscale(fptr, bscale, bzero, &status);
   fits_set_imgnull(fptr, null_val, &status);

   if(fits_read_img(fptr, data_type, first_pixel, npixels, NULL, pixels,
		    NULL, &status))
   {
      exit_code = CFITSIO_ERROR_LIBRARY; 
      goto error_exit;
   }

   // Just checking....
   cfitsio_dump_image(pixels, &info, 1, 2, 1, 10);

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
   return exit_code;
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

   DEBUGMSG(("In cfitsio_dump_image() => image = %p\n", image));

   if((from_row < 1)||(to_row > info->naxes[1])||(from_col < 1)||
      (to_col > info->naxes[0]))
   {
      DEBUGMSG(("cfitsio_dump_image() out of range.\n"));
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

void cfitsio_free_image(void **image)
{
   /* TODO: Free the data */

   *image = NULL;
}

//****************************************************************************

int cfitsio_read_file(char* filename, CFITSIO_KEYWORD** list, void** image)
{
   /* TODO */

   return CFITSIO_SUCCESS;
}

//****************************************************************************


void cfitsio_free_keysandimg(CFITSIO_KEYWORD** list, void** image)
{
   cfitsio_free_keys(list);
   cfitsio_free_image(image);
}

//****************************************************************************

int cfitsio_write_file(char* fits_filename, char* header, void* image, 
		       CFITSIO_COMPRESSION_TYPE compression_type)
{

   fitsfile *fptr=NULL; 
   int status=0, error_code = CFITSIO_FAIL;
   char * cptr;
   char filename[500];
   int nkeys, data_type, i;


   long	first_pixel = 1;	// starting point 
   long	null_val = 0;		// don't check for null values in the image 
   double	bscale = 1.0;	// over ride BSCALE, if there is 
   double	bzero = 0.0;	// over ride BZERO, if there is 

   long	npixels;

   CFITSIO_IMAGE_INFO info;


   DEBUGMSG(("cfitsio_write_file() => fits_filename = %s\n",fits_filename));


   if(cfitsio_get_image_info(header, &info)!= CFITSIO_SUCCESS)
   {
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }

   DEBUGMSG(("BITPIX=%d\n",info.bitpix));
   DEBUGMSG(("NAXIS=%ld\n",info.naxis));
   for(i=0;i<info.naxis;i++)
   {
      DEBUGMSG(("NAXES[%d]=%ld\n",i,info.naxes[i]));
   }


   first_pixel = 1;                              
   npixels = ((info.naxes[0]) * (info.naxes[1]));    

   switch(info.bitpix)
   {
   case(BYTE_IMG):	data_type = TBYTE; break;
   case(SHORT_IMG):	data_type = TSHORT; break;
   case(LONG_IMG):	data_type = TLONG; break; 
   case(FLOAT_IMG):	data_type = TFLOAT; break; 
   case(DOUBLE_IMG):	data_type = TDOUBLE; break;
   }

   // Remove the file, if alreay exit
   strcpy(filename, fits_filename);
   cptr = strstr(filename,"[compress");  //"["
   if(cptr)	*cptr = '\0';
   remove(filename); 


   switch(compression_type)
   {
   case (C_DEFAULT):
   case (C_RICE): strcat(filename,"[compress]");
      break;

   case(C_NONE):					// not compress
   case(C_HCOMPRESS):			// hcompress algorithm, whole image
   case(C_GZIP):					// gzip algorithm
   case(C_PLIO):					// plio algorithm
      //for now, either rice or none
      break;

   case(C_EXTENDED_FILENAME):	// user spec
      strcpy(filename, fits_filename);
      break;
   default:
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
      break;
   }
  	
   status = 0; // first thing!

   if(fits_create_file(&fptr, filename, &status)) // create new FITS file 
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }

   if(fits_create_img(fptr, info.bitpix, info.naxis, info.naxes, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   if((!header)||(!strstr(header,"COMMENT CFITSIO_HEADER_NKEYS=")))
   {
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }
   sscanf((header+29),"%d",&nkeys);
	

   cptr = header + FLEN_CARD; //skip the first one
   for(i=1;i<=nkeys;i++,cptr=cptr+FLEN_CARD)
   {
      if(fits_get_keyclass(cptr) > TYP_CMPRS_KEY)
	 fits_write_record(fptr, cptr, &status);
   }

   //Turn off scaling and offset and null_value
   fits_set_bscale(fptr, bscale, bzero, &status);
   fits_set_imgnull(fptr, null_val, &status);

   if(fits_write_img(fptr, data_type, first_pixel, npixels, image, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }
	
   fits_close_file(fptr, &status);
   if(status == 0) return CFITSIO_SUCCESS;


 error_exit:
   cfitsio_print_error(status);
   if(fptr) fits_close_file(fptr, &status);
   return error_code;
}

//****************************************************************************

int cfitsio_pure_write_file(char* fits_filename, char* header, void* image)
{
   fitsfile *fptr=NULL; 
   int status=0, error_code = CFITSIO_FAIL;
   char * cptr;
   int nkeys, data_type, i;


   long	first_pixel = 1;	// starting point 
   long	null_val = 0;		// don't check for null values in the image 
   double	bscale = 1.0;	// over ride BSCALE, if there is 
   double	bzero = 0.0;	// over ride BZERO, if there is 

   long	npixels;

   CFITSIO_IMAGE_INFO info;


   DEBUGMSG(("cfitsio_write_file() => fits_filename = %s\n",fits_filename));


   if(cfitsio_get_image_info(header, &info)!= CFITSIO_SUCCESS)
   {
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }

   DEBUGMSG(("BITPIX=%d\n",info.bitpix));
   DEBUGMSG(("NAXIS=%ld\n",info.naxis));
   for(i=0;i<info.naxis;i++)
   {
      DEBUGMSG(("NAXES[%d]=%ld\n",i,info.naxes[i]));
   }


   first_pixel = 1;                              
   npixels = ((info.naxes[0]) * (info.naxes[1]));    

   switch(info.bitpix)
   {
   case(BYTE_IMG):	data_type = TBYTE; break;
   case(SHORT_IMG):	data_type = TSHORT; break;
   case(LONG_IMG):	data_type = TLONG; break; 
   case(FLOAT_IMG):	data_type = TFLOAT; break; 
   case(DOUBLE_IMG):	data_type = TDOUBLE; break;
   }
   //bytepix = abs(info.bitpix)/8;

   status = 0; // first thing!

   remove(fits_filename); //if it already exist
   if(fits_create_file(&fptr, fits_filename, &status)) // create new FITS file 
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }

   if(fits_create_img(fptr, info.bitpix, info.naxis, info.naxes, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }

   if((!header)||(!strstr(header,"COMMENT CFITSIO_HEADER_NKEYS=")))
   {
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }
   sscanf((header+29),"%d",&nkeys);
	

   cptr = header + FLEN_CARD; //skip the first one
   for(i=1;i<=nkeys;i++,cptr=cptr+FLEN_CARD)
   {
      if(fits_get_keyclass(cptr) > TYP_CMPRS_KEY)
	 fits_write_record(fptr, cptr, &status);
   }

   //Turn off scaling and offset and null_value
   fits_set_bscale(fptr, bscale, bzero, &status);
   fits_set_imgnull(fptr, null_val, &status);

   if(fits_write_img(fptr, data_type, first_pixel, npixels, image, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }
	
   fits_close_file(fptr, &status);
   if(status == 0) return CFITSIO_SUCCESS;


 error_exit:
   cfitsio_print_error(status);
   if(fptr) fits_close_file(fptr, &status);
   return error_code;
}

//****************************************************************************

int cfitsio_get_image_info(char* header, CFITSIO_IMAGE_INFO* info)
{
   char key_name[FLEN_CARD], * cptr;
   int nkeys, axis_found = 0;
   int i,j;


   memset(info,0,sizeof(CFITSIO_IMAGE_INFO));

   if(!header) return CFITSIO_FAIL;
   if (!strstr(header,"COMMENT CFITSIO_HEADER_NKEYS=")) return CFITSIO_FAIL;
   sscanf((header+29),"%d",&nkeys);
	
   cptr = header + FLEN_CARD; // skip the first card


   for(i=0;i<nkeys;i++, cptr = cptr+FLEN_CARD)
   {
      if(!strncmp(cptr,"BITPIX  =",9))
      {
	 sscanf(cptr+9,"%d",&(info->bitpix));
      }

      if(!strncmp(cptr,"NAXIS   =",9))
      {
	 sscanf(cptr+9,"%ld",&(info->naxis));
      }

      if(info->naxis !=0)
      {
	 for(j=0;j<info->naxis;j++)
	 {
	    sprintf(key_name,"NAXIS%d  =",j+1);
	    if(!strncmp(cptr,key_name,9))
	    {
	       sscanf(cptr+9,"%ld",&(info->naxes[j]));
	       axis_found++;
	       if(axis_found == info->naxis) return CFITSIO_SUCCESS;
	    }
	 }
      }
   }

   return CFITSIO_FAIL;
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
// Write then compress using fits_image_compress(inptr, outfptr)

int cfitsio_write_compress(char* fits_filename, char* header, void* image, 
			   CFITSIO_COMPRESSION_TYPE compression_type)
{
   fitsfile *in_fptr=NULL, *out_fptr=NULL;
   int error_code;
   int status=0, nkeys, i;
   long tilesize[2];		
   char temp_filename[]="temp.fits";
   char *cptr;

   CFITSIO_IMAGE_INFO info;


   DEBUGMSG(("cfitsio_write_compress() => fits_filename = %s\n",fits_filename));

   if(cfitsio_get_image_info(header, &info)!= CFITSIO_SUCCESS)
   {
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }

	
   remove(temp_filename);
   if(cfitsio_pure_write_file(temp_filename, header, image)!= CFITSIO_SUCCESS)
   {
      error_code = CFITSIO_FAIL;
      goto error_exit;
   }
	

   //TH: ==> You got temp.fits ... let start there


   status = 0;

   if(fits_open_image(&in_fptr, temp_filename, READONLY, &status))
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }

   /*
     if( fits_is_compressed_image(in_fptr, &status))  //TH: fail to detect if header striped off!
     {
     DEBUGMSG(("Already compressed file"));
     error_code = CFITSIO_ERROR_ALREADY_COMPRESSED;
     goto error_exit;
     }*/


   remove(fits_filename); //if it already exist
   if(fits_create_file(&out_fptr, fits_filename, &status))
   {
      error_code = CFITSIO_ERROR_FILE_IO;
      goto error_exit;
   }


   // CFITSIO can compress image using 4 algorithms: HCOMPRESS (H), Rice (R), GZIP (G) and PLIO (P)
   // Setup compression paras, then "fits_img_compress(in_fptr, out_fptr, &status)"

   switch(compression_type)
   {
   case(C_HCOMPRESS):		// HCOMPRESS, whole image

      if(fits_set_compression_type(out_fptr, HCOMPRESS_1, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }
      tilesize[0] = info.naxes[0];
      tilesize[1] = info.naxes[1];

      if(fits_set_tile_dim(out_fptr, 2, tilesize, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }

      if(fits_set_hcomp_scale(out_fptr, 1, &status)) // loss-less. 
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }
			
      if(fits_set_hcomp_smooth(out_fptr, 0, &status)) // no smooth.
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }
			
      if((info.bitpix == 32)||(info.bitpix == 64)) //Only relevant with BITPIX = 32 or 64
      {
	 if(fits_set_noise_bits(out_fptr, 4, &status)) 
	 {				
	    error_code = CFITSIO_ERROR_LIBRARY;  
	    goto error_exit;
	 }
      }
      break;

   case(C_RICE):		// Rice, row by row

      if(fits_set_compression_type(out_fptr, RICE_1, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }

      tilesize[0] = info.naxes[0];
      tilesize[1] = 1;

      if(fits_set_tile_dim(out_fptr, 2, tilesize, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }
			
      if((info.bitpix == 32)||(info.bitpix == 64)) //Only relevant with BITPIX = 32 or 64
      {
	 if(fits_set_noise_bits(out_fptr, 4, &status)) 
	 {				
	    error_code = CFITSIO_ERROR_LIBRARY;  
	    goto error_exit;
	 }
      }
      break;

   case(C_EXTENDED_FILENAME):
      if(!strstr(fits_filename,"[compress")) 
      {
	 error_code = CFITSIO_ERROR_ARGS;
	 goto error_exit;
      }
      break;

   case(C_NONE):
   default:

      if(fits_set_compression_type(out_fptr, 0, &status))
      {
	 error_code = CFITSIO_ERROR_LIBRARY;  
	 goto error_exit;
      }

      break;
   }


   //Turn off scaling and offset and null_value
   fits_set_bscale(in_fptr, 1.0, 0.0, &status);
   fits_set_imgnull(in_fptr, 0, &status);
	
   fits_set_bscale(out_fptr, 1.0, 0.0, &status);
   fits_set_imgnull(out_fptr, 0, &status);
	
	
   DEBUGMSG(("Copy HDU over\n"));

   /*
   //Copy HDU over, no need
   if(fits_copy_hdu(in_fptr, out_fptr, 0,&status))
   {		
   error_code = CFITSIO_ERROR_LIBRARY;
   goto error_exit;
   }*/


   // Header needs to come in before create_image to have 1 card
   if((!header)||(!strstr(header,"COMMENT CFITSIO_HEADER_NKEYS=")))
   {
      error_code = CFITSIO_ERROR_ARGS;
      goto error_exit;
   }
   sscanf((header+29),"%d",&nkeys);
   cptr = header + FLEN_CARD; //skip the first one
   for(i=1;i<=nkeys;i++,cptr=cptr+FLEN_CARD)
   {
      if(fits_get_keyclass(cptr) > TYP_CMPRS_KEY)
	 fits_write_record(out_fptr, cptr, &status);
   }

	
   DEBUGMSG(("Compress it\n"));

   // Compress it
   if(fits_img_compress(in_fptr, out_fptr, &status)) 
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }
	
   fits_close_file(in_fptr, &status);
   fits_close_file(out_fptr, &status);
   if(status == 0) return CFITSIO_SUCCESS;

 error_exit:
   if(in_fptr) fits_close_file(in_fptr, &status);
   if(out_fptr) fits_close_file(out_fptr, &status);
   if(status) cfitsio_print_error(status);
   return error_code;
}

//****************************************************************************
// This works => add/plan to image
//
int cfitsio_gen2d_image(char* fits_filename, long width, long height)
{
   fitsfile *fptr=NULL;     
   CFITSIO_IMAGE_INFO info;
   int error_code, status=0;
   double bscale=1.0, bzero=0.0;

   int i, j;
   long  fpixel, npixels;
   unsigned short ** row_array, *iptr;


   info.bitpix = USHORT_IMG;
   info.naxis = 2;
   info.naxes[0]=width;
   info.naxes[1]=height;

	
   row_array = (unsigned short**) malloc(info.naxes[0]* sizeof(unsigned short*));
   npixels = info.naxes[0] * info.naxes[1];

   row_array[0] = (unsigned short*) malloc(npixels * sizeof(unsigned short));
   if((row_array[0] == NULL)||(row_array == NULL))
   {
      error_code =  CFITSIO_ERROR_OUT_OF_MEMORY;
      goto error_exit;
   }

   for(i=1;i<info.naxes[0];i++) *(row_array+i) = *(row_array+i-1) + info.naxes[0];

   DEBUGMSG(("From read_image() => image = %p\n", row_array[0]));
   DEBUGMSG((" 1st = %p \n 2nd = %p \n 3nd = %p\n",*(row_array), *(row_array+1), *(row_array+2)));


   status = 0; 
   remove(fits_filename);     
   if(fits_create_file(&fptr, fits_filename, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }



   if(fits_create_img(fptr,  info.bitpix, info.naxis, info.naxes, &status))
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }
      
   // initialize the values in the image with a linear ramp function 
   for (j = 0; j < info.naxes[1]; j++)
   {   
      iptr = *(row_array+j);
      for (i = 0; i < info.naxes[0]; i++, iptr++)
      {
	 *iptr = (unsigned short) (i+j);
      }
   }

   fpixel = 1;
 
   fits_update_key(fptr, TDOUBLE, "BSCALE", &bscale, "BSCALE", &status);
   fits_update_key(fptr, TDOUBLE, "BZERO", &bzero, "BZERO", &status);

   if( fits_write_img(fptr, TUSHORT, fpixel, npixels, row_array[0], &status) )
   {
      error_code = CFITSIO_ERROR_LIBRARY;
      goto error_exit;
   }
     
   free(row_array[0]); 
   free(row_array);

   fits_close_file(fptr, &status);
   if(status == 0) return CFITSIO_SUCCESS;

 error_exit:

   if(status) cfitsio_print_error(status);
   if(fptr) fits_close_file(fptr, &status);
   if(row_array[0]) free(row_array[0]);
   if(row_array) free(row_array);

   return error_code;
}

//****************************************************************************
//****************************************************************************
//****************************************************************************
//****************************************************************************
