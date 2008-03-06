//****************************************************************************
// CFITSIO.C
//
// Simple test code demo the use of CFITSIO.
// 
//
//****************************************************************************

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "fitsio.h"
#include "cfitsio.h"

//****************************************************************************


int main()
{

   CFITSIO_KEYWORD* keys  = NULL;
   void* image = NULL;
   char* header = NULL;

   // Read keywords and values from a fits file
   cfitsio_read_keys("image_1.fits", &keys); 
   cfitsio_print_keys(keys);


   // Read Header from a fits file
   cfitsio_read_header("hmi_image_1.fits", &header);
   cfitsio_print_header(header);

   // Read Image from a fits file
   cfitsio_read_image("hmi_image_1.fits", &image); 



   // Write none_compress file
   cfitsio_write_file("hmi_image_1_out.fits", header, image, C_NONE);

   // Default compress file use Rice algo and compress row by row
   cfitsio_write_file("hmi_image_1_defautl.fits", header, image, C_DEFAULT);

   // Write default compress file using Rice algo and compress row by row
   cfitsio_write_file("hmi_image_1_r.fits", header, image, C_RICE);

   // Write a compress file using Rice algo, title size 100x100
   cfitsio_write_file("hmi_image_1_c.fits[compress R 100, 100]", header, image, C_EXTENDED_FILENAME);


   if(header)	free(header);
   if(image)	free(image);
   if(keys)	free(keys); 

   return(0);
}
