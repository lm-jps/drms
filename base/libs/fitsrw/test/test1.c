#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "fitsio.h"
#include "../cfitsio.h"

//****************************************************************************
//Simple test code

int main()
{
	CFITSIO_KEYWORD*	key  = NULL;
	void*	image = NULL;
	char* header = NULL;

	cfitsio_read_keys("image_1.fits", &key); 
	cfitsio_print_keys(key);
	
	cfitsio_read_header("hmi_image_1.fits", &header);
	cfitsio_print_header(header);

	cfitsio_read_image("hmi_image_1.fits", &image); 
	cfitsio_write_file("hmi_image_1_out.fits", header, image);

	cfitsio_write_compress("hmi_image_1_rice.fits", header, image, C_ROW);
	cfitsio_write_compress("hmi_image_1_hcomp.fits", header, image, C_IMAGE);





	/*
	cfitsio_write_compress("hmi_image_1_out_h.fits", key, image, C_IMAGE);
	cfitsio_write_compress("hmi_image_1_out_r.fits", key, image, C_ROW);
	*/


	/*
	cfitsio_read_keys("image_1.fits", &key); 
	cfitsio_print_keys(key);

	cfitsio_read_image("image_1.fits", &image); 
	
	cfitsio_write_file("image_1_out.fits", key, image);
	
	cfitsio_write_compress("image_1_out_h.fits", key, image, C_IMAGE);
	cfitsio_write_compress("image_1_out_r.fits", key, image, C_ROW);
	*/

	//cfitsio_read_image("hmi_image_1.fits", &image);
	//cfitsio_write("hmi_image_1_none.fit", header , nkeys, image);

	//cfitsio_write_compress_3("hmi_image_1_row.fit",
	//	header,nkeys,image,C_ROW);

	//cfitsio_write_compress_4("hmi_image_1_none.fit", header , nkeys, image, C_NONE);

	//cfitsio_write_compress_2("image_1_file_ext.fit[compress]",header,nkeys,image,C_EXTENDED_FILENAME);


	if(header)	free(header);
	if(image)	free(image);
	if(key)		free(key); 

    return(0);
}
//****************************************************************************
//****************************************************************************
