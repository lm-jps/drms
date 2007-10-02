#ifndef __DWT_COMPRESS_H
#define __DWT_COMPRESS_H

/*************** Constants *******************/
#define DWT_HAAR 0
#define DWT_CD22 1
#define DWT_CD12 2




/*************** Prototypes *******************/
int dwt_compress(int transform, int quant, int levels, int width, int height, 
		 short *data, int *lengths, unsigned char *zdata);
int dwt_decompress(int transform, int quant, int levels, int width, int height,
		   int *lengths, unsigned char *zdata, short *data);
void dwt_down(int transform, int quant, int width, int height, 
	      short *input,  short *output);
void dwt_up(int transform, int quant, int width, int height, 
	    short *input,  short *output);

int dwt_rec(int transform, int quant, int levels, int width, int height, 
	    short *tmp, short *data, int *lengths, unsigned char *zdata);
int dwtinv_rec(int transform, int quant, int levels, int width, int height, 
	       short *tmp, int *lengths, unsigned char *zdata, 
	       short *data);



#endif
