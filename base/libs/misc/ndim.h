#ifndef __NDIM_H
#define __NDIM_H

/* Copy data from a hyper-slab of an n-dimensional array into a 
   consecutive array. */
int ndim_unpack(int sz, int ndim, int *dims, int *start, int *end, 
		unsigned char *inarray, unsigned char *outbuf);

/* Copy consecutive data into a hyper-slab of an 
   n-dimensional array. */
int ndim_pack(int sz, int ndim, int *dims, int *start, int *end, 
	      unsigned char *inbuf, unsigned char *outarray);


/* Transpose the elements in the input array such that the 
   dimensions are permuted according to the permutation given
   in perm. */
int ndim_permute(int sz, int ndim, int *dims, int *perm, 
		 unsigned char *in, unsigned char *out);
#endif
