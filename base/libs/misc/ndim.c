#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ndim.h"

/* Pack an n-dimensional array block into a buffer. */
int ndim_pack(int sz, int ndim, int *dims, int *start, int *end, 
	      unsigned char *inarray, unsigned char *outbuf)
{
  int i, total, firstdim, onechunk;
  int *dope, n1;
  int *id;
  unsigned char *in, *out, *in_first;

  if (!dims || !start || !end || !inarray || !outbuf)
    return -1;

  /* Fast return if data is 1-dimensional. */
  if (ndim==1)
  {
    memcpy(outbuf,&inarray[sz*start[0]], sz*(end[0]-start[0]+1));
    return 0;
  }

  /* find first partial dimension in slice, size of consecutive chunks,
     and total size of data to be copied. */
  firstdim = 0; 
  n1 = sz;
  total = sz;
  for (i=0; i<ndim; i++)
  {
    total *= end[i]-start[i]+1;
    if (i<ndim-1 && start[i]==0 && end[i]==dims[i]-1 && firstdim==i)
    {
      firstdim = i+1; 
      n1 *= end[i]-start[i]+1;
    }
  }

  /* Set dope vector. */
  dope = malloc(ndim*sizeof(int));
  dope[0] = sz;
  for (i=1;i<ndim; i++)
    dope[i] = dope[i-1]*dims[i-1];

  /* Fast return if all the data is consecutive. */
  onechunk = 0;
  if (firstdim == ndim-1)
    onechunk = 1;
  else
  {
    onechunk = 1;
    for (i=firstdim; i<ndim; i++)
    {
      if (start[i] != end[i])
	onechunk = 0;
    }
  }
  /* Set offset of first consecutive block. */
  in = in_first = inarray;
  id = malloc(ndim*sizeof(int));
  for (i=0; i<ndim; i++)
  {
    id[i] = start[i];
    if (i==firstdim)
      in_first = in;
    in += dope[i]*id[i];
  }

  if (onechunk)
  {
    //    printf("copying a single chunk.\n");
    memcpy(outbuf,in,total);
  }
  else
  {
    out = outbuf;
    while ((int) (out-outbuf) < total)
    {
      /* Copy consecutive elements. */
      /*      p = in;
	      for (i=0; i<n1; i++)
	      *out++ = *p++; */
    
      memcpy(out, in, n1);
      out += n1;
      
      /* Increment the index for the first partial dimension. */
      id[firstdim] += 1;
      if (id[firstdim] > end[firstdim])
      {
	/* We finished the loop over the first partial dimension.
	   Let the index increment trickle up. */
	in = in_first;
	for (i=firstdim; i<ndim-1; i++)
	{
	  if (id[i] > end[i])
	  {
	    id[i] = start[i];
	    id[i+1] += 1;
	  }
	  /* Compute new starting index. */
	  in += dope[i]*id[i];	
	}
	in += dope[ndim-1]*id[ndim-1];	
      }
      else
	in += dope[firstdim];
    }
  }
  free(dope);
  free(id);
  return 0;
}


/* Unpack data from a buffer into an n-dimensional array. */
int ndim_unpack(int sz, int ndim, int *dims, int *start, int *end, 
		unsigned char *inbuf, unsigned char *outarray)
{
  int i, total, firstdim, onechunk;
  int *dope, n1;
  int *id;
  unsigned char *in, *out, *out_first;

  if (!dims || !start || !end || !inbuf || !outarray)
    return -1;

  /* Fast return if data is 1-dimensional. */
  if (ndim==1)
  {
    memcpy(&outarray[sz*start[0]],inbuf, sz*(end[0]-start[0]+1));
    /*
      in = inbuf + sz*start[0];
      out = outarray;
      for (i=0; i<sz*(end[0]-start[0]+1); i++)
      *out++ = *in++;
      */
    return 0;
  }

  /* find first partial dimension in slice, size of consecutive chunks,
     and total size of data to be copied. */
  firstdim = 0; 
  n1 = sz;
  total = sz;
  for (i=0; i<ndim; i++)
  {
    total *= end[i]-start[i]+1;
    if (i<ndim-1 && start[i]==0 && end[i]==dims[i]-1 && firstdim==i)
    {
      firstdim = i+1; 
      n1 *= end[i]-start[i]+1;
    }
  }

  /* Set dope vector. */
  dope = malloc(ndim*sizeof(int));
  dope[0] = sz;
  for (i=1;i<ndim; i++)
    dope[i] = dope[i-1]*dims[i-1];

  /* Fast return if all the data is consecutive. */
  onechunk = 0;
  if (firstdim == ndim-1)
    onechunk = 1;
  else
  {
    onechunk = 1;
    for (i=firstdim; i<ndim; i++)
    {
      if (start[i] != end[i])
	onechunk = 0;
    }
  }

  
  /* Set offset of first consecutive block. */
  //  printf("firstdim = %d\n",firstdim);
  out = out_first = outarray;
  id = malloc(ndim*sizeof(int));
  for (i=0; i<ndim; i++)
  {
    id[i] = start[i];
    if (i==firstdim)
      out_first = out;
    out += dope[i]*id[i];
  }
  //  printf("out=%p, out_first=%p\n",out, out_first);

  if (onechunk)
  {
    //    printf("copying a single chunk. out=%p, outarray=%p\n",out, outarray);
    memcpy(out,inbuf,total);
  }
  else
  {
    in = inbuf;
    while ((int) (in-inbuf) < total)
    {
      /* Copy consecutive elements. */
      /*      p = out;
	      for (i=0; i<n1; i++)
	      *p++ = *in++;
	      */
      memcpy(out,in,n1);
      in += n1;
      
      /* Increment the index for the first partial dimension. */
      id[firstdim] += 1;
      if (id[firstdim] > end[firstdim])
      {
	/* We finished the loop over the first partial dimension.
	   Let the index increment trickle up. */
	out = out_first;
	for (i=firstdim; i<ndim-1; i++)
	{
	  if (id[i] > end[i])
	  {
	    id[i] = start[i];
	    id[i+1] += 1;
	  }
	  /* Compute new starting index. */
	  out += dope[i]*id[i];	
	}
	out += dope[ndim-1]*id[ndim-1];	
      }
      else
	out += dope[firstdim];
    }
  }
  free(dope);
  free(id);
  return 0;
}



/* Permute n-dimensional array. */
int ndim_permute(int sz, int ndim, int *dims, int *perm, 
		 unsigned char *in, unsigned char *out)
{
  int i, bytes, count, n, identity;
  int *dope1, *dope2;
  int *id, *permdims;
  unsigned char *p1, *p2;


  if (!dims || !perm || !in || !out || ndim<=0)
    return -1;

  /* Fast return if data is 1-dimensional. */
  if (ndim==1)
  {
    memcpy(out,in, sz*dims[0]);
    return 0;
  }

  permdims = malloc(ndim*sizeof(int));
  memset(permdims,0,ndim*sizeof(int));
  /* Count how many times each dimension occurs and check that
     the elements in perm are within 0 to ndim-1. */
  identity = 1;
  for (i=0; i<ndim; i++)
  {
    if (perm[i]<0 || perm[i]>=ndim)
    {
      fprintf(stderr,"ERROR in ndim_permute: perm is not a valid permutation "
	      "of 0 through ndim-1=%d\nbecause perm[%d]=%d.\n",ndim-1,i,perm[i]);
      return 1;
    }
    permdims[perm[i]] += 1;
    if (perm[i] != i)
      identity=0;
  }

  /* find first permuted dimension and total size of data to be copied. */
  //  firstdim = 0; 
  count = 1;
  for (i=0; i<ndim; i++)
  {
    count *= dims[i];
    //    if (permdims[i] == dims[i] && firstdim=i)
    //      firstdim = i+1; 
  }
  bytes = sz*count;


  /* Fast return if the permutation is the identity. */
  if (identity)
  {
    memcpy(out,in,bytes);
    return 0;
  }

  /* Check that each dimension occurs exactly once. */
  for (i=0; i<ndim; i++)
  {    
    if (permdims[i]!=1)
    {
      fprintf(stderr,"ERROR in ndim_permute: perm is not a valid permutation "
	      "because %d occurs %d times.\n",i,permdims[i]);
      return 1;
    }
  }
  /* Set up array of permuted dimensions. */
  for (i=0; i<ndim; i++)
  {
    permdims[i] = dims[perm[i]];
#ifdef DEBUG
    printf("permdims[%d] = %d\n",i,permdims[i]);
#endif
  }


  /* Set dope vector. */
  dope1 = malloc(ndim*sizeof(int));
  dope2 = malloc(ndim*sizeof(int));
  id = malloc(ndim*sizeof(int));
  dope1[0] = sz;
  id[0] = sz;
  dope2[perm[0]] = sz;
  for (i=1; i<ndim; i++)
  {
    dope1[i] = dope1[i-1]*dims[i-1];
    id[i] = id[i-1]*permdims[i-1];
    /* Permute dope2 so we van use the original coordinates to index it. */
    dope2[perm[i]] = id[i];
  }
  free(permdims);

  /* Clear offset array. */
  memset(id, 0, ndim*sizeof(int));  
  p1 = in;
  p2 = out;

#ifdef DEBUG
  printf("sz = %d\n",sz);
#endif

  for (n=0; n<count; n++)
  {
    memcpy(p2, p1, sz);
    id[0] += 1;
#ifdef DEBUG
    printf("id = (%d",id[0]);
    for (i=1;i<ndim;i++)
      printf(",%d",id[i]);
    printf(")\n");
#endif
    if (id[0] >= dims[0])    
    {
      /* We finished the loop over the first dimension.
	 Let the index increment trickle up. */
      p1 = in;
      p2 = out;
      for (i=0; i<ndim-1; i++)
      {
	if (id[i] >= dims[i])
	{
	  id[i] = 0;
	  id[i+1] += 1;
	}
	/* Advance pointers to new point. */
	p1 += dope1[i]*id[i];	
	p2 += dope2[i]*id[i];	
      }
      p1 += dope1[ndim-1]*id[ndim-1];	
      p2 += dope2[ndim-1]*id[ndim-1];	
    }
    else
    {
      p1 += dope1[0];
      p2 += dope2[0];
    }
  }
  free(dope1);
  free(dope2);
  free(id);
  return 0;
}



