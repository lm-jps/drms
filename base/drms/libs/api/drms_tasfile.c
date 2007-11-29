#include "drms.h"
#include "drms_tasfile.h"
#include "adler32.h"
#include "ndim.h"
#include "xmem.h"

/* When the blocksize is not specified in a call to tasfile_write, try
   to make the size of a block this many bytes: */
#define GOAL_SIZE (1<<20) 

#define TRY(code) if(!(code)) goto bailout;

/* 
Header:

field     |  size    | meaning
-----------------------------------
"DRMS TAS"|  8 bytes | Magic header
version   |  int32   | file format version
type      |  int32   | Data type given as enum value of DRMS_Type_t type.
compress  |  int32   | Type of compression used
indexstart|  int64   | Offset of first byte in index
heapstart |  int64   | Offset of first byte in heap
naxis     |  int32   | number of dimensions
axis0     |  int32   | length of first dimension
axis1     |  int32   | length of second dimension
...                    
axisn     |  int32   | length of n'th dimension
blksz0    |  int32   | Blocking size of first dimension
blksz1    |  int32   | Blocking size of second dimension
...                   
blkszn    |  int32   | Blocking size of n'th dimension
index data|          | array of per block data: 
                       (offset, size, Adler32 checksum)
                       (int64, int64, int32)
heap data |          | Heap of compressed data for each block.
-----------------------------------

*/

#ifdef DEBUG
					/*  Print header contents  */
static void print_tasheader (TAS_Header_t *h) {
  int i;
 
  printf("magic   = %.8s\n",h->magic);
  printf("version = %d\n",h->version);
  printf("type    = %d\n",h->type);
  printf("compress= %d\n",h->compress);
  printf("naxis   = %d\n",h->naxis);
  for (i=0; i<h->naxis; i++)
    printf ("axis[%3d] = %d, blksz[%d] = %d\n",i,h->axis[i], i,h->blksz[i]);
}
#endif

	/* Calculate number of blocks in i'th dimension given number of 
						elements and block size. */
static int calc_tasblockcount (TAS_Header_t *h, int *baxis) {
  int i, nblk = 1;
  for (i=0; i<h->naxis; i++) {
    baxis[i] = (h->axis[i]+h->blksz[i]-1)/h->blksz[i]; 
    nblk *= baxis[i]; /* total number of blocks. */
  }
  return nblk;
}
					/*  Read TAS header from a file  */
static int tasfile_readheader(FILE *fp, TAS_Header_t *h) {
  int i;

  memset (h, 0, sizeof(TAS_Header_t));
			/*  Read header and byteswap fields if necessary  */
  fread (h, sizeof(TAS_Header_t), 1, fp);
  if (strncmp (h->magic,"DRMS TAS",8)) {
    fprintf (stderr, "Bad magic string in TAS file: '%.8s'\n", h->magic);
    return 1;
  }
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, (char *)&h->version);
  byteswap (4, 1, (char *)&h->type);
  byteswap (4, 1, (char *)&h->compress);
  byteswap (4, 1, (char *)&h->naxis);
#endif
  if (h->naxis > DRMS_MAXRANK) {
    fprintf (stderr, "ERROR: Naxis (%d) in TAS file exceeds DRMS_MAXRANK "
	    "(%d)\n",  h->naxis, DRMS_MAXRANK);
    return 1;
  }
#if __BYTE_ORDER == __BIG_ENDIAN
  for (i=0; i < h->naxis; i++) {
    byteswap (4, 1, (char *)&h->axis[i]);
    byteswap (4, 1, (char *)&h->blksz[i]);
  }
#endif

  for (i=0; i<h->naxis; i++) {
    if (h->blksz[i] <= 0 || h->blksz[i] > h->axis[i] )
    {    
      fprintf (stderr,"ERROR: Bad block size in TAS file: blksz[%d]=%d, "
	      "axis[%d]=%d\n", i, h->blksz[i], i, h->axis[i]);
      return 1;
    }
    if (h->axis[i] <= 0) {    
      fprintf (stderr,"ERROR: Bad axis size in TAS file: axis[%d] = %d.\n",
	      i, h->axis[i]);
      return 1;
    }    
  }
  return 0;
}
/*
 *  Write TAS header to a file
 */
static int tasfile_writeheader (FILE *fp, TAS_Header_t *h) {
  TAS_Header_t hswap;

  if (h->naxis>DRMS_MAXRANK) {
    fprintf (stderr, "ERROR: Naxis (%d) exceeds DRMS_MAXRANK (%d)\n", 
	    h->naxis, DRMS_MAXRANK);
    return DRMS_ERROR_INVALIDRANK;
  }
				/*  Byteswap and write header to file  */
  memcpy (&hswap, h, sizeof (TAS_Header_t));
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, (char *)&hswap.version);
  byteswap (4, 1, (char *)&hswap.type);
  byteswap (4, 1, (char *)&hswap.compress);
  byteswap (4, 1, (char *)&hswap.naxis);
  byteswap (4, h->naxis, (char *)&hswap.axis);
  byteswap (4, h->naxis, (char *)&hswap.blksz);
#endif
  if (!fwrite (&hswap,sizeof(hswap), 1, fp)) {
    fprintf (stderr, "ERROR: Failed to write TAS header.\n");
    return 1;
  }
  return 0;
}


/* Write index array. */
static int tasfile_writeindex(FILE *fp, int nblk, TAS_Index_t *index) {
  int status=0;
  TAS_Index_t *iswap;
  /* Seek to beginning of index. */
  if (fseek(fp, sizeof(TAS_Header_t), SEEK_SET)) {
    fprintf (stderr, "ERROR: Failed to seek to index in TAS file.\n");
    return 1;
  }
#if __BYTE_ORDER == __BIG_ENDIAN
  {
  int i;
  XASSERT(iswap =  malloc(nblk*sizeof(TAS_Index_t)));  
  memcpy(iswap, index, nblk*sizeof(TAS_Index_t));
  for (i=0; i<nblk; i++) {
      byteswap(8, 1, (char *)&iswap[i].offset);
      byteswap(8, 1, (char *)&iswap[i].length);
      byteswap(4, 1, (char *)&iswap[i].adler32);
  }
  }
#else
  iswap = index;
#endif
  /* Write index to file. */
  if (!fwrite (iswap, nblk * sizeof (TAS_Index_t), 1, fp)) {
    fprintf (stderr, "ERROR: Failed to write index to TAS file.\n");
   status = 1;
  }
#if __BYTE_ORDER == __BIG_ENDIAN
  free(iswap);
#endif
  return status;
}

/* Write index array. */
static  TAS_Index_t *tasfile_readindex (FILE *fp, int nblk, TAS_Header_t *h) {
  TAS_Index_t *index;
				/*  Seek to beginning of index in file  */
  if (fseek (fp, sizeof(TAS_Header_t), SEEK_SET)) {
    fprintf (stderr, "ERROR: Failed to seek to index in TAS file.\n");
    return NULL;
  }
					/*  Read index array from file  */
  XASSERT (index = malloc (nblk * sizeof (TAS_Index_t)));
  if (!fread (index, nblk * sizeof (TAS_Index_t), 1, fp)) {
    fprintf (stderr, "ERROR: Failed to read index from TAS file.\n");
    free (index);
    return NULL;
  }
#if __BYTE_ORDER == __BIG_ENDIAN
{
  int i;
  for (i=0; i<nblk; i++) {
    byteswap (8, 1, (char *)&index[i].offset);
    byteswap (8, 1, (char *)&index[i].length);
    byteswap (4, 1, (char *)&index[i].adler32);
  }
}
#endif
#ifdef DEBUG
{
  int idx;
  for (idx=0; idx<nblk; idx++) {
    printf("index[%d].offset = %llu\n"
	   "index[%d].length = %llu\n"
	   "index[%d].adler32 = %x\n",
	   idx, index[idx].offset, idx, index[idx].length,
	   idx, index[idx].adler32);
  }
}
#endif
  return index;
}

/* Check that the hyper-slab designated by start and end is non-empty. */
static int tasfile_slice_empty(int naxis, int *start, int *end) {
  int i;
  for (i=0; i<naxis; i++) {
    if (start[i]>end[i]) {
      fprintf (stderr, "ERROR: Empty slice: start[%d] = %d > end[%d] = %d\n", 
	      i,start[i],i,end[i]);
      return i+1;
    }
  }
  return 0;
}


/* Check that the hyper-slab designated by "start" and "end" is contained
   in the hypercube spanned by the origin and the point in "axis". */
static int tasfile_slice_notcontained (int naxis, int *start, int *end,
    int *axis) {
  int i;
  for (i=0; i<naxis; i++) {
    if (start[i]<0 || start[i]>=axis[i]) {
      fprintf (stderr, "ERROR: Requested slice exceeds array bounds: "
	      "start[%d] = %d, axis[%d] = %d\n", i,start[i],i,axis[i]);
      return i+1;
    }
    if (end[i]<0 || end[i]>=axis[i]) {
      fprintf (stderr, "ERROR: Requested slice exceeds array bounds: "
	      "end[%d] = %d, axis[%d] = %d\n", i,end[i],i,axis[i]);
      return i+1;
    }
  }
  return 0;
}



/*****************************************************************************/

/* Read an entire TAS file into memory. */
int drms_tasfile_read(char *filename, DRMS_Type_t type, 
		      double bzero, double bscale, DRMS_Array_t *rf)
{
  int stat, i, j, insz, outsz, maxzblk;
  int nblk, blksz, count, chksum;
  unsigned char *zbuf, *bbuf;
  long long n, lastoffset;
  int baxis[DRMS_MAXRANK]={0}, bidx[DRMS_MAXRANK]={0};
  int start[DRMS_MAXRANK]={0}, end[DRMS_MAXRANK]={0};
  TAS_Header_t h;
  TAS_Index_t *index, *ip;
  FILE *fp;  
  int convert=1;

  if ((fp = fopen(filename,"r")))
  {
    /* Read file header. */
    if (tasfile_readheader(fp, &h))
      goto bailout0;

    /* Calculate size of data, block sizes. */
    n = 1;
    blksz = 1;
    for (i=0; i<h.naxis; i++)
    {
      n *= h.axis[i];     /* total number of elements. */
      blksz *= h.blksz[i]; /* Size of a full block. */
    }
    /* calculate number of blocks per dimension and in total. */
    nblk = calc_tasblockcount(&h,baxis);
    /* Read index array from file. */
    if ((index = tasfile_readindex(fp, nblk, &h)) == NULL)
    {
      fprintf (stderr, "ERROR: Failed to read index from TAS file %s\n",
	      filename);
      goto bailout1;
    }

#ifdef DEBUG
    print_tasheader(&h);
    printf("n = %lld, blksz = %d\n",n,blksz);
    printf("nblk = %d\n",nblk);
    for (i=0; i<h.naxis; i++)
      printf("baxis[%d] = %d\n",i,baxis[i]);
#endif

    /* Prepare output array data structure. */
    if (type == DRMS_TYPE_RAW || (h.type == type && bscale==1.0 && bzero==0.0))
      convert = 0;
    if (convert)
      rf->type = type;
    else
      rf->type = h.type;
    outsz = drms_sizeof(rf->type);
    insz = drms_sizeof(h.type);
    rf->naxis = h.naxis;
    for (i=0; i<h.naxis; i++)
      rf->axis[i] = h.axis[i];
    XASSERT(rf->data = malloc(n*outsz));
    
    /* Find largest buffer size required. */
    maxzblk = 0;    
    for (i=0, ip = index; i<nblk; i++, ip++)
    {
      if (ip->length > maxzblk)
	maxzblk = ip->length;
    }
    /* Allocate temporary buffers. */
    if (maxzblk > blksz*outsz)
      XASSERT(zbuf = malloc(maxzblk));
    else
      XASSERT(zbuf = malloc(blksz*outsz));
    XASSERT(bbuf = malloc(blksz*insz));

    /* Main loop: Read one block at a time and unpack it into the
       main array. */
    lastoffset = -1;
    for (j=0, ip=index; j < nblk; j++, ip++)
    {
      /* Get the location and size of the next block to be unpacked. */
      count = 1;
      for (i=0; i<h.naxis; i++)
      {
	start[i] = bidx[i]*h.blksz[i];
      	end[i] = start[i] + h.blksz[i] - 1;
	if (end[i] >= h.axis[i])
	  end[i] = h.axis[i]-1;
	count *= (end[i]-start[i]+1);
#ifdef DEBUG
	printf("start[%3d] = %d, end[%3d] = %d, count = %d\n",
	       i,start[i],i,end[i],count);
#endif
      }
      
#ifdef DEBUG
      printf("index[%d] = (%lld, %lld, 0x%08x)\n",
	     j,ip->offset,ip->length,ip->adler32);
#endif
      /* Read the raw block data from disk. */
      if (ip->length > 0) /* Sparse empty block. */
      {
	/* Save the fseek call if we are reading sequentially. */
	if (lastoffset != ip->offset)
	{
#ifdef DEBUG	  
	  printf("Doing fseek to %lu. (lastoffset = %lu)\n",
		 ip->offset, lastoffset);
#endif
	  if (fseek(fp, ip->offset, SEEK_SET))
	  {
	    fprintf (stderr, "ERROR: Failed to seek to index in TAS file.\n");
	    goto bailout2;
	  }
	}
	/* Read block data from file. */
	if (!fread(zbuf, ip->length, 1, fp))
	{
	  free(index);
	  goto bailout2;
	}
	lastoffset = ip->offset+ip->length;
	/* Verify block checksum. */
	if ((chksum = adler32sum(1, ip->length, zbuf)) != ip->adler32)
	{
	  fprintf (stderr, "ERROR: Checksum error in data block %d in file %s\n",
		  j,filename);
	  goto bailout2;
	}
	/* Uncompress. */
	stat = drms_uncompress(h.compress, ip->length, zbuf, h.type, count, bbuf);
      }
      else
      {
	/* Empty (sparse) block. Fill it with MISSING. */
	DRMS_Array_t arr;
	memset(&arr,0,sizeof(DRMS_Array_t));
	arr.type = h.type;
	arr.naxis = 1;
	arr.axis[0] = count;
	arr.data = bbuf;
	drms_array2missing(&arr);
      }
      /* Do type conversion and scaling if requested. */
      if (convert)
      {
	drms_array_rawconvert(count, rf->type, bzero, bscale, zbuf, h.type, 
			      bbuf); 
      }
      else
      {
	unsigned char *tmp = zbuf;
	zbuf = bbuf;
	bbuf = tmp;
      }	
      /* Copy uncompressed data to main array. */
      ndim_unpack(drms_sizeof(rf->type), h.naxis, h.axis, start, end, 
		  zbuf, rf->data);
      /* Increment block index counter. */
      bidx[0] += 1;
      for (i=0; i<h.naxis-1; i++)
      {
	if (bidx[i] >= baxis[i])
	{
	  bidx[i] = 0;
	  bidx[i+1] += 1;
	}
      }
    }
    free(index);
    free(zbuf);
    free(bbuf);
    fclose(fp);
    return 0; /* Success. */
  }
  else
  {
    fprintf (stderr, "ERROR: Couldn't open file '%s'\n",filename);
    return 1;
  }
 bailout2:
  free(zbuf);
  free(bbuf);
 bailout1:
  free(index);
  free(rf->data);
 bailout0:
  fclose(fp);
  fprintf (stderr, "ERROR: drms_tasfile_read failed '%s'\n",filename);
  return 1;
}



/* Read an arbitrary slice of a TAS file into memory. */
int drms_tasfile_readslice(FILE *fp, DRMS_Type_t type, double bzero, 
			   double bscale,  int naxis, int *start, int *end, 
			   DRMS_Array_t *rf)
{
  int stat, i, j, insz, outsz, maxzblk, count, chksum, contained, skip;
  unsigned char *zbuf, *bbuf;
  long long n;
  int baxis[DRMS_MAXRANK] = {0},  nblk, blksz;
  int bidx[DRMS_MAXRANK]={0};
  int bstart[DRMS_MAXRANK]={0}, bend[DRMS_MAXRANK]={0};
  int sstart[DRMS_MAXRANK]={0}, send[DRMS_MAXRANK]={0};
  int bsize[DRMS_MAXRANK]={0}, axis[DRMS_MAXRANK]={0};
  TAS_Header_t h;
  TAS_Index_t *index, *ip;
  int convert = 1;

  /* Open existing file for reading and writing. */
  if (!fp)
    return DRMS_ERROR_IOERROR;

  /* Read file header. */
  rewind(fp);
  if (tasfile_readheader(fp, &h))
    goto bailout0;

  /* Sanity check the dimensions of the requested slice compared to
     the size of the array in the file. */
  if (naxis!=h.naxis)
  {
    fprintf (stderr, "ERROR: Number of dimensions (%d) in TAS file does not "
	    "matvhes number of dimensions in requested slice (%d)\n", 
	    h.naxis, naxis);
    goto bailout0;
  }

  /* Check that the specified slice is valid. */
  if (tasfile_slice_empty(naxis, start, end) ||
      tasfile_slice_notcontained(naxis, start, end, h.axis))
    goto bailout0;

  /* Calculate size of data, block sizes. */
  n = 1;
  blksz = 1;
  for (i=0; i<h.naxis; i++)
  {
    axis[i] = end[i]-start[i]+1;
    n *= axis[i];     /* total number of elements. */
    blksz *= h.blksz[i]; /* Size of a full block. */
  }

  /* calculate number of blocks per dimension and in total. */
  nblk = calc_tasblockcount(&h,baxis);

  /* Read index array from file. */
  if ((index = tasfile_readindex(fp, nblk, &h)) == NULL)
  {
    fprintf (stderr, "ERROR: Failed to read index from TAS file/\n");
    goto bailout0;
  }


#ifdef DEBUG
  print_tasheader(&h);
  for (i=0; i<naxis; i++)
    printf("start[%d] = %d, end[%d] = %d, axis[%d] = %d, baxis[%d] = %d\n",
	   i,start[i],i,end[i],i,h.axis[i],i,baxis[i]);
  printf("n = %lld, blksz = %d\n",n,blksz);
  printf("nblk = %d\n",nblk);
#endif


  /* Prepare output array data structure. */
  if (type == DRMS_TYPE_RAW || (h.type==type && bzero==0.0 && bscale==1.0))
    convert = 0;
  if (convert)
    rf->type = type;
  else
    rf->type = h.type;
  outsz = drms_sizeof(rf->type);
  insz = drms_sizeof(h.type);
  rf->naxis = h.naxis;
  for (i=0; i<h.naxis; i++)
    rf->axis[i] = axis[i];
  XASSERT(rf->data = malloc(n*outsz));
  drms_array2missing(rf);
    

  /* Find largest buffer size required. */
  maxzblk = 0;    
  for (i=0, ip = index; i<nblk; i++, ip++)
  {
    if (ip->length > maxzblk)
      maxzblk = ip->length;
  }
  /* Allocate temporary buffers. */
  if (maxzblk > blksz*outsz)
    XASSERT(zbuf = malloc(maxzblk));
  else
    XASSERT(zbuf = malloc(blksz*outsz));
  XASSERT(bbuf = malloc(blksz*insz));

  /* Main loop: Read one block at a time and unpack it into the
     main array. */
  for (j=0, ip=index; j < nblk; j++, ip++)
  {
    /* Get the location and size of the next block to be unpacked. */
    count = 1;
    skip = 0;
    for (i=0; i<h.naxis; i++)
    {
      bstart[i] = bidx[i]*h.blksz[i];
      bend[i] = bstart[i] + h.blksz[i] - 1;	
      if (bend[i] >= h.axis[i])
	bend[i] = h.axis[i]-1;
      /* If this block is not in the slice then continue. */
      if (bstart[i]>end[i] || bend[i]<start[i])
      {
	skip = 1;
	continue;
      }
      /* Block size in memory. */
      bsize[i] = bend[i]-bstart[i]+1;
      /* Accumulate block element count. */
      count *= bsize[i];
    }

    if (!skip && ip->length > 0) /* Use this block if it overlaps the requested slice. */
    {
      /* Seek to the beginning of the block. */
      if (fseek(fp, ip->offset, SEEK_SET))
      {
	fprintf (stderr, "ERROR: Seek to data block %d in TAS file failed\n",j);
	goto bailout2;
      }
	
      /* Read the raw block data from disk. */
#ifdef DEBUG	
      printf("Using block!\nelement count = %d\n",count);
      printf("index[%d] = (%lld, %lld, 0x%08x)\n",
	     j,ip->offset,ip->length,ip->adler32);
#endif
      if (!fread(zbuf, ip->length, 1, fp))
      {
	goto bailout2;
      }
	
      if ((chksum = adler32sum(1, ip->length, zbuf)) != ip->adler32)
      {
	fprintf (stderr, "ERROR: Checksum error in data block %d in TAS file.\n",j);
	goto bailout2;
      }
	
      /* Uncompress. */
      stat = drms_uncompress(h.compress, ip->length, zbuf, h.type, count, bbuf);
      if (convert)
	drms_array_rawconvert(count, rf->type, bzero, bscale, zbuf, h.type, 
			      bbuf); 
      else
      {
	unsigned char *tmp = zbuf;
	zbuf = bbuf;
	bbuf = tmp;
      }
	
      /* Check if the block just read is contained in the requested slot.
	 If not a slice must be chopped off before copying it to the 
	 output array. */	 
      contained = 1;
      for (i=0; i<h.naxis; i++)
      {
	if ( bstart[i] < start[i] || bend[i] > end[i] )
	{
	  contained = 0;
	  break;
	}
      }

      if (contained)
      {
	for (i=0; i<h.naxis; i++)
	{
	  sstart[i] = bstart[i] - start[i];
	  send[i] = bend[i] - start[i];
	}
	/* Copy uncompressed data to main array. */
	ndim_unpack(drms_sizeof(rf->type), h.naxis, rf->axis, sstart, send, 
		    zbuf, rf->data);
      }
      else
      {
	/* extraxt useful part of block. */
	for (i=0; i<h.naxis; i++)
	{
	  sstart[i] = bstart[i]<start[i] ? start[i]-bstart[i] : 0;
	  send[i] =  bend[i]>end[i] ? end[i]-bstart[i] : bend[i]-bstart[i];
	}
	ndim_pack(drms_sizeof(rf->type), h.naxis, bsize, sstart, send, 
		  zbuf, bbuf);
	/* Copy uncompressed data to main array. */
	for (i=0; i<h.naxis; i++)
	{
	  sstart[i] = bstart[i]<start[i] ? 0 : bstart[i]-start[i];
	  send[i] =  bend[i]>end[i] ? end[i]-start[i] : bend[i]-start[i];
	}
	ndim_unpack(drms_sizeof(rf->type), h.naxis, rf->axis, sstart, send, 
		    bbuf, rf->data);
      }
    }
    /* Increment block index counter. */
    bidx[0] += 1;
    for (i=0; i<h.naxis-1; i++)
    {
      if (bidx[i] >= baxis[i])
      {
	bidx[i] = 0;
	bidx[i+1] += 1;
      }
    }
  }
  free(index);
  free(zbuf);
  free(bbuf);
  return 0; /* Success. */
 bailout2:
  free(zbuf);
  free(bbuf);
  free(index);
  free(rf->data);
 bailout0:
  fprintf (stderr, "ERROR: drms_tasfile_readslice failed\n");
  return 1;
}



/* Write an array into a slice of a TAS file. */
int drms_tasfile_writeslice(FILE *fp, double bzero, double bscale, 
			    int *start, DRMS_Array_t *array)
{
  int i, j, zsize, status = DRMS_NO_ERROR, insz, outsz, slblk, n_blk, n, count;
  int end[DRMS_MAXRANK]={0}, boffset[DRMS_MAXRANK]={0};
  int sbaxis[DRMS_MAXRANK] = {0}, baxis[DRMS_MAXRANK] = {0}, nblk;
  int bidx[DRMS_MAXRANK]={0}, bdope[DRMS_MAXRANK]={0}, idx;
  int sstart[DRMS_MAXRANK]={0}, send[DRMS_MAXRANK]={0};
  unsigned char *zbuf, *bbuf;
  int *blocksize;
  TAS_Header_t h;
  long fend, lastoffset;
  struct stat file_stat;
  TAS_Index_t *index=NULL;
  uint64_t length;
  int *idx_list; /* List of indices of blocks written. */

  
  /* Open existing file for reading and writing. */
  if (!fp)
    return DRMS_ERROR_IOERROR;

  if (fstat(fileno(fp), &file_stat)) 
  {
    fprintf (stderr, "ERROR: Can't stat TAS file.\n");
    goto bailout0;
   
  }

  /* Determine if this is large enough to contain a valid TAS header
     and read it if that is the case. */
  if (file_stat.st_size <= sizeof(TAS_Header_t) || tasfile_readheader(fp, &h))
  {
    fprintf (stderr, "Trying to write slice to invalid TAS file.\n");
    status = DRMS_ERROR_IOERROR; 
    goto bailout0;
  }

  /* Sanity check input dimensions. */
  if (h.naxis != array->naxis)
  {
    fprintf (stderr, "ERROR: Number of dimensions (%d) in the TAS file "
	    "does not match that (%d) of the slice being written to it\n", 
	    h.naxis, array->naxis);
    status = DRMS_ERROR_IOERROR;
    goto bailout0;
  }
  blocksize = h.blksz;

  /* Set up "end" array for slice and check that the slice is 
     consistent with the array in the file, i.e.:
     1. The slice does not exceed the indices of the array in the file.
     2. The slice corresponds to a set of whole tiles in the file. 
  */
  for (i=0; i<h.naxis; i++)
    end[i] = start[i] + array->axis[i] - 1;
  if (tasfile_slice_notcontained(h.naxis, start, end, h.axis))
    goto bailout0;
  /* Check if the slice corresponds to a se of whole tiles. */
  for (i=0; i<h.naxis; i++)
  {
    if ( (start[i] % h.blksz[i]) || 
	 (((end[i]+1) % h.blksz[i]) && ((end[i]+1)<h.axis[i])))
    {
      fprintf (stderr, "ERROR: Slice does not correspond to a set of whole "
	      "tiles: start[%d] = %d, end[%d]=%d, blksz[%d] = %d, "
	      "axis[%d] = %d\n", 
	      i, start[i], i, end[i], i, h.blksz[i], i, h.axis[i]);
      goto bailout0;
    }
  }
  
  /* calculate number of blocks per dimension and in total. */
  nblk = calc_tasblockcount(&h,baxis);
  /* Read index array from file. */
  if ((index = tasfile_readindex(fp, nblk, &h)) == NULL)
  {
    fprintf (stderr, "ERROR: Failed to read index from TAS file\n");
    goto bailout1;
  }
  

  /* Calculate size of data, block sizes. */
  insz = drms_sizeof(array->type); /* Number of bytes per input element. */
  outsz = drms_sizeof(h.type);     /* Number of bytes per output element. */
  n_blk = 1;                       /* Number of elements in a block. */
  n = 1;                           /* Number of elements in file. */
  slblk = 1;                       /* Number of blocks in slice. */
  for (i=0; i<h.naxis; i++)
  {
    n *= h.axis[i];    
    n_blk *= h.blksz[i];
    sbaxis[i] = (array->axis[i] + h.blksz[i]-1)  / h.blksz[i]; /* # blocks in slice in the i'th dim. */
    slblk *= sbaxis[i];
#ifdef DEBUG
    printf("sbaxis[%d] = %d\n",i,sbaxis[i]);
#endif  
  }
#ifdef DEBUG
  printf("slblk = %d\n",slblk);
#endif  

  /* Allocate temporary buffers. */
  XASSERT(idx_list = malloc(slblk*sizeof(int)));
  zsize = (n_blk*outsz*11)/10 + 1200; /* Allocate enough space for GZIP
					 compress + 100 bytes. */
  if (n_blk*insz>zsize)
  {
    XASSERT(zbuf = malloc(n_blk*insz));
    XASSERT(bbuf = malloc(n_blk*insz));
  }
  else
  {
    XASSERT(zbuf = malloc(zsize));
    XASSERT(bbuf = malloc(zsize));
  }

  /* Compute offset multipliers for file. */
  bdope[0] = 1;
  for (i=1; i<h.naxis; i++)
    bdope[i] = bdope[i-1]*baxis[i-1];
 
  /* bidx is the block index in the file, boffset is the starting
     block index. */
  for (i=0; i<h.naxis; i++)
    bidx[i] = boffset[i] = start[i] / h.blksz[i];

  /* Seek to the end of the file in preparation for writing out the 
     compressed data blocks. */
  if (fseek(fp, 1, SEEK_END))
  {
    fprintf (stderr, "ERROR: Failed to seek to end of TAS file\n");
    goto bailout1;
  }
  lastoffset = fend = ftell(fp);
  for (j=0; j<slblk; j++)
  {
    idx = 0;
    count = 1;
#ifdef DEBUG
    printf("Writing out block [");
#endif
    for (i=0; i<h.naxis; i++)
    {
      sstart[i] = (bidx[i] - boffset[i])*h.blksz[i];
      send[i] =  sstart[i] + h.blksz[i] - 1;
      if (send[i] >= array->axis[i])
	send[i] = array->axis[i]-1;
      idx += bdope[i]*bidx[i];
      count *= (send[i]-sstart[i]+1);
#ifdef DEBUG
      printf("%d-%d",sstart[i],send[i]);     
      if (i<h.naxis-1)
	fputc(',',stdout);
#endif
    } 
    idx_list[j] = idx;
#ifdef DEBUG
    printf("] from slice\n");
#endif
    /* Copy a block of data from main array to buffer. */
    ndim_pack(insz, h.naxis, array->axis, sstart, send, 
	      array->data, bbuf);
    
    /* Convert and scale to desired output type. */
    if (h.type!=array->type || bzero!=0.0 || bscale!=1.0)
    {
#ifdef DEBUG
      printf("Converting\n");
#endif
      drms_array_rawconvert(count, h.type, bzero, bscale, zbuf, array->type, 
			    bbuf); 
    }
    else
    {
      unsigned char *tmp = zbuf;
      zbuf = bbuf;
      bbuf = tmp;
    }	

    /* Compress the data. */
    length = drms_compress(h.compress, h.type, count, zbuf, zsize, bbuf);

    /* If the compressed length is larger than that of the old compressed 
       data then append the new compressed data to the end of the file. 
       This creates a "junk block" in the file where the old data was stored. 
       The junk block can only be gotten rid of by defragmenting the file. */
    if (length > index[idx].length)
    {
      /* Find index entry. */
      index[idx].offset = fend;
      /* Compress buffer contents. */
      index[idx].length = length;
      fend += length;
    }
    
    /* Compute checksum for compressed data. */
    index[idx].adler32 = adler32sum(1, index[idx].length, bbuf);

#ifdef DEBUG    
    printf("index[%d].offset = %llu\n"
	   "index[%d].length = %llu\n"
	   "index[%d].adler32 = %d\n",
	   idx, index[idx].offset,idx,index[idx].length,
	   idx, index[idx].adler32);
#endif
    
    /* Seek to new location if the file pointer is not already 
       in the right place. */
    if (lastoffset != index[idx].offset)
    {
#ifdef DEBUG
      printf("fseeking to offset %lu\n",index[idx].offset);
#endif
      if (fseek(fp,index[idx].offset,SEEK_SET))
      {
	fprintf (stderr, "ERROR: fseek failed on TAS file.\n");
	status = DRMS_ERROR_IOERROR;
	goto bailout1;
      }
    }
      
    /* Write compressed data to file. */
    if (!fwrite(bbuf,index[idx].length, 1, fp))
    {
      fprintf (stderr, "ERROR: Failed to write data block %d to "
	      "TAS file.\n",j);
      status = DRMS_ERROR_IOERROR;
      goto bailout1;
    }
    lastoffset = index[idx].offset + index[idx].length;

    /* Advance block index. */
    bidx[0] += 1;
    for (i=0; i<h.naxis-1; i++)
    {
      if (bidx[i] >= baxis[i])
      {
	bidx[i] = 0;
	bidx[i+1] += 1;
      }
    }
    
  }


  /* Write the entries in the index that were actually changed. */
#if __BYTE_ORDER == __BIG_ENDIAN
  for (i=0; i<slblk; i++) {
    byteswap (8, 1, (char *)&index[idx_list[i]].offset);
    byteswap (8, 1, (char *)&index[idx_list[i]].length);
    byteswap (4, 1, (char *)&index[idx_list[i]].adler32);
  }
#endif
  j = 0;
  while (j < slblk ) {
				/*  Find next consecutive run if indices  */
    i = j + 1;
    while (i < slblk && idx_list[i-1] == idx_list[i] - 1) ++i;
#ifdef DEBUG
    printf ("Writing indices %d-%d.\n",idx_list[j],idx_list[i-1]);
#endif
    if (fseek (fp, sizeof (TAS_Header_t) + idx_list[j] * sizeof (TAS_Index_t), 
	      SEEK_SET)) {
      fprintf (stderr, "ERROR: Failed to seek to index in TAS file.\n");
      goto bailout1;
    }
    if (!fwrite (&index[idx_list[j]], (i-j)*sizeof (TAS_Index_t), 1, fp)) {
      fprintf (stderr, "ERROR: Failed to write index to TAS file.\n");
      goto bailout1;
    }  
    j = i;
  }
  free (idx_list);
  free (zbuf);
  free (bbuf);
  return 0;

 bailout1:
  if (index)
    free(index);
 bailout0:
  return status;
}





/* Write an entire TAS file to disk. */
int drms_tasfile_write(char *filename, DRMS_Type_t type, double bzero, 
		       double bscale, DRMS_Compress_t compression, 
		       int *blocksize, DRMS_Array_t *array)
{
  int i, j, insz, outsz, count, zsize, status;
  FILE *fp;
  unsigned char *zbuf, *bbuf;
  long long n, nblk, blk_bytes;
  int baxis[DRMS_MAXRANK] = {0}, bidx[DRMS_MAXRANK] = {0};
  int start[DRMS_MAXRANK] = {0}, end[DRMS_MAXRANK]={0};
  TAS_Header_t h={"DRMS TAS", TASFILE_VERSION};
  TAS_Index_t *index, *ip;


  /* Create new empty TAS file. */
  if (type == DRMS_TYPE_RAW)
    type = array->type;  
  if ((status = drms_tasfile_create(filename, compression, type, array->naxis, 
				    array->axis, blocksize, &h)))
  {
    fprintf (stderr, "ERROR: Failed to create new TAS file '%s'.\n",filename);
    return status;
  }

  /* (re-) Open the new file. */
  if ((fp = fopen(filename,"r+"))==NULL)
  {
    fprintf (stderr, "ERROR: Failed to open newly created TAS file '%s'.\n",
	    filename);
    return DRMS_ERROR_IOERROR;
  }

  /* Calculate number of elements. */
  outsz = drms_sizeof(h.type);
  insz = drms_sizeof(array->type);

  /* Calculate number of blocks per dimension and in total. */
  nblk = calc_tasblockcount(&h,baxis);


  /* Calculate size of data, block sizes. */
  blk_bytes = 1;
  n = 1;
  for (i=0; i<h.naxis; i++)
  {
    n *= h.axis[i];     /* total number of elements. */
    blk_bytes *= h.blksz[i]; /* Size of a full block. */
  }
  blk_bytes *= insz;
#ifdef DEBUG
  printf("nblk = %d\n",nblk);
  printf("n = %lld, blk_bytes = %d\n",n,blk_bytes);
  for (i=0; i<h.naxis; i++)
    printf("baxis[%d] = %d\n",i,baxis[i]);
#endif
  
  /* Skip over header and index part of file, which will be written last. */
  if (fseek(fp, sizeof(TAS_Header_t) + nblk*sizeof(TAS_Index_t), SEEK_SET))
  {
    fprintf (stderr, "ERROR: Failed to seek past index while writing TAS "
	    "file %s.\n", filename);
    goto bailout0;
  }

   
  /* Allocate temporary buffers. */
  XASSERT(index = malloc(nblk*sizeof(TAS_Index_t)));
  zsize = (n*outsz*11)/10 + 1200; /* Make sure there is enough space for GZIP
				     compress + 100 bytes. */
  if (blk_bytes>zsize)
  {
    XASSERT(zbuf = malloc(blk_bytes));
    XASSERT(bbuf = malloc(blk_bytes));
  }
  else
  {
    XASSERT(zbuf = malloc(zsize));
    XASSERT(bbuf = malloc(zsize));
  }
    
  ip = index;
  /* Calculate offset of the first block in file. */
  ip->offset = sizeof(h) + nblk*sizeof(TAS_Index_t);


  /***** Main loop: Compress and write the data one block at a time. *****/
  for (j=0; j < nblk; j++, ip++)
  {
    /* Get the location and size of the next block to be unpacked. */
    count = 1;
    for (i=0; i<h.naxis; i++)
    {
      start[i] = bidx[i]*h.blksz[i];
      end[i] = start[i] + h.blksz[i] - 1;
      if (end[i] >= h.axis[i])
	end[i] = h.axis[i]-1;
      count *= (end[i]-start[i]+1);
#ifdef DEBUG
      printf("start[%3d] = %d, end[%3d] = %d, count = %d\n",
	     i,start[i],i,end[i],count);
#endif
    }

    /* Copy a block of data from main array to buffer. */
    ndim_pack(insz, h.naxis, h.axis, start, end, array->data, bbuf);

    /* Convert and scale to desired output type. */
    if (h.type!=array->type || bzero!=0.0 || bscale!=1.0)
    {
#ifdef DEBUG
      printf("Converting\n");
#endif
      drms_array_rawconvert(count, h.type, bzero, bscale, zbuf, array->type, 
			    bbuf); 
    }
    else
    {
      unsigned char *tmp = zbuf;
      zbuf = bbuf;
      bbuf = tmp;
    }	

    /* Compress buffer contents. */
    ip->length = drms_compress(h.compress, h.type, count, zbuf, zsize, bbuf);

    /* Compute checksum for compressed data. */
    ip->adler32 = adler32sum(1,ip->length,bbuf);

    /* Write compressed data to file. */
    if (!fwrite(bbuf,ip->length, 1, fp))
    {
      fprintf (stderr, "ERROR: Failed to write data block %d to "
	      "TAS file %s.\n",j,filename);
      goto bailout1;
    }
    /* Calculate offset of the next block in file. */
    if (j<nblk-1)
      (ip+1)->offset = ip->offset + ip->length;

    /* Increment block index counter. */
    bidx[0] += 1;
    for (i=0; i<h.naxis-1; i++)
    {
      if (bidx[i] >= baxis[i])
      {
	bidx[i] = 0;
	bidx[i+1] += 1;
      }
    }
  }

  /* Write index to file. */
  if (tasfile_writeindex(fp, nblk, index))
  {
    fprintf (stderr, "ERROR: Failed to seek to end of TAS file %s\n",
	    filename);
    goto bailout1;
  }
  fclose(fp);

  free(index);
  free(zbuf);
  free(bbuf);
  return 0; /* Success. */


 bailout1:
  free(zbuf);
  free(bbuf);
  free(index);
 bailout0:
  fprintf (stderr, "ERROR:drms_ tasfile_write failed '%s'\n",filename);
  fclose(fp);
  return 1;
}




/* Create an empty TAS file. */
int drms_tasfile_create (char *filename, DRMS_Compress_t compression, 
    DRMS_Type_t type, int naxis, int *axis, int *blocksize,
    TAS_Header_t *header) {
  FILE *fp;
  int i, n, sz, nblk;
  int tmp[DRMS_MAXRANK] = {0}, baxis[DRMS_MAXRANK]={0};
  TAS_Header_t h={"DRMS TAS", TASFILE_VERSION};
  float B;

  if (naxis > DRMS_MAXRANK) {
    fprintf (stderr, "ERROR: Naxis (%d) exceeds DRMS_MAXRANK (%d)\n", 
	    naxis, DRMS_MAXRANK);
    return DRMS_ERROR_INVALIDRANK;
  }    

  if ((fp = fopen (filename, "w"))) {
    h.type = type;
    /*  Calculate number of elements. */
    sz = drms_sizeof(type);
    n = 1;
    for (i=0; i<naxis; i++)
      n *= axis[i];

    if (blocksize == NULL) {
      blocksize = tmp;
      if (compression == DRMS_COMP_NONE) { 
	/* Pick a blocksize. Try to divide all dimension in the same number 
	   of times a such that the size of a single block is GOAL_SIZE.  */
	B = powf(((float)n*sz)/GOAL_SIZE, 1.0f/naxis);
	
	for (i=0; i<naxis; i++) {
	  if ( B <= 1 )
	    blocksize[i] = axis[i];
	  else if ( B >= axis[i])
	    blocksize[i] = 1;
	  else
	    blocksize[i] = ceilf(axis[i]/B);
#ifdef DEBUG
	  printf("axis[%d] = %d, blocksize[%d] = %d\n",
		 i,axis[i],i,blocksize[i]);
#endif
	}
      }
      else {
	n = 1;
	for (i=0; i<naxis; i++) {
	  n *= axis[i];
	  if (n*sz>=GOAL_SIZE)
	    blocksize[i] = 1;
	  else
	    blocksize[i] = axis[i];
#ifdef DEBUG
	  printf("axis[%d] = %d, blocksize[%d] = %d\n",
		 i,axis[i],i,blocksize[i]);
#endif
	}
      }
    }
    h.compress = compression;
    h.naxis = naxis;
    for (i=0; i<h.naxis; i++) {
      h.axis[i] = axis[i];
      h.blksz[i] = blocksize[i];
    }

    /* Write header to file. */
    if(tasfile_writeheader(fp, &h))
      goto bailout0;

    /* calculate number of blocks per dimension and in total. */
    nblk = calc_tasblockcount(&h,baxis);

    /* Skip over index part of file, which will be written last. */
    if (fseek (fp, nblk*sizeof(TAS_Index_t)-1, SEEK_CUR)) {
      fprintf (stderr, "ERROR: Failed to seek past index while writing TAS "
	      "file %s.\n", filename);
      goto bailout0;
    }
    if (fputc (0, fp)) {
      fprintf (stderr, "ERROR: Failed to zero byte to TAS file %s.\n",filename);
      goto bailout0;
    }
    if (header)
      memcpy(header, &h, sizeof(TAS_Header_t));
    fclose(fp);    
    return 0;
  }
  else
    return DRMS_ERROR_IOERROR;
    
 bailout0:
  fclose(fp);    
  return DRMS_ERROR_IOERROR;
}




/*****************************************************************************/

/* Defragment a TAS file. */
int drms_tasfile_defrag (char *tasfile, char *scratchdir) {
  char *template;
  int i, j, nblk, baxis[DRMS_MAXRANK], fdout;
  unsigned char *buf;
  long long lastoffset, outoffset;
  uint64_t len;
  TAS_Header_t h;
  TAS_Index_t *index;
  FILE *fp, *fpout;  
  int donothing=1;

  if (!(fp = fopen(tasfile,"r"))) {
    fprintf (stderr, "Failed to open TAS file '%s' for reading.\n",tasfile);
    return 1;
  }
  /* Read file header. */
  if (tasfile_readheader(fp, &h))
    return 1;

  /* calculate number of blocks per dimension and in total. */
  nblk = calc_tasblockcount(&h,baxis);
  /* Read index array from file. */
  if ((index = tasfile_readindex (fp, nblk, &h)) == NULL) {
    fprintf (stderr, "ERROR: Failed to read index from TAS file %s\n",
	    tasfile);
    return 1;
  }
  /* Find largest block and check if there is anything to do. */
  len = 0;    
  for (i=0; i<nblk; i++)
  {
    if (index[i].length > len)
      len = index[i].length;
    if (i>0 && index[i].offset != index[i-1].offset+index[i-1].length)
      donothing = 0;
  }
  if (donothing)
  {
    //#ifdef DEBUG
    printf("Nothing to do, file is already defragmented.\n");
    //#endif
    return 0;
  }

  XASSERT(buf = malloc(len));
  
  if (scratchdir) {
    XASSERT(template = malloc (strlen(scratchdir)+20));
    sprintf(template,"%s/XXXXXX",scratchdir);
  }
  else {
    XASSERT(template = malloc(25));
    sprintf (template, "/tmp/XXXXXX");
  }
  if ((fdout = mkstemp (template)) == -1) {
    perror ("Failed to create temporary file");
    return 1;
  }
  if (!(fpout = fdopen (fdout,"w"))) {
    fprintf (stderr, "Failed to open TAS file '%s' for reading",tasfile);
    perror ("");
    return 1;
  }
    /* Write header to file. */
  if (tasfile_writeheader(fpout, &h)) return 1;


  /* Main loop: Read one block at a time and unpack it into the
     main array. */
  lastoffset = -1;
  outoffset = sizeof(TAS_Header_t) + nblk*sizeof(TAS_Index_t);
  for (j=0; j < nblk; j++) {
    if (lastoffset != index[i].offset) {
#ifdef DEBUG	  
      printf ("Doing fseek to %lu. (lastoffset = %lu)\n",
	     index[i].offset, lastoffset);
#endif
      if (fseek (fp, index[i].offset, SEEK_SET)) {
	fprintf (stderr, "ERROR: Failed to seek to index in TAS file.\n");
	return 1;
      }
    }
    if (!fread (buf, index[i].length, 1, fp)) {
      free(index);
      return 1;
    }
    lastoffset = index[i].offset+index[i].length;
					/*  Update offset and write out  */
    index[i].offset = outoffset;
    if (!fwrite(buf,index[i].length, 1, fpout)) {
      fprintf (stderr, "ERROR: Failed to write data block %d to "
	      "TAS file %s.\n", j,template);
      return 1;
    }
    outoffset += index[i].length;
  }
						/*  Write index to file  */
  if (tasfile_writeindex (fp, nblk, index)) {
    fprintf (stderr, "ERROR: Failed to seek to end of TAS file %s\n",tasfile);
    return 1;
  }
  fclose(fp);
  fclose(fpout);
  if (rename (template, tasfile)) {
    perror ("ERROR: Failed to overwrite %s with defragmented version");
    return 1;
  }

  return 0;
}
