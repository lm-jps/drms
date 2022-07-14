//#define DEBUG
#ifdef LINUX
#define _GNU_SOURCE
#endif /* LINUX */
#include "drms.h"
#include "drms_priv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <libgen.h>
#include <fcntl.h> 
#include <unistd.h>
#include "printk.h"
#include "rice.h"


/*************** Magic numbers **************/
#define NFS_SUPER_MAGIC (0x6969)


/******** Local functions **************/
static int fits_copy_header(char *out, char *in, int headlen);
static int fits_get_sizes(int headlen, char *header, int *bitpix, 
			  int *naxis, int *axis);
static int fits_get_blank(int headlen, char *header, DRMS_Type_t type,
			  DRMS_Type_Value_t *blank);
static int fits_get_scaling(int headlen, char *header, double *bzero, 
			    double *bscale);

#ifdef _GNU_SOURCE 
#define WRITE(fh,buf,len)  (write(fh, buf,len)!=-1)
#define CLOSE(fh) close(fh)
#else
#define WRITE(fh,buf,len)  (fwrite(buf, len, 1, fh)==1)
#define CLOSE(fh) fclose(fh)
#endif

static int drms_type2bitpix(DRMS_Type_t type)
{
  switch(type)
  {
  case DRMS_TYPE_CHAR: 
    return 8;
    break;
  case DRMS_TYPE_SHORT:
    return 16;
    break;
  case DRMS_TYPE_INT:  
    return 32;
    break;
  case DRMS_TYPE_LONGLONG:  
    return 64;
    break;
  case DRMS_TYPE_FLOAT:
    return -32;
    break;
  case DRMS_TYPE_DOUBLE: 	
  case DRMS_TYPE_TIME: 
    return -64;
    break;
  case DRMS_TYPE_STRING: 
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)type);
    XASSERT(0);
    return -1;
    break;
  }
}



static DRMS_Type_t drms_bitpix2type(int bitpix)
{
  switch(bitpix)
  {
  case 8:
    return DRMS_TYPE_CHAR; 
    break;
  case 16:
    return DRMS_TYPE_SHORT;
    break;
  case 32:
    return DRMS_TYPE_INT;
    break;
  case 64:
    return DRMS_TYPE_LONGLONG;  
    break;
  case -32:
    return DRMS_TYPE_FLOAT;
    break;
  case -64:
    return DRMS_TYPE_DOUBLE; 	
    break;
  default:
    fprintf(stderr, "ERROR: Invalid bitpix value %d\n",bitpix);
    XASSERT(0);
    break;
  }
}


int drms_writefits(const char* file, int compress, int headlen, char *header, 
		   DRMS_Array_t *arr)
{
#ifdef _GNU_SOURCE 
  int fh;
  struct statfs stat;
  int oflags;
#else
  FILE *fh;
#endif
  int i, rem, hl,nzhead, nzdata, align, sz, bitpix;
  unsigned char *zdata, *rawbuf;
  char *head;
  char *p,*p1;
  unsigned int npix,buflen;
  int ndata_blocks, len;
  DRMS_Type_Value_t missing;


  if (headlen % 80)
  {
    printkerr("Error: Fits header length must be a multiple of 80.\n");
    return 1;
  }
    
#ifdef _GNU_SOURCE 
  strncpy(buf,file,1024);
  if (statfs(dirname(buf), &stat))
  {
    printkerr("Couldn't stat file %s.\n",file);
    return 1;
  }

  /* Test if the filesystem is NFS. If so don't attempt
     to do unbuffered I/O. */
  if (compress && stat.f_type != NFS_SUPER_MAGIC)
  {
    align = stat.f_bsize;
    oflags = O_TRUNC|O_WRONLY|O_CREAT|O_DIRECT;
  }
  else
  {
    align = 1;
    oflags = O_TRUNC|O_WRONLY|O_CREAT;
  }
  if ((fh=open(file,oflags,0644)) == -1)
  {
    printkerr("Couldn't open file %s.\n",file);
    return 1;
  }
#else
  align = 1;
  if (!(fh=fopen(file,"w")))
  {
    printkerr("Couldn't open file %s.\n",file);
    return 1;
  }
#endif
  
  bitpix = drms_type2bitpix(arr->type);
  /* Write standard FITS keywords. */
  XASSERT(head = malloc(headlen+2*2880));
  memset(head,' ',headlen+2*2880);
  p = head;
  CHECKSNPRINTF(snprintf(p,80,"SIMPLE  =                    T"), 80);
  p += 80;
  CHECKSNPRINTF(snprintf(p,80,"BITPIX  = %20d / Number of bits per data pixel",bitpix), 80);
  p += 80;
  CHECKSNPRINTF(snprintf(p,80,"NAXIS   = %20d / Number of data axes",arr->naxis), 80);
  p += 80;
  for (i=0; i<arr->naxis; i++)
  {
    CHECKSNPRINTF(snprintf(p,80,"NAXIS%-3d= %20d",i+1,arr->axis[i]), 80);
    p += 80;
  }
  if (bitpix>0)
  {
    int len;
    char blankstr[80];

    p1 = p;
    p1 += sprintf(p1,"BLANK   = ");
    drms_missing(arr->type, &missing);
    drms_sprintfval(blankstr, arr->type, &missing, 1);
    len = strlen(blankstr);
    p1 += 20-len;
    memcpy(p1,blankstr,len);
    p += 80;
    if ( arr->israw && (arr->bscale!=1.0 || fabs(arr->bzero)!=0.0))
    {
      CHECKSNPRINTF(snprintf(p,80,"BZERO   = %20.12lG ",arr->bzero), 80);
      p += 80;
      CHECKSNPRINTF(snprintf(p,80,"BSCALE  = %20.12lG ",arr->bscale), 80);
      p += 80;
    }      
  }
  if (headlen>0 && header!=NULL) 
    p += fits_copy_header(p,header,headlen);
  CHECKSNPRINTF(snprintf(p,80,"END"), 80);
  p += 80;
    
  /* Pad with empty lines to get an integer multiple of 36 lines. 
     When do we get rid of the #$%#!@%#&^@%! punch card heritage?!?!?*/
  while ((p-head)%2880)
  {
    memset(p,' ',80);
    p += 80;
  }
  hl = p-head;
  for(i=0; i<hl; i++)
    if (head[i]=='\0')
      head[i] = ' ';

  npix = 1;
  for (i=0;i<arr->naxis;i++)
    npix *= arr->axis[i]; 
  sz = drms_sizeof(arr->type);

  if (compress)
  {
    buflen = npix*sz;
    buflen += 2880 - (buflen % 2880);
    ndata_blocks = buflen/2880;
    XASSERT(rawbuf = malloc(buflen+2880+35+2*align));
    zdata = rawbuf + (align - ((unsigned long)rawbuf % align));

    /* Generate compressed header block. */ 
    len = 35;
    nzhead = rice_encode1(head, 2880, &zdata[len], buflen, 8);
    if (nzhead < 0) 
    {
      printkerr( "Header compression failed with error code %d\n", 
		 nzhead);
      free(rawbuf);
      goto bailout;
    }
    len += nzhead;
    

    /* Generate compressed data block. */ 
    nzdata = drms_compress(DRMS_COMP_RICE, arr->type, npix, arr->data, 
			   buflen, &zdata[len]);
    if (nzdata < 0) 
    {
      printkerr( "Data compression failed with error code %d\n", 
		 nzhead);
      free(rawbuf);
      goto bailout;
    }
    len += nzdata;


    /* Generate uncompressed mini header block. */ 
    CHECKSNPRINTF(snprintf(head,2880, "%2d %2d %1d %8d %8d %8d\n", bitpix, 64, 1, 
			   ndata_blocks,nzhead, nzdata), 2880);
    memcpy(zdata, head,35);

    /* Write entire compressed file. */ 
    rem =  align - (len % align); 
    memset(&zdata[len],0,rem);
    len += rem;
    if (!WRITE(fh, zdata, len))
    {
      printkerr( "Failed to write compressed data to file %s\n",file);
      free(rawbuf);
      goto bailout;
    }
    free(rawbuf);
  }
  else
  {
    /* Write header block. */ 
    if (!WRITE(fh,head,hl))
    {
      printkerr( "Failed to write header data to file %s\n",file);
      goto bailout;
    }

    /* The FITS standard stipulates storing binary data in big endian format. */
#if __BYTE_ORDER == __LITTLE_ENDIAN
    drms_byteswap(arr->type, npix, arr->data);
#endif
    
    /* Write data */
    if (!WRITE(fh,arr->data,sz*npix))
    {
      printkerr( "Failed to write data to file %s\n",file);
      goto bailout;
    }
  
    /* Pad with zeros to get an integer number of 2880 byte records. */
    memset(head,0,2880);
    rem = 2880 - ((sz*npix) % 2880);
    if (!WRITE(fh, head, rem))
    {
      printkerr( "Failed to write zero padding to file %s\n",file);
      goto bailout;
    }      
  
    /* Restore data buffer to native format. */
#if __BYTE_ORDER == __LITTLE_ENDIAN
    drms_byteswap(arr->type, npix, arr->data);
#endif
  }
  /* File successfully written to disk. Close it and return 0 to signal
     success. */
  free(head);
  CLOSE(fh);
  return 0;


 bailout:
  /* Something went wrong. Close the file, remove it and return 
     an error code. */
  free(head);
  CLOSE(fh);
  unlink(file);
  return 1;  
}
#undef WRITE
#undef CLOSE


/* 
   Read simple FITS array from file. The raw header data is returned
   in *header.  The array data is returned in arr. The missing, bzero,
   and bscale fields in the returned array are set to the values found
   in the header for BLANK, BZERO and BSCALE. If readraw=0 then the
   data is scaled according to bscale and bzero, if readraw=1 no
   scaling is done.  

   Return codes: 
     status = 0: success, 
     status = 1: an error occured.
*/
DRMS_Array_t *drms_readfits(const char* file, int readraw, int *headlen, 
			    char **header, int *status)
{
  FILE *fh;
  struct stat file_stat;
  char buf[2880];
  int END_not_found = 1, statflag=0;
  char *line;
  unsigned long npix;
  unsigned char *zdata, *zhead;
  int compress, nhead_blocks=0, ndata_blocks=0, bitpix=0;
  int nhead=0,i,sz,n;
  int nzhead, nzdata, blk;
  DRMS_Type_Value_t missing;
  DRMS_Array_t *arr, *out;
  double *new;

    
  if (!(fh = fopen(file, "r"))) 
  {
    printkerr("Can't open file %s for input\n", file);
    if (*status)
      *status = DRMS_ERROR_IOERROR;
    return NULL;
  }
  if (fstat(fileno(fh), &file_stat)) {
    printkerr("Can't stat file %s\n", file);
    statflag = DRMS_ERROR_IOERROR;
    goto bailout;
  }

  /* Do a minimal check to see if it is likely a conforming 
     FITS file, or a FITZ file. */
  compress = !strcasecmp(&file[strlen(file)-5],".fitz");
  fread(buf, 1, 80, fh);
  if (strncmp(buf,"SIMPLE  =                    T",
	      sizeof("SIMPLE  =                    T")-1))
  {
    if (!compress)
    {
      printkerr("Input file is not a valid FITS file.\n");
      statflag = DRMS_ERROR_INVALIDFILE;
      goto bailout;
    }
  }
  else
  {
    if (compress)
    {
      printkerr("Input file is not a valid FITZ file.\n");
      statflag = DRMS_ERROR_INVALIDFILE;
      goto bailout;
    }
  }
  rewind(fh);


  if (compress == 0)
  {
    if (file_stat.st_size % 2880) {
      printkerr("Bad input FITS file size\n");
      statflag = DRMS_ERROR_INVALIDFILE;
      goto bailout;
    }

    // determine primary header length and data type
    while (END_not_found) 
    {
      if (2880 != fread(buf, 1, 2880, fh)) {
	printkerr("Read failed on FITS file %s\n", file); 
	statflag = DRMS_ERROR_IOERROR;
	goto bailout;
      }
      for (i=0; i<36; ++i) 
      {
	line = buf + i*80;
	if (!strncmp(line, "END     ", 8))
	  END_not_found = 0;
      }
      ++nhead_blocks;
    }  
    *headlen = nhead = nhead_blocks*2880;    
    XASSERT(*header = malloc(nhead));
    rewind(fh);
    if (1 != fread(*header, nhead, 1, fh)) 
    {
      printkerr( "Failed to read FITS header from file %s\n",file);
      free(*header);
      statflag = DRMS_ERROR_IOERROR;
      goto bailout;
    }
  }
  else
  {
    if (fscanf(fh, "%d %d %d %d %d %d\n", &bitpix, &blk, 
	       &nhead_blocks, &ndata_blocks, &nzhead, &nzdata) != 6) 
    {
      printkerr("Can't read compressed file header from file %s\n",file);
      statflag = DRMS_ERROR_IOERROR;
      goto bailout;
    }
    *headlen = nhead = nhead_blocks*2880;    
    XASSERT(zhead = (unsigned char *) malloc(nzhead));
    if (fread(zhead, nzhead, 1, fh) != 1) 
    {
      printkerr("Can't read compressed FITS header from file %s\n",file);
      free(zhead);
      statflag = DRMS_ERROR_IOERROR;
      goto bailout;
    }
    XASSERT(*header = malloc(nhead));
    if (rice_decode1(zhead, nzhead, *header, nhead, 8)) {
      printkerr("Decompressing FITS header failed with error code"
		" %d\n",status);
      free(zhead);
      free(*header);
      *header = NULL;
      statflag = DRMS_ERROR_COMPRESSFAILED;
      goto bailout;
    }
    free(zhead);
  }
  
  XASSERT(arr = malloc(sizeof(DRMS_Array_t)));
  if (fits_get_sizes(nhead, *header, &bitpix, &arr->naxis, arr->axis))
  {
    printkerr( "Invalid header.\n");
    free(*header);
    *header = NULL;
    statflag = DRMS_ERROR_INVALIDFILE;
    goto bailout;
  }
  /* Get missing value from header. */
  arr->type = drms_bitpix2type(bitpix);

  npix = 1;
  for (i=0; i<arr->naxis; i++)
    npix *= (arr->axis)[i];
  sz = drms_sizeof(arr->type);

  if (compress == 0)
  {
    XASSERT(arr->data = malloc(npix*sz));
    /* Read raw data from the files. */
    if (fread(arr->data, npix*sz, 1, fh) != 1) 
    {
      printkerr( "Failed to read FITS data\n");
      statflag = DRMS_ERROR_IOERROR;
      goto bailout2;
    }
    
#if __BYTE_ORDER == __LITTLE_ENDIAN
    drms_byteswap(arr->type, npix, arr->data);
#endif
  }
  else
  {
    XASSERT(arr->data = malloc(ndata_blocks*2880));
    XASSERT(zdata = malloc(nzdata));
    if (fread(zdata, nzdata, 1, fh) != 1) 
    {
      printkerr( "Failed to read compressed FITS data\n");
      free(zdata);
      statflag = DRMS_ERROR_IOERROR;
      goto bailout2;
    }
    if (drms_uncompress(DRMS_COMP_RICE, nzdata, zdata, arr->type, npix, 
			arr->data) != npix)
    {
      printkerr( "Decompression of FITS data failed with error code %d\n",
		 status);
      free(zdata);
      statflag = DRMS_ERROR_COMPRESSFAILED;
      goto bailout2;
    }
    free(zdata);
  }    

  /* Check if this fits file had non-trivial bzero, bscale values. 
     If so scale and convert to double. */
  arr->israw = 0;
  arr->bzero = 0.0;
  arr->bscale = 1.0;

  if (bitpix>0)
  {
    if (readraw)
    {
#ifdef DEBUG
      printf("Reading raw\n");
#endif
      arr->israw = 1;
      fits_get_scaling(nhead, *header, &arr->bzero, &arr->bscale);
    }
    else
    {
#ifdef DEBUG
      printf("Reading non-raw\n");
#endif
      /* Apply scaling if required. */
      if (fits_get_scaling(nhead, *header, &arr->bzero, &arr->bscale))
      {
#ifdef DEBUG
	printf("Found scaling, bzero=%f, bscale=%g\n",arr->bzero,arr->bscale);
#endif
        out = drms_array_create(DRMS_TYPE_DOUBLE, arr->naxis, 
				arr->axis, NULL, NULL);
	n = drms_array_count(arr);
	new = out->data;
	/* Set blanks to NaN. */
	if (fits_get_blank(nhead, *header, arr->type, &missing))
	{	  
#ifdef DEBUG
	  printf("Found blank = ");
	  drms_printfval(arr->type, &missing);
	  printf("\n");
#endif
	  switch(arr->type)
	  {
	  case DRMS_TYPE_CHAR:       
	    {
	      char *ssrc = (char *) (arr->data);     
	      for (i=0; i<n; i++, ssrc++, new++)
	      {
		if (*ssrc == missing.char_val)
		  *new = DRMS_MISSING_DOUBLE;
		else
		  *new = arr->bscale * *ssrc + arr->bzero; 
	      }
	    }
	    break;
	  case DRMS_TYPE_SHORT:
	    {
	      short *ssrc = (short *) (arr->data);
	      for (i=0; i<n; i++, ssrc++, new++)
	      {
		if (*ssrc == missing.short_val)
		  *new = DRMS_MISSING_DOUBLE;
		else
		  *new = arr->bscale * *ssrc + arr->bzero; 
	      }
	    }
	    break;
	  case DRMS_TYPE_INT:  
	    {
	      int *ssrc = (int *) (arr->data);
	      for (i=0; i<n; i++, ssrc++, new++)
	      {
		if (*ssrc == missing.int_val)
		  *new = DRMS_MISSING_DOUBLE;
		else
		  *new = arr->bscale * *ssrc + arr->bzero; 
	      }
	    }
	    break;
	  case DRMS_TYPE_LONGLONG:  
	    {
	      long long *ssrc = (long long *) (arr->data);
	      for (i=0; i<n; i++, ssrc++, new++)
	      {
		if (*ssrc == missing.longlong_val)
		  *new = DRMS_MISSING_DOUBLE;
		else
		  *new = arr->bscale * *ssrc + arr->bzero; 
	      }
	    }
	    break;
	  default:
	    break;
	  }
	}
	else
	{
	  switch(arr->type)
	  {
	  case DRMS_TYPE_CHAR:       
	    {
	      char *ssrc = (char *) (arr->data);     
	      for (i=0; i<n; i++, ssrc++, new++)
		*new = arr->bscale * *ssrc + arr->bzero; 
	    }
	    break;
	  case DRMS_TYPE_SHORT:
	    {
	      short *ssrc = (short *) (arr->data);
	      for (i=0; i<n; i++, ssrc++, new++)
		*new = arr->bscale * *ssrc + arr->bzero; 
	    }
	    break;
	  case DRMS_TYPE_INT:  
	    {
	      int *ssrc = (int *) (arr->data);
	      for (i=0; i<n; i++, ssrc++, new++)
		*new = arr->bscale * *ssrc + arr->bzero; 
	    }
	    break;
	  case DRMS_TYPE_LONGLONG:  
	    {
	      long long *ssrc = (long long *) (arr->data);
	      for (i=0; i<n; i++, ssrc++, new++)
		*new = arr->bscale * *ssrc + arr->bzero; 
	    }
	    break;
	  default:
	    break;
	  }
	}
	arr->type = DRMS_TYPE_DOUBLE;
	free(arr->data);
	arr->data = out->data;
	free(out);
      }
      else
      {
#ifdef DEBUG
      printf("No scaling\n");
#endif
	/* Set blanks to the internal missing value for the relevant type. */
	if (fits_get_blank(nhead, *header, arr->type, &missing))
	{
#ifdef DEBUG
	  printf("Found blank = ");
	  drms_printfval(arr->type, &missing);
	  printf("\n");
#endif
	  n = drms_array_count(arr);
	  switch(arr->type)
	  {
	  case DRMS_TYPE_CHAR:       
	    {
	      char *ssrc = (char *) (arr->data);     
	      if (missing.char_val != DRMS_MISSING_CHAR)
	      {
		for (i=0; i<n; i++, ssrc++)
		{
		  if (*ssrc == DRMS_MISSING_CHAR)
		    statflag = DRMS_WARNING_BADBLANK;
		  else if (*ssrc == missing.char_val)
		    *ssrc = DRMS_MISSING_CHAR;
		}
	      }
	    }
	    break;
	  case DRMS_TYPE_SHORT:
	    {
	      short *ssrc = (short *) (arr->data);
	      for (i=0; i<n; i++, ssrc++)
	      {
		if (*ssrc == DRMS_MISSING_SHORT)
		  statflag = DRMS_WARNING_BADBLANK;
		else if (*ssrc == missing.short_val)
		  *ssrc = DRMS_MISSING_SHORT;
	      }
	    }
	    break;
	  case DRMS_TYPE_INT:  
	    {
	      int *ssrc = (int *) (arr->data);
	      for (i=0; i<n; i++, ssrc++)
	      {
		if (*ssrc == DRMS_MISSING_INT)
		  statflag = DRMS_WARNING_BADBLANK;
		else if (*ssrc == missing.int_val)
		  *ssrc = DRMS_MISSING_INT;
	      }
	    }
	    break;
	  case DRMS_TYPE_LONGLONG:  
	    {
	      long long *ssrc = (long long *) (arr->data);
	      for (i=0; i<n; i++, ssrc++)
	      {
		if (*ssrc == DRMS_MISSING_LONGLONG)
		  statflag = DRMS_WARNING_BADBLANK;
		else if (*ssrc == missing.longlong_val)
		  *ssrc = DRMS_MISSING_LONGLONG;
	      }
	    }
	    break;
	  default:
	    break;
	  }
	}
      }
    }
  }
  if (status)
    *status = statflag;
  fclose(fh);
  return arr;

 bailout2:
  free(arr->data);
  arr->data = NULL;
  free(*header);
  *header = NULL;
 bailout:
  fclose(fh);
  if (status)
    *status = statflag;
  return NULL;
}

static int fits_copy_header(char *out, char *in, int headlen)
{
  int newlen=0;

  while (headlen>0)
  {
    if (strncmp(in, "SIMPLE  =", 9) &&
	strncmp(in, "BITPIX  =", 9) &&	
	strncmp(in, "BLANK   =", 9) &&	
	strncmp(in, "BZERO   =", 9) &&	
	strncmp(in, "BSCALE  =", 9) &&	
	strncmp(in, "NAXIS", 5) &&
	strncmp(in, "END     ", 8))
    {
      memcpy(out, in, 80);
      out += 80;
      newlen += 80;
    }
    in += 80;
    headlen -= 80;
  }
  return newlen;
}

void drms_print_fitsheader(int headlen, char *header)
{
  int i;
  char *p=header;

  for (i=0; i<headlen; i+=80, p+=80)
  {
    fwrite(p, 80, 1,stdout);
    fputc('\n', stdout);
    if (!strncmp(p, "END     ", 8))
      break;
  }
}



static int fits_get_sizes(int headlen, char *header, int *bitpix, 
			  int *naxis, int *axis)
{
  int i;
  *naxis=0;
  *bitpix=0;
  
  while (headlen>0)
  {
    if (!*bitpix && !strncmp(header, "BITPIX  =", 9)) 
    {
      sscanf(header, "BITPIX  = %d", bitpix);
      if (!(*bitpix==8 || *bitpix==16 || *bitpix==32 || *bitpix==64 || 
	    *bitpix==-32 ||*bitpix==-64)) 
      {
	printkerr( "BITPIX = %d not allowed\n", *bitpix);
	return 1;
      }
    }
    else if (!*naxis && !strncmp(header, "NAXIS   =",9))
    {
      sscanf(header, "NAXIS   = %d", naxis);
      if (*naxis<1 || *naxis>DRMS_MAXRANK) 
      {
	printkerr( "NAXIS = %d not allowed\n", naxis);
	return 1;
      }
      else
	memset(axis,0,*naxis*sizeof(int));
    }
    else if (!strncmp(header, "NAXIS",5))
    {
      if (!*naxis)
      {
	printkerr( "NAXISXXX not allowed before NAXIS\n");
	return 1;
      }	    
      sscanf(header, "NAXIS%d", &i);
      i--;
      //      printk("header = '%.80s', i = %d\n",header,i);
      sscanf(header+9, "%d", &axis[i]);
      //      printk("axis[%d] = %d\n",i,axis[i]);
    }
    headlen -= 80;
    header += 80;
  }

  /* Make sure we got NAXIS and BITPIX. */
  if ( *naxis==0 || *bitpix==0 )
    return 1;

  /* Make sure we got all the axis lengths. */
  for  (i=0; i<*naxis; i++)
    if (axis[i] == 0)
      return 1;
  
  return 0;  
}



static int fits_get_blank(int headlen, char *header, DRMS_Type_t type,
			  DRMS_Type_Value_t *blank)
{
    DRMS_Value_t vholder;
    memset(&vholder, 0, sizeof(DRMS_Value_t));

  while (headlen>0)
  {
    if (!strncmp(header, "BLANK   =", 9)) 
    {
      drms_sscanf2(header+9, NULL, 0, type, &vholder);
      *blank = vholder.value;
      return 1;
    }
    headlen -= 80;
    header += 80;
  }
  return 0;
}



static int fits_get_scaling(int headlen, char *header, double *bzero, 
			    double *bscale)
{
  int cnt=0;
  *bzero = 0.0;
  *bscale = 1.0;
  while (headlen>0)
  {
    if (!strncmp(header, "BZERO   =", 9)) 
    {
      sscanf(header+9, "%lf", bzero);
      cnt |= 1;
    }
    else if (!strncmp(header, "BSCALE  =", 9)) 
    {
      sscanf(header+9, "%lf", bscale);
      cnt |= 2;
    }
    headlen -= 80;
    header += 80;
  }
  return cnt==3;
}

