/*
 *  drms_binfile.c						2007.11.20
 *
 *  functions defined:
 *	drms_binfile_read
 *	drms_binfile_write
 *	drms_zipfile_read
 *	drms_zipfile_write
 */
#include "drms.h"
#include <zlib.h>

/*
 *  I/O of nearly raw binary files and gz compressed versions of same
 *  The (uncompressed) files have a header with the following format:
 *
 *	field    |  size    | meaning
 *	-----------------------------------
 *	DRMS RAW |  8 bytes | Magic header
 *	type     |  int32   | Data type given as enum value of DRMS_Type_t type.
 *	naxis    |  int32   | number of dimensions
 *	axis0    |  int32   | length of first dimension
 *	axis1    |  int32   | length of second dimension
 *	...                   
 *	axisn    |  int32   | length of n'th dimension
 *	buflen   |  int64   | total length of data buffer	-removed-
 *	data     |  buflen  | array data
 *
 *  For types other than DRMS_TYPE_STRING buflen should be
 *    sizeof (type) * axis0 * axis1 * ... *axisn bytes
 *  For DRMS_TYPE_STRING it is the total length of the 
 *    axis0*axis1*...*axisn zero-terminated strings stored consecutively
 *    in the file.
 */

#define TRY(code) if(!(code)) goto bailout;
/*
 *
 */
int drms_binfile_read (char *filename, int nodata, DRMS_Array_t *rf) {
  int i;
  FILE *fp;
  char magic[9], *p, **sptr;
  long long bufsize;

  if ((fp = fopen (filename, "r")) == NULL) {
    fprintf (stderr, "ERROR: Couldn\'t open file \"%s\"\n", filename);
    return 1;
  }
  TRY (fread (magic, 8, 1, fp));
  if (strncmp (magic, "DRMS RAW", 8)) {
    magic[8] = 0;
    fprintf (stderr,"Bad magic string for in file %s: \"%s\"\n", filename,
	magic);
    goto bailout;
  }
  TRY (fread (&rf->type, 4, 1, fp));
  TRY (fread (&rf->naxis, 4, 1, fp));
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, &rf->type);
  byteswap (4, 1, &rf->naxis);
#endif
  if (rf->naxis > DRMS_MAXRANK) {
    fprintf (stderr, "ERROR: Naxis (%d) in file exceeds DRMS_MAXRANK (%d)\n",
	rf->naxis, DRMS_MAXRANK);
    goto bailout;
  }
  for (i = 0; i < rf->naxis; i++) TRY (fread (&rf->axis[i], 4, 1, fp));       
  TRY (fread (&rf->buflen, 8, 1, fp));       
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, rf->naxis, rf->axis);
  byteswap (8, 1, rf->buflen);
#endif    
  if (nodata) {
    rf->data = NULL;
    fclose (fp);
    return 0;
  }
						 /*  Calculate size of data  */
  bufsize = drms_sizeof (rf->type);
  for (i = 0; i < rf->naxis; i++) bufsize *= rf->axis[i];
  XASSERT (rf->data = malloc (bufsize));

  if (rf->type == DRMS_TYPE_STRING) {
    XASSERT ((rf->strbuf = malloc (rf->buflen)));
    if (!fread (rf->strbuf, rf->buflen, 1, fp)) {
      free (rf->data);
      free (rf->strbuf);
      goto bailout;
    }
    p = rf->strbuf;
    sptr = rf->data;
    *sptr++ = p;
    while (p < rf->strbuf + rf->buflen) {
      if (*p == 0) {
	if (sptr > (char **)(((char *)rf->data) + bufsize)) {
	  free (rf->data);
	  free (rf->strbuf);
	  goto bailout;
	}
	*sptr++ = p;	    
      }
      p++;
    }
  } else {
    rf->strbuf = NULL;
    if (!fread (rf->data, bufsize, 1, fp)) {
      free (rf->data);
      goto bailout;
    }
#if __BYTE_ORDER == __BIG_ENDIAN
    drms_byteswap (rf->type, bufsize / drms_sizeof (rf->type), rf->data);
#endif
  }	
  fclose (fp);
  return 0;
bailout:
  fprintf (stderr, "ERROR: binfile_read failed \"%s\"\n", filename);
  fclose (fp);
  return 1;
}
/*
 *
 */
int drms_binfile_write (char *filename, DRMS_Array_t *rf) {
  int i;
  FILE *fp;
  char magic[9] = "DRMS RAW";
  size_t bufsize;

  if (rf->type == DRMS_TYPE_STRING) {
    fprintf (stderr, "ERROR: data type DRMS_TYPE_STRING unsupported\n");
    return 1;
  }
  if ((fp = fopen (filename,"w")) == NULL) {
    fprintf (stderr,"ERROR: Couldn't open file \"%s\"\n", filename);
    return 1;
  }
  if (rf->naxis > DRMS_MAXRANK) {
    fprintf (stderr, "ERROR: Naxis (%d) in file exceeds DRMS_MAXRANK (%d)\n",
	rf->naxis, DRMS_MAXRANK);
    goto bailout;
  }

  TRY( fwrite (magic, 8, 1, fp));
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, &rf->type);
  byteswap (4, 1, &rf->naxis);
  TRY (fwrite (&rf->type, 4, 1, fp));
  TRY (fwrite (&rf->naxis, 4, 1, fp));
  byteswap (4, 1,&rf->type);
  byteswap (4, 1,&rf->naxis);
  byteswap (4, rf->naxis, rf->axis);
  for (i = 0; i < rf->naxis; i++) TRY (fwrite (&rf->axis[i], 4, 1, fp));           
  byteswap (4, rf->naxis, rf->axis);
#else    
  TRY (fwrite (&rf->type, 4, 1, fp));
  TRY (fwrite (&rf->naxis, 4, 1, fp));
  for (i = 0; i < rf->naxis; i++) TRY (fwrite (&rf->axis[i], 4, 1, fp));           
#endif

  bufsize = drms_sizeof (rf->type);
  for (i = 0; i < rf->naxis; i++) bufsize *= rf->axis[i];
  rf->buflen = bufsize;
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (8, 1, &rf->buflen);
  TRY (fwrite (&rf->buflen, 8, 1, fp));           
  byteswap (8, 1, &rf->buflen);
  drms_byteswap (rf->type, bufsize / drms_sizeof(rf->type), rf->data);
  fwrite (rf->data, bufsize, 1, fp);
  drms_byteswap (rf->type, bufsize / drms_sizeof(rf->type), rf->data);
#else
  TRY (fwrite (&rf->buflen, 8, 1, fp));           
  fwrite (rf->data, bufsize, 1, fp);
#endif
  fclose (fp);
  return 0;
bailout:
  fclose (fp);
  unlink (filename);
  return 1;
}
/*
 *********************  ZIPFILE  *******************
 */
int drms_zipfile_read (char *filename, int nodata, DRMS_Array_t *rf) {
  int i;
  gzFile fp;
  char **sptr;
  char *p;
  char magic[9];
  size_t bufsize;

  if ((fp = gzopen (filename, "r")) == NULL) {
    fprintf (stderr, "ERROR: Couldn't open file \"%s\"\n", filename);
    return 1;
  }
  TRY (gzread (fp, magic, 8));
  if (strncmp (magic, "DRMS RAW", 8)) {
    magic[8] = 0;
    fprintf (stderr, "Bad magic string for in file %s: \"%s\"\n", filename,
	magic);
    goto bailout;
  }
  TRY (gzread (fp, &rf->type, 4));
  TRY( gzread (fp, &rf->naxis, 4));
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, &rf->type);
  byteswap (4, 1, &rf->naxis);
#endif
  if (rf->naxis > DRMS_MAXRANK) {
    fprintf (stderr,"ERROR: Naxis (%d) in file exceeds DRMS_MAXRANK (%d)\n",
	rf->naxis, DRMS_MAXRANK);
    goto bailout;
  }
  for (i=0; i < rf->naxis; i++) TRY (gzread (fp, &rf->axis[i], 4));           
  TRY (gzread (fp, &rf->buflen, 8));       
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, rf->naxis, rf->axis);
  byteswap (8, 1, rf->buflen);
#endif    
  if (nodata) {
    rf->data = NULL;
    gzclose (fp);
    return 0;
  }
						 /*  Calculate size of data  */
  bufsize = drms_sizeof (rf->type);
  for (i = 0;i < rf->naxis; i++) bufsize *= rf->axis[i];
  XASSERT (rf->data = malloc (bufsize));

  if (rf->type == DRMS_TYPE_STRING) {
    XASSERT ((rf->strbuf = malloc (rf->buflen)));
    if (gzread (fp, rf->strbuf, rf->buflen) < rf->buflen) {
      free (rf->data);
      free (rf->strbuf);
      goto bailout;
    }
    p = rf->strbuf;
    sptr = rf->data;
    *sptr++ = p;
    while (p < rf->strbuf + rf->buflen) {
      if (*p == 0) {
	if (sptr > (char **)(((char *)rf->data) + bufsize)) {
	  free (rf->data);
	  free (rf->strbuf);
	  goto bailout;
	}
	*sptr++ = p;	    
      }
      p++;
    }
  } else {
    if (gzread (fp, rf->data, bufsize) < bufsize) {
      free (rf->data);
      goto bailout;
    }
#if __BYTE_ORDER == __BIG_ENDIAN
    drms_byteswap (rf->type, bufsize / drms_sizeof (rf->type), rf->data);
#endif
  }
  gzclose (fp);
  return 0;
bailout:
  fprintf (stderr, "zipfile_read failed\n");
  gzclose (fp);
  return 1;
}
/*
 *
 */
int drms_zipfile_write (char *filename, DRMS_Array_t *rf) {
  gzFile *fp;
  int i;
  char magic[] = "DRMS RAW";
  size_t bufsize;

  if (rf->type == DRMS_TYPE_STRING) {
    fprintf (stderr, "ERROR: data type DRMS_TYPE_STRING unsupported\n");
    return 1;
  }
		  /*  Use Huffman coding only for floating point data - NO!. */
/*
  if (rf->type == DRMS_TYPE_FLOAT || rf->type == DRMS_TYPE_DOUBLE)
    fp = gzopen (filename,"wb1h");
  else
*/
  fp = gzopen (filename, "wb1");
  if (!fp) {
    fprintf (stderr, "ERROR: Couldn\'t write file \"%s\"\n", filename);
    return 1;
  }
  if (rf->naxis>DRMS_MAXRANK) {
    fprintf (stderr, "ERROR: Naxis (%d) in file exceeds DRMS_MAXRANK (%d)\n",
	rf->naxis, DRMS_MAXRANK);
    goto bailout;
  }
  TRY(gzwrite (fp, magic, 8));
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, &rf->type);
  byteswap (4, 1, &rf->naxis);
#endif
  TRY(gzwrite (fp, &rf->type, 4));
  TRY(gzwrite (fp, &rf->naxis, 4));
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, 1, &rf->type);
  byteswap (4, 1, &rf->naxis);
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, rf->naxis, rf->axis);
#endif    
  for (i = 0; i < rf->naxis; i++) TRY (gzwrite (fp, &rf->axis[i], 4));           
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (4, rf->naxis, rf->axis);
#endif    

  bufsize = drms_sizeof (rf->type);
  for (i = 0; i < rf->naxis; i++) bufsize *= rf->axis[i];
  rf->buflen = bufsize;
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (8, 1, &rf->buflen);
  drms_byteswap (rf->type, bufsize / drms_sizeof (rf->type), rf->data);
#endif
  TRY (gzwrite (fp, &rf->buflen, 8));           
  if (gzwrite(fp, rf->data, bufsize) != bufsize) goto bailout;
#if __BYTE_ORDER == __BIG_ENDIAN
  byteswap (8, 1, &rf->buflen);
  drms_byteswap (rf->type, bufsize / drms_sizeof (rf->type), rf->data);
#endif
  gzclose (fp);
  return 0;
bailout:
  fprintf (stderr, "zipfile_write failed\n");
  gzclose (fp);
  unlink (filename);
  return 1;
}
