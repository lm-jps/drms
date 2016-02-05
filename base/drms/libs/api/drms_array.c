#include "drms.h"

// #define DEBUG


/* Assemble an array struct from its constituent parts. */
DRMS_Array_t *drms_array_create(DRMS_Type_t type, int naxis, 
				int *axis, void *data, int *status)
{
  int i, stat=0;
  DRMS_Array_t *arr;

  arr = malloc(sizeof(DRMS_Array_t));
  XASSERT(arr);
  memset(arr,0,sizeof(DRMS_Array_t));

  /* Initialize the bscale and bzero values. If they are not 1.0 and 0.0, then the caller must set them to values appropriate for the 
   * data. */
  arr->bscale = 1.0;
  arr->bzero = 0.0;

  if (naxis <= DRMS_MAXRANK)
  {
    arr->naxis = naxis;
  }
  else
  {
    stat = DRMS_ERROR_INVALIDRANK;
    goto bailout;
  }
  if (type != DRMS_TYPE_RAW) /* RAW is not a proper type. Just a flag for 
				the segment reader routines. */
    arr->type = type;
  else
  {
    stat = DRMS_ERROR_INVALIDTYPE;
    goto bailout;
  }
  memcpy(arr->axis,axis,naxis*sizeof(int));
  if (data)
    arr->data = data;
  else
  {
     arr->data = malloc(drms_array_size(arr));
     XASSERT(arr->data);
  }

  /* Set offset multiplier. */
  arr->dope[0] = drms_sizeof(arr->type);
  for (i=1; i<arr->naxis; i++)
    arr->dope[i] = arr->dope[i-1]*arr->axis[i-1];
    
  if (status)
    *status = stat;
  return arr;

 bailout:
  free(arr);
  if (status)
    *status = stat;
  return NULL;
}

/* Free data part of array and the array structure itself. */
void drms_free_array(DRMS_Array_t *arr)
{
  if  (arr)
  {
    if (arr->data)
      free(arr->data);
    free(arr);
  }
}

/* Convert array from one DRMS type to another in place. */
void drms_array_convert_inplace(DRMS_Type_t newtype, double bzero, 
				double bscale, DRMS_Array_t *src)
{
  DRMS_Array_t *tmp;

  if (newtype != src->type || fabs(bzero)!=0.0 || bscale!=1.0)
  {
    tmp = drms_array_convert(newtype, bzero, bscale, src);
    src->type = newtype;
    free(src->data);
    src->data = tmp->data;
    free(tmp);
  }
}


/* Convert array from one DRMS type to another. */
DRMS_Array_t *drms_array_convert(DRMS_Type_t dsttype, double bzero, 
				 double bscale, DRMS_Array_t *src)
{
  long long n;
  DRMS_Array_t *arr;

  arr = drms_array_create(dsttype, src->naxis, src->axis, NULL, NULL);
  n = drms_array_count(src);
  drms_array_rawconvert(n, dsttype, bzero, bscale, arr->data, 
			src->type, src->data);
  return arr;
}



/* Take out a hyperslab of an array. */
DRMS_Array_t *drms_array_slice(int *start, int *end, DRMS_Array_t *src)
{
  int i;
  DRMS_Array_t *arr;
  int axis[DRMS_MAXRANK];


  for (i=0; i<src->naxis; i++)
  {
    if (start[i]>end[i])
    {
      fprintf(stderr,"ERROR in drms_array_slice: end[%d]=%d > start[%d]=%d\n",
	      i,end[i],i,start[i]);
      return NULL;
    }
    axis[i] = end[i]-start[i]+1;
  }
  arr = drms_array_create(src->type, src->naxis, axis, NULL, NULL);
  /* Cut out the desired slice */
  ndim_pack(drms_sizeof(src->type), src->naxis, src->axis, start, end, 
	    src->data, arr->data);
  return arr;
}


/* Rearrange the array elements such that the dimensions are ordered
   according to the permutation given in "perm". This is a generalization of 
   the matrix transpose operator to n dimensions. */
DRMS_Array_t *drms_array_permute(DRMS_Array_t *src, int *perm, int *status)
{
  int i, stat;
  DRMS_Array_t *dst;

  dst = drms_array_create(src->type, src->naxis,  src->axis, NULL, &stat);
  if (stat)
    goto bailout;
  stat = ndim_permute(drms_sizeof(src->type), src->naxis, src->axis, perm, 
		      src->data, dst->data);
  if (stat)
    goto bailout;
  else
    for (i=0; i<dst->naxis; i++)
      dst->axis[i] = src->axis[perm[i]];
  return dst;

 bailout:
  if (status)
    *status = stat;
  return NULL;
}

/* Convert array from one DRMS type to another. */
int drms_array_rawconvert(int n, DRMS_Type_t dsttype, double bzero, 
			  double bscale, void *dst,  DRMS_Type_t srctype, 
			  void *src)
{
  int stat=-1;

  switch(dsttype)    
  {
  case DRMS_TYPE_CHAR: 
    stat = drms_array2char(n, srctype, bzero, bscale,  src,  (char *) dst);
    break;
  case DRMS_TYPE_SHORT:
    stat = drms_array2short(n, srctype, bzero, bscale, src,  (short *) dst);
    break;
  case DRMS_TYPE_INT:  
    stat = drms_array2int(n, srctype, bzero, bscale, src,  (int *) dst);
    break;
  case DRMS_TYPE_LONGLONG:  
    stat = drms_array2longlong(n, srctype, bzero, bscale, src,  (long long *) dst);
    break;
  case DRMS_TYPE_FLOAT:
    stat = drms_array2float(n, srctype, bzero, bscale, src,  (float *) dst);
    break;
  case DRMS_TYPE_DOUBLE: 	
    stat = drms_array2double(n, srctype, bzero, bscale, src,  (double *) dst);
    break;
  case DRMS_TYPE_TIME: 
    stat = drms_array2time(n, srctype, bzero, bscale, src,  (double *) dst);
    break;
  case DRMS_TYPE_STRING: 
    stat = drms_array2string(n, srctype, bzero, bscale, src, (char **) dst);
    break;
  default:
    fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)dsttype);    
  }  
  return stat;
}



void drms_array2missing(DRMS_Array_t *arr)
{
  int i, n;

  n = drms_array_count(arr);
  switch(arr->type)
  {
  case DRMS_TYPE_CHAR:       
    {
      char *ssrc = (char *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_CHAR;
    }
    break;
  case DRMS_TYPE_SHORT:
    {
      short *ssrc = (short *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_SHORT;
    }
    break;
  case DRMS_TYPE_INT:  
    {
      int *ssrc = (int *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_INT;
    }
    break;
  case DRMS_TYPE_LONGLONG:  
    {
      long long *ssrc = (long long *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_LONGLONG;
    }
    break;
  case DRMS_TYPE_FLOAT:
    {
      float *ssrc = (float *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_FLOAT;
    }
    break;
  case DRMS_TYPE_DOUBLE: 	
    {
      double *ssrc = (double *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_DOUBLE;
    }
    break;
  case DRMS_TYPE_TIME: 
    {
      double *ssrc = (double *) (arr->data);
      for (i=0; i<n; i++, ssrc++)
	*ssrc = DRMS_MISSING_TIME;
    }
    break;
  case DRMS_TYPE_STRING: 
    {
      char **ssrc = (char **) (arr->data);
      for (i=0; i<n; i++)
        ssrc[i] = NULL;

    }
    break;
  default:
    break;
  }
}



/* Print array according to type with given coluimn and row 
   separator strings. */
void drms_array_print(DRMS_Array_t *arr, const char *colsep, const char *rowsep)
{
	drms_array_fprint(stdout, arr, colsep, rowsep);
}

/*Print array according to file to type with given column and row separator strings. */

void drms_array_fprint(FILE *keyfile, DRMS_Array_t *arr, const char *colsep, 
			const char *rowsep)
{
  int i, j, k, sz, count, index[DRMS_MAXRANK];
  unsigned char *p;  

  sz = drms_sizeof(arr->type);
  p = arr->data;
  switch(arr->naxis)
  {
  case 1:
    arr->axis[1]=1;
  case 2:
    for (i=0; i<arr->axis[1]; i++)
    {
      drms_fprintfval_raw(keyfile, arr->type, p);
      p += sz;
      for (j=1; j<arr->axis[0]; j++, p += sz)
      {
	fprintf(keyfile,"%s",colsep);
	drms_fprintfval_raw(keyfile, arr->type, p);
      }
      fprintf(keyfile, "%s",rowsep);
    }
    break;
  default:
    count = drms_array_count(arr);
#ifdef DEBUG
    printf("count = %d, sz = %d\n",count,sz);
#endif
    memset(index,0,arr->naxis*sizeof(int));
    //    chunk = sz*arr->axis[0]*arr->axis[1];
    i = 0;
    while ( i<count )
    {
      fprintf(keyfile, "array");
      for (j=arr->naxis-1; j>1; j--)
	fprintf(keyfile, "[%d]",index[j]);
      fprintf(keyfile, "[*][*] = \n");
      for (j=2; j<arr->naxis-1; j++)
      {
	if (index[j]==arr->axis[j]-1)
	{
	  index[j] = 0;
	  index[j+1] += 1;
	}
	else
	  index[j] += 1;
      }
      for (k=0; k<arr->axis[1]; k++)
      {	
	drms_fprintfval_raw(keyfile, arr->type, p);
	p += sz;
	i++;
	for (j=1; j<arr->axis[0]; j++, i++)
	{
	  fprintf(keyfile, "%s",colsep);
	  drms_fprintfval_raw(keyfile, arr->type, p);
	  p += sz;
	}
	fprintf(keyfile,"%s",rowsep);
      }
    }
  }
}    




/*************** Careful array conversion routines. ********************/
int drms_array2char(int n, DRMS_Type_t src_type, double bzero, double bscale, 
		    void *src,  char *dst)
{
  int stat, i;
  double value;
  double rangechk;

  if (bzero == 0.0 && bscale==1.0)
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n);    
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{	  
	  if (*ssrc == DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_CHAR;	    
	  else if (!(*ssrc < SCHAR_MIN || *ssrc > SCHAR_MAX))
	    *dst = (char) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_CHAR;
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_CHAR;	    
	  else if (!(*ssrc < SCHAR_MIN || *ssrc > SCHAR_MAX))
	    *dst = (char) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_CHAR;
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
          if (*ssrc == DRMS_MISSING_LONGLONG)
            *dst = DRMS_MISSING_CHAR;       
          else if (!(*ssrc < SCHAR_MIN || *ssrc > SCHAR_MAX))
            *dst = (char) *ssrc;
          else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_CHAR;
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
          if (isnan(*ssrc))
            *dst = DRMS_MISSING_CHAR;       
          else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	       *dst = (char)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_CHAR;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
          if (*ssrc == DRMS_MISSING_TIME)
            *dst = DRMS_MISSING_CHAR;       
          else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	       *dst = (char)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_CHAR;
	     }
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
          if (isnan(*ssrc))
            *dst = DRMS_MISSING_CHAR;       
          else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	       *dst = (char)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_CHAR;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	double val;
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = strtod(*ssrc,&endptr);
	  if (val==0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst =  DRMS_MISSING_CHAR;
	  }
	  else
	  {
	     rangechk = round(val);
	     if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	       *dst = (char)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_CHAR;
	     }
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }

  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;

	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;

	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;

	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;

	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_CHAR;
	  else
	  {
	    value = bscale* *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SCHAR_MIN || rangechk > SCHAR_MAX))
	      *dst = (char)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_CHAR;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = strtod(*ssrc,&endptr);
	  if (value==0.0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst =  DRMS_MISSING_CHAR;
	  }
	  else 
	  {
	    value  = bscale * value + bzero;
	    rangechk = round(value);

	    if ( rangechk < SCHAR_MIN || rangechk > SCHAR_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_CHAR;
	    }
	    else
	      *dst = (char)rangechk;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  return stat;  
}



int drms_array2short(int n, DRMS_Type_t src_type, double bzero, double bscale, 
		     void *src, short *dst)
{
  double value;
  int stat, i;
  double rangechk;

  if (fabs(bzero)==0.0 && bscale==1.0)
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
          if (*ssrc == DRMS_MISSING_CHAR)
            *dst = DRMS_MISSING_SHORT;       
          else 	
	    *dst = (short) *ssrc;
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n*sizeof(short));    
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_INT)
            *dst = DRMS_MISSING_SHORT;       
	  else if (!(*ssrc < SHRT_MIN || *ssrc > SHRT_MAX))
	    *dst = (short) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_SHORT;
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_LONGLONG)
            *dst = DRMS_MISSING_SHORT;       
	  else 	  if (!(*ssrc < SHRT_MIN || *ssrc > SHRT_MAX))
	    *dst = (short) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_SHORT;
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_SHORT;       
	  else
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	       *dst = (short)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_SHORT;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_TIME)
            *dst = DRMS_MISSING_SHORT;       
	  else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	       *dst = (short)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_SHORT;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_SHORT;       
	  else
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	       *dst = (short)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_SHORT;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	double val;
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = strtod(*ssrc,&endptr);
	  if (val==0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_SHORT;
	  }
	  else
	  {
	     rangechk = round(val);
	     if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	       *dst = (short)rangechk;
	     else
	     {
		stat = DRMS_RANGE;
		*dst =  DRMS_MISSING_SHORT;
	     }
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_SHORT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < SHRT_MIN || rangechk > SHRT_MAX))
	      *dst = (short)round(rangechk);
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_SHORT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = strtod(*ssrc,&endptr);
	  if (value==0.0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_SHORT;
	  }
	  else 
	  {
	    value = bscale*value + bzero;
	    rangechk = round(value);

	    if (rangechk < SHRT_MIN || rangechk > SHRT_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_SHORT;
	    }
	    else
	      *dst = (short)rangechk;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  return stat;  
}



int drms_array2int(int n, DRMS_Type_t src_type, double bzero, double bscale,
		   void *src, int *dst)
{
  double value;
  int stat, i;
  double rangechk;

  if (fabs(bzero)==0.0 && bscale==1.0)
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_CHAR)
            *dst = DRMS_MISSING_INT;       
	  else 
	    *dst = (int) *ssrc;
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_SHORT)
            *dst = DRMS_MISSING_INT;       
	  else 
	    *dst = (int) *ssrc;
      }
      break;
    case DRMS_TYPE_INT:  
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n*sizeof(int));    
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_LONGLONG)
            *dst = DRMS_MISSING_INT;       
	  else if (!(*ssrc < INT_MIN || *ssrc > INT_MAX))
	    *dst = (int) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_INT;
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_INT;       
	  else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	       *dst = (int)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_INT;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_TIME)
            *dst = DRMS_MISSING_INT;       
	  else
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	       *dst = (int)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_INT;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_INT;       
	  else
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	       *dst = (int)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_INT;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	double val;
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = strtod(*ssrc,&endptr);
	  if (val==0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst =  DRMS_MISSING_INT;
	  }
	  else
	  {
	     rangechk = round(val);
	     if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	       *dst = (int)rangechk;
	     else
	     {
		stat = DRMS_RANGE;
		*dst =  DRMS_MISSING_INT;
	     }
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_INT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_INT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_INT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_INT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	  {
	    *dst = DRMS_MISSING_INT;
	  }
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_INT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_INT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < INT_MIN || rangechk > INT_MAX))
	      *dst = (int)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_INT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = strtod(*ssrc,&endptr);
	  if (value==0.0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_INT;
	  }
	  else 
	  {
	    value = bscale*value + bzero;
	    rangechk = round(value);

	    if ( rangechk < INT_MIN || rangechk > INT_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_INT;
	    }
	    else
	      *dst = (int)rangechk;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  return stat;  
}

int drms_array2longlong(int n, DRMS_Type_t src_type, double bzero, 
			double bscale, void *src, long long *dst)
{
  double value;
  int stat, i;
  double rangechk;

  if (fabs(bzero)==0.0 && bscale==1.0)
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_CHAR)
            *dst = DRMS_MISSING_LONGLONG;       
	  else 	 
	    *dst = (long long) *ssrc;
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_SHORT)
            *dst = DRMS_MISSING_LONGLONG;       
	  else 	
	    *dst = (long long) *ssrc;
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_INT)
            *dst = DRMS_MISSING_LONGLONG;       
	  else 
	    *dst = (long long) *ssrc;
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n*sizeof(long long));    
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_LONGLONG;       
	  else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	       *dst = (long long)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_LONGLONG;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_TIME)
            *dst = DRMS_MISSING_LONGLONG;       
	  else 
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	       *dst = (long long)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_LONGLONG;
	     }

	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_LONGLONG;       
	  else
	  {
	     rangechk = round(*ssrc);
	     if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	       *dst = (long long)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_LONGLONG;
	     }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	double val;
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = strtod(*ssrc,&endptr);
	  if (val==0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst =  DRMS_MISSING_LONGLONG;
	  }
	  else
	  {
	     rangechk = round(val);
	     if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	       *dst = (long long)rangechk;
	     else
	     {
		stat = DRMS_RANGE;  
		*dst = DRMS_MISSING_LONGLONG;
	     }
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_LONGLONG;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    rangechk = round(value);
	    if (!(rangechk < LLONG_MIN || rangechk > LLONG_MAX))
	      *dst = (long long)rangechk;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_LONGLONG;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = strtod(*ssrc,&endptr);
	  if (value==0.0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_LONGLONG;
	  }
	  else 
	  {
	    value = bscale*value + bzero;
	    rangechk = round(value);

	    if ( rangechk < LLONG_MIN || rangechk > LLONG_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_LONGLONG;
	    }
	    else
	      *dst = (long long)rangechk;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }

  return stat;  
}



int drms_array2float(int n, DRMS_Type_t src_type, double bzero, double bscale, 
		     void *src, float *dst)
{
  double value;
  int stat, i;

  if ( fabs(bzero)==0.0 && bscale==1.0 )
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_CHAR)
            *dst = DRMS_MISSING_FLOAT;       
	  else 	
	    *dst = (float) *ssrc;
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_SHORT)
            *dst = DRMS_MISSING_FLOAT;       
	  else 	
	    *dst = (float) *ssrc;
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_INT)
            *dst = DRMS_MISSING_FLOAT;       
	  else 	
	    *dst = (float) *ssrc;
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_LONGLONG)
            *dst = DRMS_MISSING_FLOAT;       
	  else 	
	    *dst = (float) *ssrc;
      }
      break;
    case DRMS_TYPE_FLOAT:
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n);    
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc == DRMS_MISSING_TIME)
            *dst = DRMS_MISSING_FLOAT;       
	  else 	if (!(*ssrc < -FLT_MAX || *ssrc > FLT_MAX))
	    *dst = (float) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_FLOAT;
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_FLOAT;       
	  else 	if (!(*ssrc < -FLT_MAX || *ssrc > FLT_MAX))
	    *dst = (float) *ssrc;
	  else
	  {
	    stat = DRMS_RANGE;  
	    *dst = DRMS_MISSING_FLOAT;
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	float val;
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = strtof(*ssrc, &endptr);
	  if (val==0 && endptr==*ssrc )	
	  {
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_FLOAT;
	  }
	  else if ( (val == HUGE_VALF || val == -HUGE_VALF) && errno==ERANGE)
	  {
	    stat = DRMS_RANGE;
	    *dst = DRMS_MISSING_FLOAT;
	  }
	  else 
	    *dst = val;
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_FLOAT;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -FLT_MAX || value > FLT_MAX))
	      *dst = (float) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_FLOAT;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = strtod(*ssrc,&endptr);
	  if (value==0.0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_FLOAT;
	  }
	  else if ((value == HUGE_VALF || value == -HUGE_VALF) && errno==ERANGE)
	  {
	    stat = DRMS_RANGE;
	    *dst = DRMS_MISSING_FLOAT;
	  }
	  else 
	  {
	    value = bscale*value + bzero;
	    if ( value < -FLT_MAX || value > FLT_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_FLOAT;
	    }
	    else
	      *dst = (float) value;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  return stat;  
}


int drms_array2double(int n, DRMS_Type_t src_type, double bzero, double bscale,
		      void *src, double *dst)
{
  long double value;
  int stat, i;

  if (fabs(bzero)==0.0 && bscale==1.0)
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_CHAR)
            *dst = DRMS_MISSING_DOUBLE;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_SHORT)
            *dst = DRMS_MISSING_DOUBLE;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_INT)
            *dst = DRMS_MISSING_DOUBLE;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_LONGLONG)
            *dst = DRMS_MISSING_DOUBLE;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_DOUBLE;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_TIME)
            *dst = DRMS_MISSING_DOUBLE;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_DOUBLE: 	
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n*sizeof(double));
      break;
    case DRMS_TYPE_STRING: 
      {
	double val;
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = strtod(*ssrc, &endptr);
	  if (val==0 && endptr==*ssrc )	
	  {
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_DOUBLE;
	  }
	  else if ( (val == HUGE_VAL || val == -HUGE_VAL) && errno==ERANGE)
	  {
	    stat = DRMS_RANGE;
	    *dst = DRMS_MISSING_DOUBLE;
	  }
	  else 
	    *dst = val;
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_DOUBLE;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_DOUBLE;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_DOUBLE;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_DOUBLE;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_DOUBLE;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_DOUBLE;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_DOUBLE;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_DOUBLE;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_DOUBLE;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_DOUBLE;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_DOUBLE;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_DOUBLE;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char *endptr;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = strtold(*ssrc,&endptr);
	  if (value==0.0 && endptr==*ssrc )	
	  {	  
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_DOUBLE;
	  }
	  else if ((value == HUGE_VAL || value == -HUGE_VAL) && errno==ERANGE)
	  {
	    stat = DRMS_RANGE;
	    *dst = DRMS_MISSING_DOUBLE;
	  }
	  else 
	  {
	    value = bscale*value + bzero;
	    if ( value < -DBL_MAX || value > DBL_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_DOUBLE;
	    }
	    else
	      *dst = (double) value;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  return stat;  
}



int drms_array2time(int n, DRMS_Type_t src_type, double bzero, double bscale, 
		    void *src, double *dst)
{
  long double value;
  int stat, i;

  if (fabs(bzero)==0.0 && bscale==1.0)
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_CHAR)
            *dst = DRMS_MISSING_TIME;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_SHORT)
            *dst = DRMS_MISSING_TIME;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_INT)
            *dst = DRMS_MISSING_TIME;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (*ssrc == DRMS_MISSING_LONGLONG)
            *dst = DRMS_MISSING_TIME;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_TIME;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_TIME: 
      stat = DRMS_SUCCESS;
      memcpy(dst,src,n*sizeof(double));
      break;
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	  if (isnan(*ssrc))
            *dst = DRMS_MISSING_TIME;       
	  else 
	    *dst = (double) *ssrc;
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	double val;
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  val = sscan_time(*ssrc);
	  if (val < 0)
	  {
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_TIME;
	  }
	  else
	    *dst = val;
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }
  else
  {
    switch(src_type)
    {
    case DRMS_TYPE_CHAR:       
      {
	char *ssrc = (char *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_CHAR)
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_SHORT:
      {
	short *ssrc = (short *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_SHORT)
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_INT:  
      {
	int *ssrc = (int *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_INT)
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_LONGLONG:  
      {
	long long *ssrc = (long long *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_LONGLONG)
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_FLOAT:
      {
	float *ssrc = (float *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_TIME: 
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (*ssrc==DRMS_MISSING_TIME)
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
    case DRMS_TYPE_DOUBLE: 	
      {
	double *ssrc = (double *) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  if (isnan(*ssrc))
	    *dst = DRMS_MISSING_TIME;
	  else
	  {
	    value = bscale * *ssrc + bzero;
	    if (!(value < -DBL_MAX || value > DBL_MAX))
	      *dst = (double) value;
	    else
	    {
	      stat = DRMS_RANGE;  
	      *dst = DRMS_MISSING_TIME;
	    }
	  }
	}
      }
      break;
    case DRMS_TYPE_STRING: 
      {
	char **ssrc = (char **) src;
	stat = DRMS_SUCCESS;
	for (i=0; i<n; i++, ssrc++, dst++)
	{
	  value = (long double)sscan_time(*ssrc);
	  if (value < 0)
	  {
	    stat = DRMS_BADSTRING;
	    *dst = DRMS_MISSING_TIME;
	  }
	  else 
	  {
	    value = bscale*value + bzero;
	    if ( value < -DBL_MAX || value > DBL_MAX)
	    {
	      stat = DRMS_RANGE;
	      *dst =  DRMS_MISSING_TIME;
	    }
	    else
	      *dst = (double) value;
	  }
	}
      }
      break;
    default:
      stat = DRMS_ERROR_INVALIDTYPE;
    }
  }

  return stat;  
}


int drms_array2string(int n, DRMS_Type_t src_type, double bzero, double bscale,
		      void *src,  char **dst)
{
  int stat,i;
  char *result;
  
  result = NULL;
  stat = DRMS_SUCCESS;
  dst = malloc(n*sizeof(char *));
  XASSERT(dst);  
  switch(src_type)
  {
  case DRMS_TYPE_CHAR:       
    { char *ssrc = (char *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
         *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (*ssrc==DRMS_MISSING_CHAR)
	  **dst=0;
	else
	  CHECKSNPRINTF(snprintf(*dst, 2, "%24.17lg", bscale* *ssrc + bzero), 2);
      }
    }
    break;
  case DRMS_TYPE_SHORT:
    { short *ssrc = (short *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
        *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (*ssrc==DRMS_MISSING_SHORT)
	  **dst=0;
	else
	  CHECKSNPRINTF(snprintf(*dst, 2, "%24.17lg", bscale* *ssrc + bzero), 2);
      }
    }
    break;
  case DRMS_TYPE_INT:  
    { int *ssrc = (int *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
        *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (*ssrc==DRMS_MISSING_INT)
	  **dst=0;
	else
	  CHECKSNPRINTF(snprintf(*dst, 2, "%24.17lg", bscale* *ssrc + bzero), 2);
      }
    }
    break;
  case DRMS_TYPE_LONGLONG:  
    { long long *ssrc = (long long *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
        *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (*ssrc==DRMS_MISSING_LONGLONG)
	  **dst=0;
	else
	  CHECKSNPRINTF(snprintf(*dst, 2, "%24.17lg", bscale* *ssrc + bzero), 2);
      }
    }
    break;
  case DRMS_TYPE_FLOAT:
    { float *ssrc = (float *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
        *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (isnan(*ssrc))
	  **dst=0;
	else
	  CHECKSNPRINTF(snprintf(*dst, 2, "%24.17lg", bscale* *ssrc + bzero), 2);
      }
    }
    break;
  case DRMS_TYPE_DOUBLE: 	
    { double *ssrc = (double *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
        *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (isnan(*ssrc))
	  **dst=0;
	else
	  CHECKSNPRINTF(snprintf(*dst, 2, "%24.17lg", bscale* *ssrc + bzero), 2);
      }
    }
    break;
  case DRMS_TYPE_TIME: 
    { double *ssrc = (double *) src;
      for (i=0; i<n; i++, ssrc++, dst++)
      {
        *dst = malloc(DRMS_ARRAY2STRING_LEN);
        XASSERT(*dst);
	if (*ssrc==DRMS_MISSING_TIME)
	  **dst=0;
	else
	  sprint_time(*dst, bscale * *ssrc + bzero, "TAI", 0);
      }
    }
    break;
  case DRMS_TYPE_STRING:
    { char **ssrc = (char **) src;
      for (i=0; i<n; i++, ssrc++, dst++)	
	copy_string(dst, *ssrc);
    }
  default:
    stat = DRMS_ERROR_INVALIDTYPE;
  }
  return stat;  
}
