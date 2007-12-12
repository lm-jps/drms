/* drms_array.h */

/**
@file drms_array.h
@brief Functions to access DRMS array data structures.
@sa drms_record.h drms_keyword.h drms_link.h drms_segment.h drms_types.h
*/

#ifndef DRMS_ARRAY_H
#define DRMS_ARRAY_H

#include "drms_statuscodes.h"
#include "drms_array.h"
#include "drms_priv.h"


#define DRMS_ARRAY2STRING_LEN 30   // use in drms_array2string
                                   // default str len conversion
                                   // ISS 14-MAY-2007

/* MACROS to fetch/store array values that avoid overhead associated 
 * with calling a function.  To be used in data iteration loops. 
*/

/* VAL - Output value (DRMS_Value_t)
 * X   - Input (DRMS_Array_t *)
 * Y   - Index into array of data (int)
 */
#define DRMS_ARRAY_GETVAL(VAL, X, Y)                                 \
{                                                                    \
   int gvErr = 0;                                                    \
   switch (X->type)                                                  \
   {                                                                 \
      case DRMS_TYPE_CHAR:                                           \
	VAL.value.char_val = *((char *)X->data + Y);                 \
	break;                                                       \
      case DRMS_TYPE_SHORT:                                          \
	VAL.value.short_val = *((short *)X->data + Y);               \
	break;                                                       \
      case DRMS_TYPE_INT:                                            \
	VAL.value.int_val = *((int *)X->data + Y);                   \
	break;                                                       \
      case DRMS_TYPE_LONGLONG:                                       \
	VAL.value.longlong_val = *((long long *)X->data + Y);        \
	break;                                                       \
      case DRMS_TYPE_FLOAT:                                          \
	VAL.value.float_val = *((float *)X->data + Y);               \
	break;                                                       \
      case DRMS_TYPE_DOUBLE:                                         \
	VAL.value.double_val = *((double *)X->data + Y);             \
	break;                                                       \
      case DRMS_TYPE_TIME:                                           \
	VAL.value.time_val = *((double *)X->data + Y);               \
	break;                                                       \
      case DRMS_TYPE_STRING:                                         \
	VAL.value.string_val = strdup((char *)X->data + Y);          \
	break;                                                       \
      default:                                                       \
	fprintf(stderr, "Invalid drms type: %d\n", (int)X->type);    \
	gvErr = 1;                                                   \
   }                                                                 \
   if (!gvErr)                                                       \
   {                                                                 \
      VAL.type = X->type;                                            \
   }                                                                 \
}

/* VAL - Input value (DRMS_Value_t)
 * X   - Input (DRMS_Array_t *)
 * Y   - Index into array of data (int)
 */
#define DRMS_ARRAY_SETVAL(VAL, X, Y)                                    \
{                                                                       \
   if (VAL.type == X->type)                                             \
   {                                                                    \
      switch (X->type)                                                  \
      {                                                                 \
         case DRMS_TYPE_CHAR:                                           \
	   *((char *)X->data + Y) = VAL.value.char_val;                 \
	   break;                                                       \
         case DRMS_TYPE_SHORT:                                          \
	   *((short *)X->data + Y) = VAL.value.short_val;               \
   	   break;                                                       \
         case DRMS_TYPE_INT:                                            \
	   *((int *)X->data + Y) = VAL.value.int_val;                   \
           break;                                                       \
         case DRMS_TYPE_LONGLONG:                                       \
	   *((long long *)X->data + Y) = VAL.value.longlong_val;        \
	   break;                                                       \
         case DRMS_TYPE_FLOAT:                                          \
	   *((float *)X->data + Y) = VAL.value.float_val;               \
	   break;                                                       \
         case DRMS_TYPE_DOUBLE:                                         \
	   *((double *)X->data + Y) = VAL.value.double_val;             \
	   break;                                                       \
         case DRMS_TYPE_TIME:                                           \
	   *((double *)X->data + Y) = VAL.value.time_val;               \
	   break;                                                       \
         case DRMS_TYPE_STRING:                                         \
         {                                                              \
           char **pStr = ((char **)X->data + Y);                        \
           *pStr = strdup(VAL.value.string_val);                        \
	   break;                                                       \
	 }                                                              \
         default:                                                       \
	   fprintf(stderr, "Invalid drms type: %d\n", (int)X->type);    \
      }                                                                 \
   }									\
}

/* drms_array_set(array, value, i1, i2, ..., in); */
static inline void drms_array_setv(DRMS_Array_t *arr, ...)
{
   /* XXX Not implemented - unused arr causes compiler warning */
   if (arr)
   {

   }
}


/* Compute offset (in bytes) into data array of element with indices
   given in index argument. */
static inline int drms_array_offset(DRMS_Array_t *arr, int *indexarr)
{
  int i, idx;
  
  for (i=0, idx=0; i<arr->naxis; i++)
    idx += indexarr[i]*arr->dope[i];
  return idx;
}

/* Calculate the number of entries in an n-dimensional array. */
static inline long long drms_array_count(DRMS_Array_t *arr)
{
  int i;
  long long n;
  
  n=1;
  for (i=0; i<arr->naxis; i++)
    n *= arr->axis[i];
  return n;
}
/* Calculate the size in bytes of an n-dimensional array. */
static inline long long drms_array_size(DRMS_Array_t *arr)
{
   int size = drms_sizeof(arr->type);
   long long count = drms_array_count(arr);
   return size * count;
}

/* Returned the number of axes in a multi-dimensional array. */
static inline int drms_array_naxis(DRMS_Array_t *arr)
{
  return arr->naxis;
}

/* Return the number of entries along the n'th axis of a 
   multi-dimensional array. */
static inline int drms_array_nth_axis(DRMS_Array_t *arr, int n)
{
  if (n<arr->naxis)
    return arr->axis[n];
  else
    return DRMS_ERROR_INVALIDDIMS;
}

/* index is into a unidimensional data array */
/* converts src to arr->type if necessary */
static inline int drms_array_setext(DRMS_Array_t *arr, long long index, DRMS_Value_t *src)
{
   int status;
   DRMS_Type_t srctype = src->type;

   switch(arr->type)
   {
      case DRMS_TYPE_CHAR: 
	{ 
	   char *p = arr->data;
	   if (srctype != DRMS_TYPE_CHAR)
	   {
	      p[index] = drms2char(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).char_val;
	   }
	}
	break;
      case DRMS_TYPE_SHORT:
	{ 
	   short *p = arr->data;
	   if (srctype != DRMS_TYPE_SHORT)
	   {
	      p[index] = drms2short(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).short_val;
	   }
	}
	break;
      case DRMS_TYPE_INT:  
	{ 
	   int *p = arr->data;
	   if (srctype != DRMS_TYPE_INT)
	   {
	      p[index] = drms2int(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).int_val;
	   }
	}
	break;
      case DRMS_TYPE_LONGLONG:  
	{ 
	   long long *p = arr->data;
	   if (srctype != DRMS_TYPE_LONGLONG)
	   {
	      p[index] = drms2longlong(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).longlong_val;
	   }
	}
	break;
      case DRMS_TYPE_FLOAT:
	{ 
	   float *p = arr->data;
	   if (srctype != DRMS_TYPE_FLOAT)
	   {
	      p[index] = drms2float(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).float_val;
	   }
	}
	break;
      case DRMS_TYPE_TIME: 
	{ 
	   double *p = arr->data;
	   if (srctype != DRMS_TYPE_TIME)
	   {
	      p[index] = drms2time(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).time_val;
	   }
	}
	break;
      case DRMS_TYPE_DOUBLE: 	
	{ 
	   double *p = arr->data;
	   if (srctype != DRMS_TYPE_DOUBLE)
	   {
	      p[index] = drms2double(srctype, &(src->value), &status);
	   }
	   else
	   {
	      p[index] = (src->value).double_val;
	   }
	}
	break;
      case DRMS_TYPE_STRING: 
	{
	   char **p  = ((char **) arr->data) + index;
	   if (*p)
	     free(*p);
	   *p = drms2string(srctype, &(src->value), &status);
	}
	break;
      default:
	fprintf(stderr, "ERROR: Unhandled DRMS type %d\n",(int)arr->type);
	XASSERT(0);
   }

   return status;
}

static inline int drms_array_set(DRMS_Array_t *arr, int *indexarr, DRMS_Value_t *src)
{
  int i;
  i = drms_array_offset(arr,indexarr);
  return drms_array_setext(arr, i, src);
}

static inline int drms_array_setchar_ext(DRMS_Array_t *arr, long long index, char value)
{
   if (arr->type == DRMS_TYPE_CHAR)
   {
      char *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setchar(DRMS_Array_t *arr, int *indexarr, char value)
{
  if (arr->type == DRMS_TYPE_CHAR)
  {
     int index = drms_array_offset(arr, indexarr);
     char *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setshort_ext(DRMS_Array_t *arr, long long index, short value)
{
   if (arr->type == DRMS_TYPE_SHORT)
   {
      short *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setshort(DRMS_Array_t *arr, int *indexarr, short value)
{
  if (arr->type == DRMS_TYPE_SHORT)
  {
     int index = drms_array_offset(arr, indexarr);
     short *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setint_ext(DRMS_Array_t *arr, long long index, int value)
{
   if (arr->type == DRMS_TYPE_INT)
   {
      int *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setint(DRMS_Array_t *arr, int *indexarr, int value)
{
  if (arr->type == DRMS_TYPE_INT)
  {
     int index = drms_array_offset(arr, indexarr);
     int *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setlonglong_ext(DRMS_Array_t *arr, long long index, long long value)
{
   if (arr->type == DRMS_TYPE_LONGLONG)
   {
      long long *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setlonglong(DRMS_Array_t *arr, int *indexarr, long long value)
{
  if (arr->type == DRMS_TYPE_LONGLONG)
  {
     int index = drms_array_offset(arr, indexarr);
     long long *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setfloat_ext(DRMS_Array_t *arr, long long index, float value)
{
   if (arr->type == DRMS_TYPE_FLOAT)
   {
      float *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setfloat(DRMS_Array_t *arr, int *indexarr, float value)
{
  if (arr->type == DRMS_TYPE_FLOAT)
  {
     int index = drms_array_offset(arr, indexarr);
     float *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setdouble_ext(DRMS_Array_t *arr, long long index, double value)
{
   if (arr->type == DRMS_TYPE_DOUBLE)
   {
      double *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setdouble(DRMS_Array_t *arr, int *indexarr, double value)
{
  if (arr->type == DRMS_TYPE_DOUBLE)
  {
     int index = drms_array_offset(arr, indexarr);
     double *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_settime_ext(DRMS_Array_t *arr, long long index, double value)
{
   if (arr->type == DRMS_TYPE_TIME)
   {
      double *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_settime(DRMS_Array_t *arr, int *indexarr, double value)
{
  if (arr->type == DRMS_TYPE_TIME)
  {
     int index = drms_array_offset(arr, indexarr);
     double *p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setstring_ext(DRMS_Array_t *arr, long long index, char *value)
{
   if (arr->type == DRMS_TYPE_STRING)
   {
      char **p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

static inline int drms_array_setstring(DRMS_Array_t *arr, int *indexarr, char *value)
{
  if (arr->type == DRMS_TYPE_STRING)
  {
     int index = drms_array_offset(arr, indexarr);
     char **p = arr->data;
     p[index] = value;
     return DRMS_SUCCESS;
  }

   return DRMS_ERROR_INVALIDDATA;
}

DRMS_Array_t *drms_array_permute(DRMS_Array_t *src, int *perm, int *status);

void drms_array_print(DRMS_Array_t *arr, const char *colsep, 
		      const char *rowsep);

DRMS_Array_t *drms_array_create(DRMS_Type_t type, int naxis, 
				int *axis, void *data, int *status);
void drms_free_array(DRMS_Array_t *src);

DRMS_Array_t *drms_array_convert(DRMS_Type_t dsttype, double bzero, 
				 double bscale, DRMS_Array_t *src);

void drms_array_convert_inplace(DRMS_Type_t newtype, double bzero, 
				double bscale, DRMS_Array_t *src);

void drms_array2missing(DRMS_Array_t *arr);

DRMS_Array_t *drms_array_slice(int *start, int *end, DRMS_Array_t *src);


/**** Internal functions (not for modules) ****/

int drms_array_rawconvert(int n, DRMS_Type_t dsttype, double bzero, 
			  double bscale, void *dst, DRMS_Type_t srctype, 
			  void *src);

/* Low level array conversion functions. */

/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to \a char values
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the \a char datatype, they are replaced by the \a char miss-
   ing value; otherwise, the scaled values are
   placed in dst.  (This is almost certainly a bug!) If src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character repre-
   sentations of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the \a char datatype, they are placed
   in  dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2char(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, char *dst);
/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to \a short values
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the \a short datatype, they are replaced by the \a short miss-
   ing value; otherwise, the scaled values are
   placed in dst.  (This is almost certainly a bug!) If src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character repre-
   sentations of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the \a short datatype, they are placed
   in  dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2short(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, short *dst);

/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to \a int values
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the \a int datatype, they are replaced by the \a int miss-
   ing value; otherwise, the scaled values are
   placed in dst.  (This is almost certainly a bug!) If src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character repre-
   sentations of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the \a int datatype, they are placed
   in  dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2int(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, int *dst);

/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to \a longlong values
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the \a longlong datatype, they are replaced by the \a longlong miss-
   ing value; otherwise, the scaled values are
   placed in dst.  (This is almost certainly a bug!) If src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character repre-
   sentations of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the \a longlong datatype, they are placed
   in  dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2longlong(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, long long *dst);

/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to \a float values
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the \a float datatype, they are replaced by the \a float miss-
   ing value; otherwise, the scaled values are
   placed in dst.  (This is almost certainly a bug!) If src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character repre-
   sentations of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the \a float datatype, they are placed
   in  dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2float(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, float *dst);

/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to \a double values
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the \a double datatype, they are replaced by the \a double miss-
   ing value; otherwise, the scaled values are
   placed in dst.  (This is almost certainly a bug!) If src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character repre-
   sentations of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the \a double datatype, they are placed
   in  dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2double(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, double *dst);


/**
   Converts the first \a n values of \a src, interpreted as
   being of type \a src_type, to \a doubles, scaling them by \a bzero and \a bscale.
   If the resulting scaled numeric values are outside the representable
   range for the type, the value is replaced by DRMS_MISSING_TIME. If the
   data are of type ::DRMS_TYPE_STRING, then they are scanned by ::sscan_time,
   then scaled, and then checked for validity as \a doubles.

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2time(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, double *dst);

/**
   Converts the first \a n values of \a src, interpreted as
   being of type \a src_type, to strings which are placed in the array \a dst
   using a \a sprintf function  with a %24.17lg format, if the type is
   numeric.  If  the  type is ::DRMS_TYPE_TIME, the \a sprint_time function is
   used, with a TAI representation accurate to the nearest second. If  the
   type  is  ::DRMS_TYPE_STRING,  the strings are simply copied. For numeric
   types (including ::DRMS_TYPE_TIME), the data are first  scaled  by  \a bzero
   and \a bscale before being printed to strings. Data with values represent-
   ing missing data are represented by single-character null terminators;
   however,  no  check is performed on whether the scaling would result in
   valid data for the type.

   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2string(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, char **dst);



#endif
