/* drms_array.h */

/**
@file drms_array.h
@brief Functions to access DRMS array data structures.
@sa drms_record.h drms_keyword.h drms_link.h drms_segment.h drms_types.h
*/

#ifndef DRMS_ARRAY_H
#define DRMS_ARRAY_H

#include "drms_statuscodes.h"
#include "drms_priv.h"

#define INLINE static inline

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
INLINE void drms_array_setv(DRMS_Array_t *arr, ...)
{
   /* XXX Not implemented - unused arr causes compiler warning */
   if (arr)
   {

   }
}

/**
   @name Information and Diagnostics
*/
/* @{ */
/* Compute offset (in bytes) into data array of element with indices
   given in index argument. */
/**
   Returns the offset, in bytes, from the start of the
   array @a arr->data of the datum at the coordinate value specified by the
   index array @a indexarr, which must be of dimension @a arr->naxis
   (at least).

   @param arr The DRMS array struct for which the offset is being calculated.
   @param indexarr The index into @a arr of the datum for which the offset 
   is being calculated.
   @return The bytes offset of the datum at @a indexarr.
*/
INLINE int drms_array_offset(DRMS_Array_t *arr, int *indexarr)
{
  int i, idx;
  
  for (i=0, idx=0; i<arr->naxis; i++)
    idx += indexarr[i]*arr->dope[i];
  return idx;
}

/**
   Prints the values of the data in @a arr->data 
   in tabular form, with colums separated by the string @a colsep and
   rows separated by the string @a rowsep. One-dimensional arrays are
   printed as a single row, two-dimansional arrays in column-major @a i.e.
   storage order. Arrays of higher dimension are printed as successive tables,
   each labeled by a header line giving the array index or indices for the
   corresponding dimension(s) above the 2nd.

   @param arr The DRMS array struct whose data values are being printed.
   @param colsep A string that will be printed between columns of output.
   @param rowsep A string that will be printed between rows of output.
*/
void drms_array_print(DRMS_Array_t *arr, const char *colsep, 
		      const char *rowsep);

/* Calculate the number of entries in an n-dimensional array. */
/**
   Returns the total number of data points in the
   array @a arr->data, i.e. the product of @a arr->axis[i].

   @param arr The DRMS array struct whose data points are to be counted.
   @return The number of data points in @a arr.
*/
INLINE long long drms_array_count(DRMS_Array_t *arr)
{
  int i;
  long long n;
  
  n=1;
  for (i=0; i<arr->naxis; i++)
    n *= arr->axis[i];
  return n;
}
/* Calculate the size in bytes of an n-dimensional array. */
/**
   Returns the total size in bytes of the data array
   @a arr->data, i.e. the product of ::drms_array_count and
   ::drms_sizeof(@a arr->type).

   @param arr The DRMS array struct whose size is being calculated.
   @return The byte size of @a arr.
*/
INLINE long long drms_array_size(DRMS_Array_t *arr)
{
   int size = drms_sizeof(arr->type);
   long long count = drms_array_count(arr);
   return size * count;
}

/* Returned the number of axes in a multi-dimensional array. */
/**
   Returns the rank of the array, @a arr->naxis.

   @param arr The DRMS array struct whose number of axes is being counted.
   @return The number of axes in @a arr.
*/
INLINE int drms_array_naxis(DRMS_Array_t *arr)
{
  return arr->naxis;
}

/* Return the number of entries along the n'th axis of a 
   multi-dimensional array. */
/**
   Returns the dimension of axis @a n of the array, @a arr->axis[n].

   @param arr The DRMS array struct whose dimension length is being returned.
   @param n The axis number of @a arr whose length is being returned.
   @return The length of axis @a n.
*/
INLINE int drms_array_nth_axis(DRMS_Array_t *arr, int n)
{
  if (n<arr->naxis)
    return arr->axis[n];
  else
    return DRMS_ERROR_INVALIDDIMS;
}
/* @} */

/**
   @name Filling
*/
/* @{ */
/**
   Set an array element in manner similar to ::drms_array_set and
   ::drms_array_setext, except do not perform a type conversion;
   the array data type @a arr->type must match
   the type of @a value.

   @param arr  The DRMS array struct whose array element is set.
   @param index The index of the array element whose value is set.
   @param value The to value which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_setchar_ext(DRMS_Array_t *arr, long long index, char value)
{
   if (arr->type == DRMS_TYPE_CHAR)
   {
      char *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

INLINE int drms_array_setchar(DRMS_Array_t *arr, int *indexarr, char value)
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

INLINE int drms_array_setshort_ext(DRMS_Array_t *arr, long long index, short value)
{
   if (arr->type == DRMS_TYPE_SHORT)
   {
      short *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

INLINE int drms_array_setshort(DRMS_Array_t *arr, int *indexarr, short value)
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

INLINE int drms_array_setint_ext(DRMS_Array_t *arr, long long index, int value)
{
   if (arr->type == DRMS_TYPE_INT)
   {
      int *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

INLINE int drms_array_setint(DRMS_Array_t *arr, int *indexarr, int value)
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

INLINE int drms_array_setlonglong_ext(DRMS_Array_t *arr, long long index, long long value)
{
   if (arr->type == DRMS_TYPE_LONGLONG)
   {
      long long *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

INLINE int drms_array_setlonglong(DRMS_Array_t *arr, int *indexarr, long long value)
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

INLINE int drms_array_setfloat_ext(DRMS_Array_t *arr, long long index, float value)
{
   if (arr->type == DRMS_TYPE_FLOAT)
   {
      float *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

INLINE int drms_array_setfloat(DRMS_Array_t *arr, int *indexarr, float value)
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

INLINE int drms_array_setdouble_ext(DRMS_Array_t *arr, long long index, double value)
{
   if (arr->type == DRMS_TYPE_DOUBLE)
   {
      double *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

INLINE int drms_array_setdouble(DRMS_Array_t *arr, int *indexarr, double value)
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

/**
   Set an array element in manner similar to ::drms_array_set and
   ::drms_array_setext, except do not perform a type conversion;
   the array data type @a arr->type must be ::DRMS_TYPE_TIME).

   @param arr  The DRMS array struct whose array element is set.
   @param index The index of the array element whose value is set.
   @param value The time to which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_settime_ext(DRMS_Array_t *arr, long long index, double value)
{
   if (arr->type == DRMS_TYPE_TIME)
   {
      double *p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

/**
   Set an array element in manner similar to ::drms_array_set and
   ::drms_array_setext, except do not perform a type conversion;
   the array data type @a arr->type must be ::DRMS_TYPE_TIME).

   @param arr  The DRMS array struct whose array element is set.
   @param index The index of the array element whose value is set.
   @param value The to time which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_settime(DRMS_Array_t *arr, int *indexarr, double value)
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

/**
   Set an array element in manner similar to ::drms_array_set and
   ::drms_array_setext, except do not perform a type conversion;
   the array data type @a arr->type must match
   the type of @a value.  This function places the
   start address of the string in the array, it does not do a string
   copy.

   @param arr  The DRMS array struct whose array element is set.
   @param index The index of the array element whose value is set.
   @param value The string to which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_setstring_ext(DRMS_Array_t *arr, long long index, char *value)
{
   if (arr->type == DRMS_TYPE_STRING)
   {
      char **p = arr->data;
      p[index] = value;
      return DRMS_SUCCESS;
   }
   
   return DRMS_ERROR_INVALIDDATA;
}

/**
   Set an array element in manner similar to ::drms_array_set and
   ::drms_array_setext, except do not perform a type conversion;
   the array data type @a arr->type must match
   the type of @a value.  This function places the
   start address of the string in the array, it does not do a string
   copy.

   @param arr  The DRMS array struct whose array element is set.
   @param index The index of the array element whose value is set.
   @param value The string to which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_setstring(DRMS_Array_t *arr, int *indexarr, char *value)
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

/* index is into a unidimensional data array */
/* converts src to arr->type if necessary */
/**
   Sets the value of the array element indexed by
   @a index from the start of the array @a arr->data (in units of the
   size of the array data type) to the value @a src, converted to the type
   @a src->type by @a drms2* as necessary.

   @param arr The DRMS array struct whose array element is set.
   @param index The index of the array element whose value is set.
   @param src The to which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_setext(DRMS_Array_t *arr, long long index, DRMS_Value_t *src)
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


/**
   Sets the value of the @a arr->data array element
   indexed by the vector @a indexarr, of length @a arr->naxis (at least)
   to the value @a src, converted to the type 
   @a src->type by @a drms2* as necessary.

   @param arr The DRMS array struct whose array element is set.
   @param indexarr The array of index values that specify an 
   array element whose value is set.
   @param src The to which the designed array element is set.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
INLINE int drms_array_set(DRMS_Array_t *arr, int *indexarr, DRMS_Value_t *src)
{
  int i;
  i = drms_array_offset(arr,indexarr);
  return drms_array_setext(arr, i, src);
}

/**
   Sets all the values in the array @a arr->data
   to the DRMS entity representing missing (fill) data for the data type
   @a arr->type, for example ::DRMS_MISSING_SHORT or ::DRMS_MISSING_DOUBLE.
   If the data type is ::DRMS_TYPE_STRING, the values are set to single-byte
   null terminators. Note that the value defined for ::DRMS_MISSING_TIME is in
   fact a valid double-precision number, and that this value is defined
   differently from the definitions used for representation of invalid
   strings in ::sscan_time.

   @param arr The DRMS array struct whose values are set to missing.
*/
void drms_array2missing(DRMS_Array_t *arr);
/* @} */

/**
   @name Creation and Destruction
*/
/* @{ */
/**
   Creates a ::DRMS_Array_t struct with the specified
   values for the elements @a type, @a naxis, and @a axis. The array
   @a axis must be of length @a naxis (at least) unless it is NULL.
   If @a data is
   a nun-NULL pointer, the data pointer of the created array is set to it;
   otherwise, a data space of appropriate size is malloc'd (but left with
   unknown contents). @a type cannot be ::DRMS_TYPE_RAW. No checks are made
   for legitimate (positive) values of @a axis[n]. None of the other
   elements of the struct are filled except @a dope.

   @param type The data type of the DRMS array struct to create.
   @param naxis The number of axes of the DRMS array struct to create.
   @param axis The lengths of each axis of the DRMS array struct to create.
   @param data A linear array of data values, of type @a type, which
   are "stolen" by the DRMS array struct being created.
   @param DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
   @return The created DRMS array struct.
*/
DRMS_Array_t *drms_array_create(DRMS_Type_t type, int naxis, 
				int *axis, void *data, int *status);

/**
   Frees the array struct @a src as well as its 
   member @a src->data as necessary.

   IMPORTANT NOTE: Since this function frees @a src->data, do not free @a src->data 
   before or after calling this function.

   @param src The DRMS array struct to free.
*/
void drms_free_array(DRMS_Array_t *src);
/* @} */


/**
   @name Slicing and Permutation
*/
/* @{ */
/**
   Returns a newly created array struct (created
   with ::drms_array_create, q.v.) in which the data element
   corresponds to a subset hypercube of dimension @a src->naxis with
   only the index values for dimension @a n in the closed range
   [@a start[n], @a end[n]]. The extraction is performed by the function
   ::ndim_pack.

   @param start The index value of the dimension (orthogonal to the slice) that
   marks the start of the slice.
   @param end The index value of the dimension (orthogonal to the slice) that
   marks the end of the slice.
   @param src The DRMS array struct from which a slice is to be extracted.
   @return The created DRMS array struct.
*/
DRMS_Array_t *drms_array_slice(int *start, int *end, DRMS_Array_t *src);
/**
   Rearranges the array elements in @a arr->data
   such that the dimensions are ordered according to the permutation given
   in the vector @a perm (dimension @a arr->naxis), using the function
   @a ndim_perm. This is a generalization of the matrix transpose operator
   to n dimensions. The permuted data are returned in a newly created
   array struct, created wih ::drms_array_create.

   @param src The DRMS array struct whose data will be transposed.
   @param perm Vector specifying the order in which data should be transposed.
   @param status DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
   @return The created DRMS array struct.
*/
DRMS_Array_t *drms_array_permute(DRMS_Array_t *src, int *perm, int *status);
/* @} */

/* Low level array conversion functions. */
/**
@name Scaling and Type Conversion
*/
/* @{ */
/**
   Convert the first \a n values of \a src,
   interpreted as being of type \a src_type, to 
   values of the destination data type (the data type of \a dst)
   and place the values in \a dst. The values
   are first scaled by \a bzero and \a bscale.  If the scaled values are not within
   the range representable by the destination datatype, they are replaced by the 
   destination-datatype-specific missing value; otherwise, the scaled values are
   placed in \a dst.  (This is almost certainly a bug!) If \a src_type is
   ::DRMS_TYPE_STRING, the data strings are interpreted as character representations 
   of numbers with \a strtod, and then scaled. If the resulting
   values are within the representable range for the destination datatype, they are placed
   in \a dst, otherwise the appropriate missing value is used. (This appears
   to be the only case for which these functions behave as expected.)

   @param n Number of values to convert.
   @param src_type Source datatype (see ::DRMS_Type_t).
   @param bzero Offset by which raw data values are to be shifted to 
   produce actual data values.
   @param bscale Scaling factor by which raw data values are to be multiplied to 
   produce actual data values.
   @param src Source data, which is of type \a srctype.
   @param dst Contiguous array of the destination data type into which the converted
   data are stored upon success.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2char(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, char *dst);
int drms_array2short(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, short *dst);
int drms_array2int(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, int *dst);
int drms_array2longlong(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, long long *dst);
int drms_array2float(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, float *dst);
int drms_array2double(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, double *dst);

/**
   Converts the first \a n values of \a src, interpreted as
   being of type \a src_type, to \a doubles, scaling them by \a bzero and \a bscale.
   If the resulting scaled numeric values are outside the representable
   range for the type, the value is replaced by ::DRMS_MISSING_TIME. If the
   data are of type ::DRMS_TYPE_STRING, then they are scanned by ::sscan_time,
   then scaled, and then checked for validity as \a doubles.

   @param n Number of values to convert.
   @param src_type Source datatype (see ::DRMS_Type_t).
   @param bzero Offset by which raw data values are to be shifted to 
   produce actual data values.
   @param bscale Scaling factor by which raw data values are to be multiplied to 
   produce actual data values.
   @param src Source data, which is of type \a srctype.
   @param dst Contiguous array of the time datatype values into which the converted
   data are stored upon success.
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

   @param n Number of values to convert.
   @param src_type Source datatype (see ::DRMS_Type_t).
   @param bzero Offset by which raw data values are to be shifted to 
   produce actual data values.
   @param bscale Scaling factor by which raw data values are to be multiplied to 
   produce actual data values.
   @param src Source data, which is of type \a srctype.
   @param dst Contiguous array of string pointers into which the converted
   data are stored upon success.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_array2string(int n, DRMS_Type_t src_type, double bzero, double bscale, void *src, char **dst);

/**
   Converts the data in \a src->data to type
   \a dsttype, scaling by the values \a bzero and \a bscale, by
   calling the function ::drms_array_rawconvert. It returns a newly
   created array struct with the converted values but without copying or
   setting the elements other than those set by ::drms_array_create.
   In particular, the elements \a bzero and \a bscale are not set.
   ::drms_array_convert_inplace performs the same type conversion,
   but instead of returning a new array simply replaces the \a src->data
   element.

   @param dst Contiguous array of type \a dsttype into which the converted
   data are stored upon success.
   @param bzero Offset by which raw data values are to be shifted to 
   produce actual data values.
   @param bscale Scaling factor by which raw data values are to be multiplied to 
   produce actual data values.
   @param src The source array containing that data to be converted.
*/
DRMS_Array_t *drms_array_convert(DRMS_Type_t dsttype, double bzero, 
				 double bscale, DRMS_Array_t *src);

/**
   Performs the same type conversion as ::drms_array_convert,
   but instead of returning a new array simply replaces the \a src->data
   element.

   @param dst Contiguous array of type \a dsttype into which the converted
   data are stored upon success.
   @param bzero Offset by which raw data values are to be shifted to 
   produce actual data values.
   @param bscale Scaling factor by which raw data values are to be multiplied to 
   produce actual data values.
   @param src The source array containing that data to be converted.
*/
void drms_array_convert_inplace(DRMS_Type_t newtype, double bzero, 
				double bscale, DRMS_Array_t *src);

/**
   Converts the first \a n values of \a src->data, interpreted as being
   of type \a srctype, to type \a dsttype, with scaling by \a bzero
   and \a bscale as applicable for the datatype, by calling the appropriate
   function \a drms_array2*. The resulting data are placed in \a dst. 

   @param n Number of values to convert.
   @param dsttype Destination datatype (see ::DRMS_Type_t).
   @param bzero Offset by which raw data values are to be shifted to 
   produce actual data values.
   @param bscale Scaling factor by which raw data values are to be multiplied to 
   produce actual data values.
   @param dst Contiguous array of type \a dsttype into which the converted
   data are stored upon success.
   @param srctype Source datatype (see ::DRMS_Type_t).
   @param src Source data, which is of type \a srctype.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/			
int drms_array_rawconvert(int n, DRMS_Type_t dsttype, double bzero, 
			  double bscale, void *dst, DRMS_Type_t srctype, 
			  void *src);
/* @} */

#endif
