#include <stdio.h>
#ifndef __USE_ISOC99
  #define  __USE_ISOC99
  #include <stdlib.h>
  #undef __USE_ISOC99
#else
  #include <stdlib.h>
#endif
#include <string.h>
#include "db.h"
#include "xassert.h"
#include "xmem.h"



/* Global variables used to pass information needed by qsort 
   comparison function on the side. */
static DB_Binary_Result_t *__sort_res;
static int *__sort_cols;
static int __num_sort_cols;
static void db_permute(DB_Binary_Result_t *res, int n, int *p);



int db_type_compare(DB_Type_t type, char *p1, char *p2)
{
  switch(type)
  {
  case DB_CHAR:    
    return *p1 - *p2; 
  case DB_INT1:    
    return *((db_int1_t *) p1) - *((db_int1_t *) p2); 
  case DB_INT2:
    return *((db_int2_t *) p1) - *((db_int2_t *) p2); 
  case DB_INT4:
    return *((db_int4_t *) p1) - *((db_int4_t *) p2); 
  case DB_INT8:
    {
    db_int8_t v1, v2;
    v1 = *((db_int8_t *) p1);
    v2 = *((db_int8_t *) p2); 
    if (v1<v2)
      return -1;
    else if (v1>v2)
      return 1;
    else 
      return 0;
    }
  case DB_FLOAT:
    {
    db_float_t v1, v2;
    v1 = *((db_float_t *) p1);
    v2 = *((db_float_t *) p2); 
    if (v1<v2)
      return -1;
    else if (v1>v2)
      return 1;
    else 
      return 0;
    }
  case DB_DOUBLE:
    {
      db_double_t v1, v2;
    v1 = *((db_double_t *) p1);
    v2 = *((db_double_t *) p2); 
    if (v1<v2)
      return -1;
    else if (v1>v2)
      return 1;
    else 
      return 0;
    }
  case DB_STRING:
  case DB_VARCHAR:
    return strcmp(p1,p2);
  default:
    return 0;
  }
  return 0;
}



int db_binary_record_compare(DB_Binary_Result_t *res, int num_cols, int *cols,
			     int i1, int i2)
{
  int i;
  int col, comp, size;
  DB_Type_t type;

  if (i1==i2)
    return 0;
  for (i=0; i<num_cols; i++)
  {
    col = cols[i];
    type = res->column[col].type;
    size = res->column[col].size; 
    comp = db_type_compare(type, 
			   res->column[col].data+i1*size,
			   res->column[col].data+i2*size);
    if (comp)
      return comp;
  }
  return 0;
}

/* Qsort wrapper for comparison. */
static int qsort_compare(const void *p1, const void *p2)
{
  int i1, i2;

  i1 = *((int *) p1);
  i2 = *((int *) p2);
  return db_binary_record_compare(__sort_res, __num_sort_cols, __sort_cols, 
				  i1, i2);
}

/* Sort query result on the columns given in "cols". */
int db_sort_binary_result(DB_Binary_Result_t *res, int num_cols, int *cols)
{
  int i, permute;
  int *p;
  int num_rows;

  /* Sanity check input arguments. */
  if (!cols || !res || num_cols<0)
    return 1;

  /* Make sure column numbers are valid. */
  for (i=0;i<num_cols;i++)
  {
    if (cols[i]<0 || cols[i]>res->num_cols)
      return 1;
  }
  
  /* Check for fast exit in case no sort columns are specified
     or the result set is empty. */
  if (num_cols==0 || res->num_rows==0 )
    return 0;
  
  __sort_cols = cols; 
  __sort_res = res;
  __num_sort_cols = num_cols;
  num_rows = res->num_rows;
  XASSERT( (p = malloc(num_rows*sizeof(int))));
  for (i=0; i<num_rows; i++)
    p[i] = i;

  qsort(p, num_rows, sizeof(int), qsort_compare);
  
  /* Check if any permutations were performed. */
  permute = 0;
  for (i=0; i<num_rows; i++)
  {
    if ( p[i] != i)
    {
      permute=1;
      break;
    }
  }
  
  if (permute)
    db_permute(res, res->num_rows, p);

  free(p);
  return 0;
}


static void db_permute(DB_Binary_Result_t *res, int n, int *p)
{
  int i,j;
  char *buf;

  /* Permute rows according to indices in p. */
  for (i=0;i<res->num_cols; i++)
  {
    XASSERT(buf = malloc(res->column[i].size*n));
    for (j=0; j<n; j++)
      memcpy(buf + j*res->column[i].size, 
	     res->column[i].data + p[j]*res->column[i].size, 
	     res->column[i].size); 
    free(res->column[i].data);
    res->column[i].data = buf;
    res->column[i].num_rows = n;
  }
  res->num_rows = n;
}


/* */
int db_maxbygroup(DB_Binary_Result_t *res, int maxcol, int num_cols, int *cols)
{
  int i, n, *p, max;

  /* Sanity check input arguments. */
  if (!cols || !res || num_cols<0)
    return 1;

  for (i=0; i<num_cols; i++)
    if (cols[i] == maxcol)
      return 1; /* max columm cannot be in the grouping set. */
  

  /* Start by sorting. */
  if (db_sort_binary_result(res, num_cols, cols))
    return 1;
  
  if (res->num_rows==1)
    return 0;

  /* Select max(maxcolumn),* group by cols[*] */
  XASSERT( (p = malloc(res->num_rows*sizeof(int))));
  n = 0;
  max = 0; 
  i = 1;
  while (i<res->num_rows)
  {
    while(i<res->num_rows && 
	  !db_binary_record_compare(res, num_cols, cols, i, max))
    {
      if (db_binary_record_compare(res, 1, &maxcol, i, max) > 0)
	max = i;
      i++;
    }
    p[n++] = max;
    max = i++;
    if (i==res->num_rows)
      p[n++] = max;      
  }
  db_permute(res, n, p);
  free(p);
  return 0;
}  
