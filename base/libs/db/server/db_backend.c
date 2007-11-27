/*
 *  db_backend.c						2007.11.26
 *
 *  This file contains backend functions that are database independent. 
 *    The remaining backend functions are found in the database specific
 *    files db_oracle.c, db_mysql.c, and db_postgresql.c.
 *
 *  functions defined:
 *	db_dmsv
 *	db_bulk_insertv
 *	db_query_binv
 */
#include <stdio.h>
#ifndef __USE_ISOC99
  #define  __USE_ISOC99
  #include <stdlib.h>
  #undef __USE_ISOC99
#else
  #include <stdlib.h>
#endif
#include <string.h>
#include <alloca.h>
#include <stdarg.h>
#include "db.h"
#include "xassert.h"
#include "util.h"
#include "xmem.h"
/*
 *  DMS function with variable argument list. Collects arguments in
 *    list of void * and calls db_dms_array.
 */
int db_dmsv (DB_Handle_t  *dbin, int *row_count,  char *query,
    int n_rows, ...) {
  int status=1;
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];
  int n,i;
  char *q;
  db_char_t tchar;   
  db_int1_t tint1;   
  db_int2_t tint2;   
  db_int4_t tint4;   
  db_int8_t tint8;   
  db_float_t tfloat;  
  db_double_t tdouble; 
  
  va_list ap;
  va_start(ap, n_rows);   
  
  /* Collect arguments. */
  q = (char *)query;
  n = 0;
  while (*q)
  {
    if (*q == '?')
    {
      if (n>=MAXARG)
      {
	fprintf(stderr,"Maximum number of arguments exceeded.\n");
	goto failure;
      }
      n++;
    }
    q++;
  }

  for (i=0; i<n; i++)
  {
    intype[i] = va_arg(ap, DB_Type_t);
    if (n_rows == -1) /* Ac single set of arguments passed by value. */
    {    
      switch(intype[i])
      {
      case DB_CHAR:
	tchar = (db_char_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_char_t));
	memcpy(argin[i], &tchar, sizeof(db_char_t));
	break;
      case DB_INT1:
	tint1 = (db_int1_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int1_t));
	memcpy(argin[i], &tint1, sizeof(db_int1_t));
	break;
      case DB_INT2:
	tint2 = (db_int2_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int2_t));
	memcpy(argin[i], &tint2, sizeof(db_int2_t));
	break;
      case DB_INT4:
	tint4 = va_arg(ap, db_int4_t);
	argin[i] = alloca(sizeof(db_int4_t));
	memcpy(argin[i], &tint4, sizeof(db_int4_t));
	break;
      case DB_INT8:
	tint8 = va_arg(ap, db_int8_t);
	argin[i] = alloca(sizeof(db_int8_t));
	memcpy(argin[i], &tint8, sizeof(db_int8_t));
	break;
      case DB_FLOAT:
	tfloat = (db_float_t) va_arg(ap,double);
	argin[i] = alloca(sizeof(db_float_t));
	memcpy(argin[i], &tfloat, sizeof(db_float_t));
	break;
      case DB_DOUBLE:
	tdouble = va_arg(ap, db_double_t);
	argin[i] = alloca(sizeof(db_double_t));
	memcpy(argin[i], &tdouble, sizeof(db_double_t));
	break;
      case DB_STRING:
      case DB_VARCHAR:
	argin[i] = alloca(sizeof(char **));
	*((char **)argin[i]) = va_arg(ap, db_string_t);
	break;
      }
    }
    else
    {
      argin[i] = va_arg(ap, void *);
    }
  }

  if (n_rows == -1)
    n_rows = 1;

  status = db_dms_array(dbin, row_count, query, n_rows, n,
			intype, argin );
 failure:
  va_end(ap);
  return status;
}
/*
 *  DMS function with variable argument list. Collects arguments in
 *    list of void * and calls db_dms_array.
 */
int db_bulk_insertv (DB_Handle_t  *dbin, char *table, int n_rows, int n_cols,
    ...) {
  int status=1;
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];
  int i;
  db_char_t tchar;   
  db_int1_t tint1;   
  db_int2_t tint2;   
  db_int4_t tint4;   
  db_int8_t tint8;   
  db_float_t tfloat;  
  db_double_t tdouble; 
  
  va_list ap;
  va_start(ap, n_cols);   
  
  for (i=0; i<n_cols; i++) {
    intype[i] = va_arg(ap, DB_Type_t);
    if (n_rows == -1) /* Ac single set of arguments passed by value. */
    {    
      switch(intype[i])
      {
      case DB_CHAR:
	tchar = (db_char_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_char_t));
	memcpy(argin[i], &tchar, sizeof(db_char_t));
	break;
      case DB_INT1:
	tint1 = (db_int1_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int1_t));
	memcpy(argin[i], &tint1, sizeof(db_int1_t));
	break;
      case DB_INT2:
	tint2 = (db_int2_t) va_arg(ap, int);
	argin[i] = alloca(sizeof(db_int2_t));
	memcpy(argin[i], &tint2, sizeof(db_int2_t));
	break;
      case DB_INT4:
	tint4 = va_arg(ap, db_int4_t);
	argin[i] = alloca(sizeof(db_int4_t));
	memcpy(argin[i], &tint4, sizeof(db_int4_t));
	break;
      case DB_INT8:
	tint8 = va_arg(ap, db_int8_t);
	argin[i] = alloca(sizeof(db_int8_t));
	memcpy(argin[i], &tint8, sizeof(db_int8_t));
	break;
      case DB_FLOAT:
	tfloat = (db_float_t) va_arg(ap,double);
	argin[i] = alloca(sizeof(db_float_t));
	memcpy(argin[i], &tfloat, sizeof(db_float_t));
	break;
      case DB_DOUBLE:
	tdouble = va_arg(ap, db_double_t);
	argin[i] = alloca(sizeof(db_double_t));
	memcpy(argin[i], &tdouble, sizeof(db_double_t));
	break;
      case DB_STRING:
      case DB_VARCHAR:
	argin[i] = alloca(sizeof(char **));
	*((char **)argin[i]) = va_arg(ap, db_string_t);
	break;
      }
    }
    else
    {
      argin[i] = va_arg(ap, void *);
    }
  }

  if (n_rows == -1)
    n_rows = 1;

  status = db_bulk_insert_array(dbin, table, n_rows, n_cols, intype, argin );
  va_end(ap);
  return status;
}
/*
 *  Query function with variable argument list. Collects arguments in
 *    list of void * and calls db_dms_array.
 */
DB_Binary_Result_t *db_query_binv (DB_Handle_t  *dbin,  char *query, ...) {
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];
  int n,i;
  char *q;
  DB_Binary_Result_t *result;
  db_char_t tchar;   
  db_int1_t tint1;   
  db_int2_t tint2;   
  db_int4_t tint4;   
  db_int8_t tint8;   
  db_float_t tfloat;  
  db_double_t tdouble; 

  va_list ap;
  va_start(ap, query);   
  
  /* Collect arguments. */
  q = (char *)query;
  n = 0;
  while (*q)
  {
    if (*q == '?')
    {
      if (n>=MAXARG)
      {
	fprintf(stderr,"Maximum number of arguments exceeded.\n");
	result = NULL;
	goto failure;
      }
      n++;
    }
    q++;
  }

  for (i=0; i<n; i++)
  {
    intype[i] = va_arg(ap, DB_Type_t);
    switch(intype[i])
    {
    case DB_CHAR:
      tchar = (db_char_t) va_arg(ap, int);
      argin[i] = alloca(sizeof(db_char_t));
      memcpy(argin[i], &tchar, sizeof(db_char_t));
      break;
    case DB_INT1:
      tint1 = (db_int1_t) va_arg(ap, int);
      argin[i] = alloca(sizeof(db_int1_t));
      memcpy(argin[i], &tint1, sizeof(db_int1_t));
      break;
    case DB_INT2:
      tint2 = (db_int2_t) va_arg(ap, int);
      argin[i] = alloca(sizeof(db_int2_t));
      memcpy(argin[i], &tint2, sizeof(db_int2_t));
      break;
    case DB_INT4:
      tint4 = va_arg(ap, db_int4_t);
      argin[i] = alloca(sizeof(db_int4_t));
      memcpy(argin[i], &tint4, sizeof(db_int4_t));
      break;
    case DB_INT8:
      tint8 = va_arg(ap, db_int8_t);
      argin[i] = alloca(sizeof(db_int8_t));
      memcpy(argin[i], &tint8, sizeof(db_int8_t));
      break;
    case DB_FLOAT:
      tfloat = (db_float_t) va_arg(ap,double);
      printf("tfloat = %f\n",tfloat);
      argin[i] = alloca(sizeof(db_float_t));
      memcpy(argin[i], &tfloat, sizeof(db_float_t));
      break;
    case DB_DOUBLE:
      tdouble = va_arg(ap, db_double_t);
      argin[i] = alloca(sizeof(db_double_t));
      memcpy(argin[i], &tdouble, sizeof(db_double_t));
      break;
    case DB_STRING:
    case DB_VARCHAR:
      argin[i] = va_arg(ap, db_string_t);
      break;
    }
  }

    
  result = db_query_bin_array(dbin, query, n, intype, argin );

failure:
  va_end(ap);
  return result;
}
