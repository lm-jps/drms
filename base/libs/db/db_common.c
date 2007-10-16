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
#include <assert.h>
#include <netinet/in.h>
#define DB_COMMON_C
#include "db.h"
#undef DB_COMMON_C
#include "xassert.h"
#include "byteswap.h"
#include "xmem.h"

#ifdef ORACLE
static const char *dbtype_string[] = {"char","integer","integer","integer",
				      "integer","float","double","varchar",
				      "varchar","unknown"};
#endif
#ifdef MYSQL 
static const char *dbtype_string[] = {"char","int2","int2","int4","int8",
				      "float4","float8","text","varchar",
				      "unknown"};
#endif
#ifdef POSTGRESQL
static const char *dbtype_string[] = {"char","int2","int2","int4","int8",
				      "float4","float8","text","varchar",
				      "unknown"};
#endif
const char  UNKNOWN_TYPE_STR[] = "unknown type";

void db_set_error_message(char *err)
{
  strncpy(__db_error_message, err, 4095);
}

void db_get_error_message(int maxlen, char *err)
{
  strncpy(err, __db_error_message, maxlen);
}

const char *db_type_string(DB_Type_t dbtype)
{
  switch(dbtype)
  {
  case DB_CHAR:
    return dbtype_string[0];
  case DB_INT1:
    return dbtype_string[1];
  case DB_INT2:
    return dbtype_string[2];
  case DB_INT4:
    return dbtype_string[3];
  case DB_INT8:
    return dbtype_string[4];
  case DB_FLOAT:
    return dbtype_string[5];
  case DB_DOUBLE:
    return dbtype_string[6];
  case DB_STRING:
    return dbtype_string[7];
  case DB_VARCHAR:
    return dbtype_string[8];
  default:
    return UNKNOWN_TYPE_STR;
  }
}


/* Static prototypes */
static void print_separator(int width);

#ifndef LINUX
float strtof (const char *nptr, char **endptr)
{
  return (float) strtod( nptr,  endptr);
}   
#endif



#ifdef ORACLE
char *db_stringtype_maxlen(int maxlen)
{
  char *str;
  XASSERT( str = malloc(20));
  sprintf(str,"varchar(%d)",maxlen);
  return str;
}
#else
char *db_stringtype_maxlen(int maxlen)
{
  return (char *)db_type_string(DB_STRING);
}
#endif

int db_sizeof(DB_Type_t dbtype)
{
  switch(dbtype)
  {
    /* ORACLE is retarded when it comes to native integer types. 
       It basically only has 32 bit integers. */
  case DB_CHAR:
    return sizeof(db_char_t);
  case DB_INT1:
    return sizeof(db_int1_t);
  case DB_INT2:
    return sizeof(db_int2_t);    
  case DB_INT4:
    return sizeof(db_int4_t);    
  case DB_INT8:
    return sizeof(db_int8_t);    
  case DB_FLOAT:
    return sizeof(db_float_t);
  case DB_DOUBLE:
    return sizeof(db_double_t);
  case DB_STRING:
  case DB_VARCHAR:
    return sizeof(char *);
  default:
    return 0;
  }
}


static void print_separator(int width)
{
  int i;
  putchar('+');
  for (i=0;i<width-2;i++)
    putchar('-');
  putchar('+');
  putchar('\n');
}

static void print_separator1(int width)
{
  int i;
  for (i=0;i<width;i++)
    putchar('-');
  putchar('\n');
}


void db_print_text_result(DB_Text_Result_t *res)
{
  unsigned int i,j,tw, len;
  unsigned int *column_width;

  column_width = alloca(res->num_cols);
  tw=res->num_cols*3+1;
  for (j=0; j<res->num_cols; j++)
  {
    column_width[j] = res->column_width[j];
    if (res->column_name[j])
    {
      len = strlen(res->column_name[j]);
      if (len>column_width[j])
	column_width[j] = len;
    }
    tw += column_width[j];
  }
	
  if (res->num_rows == 0)
  {
    //    printf("EMPTY SET\n");
    return;
  }

  print_separator(tw);

  for (j=0; j<res->num_cols; j++)
  {
    printf("| ");
    printf("%-*s",res->column_width[j],res->column_name[j]);
    printf(" ");
  }
  printf("|\n");



  print_separator(tw);

  for (i=0; i<res->num_rows; i++)
  {
    for (j=0; j<res->num_cols; j++)
    {
      printf("| ");
      printf("%-*s",column_width[j],res->field[i][j]);
      printf(" ");
    }
    printf("|\n");
  }

  print_separator(tw);
}



/* Type conversion routines. */
char db_binary_field_getchar(DB_Binary_Result_t *res, unsigned int row, 
			     unsigned int col)
{  
  if ( row<res->num_rows && col<res->num_cols)
    return dbtype2char(res->column[col].type,
		      res->column[col].data+row*res->column[col].size);
  else
  {
    fprintf(stderr,"ERROR: Invalid (row,col) index > (0..%d,0..%d)\n",
	    res->num_rows, res->num_cols);
    XASSERT(0);
    return '\0';
  }  
}

int db_binary_field_getint(DB_Binary_Result_t *res, unsigned int row, 
			   unsigned int col)
{  
  if ( row<res->num_rows && col<res->num_cols)
    return dbtype2int(res->column[col].type,
		      res->column[col].data+row*res->column[col].size);
  else
  {
    fprintf(stderr,"ERROR: Invalid (row,col) index > (0..%d,0..%d)\n",
	    res->num_rows, res->num_cols);
    XASSERT(0);
    return 0;
  }  
}


long long db_binary_field_getlonglong(DB_Binary_Result_t *res, unsigned int row, 
			   unsigned int col)
{  
  if ( row<res->num_rows && col<res->num_cols)
    return dbtype2longlong(res->column[col].type,
		      res->column[col].data+row*res->column[col].size);
  else
  {
    fprintf(stderr,"ERROR: Invalid (row,col) index > (0..%d,0..%d)\n",
	    res->num_rows, res->num_cols);
    XASSERT(0);
    return 0L;
  }  
}

float db_binary_field_getfloat(DB_Binary_Result_t *res, unsigned int row, 
			   unsigned int col)
{  
  if ( row<res->num_rows && col<res->num_cols)
    return dbtype2float(res->column[col].type,
		      res->column[col].data+row*res->column[col].size);
  else
  {
    fprintf(stderr,"ERROR: Invalid (row,col) index > (0..%d,0..%d)\n",
	    res->num_rows, res->num_cols);
    XASSERT(0);
    return 0;
  }  
}

double db_binary_field_getdouble(DB_Binary_Result_t *res, unsigned int row, 
			   unsigned int col)
{  
  if ( row<res->num_rows && col<res->num_cols)
    return dbtype2double(res->column[col].type,
		      res->column[col].data+row*res->column[col].size);
  else
  {
    fprintf(stderr,"ERROR: Invalid (row,col) index > (0..%d,0..%d)\n",
	    res->num_rows, res->num_cols);
    XASSERT(0);
    return 0;
  }  
}

void db_binary_field_getstr(DB_Binary_Result_t *res, unsigned int row, 
			    unsigned int col, int len, char *str)
{  
  if ( row<res->num_rows && col<res->num_cols)
    dbtype2str(res->column[col].type,
	       res->column[col].data+row*res->column[col].size,
	       len,str);
  else
  {
    fprintf(stderr,"ERROR: Invalid (row,col) index > (0..%d,0..%d)\n",
	    res->num_rows, res->num_cols);
    XASSERT(0);
  }
}



int db_binary_field_is_null(DB_Binary_Result_t *res, unsigned int row, 
		       unsigned int col)
{  
  XASSERT( row<res->num_rows && col<res->num_cols );
  return res->column[col].is_null[row];
}


int db_binary_default_width(DB_Type_t dbtype)
{
  switch(dbtype)
  {
  case DB_CHAR:    
    return 1; 
  case DB_INT1:    
    return 4; 
  case DB_INT2:
    return 6;
  case DB_INT4:
    return 12;
  case DB_INT8:
    return 23;
  case DB_FLOAT:
    return 12;
  case DB_DOUBLE:
    return 20;
  case DB_STRING:
    return 0;
  case DB_VARCHAR:
    return 0;
  default:
    return 0;
  }
}


void db_print_binary_result(DB_Binary_Result_t *res)
{
  unsigned int i,j, len, total_width;
  unsigned int *column_width;
  
  column_width = alloca(res->num_cols*sizeof(int));
 

  for (j=0; j<res->num_cols; j++)
  {
    if ( res->column[j].type == DB_STRING || 
	 res->column[j].type == DB_VARCHAR )
    {
      column_width[j] = 0;
      for (i=0; i<res->num_rows; i++)
      {
	len = strlen(res->column[j].data+i*res->column[j].size);
	if (len>column_width[j])
	  column_width[j] = len;
      }
    }
    else
      column_width[j] = db_binary_default_width(res->column[j].type);
    if (res->column[j].column_name)
      len = strlen(res->column[j].column_name);
    else 
      len = 7;
    if (len>column_width[j])
      column_width[j] = len;
  }
      
  total_width = 0;
  for (j=0; j<res->num_cols; j++)
  {
    if (res->column[j].column_name)
	printf("%-*s",column_width[j],res->column[j].column_name);
    else
	printf("%-s %03d","Col " ,j);

    if (j<res->num_cols-1)
      printf(" | ");
    total_width += column_width[j];
  }
  printf("\n");

  print_separator1(total_width+(res->num_cols-1)*3);

  for (i=0; i<res->num_rows; i++)
  {
    for (j=0; j<res->num_cols; j++)
    {
      if (res->column[j].is_null[i])
	printf("%*s",column_width[j],"NULL");
      else
	db_print_binary_field(res->column[j].type, column_width[j],
			      res->column[j].data+i*res->column[j].size);
      if (j<res->num_cols-1)
	printf(" | ");
    }
    printf("\n");
  }
}


void db_print_binary_field_type(DB_Type_t dbtype)
{
  switch(dbtype)
  {
  case DB_CHAR:    
    printf("DB_CHAR");
    break;
  case DB_INT1:    
    printf("DB_INT1");
    break;
  case DB_INT2:
    printf("DB_INT2");
    break;
  case DB_INT4:
    printf("DB_INT4");
    break;
  case DB_INT8:
    printf("DB_INT8");
    break;
  case DB_FLOAT:
    printf("DB_FLOAT");
    break;
  case DB_DOUBLE:
    printf("DB_DOUBLE");
    break;
  case DB_STRING:
    printf("DB_STRING");
    break;
  case DB_VARCHAR:
    printf("DB_VARCHAR");
    break;
  default:
    return;
  }
}


int db_sprint_binary_field(DB_Type_t dbtype, int width, char *data, char *dst)
{
  if (width>0)
  {
    switch(dbtype)
    {
    case DB_CHAR:    
      return snprintf(dst,width+1,"%*c",width,*((char *)data));
    case DB_INT1:    
      return snprintf(dst,width+1,"%*hhd",width,*((char *)data));
    case DB_INT2:
      return snprintf(dst,width+1,"% *hd",width,*((short *)data));
    case DB_INT4:
      return snprintf(dst,width+1,"% *d",width,*((int *)data));
    case DB_INT8:
      return snprintf(dst,width+1,"% *lld",width,*((long long *)data));
    case DB_FLOAT:
      return snprintf(dst,width+1,"% *g",width,*((float *)data));
    case DB_DOUBLE:
      return snprintf(dst,width+1,"% *g",width,*((double *)data));
    case DB_STRING:
      return snprintf(dst,width+1,"%-*s",width,(char *)data);
    case DB_VARCHAR:
      return snprintf(dst,width+1,"%-*s",width,(char *)data);
    default:
      return 0;
    }
  }
  else
  {
    switch(dbtype)
    {
    case DB_CHAR:    
      return sprintf(dst,"%c",*((db_char_t *)data));
    case DB_INT1:    
      return sprintf(dst,"% hhd",*((db_int1_t *)data));
    case DB_INT2:
      return sprintf(dst,"% hd",*((db_int2_t *)data));
    case DB_INT4:
      return sprintf(dst,"% d",*((db_int4_t *)data));
    case DB_INT8:
      return sprintf(dst,"% lld",*((db_int8_t *)data));
    case DB_FLOAT:
      return sprintf(dst,"% g",*((db_float_t *)data));
    case DB_DOUBLE:
      return sprintf(dst,"% g",*((db_double_t *)data));
    case DB_STRING:
      return sprintf(dst,"%-s",*((db_string_t *)data));
    case DB_VARCHAR:
      return sprintf(dst,"%-s",*((db_varchar_t *)data));
    default:
      return 0;
    }
  }    
}



void db_print_binary_field(DB_Type_t dbtype, int width, char *data)
{
  char *buf;

  buf = alloca(width+1);
  db_sprint_binary_field(dbtype, width, data, buf);
  fputs(buf,stdout);
}



  
void db_free_binary_result(DB_Binary_Result_t *db_result)
{
  unsigned int i;
  if (db_result)
  {
    if (db_result->column)
    {
      for (i=0; i<db_result->num_cols; i++)
      {	
	if (db_result->column[i].column_name)
	  free(db_result->column[i].column_name);
	if (db_result->column[i].data)
	  free(db_result->column[i].data);
	if (db_result->column[i].is_null)
	  free(db_result->column[i].is_null);
      }
      free(db_result->column);
    }
    free(db_result);
  }
}
  

void db_free_text_result(DB_Text_Result_t *db_result)
{
  if (db_result)
  {
    if (db_result->num_rows>0)
    {
      if (db_result->column_name)
	free(db_result->column_name);
      if (db_result->column_width)
	free(db_result->column_width);
      if (db_result->field)
	free(db_result->field);
      if (db_result->buffer)
	free(db_result->buffer);
    }
    free(db_result);
  }
}
  
    
char *search_replace(const char *string, const char *search, 
		     const char *replace)
{
  int len, ls,lr;
  char *output,*outq, *next, *prev;
    
  len = strlen(string);
  ls = strlen(search);
  lr = strlen(replace);
  XASSERT( outq = malloc((lr/ls+1)*len) );
  output = outq;
  prev = (char *)string;
  while( (prev < string+len) && (next = strstr(prev, search)))
  {
    while( prev<next)
      *outq++ = *prev++;
    sprintf(outq,"%s", replace);
    outq += lr;
    prev += ls;
    next += ls;
  }
  while (prev < string+len) 
    *outq++ = *prev++;
  *outq = '\0';
    
  return output;
}




char dbtype2char(DB_Type_t dbtype, char *data)
{
  switch(dbtype)
  {
  case DB_CHAR:    
    return (char) *((db_char_t *) data);
  case DB_INT1:    
    return (char) *((db_int1_t *) data);
  case DB_INT2:
    return (char) *((db_int2_t *) data);
  case DB_INT4:
    return (char) *((db_int4_t *) data);
  case DB_INT8:
    return (char) *((db_int8_t *) data);
  case DB_FLOAT:
    return (char) *((db_float_t *) data);
  case DB_DOUBLE:
    return (char) *((db_double_t *) data);
  case DB_STRING:
  case DB_VARCHAR:
  default:
    return *data;
  }
}  

short dbtype2short(DB_Type_t dbtype, char *data)
{
  int val;
  char *endptr;
  switch(dbtype)
  {
  case DB_CHAR: 
    return (short) *((db_char_t *) data);
  case DB_INT1:    
    return (short) *((db_int1_t *) data);
  case DB_INT2:
    return (short) *((db_int2_t *) data);
  case DB_INT4:
    return (short) *((db_int4_t *) data);
  case DB_INT8:
    return (short) *((db_int8_t *) data);
  case DB_FLOAT:
    return (short) *((db_float_t *) data);
  case DB_DOUBLE:
    return (short) *((db_double_t *) data);
  case DB_STRING:
  case DB_VARCHAR:
  default:
    val = strtol(data,&endptr,0);
    if (val==0 && endptr==data )
      fprintf(stderr,"dbtype2int: The string '%s' is not an integer.\n",data);
    return val;
  }
}  


int dbtype2int(DB_Type_t dbtype, char *data)
{
  int val;
  char *endptr;
  switch(dbtype)
  {
  case DB_CHAR: 
    return (int) *((db_char_t *) data);
  case DB_INT1:    
    return (int) *((db_int1_t *) data);
  case DB_INT2:
    return (int) *((db_int2_t *) data);
  case DB_INT4:
    return (int) *((db_int4_t *) data);
  case DB_INT8:
    return (int) *((db_int8_t *) data);
  case DB_FLOAT:
    return (int) *((db_float_t *) data);
  case DB_DOUBLE:
    return (int) *((db_double_t *) data);
  case DB_STRING:
  case DB_VARCHAR:
  default:
    val = strtol(data,&endptr,0);
    if (val==0 && endptr==data )
      fprintf(stderr,"dbtype2int: The string '%s' is not an integer.\n",data);
    return val;
  }
}  

long long dbtype2longlong(DB_Type_t dbtype, char *data)
{
  int val;
  char *endptr;
  switch(dbtype)
  {
  case DB_CHAR: 
    return (long long) *((db_char_t *) data);
  case DB_INT1:    
    return (long long) *((db_int1_t *) data);
  case DB_INT2:
    return (long long) *((db_int2_t *) data);
  case DB_INT4:
    return (long long) *((db_int4_t *) data);
  case DB_INT8:
    return (long long) *((db_int8_t *) data);
  case DB_FLOAT:
    return (long long) *((db_float_t *) data);
  case DB_DOUBLE:
    return (long long) *((db_double_t *) data);
  case DB_STRING:
  case DB_VARCHAR:
  default:
    val = strtol(data,&endptr,0);
    if (val==0 && endptr==data )
      fprintf(stderr,"dbtype2int: The string '%s' is not an integer.\n",data);
    return val;
  }
}  


float dbtype2float(DB_Type_t dbtype, char *data)
{
  float val;
  char *endptr;
  switch(dbtype)
  {
  case DB_CHAR: 
    return (float) *((db_char_t *) data);
  case DB_INT1:    
    return (float) *((db_int1_t *) data);
  case DB_INT2:
    return (float) *((db_int2_t *) data);
  case DB_INT4:
    return (float) *((db_int4_t *) data);
  case DB_INT8:
    return (float) *((db_int8_t *) data);
  case DB_FLOAT:
    return (float) *((db_float_t *) data);
  case DB_DOUBLE:
    return (float) *((db_double_t *) data);
  case DB_STRING:
  case DB_VARCHAR:
  default:
    val = strtof(data,&endptr);
    if (val==0 && endptr==data )
      fprintf(stderr,"dbtype2int: The string '%s' is not a float.\n",data);
    return val;
  }
}  


double dbtype2double(DB_Type_t dbtype, char *data)
{
  double val;
  char *endptr;
  switch(dbtype)
  {
  case DB_CHAR: 
    return (double) *((db_char_t *) data);
  case DB_INT1:    
    return (double) *((db_int1_t *) data);
  case DB_INT2:
    return (double) *((db_int2_t *) data);
  case DB_INT4:
    return (double) *((db_int4_t *) data);
  case DB_INT8:
    return (double) *((db_int8_t *) data);
  case DB_FLOAT:
    return (double) *((db_float_t *) data);
  case DB_DOUBLE:
    return (double) *((db_double_t *) data);
  case DB_STRING:
  case DB_VARCHAR:
  default:
    val = strtod(data,&endptr);
    if (val==0 && endptr==data )
      fprintf(stderr,"dbtype2int: The string '%s' is not a double.\n",data);
    return val;
  }
}  


void dbtype2str(DB_Type_t dbtype, char *data, int len, char *str)
{
  switch(dbtype)
  {
  case DB_CHAR:    
    snprintf(str, len, "%1c", *((db_char_t *) data));
    break;
  case DB_INT1:    
    snprintf(str, len, "%hhd", *((db_int1_t *) data));
    break;
  case DB_INT2:
    snprintf(str, len, "%hd", *((db_int2_t *) data));
    break;
  case DB_INT4:
    snprintf(str, len, "%d", *((db_int4_t *) data));
    break;
  case DB_INT8:
    snprintf(str, len, "%lld", *((db_int8_t *) data));
    break;
  case DB_FLOAT:
    snprintf(str, len, "%f", *((db_float_t *) data));
    break;
  case DB_DOUBLE:
    snprintf(str, len, "%lf", *((db_double_t *) data));
    break;
  case DB_STRING:
  case DB_VARCHAR:
  default:
    strncpy(str,data,len);
    break;
  }
}  




void db_lock(DB_Handle_t *h)
{
  if ( h->db_lock == NULL )
    return;
  else
    pthread_mutex_lock( h->db_lock );
}

void db_unlock(DB_Handle_t *h)
{
  if ( h->db_lock == NULL )
    return;
  else
    pthread_mutex_unlock( h->db_lock );
}
 

DB_Text_Result_t *db_binary_to_text(DB_Binary_Result_t *binres)
{
  DB_Text_Result_t *result;  
  unsigned int i,j,buflen;
  int total_width, n,len;
  long colname_length;
  char *p;

  /* Do a binary query and convert to text. */
  if (binres==NULL)
    return NULL; 

  /* Set up datastructure. */
  XASSERT( result = (DB_Text_Result_t *)malloc(sizeof(DB_Text_Result_t)) );
  result->num_rows = binres->num_rows;
  result->num_cols = binres->num_cols;
  XASSERT( result->column_name = (char **)malloc(result->num_cols*sizeof(char *)) );
  XASSERT( result->column_width = (int *)malloc(result->num_cols*sizeof(int)) );
  total_width = 0;
  colname_length = 0;
  
  /* Loop over columns to get column names, types and widths. */
  for (j=0; j<binres->num_cols; j++)
  {
    /* If this is a string column get the actual maximum
       length, otherwise get the default width. */
    if ( binres->column[j].type == DB_STRING || 
	 binres->column[j].type == DB_VARCHAR )
    {
      result->column_width[j] = 0;
      for (i=0; i<binres->num_rows; i++)
      {
	len = strlen(binres->column[j].data+i*binres->column[j].size);
	if (len>result->column_width[j])
	  result->column_width[j] = len;
      }
    }
    else
      result->column_width[j] = db_binary_default_width(binres->column[j].type);
    /* Make sure there is a least room for the string "NULL" */
    if (result->column_width[j] < 4)
      result->column_width[j] = 4;
    
    total_width += result->column_width[j]+1;
    
    /* Save cumulative size in name array. */
    result->column_name[j] = (char *) colname_length;
    if (binres->column[j].column_name)
      colname_length += strlen(binres->column[j].column_name)+1;
    else 
      colname_length += 1;
  }
  
  /* Allocate a single large buffer that can hold
     3 integers (buflen, num_rows, num_cols) in network byte order
     + column_width array + column names + all fields in the result. */
  buflen = (3+result->num_cols)*sizeof(int) + colname_length + 
    total_width*result->num_rows;
#ifdef DEBUG
  printf("buflen = %d\n",buflen);
#endif
  XASSERT( result->buffer = malloc(buflen) );
  p = result->buffer;
  
  /* Pack size info into the buffer. */
  *((int *) p) = htonl(buflen);
  p += sizeof(int);
  *((int *) p) = htonl(result->num_rows);
  p += sizeof(int);
  *((int *) p) = htonl(result->num_cols);
  p += sizeof(int);
  for (i=0; i<result->num_cols; i++)
  {
    *((int *) p) = htonl(result->column_width[i]);
    p += sizeof(int);
  }
  
  /* Copy column names. */
  for (i=0; i<result->num_cols; i++)
  {
    result->column_name[i] = p;
    if (binres->column[i].column_name)
    {
      p = memccpy(p, binres->column[i].column_name, 0, 
		  colname_length);
    }
    else
      *p++ = 0;
    XASSERT(p!=NULL);
  }
  
  /* Set up data structure for the actual contents. */
  XASSERT( result->field =  (char ***) 
	   malloc(result->num_rows*sizeof(char **) +
		  result->num_rows*result->num_cols*sizeof(char *)) );
  for (i=0; i<result->num_rows; i++)
    result->field[i] = (char **) &result->field[result->num_rows + 
						i*result->num_cols];
  
  /* Copy over the actual field contents. */
  for(i = 0; i < result->num_rows; i++)
  {
    for (j = 0; j < result->num_cols; j++)
    {
      if (binres->column[j].is_null[i])
	n = snprintf(p,result->column_width[j]+1,"%*s",result->column_width[j],"NULL");
      else
	n = db_sprint_binary_field(binres->column[j].type, 
				   /*				     result->column_width[j], */
				   0, 
				   binres->column[j].data+i*binres->column[j].size,
				   p);
      
      /*	if (n!=result->column_width[j])
		{
		printf("column= %d, value = %s\n",j,p);
		exit(1);
		} */
      result->field[i][j] = p;
      p += n;
      *p++ = 0;	
    }
  }
  buflen = (int) (p - result->buffer)+1;
  *((int *) result->buffer) = htonl(buflen);
#ifdef DEBUG
  printf("Resulting buffer length = %d\n",buflen);
#endif
  return result;
}


void db_hton(DB_Type_t dbtype, int n, void *data)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  db_byteswap(dbtype, n, data);
#else
  return;
#endif
}  

void db_byteswap(DB_Type_t dbtype, int n, char *val)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  if ( !(dbtype == DB_STRING || dbtype == DB_VARCHAR) )
    byteswap(db_sizeof(dbtype), n, val);
#else
  return;
#endif
}
