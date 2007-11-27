/*
 *  parse_params.c						2007.11.26
 *
 *  functions defined:
 *	XXX_params_parse_arguments
 *	XXX_params_free
 *	params_print
 *	XXX_params_stat
 *	XXX_params_isdef
 *	XXX_params_get_int
 *	XXX_params_get_float
 *	XXX_params_get_double
 *	XXX_params_get_string
 *	XXX_params_get_char
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "hash_table.h"
#include "parse_params.h"
#include "xmem.h"

#define PARAMBUFLEN (256*4096)
static char param_buffer[PARAMBUFLEN];
static Hash_Table_t ParamHash;

/* Missing prototype in stdlib.h?!? */
float strtof (const char *nptr, char **endptr);

/*
 *  Operators defining a Hash table on strings
 */
static unsigned int hash (const void *v) {
  char *c = (char *) v;
  unsigned int sum = 0;

  while (*c) sum += (unsigned int)*c++ + 31 * sum;
  return sum;
}
/*
 *
 */
static int equal (const void *a, const void *b) {
  return  strncmp ((char *)a, (char *)b, LINEMAX);
}
/*
 *
 */
static void print (const void *key, const void *value) {
  printf("%s = %s\n", (char *)key, (char *)value);
}
/*
 *  Simple parameter parsing functions. The parameters are read in
 *    and a Hash table mapping parameter names to their values is
 *    constructed
 */
/*  parse a line of the form
 *     <ws>* <name> <ws>* '=' <ws>* <value> <ws>* '#' <comments>, 
 *     where <ws> = white space
 *           <value> =  <char \ <ws>> | '"' <char>* '"'
 *     It i assumed that the string in inbuf ends in '\0'
 *     and that strlen(inbuf) is smaller than the length of
 *     the buffers for name and value.
 */
static int parse_line (char *inbuf, char **outbuf, char **pname,
    char **pvalue) {
  char *instart, *name, *value;

  instart = inbuf;
  *pname = NULL; *pvalue = NULL;
  name = *outbuf;  *name = '\0';
  while(isspace(*inbuf)) ++inbuf;
  if (*inbuf == '\0')
    return 0;
  while(isgraph(*inbuf) && *inbuf != '=' && *inbuf != '#' )
    *name++ = (char) toupper((int)(*inbuf++));
  if (*inbuf == '#') 
    return 0;
  *name++ = '\0';
  *pname = *outbuf;  *pvalue = name;
  *name++ = '\0';    
  *outbuf = name;
  while(isspace(*inbuf)) ++inbuf;
  if (*inbuf != '=')
  {
    fprintf(stderr,"'%s': Unexpected parameter string format: Expected '='.\n",instart);
    return 1;
  }
  ++inbuf;
  while(isspace(*inbuf)) ++inbuf;  
  if (*inbuf == '#') 
    return 1;
  if ( *inbuf == '"' ) 
  {
    /* This parameter has a string value. */
    ++inbuf;
    value = name;
    *pvalue = value;
    while(isprint(*inbuf) && *inbuf != '#'  && *inbuf != '"' )
      *value++ = *inbuf++;
    if ( *inbuf != '"' )
      fprintf(stderr,"'%s': Unterminated string missing '\"'.\n",instart);      
  }
  else 
  {    
    value = name;
    *pvalue = value;
    while(isgraph(*inbuf) && *inbuf != '#' )
      *value++ = *inbuf++;
  }
  *value++ = '\0'; 
  *outbuf = value;

  return 2;
}
/*
 *
 */
static int parse_file (FILE *stream, Hash_Table_t *h, char **bufstart,
    char *bufend) {
  int count, status;
  char linebuf[LINEMAX], *name, *value;

  count = 0;
  while(fgets(linebuf, LINEMAX, stream)) {
    if ((int)(bufend - *bufstart) < LINEMAX) {
      fprintf(stderr,"Parameter buffer depleted.\n");
      exit(1);
    }
    status = parse_line(linebuf, bufstart, &name, &value);
    /*     printf("name = %s, value = %s\n",name,value); */
    if (status) {
      hash_insert(h,name,value);
      ++count;
    }
  }
  return count;
}
/*
 *
 */
int XXX_params_parse_arguments (int argc, char *argv[]) {
  int count;
  int i, status;
  char *bufstart = param_buffer, *bufend = &param_buffer[PARAMBUFLEN-1];
  char *name, *value;
  FILE *stream;

  count = 0;
  hash_init(&ParamHash,503,1,equal,hash);
  for (i=1; i<argc; i++) 
  {
    if (argv[i][0] == '-') {
      status  = parse_line(&argv[i][1], &bufstart, &name, &value);
      if (status)
      {
	hash_insert(&ParamHash,name,value);
	++count;
      }
    }
    else
    {
      if ((stream = fopen(argv[i],"r")))
      {
	count += parse_file(stream, &ParamHash,&bufstart,bufend);
	fclose(stream);
      }
      else
	fprintf(stderr,"parse_params: Couldn't open file '%s'.\n",argv[i]);
    }
  }
  return count;  
}
/*
 *  Free data structures allocated for Hash table and empty buffer
 */
void XXX_params_free (void) {
  hash_free (&ParamHash);
}
/*
 *  Debugging functions
 */
void params_print (void) {
  hash_map(&ParamHash,print);
}
/*
 *
 */
void XXX_params_stat (void) {
  hash_stat(&ParamHash); 
}
/*
 *  Functions for looking up the value of an argument in the Hash table
 */
int XXX_params_isdef (const char *name) {
  int i;
  char buf[LINEMAX], *p=buf;
  
  for (i=0; i<LINEMAX-1 && *name!='\0'; i++)
    *p++ = toupper(*name++);
  *p = '\0';
  return hash_member(&ParamHash, buf);
}
/*
 *
 */
int XXX_params_get_int (const char *name) {
  int i;
  const char *value;
  char *endptr;
  long int val;
  char buf[LINEMAX], *p=buf;
  
  for (i=0; i<LINEMAX-1 && *name!='\0'; i++)
    *p++ = toupper(*name++);
  *p = '\0';
  if((value = hash_lookup(&ParamHash,buf)))
  {
    val = strtol(value,&endptr,10);
    if (value[0]=='\0' || *endptr!='\0' ) 
      fprintf(stderr,"GetArgInt: The value of '%s' = '%s' is not an int.\n",name, value);
    else
      return (int) val;
  }
  else
    fprintf(stderr,"Unknown parameter '%s'.\n",name);
  exit(1);    
}
/*
 *
 */
float XXX_params_get_float (const char *name) {
  int i;
  const char *value;
  char *endptr;
  float val;
  char buf[LINEMAX], *p=buf;
  
  for (i=0; i<LINEMAX-1 && *name!='\0'; i++)
    *p++ = toupper(*name++);
  *p = '\0';
  if((value = hash_lookup(&ParamHash,buf)))
  {
    val = strtof(value,&endptr);
    if (val==0 && endptr==value )
      fprintf(stderr,"GetArgFloat: The value of '%s' = '%s' is not a float.\n",name, value);
    else
      return val;
  }
  else
    fprintf(stderr,"Unknown parameter '%s'.\n",name);
  exit(1);
}
/*
 *
 */
float XXX_params_get_double (const char *name) {
  int i;
  const char *value;
  char *endptr;
  double val;
  char buf[LINEMAX], *p=buf;
  
  for (i=0; i<LINEMAX-1 && *name!='\0'; i++)
    *p++ = toupper(*name++);
  *p = '\0';
  if((value = hash_lookup(&ParamHash,buf)))
  {
    val = strtod(value,&endptr);
    if (val==0 && endptr==value )
      fprintf(stderr,"GetArgDouble: The value of '%s' = '%s' is not a double.\n",name, 
	      value);
    else
      return val;
  }
  else
    fprintf(stderr,"Unknown parameter '%s'.\n",name);
  exit(1);
}
/*
 *
 */
const char *XXX_params_get_string (const char *name) {
  int i;
  const char *value;
  char buf[LINEMAX], *p=buf;
  
  for (i=0; i<LINEMAX-1 && *name!='\0'; i++)
    *p++ = toupper(*name++);
  *p = '\0';
  if((value = hash_lookup(&ParamHash,buf)))
    return value;
  else
    fprintf(stderr,"Unknown parameter '%s'.\n",name);
  exit(1);
}
/*
 *
 */
char XXX_params_get_char (const char *name) {
  int i;
  const char *value;
  char buf[LINEMAX], *p=buf;
  
  for (i=0; i<LINEMAX-1 && *name!='\0'; i++)
    *p++ = toupper(*name++);
  *p = '\0';
  if((value = hash_lookup(&ParamHash,buf))) {
    if (strlen(value)>1)
      fprintf(stderr,"GetArgChar: The value of '%s' = '%s' is not a char.\n",name, value);
    return value[0];
  }
  else
    fprintf(stderr,"Unknown parameter '%s'.\n",name);
  exit(1);
}

