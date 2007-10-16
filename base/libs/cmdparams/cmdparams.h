#ifndef _CMDPARAMS_H
#define _CMDPARAMS_H

#include "hash_table.h"
#include <stdlib.h>

#ifdef FLIB
/* Fortran interface */
#include "cfortran.h"
#define kgModCmdParams 1
#define kgModCmdParamsStr "1"
typedef int FHandleCmdParams;
typedef int *pFHandleCmdParams;
#endif /* FLIB */

#define CMDPARAMS_INITBUFSZ (4096)
#define CMDPARAMS_MAXLINE (1024)
#define CMDPARAMS_MAXNEST (10)
			       /* Status codes returned by cmdparams_get_xxx */
#define CMDPARAMS_SUCCESS		(0)
#define CMDPARAMS_FAILURE		(-1)
#define CMDPARAMS_QUERYMODE		(-2)
#define CMDPARAMS_NODEFAULT		(-3)
#define CMDPARAMS_UNKNOWN_PARAM		(-4)
#define CMDPARAMS_INVALID_CONVERSION	(-5) 
#define CMDPARAMS_OUTOFMEMORY   	(-6) 
	       /*  Argument types in ModuleArgs_t used by cmdparams_parse()  */

typedef enum
{
   ARG_END = 0,
   ARG_FLAG,
   ARG_TIME,
   ARG_INT,
   ARG_FLOAT,
   ARG_DOUBLE,
   ARG_STRING,
   ARG_VOID,
   ARG_INTS,
   ARG_FLOATS,
   ARG_DOUBLES,
   /* ARG_STRINGS, */
   ARG_NUME,
   ARG_DATASET,
   ARG_DATASERIES,
   ARG_NEWDATA,
   ARG_NUMARGS
} ModuleArgs_Type_t;

typedef struct ModuleArgs_t {
  ModuleArgs_Type_t type;
  char *name;
  char *value;
  char *description;
  char *range;
} ModuleArgs_t; 

typedef struct CmdParams_t_struct {
  int num_args, max_args;
  char **args;
  Hash_Table_t hash;
  int buflen, head;
  char *buffer;
} CmdParams_t;

int cmdparams_parse (CmdParams_t *parms, int argc, char **argv);
int cmdparams_parsefile (CmdParams_t *parms, char *filename, int depth);

void cmdparams_freeall (CmdParams_t *parms);
void cmdparams_set (CmdParams_t *parms, char *name, char *value);
int cmdparams_exists (CmdParams_t *parms, char *name);
void cmdparams_remove (CmdParams_t *parms, char *name);
char *cmdparams_getarg (CmdParams_t *parms, int num);
void cmdparams_printall (CmdParams_t *parms);
void cmdparams_usage (char *name);
int cmdparams_numargs (CmdParams_t *parms);
int cmdparams_isflagset (CmdParams_t *parms, char *name);

char *cmdparams_get_str (CmdParams_t *parms, char *name, int *status);
int8_t cmdparams_get_int8 (CmdParams_t *parms, char *name, int *status);
int16_t cmdparams_get_int16 (CmdParams_t *parms, char *name, int *status);
int32_t cmdparams_get_int32 (CmdParams_t *parms, char *name, int *status);
int64_t cmdparams_get_int64 (CmdParams_t *parms, char *name, int *status);
float cmdparams_get_float (CmdParams_t *parms, char *name, int *status);
double cmdparams_get_double (CmdParams_t *parms, char *name, int *status);
double cmdparams_get_time (CmdParams_t *parms, char *name, int *status);
						/*  Generic integer version  */
int cmdparams_get_int (CmdParams_t *parms, char *name, int *status);
				       /*  versions without status argument  */
char *params_get_str (CmdParams_t *parms, char *name);
int8_t params_get_int8 (CmdParams_t *parms, char *name);
int16_t params_get_int16 (CmdParams_t *parms, char *name);
int32_t params_get_int32 (CmdParams_t *parms, char *name);
int64_t params_get_int64 (CmdParams_t *parms, char *name);
float params_get_float (CmdParams_t *parms, char *name);
double params_get_double (CmdParams_t *parms, char *name);
double params_get_time (CmdParams_t *parms, char *name);
char params_get_char (CmdParams_t *parms, char *name);
short params_get_short (CmdParams_t *parms, char *name);
int params_get_int (CmdParams_t *parms, char *name);
int params_isflagset (CmdParams_t *parms, char *name);

#ifdef FLIB
void f_cmdparams_get_handle(pFHandleCmdParams handle);
char *f_cmdparams_gethandle2();

void f_cmdparams_get_str(pFHandleCmdParams handle, 
			 const char *name, 
			 char *ret, 
			 int size, 
			 int *status);
void f_cmdparams_isflagset(pFHandleCmdParams handle, const char *name, int *ret);
void f_cmdparams_get_int(pFHandleCmdParams handle, const char *name, int *ret, int *status);
void f_cmdparams_get_int8(pFHandleCmdParams handle, const char *name, int8_t *ret, int *status);
void f_cmdparams_get_int16(pFHandleCmdParams handle, const char *name, int16_t *ret, int *status);
void f_cmdparams_get_int32(pFHandleCmdParams handle, const char *name, int32_t *ret, int *status);
void f_cmdparams_get_int64(pFHandleCmdParams handle, const char *name, long long *ret, int *status);
void f_cmdparams_get_float(pFHandleCmdParams handle, char *name, float *ret, int *status);
void f_cmdparams_get_double(pFHandleCmdParams handle, char *name, double *ret, int *status);
void f_cmdparams_get_time(pFHandleCmdParams handle, char *name, double *ret, int *status);

void f_params_get_str(pFHandleCmdParams handle, const char *name, char *ret, int size);
void f_params_isflagset(pFHandleCmdParams handle, const char *name, int *ret);
void f_params_get_int(pFHandleCmdParams handle, const char *name, int *ret);
void f_params_get_int8(pFHandleCmdParams handle, const char *name, int8_t *ret);
void f_params_get_int16(pFHandleCmdParams handle, const char *name, int16_t *ret);
void f_params_get_int32(pFHandleCmdParams handle, const char *name, int32_t *ret);
void f_params_get_int64(pFHandleCmdParams handle, const char *name, long long *ret);
void f_params_get_char(pFHandleCmdParams handle, char *name, signed char *ret);
void f_params_get_short(pFHandleCmdParams handle, char *name, short *ret);
void f_params_get_float(pFHandleCmdParams handle, char *name, float *ret);
void f_params_get_double(pFHandleCmdParams handle, char *name, double *ret);
void f_params_get_time(pFHandleCmdParams handle, char *name, double *ret);
#endif /* FLIB */

#endif
