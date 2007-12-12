/**
@file cmdparams.h
@brief Functions to get values in appropriate form from parsed command line and module arguments list.
@sa ::module
@bug If ::cmdparams_get_str (which is called by all the other versions of
\a cmdparams_get_XXX)cannot find a hash table entry for the requested
name, it will use the corresponding environment variable, if it exists.

These functions return values of the appropriate type from the hash list
of parameters expected to be available to a module as a global variable.
*/

#ifndef _CMDPARAMS_H
#define _CMDPARAMS_H

#include "hash_table.h"
#include <stdlib.h>

#ifdef FLIB
/* Fortran interface */
#include "cfortran.h"
#include "inthandles.h"
#define kgModCmdParams 1
#define kgModCmdParamsStr "1"
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
typedef enum {
   ARG_END = 0,
   ARG_FLAG,   ARG_TIME,    ARG_INT,        ARG_FLOAT,   ARG_DOUBLE,
   ARG_STRING, ARG_VOID,    ARG_INTS,       ARG_FLOATS,  ARG_DOUBLES,
/* ARG_STRINGS, */
   ARG_NUME,   ARG_DATASET, ARG_DATASERIES, ARG_NEWDATA, ARG_NUMARGS
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


/**
@name [cmd]params_isflagset
Check the existence of a flag parameter.
*/
/* @{ */
/**
Returns the value 0
if either the corresponding parameter is 0 or does not exist; otherwise it
returns the integer value associated with the parameter (normally -1 if the
argument is of type ::ARG_FLAG).
NOTE: These functions are identical.
*/
int cmdparams_isflagset (CmdParams_t *parms, char *name);
int params_isflagset (CmdParams_t *parms, char *name);
/* @} */


/**
@name cmdparams_get_XXX
Functions to return the value of the parameter \a name from the set of 
cmd-line parameters in \a parms.
*/
/* @{ */
/**
Returns the value in the
parameters list associated with the specified name. If there is no entry
with that name or if the type does not correspond to \a XXX,
then the value representing missing (fill) data for that type is returned,
and the \a status value is set to a non-zero code.

On a faliure such as inability to locate the named key or to successfully
parse the associated value string according to the prescribed datatype,
returns the MISSING value defined for the type.
*/
int8_t cmdparams_get_int8 (CmdParams_t *parms, char *name, int *status);
int16_t cmdparams_get_int16 (CmdParams_t *parms, char *name, int *status);
int32_t cmdparams_get_int32 (CmdParams_t *parms, char *name, int *status);
int64_t cmdparams_get_int64 (CmdParams_t *parms, char *name, int *status);
float cmdparams_get_float (CmdParams_t *parms, char *name, int *status);
double cmdparams_get_double (CmdParams_t *parms, char *name, int *status);
						/*  Generic integer version  */
int cmdparams_get_int (CmdParams_t *parms, char *name, int *status);
				       /*  versions without status argument  */
/**
Returns a pointer to the associated string.
On a failure, such as inability to locate the named key or to successfully
parse the associated value string according to the prescribed datatype,
returns a NULL pointer.
*/
char *cmdparams_get_str (CmdParams_t *parms, char *name, int *status);

/** 
Returns a double representing the internal time representation of a 
parsed date-time string.
*/
double cmdparams_get_time (CmdParams_t *parms, char *name, int *status);
/* @} */

/**
@name params_get_XXX
Functions to return the value of the parameter \a name from the set of 
cmd-line parameters in \a parms.  These functions are equivalent to the 
\a cmdparams_get_XXX functions, but 
without the third argument for a status value.
*/
/* @{ */
/**
This function is completely analogous to the \a cmdparams_get_XXX version,
except that no status is returned.
*/
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
/* @} */

#ifdef FLIB
/**
@name f_cmdparams_get_XXX, f_params_get_XXX, f_cmdparams_isflagset, f_params_isflagset
Fortran functions that interface the C cmdparams_get_XXX,  params_get_XXX functions,
f_cmdparams_isflagset, and f_params_isflagset functions.
*/
/* @{ */

/**
This function is completely analogous to the corresponding C function
(which has the same name as this function, without the "f_" prefix).
*/
void f_cmdparams_get_str(pFHandleCmdParams handle, 
			 const char *name, 
			 char *ret, 
			 int size, 
			 int *status);
void f_cmdparams_get_int(pFHandleCmdParams handle, const char *name, int *ret, int *status);
void f_cmdparams_get_int8(pFHandleCmdParams handle, const char *name, int8_t *ret, int *status);
void f_cmdparams_get_int16(pFHandleCmdParams handle, const char *name, int16_t *ret, int *status);
void f_cmdparams_get_int32(pFHandleCmdParams handle, const char *name, int32_t *ret, int *status);
void f_cmdparams_get_int64(pFHandleCmdParams handle, const char *name, long long *ret, int *status);
void f_cmdparams_get_float(pFHandleCmdParams handle, char *name, float *ret, int *status);
void f_cmdparams_get_double(pFHandleCmdParams handle, char *name, double *ret, int *status);
void f_cmdparams_get_time(pFHandleCmdParams handle, char *name, double *ret, int *status);

void f_params_get_str(pFHandleCmdParams handle, const char *name, char *ret, int size);
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

/**
This function is completely analogous to the ::cmdparams_isflagset function.
*/
void f_cmdparams_isflagset(pFHandleCmdParams handle, const char *name, int *ret);

/**
This function is completely analogous to the ::params_isflagset function.
*/
void f_params_isflagset(pFHandleCmdParams handle, const char *name, int *ret);
/* @} */

#endif /* FLIB */

/* Used by server apps */
int cmdparams_parse (CmdParams_t *parms, int argc, char **argv);
int cmdparams_parsefile (CmdParams_t *parms, char *filename, int depth);
int cmdparams_exists (CmdParams_t *parms, char *name);
void cmdparams_printall (CmdParams_t *parms);
char *cmdparams_getarg (CmdParams_t *parms, int num);
void cmdparams_usage (char *name);
int cmdparams_numargs (CmdParams_t *parms);

#endif
