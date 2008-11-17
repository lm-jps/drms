/**
@file cmdparams.h
@brief Functions to get values in appropriate form from parsed command line and module arguments list.

These functions return values of the appropriate type from the hash list
of parameters expected to be available to a module as a global variable.

@sa ::module ::module_args
*/

#ifndef _CMDPARAMS_H
#define _CMDPARAMS_H

#include "hash_table.h"
#include "hcontainer.h"
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
   /** @brief Marker denoting end of ::ModuleArgs_t array */
   ARG_END = 0,
   /** @brief Marker denoting a cmd-line flag */
   ARG_FLAG,
   /** @brief Marker denoting a time-value cmd-line argument */
   ARG_TIME,
   /** @brief Marker denoting an integer-value cmd-line argument */
   ARG_INT,
   /** @brief Marker denoting a float-value cmd-line argument */
   ARG_FLOAT,
   /** @brief Marker denoting a double-value cmd-line argument */
   ARG_DOUBLE,
   /** @brief Marker denoting a string-value cmd-line argument */
   ARG_STRING,
   /** @brief Marker denoting a void-value cmd-line argument */
   ARG_VOID,
   /** @brief Marker denoting an integer-array-value cmd-line argument */
   ARG_INTS,
   /** @brief Marker denoting a float-array-value cmd-line argument */
   ARG_FLOATS,
   /** @brief Marker denoting a double-array-value cmd-line argument */
   ARG_DOUBLES,
   /* ARG_STRINGS, */
   /** @brief Marker denoting an enumeration-value cmd-line argument */
   ARG_NUME,
   /** @brief Marker denoting a dataset-descriptor-value cmd-line argument */
   ARG_DATASET,
   /** @brief Marker denoting a dataseries-name-value cmd-line argument */
   ARG_DATASERIES,
   /** @brief Marker denoting a created dataset-value cmd-line argument */
   ARG_NEWDATA,
   /** @brief The number of elements of this enumeration */
   ARG_NUMARGS
} ModuleArgs_Type_t;

/** @brief DRMS structure for holding parsed command-line arguments */
struct ModuleArgs_struct {
  /** @brief Data type of argument. */
  ModuleArgs_Type_t type;
  /** @brief Name of argument (where <name>=<value> appears on the command line. */
  char *name;
  /** @brief Value of argument (where <name>=<value> appears on the command line. */
  char *value;
  /** @brief Free-form descriptive string. */
  char *description;
  /** @brief For type==ARG_NUME, this is a list of comma-separated string tokens that name the enum symbols. */
  char *range;
};
/** 
    @brief ModuleArgs struct reference 
    @example moduleargs_ex1.c
*/
typedef struct ModuleArgs_struct ModuleArgs_t; 

/** @brief Command-line parameter structure */
struct CmdParams_struct {
  /** @brief Number of <name>=<value> cmd-line argument pairs */
  int num_args;
  /** @brief Number of argument slots allocated */
  int max_args;
  /** @brief Array of <name>=<value> cmd-line argument pairs */
  char **args;
  /** @brief Hash table mapping <name> to <value> for each cmd-line argument */
  Hash_Table_t hash;
  int buflen;
  int head;
  char *buffer;
  HContainer_t *actvals;
  int argc; /* original argc passed to main() */
  char **argv; /* contains ALL cmd-line args - args doesn't have them all! */
  HContainer_t *reserved;
};
/** @brief CmdParams struct reference */
typedef struct CmdParams_struct CmdParams_t;

/**
   Parse command line and module arguments struct and create hashed parms list.
   This function, ordinarily called by the ::jsoc_main or
   other driver program, but accessible from any program, parses the command
   line (or other array of strings described by \a argc and \a argv!), included files
   if any, and the global ::module_args struct to create a hash table of all
   arguments available in the \a parms struct. Parsing obeys the following rules:

   1. All tokens after the first in the command line (\a argv) are parsed for
   include file references, flags, or parameter name value pairs.

   2. Included files, described by tokens of the form \a @filename in either
   the command line or an included file, are parsed according to the same
   rules as the command line.

   3. The declared ::module_args list is scanned, and all named parameters
   not already assigned values are assigned their declared defaults, if any.

   If any declared module argument cannot be assigned a value by the above
   rules, processing of the argument list ceases and the function returns
   the value -1. The exception is for arguments declared to be of type
   ::ARG_FLAG, which are simply not set.

   All values in the parms struct are strings, regardless of the declared
   type of the argument. For arguments of type ::ARG_NUME, the value from
   parsing will be string-compared with the set of valid strings in the
   argument range, and then set to the character representation of the
   numeric order of the (first) matching value in the range, counting from
   0.

   Command-line tokens can take any of the following forms:
   <TABLE>
   <TR>
   <TD>\a --name</TD> 
   <TD>(name cannot begin with a digit)</TD>
   </TR>
   <TR>
   <TD>\a -chars</TD>
   <TD>where chars is a set of one or more characters, all of which are
   set as key names with the value "1"</TD>
   </TR>
   <TR>
   <TD>\a name=[]value</TD>
   <TD>white space following the \a = is optional</TD>
   </TR>
   <TR>
   <TD>\a \@filename</TD>
   <TD>specifies name of file containing tokens to be parsed following same rules
   as command line</TD>
   </TR>
   <TR>
   <TD>\a value</TD>
   <TD>assigned to successive unnamed argument $n</TD>
   </TR>
   </TABLE>

   If a parameter value (or name) has embedded white space, it must be quoted.

   An extern @ref module_args declaration is required, though it can be empty.
   A member with a ::module_args.type of ::ARG_END, must terminate the
   the list of parsed members; any members following it in the declarator
   will be ignored.

   @bug Multiple assignments to a given parameter name 
   could result in unpredictable
   values. The last token in the command line should be parsed last and take
   precedence. Thus, for example, an assignment following an \@filename declaration
   will superseed the assignment in the corresponding file, and \a vice versa
   @bug Parsing of tokens in included files does not properly 
   deal with embedded white space in quoted strings.
*/
int cmdparams_parse (CmdParams_t *parms, int argc,  char *argv[]);

int cmdparams_parsefile (CmdParams_t *parms, char *filename, int depth);
void cmdparams_set (CmdParams_t *parms, const char *name, const char *value);
int cmdparams_exists (CmdParams_t *parms, char *name);
void cmdparams_printall (CmdParams_t *parms);
char *cmdparams_getarg (CmdParams_t *parms, int num);
void cmdparams_usage (char *name);
int cmdparams_numargs (CmdParams_t *parms);

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
int cmdparams_get_dblarr(CmdParams_t *parms, char *name, double **arr, int *status);
						/*  Generic integer version  */
int cmdparams_get_int (CmdParams_t *parms, char *name, int *status);
int cmdparams_get_intarr(CmdParams_t *parms, char *name, int **arr, int *status);

/**
Returns a pointer to the associated string.
On a failure, such as inability to locate the named key or to successfully
parse the associated value string according to the prescribed datatype,
returns a NULL pointer.

@bug If cmdparams_get_str (which is called by all the other versions of
\a cmdparams_get_XXX) cannot find a hash table entry for the requested
name, it will use the corresponding environment variable, if it exists.
*/
char *cmdparams_get_str (CmdParams_t *parms, char *name, int *status);

/** 
Returns a double representing the internal time representation of a 
parsed date-time string.
*/
double cmdparams_get_time (CmdParams_t *parms, char *name, int *status);
/* @} */

/* get original cmd-line params */
void cmdparams_get_argv(CmdParams_t *params, char ***argv, int *argc);

void cmdparams_reserve(CmdParams_t *params, const char *reserved, const char *owner);

int cmdparams_isreserved(CmdParams_t *params, const char *key);

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

void cmdparams_freeall (CmdParams_t *parms);

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

#endif
