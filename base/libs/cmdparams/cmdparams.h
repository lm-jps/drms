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
#include "list.h"
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
#define CMDPARAMS_MAXARGNAME (512)
#define CMDPARAMS_MAGICSTR "CpUnNamEDaRg"

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

/** @brief DRMS structure for holding parsed command-line-argument default values */
struct ModuleArgs_struct {
  /** @brief Data type of argument. */
  ModuleArgs_Type_t type;
  /** @brief Name of argument (where <name>=<value> appears on the command line. */
  char *name;
  /** @brief Value of argument (where <name>=<value> appears on the command line. */
  char *value;
  /** @brief Free-form descriptive string. */
  char *description;
  /** @brief For type==ARG_NUME, this is a list of comma-separated string tokens that name the enum symbols. For type==ARG_INT, ARG_FLOAT, or ARG_DOUBLE, this contains a range of values. */
  char *range;
};
/** 
    @brief ModuleArgs struct reference 
    @example moduleargs_ex1.c
*/
typedef struct ModuleArgs_struct ModuleArgs_t; 

/* Structure containing a single argument's information */
/* If a whitespace delimited argument has appeared on the command line, then 
 * a CmdParams_Arg_t structure is created for that argument. */
struct CmdParams_Arg_struct
{
  char *name;             /* If unnamed, then name is NULL. Unnamed args are still hashed - the key is a magic string followed by the 
                           * number of the unnamed argument. */
  int unnamednum;         /* If unnamed, the 0-based number of the argument. */
  char *cmdlinestr;       /* IFF this argument was parsed out of a cmd-line or file on a cmd-line, then 
                           * this contains the original argument from the argv vector. */
  ModuleArgs_Type_t type; /* The type of argument (eg., double, void, string, dataset, etc.). This gets set ONLY if 
                           * something has specified an expected data type. Typically, this would be done 
                           * by the code that handles default values.*/
  char *strval;           /* This is the type-indeterminate value of the argument (stored as a string). */
  void *actvals;          /* An array of values resolved into a 'data' type (a cache, so we don't resolve repeatedly). 
                           * May have just one value. */
  int nelems;             /* Number of elements in actvals. */
  int8_t accessed;        /* 1 if calling code accessed the argument with a cmdparams_get...() call. */
  int8_t casesensitive;   /* 1 if argument is stored in the args struct in a case-sensitive manner. Arguments of 
                           * the form "--ARGUMENT" are stored in a case-insensitive manner (they are stored
                           * in lower-case from, but the find operation should be performed in a 
                           * case-insensitive manner). To find such a keyword, code must first search for 
                           * the key sensitive to case. Then if the search fails, a case-insensitive search
                           * should be performed. Then if an arg is found and the casesensitive flag is set, 
                           * the search should be considered a "miss". But if the casesensitive flag is 
                           * not set, then the search should be considered a "hit". */
};

typedef struct CmdParams_Arg_struct CmdParams_Arg_t;

/** @brief Command-line parameter structure */
struct CmdParams_struct {
  HContainer_t *args;       /* A hash of CmdParams_Arg_t structures, one struct per arg. */
  CmdParams_Arg_t **argarr; /* Array of pointers into the arg structs in the args container. 
                             * argarr[i] points to the ith argument added to the args container. */
  int szargs;               /* Current number of allocated slots in argarr. */
  int argc;                 /* original argc passed to main(). */
  char **argv;              /* contains ALL cmd-line args unparsed (so the @filerefs are not resolved). */
  int numargs;              /* Total number of args (named or unnamed). */
  int numunnamed;           /* Number of unnamed arguments in param structure. */
  HContainer_t *reserved;   /* A hash of reserved keyword names (names that the program may not use - specified by
                             * the program itself). */
  HContainer_t *defps;
};
/** @brief CmdParams struct reference */
typedef struct CmdParams_struct CmdParams_t;

/**
   Parse the command line and the module's default arguments structure, creating 
   and populating a hash table, indexed by argument name, that contains 
   all the arguments specified.  All values in the hash table are strings, 
   regardless of the declared type of the argument. Parsing proceeds as follows:

   1. All tokens after the first in the command line (@a argv) are parsed for
   flags, argument name-value pairs, file references (@filename in the table below - 
   the file contains arguments, which are then parsed as well), or unnamed
   values.

   Command-line tokens can take any of the following forms:
   <TABLE>
   <TR>
   <TD>\a --name</TD> 
   <TD>'name' is the multi-byte name of a flag. If this token is present on the command-line, 
   the flag named 'name' is 'set'. ::cmdparams_isflagset(parms, "name") will return 1. 
   name cannot begin with a digit</TD>
   </TR>
   <TR>
   <TD>\a --name value</TD> 
   <TD>'name' is the name of an argument, and 'value' is the value of that argument. 
   cmdparams_get_XXX() will convert the string value to the desired type and return that value.
   'name' cannot begin with a digit.</TD>
   </TR>
   <TR>
   <TD>\a -\<chars></TD>
   <TD><chars> is a set of one or more characters. If such characters are present on 
   the command-line, then the flags named by each of those characters are 'set'. </TD>
   </TR>
   <TR>
   <TD>\a name=[]value</TD>
   <TD>'name' is the name of an argument, and 'value' is the value of that argument. 
   'name' cannot begin with a digit. White space following the \a = is optional</TD>
   </TR>
   <TR>
   <TD>\a \@filename</TD>
   <TD>'filename' specifies the name of a file containing tokens to be parsed in the same way the
   cmd-line is parsed.</TD>
   </TR>
   <TR>
   <TD>\a value</TD>
   <TD>unnamed values are also stored in the hash table. They can be accessed by providing 
   the argument number to the API functions (eg., if 'value' is the value of the nth
   argument, then that value can be accessed by providing n as the argument number, as
   in ::cmdparams_getarg(parms, n)).</TD>
   </TR>
   </TABLE>

   2. The declared ::module_args structure provides a mechanism for declaring, in module code,
   the arguments expected to appear on the cmd-line. The declaration includes the 
   argument data type and its default value, among other attributes (see ::module_args),
   although cmdparams uses these attributes inconsistently (different argument types
   make use of different sets of these attributes).
   Arguments provided on the cmd-line, but
   not in ::module_args, should be ignored by the module code. After parsing the cmd-line, 
   cmdparams then checks the ::module_args to ensure that all arguments specified in 
   ::module_args were indeed provided on the cmd-line. Should an argument be missing, cmdparams
   assigns the default value to the argument. If an argument is missing and no default 
   value is provided in ::module_args, cmdparams issues a fatal error (with an exception for
   the ARG_FLAG type - see table below). Arguments without default values are required to
   be specified on the cmd-line.
   NOTE: most of the type declarations are meaningless (eg., ARG_INT, ARG_DOUBLE) as cmdparams stores
   arguments in their string form, and only converts to a numerical type when a 
   cmdparams_get_XXX() function is called.  A few of the type declarations do have meaning:

   <TABLE>
   <TR>
   <TD>@a ARG_FLAG</TD> 
   <TD>Flags are not subject to the requirement that a default value be provided
   in the ::module_args structure. This means that flags cannot be made to be
   required arguments. It also means that specifying a default value for a flag
   has no effect. To query if a flag is set, ::cmdparams_isflagset() can be used.</TD>
   </TR>
   <TR>
   <TD>@a ARG_NUME</TD>
   <TD>The range field of ::module_args specifies a comma-separated list of acceptable enumeration
   ids. Each id in the list is associated with an integer (the enumeration value,
   which starts at 0 for the first value in the list, and then increases by 1 for each
   successive item in the list). If the range field includes "myid1, myid2, myid3", 
   then myid1 is associated with 0, myid2 is associated with 1, and myid3 is associated with 3.
   To obtain the value of an ARG_NUME argument, call ::cmdparams_get_int(). The value
   returned is the enumeration value. Putting this all together, if the range field of 
   the color argument contains "red,orange,yellow,green,blue,purple", and the cmd-line contains
   color=green, ::cmdparams_get_int() returns 3, because the id red is associated with the value 0, 
   orange is associated with 1, etc. cmdparams will fail if the cmd-line specifies
   a value for an ARG_NUME argument that is not in the list specified in the range field 
   (eg, color=aqua will cause a failure). This argument type is
   designed especially for use with driver programs that can provide menus of
   options, such as CGI forms.</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLOATS, @a ARG_DOUBLES, @a ARG_INTS</TD>
   <TD>The cmd-line for an argument of this type contains a comma-separated list of values.
   cmdparams will parse this list and store each value in the hash table under
   the name <argname>_<n>_value, where <argname> is the name of the argument in 
   ::module_args and <n> is the 0-based index into the list. 
   The number of elements in the original list is stored in a 
   new hash-table item keyed by the string <argname>_nvals. 
   For example, if "lat=0.0,5.0,10.0" is provided on the cmd-line, and the following ::module_args element 
   is present: {ARG_FLOATS, "lat", "", "", ""}, ::params_get_int (params, "lat_nvals") 
   would return 3 and ::params_get_float (params, "lat_1_value") would return the value 5.0 . The number
   of array values supplied at run time need not match the number in the 
   speficied in the value field of the ::module_args element.
   In addition, an entire 
   array of values in their specified type is stored (if the 
   type of argument is ARG_INTS, then the values are stored as ints - NOT as strings). To obtain
   this array, ::cmdparams_get_intarr(), ::cmdparams_get_flaotarr(), or ::cmdparams_get_doublearr()
   can be used.</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLOAT, @a ARG_DOUBLE, @a ARG_INT</TD>
   <TD>The range field of ::module_args specifies a range of acceptable values. Should a cmd-line value 
   for this argument be provided that is not within this range, 
   cmdparams fails with an out-of-range error. The range is a string
   with the following grammar (curlies denote optional elements, single quotes denote 
   literals, '|' denotes alternation):<BR><BR>
   { '(' | '[' } { <number1> } ',' { <number2> } { ')' | ']' } <BR><BR>
   () denote open endpoints, [] denote closed endpoints (default is closed), <number1> and
   <number2> are a real numbers, the lower and upper endpoints of the range. If <number1> is
   omitted, then -infinity is assumed. If <number2> is omitted, then +infinity is assumed.</TD>
   </TR>
   </TABLE>
   
   If any declared module argument (in ::module_args) cannot be assigned a value 
   during the above process, processing of the argument list ceases and the function returns
   the value -1. ::ARG_FLAG arguments are exempt from this rule - it is not a requirement
   that processing results in any flag to be set.

   If a parameter value (or name) has embedded white space, it must be quoted.

   An extern @ref module_args declaration is required, though it can be empty.
   A member with a ::module_args.type of ::ARG_END, must terminate the
   the list of parsed members; any members following it in the declarator
   will be ignored.

   @bug Multiple assignments to a given parameter name 
   could result in unpredictable
   values. The last token in the command line should be parsed last and take
   precedence. Thus, for example, an assignment following an \@filename declaration
   will supersede the assignment in the corresponding file, and @a vice @a versa
   @bug Parsing of tokens in included files does not properly 
   deal with embedded white space in quoted strings.
*/
int cmdparams_parse (CmdParams_t *parms, int argc,  char *argv[]);

int cmdparams_parsefile (CmdParams_t *parms, char *filename, int depth);
CmdParams_Arg_t *cmdparams_set (CmdParams_t *parms, const char *name, const char *value);
int cmdparams_exists (CmdParams_t *parms, char *name);
void cmdparams_printall (CmdParams_t *parms);
const char *cmdparams_getarg (CmdParams_t *parms, int num);
const char *cmdparams_getargument(CmdParams_t *parms, 
                                  int num, 
                                  const char **name, 
                                  const char **value, 
                                  const char **cmdlinestr, 
                                  int *accessed);
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
double cmdparams_get_double (CmdParams_t *parms, const char *name, int *status);
int cmdparams_get_dblarr(CmdParams_t *parms, char *name, double **arr, int *status);
						/*  Generic integer version  */
int cmdparams_get_int (CmdParams_t *parms, char *name, int *status);
int cmdparams_get_intarr(CmdParams_t *parms, char *name, int **arr, int *status);
int cmdparams_get_int64arr(CmdParams_t *parms, char *name, int64_t **arr, int *status);

/**
Returns a pointer to the associated string.
On a failure, such as inability to locate the named key or to successfully
parse the associated value string according to the prescribed datatype,
returns a NULL pointer.

@bug If cmdparams_get_str (which is called by all the other versions of
\a cmdparams_get_XXX) cannot find a hash table entry for the requested
name, it will use the corresponding environment variable, if it exists.
*/
const char *cmdparams_get_str (CmdParams_t *parms, const char *name, int *status);

/** 
Returns a double representing the internal time representation of a 
parsed date-time string.
*/
double cmdparams_get_time (CmdParams_t *parms, char *name, int *status);
/* @} */

/* get original cmd-line params */
void cmdparams_get_argv(CmdParams_t *params, char *const **argv, int *argc);

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
const char *params_get_str (CmdParams_t *parms, char *name);
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
