/**
@file jsoc_main.h
@brief Global variables and functions for creating a DRMS module.

These functions handle DRMS session startup and shutdown, cmd-line parameter 
parsing, and default-parameter initialization.

@sa ::module ::module_args
*/

#ifndef __MAIN_H
#define __MAIN_H
#include "timeio.h"
#include "jsoc.h"
#include "timer.h"
#include "xassert.h"
#include "drms_types.h"
#include "cmdparams.h"
#include "drms_env.h"
#include "drms_series.h"
#include "drms_keyword.h"
#include "drms_keymap.h"
#include "drms_keymap_priv.h"
#include "drms_link.h"
#include "drms_record.h"
#include "drms_segment.h"
#include "drms_array.h"
#include "drms_protocol.h"
#include "drms_statuscodes.h"
#include "drms_parser.h"
#include "drms_network.h"
#include "drms_names.h"
#include "drms_fits.h"
#include "util.h"
#include "drms_dsdsapi.h"

extern CmdParams_t cmdparams;
/* Global DRMS Environment handle. */
extern DRMS_Env_t *drms_env;

/*  Default module arguments  */
/**
   @addtogroup jsoc_main
*/
/* @{ */
/**
   @brief Global DRMS-module structure representing the default
   command-line arguments for a DRMS module. 

   ::module_args, a global array of ::ModuleArgs_t structures, provides 
   a standard mechanism for declaring the parameters expected to be used 
   by a module along with their types and default values, if any. 
   ::module_args must be declared in every module.
   The elements of the ::module_args
   array are parsed and compared with arguments supplied to the module from
   the command line or other invocation to produce a ::CmdParams_t structure 
   (::cmdparams) through which their values are 
   available through the @a params_get suite
   of functions.

   The @a module_args declarator requires at least one element, which must be
   of type ::ARG_END (which is 0, so an empty initializer as shown in
   the synopsis is acceptable). Any array elements @e following the
   ::ARG_END element are ignored.

   Although the default value (and range, if applicable) is supplied as
   a character string, it will be interpreted according to the declared type
   of the argument. Each element of ::cmdparams, except those of type ::ARG_VOID, 
   must have a @a name field. Arguments of type ::ARG_INT, ::ARG_FLOAT, ::ARG_DOUBLE,
   and ::ARG_STRING should be self-explanatory. Arguemts of type ::ARG_FLAG
   are expected to have single-character names and to be associated with
   logical binaries, with a default value of @c FALSE (0); they can be set on
   the command line via the @a -X construct (where @a X is the name of
   the element to be set to @a TRUE). ::ARG_TIME is a special case of
   ::ARG_DOUBLE, whose default or assigned values are interpreted by
   ::sscan_time (@e q.v.). ::ARG_VOID is reserved for use with
   undeclared arguments supplied on the command line; it should not be used
   for declared arguments in the @a module_args list.

   The types ::ARG_INTS, ::ARG_FLOATS, and ::ARG_DOUBLES are used for
   parameters that can be arrays of arbitrary length. The values must be
   supplied as comma separated sets enclosed within matched delimiting
   pairs of either brackets [], braces {} or parentheses () (unless there
   is only one value in the array, in which case the delimiters are optional).
   The total number of elements in the array is returned as the added parameter
   @a name_nvals, and the value for the nth element (counting from 0) as
   @a name_n_value. For example, a @ module_args element declared as:

   @c  {ARG_FLOATS, "lat", "[0.0, 5.0, 10.0]", "", ""},

   would return 3 for ::params_get_int (params, "lat_nvals") and the
   value 5.0 for params_get_float (params, "lat_1_value"). The number
   of array values supplied at run time need not match the number in the 
   default;
   indeed there is no necessity of setting any default value at all, just as
   with other types of arguments.

   ::ARG_NUME is a special type of argument representing an enumeration
   class. It makes use of the @c module_args->range field, which must be a
   comma-separated list of strings. The value returned is an integer 
   coresponding
   to the order number of the range element matching the supplied value. For
   example, a @a module_args element declared as:

   @c  {ARG_NUME, "color", "green", "", "red, yellow, green, blue"},

   would return 2 for ::params_get_int (params, "color"). A failure occurs
   if the value supplied does not match anything in the range; the type is
   designed especially for use with driver programs that can provide menus of
   options, such as CGI forms.

   ::ARG_DATASET and ::ARG_DATASERIES are special cases of
   ::ARG_STRING reserved for names of DRMS dataset specifications or series
   names in an environment where the database can be queried for 
   possible values;
   they are not currently treated differently from any other type of string
   argument.

   ::ARG_NEWDATA does not appear to be implemented; ::ARG_NUMARGS is
   reserved for internal use by the Fortran interface and should not be used.

   To summarize, @c ::ModuleArgs_t->type must have one of the following values:

   <TABLE>
   <TR>
   <TD>@a ARG_INT</TD><TD>parameter is to be interpreted as type @a int</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLOAT</TD><TD>parameter is to be interpreted as type @a double</TD>
   </TR>
   <TR>
   <TD>@a ARG_DOUBLE</TD><TD>parameter is to be interpreted as type @a double</TD>
   </TR>
   <TR>
   <TD>@a ARG_TIME</TD><TD>parameter is to be interpreted as type @a double,
   with a conversion from standard date-time string formats to a standard 
   reference epoch</TD>
   </TR>
   <TR>
   <TD>@a ARG_STRING</TD><TD>parameter is to be interpreted as type @a char*</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLAG</TD><TD>the parameter is (ordinarily) a single-character named 
   one which can take the value of 0 or 1. The default value, if present, 
   should be 0; as the
   command-line flag specifier can only set its parameter values to 1; however,
   it is better to leave the default value empty, so that the
   ::cmdparams_exists function can be used in the code.</TD>
   </TR>
   <TR>
   <TD>@a ARG_NUME</TD><TD>the parameter value is string-compared with the members 
   of the @c module_args->range list, and replaced with the string representation
   of the number corresponding to the order number of the (first) matching
   token in the list; its value is subsequently to be interpreted as type
   @a int. Basically equivalent to type @a enum</TD>
   </TR>
   <TR>
   <TD>@a ARG_INTS</TD><TD>(not yet implemented)</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLOATS</TD><TD>(not yet implemented)</TD>
   </TR>
   <TR>
   <TD>@a ARG_DOUBLE</TD><TD>synonymous with ::ARG_FLOATS</TD>
   </TR>
   <TR>
   <TD>@a ARG_VOID</TD><TD>(not yet implemented)</TD>
   </TR>
   <TR>
   <TD>@a ARG_END</TD><TD>signals the end of the parsed argument list. 
   Elements may follow in the declaration, but will be ignored. 
   Since @a ARG_END is defined as 0, an empty (null) member serves the 
   same purpose.</TD>
   </TR>
   </TABLE>

   The @c module_args->description is intended to be used only by the
   front-end handler for documentation, such as when the command is invoked
   with a @a -H help flag, or in CGI web forms.

   @bug Range inspection is limited to arguments of type ::ARG_NUME and
   ::ARG_FLOAT.
   @sa @ref module cmdparams.h ::sscan_time
*/
extern ModuleArgs_t module_args[];
/**
   @brief Global DRMS-module pointer that refers to @ref module_args
*/
extern ModuleArgs_t *gModArgs;
/**
   @brief Global DRMS-module string providing the name of the module. 
*/
extern char *module_name;
/* @} */



/*  DoIt() Module entry point - defined in <modulename>.c */
extern int DoIt (void);

CmdParams_t *GetGlobalCmdParams(void);

int JSOCMAIN_Main(int argc, char **argv, const char *module_name, int (*CallDoIt)(void));
int JSOCMAIN_Init(int argc, 
		  char **argv, 
		  const char *module_name, 
		  int *dolog,
		  int *verbose,
		  pid_t *drms_server_pid, 
		  pid_t *tee_pid,
		  int *cont);
int JSOCMAIN_Term(int dolog, int verbose, pid_t drms_server_pid, pid_t tee_pid, int abort_flag);

#ifdef FLIB
void f_cmdparams_get_handle(pFHandleCmdParams handle);
char *f_cmdparams_gethandle2();
#endif

#endif
