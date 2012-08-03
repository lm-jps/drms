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
#include "drms_link.h"
#include "drms_record.h"
#include "drms_segment.h"
#include "drms_array.h"
#include "drms_protocol.h"
#include "drms_statuscodes.h"
#include "drms_parser.h"
#include "drms_network.h"
#include "drms_names.h"
#include "util.h"
#include "drms_dsdsapi.h"
#include "drms_defs.h"

#define kARCHIVEARG "DRMS_ARCHIVE"
#define kRETENTIONARG "DRMS_RETENTION"
#define kQUERYMEMARG "DRMS_QUERY_MEM"
#define kSERVERWAITARG "DRMS_SERVER_WAIT"
#define kLoopConn "loopconn"

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
   a standard mechanism for declaring the parameters expected 
   by a module along with their types and default values, if any. 
   ::module_args must be declared in every module.
   The elements of the ::module_args
   array are parsed and used by the cmdparams library to
   populate the global ::CmdParams_t structure, which provides access
   to cmd-line arguments (please see ::cmdparams_parse
   for more information about how the cmdparams library uses ::module_args).

   The ::module_args array must contain at least one element, which must be
   of type ::ARG_END. Any array elements @e following the
   ::ARG_END element are ignored. Each element has five fields: type, name, 
   (default) value, description, and range. The value field contains
   the actual default value for the argument should the argument be ommitted from
   the cmd-line. All fields are strings, and the type
   field is required to not be NULL. The description 
   field, which provides a terse summary of the purpose of the argument,
   is always optional. But the disposition of the name, value, and range fields 
   depends on the type of argument (see the table below; 
   for example, for the ::ARG_FLAG, the 
   value field is not used, but for ::ARG_INT, that field is used).

   If applicable, the default value and range are specified as strings
   in the ::module_args array. And they are stored in the ::CmdParams_t structure
   as strings. But by specifying a type, such as ::ARG_INT, this declares 
   that the module intends to interpret the the string as a specific 
   data type (such as an int).  However, it is important to note that 
   there is NOTHING to prevent the module from interpreting the 
   stored string as any type it desires. The module's code performs this
   interpretation when it uses one of the cmdparams_get_XXX() functions - the type
   in the name of the function will cause the stored string to be interpreted 
   as a definite data type. For example, if a type of ::ARG_INT is specified, the
   module should call ::cmdparams_get_int(), but not ::cmdparams_get_time() to
   obtain and interpret the value.

   The following table summarizes the various ::ModuleArgs_t argument types,
   providing guidelines for their specification in the global ::module_args array and
   their use in on the cmd-line:

   <TABLE>
   <TR>
   <TD>@b Arg @b Type</TD>
   <TD>@b required @b fields</TD>
   <TD>@b optional @b fields</TD>
   <TD>@b Description</TD>
   </TR>
   <TR>
   <TD>@a ARG_INT</TD>
   <TD>type, name</TD>
   <TD>value, range, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as type @a int. 
   If the value field is present, it must contain an ascii string that can be converted 
   to an integer. If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present. The range
   field is optional.  If present, it must contain an ascii string that specifies a range
   of real numbers (see ::cmdparams_parse).</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLOAT</TD>
   <TD>type, name</TD>
   <TD>value, range, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as type @a float.  
   If the value field is present, it must contain an ascii string that can be converted to a float. 
   If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present. The range
   field is optional.  If present, it must contain an ascii string that specifies a range
   of real numbers (see ::cmdparams_parse).</TD>
   </TR>
   <TR>
   <TD>@a ARG_DOUBLE</TD>
   <TD>type, name</TD>
   <TD>value, range, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as type @a double. 
   If the value field is present, it must contain an ascii string that can be converted to a double. 
   If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present. The range
   field is optional.  If present, it must contain an ascii string that specifies a range
   of real numbers (see ::cmdparams_parse).</TD>
   </TR>
   <TR>
   <TD>@a ARG_TIME</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as a time string (as defined
   in ::sscan_time).
   If the value field is present, it must contain an ascii time string.
   If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present.</TD>
   </TR>
   <TR>
   <TD>@a ARG_STRING</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as a character string.
   If the value field is present, it must contain an ascii string.
   If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present.</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLAG</TD>
   <TD>type, name</TD>
   <TD>description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as a binary flag. If the name
   of the flag argument contains a single character X, then to "set" the flag, provide "-X" on 
   the cmd-line. If the name of the flag contains more than a single character "name", then
   provide "--name" on the cmd-line. Do not provide a default value, as 
   cmdparams does not use the value field for flags. If the flag is not provided on the cmd-line
   the flag is by default not set.</TD>
   </TR>
   <TR>
   <TD>@a ARG_NUME</TD>
   <TD>type, name, range</TD>
   <TD>value, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as the string representation of 
   an enumeration id, as defined in the range field. If the value field is present, 
   it must contain the string representation of an enumeration id. 
   If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present.
   The range field contains 
   a comma-separated list of enumeration
   ids (strings). The first item in the list is associated with an @a integer value of 0, and 
   each subsequent member is associated with a value one greater than the previous id. The cmd-line
   argument must contain one of the member strings. See ::cmdparams_parse for more information.</TD>
   </TR>
   <TR>
   <TD>@a ARG_INTS</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as a comma-separated list of 
   integer values. 
   If the value field is present, it must contain an ascii string that can be converted 
   to one or more integers. If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present. See ::cmdparams_parse for more information.</TD>
   </TR>
   <TR>
   <TD>@a ARG_FLOATS</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>The cmd-line argument is to be interpreted by the module as a comma-separated list of 
   floating point values. 
   If the value field is present, it must contain an ascii string that can be converted 
   to one or more integers. If the value field is missing or if the field is the empty string,
   then the cmd-line argument must be present. See ::cmdparams_parse for more information.</TD>
   </TR>
   <TR>
   <TD>@a ARG_DOUBLE</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>Synonymous with ::ARG_FLOATS</TD>
   </TR>
   <TR>
   <TD>@a ARG_VOID</TD>
   <TD>?</TD>
   <TD>none</TD>
   <TD>(not yet implemented - not clear what this type is for)</TD>
   </TR>
   <TR>
   <TD>@a ARG_DATASERIES</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>Currently synonymous with ::ARG_STRING (but reserved for use with dataseries names that
   can be used in a database query).</TD>
   </TR>
   <TR>
   <TD>@a ARG_DATASET</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>Currently synonymous with ::ARG_STRING (but reserved for use with dataseries/record filter
   names that can be used in a database query).</TD>
   </TR>
   <TR>
   <TD>@a ::ARG_NEWDATA</TD>
   <TD>type, name</TD>
   <TD>value, description</TD>
   <TD>(not yet implemented - not clear what this type is for)</TD>
   </TR>
   <TR>
   <TD>@a ARG_END</TD>
   <TD>type</TD>
   <TD>none</TD>
   <TD>Signals the end of the parsed argument list. 
   Elements may follow in the array, but will be ignored.</TD>
   </TR>
   </TABLE>

   @bug Range inspection does not currently extend to arguments of type ::ARG_INTS, ::ARG_FLOATS, 
   and ::ARG_DOUBLES

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

int RegisterDoItCleaner(DRMS_Env_t *env, pFn_Cleaner_t cb, void *data);

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
