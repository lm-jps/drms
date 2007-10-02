/*
 *  hello_world.c				~jsoc/src/pipeline/example
 *
 *  An extremely simple module...
 *
 *    Echoes the message specified in the optional "msg" argument to stdout.
 *     
 *  Responsible:  Rick Bogart			RBogart@solar.stanford.edu
 *
 *  Usage:
 *    hello_world [msg= ]
 *
 *  Parameters: (type   default 	description)
 *	msg	String	"Hello,  world"	a string to be written
 *
 *  Bugs:
 *    The module is of no particular use, and exists merely for heuristic
 *      purposes.
 *
 *  Revision history is at end of file.
 */
						  /*  required .h inclusion  */
#include <jsoc_main.h>
#include <stdio.h>
						      /*  module identifier  */
char *module_name = "hello_world";
char *version_id = "1.5";
							 /*  arguments list  */
ModuleArgs_t module_args[] = {
  {ARG_STRING, "msg", "Hello, world!", "message to be echoed"},
  {}
};

int DoIt () {
  int status;

  if (cmdparams_exists (&cmdparams, "V"))
    printf ("output of module %s version %s:\n\n", module_name, version_id);
  printf ("%s\n", cmdparams_get_str (&cmdparams, "msg", &status));
  return (0);
}

/*
 *  Revision History
 *
 *  05.11.14	Rick Bogart	created this file
 *  06.09.05	"		converted to new module structure
 */
