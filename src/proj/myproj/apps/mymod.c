#include "jsoc_main.h"
#include "drms_types.h"

char *module_name = "mymod";

typedef enum
{
   kMymodErr_Success,
   kMymodErr_Whatever
} MymodError_t;

#define kRecSetIn      "recsin"
#define kDSOut         "dsout"

ModuleArgs_t module_args[] =
{
     {ARG_STRING, kRecSetIn, "",  "Input data series."},
     {ARG_STRING, kDSOut,    "",  "Output data series."},
     {ARG_END}
};

int DoIt(void) 
{
   int status = DRMS_SUCCESS;

   /* blah, blah, blah, etc. */

   return (status == DRMS_SUCCESS) ? kMymodErr_Success : kMymodErr_Whatever;
}
