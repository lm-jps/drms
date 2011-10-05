#include "drms_defs.h"
#include "defs.h"


void drms_defs_term()
{
   defs_term();
}

const char *drms_defs_getval(const char *key)
{
   return defs_getval(key);
}
