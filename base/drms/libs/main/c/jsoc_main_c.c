#ifndef FLIB
#ifndef IDLLIB
#ifdef DEBUG_MEM
#include "xmem.h"
#endif
#include "jsoc_main.h"

int CallDoIt()
{
   return DoIt();
}

int main(int argc, char **argv) 
{
   return JSOCMAIN_Main(argc, argv, module_name, CallDoIt);
}
#endif /* !IDLLIB */
#endif /* !FLIB */
