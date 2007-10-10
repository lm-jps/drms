#ifndef FLIB
#ifndef IDLLIB
#ifdef DEBUG_MEM
#include "xmem.h"
#endif
#include "cfortran.h"
#include "jsoc_main.h"

int CallDoIt()
{
   return DoIt();
}

int main(int argc, char **argv) 
{
   int status = JSOCMAIN_Main(argc, argv, module_name, CallDoIt);
   _exit(status);
}
#endif /* !IDLLIB */
#endif /* !FLIB */
