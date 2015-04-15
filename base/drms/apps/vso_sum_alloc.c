#include "drms.h"
#include <printk.h>
#ifdef __linux__
#include <sched.h>
#endif

ModuleArgs_t module_args[] =
{
   {ARG_INT, "sunum", NULL , "SU number", ""},
   {ARG_INT, "size", NULL, "Size in bytes of the SU", ""},
   {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;
CmdParams_t cmdparams;

int main(int argc, char *argv[])
{

  long long    sunum     =   0; //20451940;
  uint64_t     size      =   0; //34554432;
  char        *sudir     =   (char *)  NULL;
  SUM_t       *sum       =   (SUM_t *) NULL;
  int          status    =   0;

  if ((status = cmdparams_parse(&cmdparams, argc, argv)) < CMDPARAMS_SUCCESS)
  {
    fprintf(stderr,"Error: Command line parsing failed. Aborting. [%d]\n", status);
    return -1;
  }

  sunum       = cmdparams_get_int64(&cmdparams, "sunum", NULL);
  size        = cmdparams_get_int64(&cmdparams, "size", NULL);

  if ((sum = SUM_open(NULL, NULL, printkerr)) == NULL)
  {
    fprintf(stderr,"ERROR: drms_open: Failed to connect to SUMS.\n");
    fflush(stdout);
    return -2;
  }

  sum->reqcnt = 1;
  sum->bytes = size;

  /* Make RPC call to the SUM server. */
  if ((status = SUM_alloc2(sum, sunum, printkerr)))
  {
    fprintf(stderr, "ERROR: SUM_alloc2 RPC call failed with "
               "error code %d\n", status);
               
    if (sum)
    {
        SUM_close(sum, printkerr);
    }
    
    return -3;
  }

  sudir = strdup(sum->wd[0]);
  fprintf(stdout, "sunum:%llu;size:%llu;sudir:%s\n",(unsigned long long)sunum,(unsigned long long)size,sudir);


  SUM_close(sum, printkerr);

  if (sum->wd[0]) {
    free(sum->wd[0]);
  }

  if (sudir) {
    free(sudir);
  }


  return 0;
}

