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

  uint64_t sunum = 0; //20451940;
  uint64_t  size  = 0; //34554432;
  char *sudir=(char *) NULL;
  SUM_t *sum=NULL;
  DRMS_SumRequest_t *request;
  int status = 0;

  if ((status = cmdparams_parse(&cmdparams, argc, argv)) < CMDPARAMS_SUCCESS)
  {
    fprintf(stderr,"Error: Command line parsing failed. Aborting. [%d]\n", status);
    return -1;
  }

  sunum       = cmdparams_get_int64(&cmdparams, "sunum", NULL);
  size        = cmdparams_get_int64(&cmdparams, "size", NULL);

  XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));

  request->opcode = DRMS_SUMALLOC2; //Not required
  request->dontwait = 0;           //Not required
  request->reqcnt = 1;

  /* For some reason, this is a double in SUMS. */
  request->bytes = (double)size;
  request->sunum[0] = sunum;

  if ((sum = SUM_open(NULL, NULL, printkerr)) == NULL)
  {
    fprintf(stderr,"ERROR: drms_open: Failed to connect to SUMS.\n");
    fflush(stdout);
    return -2;
  }

  sum->reqcnt = request->reqcnt;
  sum->bytes = request->bytes;

  /* Make RPC call to the SUM server. */
  if ((status = SUM_alloc2(sum, request->sunum[0], printf)))
  {
    fprintf(stderr,"ERROR: SUM_alloc2 RPC call failed with "
               "error code %d\n", status);
    return -3;
  }

  sudir = strdup(sum->wd[0]);
  fprintf(stdout, "sunum:%llu;size:%llu;sudir:%s\n",(unsigned long long)sunum,(unsigned long long)size,sudir);

  SUM_close(sum,printf);

  free(sum->wd[0]);


  return 0;
}

