#include "drms.h"
#include <printk.h>
#ifdef __linux__
#include <sched.h>
#endif

ModuleArgs_t module_args[] =
{
   {ARG_INTS, "sunum", NULL , "SU number", ""},
   {ARG_INT, "retention", NULL, "Retention in days", ""},
   {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;
CmdParams_t cmdparams;

int main(int argc, char *argv[])
{

  int            sunumvals  = 0;
  long long      *sunum;
  char           key[4000];
  int            retention  = 0;
  int            i          = 0;

  char *sudir=(char *) NULL;
  SUM_t *sum=NULL;
  uint64_t *dsixpt;
  int status = 0;

  if ((status = cmdparams_parse(&cmdparams, argc, argv)) < CMDPARAMS_SUCCESS)
  {
    fprintf(stderr,"Error: Command line parsing failed. Aborting. [%d]\n", status);
    return -1;
  }



  if ((sum = SUM_open(NULL, NULL, printkerr)) == NULL)
  {
    fprintf(stderr,"ERROR: drms_open: Failed to connect to SUMS.\n");
    fflush(stdout);
    return -2;
  }

  sum->mode = TEMP + TOUCH;
  sum->tdays=retention;
  dsixpt = sum->dsix_ptr;

  fprintf (stdout, "sunumvals = %d\n", sunumvals = cmdparams_get_int64 (&cmdparams, "sunum_nvals",NULL));
  sum->reqcnt=sunumvals;
  sunum = (long long *)malloc (sunumvals * sizeof (long long));
  for (i = 0; i < sunumvals; i++) {
    sprintf (key, "sunum_%d_value", i);
    sunum[i] = cmdparams_get_int64 (&cmdparams, key,NULL);
    dsixpt[i] = sunum[i];
    printf ("sunum[%d] = %llu\n", i, sunum[i]);
  }

  retention   = cmdparams_get_int64(&cmdparams, "retention", NULL);

  /* Make RPC call to the SUM server. */
  if ((status = SUM_get(sum, printkerr)))
  {
    fprintf(stderr,"ERROR: SUM_getdo RPC call failed with "
               "error code %d\n", status);
               
               if (sum)
    {
        SUM_close(sum, printkerr);
    }
    
    return -3;
  }

  for (i =0; i< sunumvals; i++) {
    sudir = strdup(sum->wd[i]);
    fprintf(stdout, "sunum:%llu;sudir:%s\n",sunum[i],sudir);
  }

  SUM_close(sum, printkerr);

  free(sum->wd[0]);

  return 0;
}

