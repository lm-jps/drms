#include "drms.h"
#include <printk.h>
#ifdef __linux__
#include <sched.h>
#endif

ModuleArgs_t module_args[] =
{
   {ARG_INT, "sunum", NULL , "SU number", ""},
   {ARG_STRING, "seriesname", NULL, "SU series name", ""},
   {ARG_STRING, "sudir", NULL, "SU number directory", ""},
   {ARG_INT, "retention", "3", "retention time", ""},
   {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;
CmdParams_t cmdparams;

int main(int argc, char *argv[])
{

  long long sunum = 0; //20451940;
  int  retention  = 60;
  char *sudir=(char *) NULL;
  char *seriesname=(char *) NULL;
  SUM_t *sum=NULL;
  DRMS_SumRequest_t *request;
  int status = 0;

  if ((status = cmdparams_parse(&cmdparams, argc, argv)) < CMDPARAMS_SUCCESS)
  {
    fprintf(stderr,"Error: Command line parsing failed. Aborting. [%d]\n", status);
    return -1;
  }

  sunum       = cmdparams_get_int64(&cmdparams, "sunum", NULL);
  sudir       = strdup(cmdparams_get_str(&cmdparams, "sudir", NULL));
  seriesname  = strdup(cmdparams_get_str(&cmdparams, "seriesname", NULL));
  retention   = cmdparams_get_int(&cmdparams, "retention", NULL);

  fprintf(stdout,"sudir=%s\n",sudir);
  fprintf(stdout,"seriesname=%s\n",seriesname);
  XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));

  request->opcode = DRMS_SUMPUT;   //Not required
  request->dontwait = 0;           //Not required
  request->reqcnt = 1;
  request->dsname = seriesname;
  request->group  = 0;
  request->mode   = TEMP + TOUCH;
  request->tdays  = retention;     //seriesinfo->retention;
  request->sunum[0] = sunum;
  request->sudir[0] = sudir;

  if ((sum = SUM_open(NULL, NULL, printkerr)) == NULL)
  {
    fprintf(stderr,"ERROR: drms_open: Failed to connect to SUMS.\n");
    fflush(stdout);
    return -2;
  }

  sum->dsname = request->dsname;
  sum->group = request->group;
  sum->mode = request->mode;
  sum->tdays = request->tdays;
  sum->reqcnt = request->reqcnt;
  sum->history_comment = request->comment;

  sum->reqcnt = request->reqcnt;
  sum->dsix_ptr[0] = sunum;
  sum->wd[0] = sudir;
fprintf(stderr,"Before SUM_put call\n");
  /* Make RPC call to the SUM server. */
  if ((status = SUM_put(sum, printf)))
  {
    fprintf(stderr,"ERROR: SUM_put RPC call failed with "
               "error code %d\n", status);
    return -3;
  } else {
    fprintf(stdout,"DRMS_SUMPUT:%lld OK\n", sunum);
    fflush(stderr);
    fflush(stdout);
  }

  if (seriesname)
  {
     free(seriesname);
  }
  
  if (sudir)
  {
     free(sudir);
  }

  if (request)
  {
     free(request);
  }

  SUM_close(sum,printf);

  return 0;
}

