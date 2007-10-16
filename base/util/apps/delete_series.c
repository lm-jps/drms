#include "drms.h"
#include "jsoc_main.h"

/* Command line parameter values. */
ModuleArgs_t module_args[] = { 
  {ARG_END}
};

char *module_name = "delete_series";

int DoIt(void) {
  int len;
  char *series;
  char yesno[20];
					    /* Parse command line parameters */
  if (cmdparams_numargs (&cmdparams) < 2) goto usage;
							  /* Get series name */
  series  = cmdparams_getarg (&cmdparams, 1);

  /* Remove existing series */
  printf("You are about to permanently erase all metadata for the series "
	 "'%s'.\nAre you sure you want to do this (yes/no)? ",series);
  fgets(yesno,10,stdin);
  len = strlen(yesno);
  if (yesno[len-1]=='\n')
    yesno[len-1]=0;
  if (!strcmp(yesno,"yes"))
  {
    memset(yesno,0,10);
    printf("I repeat: All data records from '%s' will be erased.\n"
	   "Are you REALLY sure you want to do this (yes/no)? ",series);
    fgets(yesno,10,stdin);
    len = strlen(yesno);
    if (yesno[len-1]=='\n')
      yesno[len-1]=0;
    if (!strcmp(yesno,"yes"))
    {
      printf("Removing existing series '%s'...\n",series);  
      if (!drms_delete_series(drms_env, series, 1))
	return 0;
      else
	printf("'%s': Failed to delete DRMS series.\n",series);
    } else 
      printf("Series %s is not removed.\n", series);
  } else 
    printf("Series %s is not removed.\n", series);

  return 0;

  usage:
  printf("Usage: %s seriesname\n",cmdparams_getarg(&cmdparams, 0));
  return 1;
}
    
