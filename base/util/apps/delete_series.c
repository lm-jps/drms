/**
\defgroup delete_series delete_series - delete an existing DRMS series
@ingroup drms_util

\brief Delete an existing DRMS series

\par Synopsis:
\code
delete_series [-GEN_FLAGS] seriesname
\endcode

Removes the data series from the DRMS database. This program SHOULD BE USED
WITH GREAT CARE.  The program will ask you twice to confirm that you
REALLY want to remove all records. If data segments have been made and
if they are marked with a large retention or are archivable, data
storage will remain occupied by the unreachable data. This will be
examined later and this command will be removed or modified to prevent
garbage from being left behind.

\par GEN_FLAGS: 
Ubiquitous flags present in every module.
\ref jsoc_main

\param seriesname

\bug 
Does not free non-archived storage units in SUMS

\sa
show_info create_series modify_series

*/
#include "drms.h"
#include "jsoc_main.h"

#define kKeepSums "k"

/* Command line parameter values. */
ModuleArgs_t module_args[] = { 
   {ARG_FLAG, kKeepSums, NULL, "If set, then the SUMS files belonging to the series will not be removed.", NULL},
   {ARG_END}
};

char *module_name = "delete_series";
int DoIt(void) {
  int len;
  const char *series;
  char yesno[20];
  char *series_lower;
  int keepsums = 0;
  int drmsstatus = DRMS_SUCCESS;
					    /* Parse command line parameters */
  if (cmdparams_numargs (&cmdparams) < 2) goto usage;
							  /* Get series name */
  series  = cmdparams_getarg (&cmdparams, 1);
  series_lower = strdup(series);
  
  keepsums = cmdparams_isflagset(&cmdparams, kKeepSums);

  /*See if series exists before going through any other checks */
  if (!drms_series_exists(drms_env, series_lower, &drmsstatus))
  { printf("***** The series %s does not exist, and cannot be removed. *****\n Please try again with a valid series name\n", series);
      return 1;
  } 

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
      if (!drms_delete_series(drms_env, series, 1, keepsums))
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
    
