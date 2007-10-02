/* convertFDS.c */

/* Binary to test ConvAndInterpFDS in libastro.a.  At this point, ConvAndInterpDFS
   simply regurgitates the records specified and does not manipluate them in any way.
 */

#include "jsoc_main.h"
#include "cmdparams.h"
#include "astro.h"


ModuleArgs_t module_args[] =
{
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_STRING, "series", "NOT SPECIFIED", "series name"},
  {ARG_STRING, "dateRange", "NOT SPECIFIED", "date ranges separated by commas"},
  {ARG_END}
};

char *module_name = "convertFDS";
int nice_intro ()
{
     int usage = cmdparams_get_int(&cmdparams, "h", NULL);
     if (usage)
     {
	  printf ("Usage:\n\tconvertFDS [-h] "
		  "[series=<seriesname>] [dateRange=<daterange>]\n"
		  "  details are:\n"
		  "  -h: help - show this message then exit\n"
		  "  <seriesname> - fully qualified series name.\n"
		  "  <daterange> - record set filter.\n"
		  "  The series name defaults to sdo.fdsStateVectors.\n"
		  "  The date range defulats to all records.\n"
		  "  example - convertFDS series=su_arta.testseries dateRange=2006.11.20_22:38:00-2006.11.20_22:45:00,2006.11.20_22:52:00.\n");
	  return(1);
     }
     return (0);
}

int DoIt(void)
{
     char *series = cmdparams_get_str(&cmdparams, "series", NULL);
     char *dataRange = cmdparams_get_str(&cmdparams, "dateRange", NULL);

     if (nice_intro())
     {
	  return(0);
     }

     if (strcmp(series, "NOT SPECIFIED") == 0)
     { 
	  series = NULL;
     }

     if (strcmp(dataRange, "NOT SPECIFIED") == 0)
     {
	  dataRange = NULL;
     }

     return ConvAndInterpFDS(drms_env, series, dataRange);
}
