/* fix_hmi_config_file_date takes a single date string from a filename of an HMI test config
   file (e.g. PCU or FTS .usr file) and returns a date string with corrected time.
 */

#include <stdio.h>

int main(int argc, char **argv)
{
double date;
double sscan_time(char *ctime);
int sprint_time(char *ctime, double date, char*zone, int prec);
char newdate[100];
double corr;
int try;
double time_events[][2] = {
  sscan_time("2006.01.01"), 0.0,
/* list times and correction to add to file time starting at given times */
  sscan_time("2222.01.01"), 0.0};

if (argc != 2 || strcmp(argv[1], "-h")==0)
  {
  fprintf(stderr,"fix_hmi_config_file_date DATE\n"
	"Convert DATE to corrected DATE where DATE is e.g. 2007.06.18_00:12:10\n");
  exit(0);
  }

date = sscan_time(argv[1]);
if (date >= sscan_time("2222.01.01"))
  {
  fprintf(stderr,"Date absurd, %s\n", argv[1]);
  exit(1);
  }

/* fix date here */
corr = 0;
for (try=0; date >= time_events[try][0]; try++)
	corr = time_events[try][1];
date += corr;
sprint_time(newdate, date, "UTC", 0);
printf("%19.19s\n",newdate);
exit(0);
}
