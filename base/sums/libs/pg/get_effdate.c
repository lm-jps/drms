#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

/* Return a date as a malloc'd string in yyyymmddhhmm format that is plusdays 
 * from now.  
*/
char *get_effdate(int plusdays)
{
  struct timeval tvalr;
  struct tm *t_ptr;
  time_t newtime;
  char *timestr;

  if(gettimeofday(&tvalr, NULL) == -1) {
    return("200712121212");     /* give a phoney return */
  }
  t_ptr = localtime(&tvalr.tv_sec);
  t_ptr->tm_mday = t_ptr->tm_mday + plusdays;
  newtime = mktime(t_ptr);
  t_ptr = localtime(&newtime);
  timestr = (char *)malloc(16);
  sprintf(timestr, "%04d%02d%02d%02d%02d", 
	t_ptr->tm_year+1900, t_ptr->tm_mon+1, t_ptr->tm_mday, 
	t_ptr->tm_hour, t_ptr->tm_min);
  return(timestr);
}

/* Return pointer to "Wed Jun 30 21:49:08 1993\n" */
char *get_datetime()
{
  struct timeval tvalr;
  struct tm *t_ptr;
  static char datestr[32];

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr.tv_sec);
  sprintf(datestr, "%s", asctime(t_ptr));
  return(datestr);
}

