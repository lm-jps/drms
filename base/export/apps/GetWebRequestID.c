#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>

/* GetWebRequestID - returns the next serial number for jsoc_info and jsoc_fetch requests.
 *  
 * Uses flock on a file (/home/jsoc/exports/WebRequestID) that contains the previous
 * WebRequestID.  Locks the file, reads prev number, writes next number, removes lock.
 * returns a single line on stdout containing the incremented number.
 * Format is <date>_<sequence number>.
 */

int main()
{
int RequestIDsn;
int old_date, new_date;
int year, month, day;
char RequestID[100];
int sleeps;
FILE *fp;
struct tm *now;
time_t nowtime;

fp = fopen("/home/jsoc/exports/WebRequestID", "r+");
if (!fp)
  {
  fprintf(stderr, "GetWebRequestID failed to open sn file.\n");
  exit(1);
  }

for(sleeps=0; lockf(fileno(fp),F_TLOCK,0); sleeps++)
  {
  if (sleeps >= 20)
    {
    fprintf(stderr,"Lock stuck on /home/jsoc/exports/WebRequestID, GetWebRequestID failed.\n");
    exit(1);
    }
  sleep(1);
  }

fscanf(fp,"%d_%d",&old_date,&RequestIDsn);

nowtime = time(0);
now = gmtime(&nowtime);
new_date = 10000*(now->tm_year+1900) + 100*(now->tm_mon+1) + now->tm_mday;
if (old_date != new_date)
  {
  FILE *history = fopen("/home/jsoc/exports/WebRequestID.history", "a");
  fprintf(history,"%d_%03d\n", old_date, RequestIDsn);
  fclose(history);
  RequestIDsn = 1;
  }
else
  RequestIDsn += 1;
rewind(fp);
sprintf(RequestID,"%d_%05d", new_date, RequestIDsn);

fprintf(fp,"%s\n",RequestID);
rewind(fp);
lockf(fileno(fp),F_ULOCK,0);
fclose(fp);
printf("Content-Type: text/plain\n\n%s",RequestID);
fflush(stdout);
}
