#define DEBUG 0

/*
 *  jsoc_stats1 - prints information about elapsed time of a jsoc_fetch call
 *
 */

/**
\defgroup jsoc_stats1 jsoc_stats1 - save client provided elapsed time for query
@ingroup su_export

Prints op, ds, user_IP, response_time, count, status for a jsoc_fetch call

\ref jsoc_stats1  is called by lookdata.html to record JSOC performance

\par Synopsis:

\code
jsoc_stats1  op=<command> ds=<record_set> n=<count> lag=<processing_secs> status=<status>
or
jsoc_stats1 QUERY_STRING=<url equivalent of command-line args above>
\endcode

\param command
The opcode sent to a jsoc_fetch call

\param record_set
The recordset spec sent to a jsoc_fetch call

\param count
the record limit sent to a jsoc_fetch call

\param processing_secs
The seconds in real time for jsoc_fetch to respond

\param status
The status returned by jsoc_fetch


\sa
jsoc_fetch lookdata.html

@{
*/
#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
#include "json.h"
#include <time.h>
#include <sys/types.h>
#include <unistd.h>

#define LOGFILE     "/home/jsoc/exports/logs/fetch_log"
#define kLockFile   "/home/jsoc/exports/tmp/lock.txt"

static char x2c (char *what)
  {
  char digit;
  digit = (char)(what[0] >= 'A' ? ((what[0] & 0xdf) - 'A')+10 : (what[0] - '0'));
  digit *= 16;
  digit = (char)(digit + (what[1] >= 'A' ? ((what[1] & 0xdf) - 'A')+10 : (what[1] - '0')));
  return (digit);
  }

static void CGI_unescape_url (char *url)
  {
  int x, y;
  for (x = 0, y = 0; url[y]; ++x, ++y)
    {
    if ((url[x] = url[y]) == '%')
      {
      url[x] = x2c (&url[y+1]);
      y += 2;
      }
    }
  url[x] = '\0';
  }

static void getlock(int fd, char *fname, int mustchmodlck)
  {
  int sleeps;
  for(sleeps=0; lockf(fd,F_TLOCK,0); sleeps++)
    {
    if (sleeps >= 20)
      {
      fprintf(stderr,"Lock stuck on %s, GetNextID failed.\n", fname);
      if (mustchmodlck)
        {
        fchmod(fd, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
        }
      close(fd);
      exit(1);
      }
    sleep(1);
    }
  return;
  }

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "op", "Not Specified", "<Operation>, value sent to jsoc_fetch"},
  {ARG_STRING, "ds", "Not Specified", "<record_set query>"},
  {ARG_INT, "n", "0", "RecordSet Limit"},
  {ARG_DOUBLE, "lag", "0", "Processing time, secs"},
  {ARG_INT, "status", "0", "jsoc_fetch status"},
  {ARG_STRING, "QUERY_STRING", "Not Specified", "AJAX query from the web"},
  {ARG_STRING, "REMOTE_ADDR", "0.0.0.0", "Remote IP address"},
  {ARG_STRING, "host", "0.0.0.0", "Server"},
  {ARG_END}
};

char *module_name = "jsoc_stats1";

/* Module main function. */
int DoIt(void)
  {
  char *op;
  char *ds;
  char *web_query;
  char *IP;
  char *host;
  int from_web;
  int rstatus;
  int n;
  FILE *log;
  double lag;
  int lockfd;
  struct stat stbuf;
  int mustchmodlck = (stat(kLockFile, &stbuf) != 0);
  int mustchmodlog = (stat(LOGFILE, &stbuf) != 0);;

  web_query = strdup (cmdparams_get_str (&cmdparams, "QUERY_STRING", NULL));
  from_web = strcmp (web_query, "Not Specified") != 0;

  if (from_web)
    {
    char *getstring, *p;
    CGI_unescape_url(web_query);
    getstring = strdup (web_query);
    for (p=strtok(getstring,"&"); p; p=strtok(NULL, "&"))
      {
      char *key=p, *val=index(p,'=');
      if (!val)
         {
	 fprintf(stderr,"Bad QUERY_STRING %s",web_query);
         return(1);
         }
      *val++ = '\0';
      cmdparams_set(&cmdparams, key, val);
      }
    free(getstring);
    }

  op = (char *)cmdparams_get_str (&cmdparams, "op", NULL);
  ds = (char *)cmdparams_get_str (&cmdparams, "ds", NULL);
  IP = (char *)cmdparams_get_str (&cmdparams, "REMOTE_ADDR", NULL);
  host = (char *)cmdparams_get_str (&cmdparams, "host", NULL);
  n = cmdparams_get_int (&cmdparams, "n", NULL);
  rstatus = cmdparams_get_int (&cmdparams, "status", NULL);
  lag = cmdparams_get_double (&cmdparams, "lag", NULL);

  lockfd = open(kLockFile, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG);
  if (lockfd >= 0)
    {
    getlock(lockfd, kLockFile, mustchmodlck);
    log = fopen(LOGFILE,"a");
        
    if (log)
      {
      fprintf(log, "host='%s'\t",host);
      fprintf(log, "lag=%0.3f\t",lag);
      fprintf(log, "IP='%s'\t",IP);
      fprintf(log, "op='%s'\t",op);
      fprintf(log, "ds='%s'\t",ds);
      fprintf(log, "n=%d\t",n);
      fprintf(log, "status=%d\n",rstatus);
      fflush(log);
      if (mustchmodlog)
        {
        fchmod(fileno(log), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
        }
        fclose(log);
      }
    else
      {
        fprintf(stderr, "Unable to open log file for writing: %s.\n", LOGFILE);
      }
        
      lockf(lockfd,F_ULOCK,0);
      if (mustchmodlck)
        {
        fchmod(lockfd, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
        }
      close(lockfd);
    }
  else
    {
    fprintf(stderr, "Unable to open lock file for writing: %s.\n", kLockFile);
    }

  // printf("Status: 204 No Response\n\n");
  printf("Content-Type: text/plain\n\nOK\n");
  fflush(stdout);
  return(0);
  }
