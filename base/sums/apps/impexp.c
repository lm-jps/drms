/* impexp.c
 * This program is started by tui when the interactive user hits the
 * "Start LRU Tape Unload" button. It's args are the tape ids to move from
 * their current slots to the Exit/Entry Door slots.
 * This program send these tape ids to tape_svc, gets an ack and then
 * exits. The tui will detect the tapes being unloaded via the 
 * tape_svc log file entries. 
 * Sample call:
 * impexp 013389S1 013401S1 000014S1
*/
#include <SUM.h>
#include <sys/socket.h>
#include <sys/errno.h>
#include <signal.h>
#include <rpc/rpc.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <printk.h>

void logkey();
extern int errno;
static struct timeval TIMEOUT = { 120, 0 };

FILE *logfp;
CLIENT *current_client, *clnttape;
char *dbname;
char thishost[MAX_STR];
char datestr[32];
int soi_errno = NO_ERROR;

/*********************************************************/
void open_log(char *filename)
{
  if((logfp=fopen(filename, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", filename);
  }
}

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[]. */
char *datestring()
{
  struct timeval tvalr;
  struct tm *t_ptr;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(datestr, "%s", asctime(t_ptr));
  datestr[19] = (char)NULL;
  return(&datestr[4]);          /* isolate the mmm dd hh:mm:ss */
}

/*********************************************************/
/* Outputs the variable format message (re: printf) to the log file.
*/
int write_log(const char *fmt, ...)
{
  va_list args;
  char string[4096];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  if(logfp) {
    fprintf(logfp, string);
    fflush(logfp);
  }
  else
    fprintf(stderr, string);
  va_end(args);
  return(0);
}

/*********************************************************/
void sighandler(sig)
  int sig;
{
  if(sig == SIGTERM) {
    write_log("*** %s impexp got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s impexp got SIGINT. Exiting.\n", datestring());
    exit(1);
  }
  write_log("*** %s impexp got an illegal signal %d, ignoring...\n",
			datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/*********************************************************/
void setup()
{
  int pid;
  char *cptr;
  char logname[MAX_STR];

  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  *cptr = (char)NULL;
  pid = getpid();
  sprintf(logname, "/usr/local/logs/SUM/impexp_%d.log", pid);
  open_log(logname);
  printk_set(write_log, write_log);
  write_log("\n## %s impexp on %s for pid = %d ##\n", 
		datestring(), thishost, pid);
  /*dbname = "hmidb";*/
  /*write_log("Database to connect to is %s\n", dbname);*/
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/*********************************************************/
int main(int argc, char *argv[])
{
  KEY *retlist;
  uint32_t tapeback;
  enum clnt_stat status;
  int i;
  char *call_err;
  char ext[64];

  setup();
  /* Create client handle used for calling the tape_svc */
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc");
    write_log("tape_svc not there on %s\n", thishost);
    exit(1);
  }
  retlist = newkeylist();
  if(!strcmp(argv[1], "stop")) {
    setkey_str(&retlist, "OP", "stop");
  }
  else {
    argc = argc - 2;		/* skip prog name and start/stop field */
    setkey_int(&retlist, "reqcnt", argc);	/* # of tapeids */
    setkey_str(&retlist, "OP", "start");
    for(i=0; i < argc; i++) {
      sprintf(ext, "tapeid_%d", i);
      setkey_str(&retlist, ext, argv[i+2]);
      write_log("In impexp: %s = %s\n", ext, argv[i+2]);
    }
  }
  status = clnt_call(clnttape, IMPEXPDO, (xdrproc_t)xdr_Rkey, (char *)retlist,
                    (xdrproc_t)xdr_uint32_t, (char *)&tapeback, TIMEOUT);
  write_log("impexp: tapeback=%ld, status=%d\n", tapeback, status);
  if(status != RPC_SUCCESS) {
      call_err = clnt_sperror(clnttape, "Err clnt_call for IMPEXPDO");
      write_log("%s %s\n", datestring(), call_err);
  }
  if(tapeback == 1) {
    write_log("**Error in IMPEXPDO call to tape_svc in impexp\n");
  }
  clnt_destroy(clnttape);
  fclose(logfp);
  exit(0);		/* that's it. we're done */
}
