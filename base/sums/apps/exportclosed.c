/* exportclosed.c
 * This program is started by tui when the interactive user hits an
 * export button on the "export closed tapes" page. 
 * It's args are the tape ids to move from
 * their current slots to the Export slots (currently 2201-2240).
 * This program send these tape ids to tape_svc, gets an ack and then
 * exits. The tui will detect the tapes being unloaded via the 
 * tape_svc log file entries. 
 * Sample call:
 * exportclosed start 013389S1 013401S1 000014S1
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
static struct timeval TIMEOUT = { 600, 0 }; //allow for mtx transfer cmd

#define FIRST_EXP_SLOT 2201

FILE *logfp;
CLIENT *current_client, *clnttape;
uint32_t tapeback;
enum clnt_stat status;
char *call_err;
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
static char *datestring()
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
    write_log("*** %s exportclosed got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s exportclosed got SIGINT. Exiting.\n", datestring());
    exit(1);
  }
  write_log("*** %s exportclosed got an illegal signal %d, ignoring...\n",
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
  if(cptr) *cptr = (char)NULL;
  pid = getpid();
  sprintf(logname, "/usr/local/logs/SUM/exportclosed_%d.log", pid);
  open_log(logname);
  printk_set(write_log, write_log);
  write_log("\n## %s exportclosed on %s for pid = %d ##\n", 
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

/* Call the tape_svc proc EXPCLOSEDO with the given string as an
 * "OP" keyword. Return 0 on success, else 1.
*/
int send2tapesvc(char *cmd)
{
  static KEY *retlist;
  int statback = 0;

      retlist = newkeylist();
      setkey_str(&retlist, "OP", cmd);
      status = clnt_call(clnttape, EXPCLOSEDO, (xdrproc_t)xdr_Rkey,
                (char *)retlist, (xdrproc_t)xdr_uint32_t,
                (char *)&tapeback, TIMEOUT);
      write_log("exportclosed: tapeback=%ld, status=%d\n", tapeback, status);
      if(status != RPC_SUCCESS) {
          call_err = clnt_sperror(clnttape, "Err clnt_call for EXPCLOSEDO");
          write_log("%s %s\n", datestring(), call_err);
          statback = 1;
      }
      if(tapeback == 1) {
        write_log("**Error in EXPCLOSEDO call to tape_svc in exportclosed\n");
        statback = 1;
      }
      freekeylist(&retlist);
      return(statback);
}

/*********************************************************/
int main(int argc, char *argv[])
{
  int i, slotnum;
  char cmd[256];

  setup();
  //sleep(60);	//!!TEMP

  slotnum = FIRST_EXP_SLOT;	//NOTE: external slot #
  /* Create client handle used for calling the tape_svc */
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc");
    write_log("tape_svc not there on %s\n", thishost); 
    exit(1);
  } 
  write_log("exportclosed called with op: %s\n", argv[1]);
  if(!strcmp(argv[1], "unload")) {  //unload call
    if(send2tapesvc("unload")) {
      write_log("Error sending unload: exportclosed is aborting\n");
      exit(1);
    }
    argc = argc - 2;		/* skip prog name and cmd field */
    for(i=0; i < argc; i++) {
      sprintf(cmd, "jmtx -f /dev/t950 transtape %s %d", argv[i+2], slotnum++);
      write_log("%s\n", cmd);
      if(send2tapesvc(cmd)) {
        write_log("Error sending %s to tape_svc\n", cmd);
        continue;
      }
      if(system(cmd)) {
        write_log("Error: %s\n", cmd);
      }
    }
    if(send2tapesvc("stop unload")) {
      write_log("Error sending stop unload\n");
    }
  }
  else if(!strcmp(argv[1], "load")) {  //load command
    if(send2tapesvc("load")) {
      write_log("Error sending load: exportclosed is aborting\n");
      exit(1);
    }
    argc = argc - 2;		/* skip prog name and cmd field */
    for(i=0; i < argc; i++) {
      sprintf(cmd, "jmtx -f /dev/t950 transfer %d %s", slotnum++, argv[i+2]);
      write_log("%s\n", cmd);
      if(send2tapesvc(cmd)) {
        write_log("Error sending %s to tape_svc\n", cmd);
        continue;
      }
      if(system(cmd)) {
        write_log("Error: %s\n", cmd);
      }
    }
    if(send2tapesvc("stop load")) {
      write_log("Error sending stop load\n");
    }
    if(send2tapesvc("reinventory")) {
      write_log("Error sending reinventory: exportclosed is aborting\n");
      exit(1);
    }
  }
  else {
    write_log("Bad call. arg = %s\n", argv[1]);
    exit(1);
  }

  clnt_destroy(clnttape);
  write_log("exportclosed done\n");
  fclose(logfp);
  // that's it. we're done
}
