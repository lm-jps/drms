/* driveonoff.c
 *
 * Takes the given t120 drive  on or off line. 
 * Will be a noop if the tape_svc sees the drive is busy.
 *
 * Usage: driveonoff on|off|status drive#
*/

#include <SUM.h>
#include <sys/socket.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <signal.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <printk.h>

void logkey();
extern int errno;
struct timeval TIMEOUT = { 120, 0 };
uint32_t rinfo;

FILE *logfp;
CLIENT *current_client, *clnttape;
SVCXPRT *glb_transp;
char *dbname;
char *action;
char thishost[MAX_STR];
char datestr[32];
int soi_errno = NO_ERROR;
int debugflg = 0;
int sim = 0;
int tapeoffline = 0;
int statusflg = 0;
int drivenum;

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
    write_log("*** %s sum_svc got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s sum_svc got SIGINT. Exiting.\n", datestring());
    DS_DisConnectDB();
    exit(1);
  }
  write_log("*** %s sum_svc got an illegal signal %d, ignoring...\n",
			datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/*********************************************************/
void get_cmd(int argc, char *argv[])
{
  int c;
  char *username;

  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
    printf("!!NOTE: You must be user production to run driveonoff!\n");
    exit(1);
  }

  while((--argc > 0) && ((*++argv)[0] == '-')) {
    while((c = *++argv[0])) {
      switch(c) {
      case 'd':
        debugflg=1;
        break;
      default:
        break;
      }
    }
  }
  if(argc == 1) {
    action = argv[0];
    if(!strcmp(action, "status")) {
      statusflg = 1;
    }
    else {
      printf("Usage: driveonoff on|off|status drive#(0-n)\n");
      exit(1);
    }
  }
  else if(argc != 2) {
    printf("Usage: driveonoff on|off|status drive#(0-n)\n");
    exit(1);
  }
  else {
    action = argv[0];
    drivenum = atoi(argv[1]);
    if(drivenum >= MAX_DRIVES) {
      printf("Error: drivenum >= MAX_DRIVES (%d)\n", MAX_DRIVES);
      exit(1);
    }
    if(!strcmp(action, "on")) return;
    if(!strcmp(action, "off")) return;
    if(!strcmp(action, "status")) return;
    printf("Usage: driveonoff on|off|status drive#(0-n)\n");
    exit(1);
  }
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
  sprintf(logname, "/usr/local/logs/SUM/driveonoff_%d.log", pid);
  open_log(logname);
  printk_set(write_log, write_log);
  write_log("\n## %s driveonoff on %s for pid = %d ##\n", 
		datestring(), thishost, pid);
  write_log("driveonoff %s %d\n", action, drivenum);
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
  char *call_err;
  int i, maxdrive;
  uint32_t onoffback;
  enum clnt_stat status;
  KEY *list;

  get_cmd(argc, argv);
  setup();
  /* Create client handle used for calling the tape_svc */
  //if running on j1, then the tape_svc is on d02, else the localhost
  if(strcmp(thishost, SUMSVCHOST))
    clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  else
    clnttape = clnt_create(TAPEHOST, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc");
    write_log("tape_svc not there on %s\n", thishost);
    exit(1);
  }
  if(statusflg) 
    maxdrive = MAX_DRIVES;
  else
    maxdrive = 1;
  for(i=0; i < maxdrive; i++) {
    list = newkeylist();
    setkey_str(&list, "host", thishost);
    setkey_str(&list, "action", action);
    if(statusflg) drivenum = i;
    setkey_int(&list, "drivenum", drivenum);
    status = clnt_call(clnttape, DRONOFFDO, (xdrproc_t)xdr_Rkey, (char *)list,
                    (xdrproc_t)xdr_uint32_t, (char *)&onoffback, TIMEOUT);
    if(status != RPC_SUCCESS) {
        call_err = clnt_sperror(clnttape, "Err clnt_call for DRONOFF");
        write_log("%s %s\n", datestring(), call_err);
        exit(1);
    }
    switch(onoffback) {
    case 0:
      write_log("Success drive %d is online\n", drivenum);
      printf("Success drive %d is online\n", drivenum);
      break;
    case 1:
      write_log("Success drive %d is offline\n", drivenum);
      printf("Success drive %d is offline\n", drivenum);
      break;
    case 2:
      write_log("Error, no such drive# %d\n", drivenum);
      printf("Error, no such drive# %d\n", drivenum);
      break;
    case 3:
      write_log("Drive# %d is currently busy. Try again later\n", drivenum);
      printf("Drive# %d is currently busy. Try again later\n", drivenum);
      break;
    case -1:
      write_log("**Error in DRONOFF call to tape_svc\n");
      printf("**Error in DRONOFF call to tape_svc\n");
      exit(1);
      break;
    default:
      write_log("**Error in DRONOFF call to tape_svc\n");
      printf("**Error in DRONOFF call to tape_svc\n");
      exit(1);
      break;
    }
  }
}

