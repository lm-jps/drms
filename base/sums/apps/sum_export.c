/*
 * sum_export - When a user want to get remote SUMS data via an scp
 *      it will call sum_export_svc to get the remote SUMS dir.
 *      sum_export_svc will fork off a sum_export for each request which
 *      will get the data from the given host and then send the answer back
 *      to sum_export_svc which then then send the answer
 *      to the original sum_export_svc caller.
 * Sample call:
 *	sum_export server=d00 keyfile=/tmp/keylist_10871857.log 
 */

#include <SUM.h>
#include <soi_error.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <signal.h>
#include <sum_rpc.h>
#include <printk.h>

#define LOGDIR "/usr/local/logs/SUM"

void logkey();
KEY *getsumexport(KEY *params);
struct timeval TIMEOUT = { 20, 0 };

static KEY *retlist;            /* must be static for svc dispatch rte */
char *logdir, *db, *keyfile, *server;
char datestr[32];
char logname[MAX_STR];
int pid, debugflg;
uint32_t rinfo;
FILE *logfp;
SVCXPRT *glb_transp;
CLIENT *current_client;

int open_log(char *filename)
{
  if((logfp=fopen(filename, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", filename);
    return(1);
  }
  return(0);
}

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

void sighandler(int sig)
{
  if(sig == SIGTERM) {
    printk("*** %s sum_export got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    printk("*** %s sum_export got SIGINT. Exiting.\n", datestring());
    exit(1);
  }
  printk("*** %s sum_export got an illegal signal %d, ignoring...\n",
                        datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/* Called from sum_export_svc like:
 * sum_export server=j0.Stanford.EDU keyfile=/tmp/keyname
*/
void get_cmd(int argc, char *argv[])
{
  int c;

  while((--argc > 0) && ((*++argv)[0] == '-')) {
    while((c = *++argv[0])) {
      switch(c) {
      case 'd':
        debugflg=1;     /* debug mode can also be sent in by clients */
        break;
      default:
        break;
      }
    }
  }
  server = argv[0];
  keyfile = argv[1];
}

int setup () {
  pid = getpid();
  sprintf(logname, "%s/sum_export_%d.log", LOGDIR, pid);
  if(open_log(logname)) return(1);
  printk_set(write_log, write_log);
  printk("%s\nStarting sum_export \nserver=%s\nkeyfile = %s\nlogfile = %s\n\n", datestring(), server, keyfile, logname);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
  return(0);
}


/* Module main function. */
int main(int argc, char **argv)
{
  int status = 0;
  uint32_t sumpeback;
  char cmd[128];
  char *call_err;
  KEY *list=newkeylist();
  CLIENT *clntsumpesvc;

  //sleep(60); //!!TEMP
  get_cmd(argc, argv);
  if(setup()) exit(0);
  if(file2keylist(keyfile, &list)) {	/* convert input file to keylist */
    printk("Error in file2keylist() for %s\n", keyfile);
    return(1);
  }
  sprintf(cmd, "/bin/rm -f %s", keyfile);
  system(cmd);
  keyiterate(logkey, list);            /* !!!TEMP */
  /* Create client handle used for calling the sum_export_svc */
  clntsumpesvc = clnt_create(server, SUMEXPROG, SUMEXVERS, "tcp");
  if(!clntsumpesvc) {                 /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_export_svc");
    printk("sum_export_svc not there on %s\n", server);
    return(1);
  }

  retlist = getsumexport(list);
  /* now send answer back to sum_export_svc */
    status = clnt_call(clntsumpesvc,SUMEXACK, (xdrproc_t)xdr_Rkey, 
	(char *)retlist, (xdrproc_t)xdr_uint32_t, (char *)&sumpeback, TIMEOUT);
    if(status != RPC_SUCCESS) {
      call_err = clnt_sperror(clntsumpesvc, "Err clnt_call for SUMPEACK");
      printk("%s %s\n", datestring(), call_err);
      if(status != RPC_TIMEDOUT) {
        return(status);
      }
    }
    if(sumpeback != 0) {
      printk("Error status= %d from clnt_call to SUMEXACK = %d\n", sumpeback);
      status = 1;
    }
  sprintf(cmd, "/bin/rm -f %s", logname);
  system(cmd);
  return(0);
}

/* Called to get a remote SUMS wd into a local dir via scp 
 * The keylist has "host" for the target host of the scp,
 * src_0, dest_0, src_1, dest_1, etc. for the source and destination dirs.
 * reqcnt is the key for how many src/dest pairs there are. Also uid.
 *
 * This routine will then return the 
 * answer keylist to to calling sum_export_svc which will return it to the 
 * original caller (i.e. SUM_export() in user program).
*/
KEY *getsumexport(KEY *params)
{
  uint port;
  int reqcnt;
  int status = 0;
  int xdirflg = 0;
  int first_rec, last_rec, nrecs, irec, retrieve_flg, i;
  char srcext[128], destext[128], cmd[256], errmsg[128];
  char *host, *src, *dest;

  /*printk("!!Keylist in sumpedo_1() is:\n");	/* !!!TEMP */
  /*keyiterate(logkey, params);*/
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  setkey_fileptr(&retlist, "current_client", getkey_fileptr(params, "current_client"));
  setkey_int(&retlist, "REQCODE", REMSUMRESPDO);
  reqcnt = getkey_int(params, "reqcnt");
  host = getkey_str(params, "host");
  for(i=0; i < reqcnt; i++) {		//do all scp calls in sequence
    sprintf(srcext, "src_%d", i);
    sprintf(destext, "dest_%d", i);
    src = getkey_str(params, srcext);
    dest = getkey_str(params, destext);
    port = getkey_uint(params, "port");
    if(port == 0)
      sprintf(cmd, "scp -r %s:%s/* %s", host, src, dest);
    else
      sprintf(cmd, "scp -r -P %d %s:%s/* %s", port, host, src, dest);
    printk("%s\n", cmd);
    if(system(cmd)) {
      printk("Error on: %s\n", cmd);
      status = 1;
    }
  }
  setkey_int(&retlist, "STATUS", status);
  return(retlist);  
}
