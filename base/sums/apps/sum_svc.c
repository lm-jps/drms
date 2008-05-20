/* sum_svc.c (originally $STAGING/src/pipe/rpc/pe_rpc_svc.c)
*/
/*
 * This file was originally generated (long ago and far away) using rpcgen 
 * and then edited.
 * This program is started from the command line as:
 *    sum_svc [-d] [-s] dbname &
 * where dbname = Oracle db to connect to (default = hmidbX).
 *       -d = debug mode
 *       -s = simulation mode (robot and tape cmds are simulated)
 *
 * It is normally run as user production on the host that has the SUM storage
 * local to it (and probably the tape archive unit). It handles requests for
 * all users (normally just DRMS) calling the SUM API:
 * http://sun.stanford.edu/web.hmi/development/SU_Development_Plan/SUM_API.html
 * Keeps a log file in: /usr/local/logs/SUM/sum_svc_[pid].log
 *
 * !!NOTE: It was discovered in the mdi impelmentation that clnt_call() does 
 * not return an error code when the requested client is not there, 
 * but rather causes the program to exit.
 * See if this still happens in the new hmi environment. -- No
*/

#include <SUM.h>
#include <sys/socket.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <signal.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <printk.h>

extern PART ptabx[]; 	/* defined in SUMLIB_PavailRequest.pgc */

void logkey();
extern int errno;
static void sumprog_1();
struct timeval TIMEOUT = { 60, 0 };
uint32_t rinfo;
float ftmp;
static struct timeval first[4], second[4];

FILE *logfp;
CLIENT *current_client, *clnttape;
SVCXPRT *glb_transp;
char *dbname;
char thishost[MAX_STR];
char datestr[32];
char timetag[32];
int soi_errno = NO_ERROR;
int debugflg = 0;
int sim = 0;
int tapeoffline = 0;

/*********************************************************/
void StartTimer(int n)
{
  gettimeofday (&first[n], NULL);
}

float StopTimer(int n)
{
  gettimeofday (&second[n], NULL);
  if (first[n].tv_usec > second[n].tv_usec) {
    second[n].tv_usec += 1000000;
    second[n].tv_sec--;
  }
  return (float) (second[n].tv_sec-first[n].tv_sec) +
    (float) (second[n].tv_usec-first[n].tv_usec)/1000000.0;
}

/*********************************************************/
void open_log(char *filename)
{
  if((logfp=fopen(filename, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", filename);
  }
}

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[]. 
*/
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

/* Returns and sets the global vrbl timetag[] yyyy.mm.dd.hhmmss */
char *gettimetag()
{
  struct timeval tvalr;
  struct tm *t_ptr;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(timetag, "%04d.%02d.%02d.%02d%02d%02d", 
	(t_ptr->tm_year+1900), (t_ptr->tm_mon+1), t_ptr->tm_mday, t_ptr->tm_hour, t_ptr->tm_min, t_ptr->tm_sec);
  return(timetag);
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
/*    printf("!!NOTE: You must be user production to run sum_svc!\n");
/*    exit(1);
*/
  }

  while((--argc > 0) && ((*++argv)[0] == '-')) {
    while((c = *++argv[0])) {
      switch(c) {
      case 'd':
        debugflg=1;	/* debug mode can also be sent in by clients */
        break;
      case 's':
        sim=1;		/* simulation mode */
        break;
      case 'o':
        tapeoffline=1;	/* offline mode */
        break;
      default:
        break;
      }
    }
  }
  if(argc != 1)
    dbname = "hmidbX";
  else
    dbname = argv[0];
}

/*********************************************************/
void setup()
{
  FILE *fplog;
  int pid, tpid, i;
  char *cptr;
  char logname[MAX_STR], lfile[MAX_STR], acmd[MAX_STR], line[MAX_STR];

  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;
  pid = getpid();
  sprintf(logname, "/usr/local/logs/SUM/sum_svc_%s.log", gettimetag());
  open_log(logname);
  printk_set(write_log, write_log);
  write_log("\n## %s sum_svc on %s for pid = %d ##\n", 
		datestring(), thishost, pid);
  write_log("Database to connect to is %s\n", dbname);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
  /* now touch a file for each t50/t120view that is running so it will know
   * that sum_svc has restarted */
  sprintf(lfile, "/tmp/find_tview.log");
  sprintf(acmd, "ps -ef | grep %s  1> %s 2>&1", TAPEVIEWERNAME, lfile);
  if(system(acmd)) {
    write_log("**Can't execute %s.\n", acmd);
    return;
  }
  if((fplog=fopen(lfile, "r")) == NULL) {
    write_log("**Can't open %s to find any %s\n", lfile, TAPEVIEWERNAME);
    return;
  }
  while(fgets(line, 128, fplog)) {       /* get ps lines */
    if(!(strstr(line, "perl"))) continue;
    sscanf(line, "%s %d", acmd, &tpid); /* get user name & process id */
    sprintf(logname, "/usr/local/logs/SUM/sum_restart_%d.touch", tpid);
    sprintf(acmd, "/bin/touch %s", logname);
    write_log("%s\n", acmd);
    system(acmd);
  }
  fclose(fplog);
}

/*********************************************************/
int main(int argc, char *argv[])
{
  register SVCXPRT *transp;
  int i;
  pid_t pid;
  char dsvcname[80];
  char *args[5];

  get_cmd(argc, argv);
  printf("\nPlease wait for sum_svc and tape inventory to initialize...\n");
  setup();

        /* register for client API routines to talk to us */
	(void) pmap_unset(SUMPROG, SUMVERS);
	transp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
	if (transp == NULL) {
		write_log("***cannot create tcp service\n");
		exit(1);
	}
	if (!svc_register(transp, SUMPROG, SUMVERS, sumprog_1, IPPROTO_TCP)) {
		write_log("***unable to register (SUMPROG, SUMVERS, tcp)\n");
		exit(1);
	}

#ifndef SUMNOAO
if(strcmp(thishost, "lws") && strcmp(thishost, "flap")) { /* !!TEMP don't fork on lws or flap */
  if((pid = fork()) < 0) {
    write_log("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    write_log("execvp of tape_svc\n");
    args[0] = "tape_svc";
    if(tapeoffline) { 		/* overrides any sim flg */
      args[1] = "-o";
      args[2] = dbname;
      args[3] = timetag;
      args[4] = NULL;
    }
    else if(sim) { 
      args[1] = "-s";
      args[2] = dbname;
      args[3] = timetag;
      args[4] = NULL;
    }
    else {
      args[1] = dbname;
      args[2] = timetag;
      args[3] = NULL;
    }
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() tape_svc. errno=%d\n", errno);
      exit(1);
    }
  }
  sleep(1);				/* let tape_svc start */
  for(i=0; i < MAX_DRIVES; i++) { 	/* start all the driven_svc */
    if((pid = fork()) < 0) {
      write_log("***Can't fork(). errno=%d\n", errno);
      exit(1);
    }
    else if(pid == 0) {                   /* this is the beloved child */
      sprintf(dsvcname, "drive%d_svc", i);
      write_log("execvp of %s\n", dsvcname);
      args[0] = dsvcname;
      if(tapeoffline) {                 /* overrides any sim flg */
	 args[1] = "-o";
	 args[2] = dbname;
	 args[3] = timetag;
	 args[4] = NULL;
      }
      else if(sim) {
        args[1] = "-s";
        args[2] = dbname;
        args[3] = timetag;
        args[4] = NULL;
      }
      else {
        args[1] = dbname;
        args[2] = timetag;
        args[3] = NULL;
      }
      if(execvp(args[0], args) < 0) {
        write_log("***Can't execvp() %s. errno=%d\n", dsvcname, errno);
        exit(1);
      }
    }
  }
  if((pid = fork()) < 0) {
    write_log("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    write_log("execvp of robot0_svc\n");
    args[0] = "robot0_svc";
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() robot0_svc. errno=%d\n", errno);
      exit(1);
    }
  }
}				/* !!end of TMP for lws only */
#endif

  if((pid = fork()) < 0) {
    write_log("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    write_log("execvp of sum_rm\n");
    args[0] = "sum_rm";			/* note: no -s to sum_rm */
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() sum_rm. errno=%d\n", errno);
      exit(1);
    }
  }

#ifndef SUMNOAO
if(strcmp(thishost, "lws") && strcmp(thishost, "flap")) { /* !!TEMP don't fork on lws or flap */
  /* Create client handle used for calling the tape_svc */
  sleep(3);			/* give time to start */
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc");
    write_log("tape_svc not there on %s\n", thishost);
    exit(1);
  }
}
#endif

  if(SUM_Init(dbname)) {		/* init and connect to db */
    write_log("***Can't SUM_Init()\n");
    exit(1);
  }
  sleep(5);		/* give tape inventory time to complete */
  printf("sum_svc now available\n");

  /* Enter svc_run() which calls svc_getreqset when msg comes in. 
   * svc_getreqset calls sumprog_1() to process the msg. 
   * NOTE: svc_run() never returns. 
  */
  svc_run();
  write_log("!!Fatal Error: svc_run() returned in sum_svc\n");
  exit(1);
}

/* This is the dispatch routine that's called when the client does a
 * clnt_call() to SUMPROG, SUMVERS with these procedure numbers.
 * Called by svc_getreqset() in svc_run().
*/
static void
sumprog_1(rqstp, transp)
	struct svc_req *rqstp;
	SVCXPRT *transp;
{
  char procname[128];

	//StartTimer(1);
	union __svcargun {
		Rkey sumdo_1_arg;
	} argument;
	char *result, *call_err;
        enum clnt_stat clnt_stat;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();

	switch (rqstp->rq_proc) {
	case NULLPROC:
		(void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
		return;
		break;
/*	case SUMDO:
/*		xdr_argument = (xdrproc_t)xdr_Rkey;
/*		xdr_result = (xdrproc_t)xdr_Rkey;;
/*		local = (char *(*)()) sumdo_1;
/*		break;
*/
	case OPENDO:
		sprintf(procname, "OPENDO");	//!!TEMP name tags
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) opendo_1;
		break;
	case ALLOCDO:
		sprintf(procname, "ALLOCDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) allocdo_1;
		break;
	case GETDO:
		sprintf(procname, "GETDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) getdo_1;
		break;
	case PUTDO:
		sprintf(procname, "PUTDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) putdo_1;
		break;
	case CLOSEDO:
		sprintf(procname, "CLOSEDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) closedo_1;
		break;
	case SUMRESPDO:
		sprintf(procname, "SUMRESPDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) sumrespdo_1;
		break;
	case DELSERIESDO:
		sprintf(procname, "DELSERIESDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) delseriesdo_1;
		break;
	default:
                write_log("**sumprog_1() dispatch default procedure %d,ignore\n", rqstp->rq_proc);
		svcerr_noproc(transp);
		return;
	}
	bzero((char *)&argument, sizeof(argument));
	if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
                write_log("***Error on svc_getargs()\n");
		svcerr_decode(transp);
                /*return;*/
                /* NEW: 23May2002 don't return. Can result in caller getting: */
                /* Dsds_svc returned error code 5600 */
                /* NEW: 10Jun2002 try this: */
                svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
                return;

	}
        glb_transp = transp;		     /* needed by function */
        result = (*local)(&argument, rqstp); /* call the function */
					     /* sets current_client & rinfo*/
					     /* ack sent back in the function*/

      if(result) {			/* send the result now */
        if(result == (char *)1) {
          /* no client handle. do nothing, just return */
        }
        else {
          if(debugflg) {
            write_log("\n###KEYLIST at return in sum_svc\n");
            keyiterate(logkey, (KEY *)result);
          }
          clnt_stat=clnt_call(current_client, RESPDO, (xdrproc_t)xdr_result, 
		result, (xdrproc_t)xdr_void, 0, TIMEOUT);
          if(clnt_stat != 0) {
            clnt_perrno(clnt_stat);		/* outputs to stderr */
            write_log("***Error on clnt_call() back to RESPDO procedure\n");
            write_log("***The original client caller has probably exited\n");
            call_err = clnt_sperror(current_client, "Err");
            write_log("%s\n", call_err);
          }
          clnt_destroy(current_client);
          freekeylist((KEY **)&result);
        }
      }
      else {
      }
      if (!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
	write_log("**unable to free arguments\n");
	/*exit(1);*/
      }
      //ftmp = StopTimer(1);
      //write_log("#END: %s %fsec\n", procname, ftmp);	//!!TEMP for test
      return;
}
