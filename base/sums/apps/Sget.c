/* Sget.c
*/
/*
 * Handles sum_svc type calls to get ds location. Registered at 
 * socket SUMGET,SUMGETV. This is used the the SUM_get() API.
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
#if defined(SUMS_TAPE_AVAILABLE) && SUMS_TAPE_AVAILABLE
  #include <tape.h>
#endif
#include <printk.h>
#include <unistd.h>
#include "serverdefs.h"
//!!TEMP
//#include "xsvc_run.c"

extern PART ptabx[]; 	/* defined in SUMLIB_PavailRequest.pgc */
extern SUMOPENED *sumopened_hdr;

void logkey();
extern int errno;
extern Sinfoarray sarray;
static void sumprog_1();
static void sumprog_1_array();
struct timeval TIMEOUT = { 10, 0 };
uint32_t rinfo;
uint32_t sumprog, sumvers;
int rrid = 0;
float ftmp;
char jsoc_machine[MAX_STR];
static struct timeval first[4], second[4];

FILE *logfp;
CLIENT *current_client, *clnttape, *clnttape_old;
SVCXPRT *glb_transp;
char *dbname;
char thishost[MAX_STR];
char usedhost[MAX_STR];
char hostn[MAX_STR];
char logname[MAX_STR];
char newlogname[MAX_STR];
char datestr[32];
char timetag[32];
int thispid;
int soi_errno = NO_ERROR;
int debugflg = 0;
int sim = 0;
int tapeoffline = 0;
int newlog = 0;
int logcnt = 0;

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
  if((logfp=fopen(filename, "a+")) == NULL) {
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
  printf("sig = %d\n", sig); //!!TEMP
  if(sig == SIGTERM) {
    write_log("*** %s Sget got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s Sget got SIGINT. Exiting.\n", datestring());
    DS_DisConnectDB();
    exit(1);
  }
  write_log("*** %s Sget got an illegal signal %d, ignoring...\n",
			datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/* User signal 1 is sent by a cron job to tell us to close the current
 * log file and start a new one. Usually sent just after midnight.
*/
void usr1_sig(int sig)
{
  write_log("%s usr1_sig received by Sget\n", datestring());
  //newlog = 1;                   /* tell main loop to start new log */
  logcnt++;                     /* count # used in log file name */
  write_log("%s Closing the current log file. Goodby.\n", datestring());
  fclose(logfp);
  sprintf(newlogname, "%s_%d", logname, logcnt);
  open_log(newlogname);
  write_log("\n## %s reopen log Sget on %s for pid = %d ##\n",
             datestring(), thishost, thispid);
  signal(SIGUSR1, &usr1_sig);   /* handle a signal 16 sent by cron job */
}

/*********************************************************/
void get_cmd(int argc, char *argv[])
{
  int c;
  char *username, *cptr, *cptr2;

  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, SUMS_MANAGER)) {
    printf("!!NOTE: You must be user %s to run sum_svc!\n", SUMS_MANAGER);
    exit(1);
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
  if(argc != 2) {
    printf("!!ERROR: program needs 2 args: dbname and logfilename. e.g:\n");
    printf("Sget jsoc_sums sum_svc_2011.06.06.140140.log\n");
    exit(1);
  }
  dbname = argv[0];
  //the input log name looks like: sum_svc_2011.08.09.162411.log
  //If this is a restart it looks like: sum_svc_2011.08.09.162411.log_R
  //or sum_svc_2011.08.09.162411.log_6_R if 6 days of log files exist
  sprintf(logname, "%s/%s", SUMLOG_BASEDIR, argv[1]);
  if(cptr = strstr(logname, "_R")) {
    if(cptr2 = strstr(logname, "log_R")) {
      *(cptr2+3) = (char)NULL;
      logcnt = 0;
      open_log(logname);
    }
    else if(cptr2 = strstr(logname, "log_")) {
      *cptr= (char)NULL;        //elim trailing _R
      open_log(logname);
      *(cptr2+3) = (char)NULL; 
      cptr2 = cptr2 + 4;        //point to number
      sscanf(cptr2, "%d", &logcnt);
    }
    else {
      printf("!!ERROR: called with NG log file name: %s\n", logname);
      exit(1);
    }
  }
  else { open_log(logname); }
}

/*********************************************************/
void setup()
{
  FILE *fplog;
  int tpid, i;
  char *cptr, *machine;
  char lfile[MAX_STR], acmd[MAX_STR], line[MAX_STR];

  //when change name of dcs2 to dcs1 we found out you have to use localhost
  //gethostname(thishost, MAX_STR);
  //cptr = index(thishost, '.');       /* must be short form */
  //if(cptr) *cptr = (char)NULL;
  sprintf(thishost, "localhost");
  gethostname(hostn, 80);
  cptr = index(hostn, '.');     // must be short form
  if(cptr) *cptr = (char)NULL;
  //how to call this sum process back
  sumprog = SUMGET;
  sumvers = SUMGETV;

  thispid = getpid();
  if(!(machine = (char *)getenv("JSOC_MACHINE"))) {
    sprintf(jsoc_machine, "NOTGIVEN");
    write_log("!!WARNING: No JSOC_MACHINE in env\n");
    write_log("SUMLIB_InfoGetArray() calls will fail\n");
  }
  else
    sprintf(jsoc_machine, "%s", machine);
  //sprintf(logname, "%s/sum_svc_%s.log", SUMLOG_BASEDIR, gettimetag());
  //open_log(logname);	//moved to get_cmd()
  printk_set(write_log, write_log);
  write_log("\n## %s Sget on %s (%s) for pid = %d ##\n", 
		datestring(), thishost, hostn, thispid);
  //write_log("Database to connect to is %s\n", dbname);
  //if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
  signal(SIGUSR1, &usr1_sig);   /* handle a signal 16 sent by cron job */

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
    sprintf(lfile, "%s/sum_restart_%d.touch", SUMLOG_BASEDIR, tpid);
    sprintf(acmd, "/bin/touch %s", lfile);
    write_log("%s\n", acmd);
    system(acmd);
  }
  fclose(fplog);
}

//This is called at the sum_svc exit via exit() or by return from main()
void sumbye(void) {
  printf("sumbye() called by atexit() at %s\n", datestring());
  write_log("sumbye() called by atexit() at %s\n", datestring());
}

/*********************************************************/
int main(int argc, char *argv[])
{
  register SVCXPRT *transp;
  int i;
  pid_t pid;
  char dsvcname[80], cmd[128];
  char *args[5], pgport[32];
  char *simmode;

  get_cmd(argc, argv);
  printf("\nPlease wait for sum_svc and tape inventory to initialize...\n");
  setup();
  if(atexit(sumbye)) {
    printf("Can't register sumbye() function in atexit()\n");
  }

        /* register for client API routines to talk to us */
	(void) pmap_unset(SUMGET, SUMGETV);
	transp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
	if (transp == NULL) {
		write_log("***cannot create tcp service\n");
		exit(1);
	}
        write_log("svctcp_create() port# = %u\n", transp->xp_port);
	if (!svc_register(transp, SUMGET, SUMGETV, sumprog_1, IPPROTO_TCP)) {
		write_log("***unable to register (SUMGET, SUMGETV, tcp)\n");
		exit(1);
	}

  //if(strcmp(hostn, "n02") && strcmp(hostn, "xim")) {  //!!TEMP for not n02, xim
  if(!(simmode = (char *)getenv("SUMSIMMODE"))) {
      sprintf(pgport, SUMPGPORT);
      setenv("SUMPGPORT", pgport, 1); //connect to 5430,5431, or 5434
      write_log("Sget sets SUMPGPORT env to %s\n", pgport);
  }
  else {
      write_log("Sget sim mode SUMPGPORT %s\n", (char *)getenv("SUMPGPORT"));
  }

#ifndef __LOCALIZED_DEFS__
//Only fork the tape stuff on the datacapture machines. The tape stuff for
//the main SUMS is started by a call to ssh d02.stanford.edu sum_forker 
//in sum_start_j1
if(!strcmp(hostn, "dcs0") || !strcmp(hostn, "dcs1") || !strcmp(hostn, "dcs2") || !strcmp(hostn, "dcs3")) {
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
#ifdef __LOCALIZED_DEFS__
/*************** only start sum_rm from sum_svc  *************************
  if((pid = fork()) < 0) {
    write_log("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   // this is the beloved child 
    write_log("execvp of sum_rm\n");
    args[0] = "sum_rm";			// note: no -s to sum_rm 
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() sum_rm. errno=%d\n", errno);
      exit(1);
    }
  }
************************************************************************/
#endif

#ifndef __LOCALIZED_DEFS__
/*****************dont fork here. sum_rm now runs on d02*******************
  if((pid = fork()) < 0) {
    write_log("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   // this is the beloved child 
    write_log("execvp of sum_rm\n");
    args[0] = "sum_rm";			// note: no -s to sum_rm 
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() sum_rm. errno=%d\n", errno);
      exit(1);
    }
  }
***********************************************************************/
if(strcmp(hostn, "lws") && strcmp(hostn, "n00") && strcmp(hostn, "d00") && strcmp(hostn, "n02") && strcmp(hostn, "xim")) { 
  /* Create client handle used for calling the tape_svc */
  printf("\nsum_svc waiting for tape servers to start (approx 10sec)...\n");
  sleep(13);			/* give time to start */
  //if running on j1, then the tape_svc is on TAPEHOST, else the localhost
  if(strcmp(hostn, SUMSVCHOST)) { 
    clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
    strcpy(usedhost, thishost);
  }
  else {
    clnttape = clnt_create(TAPEHOST, TAPEPROG, TAPEVERS, "tcp");
    strcpy(usedhost, TAPEHOST);
  }
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc (xsum_svc)");
    write_log("tape_svc not there on %s\n", usedhost);
    fprintf(stderr, "tape_svc not there on %s\n", usedhost);
    exit(1);
  }
  clnttape_old = clnttape;	//used by tapereconnectdo_1()
}

//!!NOTE: Add 9Sep2011 special case for xim to connect to xtape_svc
if(!strcmp(hostn, "xim")) { 
  /* Create client handle used for calling the tape_svc */
  printf("\nsum_svc waiting for tape servers to start (approx 10sec)...\n");
  sleep(10);			/* give time to start */
  //if running on j1, then the tape_svc is on TAPEHOST, else the localhost
  if(strcmp(hostn, SUMSVCHOST)) { 
    clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
    strcpy(usedhost, thishost);
  }
  else {
    clnttape = clnt_create(TAPEHOST, TAPEPROG, TAPEVERS, "tcp");
    strcpy(usedhost, TAPEHOST);
  }
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc (xsum_svc)");
    write_log("tape_svc not there on %s\n", usedhost);
//    exit(1);
  }
  clnttape_old = clnttape;	//used by tapereconnectdo_1()
}
#endif

//  if(SUM_Init(dbname)) {		/* init and connect to db */
//    write_log("***Can't SUM_Init()\n");
//    exit(1);
//  }
//Sget.c does below instead of SUM_Init()
  DS_ConnectDB(dbname);         /* connect to DB for init */
  if(DS_PavailRequest2()) {
    write_log("***Can't SUM_Init()\n");
    exit(1);
  }

  sleep(5);		/* give tape inventory time to complete */
  printf("Sget now available\n");

  /* Enter svc_run() which calls svc_getreqset when msg comes in. 
   * svc_getreqset calls sumprog_1() to process the msg. 
   * NOTE: svc_run() never returns. 
  */
  svc_run();
  //xsvc_run();
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
  uint64_t ck_client;     //used to ck high bits of current_client
  uint64_t uid;

	//StartTimer(1);
	union __svcargun {
		Rkey sumdo_1_arg;
	} argument;
	char *result, *call_err;
        enum clnt_stat clnt_stat;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();

        if(rqstp->rq_proc == INFODOARRAY) {
          sumprog_1_array(rqstp, transp);
          return;
        }

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
	case SHUTDO:
		sprintf(procname, "SHUTDO");	//!!TEMP name tags
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) shutdo_1;
		break;
	case ALLOCDO:
		sprintf(procname, "ALLOCDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) allocdo_1;
		break;
	case INFODO:
		sprintf(procname, "INFODO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) infodo_1;
		break;
        case INFODOX:
                sprintf(procname, "INFODOX");
                xdr_argument = xdr_Rkey;
                xdr_result = xdr_Rkey;
                local = (char *(*)()) infodoX_1;
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
	case NOPDO:
		sprintf(procname, "NOPDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) nopdo_1;
		break;
	case TAPERECONNECTDO:
		sprintf(procname, "TAPERESTARTDO");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) tapereconnectdo_1;
		break;
	case SUMREPARTN:
		sprintf(procname, "SUMREPARTN");
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_uint32_t;
		local = (char *(*)()) repartndo_1;
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
          if(current_client == 0) {
            write_log("***Error on clnt_call() back to orig sum_svc caller\n");
            write_log("   current_client was NULL\n");
          }
          else {
            ck_client = ((uint64_t)current_client & 0xfc00000000000000) >> 58;
            if(!((ck_client == 0) || (ck_client == 0x3f))) {
              write_log("***Error invalid current_client\n");
              //May need more info to discover the caller.
              //See email from Keh-Cheng 25Feb2011 13:44 Re:Cannot access..
            }
            else {
              clnt_stat=clnt_call(current_client, RESPDO, (xdrproc_t)xdr_result, 
  		result, (xdrproc_t)xdr_void, 0, TIMEOUT);
              if(clnt_stat != 0) {
                clnt_perrno(clnt_stat);		/* outputs to stderr */
                write_log("***Error on clnt_call() back to RESPDO procedure\n");
                if(findkey(result, "uid")) {
                  uid = getkey_uint64(result, "uid");
                  write_log("***The original client caller has probably exited. Its uid=%lu\n", uid);
                  write_log("***Removing from Sget active list of clients\n");
                  remsumopened(&sumopened_hdr, (uint32_t)uid); 
                }
                else {
                  write_log("***The original client caller has probably exited\n");
                }
                call_err = clnt_sperror(current_client, "Err");
                write_log("%s\n", call_err);
              }
              clnt_destroy(current_client); 
            }
          }
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

/**************************************************************************
      if(newlog) {      //got signal to make a new log file
        newlog = 0;
        write_log("%s Closing the current log file. Goodby.\n", datestring());
        fclose(logfp);
        sprintf(newlogname, "%s_%d", logname, logcnt);
        open_log(newlogname);
        write_log("\n## %s reopen log sum_svc on %s for pid = %d ##\n",
                datestring(), thishost, thispid);
      }
**************************************************************************/

      return;
}

/* Like sumprog_1() but for an Sunumarray decode instead of a keylist.
*/
static void
sumprog_1_array(rqstp, transp)
	struct svc_req *rqstp;
	SVCXPRT *transp;
{
  char procname[128];
  uint64_t ck_client;     //used to ck high bits of current_client
  uint64_t uid;

	//StartTimer(1);
	union __svcargun {
		Sunumarray sumdo_1_arg;
	} argument;
	char *result, *call_err;
        enum clnt_stat clnt_stat;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();

	switch (rqstp->rq_proc) {
	case INFODOARRAY:
                sprintf(procname, "INFODOARRAY");
                xdr_argument = xdr_Sunumarray;
                xdr_result = xdr_Rkey;
                local = (char *(*)()) infodoArray_1;
                break;
	default:
                write_log("**sumprog_1_array() dispatch default procedure %d,ignore\n", rqstp->rq_proc);
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
          }
          if(current_client == 0) {
            write_log("***Error on clnt_call() back to orig sum_svc caller\n");
            write_log("   current_client was NULL\n");
          }
          else {
            ck_client = ((uint64_t)current_client & 0xfc00000000000000) >> 58;
            if(!((ck_client == 0) || (ck_client == 0x3f))) {
              write_log("***Error invalid current_client\n");
              //May need more info to discover the caller.
              //See email from Keh-Cheng 25Feb2011 13:44 Re:Cannot access..
            }
            else {
              clnt_stat=clnt_call(current_client, RESPDOARRAY,(xdrproc_t)xdr_result, result, (xdrproc_t)xdr_void, 0, TIMEOUT);
              if(clnt_stat != RPC_SUCCESS) {
                if(clnt_stat != RPC_TIMEDOUT) {
                  clnt_perrno(clnt_stat);         // outputs to stderr 
                  write_log("***Error on clnt_call() back to RESPDO procedure\n");
                  if(findkey(result, "uid")) {
                    uid = getkey_uint64(result, "uid");
                    write_log("***The original client caller has probably exited. Its uid=%lu\n", uid);
                  }
                  else {
                    write_log("***The original client caller has probably exited\n");
                  }
                  call_err = clnt_sperror(current_client, "Err");
                  write_log("%s\n", call_err);

                }
                else {
                  write_log("Timeout ignored on RESPDO back to current_client\n");
                }
              }
              clnt_destroy(current_client); 
            }
          }
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

