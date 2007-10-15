/* robotn_svc.c
 * Called by tape_svc when it has a robot command to execute.
*/

#include <SUM.h>
#include <sys/errno.h>
#include <sys/wait.h>
#include <rpc/pmap_clnt.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <printk.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stropts.h>
#include <sys/mtio.h>


#define MAX_WAIT 20  /* max times to wait for rdy drive in drive_ready() */

void write_time();
void logkey();
int drive_ready();
extern int errno;
#ifdef ROBOT_0
static void robot0prog_1();
#endif
#ifdef ROBOT_1
static void robot1prog_1();
#endif
static struct timeval TIMEOUT = { 120, 0 };
uint32_t rinfo;		/* info returned by XXXdo_1() calls */
uint32_t procnum;	/* remote procedure # to call for current_client call*/

FILE *logfp;
CLIENT *current_client, *clnttape;
SVCXPRT *glb_transp;
int debugflg = 0;
char *dbname;
char *timetag;
char thishost[MAX_STR];
char datestr[32];
char logfile[MAX_STR];
char robotname[MAX_STR];

int soi_errno = NO_ERROR;

/*********************************************************/
void open_log(char *filename)
{
  if((logfp=fopen(filename, "a")) == NULL) {
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
  if(sig == SIGTERM) {	/* most likely a kill from the pvmd */
    write_log("*** %s %s got SIGTERM. Exiting.\n", datestring(), robotname);
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s %s got SIGINT. Exiting.\n", datestring(), robotname);
    /*DS_DisConnectDB();*/
    exit(1);
  }
  write_log("*** %s %s got an illegal signal %d, ignoring...\n",
			datestring(), robotname, sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/*********************************************************/
void get_cmd(int argc, char *argv[])
{
  int c;

  while(--argc > 0 && (*++argv)[0] == '-') {
    while(c = *++argv[0])
      switch(c) {
      case 'd':
        debugflg=1;	/* can also be sent in by client calls */
        break;
      default:
        break;
      }
  }
  if(argc != 2) {
    printf("\nERROR: robotn_svc must be call with dbname and timestamp\n");
    exit(1);
  }
  else {
    dbname = argv[0];
    timetag = argv[1];
  }
}

/*********************************************************/
void setup()
{
  int pid;
  char *cptr;

  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  *cptr = (char)NULL;
  #ifdef ROBOT_0
  sprintf(robotname, "robot0_svc");
  #endif
  #ifdef ROBOT_1
  sprintf(robotname, "robot1_svc");
  #endif
  /*sprintf(logfile, "/usr/local/logs/SUM/%s_%d.log", robotname, pid);*/
  pid = getppid();		/* pid of sum_svc */
  sprintf(logfile, "/usr/local/logs/SUM/tape_svc_%s.log", timetag);
  open_log(logfile);
  printk_set(write_log, write_log);
  write_log("\n## %s %s for pid = %d ##\n", 
		datestring(), robotname, pid);
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
  register SVCXPRT *transp;

  get_cmd(argc, argv);

      /* register for tape_svc to talk to us */
      #ifdef ROBOT_0
      (void) pmap_unset(ROBOT0PROG, ROBOT0VERS);
      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if(!svc_register(transp,ROBOT0PROG,ROBOT0VERS,robot0prog_1,IPPROTO_TCP)) {
	write_log("***unable to register (ROBOT0PROG, ROBOT0VERS, tcp)\n");
	exit(1);
      }
      #endif
      #ifdef ROBOT_1
      (void) pmap_unset(ROBOT1PROG, ROBOT1VERS);
      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if(!svc_register(transp,ROBOT1PROG,ROBOT1VERS,robot1prog_1,IPPROTO_TCP)) {
	write_log("***unable to register (ROBOT1PROG, ROBOT1VERS, tcp)\n");
	exit(1);
      }
      #endif
  sleep(2);			/* give time to start */
  setup();
  /* Create client handle used for calling the tape_svc */
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc");
    write_log("tape_svc not there on %s\n", thishost);
    exit(1);
  }
  /* Enter svc_run() which calls svc_getreqset when msg comes in.
   * svc_getreqset calls robot[n]prog_1() to process the msg.
   * NOTE: svc_run() never returns.
  */
  svc_run();
  write_log("!!!Fatal Error: svc_run() returned in robot[n]_svc\n");
  exit(1);
}

/* This is the dispatch routine that's called when the client does a
 * clnt_call() to ROBOT[0,1]PROG, ROBOT[0,1]VERS
*/
static void
#ifdef ROBOT_0
robot0prog_1(rqstp, transp)
#endif
#ifdef ROBOT_1
robot1prog_1(rqstp, transp)
#endif
	struct svc_req *rqstp;
	SVCXPRT *transp;
{
	union __svcargun {
		Rkey tapedo_1_arg;
	} argument;
	char *result, *call_err;
        enum clnt_stat clnt_stat;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();

	switch (rqstp->rq_proc) {
	case NULLPROC:
		(void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
		return;
	case ROBOTDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;;
		local = (char *(*)()) robotdo_1;
		break;
	case ROBOTDOORDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;;
		local = (char *(*)()) robotdoordo_1;
		break;
	default:
                write_log("**robot[0,1]prog_1() dispatch default procedure %d,ignore\n", rqstp->rq_proc);
		svcerr_noproc(transp);
		return;
	}
	bzero((char *)&argument, sizeof(argument));
	if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
                write_log("***Error on svc_getargs()\n");
		svcerr_decode(transp);
                /*return;*/
                /* NEW:23May2002 don't return.Can result in caller getting: */
                /* Dsds_svc returned error code 5600 */
                /* NEW: 10Jun2002 try this: */
                svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
                return;

	}
        glb_transp = transp;		     /* needed by function */
	/* call the function. sets current_client, procnum & rinfo */
        result = (*local)(&argument, rqstp);

      if(result) {				/* send the result now */
        if(result == (char *)1) {
          /* no client handle. do nothing, just return */
        }
        else {
          if(debugflg) {
            write_log("\nKEYS in robot[0,1]_svc response to tape_svc are:\n");
            keyiterate(logkey, (KEY *)result);
          }
          clnt_stat=clnt_call(current_client, procnum, (xdrproc_t)xdr_result,
		(char *)result, (xdrproc_t)xdr_void, 0, TIMEOUT);
          if(clnt_stat != 0) {
            clnt_perrno(clnt_stat);		/* outputs to stderr */
            write_log("***Error in robotn_svc on clnt_call() back to %d procedure\n", procnum);
            call_err = clnt_sperror(current_client, "Err");
            write_log("%s\n", call_err);
          }
          /*clnt_destroy(current_client);*/ 
          freekeylist((KEY **)&result);
        }
      }
      if (!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
	write_log("**unable to free arguments\n");
	/*exit(1);*/
      }
      return;
}

/* Send ack to original tape_svc caller. Uses global vrbls glb_transp and
 * rinfo which are set up before this call.
 * I'm not quite sure what to do on an error here?? I've never seen it and
 * will ignore it for now.
*/
void send_ack()
{
  /* send ack back with the rinfo value */
  if (!svc_sendreply(glb_transp, (xdrproc_t)xdr_uint32_t, (char *)&rinfo)) {
    write_log("***Error on immed ack back to client. FATAL???\n");
    svcerr_systemerr(glb_transp);
  }
}

/* Called by tape_svc doing: clnt_call(clntrobot[0,1], ROBOTDO, ...)
 * The robot will execute the given command to load/unload a drive.
 *  A typical param list looks like:
 * cmd1:   KEYTYP_STRING   mtx -f /dev/sg7 load 15 0 1> /tmp/mtx_robot.log 2>&1
 * snum:   KEYTYP_INT      14
 * dnum:   KEYTYP_INT      0
 * wd_0:   KEYTYP_STRING   /SUM5/D1523
 * effective_date_0:       KEYTYP_STRING   200510211235
 * sumid_0:        KEYTYP_UINT64    840
 * bytes_0:        KEYTYP_DOUBLE              1.200000e+08
 * status_0:       KEYTYP_INT      4
 * archsub_0:      KEYTYP_INT      128
 * group_id_0:     KEYTYP_INT      99
 * safe_id_0:      KEYTYP_INT      0
 * ds_index_0:     KEYTYP_UINT64    1523
 * username_0:     KEYTYP_STRING   jim
 * DEBUGFLG:       KEYTYP_INT      1
 * wd_1:   KEYTYP_STRING   /SUM1/D464
 * effective_date_1:       KEYTYP_STRING   200510211235
 * sumid_1:        KEYTYP_UINT64    460
 * bytes_1:        KEYTYP_DOUBLE              1.200000e+08
 * status_1:       KEYTYP_INT      4
 * archsub_1:      KEYTYP_INT      128
 * group_id_1:     KEYTYP_INT      99
 * safe_id_1:      KEYTYP_INT      0
 * ds_index_1:     KEYTYP_UINT64    464
 * username_1:     KEYTYP_STRING   jim
 * wd_2:   KEYTYP_STRING   /SUM1/D460
 * effective_date_2:       KEYTYP_STRING   200510211235
 * sumid_2:        KEYTYP_UINT64    458
 * bytes_2:        KEYTYP_DOUBLE              1.200000e+08
 * status_2:       KEYTYP_INT      4
 * archsub_2:      KEYTYP_INT      128
 * group_id_2:     KEYTYP_INT      99
 * safe_id_2:      KEYTYP_INT      0
 * ds_index_2:     KEYTYP_UINT64    460
 * username_2:     KEYTYP_STRING   jim
 * reqcnt: KEYTYP_INT      3
 * OP:     KEYTYP_STRING   wt
 * current_client: KEYTYP_FILEP    6917529027641701008
 * procnum:        KEYTYP_UINT32    1
 * tapeinfo:       KEYTYP_FILEP    6917529027641652544
*/
KEY *robotdo_1(KEY *params)
{
  CLIENT *client;
  static KEY *retlist;
  int sim, d, s, ret;
  int loadcmd = 0;
  char *cmd;
  char errstr[128], scr[80];

  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in robotdo_1() is:\n");
    keyiterate(logkey, params);
  }
  rinfo = 0;			/* assume success */
  send_ack();			/* give ack to caller */
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  sim = 0;
  if(findkey(params, "sim")) {
    sim = getkey_int(params, "sim");
  }

  if(findkey(params, "cmd1")) {
    cmd = GETKEY_str(params, "cmd1");
    if(strstr(cmd, " load ")) {	/* cmd like: mtx -f /dev/sg12 load 14 1 */
      loadcmd = 1;
      sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d);
      s--;                       /* must use internal slot # */
    }
    write_log("*Rb:cmd: %s\n", cmd);
    if(sim) {				/* simulation mode only */
      sleep(4);
    } 
    else {
      sleep(2);				/* !!!TEMP - test for robot ready*/
      if(system(cmd)) {
        write_log("***Rb:failure\n\n");
        setkey_int(&retlist, "STATUS", 1);   /* give err back to caller */
        sprintf(errstr, "Error on: %s", cmd);
        setkey_str(&retlist, "ERRSTR", errstr);
        current_client = clnttape;            /* set for call to tape_svc */
        procnum = TAPERESPROBOTDO;            /* this proc number */
        return(retlist);
      }
    }
    write_log("***Rb:success\n\n");
  }
  if(findkey(params, "cmd2")) {
    cmd = GETKEY_str(params, "cmd2");
    if(strstr(cmd, " load ")) {	/* cmd like: mtx -f /dev/sg12 load 14 1 */
      loadcmd = 1;
      sscanf(cmd, "%s %s %s %s %d %d", scr, scr, scr, scr, &s, &d);
      s--;                       /* must use internal slot # */
    }
    write_log("*Rb:cmd: %s\n", cmd);
    if(sim) {				/* simulation mode only */
      sleep(4);
    } 
    else {
      sleep(2);				/* !!!TEMP - test for robot ready*/
      if(system(cmd)) {
        write_log("***Rb:failure\n\n");
        setkey_int(&retlist, "STATUS", 1);   /* give err back to caller */
        sprintf(errstr, "Error on: %s", cmd);
        setkey_str(&retlist, "ERRSTR", errstr);
        current_client = clnttape;            /* set for call to tape_svc */
        procnum = TAPERESPROBOTDO;            /* this proc number */
        return(retlist);
      }
    }
    write_log("***Rb:success\n\n");
  }
  current_client = clnttape;            /* set for call to tape_svc */
  procnum = TAPERESPROBOTDO;            /* this proc number */
  setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
  if(loadcmd) {				/* make sure drive is rdy after load */
    if(!drive_ready(sim, d)) {
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr, "Error: drive not ready after: %s", cmd);
      setkey_str(&retlist, "ERRSTR", errstr);
    }
  }
  return(retlist);
}

/* Called by tape_svc doing: clnt_call(clntrobot[0,1], ROBOTDOORDO, ...)
 * The robot will execute the given commands.
 *  A typical param list looks like:
 * OP:     KEYTYP_STRING   impexp
 * cmd_1:  KEYTYP_STRING   mtx -f /dev/sg12 transfer 2 124 1> 
 *				/tmp/mtx_robot_1.log 2>&1
 * cmd_0:  KEYTYP_STRING   mtx -f /dev/sg12 transfer 1 123 1> 
 * 				/tmp/mtx_robot_0.log 2>&1
 * reqcnt: KEYTYP_INT      2
 * tapeid_0:       KEYTYP_STRING   013389S1
 * tapeid_1:       KEYTYP_STRING   013401S1
*/
KEY *robotdoordo_1(KEY *params)
{
  CLIENT *client;
  static KEY *retlist;
  int sim, reqcnt, i, mvdoor2slot;
  char *cmd, *cptr;
  char ext[96], errstr[128];

  /*debugflg = getkey_int(params, "DEBUGFLG");*/
  if(debugflg) {
    write_log("!!Keylist in robotdoordo_1() is:\n");
    keyiterate(logkey, params);
  }
  mvdoor2slot = 0;
  rinfo = 0;			/* assume success */
  send_ack();			/* give ack to caller */
  retlist = newkeylist();
  add_keys(params, &retlist);
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  current_client = clnttape;            /* set for call to tape_svc */
  procnum = TAPERESPROBOTDOORDO;            /* this proc number */
  sim = 0;
  if(findkey(params, "sim")) {
    sim = getkey_int(params, "sim");
  }
  reqcnt = getkey_int(params, "reqcnt");
  cptr = GETKEY_str(params, "OP");
  if(!strcmp(cptr, "door")) {    /* tapes were moved from door to slots */
    mvdoor2slot = 1;
  }
  for(i=0; i < reqcnt; i++) {
    sprintf(ext, "cmd_%d", i);
    cmd = GETKEY_str(params, ext);
    if(mvdoor2slot)
      write_log("*Rb:t50door: %s\n", cmd);
    else
      write_log("*Rb:door: %s\n", cmd);
    if(sim) {				/* simulation mode only */
      sleep(4);
    } 
    else {
      sleep(2);				/* !!!TEMP - test for robot ready*/
      if(system(cmd)) {
        if(mvdoor2slot)
          write_log("**Rb:t50doorfailure\n\n");
        else
          write_log("**Rb:doorfailure\n\n");
        setkey_int(&retlist, "STATUS", 1);   /* give err back to caller */
        sprintf(errstr, "Error on: %s", cmd);
        setkey_str(&retlist, "ERRSTR", errstr);
        return(retlist);
      }
    }
    if(mvdoor2slot)
      write_log("**Rb:t50doorsuccess\n\n");
    else
      write_log("**Rb:doorsuccess\n\n");
  }
  if(mvdoor2slot)
    write_log("**Rb:t50doorcomplete\n\n");
  else
    write_log("**Rb:doorcomplete\n\n");
  setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
  return(retlist);
}

/* Return with 1 if the given drive goes ready before the timeout.
 * Else return 0.
*/
int drive_ready(int sim, int drive)
{
  int fd;
  int ret = 1;
  int waitcnt = 0;
  char devname[16];
  struct mtget mt_stat;

  if(sim) { return(1); }
  sprintf(devname, "/dev/nst%d", drive);
  fd = open(devname, O_RDONLY | O_NONBLOCK);
  while(1) {
    ioctl(fd, MTIOCGET, &mt_stat);
    /*write_log("mt_gstat = 0x%0x\n", mt_stat.mt_gstat); /* !!!TEMP */
    if(mt_stat.mt_gstat == 0) {		/* not ready yet */
      if(++waitcnt == MAX_WAIT) {
        ret = 0;
        break;
      }
      sleep(1);
    }
    else break;
  }
  close(fd);
  return(ret);
}

void write_time()
{
  struct timeval tvalr;
  struct tm *t_ptr;
  char datestr[32];

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr.tv_sec);
  sprintf(datestr, "%s", asctime(t_ptr));
  write_log("%s", datestr);
}

