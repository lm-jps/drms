/* driven_svc.c
 * Called by tape_svc when it has a drive to transfer data on.
*/
/*
 * This file was originally generated using rpcgen and then edited.
 *
 * NOTE: write_log() calls have special prefixes for interaction with
 * t120view (tui).
*/

#include <SUM.h>
#include <sys/errno.h>
#include <sys/wait.h>
#include <rpc/pmap_clnt.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <string.h>
#include <printk.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stropts.h>
#include <sys/mtio.h>

#define MAX_WAIT 20  /* max times to wait for rdy in get_tape_file_num() */
#define CMDLENWRT 16384
#define GTARLOGDIR "/var/logs/SUM/gtar"
#define GTAR "/usr/local/bin/gtar"	/* gtar 1.16 on 29May2007 */
int write_wd_to_drive();
int write_hdr_to_drive();
int position_tape_eod();
int position_tape_bot();
int position_tape_file_asf();
int position_tape_file_fsf();
int get_tape_file_num(); 
void drive_reset();
uint64_t tell_blocks();
int get_cksum();
int read_drive_to_wd();
char *get_time();
void write_time();
char logfile[MAX_STR];
char md5file[64];
char md5filter[128];

void logkey();
extern int errno;
#ifdef DRIVE_0
static void drive0prog_1();
#endif
#ifdef DRIVE_1
static void drive1prog_1();
#endif
#ifdef DRIVE_2
static void drive2prog_1();
#endif
#ifdef DRIVE_3
static void drive3prog_1();
#endif
static struct timeval TIMEOUT = { 300, 0 };
uint32_t rinfo;		/* info returned by XXXdo_1() calls */
uint32_t procnum;	/* remote procedure # to call for current_client call*/

FILE *logfp;
CLIENT *current_client, *clnttape, *clntsum;
SVCXPRT *glb_transp;
int debugflg = 0;
int drivenum;
int efnum = 0;
char *dbname;
char *timetag;
char drvname[MAX_STR];
char thishost[MAX_STR];
char datestr[32];

int soi_errno = NO_ERROR;

/*********************************************************/
void open_log(char *filename)
{
  /*if((logfp=fopen(filename, "w")) == NULL) {*/
  if((logfp=fopen(filename, "a")) == NULL) {
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

/*********************************************************/
/* Outputs the variable format message (re: printf) to the log file.
*/
int write_log(const char *fmt, ...)
{
  va_list args;
  char string[CMDLENWRT];

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
    write_log("*** %s %s got SIGTERM. Exiting.\n", datestring(), drvname);
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s %s got SIGINT. Exiting.\n", datestring(), drvname);
    DS_DisConnectDB();
    exit(1);
  }
  write_log("*** %s %s got an illegal signal %d, ignoring...\n",
			datestring(), drvname, sig);
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
    while((c = *++argv[0]))
      switch(c) {
      case 'd':
        debugflg=1;	/* can also be sent in by client calls */
        break;
      default:
        break;
      }
  }
  if(argc != 2) {
    printf("\nERROR: driven_svc must be call with dbname and timestamp\n");
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
  #ifdef DRIVE_0
  sprintf(drvname, "drive0_svc");
  drivenum = 0;
  #endif
  #ifdef DRIVE_1
  sprintf(drvname, "drive1_svc");
  drivenum = 1;
  #endif
  #ifdef DRIVE_2
  sprintf(drvname, "drive2_svc");
  drivenum = 2;
  #endif
  #ifdef DRIVE_3
  sprintf(drvname, "drive3_svc");
  drivenum = 3;
  #endif
  /*sprintf(logfile, "/usr/local/logs/SUM/%s_%s.log", drvname, timetag);*/
  pid = getppid();		/* pid of sum_svc */
  sprintf(logfile, "/usr/local/logs/SUM/tape_svc_%s.log", timetag);
  open_log(logfile);
  printk_set(write_log, write_log);
  write_log("\n## %s %s for pid = %d ##\n", 
		datestring(), drvname, pid);
  write_log("Database to connect to is %s\n", dbname);
  sprintf(md5filter, "/home/production/cvs/jsoc/bin/%s/md5filter",
		getenv("JSOC_MACHINE"));
  write_log("md5filter=%s\n", md5filter); /* !!!TEMP */
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
  char compcmd[256];

  get_cmd(argc, argv);

      /* register for tape_svc to talk to us */
      #ifdef DRIVE_0
      (void) pmap_unset(DRIVE0PROG, DRIVE0VERS);
      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if(!svc_register(transp,DRIVE0PROG,DRIVE0VERS,drive0prog_1,IPPROTO_TCP)) {
	write_log("***unable to register (DRIVE0PROG, DRIVE0VERS, tcp)\n");
	exit(1);
      }
#ifndef SUMDC
      /* turn compression off */
      sprintf(compcmd, "sudo /usr/local/bin/mt -f /dev/nst0 defcompression 0");
      write_log("%s\n", compcmd);
      if(system(compcmd)) {
        write_log("***Error on: %s\n", compcmd);
        /*exit(1);*/
      }
#endif
      #endif
      #ifdef DRIVE_1
      (void) pmap_unset(DRIVE1PROG, DRIVE1VERS);
      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if(!svc_register(transp,DRIVE1PROG,DRIVE1VERS,drive1prog_1,IPPROTO_TCP)) {
	write_log("***unable to register (DRIVE1PROG, DRIVE1VERS, tcp)\n");
	exit(1);
      }
#ifndef SUMDC
      /* turn compression off */
      sprintf(compcmd, "sudo /usr/local/bin/mt -f /dev/nst1 defcompression 0");
      write_log("%s\n", compcmd);
      if(system(compcmd)) {
        write_log("***Error on: %s\n", compcmd);
        /*exit(1);*/
      }
#endif
      #endif
      #ifdef DRIVE_2
      (void) pmap_unset(DRIVE2PROG, DRIVE2VERS);
      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if(!svc_register(transp,DRIVE2PROG,DRIVE2VERS,drive2prog_1,IPPROTO_TCP)) {
	write_log("***unable to register (DRIVE2PROG, DRIVE2VERS, tcp)\n");
	exit(1);
      }
#ifndef SUMDC
      /* turn compression off */
      sprintf(compcmd, "sudo /usr/local/bin/mt -f /dev/nst2 defcompression 0");
      write_log("%s\n", compcmd);
      if(system(compcmd)) {
        write_log("***Error on: %s\n", compcmd);
        /*exit(1);*/
      }
#endif
      #endif
      #ifdef DRIVE_3
      (void) pmap_unset(DRIVE3PROG, DRIVE3VERS);
      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if(!svc_register(transp,DRIVE3PROG,DRIVE3VERS,drive3prog_1,IPPROTO_TCP)) {
	write_log("***unable to register (DRIVE3PROG, DRIVE3VERS, tcp)\n");
	exit(1);
      }
#ifndef SUMDC
      /* turn compression off */
      sprintf(compcmd, "sudo /usr/local/bin/mt -f /dev/nst3 defcompression 0");
      write_log("%s\n", compcmd);
      if(system(compcmd)) {
        write_log("***Error on: %s\n", compcmd);
        /*exit(1);*/
      }
#endif
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
  /* Create client handle used for calling the sum_svc */
  clntsum = clnt_create(thishost, SUMPROG, SUMVERS, "tcp");
  if(!clntsum) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_svc");
    write_log("***sum_svc not there on %s\n", thishost);
    exit(1);
  }

  DS_ConnectDB(dbname);      /* connect to Oracle at start */

  /* Enter svc_run() which calls svc_getreqset when msg comes in.
   * svc_getreqset calls drive[n]prog_1() to process the msg.
   * NOTE: svc_run() never returns.
  */
  svc_run();
  write_log("!!!Fatal Error: svc_run() returned in drive[n]_svc\n");
  exit(1);
}

/* This is the dispatch routine that's called when the client does a
 * clnt_call() to DRIVE[0,1]PROG, DRIVE[0,1]VERS
*/
static void
#ifdef DRIVE_0
drive0prog_1(rqstp, transp)
#endif
#ifdef DRIVE_1
drive1prog_1(rqstp, transp)
#endif
#ifdef DRIVE_2
drive2prog_1(rqstp, transp)
#endif
#ifdef DRIVE_3
drive3prog_1(rqstp, transp)
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
	case READDRVDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;;
		local = (char *(*)()) readdrvdo_1;
		break;
	case WRITEDRVDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) writedrvdo_1;
		break;
	default:
                write_log("**drive[0,1]prog_1() dispatch default procedure %d,ignore\n", rqstp->rq_proc);
		svcerr_noproc(transp);
		return;
	}
	bzero((char *)&argument, sizeof(argument));
	if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
                write_log("***Error on svc_getargs()\n");
		svcerr_decode(transp);
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
            write_log("\nKEYS in drive[0,1]_svc response to tape_svc are:\n");
            keyiterate(logkey, (KEY *)result);
          }
          clnt_stat=clnt_call(current_client, procnum, (xdrproc_t)xdr_result,
		(char *)result, (xdrproc_t)xdr_void, 0, TIMEOUT);
          if(clnt_stat != 0) {
            clnt_perrno(clnt_stat);		/* outputs to stderr */
            write_log("***Error on clnt_call() back to %d procedure (driven_svc)\n", procnum);
            call_err = clnt_sperror(current_client, "Err");
            write_log("%s\n", call_err);
          }
          /*clnt_destroy(current_client);*/ 
          freekeylist((KEY **)&result);
        }
      }
      else {
        /* the answer from dsds_svc or ampex_svc is picked up by getcmd() */
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

/* Called by tape_svc doing: clnt_call(clntdrv[0,1], READDRVDO, ...)
 * The tape is in the drive and we will position it to the given file number
 * and do the read.
 * A typical param list looks like:
 * filenum:        KEYTYP_INT      1    (from struct DRIVE)
 * tapemode:       KEYTYP_INT      2    (from struct DRIVE)
 * dnum:   KEYTYP_INT      0
 * dsix_1: KEYTYP_UINT64    2048
 * dsix_0: KEYTYP_UINT64    2047
 * username:       KEYTYP_STRING   production
 * REQCODE:        KEYTYP_INT      4
 * DEBUGFLG:       KEYTYP_INT      1
 * reqcnt: KEYTYP_INT      2
 * tdays:  KEYTYP_INT      5
 * mode:   KEYTYP_INT      16
 * uid:    KEYTYP_UINT64    1016
 * current_client: KEYTYP_FILEP    5867600
 * online_status_0:        KEYTYP_STRING   N
 * archive_status_0:       KEYTYP_STRING   Y
 * bytes_0:        KEYTYP_DOUBLE              1.200000e+08
 * create_sumid_0: KEYTYP_UINT64    1011
 * ds_index_0:     KEYTYP_UINT64    2047
 * wd_0:   KEYTYP_STRING   /SUM1/D2047
 * tapeid_0:       KEYTYP_STRING   000014S1
 * tapefilenum_0:  KEYTYP_INT      1
 * online_status_1:        KEYTYP_STRING   N
 * archive_status_1:       KEYTYP_STRING   Y
 * bytes_1:        KEYTYP_DOUBLE              1.200000e+08
 * create_sumid_1: KEYTYP_UINT64    1012
 * ds_index_1:     KEYTYP_UINT64    2048
 * wd_1:   KEYTYP_STRING   /SUM2/D2048
 * tapeid_1:       KEYTYP_STRING   000014S1
 * tapefilenum_1:  KEYTYP_INT      1
 * offline:        KEYTYP_INT      1
 * ds_index:       KEYTYP_UINT64    2054
 * partn_name:     KEYTYP_STRING   /SUM3/D2054
 * rootwd_0:       KEYTYP_STRING   /SUM2/D2053
 * rootwd_1:       KEYTYP_STRING   /SUM3/D2054
 * offcnt: KEYTYP_INT      2
 * OP:     KEYTYP_STRING   rd
 * sim:    KEYTYP_INT      0
 * reqofflinenum:  KEYTYP_INT      1
*/
KEY *readdrvdo_1(KEY *params)
{
  CLIENT *client;
  static KEY *retlist;
  int sim;
  uint64_t file_dsix[MAXSUMREQCNT+1]; /* the ds_index of all SU in a tape file*/
  uint64_t file_dsix_off[MAXSUMREQCNT+1]; /* the ds_index that are offline*/
  uint64_t uid, ds_index;
  double file_bytes[MAXSUMREQCNT+1];  /* bytes of all SU in a tape file */
  double bytes;
  int dnum, tapefilenum, reqofflinenum, status, tapemode, filenum, touch;
  int reqcnt, xtapefilenum, i, j, filenumdelta;
  char tmpname[80], newdir[80], errstr[128], dname[64], rdlog[80];
  char *wd, *oldwd, *token, *lasttoken;
  char *tapeid, *xtapeid, *cptr, *delday;
  char *initdirs[MAXSUMREQCNT];
  


  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in readdrvdo_1() is:\n");
    keyiterate(logkey, params);
  }
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  dnum = getkey_int(params, "dnum");
  sprintf(dname, "%s%d", SUMDR, dnum);
  sim = 0;
  if(findkey(params, "sim")) {
    sim = getkey_int(params, "sim");
  }
  uid = getkey_uint64(params, "uid");
  /* get which index in the params list of an offline SU */
  reqofflinenum = getkey_int(params, "reqofflinenum");

  /* get the already assigned wd and read the tape */
  sprintf(tmpname, "rootwd_%d", reqofflinenum);
  wd = getkey_str(params, tmpname);
  sprintf(tmpname, "tapefilenum_%d", reqofflinenum);
  tapefilenum = getkey_int(params, tmpname);
  rinfo = RESULT_PEND;
  send_ack();			/* give ack to caller so doesn't wait */

  current_client = clnttape;            /* set for call to tape_svc */
  procnum = TAPERESPREADDO;            /* this proc number */
  tapemode = getkey_int(params, "tapemode");
  if(tapemode == TAPE_RD_INIT) {	/* position from bot */
    if(position_tape_file_asf(sim, dnum, tapefilenum) == -1) {
      setkey_int(&retlist, "STATUS", 1); /* give error back to caller */
      sprintf(errstr, "Error on position tape to file #%d in drive #%d", 
			tapefilenum, dnum);
      setkey_str(&retlist, "ERRSTR", errstr); 
      free(wd);
      return(retlist); 
    }
  }
  else if(tapemode == TAPE_RD_CONT) {	/* ck if reading next file# */
    /*filenum = getkey_int(params, "filenum"); /* !!NG */
    /* Can't use the filenum passed in. The drives[] table may not
     * have gotten updated yet with a return from driven_svc.
    */
    if((filenum = get_tape_file_num(sim, dname, dnum)) == -1) {
      write_log("***Error: can't get file # on drive %d\n", dnum);
      setkey_int(&retlist, "STATUS", 1); /* give error back to caller */
      sprintf(errstr, "Error on position tape to file #%d in drive #%d",
                        tapefilenum, dnum);
      setkey_str(&retlist, "ERRSTR", errstr);
      free(wd);
      return(retlist);
    }
    filenumdelta = tapefilenum - filenum; /* num of files to skip */
    if(position_tape_file_fsf(sim, dnum, filenumdelta) == -1) {
      setkey_int(&retlist, "STATUS", 1); /* give error back to caller */
      sprintf(errstr, "Error on position tape to file #%d in drive #%d",
                        tapefilenum, dnum);
      setkey_str(&retlist, "ERRSTR", errstr);
      free(wd);
      return(retlist);
    }
    if((filenum = get_tape_file_num(sim, dname, dnum)) == -1) {
      write_log("***Error: can't get file # on drive %d\n", dnum);
      setkey_int(&retlist, "STATUS", 1); /* give error back to caller */
      sprintf(errstr, "Error on position tape to file #%d in drive #%d",
                        tapefilenum, dnum);
      setkey_str(&retlist, "ERRSTR", errstr);
      free(wd);
      return(retlist);
    }
    write_log("Dr%d:rd:Next file on tape=%d\n", dnum, filenum);
  }
  /* now read any SU that is offline and has the same tape/file# */
  /* NOTE: Changed 19Sep2007 
   * NOTE: Grouping the SU into a tape file for effeciency of write may not 
   *       have been a good idea. It created a new entity that the logic
   *       was not designed for. The tape files were to be individual SUs.
   *       So here's a kludge that tries to localize the damage.
   * It was found that subsequent read request were frequently for other SU
   * in the same tape file, so the tape was going back and forth reading the
   * same file. We will now read all the SU in a file regardless of whether
   * they are offline or not. But in order to get the current tracking which
   * is by individual SU to work, we will have to take all SU in the file 
   * offline, and mark their current wd del pending, before the read. Also
   * make sure that when the original wd is removed by sum_rm it does not
   * use the ds_index to mark the sum_main offline, as it is online in the
   * new wd that was assigned to the read.
   * (The original code here was saved in driven_svc.c.ORIG_RD)
  */

  reqcnt = getkey_int(params, "reqcnt");
  sprintf(tmpname, "tapeid_%d", reqofflinenum);
  tapeid = getkey_str(params, tmpname);
  /* get ds_index of each SU in the file */
  if(SUMLIB_Ds_Ix_Find(tapeid, tapefilenum, file_dsix, file_bytes)) {
      write_log("***Error: SUMLIB_Ds_Ix_Find(%s, %d)\n", tapeid, tapefilenum);
      setkey_int(&retlist, "STATUS", 1); /* give error back to caller */
      sprintf(errstr, "Error: SUMLIB_Ds_Ix_Find(%s, %d)", tapeid, tapefilenum);
      setkey_str(&retlist, "ERRSTR", errstr);
      free(wd);
      free(tapeid);
      return(retlist);
  }

  /* now determine if all these ds_index are online. If so there is nothing
   * to do. Else retrieve the ones that are offline from the tape.
  */
  status = SUMLIB_Ds_Ix_File(file_dsix, file_dsix_off);
  if(status == -1) {
     setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
     sprintf(errstr, "Error on SUMLIB_Ds_Ix_File() in driven_svc");
     setkey_str(&retlist, "ERRSTR", errstr);
     free(wd);
     free(tapeid);
     return(retlist);
  }
  if(status == 1) {
    /* at least one is offline */
    sprintf(rdlog, "%s/gtar_rd_%d_%s_%d.log",
                        GTARLOGDIR, dnum, tapeid, tapefilenum);
    if((status = read_drive_to_wd(sim, wd, dnum, tapeid, tapefilenum, rdlog, file_dsix_off))== -1) {
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr, "Error on read drive #%d into %s", dnum, wd);
      setkey_str(&retlist, "ERRSTR", errstr); 
      free(wd);
      free(tapeid);
      return(retlist); 
    }
    /* update the DB for each SU just brought online (same tape/file) */
    /* update the SUM DB with online 'Y' and the new wd */
    for(i=0; ; i++) {
      ds_index = file_dsix_off[i];
      if(!ds_index) break;
      sprintf(newdir, "%s/D%ld", wd, ds_index);
      bytes = file_bytes[i];
        if(SUM_StatOnline(ds_index, newdir)) {
          setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
          sprintf(errstr,"Error: can't put SU ix=%ld online in SUM_MAIN table", 
			ds_index);
          setkey_str(&retlist, "ERRSTR", errstr); 
          free(wd);
          free(tapeid);
          return(retlist);
        }
        if(findkey(params, "tdays"))
          touch = getkey_int(params, "tdays");
        else
          touch = 2;
        delday = (char *)get_effdate(touch);  /* delete in touch days */
        /* put on delete pending list in sum_partn_alloc table */
        if(NC_PaUpdate(newdir, uid, bytes, DADP, 0, delday,
		 0, 0, ds_index, 1, 1)) {  /* call w/commit */
          setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
          sprintf(errstr,"Error: can't put SU %s on del pend list", newdir);
          setkey_str(&retlist, "ERRSTR", errstr); 
          free(wd);
          free(tapeid);
          return(retlist);
        }
        free(delday);
        /* going to query again anyway so don't need to do this */
        /*sprintf(tmpname, "wd_%d", i);		/* update the target wd */
        /*if(findkey(retlist, tmpname)) 
        /*  setkey_str(&retlist, tmpname, newdir);
        */
    }
  }
  setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
  free(wd);
  free(tapeid);
  return(retlist);
}

/* Called by tape_svc doing: clnt_call(clntdrv[0,1], WRITEDRVDO, ...)
 * when it has  SUM storage unit(s) to write to tape.
 * The tape is in the drive and we will position it to eod and do the write.
 * The input keylist looks like:
 * cmd1:   KEYTYP_STRING   mtx -f /dev/sg7 load 15 0 1> /tmp/mtx_robot.log 2>&1
 * snum:   KEYTYP_INT      14
 * dnum:   KEYTYP_INT      0
 * wd_0:   KEYTYP_STRING   /SUM5/D1523
 * effective_date_0:       KEYTYP_STRING   200504261120
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
 * effective_date_1:       KEYTYP_STRING   200504261120
 * sumid_1:        KEYTYP_UINT64    460
 * bytes_1:        KEYTYP_DOUBLE              1.200000e+08
 * status_1:       KEYTYP_INT      4
 * archsub_1:      KEYTYP_INT      128
 * group_id_1:     KEYTYP_INT      99
 * safe_id_1:      KEYTYP_INT      0
 * ds_index_1:     KEYTYP_UINT64    464
 * username_1:     KEYTYP_STRING   jim
 * wd_2:   KEYTYP_STRING   /SUM1/D460
 * effective_date_2:       KEYTYP_STRING   200504261120
 * sumid_2:        KEYTYP_UINT64    458
 * bytes_2:        KEYTYP_DOUBLE              1.200000e+08
 * status_2:       KEYTYP_INT      4
 * archsub_2:      KEYTYP_INT      128
 * group_id_2:     KEYTYP_INT      99
 * safe_id_2:      KEYTYP_INT      0
 * ds_index_2:     KEYTYP_UINT64    460
 * username_2:     KEYTYP_STRING   jim
 * total_bytes:    KEYTYP_DOUBLE              4.800000e+08
 * reqcnt: KEYTYP_INT      3
 * OP:     KEYTYP_STRING   wt
 * current_client: KEYTYP_FILEP    6917529027641638480
 * procnum:        KEYTYP_UINT32    1
 * group_id:       KEYTYP_INT      99
 * tape_closed:    KEYTYP_INT      -1
 * STATUS: KEYTYP_INT      0
 * availblocks:    KEYTYP_UINT64   48828000
 * tapeid:	   KEYTYP_STRING   000014S
 * nxtwrtfn:	   KEYTYP_INT	   120
*/
KEY *writedrvdo_1(KEY *params)
{
  CLIENT *client;
  static KEY *retlist;
  uint64_t tell;
  int dnum, sim, tape_closed, group, filenumwrt;
  char *tapeid;
  char  errstr[128], gtarlog[80], md5sum[64];

  write_log("Called writedrvdo_1() in driven_svc\n");
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in writedrvdo_1() is:\n");
    keyiterate(logkey, params);
  }
  dnum = getkey_int(params, "dnum");
  filenumwrt = getkey_int(params, "nxtwrtfn"); /* file# to be written */
  sim = 0;
  if(findkey(params, "sim")) {
    sim = getkey_int(params, "sim");
  }
  group = getkey_int(params, "group_id");
  tapeid = GETKEY_str(params, "tapeid");
  tape_closed = getkey_int(params, "tape_closed");
  rinfo = RESULT_PEND;
  send_ack();                   /* give ack to caller so doesn't wait */

  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  client = (CLIENT *)getkey_fileptr(params, "current_client");
  /* final destination */
  setkey_fileptr(&retlist,  "current_client", (FILE *)client);
  current_client = clnttape;            /* set for call to tape_svc */
  procnum = TAPERESPWRITEDO;            /* this proc number */
  if(tape_closed == TAPECLOSED) {	/* closed tape */
      setkey_int(&retlist, "STATUS", TAPECLOSEDREJECT); /* err back to caller */
      sprintf(errstr,"Error: Can't write to closed tape %s (driven_svc)", tapeid);
      setkey_str(&retlist, "ERRSTR", errstr); 
      return(retlist);
  }
  if(tape_closed == TAPEUNINIT) {	/* uninitialized tape */
    if(position_tape_bot(sim, dnum) == -1) {
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr,"Error: Can't position tape %s to BOT in drive #%d", 
			tapeid, dnum);
      setkey_str(&retlist, "ERRSTR", errstr); 
      return(retlist);
    }
    sprintf(gtarlog, "%s/gtar_wt_%d_%s_%d.hdr.log", 
			GTARLOGDIR, dnum, tapeid, filenumwrt);
    if(write_hdr_to_drive(sim, tapeid, group, dnum, gtarlog) == -1) {
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr,"Error: Can't write TAPELABEL to tape %s in drive #%d", 
			tapeid, dnum);
      setkey_str(&retlist, "ERRSTR", errstr); 
      return(retlist);
    }
  }
    if(position_tape_eod(sim, dnum) == -1) {
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr,"Error: Can't position to EOD tape %s in drive #%d", 
			tapeid, dnum);
      setkey_str(&retlist, "ERRSTR", errstr); 
      return(retlist);
    }
    sprintf(gtarlog, "%s/gtar_wt_%d_%s_%d.log", 
			GTARLOGDIR, dnum, tapeid, filenumwrt);
    if(write_wd_to_drive(sim, params, dnum, filenumwrt, gtarlog) == -1) {
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr,"Error on write to drive #%d", dnum);
      setkey_str(&retlist, "ERRSTR", errstr); 
      return(retlist);
    }
    if(get_cksum(md5file, md5sum)) {
      write_log("***Error: can't get md5 cksum for drive %d.\n",dnum);
      setkey_str(&retlist, "md5cksum", "");
      setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
      sprintf(errstr,"Error on write to drive #%d", dnum);
      setkey_str(&retlist, "ERRSTR", errstr); 
      return(retlist);
    }
    setkey_str(&retlist, "md5cksum", md5sum);
#ifndef SUMDC
    if((tell = tell_blocks(sim, dnum)) == -1) { /* get current blk# */
      write_log("**Can't get current block# on drive %d. Proceed w/o it.\n",
		dnum);
      tell = 0;
    }
#else
    tell = 0;		/* the DC system does not support 'tell' cmd */
#endif
    setkey_uint64(&retlist, "tellblock", tell);
    setkey_int(&retlist, "gtarblock", GTARBLOCK);
  setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
  return(retlist);
}

/* Position the tape in the given drive to eod.
 * Return -1 on error.
*/
int position_tape_eod(int sim, int dnum)
{
  char cmd[128], dname[80];
  int lastfilenum;

  sprintf(dname, "%s%d", SUMDR, dnum);
  sprintf(cmd, "/usr/local/bin/mt -f %s eod 1>> %s 2>&1", dname, logfile);
  write_log("*Dr%d:wt: %s\n", dnum, cmd);
  if(sim) {			/* simulation mode only */
    sleep(4);
    lastfilenum = 999999;
  }
  else {
    if(system(cmd)) {
      write_log("***Dr%d:wt:error\n", dnum);
      return(-1);
    }
    if((lastfilenum = get_tape_file_num(sim, dname, dnum)) == -1) { 
      write_log("***Error: can't get file # on drive %d\n", dnum);
      return(-1);
    }
  }
  write_log("**Dr%d:wt:success eod=%d\n", dnum,lastfilenum); /*2 *'s here */
  return(0);
}

/* Position the tape in the given drive to bot.
 * Return -1 on error.
*/
int position_tape_bot(int sim, int dnum)
{
  char cmd[128], dname[80];

  sprintf(dname, "%s%d", SUMDR, dnum);
  sprintf(cmd, "/usr/local/bin/mt -f %s rewind 1>> %s 2>&1", dname, logfile);
  write_log("*Dr%d:wt: %s\n", drivenum, cmd);
  if(sim) {			/* simulation mode only */
    sleep(4);
  }
  else {
    if(system(cmd)) {
      write_log("***Dr%d:wt:error\n", drivenum);
      return(-1);
    }
  }
  write_log("**Dr%d:wt:success\n", drivenum); /* only 2 *'s here */
  if(get_tape_file_num(0, dname, dnum) == -1) { /* make sure ready */
    write_log("***Dr%d:wt:error\n", drivenum);
    return(-1);
  }
  write_log("**Dr%d:wt:success\n", drivenum); /* only 2 *'s here */
  return(0);
}

/* Position the tape in the given drive to the absolute given file number.
 * Return -1 on error.
*/
int position_tape_file_asf(int sim, int dnum, int fnum)
{
  char cmd[128], dname[80];

  sprintf(dname, "%s%d", SUMDR, dnum);
  sprintf(cmd, "/usr/local/bin/mt -f %s asf %d 1>> %s 2>&1", dname, fnum, logfile);
  write_log("*Dr%d:rd: %s\n", drivenum, cmd);
  if(sim) {				/* simulation mode only */
    sleep(4);
  }
  else {
    if(system(cmd)) {
      write_log("***Dr%d:rd:error\n", drivenum);
      drive_reset(dname);
      return(-1);
    }
  }
  if(get_tape_file_num(0, dname, dnum) == -1) { /* make sure ready */
    write_log("***Dr%d:rd:error\n", drivenum);
    drive_reset(dname);
    return(-1);
  }
  write_log("**Dr%d:rd:success\n", drivenum);	/* only 2 *'s here */
  return(0);
}

/* Position the tape in the given drive to fdelta files.
 * Return -1 on error.
*/
int position_tape_file_fsf(int sim, int dnum, int fdelta)
{
  char cmd[128], dname[80];

  if(fdelta == 0) { return(0); }
  sprintf(dname, "%s%d", SUMDR, dnum);
  if(fdelta < 0) {		/* going backwards */
    fdelta = -fdelta;
    /*fdelta += 2;*/
    fdelta += 1;
    sprintf(cmd, "/usr/local/bin/mt -f %s bsfm %d 1>> %s 2>&1", 
		dname, fdelta, logfile);
    
  }
  else {
    /* --fdelta;  /* NOTE: needed this before did the filenum query again */
    sprintf(cmd, "/usr/local/bin/mt -f %s fsf %d 1>> %s 2>&1", 
		dname, fdelta, logfile);
  }
  write_log("*Dr%d:rd: %s\n", drivenum, cmd);
  if(sim) {				/* simulation mode only */
    sleep(4);
  }
  else {
    if(system(cmd)) {
      write_log("***Dr%d:rd:error\n", drivenum);
      drive_reset(dname);
      return(-1);
    }
  }
  if(get_tape_file_num(0, dname, dnum) == -1) { /* make sure ready */
    write_log("***Dr%d:rd:error\n", drivenum);
    drive_reset(dname);
    return(-1);
  }
  write_log("**Dr%d:rd:success\n", drivenum);	/* only 2 *'s here */
  return(0);
}

/* Get the md5cksum just written into the given file.
 * Return 1 on error, else 0.
*/
int get_cksum(char *cfile, char *cksum)
{
  FILE *cfp;
  char row[MAXSTR];
  struct stat *statbuf = (struct stat *)malloc(sizeof(struct stat));

  stat(cfile, statbuf);
  if(statbuf->st_size == 0) {
    write_log("***Error: No md5 cksum produced.\n");
    return(1);
  }
  if (!(cfp = fopen(cfile, "r"))) {
    write_log("***Error: can't open %s\n", cfile);
    return(1);
  }
  while (fgets (row,MAXSTR,cfp)) {
      sscanf(row, "%s", cksum);
  }
  fclose(cfp);
  return(0);
}

uint64_t tell_blocks(int sim, int dnum)
{
  char cmd[128], dname[64], outname[64], scr[64], row[MAXSTR];
  uint64_t tell;
  FILE *tfp;

  /* NOTE: this can be done by multiple drives simultaneously */
  sprintf(outname, "/usr/local/logs/SUM/%d/tell_blocks.txt", dnum);
  sprintf(dname, "%s%d", SUMDR, dnum);
  sprintf(cmd, "/usr/local/bin/mt -f %s tell 1> %s 2>&1", dname, outname);
  write_log("cmd = %s\n", cmd);
  if(sim) {				/* simulation mode only */
    sleep(2);
  }
  else {
    if(system(cmd)) {
      write_log("***cmd failed\n");
      return(-1);
    }
  }
  write_log("**cmd success\n");
  if (!(tfp = fopen(outname, "r"))) {
    write_log("***Error: can't open %s\n", outname);
    return(-1);
  }
  while (fgets (row,MAXSTR,tfp)) {
    if(strstr(row, "At block ")) {
      sscanf(row, "%s %s %ld", scr, scr, &tell);
    }
  }
  fclose(tfp);
  return(tell);
}

/* Write from given dir(s) to the given drive# and log in logfile.
 * Returns the drive number on success, else -1. 
 * The keylist looks like:
 * reqcnt:      KEYTYP_INT      3
 * username_2:  KEYTYP_STRING   jim
 * ds_index_2:  KEYTYP_UINT64    376
 * safe_id_2:   KEYTYP_INT      0
 * group_id_2:  KEYTYP_INT      99
 * archsub_2:   KEYTYP_INT      128
 * status_2:    KEYTYP_INT      4
 * bytes_2:     KEYTYP_DOUBLE              1.200000e+08
 * sumid_2:     KEYTYP_UINT64    408
 * effective_date_2:    KEYTYP_STRING   200504261120
 * wd_2:        KEYTYP_STRING   /SUM3/D376
 * username_1:  KEYTYP_STRING   jim
 * ds_index_1:  KEYTYP_UINT64    378
 * safe_id_1:   KEYTYP_INT      0
 * group_id_1:  KEYTYP_INT      99
 * archsub_1:   KEYTYP_INT      128
 * status_1:    KEYTYP_INT      4
 * bytes_1:     KEYTYP_DOUBLE              1.200000e+08
 * sumid_1:     KEYTYP_UINT64    409
 * effective_date_1:    KEYTYP_STRING   200504261120
 * wd_1:        KEYTYP_STRING   /SUM5/D378
 * DEBUGFLG:    KEYTYP_INT      1
 * username_0:  KEYTYP_STRING   jim
 * ds_index_0:  KEYTYP_UINT64    379
 * safe_id_0:   KEYTYP_INT      0
 * group_id_0:  KEYTYP_INT      99
 * archsub_0:   KEYTYP_INT      128
 * status_0:    KEYTYP_INT      4
 * bytes_0:     KEYTYP_DOUBLE              1.200000e+08
 * sumid_0:     KEYTYP_UINT64    410
 * effective_date_0:    KEYTYP_STRING   200504261120
 * wd_0:        KEYTYP_STRING   /SUM1/D379
 * current_client: KEYTYP_FILEP  handle to original caller 
 * dnum:	KEYTYP_INT      1
 * nxtwrtfn:	KEYTYP_INT      120
*/
int write_wd_to_drive(int sim, KEY *params, int drive, int fnum, char *logname)
{
  char cmd[CMDLENWRT], dname[64], tmpname[64], wdroot[64], wdD[64];
  char *cptr, *wd;
  int status, cnt, i, len, tapefilenum;

  sprintf(dname, "%s%d", SUMDR, drive);
  sprintf(md5file, "/usr/local/logs/SUM/md5/cksum%d", drive); 
  sprintf(cmd, "touch %s", logname);	/* put a 0 len file there */
  if(status = system(cmd)) { 
      write_log("***Dr%d:wt:Err: %s\n", drive, cmd);
      return(-1);
  }
  /*sprintf(cmd, "tar -cvf %s -b %d ", dname, GTARBLOCK);*/
  sprintf(cmd, "%s -cf - -b %d ", GTAR, GTARBLOCK); /* output to stdout */
  cnt = getkey_int(params, "reqcnt");
  for(i = 0; i < cnt; i++) {
    sprintf(tmpname, "wd_%i", i);
    wd = GETKEY_str(params, tmpname);
    cptr = strstr(wd, "/D");
    strcpy(wdD, cptr+1);
    strcpy(wdroot, wd);
    cptr = strstr(wdroot, "/D");
    *cptr = (char)NULL;
    len = strlen(cmd) + strlen(wdroot) + strlen(wdD) + 8;
    if(len > CMDLENWRT) {
      write_log("***Error: cmd too long: %s\n", cmd);
      return(-1);
    }
    sprintf(cmd, "%s -C %s %s", cmd, wdroot, wdD);
  }
  /*sprintf(cmd, "%s 1>>%s 2>&1", cmd, logname);*/
  sprintf(cmd, "%s 2>%s | %s %d %s 2>> %s | dd of=%s bs=%db 2>> %s", 
	cmd, logname, md5filter, GTARBLOCK, md5file, logname, dname, GTARBLOCK, logname);
  write_time();
  write_log("*Dr%d:wt: %s\n", drive, cmd);
  if(sim) {                             /* simulation mode only */
    sleep(7);
  }
  else {
    if(status = system(cmd)) { /* status applies only to last cmd in pipe */
      write_log("***Dr%d:wt:Error. exit status=%d\n",drive,WEXITSTATUS(status));
      drive_reset(dname);
      return(-1);
    }
  }
  if((tapefilenum = get_tape_file_num(sim, dname, drive)) == -1) {
    write_log("***Error: can't get file # on drive %d\n", drive);
    return(-1);
  }
  write_log("Dr%d:wt:Next file on tape=%d\n", drive, tapefilenum);
  if(!sim) {
    if(tapefilenum != (fnum + 1)) {
      write_log("***Dr%d:wt:Error: Tape file expected=%d, found=%d\n", 
			drive, fnum+1, tapefilenum);
      return(-1); 
    }
  }
  write_log("***Dr%d:wt:success\n", drive); /*must be this form for t120 gui*/
  return(drive);
}

/* The tape is at bot. Write a label at the file 0 on any tape that needs 
 * to be initialized.
*/
int write_hdr_to_drive(int sim, char *tapeid, int group, int drive, char *log)
{
  FILE *lfp;
  char cmd[1024], dname[64], tmpname[64], dirname[64];
  int status;

  /* NOTE: a label can be done by multiple drives simultaneously */
  sprintf(dirname, "/usr/local/logs/SUM/%d", drive);
  sprintf(cmd, "mkdir -p %s", dirname);
  system(cmd);
  sprintf(tmpname, "%s/TAPELABEL", dirname);
  if((lfp=fopen(tmpname, "w")) == NULL) {
    write_log("***Error: Can't open %s errno=%d\n", tmpname, errno);
    return(-1);
  }
  fprintf(lfp, "Created %s", get_time());
  fprintf(lfp, "tapeid %s\n", tapeid);
  fprintf(lfp, "group_id %d\n", group);
  if(fclose(lfp)) {
    write_log("***Error: Can't close %s errno=%d\n", tmpname, errno);
    return(-1);
  }
  sprintf(dname, "%s%d", SUMDR, drive);
  sprintf(cmd, "%s -cvf %s -b %d -C %s TAPELABEL 2> %s", 
		GTAR, dname, GTARBLOCK, dirname, log);
  write_time();
  write_log("*Dr%d:wt: %s\n", drivenum, cmd);
  if(sim) {				/* simulation mode only */
    sleep(4);
  }
  else {
    if(status = system(cmd)) {
      write_log("***Dr%d:wt:Error. exit status=%d\n",drivenum,WEXITSTATUS(status));
      drive_reset(dname);
      return(-1);
    }
  }
  write_log("***Dr%d:wt:success\n", drivenum);/*must be this form for t120 gui*/
  return(drive);
}

/* Make sure the drive is ready before a timeout and then
 * get the file number of the current tape in the given drive name & number.
 * Return -1 on error, else the file number.
*/
int get_tape_file_num(int sim, char *dname, int drive) 
{
  int fd;
  int ret;
  int waitcnt = 0;
  struct mtget mt_stat;

  if(sim) { return(999999); }   /* give phoney file# back */
  fd = open(dname, O_RDONLY | O_NONBLOCK);
  while(1) {
    ioctl(fd, MTIOCGET, &mt_stat);
    /*write_log("mt_gstat = 0x%0x\n", mt_stat.mt_gstat); /* !!!TEMP */
    if(mt_stat.mt_gstat == 0) {         /* not ready yet */
      if(++waitcnt == MAX_WAIT) {
        ret = -1;
        break;
      }
      sleep(1);
    }
    else {
      ret = mt_stat.mt_fileno;
      break;
    }
  }
  close(fd);
  return(ret);
}

void drive_reset(char *dname) 
{
  int fd;
  int waitcnt = 0;
  struct mtop mt_op;
  struct mtget mt_stat;

  write_log("***PENDING RESET: drive %s\n", dname);
  fd = open(dname, O_RDONLY | O_NONBLOCK);
  mt_op.mt_op = MTRESET;	/* reset drive */
  mt_op.mt_count = 0;
  ioctl(fd, MTIOCTOP, &mt_op);
  while(1) {
     ioctl(fd, MTIOCGET, &mt_stat);
     if(mt_stat.mt_gstat == 0) {         /* not ready yet */
	if(++waitcnt == MAX_WAIT) {
	   write_log("***RESET ERROR: drive %s doesn't go ready\n", dname);
	   return;
	}
	sleep(1);
     }
  }
  close(fd);
  write_log("***RESET: drive %s\n", dname);
}

/* Read from given drive to the given dir and log in logfile.
 * Returns the drive number on success, else -1.
*/
int read_drive_to_wd(int sim, char *wd, int drive,  
	 char *tapeid, int tapefilenum, char *logname, uint64_t filedsixoff[])
{
  char cmd[8196], dname[80], md5file[80], md5sum[36], md5db[36], Ddir[128];
  uint64_t dsix;
  int i, status;

  sprintf(dname, "%s%d", SUMDR, drive);
  sprintf(md5file, "/usr/local/logs/SUM/md5/rdsum_%d", drive);
  sprintf(cmd, "dd if=%s bs=%db 2> %s | %s %d %s 2>> %s | %s xvf - -b%d -C %s", 
   dname, GTARBLOCK, logname, md5filter, GTARBLOCK, md5file, logname, GTAR, GTARBLOCK, wd);

  for(i=0; ; i++) {
    dsix = filedsixoff[i];
    if(!dsix) break;
    sprintf(Ddir, "D%ld", dsix);
    sprintf(cmd, "%s %s", cmd, Ddir);
  }
  sprintf(cmd, "%s 1>>%s 2>> %s", cmd, logfile, logname);
  write_time();
  write_log("*Dr%d:rd: %s\n", drivenum, cmd);
  if(sim) {				/* simulation mode only */
    sleep(7);
    /* !!!TEMP for test */
    /*write_log("***Dr%d:rd:!!SIM Error. exit status=%d\n", drivenum, 1);*/
    /*return(-1);*/
  }
  else {
    if(status = system(cmd)) {
      write_log("***Dr%d:rd:Error. exit status=%d\n", drivenum, WEXITSTATUS(status));
      return(-1);
    }
  }
  if(get_cksum(md5file, md5sum)) { return(-1); }
  if(SUMLIB_Get_MD5(tapeid, tapefilenum, md5db)) {
    return(-1);
  }
  if(!sim) {
    if(strcmp(md5sum, md5db)) {
      write_log("***Dr%d:rd:Error md5 compare:\n", drivenum);
      write_log(" exp=%s read=%s\n", md5db, md5sum);
      return(-1);
    }
  }
  write_log("***Dr%d:rd:success %s\n", drivenum, md5sum);
  return(drive);
}

char *get_time()
{
  struct timeval tvalr;
  struct tm *t_ptr;
  static char datestr[32];

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr.tv_sec);
  sprintf(datestr, "%s", asctime(t_ptr));
  return(datestr);
}

void write_time()
{
  write_log("%s", get_time());
}

