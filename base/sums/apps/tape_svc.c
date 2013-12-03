/* tape_svc.c
 *
 * This is spawned during the sum_svc startup.
 * sum_svc makes calls for tape reads and tapearc makes calles for writes 
 * to tape_svc. tape_svc queues the requests and when appropriate calls
 * drive#_svc (where # equals the tape drive number) to do the tape 
 * positioning and read/write. Results are eventually returned to sum_svc
 * who returns them to the original caller (from the SUMS API).
*/

#include <SUM.h>
#include <rpc/pmap_clnt.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <tape.h>
#include <printk.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "serverdefs.h"

#define REBOOT "/usr/local/logs/SUM/RESTART_AFTER_REBOOT"
#define ALRMSECTO 30	//30 will give average 45sec t.o. on rd drive

SLOT slots[MAX_SLOTS];
DRIVE drives[MAX_DRIVES];
#ifdef SUMDC
int drive_order[MAX_DRIVES];
#else
int drive_order_rd[MAX_DRIVES];
int drive_order_wt[MAX_DRIVES];
int nxtscanrd;
int nxtscanwt;
int max_drives_rd = 0;
int max_drives_wt = 0;
#endif
int noalrm = 0;
int Empty_Slot_Cnt;

void logkey();
extern void write_time();
extern int tape_inventory();
extern int tape_free_drives();
extern int kick_next_entry_rd();
extern int kick_next_entry_wt();
extern int errno;
extern TQ *q_rd_front;
extern TQ *q_wrt_front;
extern TQ *q_need_front;
extern SUMOFFCNT *offcnt_hdr;
static void tapeprog_1();
//static struct timeval TIMEOUT = { 30, 0 };
//must be <50 sec used for sum_svc msg to tape_svc
static struct timeval TIMEOUT = { 40, 0 };
uint32_t rinfo, rinfox;	/* info returned by XXXdo_1() calls */
uint32_t procnum;	/* remote procedure # to call for current_client call*/
uint32_t sumprog, sumvers;

TQ *poff, *poffrd, *poffwt;
FILE *logfp;
CLIENT *current_client, *clntsum, *clntdrv0;
CLIENT *clntrobot0;
CLIENT *clntdrv[MAX_DRIVES];
CLIENT *clntsums[SUM_MAXNUMSUM+1];
SVCXPRT *glb_transp;
int debugflg = 0;
int sim = 0;
int tapeoffline = 0;
int robotoffline = 0;
int driveonoffstatus = 0;
int pactive = 0;
int current_client_destroy;
int alrm_sec;			//secs to next alarm signal
char *dbname;
char *timetag;
char thishost[MAX_STR];
char sumhost[MAX_STR];
char hostn[MAX_STR];
char datestr[32];
char libdevname[32];
char heaplow[32];
char heaphigh[32];
char heapcln[32];
static int numSUM;

int soi_errno = NO_ERROR;

/*********************************************************/
void open_log(char *filename)
{
  char cmd[80];

  /*if((logfp=fopen(filename, "w")) == NULL) {*/
  sprintf(cmd, "/bin/rm -f %s", filename); /* make sure any old one gone */
  system(cmd);
  if((logfp=fopen(filename, "a+")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", filename);
  }
}

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[].
*/
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
    write_log("*** %s tape_svc got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s tape_svc got SIGINT. Exiting.\n", datestring());
    DS_DisConnectDB();
    exit(1);
  }
  write_log("*** %s tape_svc got an illegal signal %d, ignoring...\n",
			datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
}

/* Called when get a SIGALRM every alrm_sec seconds.
 * Clear any expired timeout on a drive.
*/
void alrm_sig(int sig)
{
  int d, e, tocheck, offcnt, kstatus;
  uint32_t ssprog;
  enum clnt_stat clnt_stat;
  SUMID_t uid;
  char *call_err;
  TQ *p;
  SUMOFFCNT *offptr;
  bool_t (*xdr_argument)(), (*xdr_result)();

    if(noalrm) {
      signal(SIGALRM, &alrm_sig);   //setup for alarm signal
      alarm(alrm_sec);		//start again
      return;
    }
    tocheck = 0;
    for(e=0; e < max_drives_rd; e++) {
      d = drive_order_rd[e];
      if(drives[d].to) {		//timeout is active
        --drives[d].tocnt;
        if(drives[d].tocnt == 0) {  //timeout 
          drives[d].to = 0;
          tocheck = 1;
        }
      }
    }
    //There might be an entry in the rd Q that was waiting on a final timeout
    if(tocheck) {
      //make sure don't re-enter
      if(!pactive) { 
      /* check the q if something else can be kicked off now */
      if((p = q_rd_front) != NULL) {	/* try to kick off next entry */
        write_log("Attempt to kick next rd from alrm_sig()\n");
        kstatus = kick_next_entry_rd(); /* sets poff if entry removed */
        poffrd = poff;
        //write_time(); 
	//write_log("kick_next_entry_rd() call from tapeprog_1() returned %d\n",
	//		kstatus);	/* !!TEMP */
        switch(kstatus) {
        case 0:			/* can't process now. remains on q */
          break;
        case 1:			/* entry started and removed from q */
          break;
        case 2:			/* error occured. ret list to orig sender*/
          setkey_int(&poff->list, "STATUS", 1); /* give error back to caller */
          uid = getkey_uint64(poff->list, "uid");
          offptr = getsumoffcnt(offcnt_hdr, uid);
          offptr->offcnt++;
          offcnt = getkey_int(poff->list, "offcnt");
          if(offcnt == offptr->offcnt) { /* now can send completion msg back */
            ssprog = getkey_uint32(poff->list, "SPROG");
            if(offptr->sprog != ssprog) { 
              write_log("**ERROR: Bug at alrm_sig. tape rd sprog != ssprog\n");
              //ssprog = offptr->sprog;    //use this for the return
            } 
            //sumvers = getkey_uint32(poff->list, "SVERS");
            remsumoffcnt(&offcnt_hdr, uid);
            //set client handle for the sums process
            switch(ssprog) {
            case SUMPROG:
              clntsum = clntsums[0];
              break;
            case SUMGET:
              clntsum = clntsums[1];
              break;
            case SUMGET1:
              clntsum = clntsums[2];
              break;
            case SUMGET2:
              clntsum = clntsums[3];
              break;
            case SUMGET3:
              clntsum = clntsums[4];
              break;
            case SUMGET4:
              clntsum = clntsums[5];
              break;
            case SUMGET5:
              clntsum = clntsums[6];
              break;
            case SUMGET6:
              clntsum = clntsums[7];
              break;
            case SUMGET7:
              clntsum = clntsums[8];
              break;
            default:
              write_log("**ERROR: bad sumprog in taperespreaddo_1()\n");
              break;
            }
            clnt_stat=clnt_call(clntsum, SUMRESPDO, (xdrproc_t)xdr_result, 
			   (char *)poff->list, (xdrproc_t)xdr_void, 0, TIMEOUT);
            if(clnt_stat != 0) {
              clnt_perrno(clnt_stat);		/* outputs to stderr */
              write_log("***Error in tape_svc on clnt_call() back to sum_svc SUMRESPDO\n");
              call_err = clnt_sperror(current_client, "Err");
              write_log("%s\n", call_err);
            }
          }
          break;
        }
      }
      }
    }
  signal(SIGALRM, &alrm_sig);   //setup for alarm signal
  alarm(alrm_sec);		//start again
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
  if(argc != 2) {
    printf("\nERROR: tape_svc must be call with dbname and timestamp\n");
    exit(1);
  }
  else {
    dbname = argv[0];
    timetag = argv[1];
  }
}

/*********************************************************/
void setoffline()
{
  FILE *drfp;
  int order0;
  char logname[MAX_STR], line[256];

  sprintf(logname, "/usr/local/logs/SUM/drive_offline.txt");
  if((drfp=fopen(logname, "r")) == NULL) {
    fprintf(stderr, "Can't open the file %s. Proceed anyway.\n", logname);
  }
  else {
    while(fgets(line, 256, drfp)) {  //set offline the given drives
      if(!strncmp(line, "#", 1)) {   //ignore line starting with #
        continue;
      }
      sscanf(line, "%d", &order0);
      if(order0 >= MAX_DRIVES) {
        write_log("Error drivenum >= MAX_DRIVES (%d)\n", MAX_DRIVES);
        write_log("Can't take drive %d offline.\n", order0);
        fclose(drfp);
        return;
      }
      sprintf(line, "take drive %d offline", order0);
      printf("%s\n", line);
      write_log("%s\n", line);
      drives[order0].offline = 1;
      //drives[order0].lock = 0;  //lock no longer used
    }
  }
  fclose(drfp);
}


void setup()
{
  FILE *sgfp, *drfp;
  int pid, ppid, order0, neworder, i;
  char *cptr, *token;
  char logname[MAX_STR], line[256], rwchars[32];

  numSUM = SUM_NUMSUM;          //in base/drms/apps/serverdefs.h;
  //when change name of dcs2 to dcs1 we found out you have to use localhost
  sprintf(thishost, "localhost");
  gethostname(hostn, MAX_STR);
  cptr = index(hostn, '.');     // must be short form
  if(cptr) *cptr = (char)NULL;
  cptr = datestring();
  ppid = getppid();		/* pid of sum_svc (old. before sum_forker) */
  pid = getpid();
  sprintf(logname, "/usr/local/logs/SUM/tape_svc_%s.log", timetag);
  open_log(logname);
  printk_set(write_log, write_log);
  write_log("\n## %s tape_svc on %s for pid = %d ##\n", 
		datestring(), thishost, pid);
  write_log("Database to connect to is %s\n", dbname);
#ifdef SUMDC
  sprintf(logname, "/usr/local/logs/SUM/drive_order.txt");
  if((drfp=fopen(logname, "r")) == NULL) {
    fprintf(stderr, "Can't open the file file %s\n", logname);
    order0 = 0;
  }
  else {
    fgets(line, 256, drfp);
    sscanf(line, "%d", &order0);
  }
  fclose(drfp);
  neworder = order0 + 1;
  if(neworder >= MAX_DRIVES) neworder = 0;
  drfp=fopen(logname, "w");
  fprintf(drfp, "%d", neworder);
  fclose(drfp);
  for(i=0; i < MAX_DRIVES; i++) {
    drive_order[i] = order0;
    //printf("drive_order[%d] = %d\n", i, order0); //!!!TEMP
    order0++;
    if(order0 >= MAX_DRIVES) order0 = 0;
  }
#else
  sprintf(logname, "/usr/local/logs/SUM/drive_order_rw.txt");
  if((drfp=fopen(logname, "r")) == NULL) {
    fprintf(stderr, "Can't open the file file %s\n", logname);
    exit(1);
  }
  else {
    i = 0;
    //NOTE: All the rd must be assigned before any wt
    while(fgets(line, 256, drfp)) {  //Must be exactly 12 (MAX_DRIVES) entries
      if(!strncmp(line, "#", 1)) {   //ignore line starting with #
        continue;
      }
      sscanf(line, "%d %s", &order0, rwchars);
      write_log("rw = %s  drive# = %d\n", rwchars, order0);
      if(!strcmp(rwchars, "rd")) {
        drive_order_rd[i] = order0;
        max_drives_rd++;
      } 
      else if(!strcmp(rwchars, "wt")) {
        drive_order_wt[i] = order0;
        max_drives_wt++;
      }
      else {
        printf("%s is incorrect format\n", logname);
        exit(1);
      }
      i++;
    }
    nxtscanrd = 0;
    nxtscanwt = max_drives_rd;
    write_log("max_drives_rd = %d, max_drives_wt = %d\n",
		max_drives_rd, max_drives_wt);
  }
  fclose(drfp);

  /* get heap values. only for non-datacapture machine */
  /* assumes format is from ia64 machine (d02) */
  sprintf(logname, "/proc/%d/maps", pid);
  if((drfp=fopen(logname, "r")) == NULL) {
    fprintf(stderr, "Can't open the file %s\n", logname);
  }
  else {
    while(fgets(line, 256, drfp)) { 
      if(!strstr(line, "[heap")) continue;
      token = (char *)strtok(line, "-");
      strcpy(heaplow, token);
      //Note: this heaphigh changes w/time. So don't want to rd file all the time
      //token = (char *)strtok(NULL, " ");
      //strcpy(heaphigh, token);
      fgets(line, 256, drfp); //assumes the next line gives max possible heap
      token = (char *)strtok(line, "-");
      strcpy(heaphigh, token);
      break;
    }
    fclose(drfp);
  }
#endif
  alrm_sec = ALRMSECTO;	//40 will give an average 60sec t.o. on a rd drive
  //if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  signal(SIGALRM, &alrm_sig);
  alarm(alrm_sec);			//set up first alarm

  sprintf(libdevname, "%s", LIBDEV);
/*  if(!(sgfp=fopen(LIBDEVFILE, "r"))) {
/*    write_log("Can't open device config file %s. Using default %s\n", 
/*		LIBDEVFILE, LIBDEV);
/*  }
/*  else {
/*    while(fgets(line, 256, sgfp)) {     // last /dev in file will be it
/*      if(line[0] == '#' || line[0] == '\n') continue;
/*      if(strstr(line, "/dev/")) {
/*        if((cptr=index(line, '\n')) != NULL) 
/*          *cptr = (char)NULL; 
/*        sprintf(libdevname, "%s", line);
/*      }
/*    }
/*    fclose(sgfp);
/*  }
*/
}

//This is called at the tape_svc exit via exit() or by return from main()
void sumbye(void) {
  printf("sumbye() called by atexit() at %s\n", datestring());
  write_log("sumbye() called by atexit() at %s\n", datestring());
}

/*********************************************************/
int main(int argc, char *argv[])
{
  register SVCXPRT *transp;
  struct stat sbuf;
  char cmd[80];
  int retry, istat, i;

  get_cmd(argc, argv);
  setup();
  if(atexit(sumbye)) {
    printf("Can't register sumbye() function in atexit()\n");
  }

      /* register for sum_svc and tapearc to talk to us */
      (void) pmap_unset(TAPEPROG, TAPEVERS);

      transp = svctcp_create(RPC_ANYSOCK, 0, 0);
      if (transp == NULL) {
	write_log("***cannot create tcp service\n");
	exit(1);
      }
      if (!svc_register(transp, TAPEPROG, TAPEVERS, tapeprog_1, IPPROTO_TCP)) {
	write_log("***unable to register (TAPEPROG, TAPEVERS, tcp)\n");
	exit(1);
      }
  /* set host name with the sum_svc */
  if(strcmp(hostn, TAPEHOST)) { //if running on d02, use j1
    sprintf(sumhost, TAPEHOST);
  }
  else {
    sprintf(sumhost, SUMSVCHOST);
  }
  sleep(3); /* driven_svc & robotn_svc forked by sum_svc, let it start */
  for(i=0; i < MAX_DRIVES; i++) {
    retry = 0;
    while(retry < 2) {		//try twice
      /* Create client handle used for calling drive[0,1,2,3]_svc */
      clntdrv0 = clnt_create(thishost, DRIVE0PROG+i, DRIVE0VERS, "tcp");
      if(!clntdrv0) {       /* program not there */
        clnt_pcreateerror("Can't get client handle to driven_svc in tape_svc");
        write_log("***tape_svc can't get drive%d_svc on %s for %u\n", 
			i, thishost, DRIVE0PROG+i);
        retry++;
        continue;
      }
      else { write_log("##tape_svc gets handle for drive%d_svc for %u\n", 
			i, DRIVE0PROG+i); }
      clntdrv[i] = clntdrv0;
      break;
    }
    if(retry >= 2) {
      write_log("***Fatal error: Can't get handle for drive%d_svc\n", i);
      send_mail("***Fatal error: Can't get handle for drive%d_svc\n", i);
      exit(1);
    }
  }
  clntrobot0 = clnt_create(thishost, ROBOT0PROG, ROBOT0VERS, "tcp");
  if(!clntrobot0) {       /* program not there */
    clnt_pcreateerror("Can't get client handle to robot0_svc in tape_svc");
    write_log("***tape_svc can't get robote0_svc on %s\n", thishost);
    exit(1);
  }

  //create handles for calling back sum process for tape operations
    //always have SUMPROG
    clntsum = clnt_create(sumhost, SUMPROG, SUMVERS, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMPROG in tape_svc");
      write_log("***tape_svc can't get handle to SUMPROG on %s\n", sumhost);
      exit(1);
    }
    clntsums[0] = clntsum;

  if(numSUM >= 2) {
    clntsum = clnt_create(sumhost, SUMGET, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET on %s\n", sumhost);
      exit(1);
    }
    clntsums[1] = clntsum;
    clntsum = clnt_create(sumhost, SUMGET1, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET1 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET1 on %s\n", sumhost);
      exit(1);
    }
    clntsums[2] = clntsum;
  }
  if(numSUM >= 3) {
    clntsum = clnt_create(sumhost, SUMGET2, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET2 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET2 on %s\n", sumhost);
      exit(1);
    }
    clntsums[3] = clntsum;
  }
  if(numSUM >= 4) {
    clntsum = clnt_create(sumhost, SUMGET3, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET3 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET3 on %s\n", sumhost);
      exit(1);
    }
    clntsums[4] = clntsum;
  }
  if(numSUM >= 5) {
    clntsum = clnt_create(sumhost, SUMGET4, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET4 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET4 on %s\n", sumhost);
      exit(1);
    }
    clntsums[5] = clntsum;
  }
  if(numSUM >= 6) {
    clntsum = clnt_create(sumhost, SUMGET5, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET5 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET5 on %s\n", sumhost);
      exit(1);
    }
    clntsums[6] = clntsum;
  }
  if(numSUM >= 7) {
    clntsum = clnt_create(sumhost, SUMGET6, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET6 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET6 on %s\n", sumhost);
      exit(1);
    }
    clntsums[7] = clntsum;
  }
  if(numSUM >= 8) {
    clntsum = clnt_create(sumhost, SUMGET7, SUMGETV, "tcp");
    if(!clntsum) {
      clnt_pcreateerror("Can't get client handle to SUMGET7 in tape_svc");
      write_log("***tape_svc can't get handle to SUMGET7 on %s\n", sumhost);
      exit(1);
    }
    clntsums[8] = clntsum;
  }

  /* No robot1 for now... */
  /*clntrobot1 = clnt_create(thishost, ROBOT1PROG, ROBOT1VERS, "tcp");
  /*if(!clntrobot1) {       /* program not there */
  /*  clnt_pcreateerror("Can't get client handle to robot1_svc in tape_svc");
  /*  write_log("tape_svc can't get robot1_svc on %s\n", thishost);
  /*  exit(1);
  /* }
  */

  DS_ConnectDB(dbname);	/* connect to Oracle before inventory */

  /* now inventory the tape unit to get tapeid and slot and drive info */
  /* Must be done after svc_register() so 
   * sum_svc will see us, as the inventory takes awhile */
  /* NOTE: the inventory sometimes returns only 64 slots instead of the 120.
   * If so tape_inventory() will ret -1 and we will try again for a number 
   * of times and then finally fail.
  */
  if(tapeoffline == 0) {
    retry = 6;
    while(retry) {
#ifdef SUMDC
      if((istat = tape_inventory(sim, 1)) == 0) { 
#else
      if((istat = tape_inventory(sim, 0)) == 0) { /* no catalog here */
#endif 
        write_log("***Error: Can't do tape inventory. Will retry...\n");
        --retry;
        if(retry == 0) {
          write_log("***Fatal error: Can't do tape inventory\n");
          (void) pmap_unset(TAPEPROG, TAPEVERS);
          exit(1);
        }
        continue;
      }
      if(istat == -1) {	/* didn't get full slot count. retry */
        --retry;
        if(retry == 0) {
          write_log("***Fatal error: Can't do tape inventory\n");
          (void) pmap_unset(TAPEPROG, TAPEVERS);
          exit(1);
        }
      }
      else { retry = 0; }
    }
    /* Return any tapes in drives to free slots */
    /* Do this after machine reboot or tape driver reload. */
    /* sum_start will ask if reboot and touch the REBOOT file */
    if(!stat(REBOOT, &sbuf)) {	//it's a reboot
      printf("!!NOTE: tape unload in progress. May take some time.\n");
      if(!tape_free_drives()) {
        write_log("**Fatal error: Can't free tapes in drives\n");
        (void) pmap_unset(TAPEPROG, TAPEVERS);
        exit(1);
      }
      sprintf(cmd, "/bin/rm -f %s", REBOOT); /* make sure old one gone */
      system(cmd);
    }
    else {
      printf("!!!NOTE: tape unload disabled\n");
    }
  }
  setoffline();		//check text file for any drives to be offline
  /* Enter svc_run() which calls svc_getreqset when msg comes in.
   * svc_getreqset calls tapeprog_1() to process the msg.
   * NOTE: svc_run() never returns.
  */
  svc_run();
  write_log("!!!Fatal Error: svc_run() returned in tape_svc\n");
  exit(1);
}

/* This is the dispatch routine that's called when the client does a
 * clnt_call() to TAPEPROG, TAPEVERS
*/
static void 
tapeprog_1(rqstp, transp)
	struct svc_req *rqstp;
	SVCXPRT *transp;
{
	char *result, *call_err;
        int force = 0;
        int rdflg = 0;
        uint32_t sprog;
        enum clnt_stat clnt_stat;
        SUMOFFCNT *offptr;
        SUMID_t uid;
        int kstatus, offcnt, errorcase;
        TQ *p;
	union __svcargun {
		Rkey tapedo_1_arg;
	} argument;
        uint32_t spare;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();
        pactive = 1;		//process active. used by alrm_sig
        rinfox = 1;
	switch (rqstp->rq_proc) {
	case NULLPROC:
		(void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
                pactive = 0;
		return;
	case READDO:
                if(tapeoffline) rinfox = SUM_TAPE_SVC_OFF; //special case 
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;;
		local = (char *(*)()) readdo_1;
		break;
	case WRITEDO:
                if(tapeoffline) rinfox = SUM_TAPE_SVC_OFF; //special case 
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) writedo_1;
		break;
        case TAPERESPREADDO:
		rdflg = 1;
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) taperespreaddo_1;
		break;
        case TAPERESPWRITEDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) taperespwritedo_1;
		break;
        case TAPERESPROBOTDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) taperesprobotdo_1;
		break;
        case TAPERESPROBOTDOORDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) taperesprobotdoordo_1;
		break;
	case IMPEXPDO:
		xdr_argument = xdr_Rkey;
                xdr_result = xdr_uint32_t;
		local = (char *(*)()) impexpdo_1;
		break;
	case EXPCLOSEDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
                xdr_result = xdr_uint32_t;
		local = (char *(*)()) expclosedo_1;
		break;
        case TAPETESTDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) tapetestdo_1;
		break;
	case ONOFFDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
                xdr_result = xdr_uint32_t;
		local = (char *(*)()) onoffdo_1;
		break;
	case ROBOTONOFFDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
                xdr_result = xdr_uint32_t;
		local = (char *(*)()) robotonoffdo_1;
		break;
	case DRONOFFDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
                xdr_result = xdr_uint32_t;
		local = (char *(*)()) dronoffdo_1;
		break;
	case JMTXTAPEDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;
		local = (char *(*)()) jmtxtapedo_1;
		break;
	case TAPENOPDO:
                force = 1;			/* always make this call */
		xdr_argument = xdr_Rkey;
                xdr_result = xdr_uint32_t;
		local = (char *(*)()) tapenopdo_1;
		break;
	default:
                write_log("**tapeprog_1() dispatch default procedure %d,ignore\n", rqstp->rq_proc);
		svcerr_noproc(transp);
                pactive = 0;
		return;
	}
	bzero((char *)&argument, sizeof(argument));
	if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
          write_log("***Error on svc_getargs()\n");
          svcerr_decode(transp);
          svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
          pactive = 0;
          return;
	}
        glb_transp = transp;		     /* needed by function */
        poff = NULL;
        current_client_destroy = 0;
        if((tapeoffline) && (!force)) {		/* return err for all calls */
          rinfo = rinfox;		/* give err to caller */
	  send_ack();                   /* ack original sum_svc caller */
          result = (KEY *)1;		/* return immediately */
        }
        else {
	  /* call the function. sets current_client, procnum & rinfo */
          result = (*local)(&argument, rqstp);
        }
      if(result) {				/* send the result now */
        if(result == (char *)1) {
          /* no client handle. do nothing, just return */
        }
        else {
          if(debugflg) {
            write_log("\nKEYS in tape_svc response are:\n");
            keyiterate(logkey, (KEY *)result);
          }
          if(current_client == 0) {
            write_log("***Error on clnt_call() back to orig tape_svc caller\n");
            write_log("   current_client was NULL\n");
          }
          else {
            /***********************only for ia64**********************************
            sprintf(heapcln, "%lx", current_client); 
            if(rdflg) {
              write_log("current_client = %s\n", heapcln); //!!TEMP for debug
            }
            if(((strcmp(heapcln, heaphigh)) > 0) || ((strcmp(heapcln, heaplow)) < 0)) {
              write_log("***Error: current_client = %lx is off heap\n", 
			current_client);
            }
            else {
            ***********************only for ia64**********************************/
              clnt_stat=clnt_call(current_client, procnum, (xdrproc_t)xdr_result, 
			result, (xdrproc_t)xdr_void, 0, TIMEOUT);
              if(clnt_stat != 0) {
                clnt_perrno(clnt_stat);		/* outputs to stderr */
                write_log("***Error in tape_svc on clnt_call() back to %ld procedure\n", procnum);
                call_err = clnt_sperror(current_client, "Err");
                write_log("%s %s\n", datestring(), call_err);
              }
              if(current_client_destroy) clnt_destroy(current_client);
            //}
          }
          freekeylist((KEY **)&result);
        }
        if(poff) {		/* free this q entry */
          //write_log("%lu free at line 497\n", poff); //!!TEMP
          free(poff->tapeid);
          free(poff->username);
          freekeylist((KEY **)&poff->list);
          free(poff);
          poff = NULL;
        }
      }
      if (!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
	write_log("***unable to free arguments\n");
	/*exit(1);*/
      }
      /* check the q if something else can be kicked off now */
      if((p = q_rd_front) != NULL) {	/* try to kick off next entry */
        kstatus = kick_next_entry_rd(); /* sets poff if entry removed */
        poffrd = poff;
        //write_time(); 
	//write_log("kick_next_entry_rd() call from tapeprog_1() returned %d\n",
	//		kstatus);	/* !!TEMP */
        switch(kstatus) {
        case 0:			/* can't process now. remains on q */
          break;
        case 1:			/* entry started and removed from q */
          break;
        case 2:			/* error occured. ret list to orig sender*/
          setkey_int(&poff->list, "STATUS", 1); /* give error back to caller */
          uid = getkey_uint64(poff->list, "uid");
          offptr = getsumoffcnt(offcnt_hdr, uid);
          offptr->offcnt++;
          offcnt = getkey_int(poff->list, "offcnt");
          if(offcnt == offptr->offcnt) { /* now can send completion msg back */
            remsumoffcnt(&offcnt_hdr, uid);
            sprog = getkey_uint32(poff->list, "SPROG");
            if(offptr->sprog != sprog) {
              write_log("**ERROR: Bug. In tape rd sprog != sumprog\n");
              sprog = offptr->sprog;    //use this for the return
            }

            //sumvers = getkey_uint32(poff->list, "SVERS");
            //set client handle for the sums process
            switch(sprog) {
            case SUMPROG:
              clntsum = clntsums[0];
              break;
            case SUMGET:
              clntsum = clntsums[1];
              break;
            case SUMGET1:
              clntsum = clntsums[2];
              break;
            case SUMGET2:
              clntsum = clntsums[3];
              break;
            case SUMGET3:
              clntsum = clntsums[4];
              break;
            case SUMGET4:
              clntsum = clntsums[5];
              break;
            case SUMGET5:
              clntsum = clntsums[6];
              break;
            case SUMGET6:
              clntsum = clntsums[7];
              break;
            case SUMGET7:
              clntsum = clntsums[8];
              break;
            default:
              write_log("**ERROR: bad sumprog in taperespreaddo_1()\n");
              break;
            }
            clnt_stat=clnt_call(clntsum, SUMRESPDO, (xdrproc_t)xdr_result, 
			   (char *)poff->list, (xdrproc_t)xdr_void, 0, TIMEOUT);
            if(clnt_stat != 0) {
              clnt_perrno(clnt_stat);		/* outputs to stderr */
              write_log("***Error in tape_svc on clnt_call() back to sum_svc SUMRESPDO\n");
              call_err = clnt_sperror(current_client, "Err");
              write_log("%s\n", call_err);
            }
          }
          break;
        }
      }
      while((p = q_wrt_front) != NULL) { 
        errorcase = 0;
        kstatus = kick_next_entry_wt(); /* sets poff if entry removed */
        poffwt = poff;
        //write_time(); 
	//write_log("kick_next_entry_wt() call from tapeprog_1() returned %d\n",
	//		kstatus);
        switch(kstatus) {
        case 0:			/* can't process now. remains on q */
          break;
        case 1:			/* entry started and removed from q */
          break;
        case 2: case 3:		/* error occured. ret list to orig sender*/
          setkey_int(&poff->list, "STATUS", 1);/* give err back to caller */
          uid = getkey_uint64(poff->list, "uid");
          errorcase = 1;
          //NOTE there is no getsumoffcnt() to get! 22May2012
          //if(offptr = getsumoffcnt(offcnt_hdr, uid)) {
            //offptr->offcnt++;
            //offcnt = getkey_int(poff->list, "offcnt");
            //if(offcnt == offptr->offcnt) { //now can send completion msg back
              //remsumoffcnt(&offcnt_hdr, uid);
              sumprog = getkey_uint32(poff->list, "SPROG");
              //sumvers = getkey_uint32(poff->list, "SVERS");
              //set client handle for the sums process
              switch(sumprog) {
              case SUMPROG:
                clntsum = clntsums[0];
                break;
              case SUMGET:
                clntsum = clntsums[1];
                break;
              case SUMGET1:
                clntsum = clntsums[2];
                break;
              case SUMGET2:
                clntsum = clntsums[3];
                break;
              case SUMGET3:
                clntsum = clntsums[4];
                break;
              case SUMGET4:
                clntsum = clntsums[5];
                break;
              case SUMGET5:
                clntsum = clntsums[6];
                break;
              case SUMGET6:
                clntsum = clntsums[7];
                break;
              case SUMGET7:
                clntsum = clntsums[8];
                break;
              default:
                write_log("**ERROR: bad sumprog in taperespreaddo_1()\n");
                break;
              }
              clnt_stat=clnt_call(clntsum, SUMRESPDO, (xdrproc_t)xdr_result, 
  			(char *)poff->list, (xdrproc_t)xdr_void, 0, TIMEOUT);
              if(clnt_stat != 0) {
                clnt_perrno(clnt_stat);		/* outputs to stderr */
                write_log("***Error in tape_svc on clnt_call() back to sum_svc SUMRESPDO\n");
                call_err = clnt_sperror(current_client, "Err");
                write_log("%s\n", call_err);
              }
            //}
          //}
          break;
        }
        if(errorcase == 0) break;	/* end while(p=q_wrt_front) */
      }
      if(poffrd) {		/* free this q entry */
        //write_log("%lu free at line 577\n", poffrd); //!!TEMP
        free(poffrd->tapeid);
        free(poffrd->username);
        freekeylist((KEY **)&poffrd->list);
        free(poffrd);
        poffrd = NULL;
      }
      if(poffwt) {		/* free this q entry */
        //write_log("%lu free at line 585\n", poffwt); //!!TEMP
        free(poffwt->tapeid);
        free(poffwt->username);
        freekeylist((KEY **)&poffwt->list);
        free(poffwt);
        poffwt = NULL;
      }
      poff = NULL;
      pactive = 0;
      return;
}
