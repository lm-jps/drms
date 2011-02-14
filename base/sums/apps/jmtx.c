/**
\defgroup jmtx jmtx - Do an mtx command under SUMS
@ingroup jmtx

\brief This is a stand alone program that talks to tape_svc to execute
mtx load, unload and status commands via SUMS, so that SUMS remains
aware where all the tapes are.

\par Synopsis:

\code
jmtx -f /dev/t950 status|load|unload [slot#] [drive#]
\endcode

\par NOTES:
<pre>
You must be user production to run.
The "-f /dev/t950" is vestigial from the mtx command. The jmtx will 
talk to its configured tape_svc which determines the device. It is left
in to be symetric with the mtx command and to show the users intention.\n
jmtx will still work with drives that have been taken offline via e.g.\n
driveonoff off 10\n
If you're going to work with some drive, it is usually best to take it 
offline first, so that you and SUMS don't interleave your use of it.

The jmtx commands show up in the tape_svc_XX.log files like so:\n
*Rb:cmd: mtx -f /dev/t950 load 514 11 1> /tmp/mtx/mtx_robot_104.log 2>&1

</pre>
*/

/*-----------------------------------------------------------------------------
 * /home/production/cvs/JSOC/base/sum/apps/jmtx.c
 *
 * This is a stand alone program that talks to tape_svc to
 * perform mtx like commads with the full knowledge of SUMS.
 *
 * Only these mtx commands are implemented:
 *   status
 *   load
 *   unload
*/

#include <SUM.h>
#include <soi_key.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <printk.h>

char *get_eff_date(int plusdays);

#define TODAY (atol(get_eff_date(0)))

extern void printkey (KEY *key);
void usage();
void get_cmd(int argc, char *argv[]);
void setup();
void goaway();
void sighandler(int sig);
KEY *jmtxdo_1(KEY *params);
static void jmtxprog_1(struct svc_req *rqstp, SVCXPRT *transp);

static struct timeval TIMEOUT = { 30, 0 };
uint32_t rinfo;         /* info returned by XXXdo_1() calls */
CLIENT *current_client, *clnttape, *clntsum;
SVCXPRT *glb_transp;
SVCXPRT *transp;

KEY *alist;
static int WRTSTATUS;

int soi_errno = NO_ERROR;
char *username;
char thishost[MAX_STR];
char devname[64];
char mode[64];
int drivenum;
int slotnum, slot1, slot2;
time_t now;
int verbose = 0;
int debugflg = 0;

static struct timeval first[6], second[6];
float ftmp;

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


int send_mail(char *fmt, ...)
{
  va_list args;
  char string[1024], cmd[1024];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  /* !!TBD send to admin alias instead of jim */
  sprintf(cmd, "echo \"%s\" | Mail -s \"jmtx mail\" jim@sun.stanford.edu", string);
  system(cmd);
  va_end(args);
  return(0);
}

void usage()
{
  printf("This is a stand alone program that will execute mtx load, unload, transfer and \n");
  printf("status commands via SUMS, so that it knows where all the tapes are.\n");
  printf("Usage: jmtx -f /dev/t950 status|load|unload|transfer [slot#] [drive#]\n");
  printf("NOTE: you must be user production to run.\n");
  printf("NOTE: The -f arg is for symmetry with mtx only. You will connect\n");
  printf("    to the sum_svc that you are configured to and get its drives.\n");
  exit(1);
}

/* Get and validate the calling sequence. */
void get_cmd(int argc, char *argv[])
{
  int c;

  while(--argc > 0 && (*++argv)[0] == '-') {
    while((c = *++argv[0]))
      switch(c) {
      case 'd':
        debugflg=1;
        break;
      case 'v':
        verbose=1;
        break;
      case 'f':
        ++argv[0];
        if(*++argv[0] != NULL) {        /* get dev name */
          strcpy(devname, argv[0]);
        }
        while(*++argv[0] != NULL);
        --argv[0];
        break;
      default:
        usage();
        break;
      }
  }
  if(argc == 2) {
    strcpy(mode, argv[1]);
    if(strcmp(mode, "status")) {
      usage();
    }
    return;
  }
  else if(argc != 4) {
    usage();
  }
  strcpy(mode, argv[1]);
  if(strcmp(mode, "load") && strcmp(mode, "unload") && strcmp(mode, "transfer")) {
    usage();
  }
  if(!strcmp(mode, "transfer")) {
    slot1 = atoi(argv[2]);
    slot2 = atoi(argv[3]);
  } else {
    slotnum = atoi(argv[2]);
    drivenum = atoi(argv[3]);
  }
}

/* Release resources, disconnect from the db and exit with error */
void goaway()
{
  /********************************************
  if(is_connected)
  if(DS_DisConnectDB())
    fprintf(stderr, "DS_DisconnectDB() error\n");
  *******************************************/
  (void) pmap_unset(JMTXPROG, JMTXVERS);
  exit(1);
}


void sighandler(int sig) 
{
  char line[80];

  printf("\n jmtx received a termination signal\n");
  goaway();
}


/* Inital setup stuff. Called once at start of the program. */
void setup()
{
  char *cptr;
  int n;
  char pgport[32];

  printk_set(printf, printf);
  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;
  //if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);

    /* register for tape_svc to be able to talk to us */
    (void) pmap_unset(JMTXPROG, JMTXVERS);
    transp = svctcp_create(RPC_ANYSOCK, 0, 0);
    if (transp == NULL) {
            printf("***cannot create tcp service\n");
            exit(1);
    }
    if (!svc_register(transp, JMTXPROG, JMTXVERS, jmtxprog_1, IPPROTO_TCP)) {
            printf("***unable to register (TAPEARCPROG, TAPEARCVERS, tcp)\n");
            exit(1);
    }
  /* Create client handle used for calling the tape_svc */
  //if running on j1, then the tape_svc is on d02, else the localhost
  if(strcmp(thishost, SUMSVCHOST))
    clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  else
    clnttape = clnt_create(TAPEHOST, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc in jmtx");
    printf("jmtx can't get tape_svc handle on %s\n", thishost);
    exit(1);
  }
}


int main(int argc, char *argv[])
{ 
  int status;
  uint32_t retstat = 0;
  char *call_err;

  get_cmd(argc, argv);                  /* check the calling sequence */
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
    printf("!!NOTE: You must be user production to run tapearc!\n");
    exit(1);
  }
  //printf ("Current effective_date is %ld\n", TODAY);
  //time (&now); printf ("%s\n",asctime(localtime(&now)));
  setup();

  alist = newkeylist();
  setkey_str(&alist, "mode", mode);
  if(strcmp(mode, "status")) {		//not status cmd so send other params
    if(strcmp(mode, "transfer")) {      //not transfer cmd
      setkey_int(&alist, "slotnum", slotnum);
      setkey_int(&alist, "drivenum", drivenum);
    }
    else {
      setkey_int(&alist, "slot1", slot1);
      setkey_int(&alist, "slot2", slot2);
    }
  }
  //setkey_int(&alist, "DEBUGFLG", 1);	//!!TEMP
  status = clnt_call(clnttape, JMTXTAPEDO, (xdrproc_t)xdr_Rkey, (char *)alist,
                    (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
  if(status != 0) {
    if(status != RPC_TIMEDOUT) {  /* allow timeout? */
      clnt_perrno(status);         /* outputs to stderr */
      printf("\n***Error on clnt_call() to tape_svc JMTXTAPEDO procedure\n");
      call_err = clnt_sperror(clnttape, "Err");
      printf("%s\n", call_err);
      goaway();
    }
    else {
      printf("\nThe call to tape_svc has timed out. Still will accept a response...\n");
    }
  }
  if(retstat == RESULT_PEND) {
      printf("Waiting for results from tape_svc...\n");
  }
  else {
      if(retstat != 0) {
       printf("Error in return status = %d\n", retstat);
       goaway();
      }
  }
  svc_run();		/* doesn't return. get answer */
}


/* This is the dispatch routine that's called when the client does a
 * clnt_call() to JMTXPROG, JMTXVERS
*/
static void
jmtxprog_1(rqstp, transp)
	struct svc_req *rqstp;
	SVCXPRT *transp;
{
	union __svcargun {
		Rkey jmtxdo_1_arg;
	} argument;
	char *result;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();
	switch (rqstp->rq_proc) {
	case NULLPROC:
                printf("Called NULLPROC in jmtxprog_1()\n"); /* !!TEMP */
		(void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
		return;
	case JMTXDO:
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;;
		local = (char *(*)()) jmtxdo_1;
		break;
	default:
                printf("**jmtxprog_1() dispatch default procedure %ld,ignore\n", rqstp->rq_proc);
		(void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
		svcerr_noproc(transp);
		return;
	}
	bzero((char *)&argument, sizeof(argument));
	if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
                printf("***Error on svc_getargs()\n");
		svcerr_decode(transp);
                svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
                return;

	}
        /* send immediate ack back */
        if(!svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL)) {
          svcerr_systemerr(transp);
          return;
        }
        glb_transp = transp;		     /* needed by function */
        result = (*local)(&argument, rqstp); /* call the function */
					     /* sets current_client & rinfo */

      if (!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
	printf("**unable to free arguments\n");
	/*exit(1);*/
      }
      return;
}

/* Called when we get a JMTXDO msg to jmtx (from tape_svc).
 * mode:   KEYTYP_STRING   load
 * slotnum:        KEYTYP_INT      22
 * drivenum:       KEYTYP_INT      0
 * DEBUGFLG:       KEYTYP_INT      1
 * current_client: KEYTYP_FILEP    5476432
 * procnum:        KEYTYP_UINT32   1
*/
KEY *jmtxdo_1(KEY *params)
{
  char *errstr, *sumserv;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      printf("!!TEMP in jmtxdo_1() call in tape_svc. keylist is:\n");
      keyiterate(printkey, params);
    }
  }
  //drivenum = getkey_int(params, "drivenum");
  if(WRTSTATUS = getkey_int(params, "STATUS")) {
    printf("**Error return for jmtx\n");
    sumserv = (char *)getenv("SUMSERVER");
    if(findkey(params, "ERRSTR")) {
      errstr = getkey_str(params, "ERRSTR");
      printf("%s", errstr);
    }
    printf("\nSee %s:/usr/local/logs/SUM/tape_svc_XX.log\n", sumserv);
    (void) pmap_unset(JMTXPROG, JMTXVERS);
    exit(1);
  }
  else {
    if(findkey(params, "MSG")) {
      printf("%s\n", getkey_str(params, "MSG"));
    }
    printf("Success\n");
  }
  (void) pmap_unset(JMTXPROG, JMTXVERS);
  exit(0);
  //return((KEY *)1);
}


/* Return a date as a malloc'd string in yyyymmddhhmm format that is plusdays
 * from now.
 */
char *get_eff_date(int plusdays)
{
   struct timeval tvalr;
   struct tm *t_ptr;
   time_t newtime;
   char *timestr;
  	 
   if(gettimeofday(&tvalr, NULL) == -1) {
      return("200712121212");     /* give a phoney return */
   }
   t_ptr = localtime(&tvalr.tv_sec);
   t_ptr->tm_mday = t_ptr->tm_mday + plusdays;
   newtime = mktime(t_ptr);
   t_ptr = localtime(&newtime);
   timestr = (char *)malloc(32);
   sprintf(timestr, "%04d%02d%02d%02d%02d",
	   t_ptr->tm_year+1900, t_ptr->tm_mon+1, t_ptr->tm_mday,
	   t_ptr->tm_hour, t_ptr->tm_min);
   return(timestr);
}
