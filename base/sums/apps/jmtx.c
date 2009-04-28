/*-----------------------------------------------------------------------------
 * /home/production/cvs/JSOC/base/sum/apps/jmtx.c
 *
 * This is a stand alone program that talks to tape_svc to
 * perform mtx like commads with the full knowledge of SUMS.
 *
 * Only these mtx commands are implimented:
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
void print_list(char *title, PADATA *p);
void print_entry(PADATA *p);
void sighandler(int sig);
KEY *jmtxdo_1(KEY *params);
static void jmtxprog_1(struct svc_req *rqstp, SVCXPRT *transp);

static struct timeval TIMEOUT = { 3, 0 };
uint32_t rinfo;         /* info returned by XXXdo_1() calls */
CLIENT *current_client, *clnttape, *clntsum;
SVCXPRT *glb_transp;
SVCXPRT *transp;

int TAPEARCDO_called;
KEY *alist;
static int WRTSTATUS;
PADATA *walker;
int curr_group_id;

int count_list(PADATA *p);
int storeunitarch(int docnt);
int soi_errno = NO_ERROR;
char *dbname;
char *username;
char thishost[MAX_STR];
char devname[64];
char mode[64];
int drivenum;
int slotnum;
time_t now;
int verbose = 0;
int debugflg = 0;
int queryflg = 0;
int test60d = 0;
int aminflg = 0;
int archive_minimum = 0;
int archive_pending = 0;
int is_connected=0;
int ctrlcnt = 0;

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


void print_entry(PADATA *p) {
   if (!p) return;
   printf ("%d\t%15.6e\t%s\n", p->group_id, p->bytes, p->wd);
}

void print_list(char *title, PADATA *p) {
   PADATA *walker = p;
   if (title) printf("\n%s\n", title);
   while (walker) {
      print_entry (walker);
      walker = walker->next;
   }
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
  printf("This is a stand alone program that will execute mtx load, unload and \n");
  printf("status commands via SUMS, so that it knows where all the tapes are.\n");
  printf("Usage: jmtx -f /dev/t950 status|load|unload [slot#] [drive#]\n");
  printf("NOTE: you must be user production to run.\n");
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
  slotnum = atoi(argv[2]);
  drivenum = atoi(argv[3]);
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
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc in jmtx");
    printf("jmtx can't get tape_svc handle on %s\n", thishost);
    exit(1);
  }
}

/* call write procedure in tape_svc. Returns 1 if error. Counts number
 * of times successfully called and awaiting reply in call_tape_svc_cnt.
 */
int call_tape_svc(int groupid, double bytes, uint64_t index) {
  int status;
  uint32_t retstat;
  char *call_err;

    //WRTSTATUS = 0;
    StartTimer(0); //!!TEMP for debug. time call is case timeout 
    status = clnt_call(clnttape, WRITEDO, (xdrproc_t)xdr_Rkey, (char *)alist,
                    (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
/*********************!!!TEMP**************
    status = clnt_call(clnttape, TAPETESTDO, (xdrproc_t)xdr_Rkey, (char *)alist,
                    (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
********************/
    if(status != 0) {
      clnt_perrno(status);         /* outputs to stderr */
      printf("***Error on clnt_call() to tape_svc WRITEDO procedure\n");
      /* !!!TEMP */
      /*printf("***Error on clnt_call() to tape_svc TAPETESTDO procedure\n");*/
      call_err = clnt_sperror(clnttape, "Err");
      printf("%s\n", call_err);
      if(status != RPC_TIMEDOUT) {  /* allow timeout? */
        printf("I'm aborting now...\n");
        if(is_connected)
        if(DS_DisConnectDB())
          fprintf(stderr, "DS_DisconnectDB() error\n");
        exit(1);
      }
      else {			/* so what do we do with a timeout? */
        /* !!!TEMP try this for now */
        ftmp = StopTimer(0);
        printf("Measured timeout was %f sec\n", ftmp);
        //call_tape_svc_cnt++;
        printf("RESULT_PEND (!!ASSUMED) group_id=%d bytes=%g 1st_ds_index=%lu. Arc in progress...\n", groupid, bytes, index);
        return(0);
      }
    }
    if(retstat == RESULT_PEND) {
      //call_tape_svc_cnt++;
      printf("RESULT_PEND group_id=%d bytes=%g 1st_ds_index=%lu. Arc in progress...\n",
		groupid, bytes, index);
    }
    else {
      if(retstat == NO_TAPE_IN_GROUP) {
        printf("Can't assign tape for group id %d\n", groupid);
        printf("Check tape_svc log for any error messages\n");
        printf("I'm aborting now...\n");
        if(is_connected)
        if(DS_DisConnectDB())
          fprintf(stderr, "DS_DisconnectDB() error\n");
        exit(1);
      }
      else if(retstat == NO_CLNTTCP_CREATE) {
        printf("NO_CLNTTCP_CREATE error on ret from clnt_call(clntape,WRITEDO)\n");
        printf("Check tape_svc log for any error messages\n");
        printf("I'm aborting now...\n");
        if(is_connected)
        if(DS_DisConnectDB())
          fprintf(stderr, "DS_DisconnectDB() error\n");
        exit(1);
      }
      else
        printf("retstat = %d on ret from clnt_call(clntape,WRITEDO)\n",retstat);
    }
    return(0);
}

int main(int argc, char *argv[])
{ 
  int status;
  uint32_t retstat;
  char *call_err;

  get_cmd(argc, argv);                  /* check the calling sequence */
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
    printf("!!NOTE: You must be user production to run tapearc!\n");
    exit(1);
  }
  printf ("Current effective_date is %ld\n", TODAY);
  time (&now); printf ("%s\n",asctime(localtime(&now)));
  setup();

  alist = newkeylist();
  setkey_str(&alist, "mode", mode);
  if(strcmp(mode, "status")) {		//not status cmd so send other params
    setkey_int(&alist, "slotnum", slotnum);
    setkey_int(&alist, "drivenum", drivenum);
  }
  setkey_int(&alist, "DEBUGFLG", 1);
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
  }
  if(retstat == RESULT_PEND) {
      printf("Waiting for results from tape_svc...\n");
  }
  else {
      if(retstat != 0) {
        printf("Error in return status = %s\n", retstat);
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

/* Called when we get a JMTXDO msg to tapearc.
   * !!!!! TBD !!!!!!!!!!!!!!!!!!!
 * When drive[0,1]_svc is done with rd or wrt calls tape_svc TAPERESPWRITEDO
 * which will then forward on the msg to here, i.e. tapearc TAPEARCDO.
 * The keylist here looks like:
 * dnum:   KEYTYP_INT      0
 * wd_0:   KEYTYP_STRING   /SUM5/D1523
 * effective_date_0:       KEYTYP_STRING   200504281238
 * sumid_0:        KEYTYP_UINT64    840
 * bytes_0:        KEYTYP_DOUBLE    1.2000000000000000e+08
 * status_0:       KEYTYP_INT      4
 * archsub_0:      KEYTYP_INT      128
 * group_id_0:     KEYTYP_INT      99
 * safe_id_0:      KEYTYP_INT      0
 * ds_index_0:     KEYTYP_UINT64    1523
 * username_0:     KEYTYP_STRING   jim
 * DEBUGFLG:       KEYTYP_INT      1
 * wd_1:   KEYTYP_STRING   /SUM1/D464
 * effective_date_1:       KEYTYP_STRING   200504281238
 * sumid_1:        KEYTYP_UINT64    460
 * bytes_1:        KEYTYP_DOUBLE    1.2000000000000000e+08
 * status_1:       KEYTYP_INT      4
 * archsub_1:      KEYTYP_INT      128
 * group_id_1:     KEYTYP_INT      99
 * safe_id_1:      KEYTYP_INT      0
 * ds_index_1:     KEYTYP_UINT64    464
 * username_1:     KEYTYP_STRING   jim
 * wd_2:   KEYTYP_STRING   /SUM1/D460
 * effective_date_2:       KEYTYP_STRING   200504281238
 * sumid_2:        KEYTYP_UINT64    458
 * bytes_2:        KEYTYP_DOUBLE    1.2000000000000000e+08
 * status_2:       KEYTYP_INT      4
 * archsub_2:      KEYTYP_INT      128
 * group_id_2:     KEYTYP_INT      99
 * safe_id_2:      KEYTYP_INT      0
 * ds_index_2:     KEYTYP_UINT64    460
 * username_2:     KEYTYP_STRING   jim
 * reqcnt: KEYTYP_INT      3
 * OP:     KEYTYP_STRING   wt
 * current_client: KEYTYP_FILEP    6917529027641818912
 * procnum:        KEYTYP_UINT32    1
 * nxtwrtfn:       KEYTYP_INT      0
 * group_id:       KEYTYP_INT      99
 * STATUS: KEYTYP_INT      0
 * availblocks:    KEYTYP_UINT64   48828000
*/
KEY *jmtxdo_1(KEY *params)
{
  int groupid;
  uint64_t index;

  //StartTimer(6); //!!TEMP for debug. time for jmtxdo_1()
  groupid = getkey_int(params, "group_id");
  index = getkey_uint64(params, "ds_index_0");
  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    printf("!!TEMP in jmtxdo_1() call in tapearc. keylist is:\n");
    keyiterate(printkey, params);
  }
  }
  if(WRTSTATUS = getkey_int(params, "STATUS")) {
    printf("**Error return for write of group_id=%d 1st ds_index=%lu\n",
			groupid, index);
  }
  else {
  printf("Successful write for group_id=%d 1st_ds_index=%lu\n",
			groupid, index);
  }
  //--call_tape_svc_cnt;
  //printf("call_tape_svc_cnt = %d\n", call_tape_svc_cnt); //!!TEMP

/****************************************************************************
  if(storeunitarch(1)) {	 // send another chunk to tape_svc
    if(call_tape_svc_cnt == 0) { // wait until all finish 
      if(is_connected)
        if(DS_DisConnectDB())
        fprintf(stderr, "DS_DisconnectDB() error\n");
      exit(0);
    }
  }
****************************************************************************/
  //ftmp = StopTimer(6);
  //printf("Time 6 for tapearcdo_1() when a tape wt is done =  %f sec\n", ftmp);
  return((KEY *)1);
}

/* Get a bunch of wds from the same group until a new group is hit 
 * or the wds total TAR_FILE_SZ of storage. Will get docnt chunks.
 * Send this keylist of storage units to be archived to tape_svc as a
 * WRITEDO request. Uses the global variables in tapearc.c.
 * Return 0 if more SU to do. Return 1 when all SU have been done.
*/
int storeunitarch(int docnt)
{
  int i, chunkcnt, wd_max_call_cnt;
  uint32_t sback;
  uint64_t first_index;
  char name[128];
  enum clnt_stat status;
  double curr_group_sz, total_bytes;

  if(!walker) return(1);
  curr_group_id = walker->group_id;
  first_index = walker->ds_index;
  curr_group_sz = 0.0;
  i = 0;
  chunkcnt = 0;
  wd_max_call_cnt = 0;
  alist = newkeylist();
  while(walker) {
    /* ck if DAAEDDP so don't write to tape but make del pend */
    if(walker->archsub == DAAEDDP) {	/* don't write to tape */
      printf("Don't archive temp storage unit at %s\n", walker->wd);
      if(NC_PaUpdate(walker->wd, walker->sumid, walker->bytes, walker->status,
	walker->archsub, walker->effective_date, 0, 0, walker->ds_index, 
	0, 1)) 
      {				/* del from arch pend list */
        printf("Error on NC_PaUpdate of %s to del from arch pend\n",walker->wd);
      }
      /* now put on del pend list */
      walker->status = DADP;
      if(NC_PaUpdate(walker->wd, walker->sumid, walker->bytes, walker->status,
	walker->archsub, walker->effective_date, walker->group_id, 0, 
	walker->ds_index, 1, 1)) 
      {	
        printf("Error on NC_PaUpdate of %s to add to del pend\n",walker->wd);
      }
      walker=walker->next;
      continue;
    }

    /* get a bunch of wds from the same group until a new group is hit */
    /* or the wds total TAR_FILE_SZ of storage */
    curr_group_sz += walker->bytes;
    if(curr_group_sz >= TAR_FILE_SZ) {		/* already big enough */
      total_bytes = curr_group_sz;
      sprintf(name, "wd_%d", i);
      setkey_str(&alist, name, walker->wd);
      sprintf(name, "effective_date_%d", i);
      setkey_str(&alist, name, walker->effective_date);
      sprintf(name, "sumid_%d", i);
      setkey_uint64(&alist, name, walker->sumid);
      sprintf(name, "bytes_%d", i);
      setkey_double(&alist, name, walker->bytes);
      sprintf(name, "status_%d", i);
      setkey_int(&alist, name, walker->status);
      sprintf(name, "archsub_%d", i);
      setkey_int(&alist, name, walker->archsub);
      sprintf(name, "group_id_%d", i);
      setkey_int(&alist, name, walker->group_id);
      sprintf(name, "safe_id_%d", i);
      setkey_int(&alist, name, walker->safe_id);
      sprintf(name, "ds_index_%d", i);
      setkey_uint64(&alist, name, walker->ds_index);
      sprintf(name, "username_%d", i);
      setkey_str(&alist, name, username);
      setkey_int(&alist, "DEBUGFLG", debugflg);
      chunkcnt++;
      i++;
      walker=walker->next;
    }
    else if((walker->group_id == curr_group_id) && (curr_group_sz < TAR_FILE_SZ) && (wd_max_call_cnt < MAXSUMREQCNT)) {
      total_bytes = curr_group_sz;
      wd_max_call_cnt++;
      sprintf(name, "wd_%d", i);
      setkey_str(&alist, name, walker->wd);
      sprintf(name, "effective_date_%d", i);
      setkey_str(&alist, name, walker->effective_date);
      sprintf(name, "sumid_%d", i);
      setkey_uint64(&alist, name, walker->sumid);
      sprintf(name, "bytes_%d", i);
      setkey_double(&alist, name, walker->bytes);
      sprintf(name, "status_%d", i);
      setkey_int(&alist, name, walker->status);
      sprintf(name, "archsub_%d", i);
      setkey_int(&alist, name, walker->archsub);
      sprintf(name, "group_id_%d", i);
      setkey_int(&alist, name, walker->group_id);
      sprintf(name, "safe_id_%d", i);
      setkey_int(&alist, name, walker->safe_id);
      sprintf(name, "ds_index_%d", i);
      setkey_uint64(&alist, name, walker->ds_index);
      sprintf(name, "username_%d", i);
      setkey_str(&alist, name, username);
      setkey_int(&alist, "DEBUGFLG", debugflg);
      i++;
      walker=walker->next;
      continue;
    }
    setkey_double(&alist, "total_bytes", total_bytes);
    setkey_int(&alist, "reqcnt", i);
    if(i != 0) {	/* make sure didn't hit new group at beginning */
      //only do if big enough of hit max # of files 
      if((curr_group_sz >= TAR_FILE_SZ) || (wd_max_call_cnt >= MAXSUMREQCNT)) {
        if(call_tape_svc(curr_group_id, total_bytes, first_index)) {
          fprintf(stderr, "**Error on tape write for group %d\n", curr_group_id);
        }
      }
      else {
        printf("Abandon partial block for group=%d index=%lu files=%d bytes=%g\n", curr_group_id, first_index, wd_max_call_cnt, total_bytes);
      }
    }
    freekeylist(&alist);
    if(chunkcnt == docnt) {
      return(0);
    }
    if(walker) {
      curr_group_id = walker->group_id;
      first_index = walker->ds_index;
    }
    curr_group_sz = 0.0;
    wd_max_call_cnt = 0;
    i = 0;
    alist = newkeylist();
  }
  if(i != 0) {		// write out anything left over 
    if(curr_group_sz >= TAR_FILE_SZ) {	//only do if big enough
      setkey_double(&alist, "total_bytes", total_bytes);
      setkey_int(&alist, "reqcnt", i);
      if(call_tape_svc(curr_group_id, total_bytes, first_index)) {
        fprintf(stderr, "**Error on tape write for group %d\n", curr_group_id);
      }
    }
    else {
      printf("Abandon left over for group=%d index=%lu bytes=%g\n", 
			curr_group_id, first_index, total_bytes);
    }
  }
  return(0);
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
