/*-----------------------------------------------------------------------------
 * /home/jim/cvs/JSOC/base/sum/tapearc.c
 *
 * This is a stand alone program that will archive all the SUMS archive pending 
 * storage units to the tape_svc supported drives . 
 * It gets all the archive pending working
 * directories from the Oracle sum_partn_alloc table.
 *
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

extern char *get_effdate(int plusdays);

#define TODAY (atol(get_effdate(0)))
#define TAR_FILE_SZ 500000000	/* try to make a tar file this size */
#define WD_MAX_CALL_CNT 512	/* max wds to put in any one call to tape */
#define ARCH_CHUNK 10		/* this many archive calls to tape_svc */

extern void printkey (KEY *key);
void usage();
void get_cmd(int argc, char *argv[]);
void setup();
void goaway();
void print_list(char *title, PADATA *p);
void print_entry(PADATA *p);
void sighandler(int sig);
KEY *tapearcdo_1(KEY *params);
static void tapearcprog_1(struct svc_req *rqstp, SVCXPRT *transp);

static struct timeval TIMEOUT = { 180, 0 };
static int WRTSTATUS;
uint32_t rinfo;         /* info returned by XXXdo_1() calls */
int call_tape_svc_cnt;
int TAPEARCDO_called;
int curr_group_id;
PADATA *walker;
PADATA *aplist = NULL;/* linked list of padata for archive pending status */
KEY *alist;
CLIENT *current_client, *clnttape, *clntsum;
SVCXPRT *glb_transp;
SVCXPRT *transp;

int count_list(PADATA *p);
int storeunitarch(int docnt);
int soi_errno = NO_ERROR;
char *dbname;
char *username;
char thishost[MAX_STR];
time_t now;
int verbose = 0;
int debugflg = 0;
int queryflg = 0;
int aminflg = 0;
int archive_minimum = 0;
int archive_pending = 0;
int is_connected=0;
int ctrlcnt = 0;
/************************************************************************/

/* Count the number of tapearc processing running.
   Return count or -1 on error
*/
int find_tapearc()
{
  FILE *fplog;
  char acmd[128], line[128], look[64];
  char log[] = "/usr/local/logs/SUM/find_tapearc.log";
  int count = 0;

  sprintf(acmd, "ps -ef | grep tapearc  1> %s 2>&1", log);
  if(system(acmd)) {
    printf("Can't execute %s.\n", acmd);
    return(-1);
  }
  if((fplog=fopen(log, "r")) == NULL) {
    printf("Can't open %s to find tapearc\n", log);
    return(-1);
  }
  sprintf(look, " %s", dbname);
  while(fgets(line, 128, fplog)) {       /* get ps lines */
     if (strstr(line, look) && strstr(line, "tapearc")) count++;
  }
  fclose(fplog);
  return (count);
}

int count_list(PADATA *p) {
   PADATA *walker = p;
   int count=0;
   while (walker) {
      count++;
      walker=walker->next;
   }
   return count;
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
  sprintf(cmd, "echo \"%s\" | Mail -s \"tapearc mail\" jim@sun.stanford.edu", string);
  system(cmd);
  va_end(args);
  return(0);
}

void usage()
{
  printf("This is a stand alone program that will archive all the SUMS\n");
  printf("archive pending storage units to the tape_svc supported drives.\n");
  printf("Usage: tapearc [-v] [-q] [-d] [-n arcmin] database_name\n");
  printf(" where -v = verbose mode\n");
  printf("       -q = query only mode\n");
  printf("       -d = run  in debug mode\n");
  printf("       -n arcmin = don't archive unless # in ap list > arcmin.\n");
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
      case 'n':
        aminflg=1;
        break;
      case 'q':
        queryflg=1;
        break;
      default:
        usage();
        break;
      }
  }
  if(aminflg) {if(argc !=2) usage();}
  else if(argc != 1) usage();
  if (aminflg) {archive_minimum = atoi(argv[0]); dbname = argv[1];}
  else dbname = argv[0];
}

/* Release resources, disconnect from the db and exit with error */
void goaway()
{
  if(is_connected)
  if(DS_DisConnectDB())
    fprintf(stderr, "DS_DisconnectDB() error\n");
  exit(1);
}


void sighandler(int sig) 
{
  char line[80];

  /*printf("\n tapearc received a termination signal\n");
  /*goaway();
  */
  if(ctrlcnt++ == 0) {
    printf("\nInterrupt not allowed during tape write operations...\n");
    printf("Hit ^C again for an option to abort...\n");
  }
  else {
    printf("Do you want to force an abort (make sure you know what you're doing) [y/n] = ");
    if(gets(line) == NULL) { return; }
    if(!strcmp(line, "y")) { goaway(); }
  }
}


/* Inital setup stuff. Called once at start of the program. */
void setup()
{
  char *cptr;
  int n;

  printk_set(printf, printf);
  n = find_tapearc();
  if(n != 1) {
     printf("Only one tapearc %s allowed at a time. I see %d\n", dbname,n);
     send_mail("Only one tapearc %s allowed at a time. I see %d\n", dbname,n);
     exit(1); 
  }
  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  *cptr = (char)NULL;
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);

    /* register for tape_svc to be able to talk to us */
    (void) pmap_unset(TAPEARCPROG, TAPEARCVERS);
    transp = svctcp_create(RPC_ANYSOCK, 0, 0);
    if (transp == NULL) {
            printf("***cannot create tcp service\n");
            exit(1);
    }
    if (!svc_register(transp, TAPEARCPROG, TAPEARCVERS, tapearcprog_1, IPPROTO_TCP)) {
            printf("***unable to register (TAPEARCPROG, TAPEARCVERS, tcp)\n");
            exit(1);
    }
  /* Create client handle used for calling the tape_svc */
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc in tapearc");
    printf("tapearc can't get tape_svc handle on %s\n", thishost);
    exit(1);
  }
  clntsum = clnt_create(thishost, SUMPROG, SUMVERS, "tcp");
  if(!clntsum) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_svc in tapearc");
    printf("***tapearc can't get sum_svc on %s\n", thishost);
    exit(1);
  }


  if (DS_ConnectDB(dbname)) {
     fprintf(stderr,"Error in connecting to database %s\n", dbname);
     goaway();
  }
  is_connected=1;
  call_tape_svc_cnt = 0;
}

/* call write procedure in tape_svc. Returns 1 if error. Counts number
 * of times successfully called and awaiting reply in call_tape_svc_cnt.
 */
int call_tape_svc(int groupid, double bytes, uint64_t index) {
  int status;
  uint32_t retstat;
  char *call_err;

    WRTSTATUS = 0;
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
        call_tape_svc_cnt++;
        printf("RESULT_PEND (!!ASSUMED) group_id=%d bytes=%g 1st_ds_index=%ld. Arc in progress...\n", groupid, bytes, index);
        return(0);
      }
    }
    if(retstat == RESULT_PEND) {
      call_tape_svc_cnt++;
      printf("RESULT_PEND group_id=%d bytes=%g 1st_ds_index=%ld. Arc in progress...\n",
		groupid, bytes, index);
    }
    else {
      if(retstat == NO_TAPE_IN_GROUP) {
        printf("Can't assign tape for group id %d\n", groupid);
        printf("I'm aborting now...\n");
        if(is_connected)
        if(DS_DisConnectDB())
          fprintf(stderr, "DS_DisconnectDB() error\n");
        exit(1);
      }
      else if(retstat == NO_CLNTTCP_CREATE) {
        printf("NO_CLNTTCP_CREATE error on ret from clnt_call(clntape,WRITEDO)\n");
      }
      else
        printf("retstat = %d on ret from clnt_call(clntape,WRITEDO)\n",retstat);
    }
    return(0);
}

int main(int argc, char *argv[])
{ 

  get_cmd(argc, argv);                  /* check the calling sequence */
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  /********** !!!TMP form user jim or production ********************/
  if(strcmp(username, "production")) {
    if(strcmp(username, "jim")) {
      printf("!!NOTE: You must be user production to run tapearc!\n");
      exit(1);
    }
  }
  printf ("Current effective_date is %ld\n", TODAY);
  time (&now); printf ("%s\n",asctime(localtime(&now)));
  printf ("Datasets will be archived if there are more than %d\n",
		archive_minimum);
  setup();

  /* get partn_alloc table - all entries with status DAAP or DASAP */
  aplist = NC_PaRequest_AP();
  archive_pending = count_list(aplist);
  if (verbose || queryflg) {
    printf("\nThere are %d entries in the archive pending list.\n",
         archive_pending);
    print_list ("archive pending list", aplist);
    printf("archive pending will be grouped into %d byte tar files\n", TAR_FILE_SZ);
    if(queryflg) goaway();
  }
  if (archive_pending <= archive_minimum) {
     printf("\nThe number of archive pending entries (%d) is <= %d\n",
        archive_pending, archive_minimum);
     goaway();
  }

  walker = aplist;	/* this list is in descending group_id order */
  storeunitarch(ARCH_CHUNK); /* kick of first chunk of archives */
  if(call_tape_svc_cnt == 0) { /* wait until all finish */
    if(is_connected)
      if(DS_DisConnectDB())
      fprintf(stderr, "DS_DisconnectDB() error\n");
    exit(0);
  }
  svc_run();		/* doesn't return */
}


/* This is the dispatch routine that's called when the client does a
 * clnt_call() to TAPEARCPROG, TAPEARCVERS
*/
static void
tapearcprog_1(rqstp, transp)
	struct svc_req *rqstp;
	SVCXPRT *transp;
{
	union __svcargun {
		Rkey tapearcdo_1_arg;
	} argument;
	char *result;

	bool_t (*xdr_argument)(), (*xdr_result)();
	char *(*local)();
	switch (rqstp->rq_proc) {
	case NULLPROC:
                printf("Called NULLPROC in tapearcprog_1()\n"); /* !!TEMP */
		(void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
		return;
	case TAPEARCDO:
                /*printf("Called tapearcprog_1() TAPEARCDO\n");/* !!TEMP */
                TAPEARCDO_called = 1;		/* !!TEMP */
		xdr_argument = xdr_Rkey;
		xdr_result = xdr_Rkey;;
		local = (char *(*)()) tapearcdo_1;
		break;
	default:
                printf("**tapearcprog_1() dispatch default procedure %ld,ignore\n", rqstp->rq_proc);
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

/* Called when we get a TAPEARCDO msg to tapearc.
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
KEY *tapearcdo_1(KEY *params)
{
  int groupid;
  uint64_t index;

  groupid = getkey_int(params, "group_id");
  index = getkey_uint64(params, "ds_index_0");
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    printf("!!TEMP in tapearcdo_1() call in tapearc. keylist is:\n");
    keyiterate(printkey, params);
  }
  if(WRTSTATUS = getkey_int(params, "STATUS")) {
    printf("**Error return for write of group_id=%d 1st ds_index=%ld\n", 
			groupid, index);
  }
  else {
    printf("Successful write for group_id=%d 1st_ds_index=%ld\n", 
			groupid, index);
  }
  --call_tape_svc_cnt;
  if(storeunitarch(1)) {	 /* send another chunk to tape_svc */
    if(call_tape_svc_cnt == 0) { /* wait until all finish */
      if(is_connected)
        if(DS_DisConnectDB())
        fprintf(stderr, "DS_DisconnectDB() error\n");
      exit(0);
    }
  }
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
    else if((walker->group_id == curr_group_id) && (curr_group_sz < TAR_FILE_SZ) && (wd_max_call_cnt < WD_MAX_CALL_CNT)) {
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
      if(call_tape_svc(curr_group_id, total_bytes, first_index)) {
        fprintf(stderr, "**Error on tape write for group %d\n", curr_group_id);
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
  if(i != 0) {		/* write out anything left over */
    setkey_double(&alist, "total_bytes", total_bytes);
    setkey_int(&alist, "reqcnt", i);
    if(call_tape_svc(curr_group_id, total_bytes, first_index)) {
      fprintf(stderr, "**Error on tape write for group %d\n", curr_group_id);
    }
  }
  return(0);
}
