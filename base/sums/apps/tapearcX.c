/*-----------------------------------------------------------------------------
 * /home/jim/cvs/JSOC/base/sum/tapearcX.c
 *
 * New tapearc that's called by tape_do_archive.pl with a manifest file
 * of tape files to write.
 *
 * This is a stand alone program that will archive all the SUMS archive pending 
 * storage units, from a manifest file, to the tape_svc supported drives . 
 *
*/

#include <SUM.h>
#include <soi_key.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <printk.h>

char *get_eff_date(int plusdays);

#define TODAY (atol(get_eff_date(0)))
#define TAR_FILE_SZ 500000000	/* try to make a tar file this size bytes */
#define ARCH_CHUNK 30		/* this many archive calls to tape_svc */
#define NOTAPEARC "/usr/local/logs/soc/NOTAPEARC" /* touched by t50view */
#define MYNAME "tapearcX"
#define MANIFESTMVDIR "/usr/local/logs/manifest/DONE"

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

//static struct timeval TIMEOUT = { 10, 0 };
static struct timeval TIMEOUT = { 120, 0 };
static int WRTSTATUS;
uint32_t rinfo;         /* info returned by XXXdo_1() calls */
int TAPEARCDO_called;
int curr_group_id;
int filegroup;
int thispid;
PADATA *walker;
PADATA *aplist = NULL;/* linked list of padata for archive pending status */
KEY *alist;
FILE *fpman;
CLIENT *current_client, *clnttape;
SVCXPRT *glb_transp;
SVCXPRT *transp;

int count_list(PADATA *p);
int storeunitarch(int docnt);
int soi_errno = NO_ERROR;
char *dbname = "jsoc_sums";
char *username;
char *manifest;
char *mfilename;
char manifestmv[MAX_STR];
char thishost[MAX_STR];
char mline[256];
time_t now;
int verbose = 0;
int debugflg = 0;
int test60d = 0;
int archive_minimum = 0;
int archive_pending = 0;
int is_connected=0;
int ctrlcnt = 0;
int found = 0;
int do_tape_called = 0;
int mstatus;

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
     if (strstr(line, look) && strstr(line, "tapearc")) {
       if(!strstr(line, "sh ")) count++;
     }
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
  sprintf(cmd, "echo \"%s\" | Mail -s \"tapearc mail\" jsoc_sysadm@sun.stanford.edu", string);
  system(cmd);
  va_end(args);
  return(0);
}

void usage()
{
  printf("This is a stand alone program that will archive all the SUMS\n");
  printf("archive pending storage units, from the given manifest file,\n");
  printf("to the tape_svc supported drives.\n");
  printf("The maifest file is produced by tape_do_archive.pl\n");
  printf("Usage: tapearcX [-d] [-h] manifiest_file\n");
  printf(" where -d = pass a debug flag onto tape_svc\n");
  printf("       -h = help. print usage\n");
  printf("Sample call:\n");
  printf("       tapearcX /usr/local/logs/manifest/manifest.group3\n");
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
      case 'h':
        usage();
        break;
      default:
        usage();
        break;
      }
  }
  if(argc != 1) usage();
  manifest = argv[0];
  if(mfilename = rindex(manifest, '/'))  mfilename++;
  else mfilename = manifest;
  sprintf(manifestmv, "%s/%s.%ld", MANIFESTMVDIR, mfilename, TODAY);
  thispid = getpid();
}

/* Release resources, disconnect from the db and exit with error */
/* Must move off the manifest file as some may already be archived.
 * You just create a new manifest file for this group again in order 
 * to continue.
*/
void goaway()
{
  if(is_connected)
  if(DS_DisConnectDB())
    fprintf(stderr, "DS_DisconnectDB() error\n");
  if(rename(manifest, manifestmv)) {   //must move off old manifest
    fprintf(stderr, "Can't mv %s to %s\n", manifest, manifestmv);
  }
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
  char pgport[32];

  printk_set(printf, printf);
  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;

  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);

    /* register for tape_svc to be able to talk to us */
    /* Use the group# from the manifest file as the rpc vers number */
    (void) pmap_unset(TAPEARCPROG, (u_long)filegroup);
    transp = svctcp_create(RPC_ANYSOCK, 0, 0);
    if (transp == NULL) {
            printf("***cannot create tcp service\n");
            exit(1);
    }
    if (!svc_register(transp, TAPEARCPROG, (u_long)filegroup, tapearcprog_1, IPPROTO_TCP)) {
        printf("***unable to register (TAPEARCPROG, (u_long)filegroup, tcp)\n");
        exit(1);
    }
    printf("Registered TAPEARCPROG with vers# %d\n", filegroup); 
  /* Create client handle used for calling the tape_svc */
  clnttape = clnt_create(thishost, TAPEPROG, TAPEVERS, "tcp");
  if(!clnttape) {       /* server not there */
    clnt_pcreateerror("Can't get client handle to tape_svc in tapearc");
    printf("tapearc can't get tape_svc handle on %s\n", thishost);
    exit(1);
  }

  if (DS_ConnectDB(dbname)) {
     fprintf(stderr,"Error in connecting to database %s\n", dbname);
     goaway();
  }
  is_connected=1;
}

/* call write procedure in tape_svc. Returns 1 if error.
 */
int call_tape_svc() {
  int status;
  uint32_t retstat;
  char *call_err;

    WRTSTATUS = 0;
    StartTimer(0); //!!TEMP for debug. time call is case timeout 
    setkey_int(&alist, "tapearcvers", filegroup);
    setkey_int(&alist, "tapearcXpid", thispid);
    status = clnt_call(clnttape, WRITEDO, (xdrproc_t)xdr_Rkey, (char *)alist,
                    (xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);
    if(status != 0) {
      clnt_perrno(status);         /* outputs to stderr */
      printf("***Error on clnt_call() to tape_svc WRITEDO procedure\n");
      call_err = clnt_sperror(clnttape, "Err");
      printf("%s\n", call_err);
      if(status != RPC_TIMEDOUT) {  /* allow timeout? */
        printf("I'm aborting now...\n");
        goaway();
      }
      else {			/* so what do we do with a timeout? */
        /* !!!TEMP try this for now */
        ftmp = StopTimer(0);
        printf("Measured timeout was %f sec\n", ftmp);
        printf("RESULT_PEND (!!ASSUMED). Arc in progress...\n");
        return(0);
      }
    }
    if(retstat == RESULT_PEND) {
      printf("RESULT_PEND. Arc in progress...\n");
    }
    else {
      if(retstat == NO_TAPE_IN_GROUP) {
        printf("Can't assign tape for group\n");
        printf("Check tape_svc log for any error messages\n");
        printf("I'm aborting now...\n");
        goaway();
      }
      else if(retstat == NO_CLNTTCP_CREATE) {
        printf("NO_CLNTTCP_CREATE error on ret from clnt_call(clntape,WRITEDO)\n");
        printf("Check tape_svc log for any error messages\n");
        printf("I'm aborting now...\n");
        goaway();
      }
      else if(retstat == SUM_TAPE_SVC_OFF) {
        printf("SUM_TAPE_SVC_OFF error on ret from clnt_call(clntape,WRITEDO)\n");
        printf("The tape_svc has been set off line. I'm aborting\n");
        goaway();
      }
      else
        printf("retstat = %d on ret from clnt_call(clntape,WRITEDO)\n",retstat);
    }
    return(0);
}

/* PADATA *aplist has all the arch pend sudirs that go into a tape file.
 * Call tape_svc with the keylist of this info.
*/
int do_tape_file() 
{
  struct stat stbuf;
  int i = 0;
  char name[128];
  double total_bytes = 0.0;

  do_tape_called = 1;		//at least one call to tape_svc
  walker = aplist;
  if(!walker) return(1);
  alist = newkeylist();
    while(walker) {
      if(stat(walker->wd, &stbuf)) {            //don't archive bad dir
        send_mail("%s: Don't archive bad dir: %s \n", MYNAME, walker->wd);
        printf("%s: Don't archive bad dir: %s \n", MYNAME, walker->wd);
      }
      else {
      if(walker->group_id < 0) {
        send_mail("%s: Don't archive neg group#: %d \n  ds_index=%u\n", MYNAME, walker->group_id, walker->ds_index);
        printf("%s: Don't archive neg group#: %d \n", MYNAME, walker->group_id);
      }
      else {
        sprintf(name, "wd_%d", i);
        setkey_str(&alist, name, walker->wd);
        sprintf(name, "effective_date_%d", i);
        setkey_str(&alist, name, walker->effective_date);
        sprintf(name, "sumid_%d", i);
        setkey_uint64(&alist, name, walker->sumid);
        sprintf(name, "bytes_%d", i);
        setkey_double(&alist, name, walker->bytes);
        total_bytes += walker->bytes;
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
        i++;
      }
      }
      walker=walker->next;
    }
    setkey_double(&alist, "total_bytes", total_bytes);
    setkey_int(&alist, "DEBUGFLG", debugflg);
    setkey_int(&alist, "reqcnt", i);
    call_tape_svc();
}

/* Read the input manifest and extract another tape file from it and
 * put in info in PADATA *aplist.
 * First call must have found=0 and aplist = NULL.
 * Return 1 if another tape file was found, else 0 (can be blank file in
 * middle of manifest). Return -1 when all the lines of the manifest 
 * have been read.
*/
int file_from_manifest()
{
  char *token;
  int group;
  int l_status = DAAP;
  int l_archsub = DAADP;
  char dsname[96], sudir[32];
  char eff_date[32];
  uint64_t sumid, sunum;
  double bytes;
  int tmp;

  while(fgets(mline, 256, fpman)) {       //get manifest lines
    if(!strncmp(mline, "\n", 1)) { continue; }
    if(strstr(mline, "#File ")) {  //next tape file info (maybe leading sp)
      //printf("%s\n", mline);
      printf("Found next file: %s\n", mline);
      print_list ("archive pending list for prev file", aplist);
      if(found) do_tape_file();		//send file to tape_svc
      fgets(mline, 256, fpman);		//skip next comment line
      aplist = NULL;
      if(found) {
        found = 0;
        return(1);
      }
      return(0);
    }
    found = 1;
    token = (char *)strtok(mline, " ");
    group = (int)atoi(token);
    token = (char *)strtok(NULL, " ");
    strcpy(dsname, token);
    token = (char *)strtok(NULL, " ");
    sunum = (uint64_t)strtol(token, NULL, 0);
    token = (char *)strtok(NULL, " ");
    strcpy(sudir, token);
    token = (char *)strtok(NULL, " ");
    bytes = (double)strtod(token, NULL);
    token = (char *)strtok(NULL, " ");
    sumid = (uint64_t)strtol(token, NULL, 0);
    token = (char *)strtok(NULL, " ");
    strcpy(eff_date, token);
    tmp = strlen(eff_date);
    if (eff_date[tmp-1] == '\n') eff_date[tmp-1] = '\0';
    setpadatar(&aplist, sudir, sumid, bytes, l_status, l_archsub, eff_date, group, 0, sunum);
  }
  if(found) do_tape_file();
  found = 0;
  //No more manifest to read. All done with tapearcX 
  // (still an ack due fr tape_svc)
  return(-1);
}

int main(int argc, char *argv[])
{ 
  FILE *notapearc;
  char *token;

  get_cmd(argc, argv);                  /* check the calling sequence */
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
    printf("!!NOTE: You must be user production to run tapearc!\n");
    exit(1);
  }
  if((notapearc=fopen(NOTAPEARC, "r")) != NULL) {
     printf("Can't run a tapearc while Imp/Exp of tapes is active\n");
     fclose(notapearc);
     exit(-1);
  }

  printf ("Current effective_date is %ld\n", TODAY);
  time (&now); printf ("%s\n",asctime(localtime(&now)));
  printf ("Datasets will be archived according to manifest file %s\n",
		manifest);
  if((fpman=fopen(manifest, "r")) == NULL) {
    printf("Can't open manifest file %s\n", manifest);
    return(-1);
  }
  while(fgets(mline, 256, fpman)) {	//get group= line & skip comments
    if(!strncmp(mline, "#group=", 7)) {
      token = (char *)strtok(mline, "=");
      token = (char *)strtok(NULL, " ");
      filegroup = (int)atoi(token);
    }
    if(!strncmp(mline, "#File ", 6)) {	//find line with #File
      printf("%s\n", mline);
      fgets(mline, 256, fpman);		//skip next comment line
      break;
    }
  }
  setup();
  while(1) {
    mstatus = file_from_manifest();
    if(mstatus == 0) continue; //try again
    if(mstatus == -1) {	//manifest done
      //the disconnect will be done in the dispatch code
      if(do_tape_called == 0) {		//tape_svc never called. go away
        goaway();
      }
    }
    break;
  }
  do_tape_called = 0;	//reset for next cycle

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

  //StartTimer(6); //!!TEMP for debug. time for tapearcdo_1()
  groupid = getkey_int(params, "group_id");
  index = getkey_uint64(params, "ds_index_0");
  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    printf("!!TEMP in tapearcdo_1() call in tapearc. keylist is:\n");
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
  if(mstatus == -1) {
    goaway();
  }
  while(1) {
    mstatus = file_from_manifest();
    if(mstatus == 0) continue; //try again
    if(mstatus == -1) {	//manifest done
      //the disconnect will be done in the dispatch code
      if(do_tape_called == 0) {		//tape_svc never called. go away
        goaway();
      }
    }
    break;
  }
  do_tape_called = 0;
  return((KEY *)1);
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
