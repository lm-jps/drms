/**
For Full SUMS API see:\n
http://sun.stanford.edu/web.hmi/development/SU_Development_Plan/SUM_API.html

   @addtogroup sum_api
   @{
*/

/**
   @fn SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...))

	A DRMS instance opens a session with SUMS. It gives the  server
	name to connect to, defaults to SUMSERVER env else SUMSERVER define.
	The db name has been depricated and has no effect. The db will be
	the one that sum_svc was started with, e.g. sum_svc hmidb.
	The history is a printf type logging function.
	Returns a pointer to a SUM handle that is
	used to identify this user for this session. 
	Returns NULL on failure.
	Currently the dsix_ptr[] and wd[] arrays are malloc'd to size
	SUMARRAYSZ (64).
*/

/**
  @}
*/


/* sum_open.c */
/* Here are the API functions for SUMS.
 * This is linked in with each program that is going to call SUMS.
*/
#include <SUM.h>
#include <soi_key.h>
#include <rpc/rpc.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sum_rpc.h>

extern int errno;

/* Static prototypes. */
SUMID_t sumrpcopen_1(KEY *argp, CLIENT *clnt, int (*history)(const char *fmt, ...));
static void respd(struct svc_req *rqstp, SVCXPRT *transp);
int getanymsg(int block);
static int getmsgimmed ();
static char *datestring(void);

/* External prototypes */
extern void pmap_unset();
extern void printkey (KEY *key);

//static struct timeval TIMEOUT = { 240, 0 };
static struct timeval TIMEOUT = { 3600, 0 };
static int RESPDO_called;
static SUMOPENED *sumopened_hdr = NULL;/* linked list of opens for this client*/
static CLIENT *cl;
static SVCXPRT *transp[MAXSUMOPEN];
static SUMID_t transpid[MAXSUMOPEN];
static int numopened = 0;
//KEY *infoparams;

/* Returns 1 if ok to shutdown sums.
 * Return 0 is someone still has an active SUM_open().
 * Once called, will prevent any user from doing a new SUM_open()
 * unless query arg = 1.
*/
int SUM_shutdown(int query, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *server_name, *cptr, *username;
  char *call_err;
  int response;
  enum clnt_stat status;

  if (!(server_name = getenv("SUMSERVER")))
  {
    server_name = alloca(sizeof(SUMSERVER)+1);
    strcpy(server_name, SUMSERVER);
  }
  cptr = index(server_name, '.');	/* must be short form */
  if(cptr) *cptr = (char)NULL;
  /* Create client handle used for calling the server */
  cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
  if(!cl) {                   /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_svc");
    (*history)("sum_svc not there on %s\n", server_name);
    return(1);			//it's not there, so say ok to shutdown
  }
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  klist = newkeylist();
  setkey_str(&klist, "USER", username);
  setkey_int(&klist, "QUERY", query);
  status = clnt_call(cl, SHUTDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&response, TIMEOUT);
  /* NOTE: Must honor the timeout here as get the ans back in the ack
  */
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(cl, "Err clnt_call for SHUTDO");
    (*history)("%s %s status=%d\n", datestring(), call_err, status);
    return (1);
  }
  return(response);
}

/* This must be the first thing called by DRMS to open with the SUMS.
 * Any one client may open up to MAXSUMOPEN times (TBD check) 
 * (although it's most likely that a client only needs one open session 
 * with SUMS, but it's built this way in case a use arises.).
 * Returns 0 on error else a pointer to a SUM structure.
 * **NEW 7Oct2005 the db arg has been depricated and has no effect. The db
 * you will get depends on how sum_svc was started, e.g. "sum_svc jsoc".
*/
SUM_t *SUM_open(char *server, char *db, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  SUM_t *sumptr;
  SUMID_t sumid;
  char *server_name, *cptr, *username;

  if(numopened >= MAXSUMOPEN) {
    (*history)("Exceeded max=%d SUM_open() for a client\n", MAXSUMOPEN);
    return(NULL);
  }

  if (server)
  {
      server_name = alloca(strlen(server)+1);
      strcpy(server_name, server);
  }
  else
  {
    if (!(server_name = getenv("SUMSERVER")))
    {
      server_name = alloca(sizeof(SUMSERVER)+1);
      strcpy(server_name, SUMSERVER);
    }
  }
  cptr = index(server_name, '.');	/* must be short form */
  if(cptr) *cptr = (char)NULL;
  /* Create client handle used for calling the server */
  cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
  if(!cl) {                   /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_svc");
    (*history)("sum_svc not there on %s\n", server_name);
    return(NULL);
  }
  if (!db)
  {
    if (!(db = getenv("SUMDB")))
    {
      db = alloca(sizeof(SUMDB)+1);
      strcpy(db, SUMDB);
    }
  }
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  klist = newkeylist();
  setkey_str(&klist, "db_name", db);
  setkey_str(&klist, "USER", username);
  /* get a unique id from sum_svc for this open */
  if((sumid = sumrpcopen_1(klist, cl, history)) == 0) {
    (*history)("Failed to get SUMID from sum_svc\n");
    clnt_destroy(cl);
    freekeylist(&klist);
    return(NULL);
  }
  numopened++;
  sumptr = (SUM_t *)malloc(sizeof(SUM_t));
  sumptr->sinfo = NULL;
  sumptr->cl = cl;
  sumptr->uid = sumid;
  sumptr->username = username;
  sumptr->tdays = 0;
  sumptr->debugflg = 0;		/* default debug off */
  sumptr->storeset = JSOC;	/* default storage set */
  sumptr->dsname = "<none>";
  sumptr->history_comment = NULL;
  sumptr->dsix_ptr = (uint64_t *)malloc(sizeof(uint64_t) * SUMARRAYSZ);
  sumptr->wd = (char **)calloc(SUMARRAYSZ, sizeof(char *));
  setsumopened(&sumopened_hdr, sumid, sumptr, username); //put in open list
  freekeylist(&klist);
  return(sumptr);
}

/* Open with sum_svc. Return 0 on error.
*/
SUMID_t sumrpcopen_1(KEY *argp, CLIENT *clnt, int (*history)(const char *fmt, ...))
{
  char *call_err;
  enum clnt_stat status;
  SUMID_t suidback;
  SVCXPRT *xtp;
                                                                        
  status = clnt_call(clnt, OPENDO, (xdrproc_t)xdr_Rkey, (char *)argp, 
			(xdrproc_t)xdr_uint32_t, (char *)&suidback, TIMEOUT);
  suidback = (SUMID_t)suidback;

  /* NOTE: Must honor the timeout here as get the ans back in the ack
  */
  if(status != RPC_SUCCESS) {
    call_err = clnt_sperror(clnt, "Err clnt_call for OPENDO");
    (*history)("%s %s status=%d\n", datestring(), call_err, status);
    return (0);
  }
  /* (*history)("suidback = %d\n", suidback); /* !!TEMP */

  /* register for future calls to receive the sum_svc completion msg */
  /* Use our suidback as the version number. */
  if(suidback) {
    (void)pmap_unset(RESPPROG, suidback); /* first unreg any left over */
    xtp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
    if (xtp == NULL) {
      (*history)("cannot create tcp service in sumrpcopen_1() for responses\n");
      return(0);
    }
    if (!svc_register(xtp, RESPPROG, suidback, respd, IPPROTO_TCP)) {
      (*history)("unable to register RESPPROG in sumrpcopen_1()\n");
      return(0);
    }
    transp[numopened] = xtp;
    transpid[numopened] = suidback;
  }
  return (suidback);
}

/* Allocate the storage given in sum->bytes.
 * Return non-0 on error, else return wd of allocated storage in *sum->wd.
*/
int SUM_alloc(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  int msgstat;
  enum clnt_stat status;

  if(sum->reqcnt != 1) {
    (*history)("Invalid reqcnt = %d for SUM_alloc(). Can only alloc 1.\n",
		sum->reqcnt);
    return(1);
  }
  klist = newkeylist();
  setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "storeset", sum->storeset);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", ALLOCDO);
  setkey_str(&klist, "USER", sum->username);
  if(sum->debugflg) {
    (*history)("In SUM_alloc() the keylist is:\n");
    keyiterate(printkey, klist);
  }
  status = clnt_call(sum->cl, ALLOCDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->cl, "Err clnt_call for ALLOCDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (1);
    }
  }
  if(retstat) {			/* error on ALLOCDO call */
    (*history)("Error in SUM_alloc()\n");
    return(retstat);
  }
  else {
    msgstat = getanymsg(1);	/* get answer to ALLOCDO call */
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    return(sum->status);
  }
}

/* Allocate the storage given in sum->bytes for the given sunum.
 * Return non-0 on error, else return wd of allocated storage in *sum->wd.
*/
int SUM_alloc2(SUM_t *sum, uint64_t sunum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  int msgstat;
  enum clnt_stat status;

  //!!TEMP until learn how to validate the given sunum
  //(*history)("!TEMP reject of SUM_alloc2() call until we can validate sunum\n");
  //return(1);

  if(sum->reqcnt != 1) {
    (*history)("Invalid reqcnt = %d for SUM_alloc2(). Can only alloc 1.\n",
		sum->reqcnt);
    return(1);
  }
  klist = newkeylist();
  setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "storeset", sum->storeset);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_uint64(&klist, "SUNUM", sunum); //unique to the SUM_alloc2() call
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", ALLOCDO);
  setkey_str(&klist, "USER", sum->username);
  if(sum->debugflg) {
    (*history)("In SUM_alloc2() the keylist is:\n");
    keyiterate(printkey, klist);
  }
  status = clnt_call(sum->cl, ALLOCDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->cl, "Err clnt_call for ALLOCDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (1);
    }
  }
  if(retstat) {			/* error on ALLOCDO call */
    (*history)("Error in SUM_alloc2()\n");
    return(retstat);
  }
  else {
    msgstat = getanymsg(1);	/* get answer to ALLOCDO call */
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    return(sum->status);
  }
}

/* Return information from sum_main for the given sunum (ds_index).
 * Return non-0 on error, else sum->sinfo has the SUM_info_t pointer.
*/
int SUM_info(SUM_t *sum, uint64_t sunum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  uint32_t retstat;
  int msgstat;
  enum clnt_stat status;

  if(sum->sinfo == NULL) 
    sum->sinfo = (SUM_info_t *)malloc(sizeof(SUM_info_t));
  klist = newkeylist();
  setkey_uint64(&klist, "SUNUM", sunum); 
  setkey_str(&klist, "username", sum->username);
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", INFODO);
  status = clnt_call(sum->cl, INFODO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->cl, "Err clnt_call for INFODO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (1);
    }
  }
  if(retstat) {			/* error on INFODO call */
    if(retstat != SUM_SUNUM_NOT_LOCAL)
      (*history)("Error in SUM_info()\n"); //be quite for show_info sake
    return(retstat);
  }
  else {
    msgstat = getanymsg(1);	/* get answer to INFODO call */
    freekeylist(&klist);
    if(msgstat == ERRMESS) return(ERRMESS);
    //printf("\nIn SUM_info() the keylist is:\n"); //!!TEMP
    //keyiterate(printkey, infoparams);
    return(0);
  }
}


/* Close this session with the SUMS. Return non 0 on error.
*/
int SUM_close(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  static char res;
  enum clnt_stat status;
  int i, stat;
  int errflg = 0;

  if(sum->debugflg) {
    (*history)("SUM_close() call: uid = %lu\n", sum->uid);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", CLOSEDO);
  setkey_str(&klist, "USER", sum->username);
  bzero((char *)&res, sizeof(res));
  status = clnt_call(sum->cl, CLOSEDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_void, &res, TIMEOUT);

/* NOTE: These rtes seem to return after the reply has been received despite
 * the timeout value. If it did take longer than the timeout then the timeout
 * error status is set but it should be ignored.
*/
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->cl, "Err clnt_call for CLOSEDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      errflg = 1;
    }
  }

  stat = getmsgimmed();		//clean up pending response

  (void)pmap_unset(RESPPROG, sum->uid); /* unreg response server */
  remsumopened(&sumopened_hdr, sum->uid); /* rem from linked list */
  clnt_destroy(sum->cl);	/* destroy handle to sum_svc */
  for(i=0; i < MAXSUMOPEN; i++) {
    if(transpid[i] == sum->uid) {
      svc_destroy(transp[i]);
      --numopened;
      break;
    }
  }
  free(sum->dsix_ptr);
  free(sum->wd);
  if(sum->sinfo) free(sum->sinfo);
  free(sum);
  freekeylist(&klist);
  if(errflg) return(1);
  return(0);
}

/* Get the wd of the storage units given in dsix_ptr of the given sum.
 * Return 0 on success w/data available, 1 on error, or RESULT_PEND
 * when data will be sent later and caller must do a sum_wait() or sum_poll() 
 * when he is ready for it.
*/
int SUM_get(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char *call_err;
  char **cptr;
  int i, cnt, msgstat;
  char dsix_name[64];
  uint64_t *dxlong;
  uint32_t retstat;
  enum clnt_stat status;

  if(sum->debugflg) {
    (*history)("SUM_get() call:\n");
  }
  if(sum->reqcnt > MAXSUMREQCNT) {
    (*history)("Requent count of %d > max of %d\n", sum->reqcnt, MAXSUMREQCNT);
    return(1);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid); 
  setkey_int(&klist, "mode", sum->mode);
  setkey_int(&klist, "tdays", sum->tdays);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", GETDO);
  setkey_str(&klist, "username", sum->username);
  dxlong = sum->dsix_ptr;
  for(i = 0; i < sum->reqcnt; i++) {
    sprintf(dsix_name, "dsix_%d", i);
    setkey_uint64(&klist, dsix_name, *dxlong++);
  }
  status = clnt_call(sum->cl, GETDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->cl, "Err clnt_call for GETDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (1);
    } else {
      (*history)("%s Ignore timeout in SUM_get()\n", datestring());
    }
  }
  freekeylist(&klist);
  if(sum->debugflg) {
    (*history)("retstat in SUM_get = %ld\n", retstat);
  }
  if(retstat == 1) return(1);			 /* error occured */
  if(retstat == RESULT_PEND) return((int)retstat); /* caller to check later */
  msgstat = getanymsg(1);	/* answer avail now */
  if(msgstat == ERRMESS) return(ERRMESS);
  if(sum->debugflg) {
    /* print out wd's found */
    (*history)("In SUM_get() the wd's found are:\n");
    cnt = sum->reqcnt;
    cptr = sum->wd;
    for(i = 0; i < cnt; i++) {
      printf("wd = %s\n", *cptr++);
    }
  }
  return(sum->status);
}

/* Puts storage units from allocated storage to the DB catalog.
 * Caller gives disposition of a previously allocated data segments. 
 * Returns 0 on success.
*/
int SUM_put(SUM_t *sum, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  char dsix_name[64];
  char *call_err;
  char **cptr;
  uint64_t *dsixpt;
  int i, cnt, msgstat;
  uint32_t retstat;
  enum clnt_stat status;

  if(sum->reqcnt != 1) {
    (*history)("Invalid reqcnt = %d for SUM_put(). Can only put 1.\n",
		sum->reqcnt);
    return(1);
  }
  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  if(sum->debugflg) {
    (*history)("Going to PUT wd=%s, ix=%lu\n", *cptr, *dsixpt);
  }
  klist = newkeylist();
  setkey_uint64(&klist, "uid", sum->uid);
  setkey_int(&klist, "mode", sum->mode);
  setkey_int(&klist, "tdays", sum->tdays);
  setkey_int(&klist, "reqcnt", sum->reqcnt);
  setkey_str(&klist, "dsname", sum->dsname);
  setkey_str(&klist, "history_comment", sum->history_comment);
  setkey_str(&klist, "username", sum->username);
  setkey_int(&klist, "group", sum->group);
  setkey_int(&klist, "storage_set", sum->storeset);
  setkey_double(&klist, "bytes", sum->bytes);
  setkey_int(&klist, "DEBUGFLG", sum->debugflg);
  setkey_int(&klist, "REQCODE", PUTDO);
  for(i = 0; i < sum->reqcnt; i++) {
    sprintf(dsix_name, "dsix_%d", i);
    setkey_uint64(&klist, dsix_name, *dsixpt++);
    sprintf(dsix_name, "wd_%d", i);
    setkey_str(&klist, dsix_name, *cptr++);
  }
  status = clnt_call(sum->cl, PUTDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(sum->cl, "Err clnt_call for PUTDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (1);
    }
  }
  freekeylist(&klist);
  if(retstat == 1) return(1);           /* error occured */
  /* NOTE: RESULT_PEND cannot happen for SUM_put() call */
  /*if(retstat == RESULT_PEND) return((int)retstat); /* caller to check later */
  msgstat = getanymsg(1);		/* answer avail now */
  if(msgstat == ERRMESS) return(ERRMESS);
  if(sum->debugflg) {
    (*history)("In SUM_put() print out wd's \n");
    cnt = sum->reqcnt;
    cptr = sum->wd;
    for(i = 0; i < cnt; i++) {
      printf("wd = %s\n", *cptr++);
    }
  }
  return(sum->status);
}

/* Called by the delete_series program before it deletes the series table.
 * Called with a pointer to a full path name that contains the sunums
 * that are associated with the series about to be deleted.
 * Returns 1 on error, else 0.
*/
int SUM_delete_series(char *filename, char *seriesname, int (*history)(const char *fmt, ...))
{
  KEY *klist;
  CLIENT *cl;
  char *call_err;
  char *cptr, *server_name, *username;
  uint32_t retstat;
  enum clnt_stat status;

  klist = newkeylist();
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  setkey_str(&klist, "USER", username);
  setkey_int(&klist, "DEBUGFLG", 1);		/* !!!TEMP */
  setkey_str(&klist, "FILE", filename);
  setkey_str(&klist, "SERIESNAME", seriesname);
  if (!(server_name = getenv("SUMSERVER"))) {
    server_name = (char *)alloca(sizeof(SUMSERVER)+1);
    strcpy(server_name, SUMSERVER);
  }
  cptr = (char *)index(server_name, '.');	/* must be short form */
  if(cptr) *cptr = (char)NULL;
  /* Create client handle used for calling the server */
  cl = clnt_create(server_name, SUMPROG, SUMVERS, "tcp");
  if(!cl) {                   /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_svc");
    (*history)("sum_svc not there on %s\n", server_name);
    return(1);
  }
  status = clnt_call(cl, DELSERIESDO, (xdrproc_t)xdr_Rkey, (char *)klist, 
			(xdrproc_t)xdr_uint32_t, (char *)&retstat, TIMEOUT);

  /* NOTE: These rtes seem to return after the reply has been received despite
   * the timeout value. If it did take longer than the timeout then the timeout
   * error status is set but it should be ignored.
  */
  if(status != RPC_SUCCESS) {
    if(status != RPC_TIMEDOUT) {
      call_err = clnt_sperror(cl, "Err clnt_call for DELSERIESDO");
      (*history)("%s %d %s\n", datestring(), status, call_err);
      freekeylist(&klist);
      return (1);
    }
  }
  freekeylist(&klist);
  clnt_destroy(cl);		/* destroy handle to sum_svc */
  if(retstat == 1) return(1);           /* error occured */
  return(0);
}

/* Check if the response for a  previous request is complete.
 * Return 0 = msg complete, the sum has been updated
 * TIMEOUTMSG = msg still pending, try again later
 * ERRMESS = fatal error (!!TBD find out what you can do if this happens)
 * NOTE: Upon msg complete return, sum->status != 0 if error anywhere in the 
 * path of the request that initially returned the RESULT_PEND status.
*/
int SUM_poll(SUM_t *sum) 
{
  int stat;

  stat = getanymsg(0);
  if(stat == RPCMSG) return(0);		/* all done ok */
  else return(stat);
}

/* Wait until the expected response is complete.
 * Return 0 = msg complete, the sum has been updated
 * ERRMESS = fatal error (!!TBD find out what you can do if this happens)
 * NOTE: Upon msg complete return, sum->status != 0 if error anywhere in the 
 * path of the request that initially returned the RESULT_PEND status.
*/
int SUM_wait(SUM_t *sum) 
{
  int stat;

  while(1) {
    stat = getanymsg(0);
    if(stat == TIMEOUTMSG) {
      usleep(1000);
      continue;
    }
    else break;
  }
  if(stat == RPCMSG) return(0);		/* all done ok */
  else return(stat);
}

/**************************************************************************/

/* Attempt to get any sum_svc completion msg.
 * If block = 0 will timeout after 0.5 sec, else will wait until a msg is
 * received.
 * Returns the type of msg or timeout status.
*/
int getanymsg(int block)
{
  fd_set readfds;
  struct timeval timeout;
  int wait, retcode = ERRMESS, srdy, info;
  static int ts=0;   /* file descriptor table size */

  wait = 1;
  timeout.tv_sec=0;
  timeout.tv_usec=500000;
  if(!ts) ts = getdtablesize();
  while(wait) {
    readfds=svc_fdset;
    srdy=select(ts,&readfds,(fd_set *)0,(fd_set *)0,&timeout); /* # ready */
    switch(srdy) {
    case -1:
      if(errno==EINTR) {
        continue;
      }
      fprintf(stderr, "%s\n", datestring());
      perror("getanymsg: select failed");
      retcode = ERRMESS;
      wait = 0;
      break;
    case 0:			  /* timeout */
      if(block) continue;
      retcode = TIMEOUTMSG;
      wait = 0;
      break;
    default:
      /* can be called w/o dispatch to respd(), but will happen on next call */
      RESPDO_called = 0;	  /* set by respd() */
      svc_getreqset(&readfds);    /* calls respd() */
      retcode = RPCMSG;
      if(RESPDO_called) wait = 0;
      break;
    }
  }
  return(retcode);
}

// Like getanymsg() above but w/no timeout for immediate SUM_close() use.
int getmsgimmed()
{
  fd_set readfds;
  struct timeval timeout;
  int wait, retcode = ERRMESS, srdy, info;
  static int ts=0;   /* file descriptor table size */

  wait = 1;
  timeout.tv_sec=0;
  timeout.tv_usec=0;
  if(!ts) ts = getdtablesize();
  while(wait) {
    readfds=svc_fdset;
    srdy=select(ts,&readfds,(fd_set *)0,(fd_set *)0,&timeout); /* # ready */
    switch(srdy) {
    case -1:
      if(errno==EINTR) {
        continue;
      }
      fprintf(stderr, "%s\n", datestring());
      perror("getanymsg: select failed");
      retcode = ERRMESS;
      wait = 0;
      break;
    case 0:			  /* timeout */
      retcode = TIMEOUTMSG;
      wait = 0;
      break;
    default:
      /* can be called w/o dispatch to respd(), but will happen on next call */
      RESPDO_called = 0;	  /* set by respd() */
      svc_getreqset(&readfds);    /* calls respd() */
      retcode = RPCMSG;
      if(RESPDO_called) wait = 0;
      break;
    }
  }
  return(retcode);
}

/* Function called on receipt of a sum_svc response message.
 * Called from respd().
*/
KEY *respdo_1(KEY *params)
{
  SUM_t *sum;
  SUM_info_t *sinfo;
  SUMOPENED *sumopened;
  char *wd;
  char **cptr;
  uint64_t *dsixpt;
  uint64_t dsindex;
  int i, reqcnt, reqcode;
  char name[80];
                                         
  sumopened = getsumopened(sumopened_hdr, getkey_uint64(params, "uid"));
  sum = (SUM_t *)sumopened->sum;
  if(sum == NULL) {
    printf("**Response from sum_svc does not have an opened SUM_t *sum\n");
    printf("**Don't know what this will do to the caller, but this is a logic bug\n");
    return((KEY *)NULL);
  }
  if(sum->debugflg) {
    printf("\nIn respdo_1() the keylist is:\n");
    keyiterate(printkey, params);
  }
  reqcnt = getkey_int(params, "reqcnt");
  reqcode = getkey_int(params, "REQCODE");
  sum->status = getkey_int(params, "STATUS");
  cptr = sum->wd;
  dsixpt = sum->dsix_ptr;
  switch(reqcode) {
  case ALLOCDO:
    wd = getkey_str(params, "partn_name");
    dsindex = getkey_uint64(params, "ds_index");
    *cptr = wd;
    *dsixpt = dsindex;
    break;
  case GETDO:
    for(i = 0; i < reqcnt; i++) {
        sprintf(name, "wd_%d", i);
      wd = getkey_str(params, name);
      *cptr++ = wd;
      if(findkey(params, "ERRSTR")) {
        printf("%s\n", GETKEY_str(params, "ERRSTR"));
      }
    } 
    break;
  case INFODO:
    //add_keys(infoparams, &params);
    sinfo = sum->sinfo;
    sinfo->sunum = getkey_uint64(params, "SUNUM");
    strcpy(sinfo->online_loc, GETKEY_str(params, "online_loc"));
    strcpy(sinfo->online_status, GETKEY_str(params, "online_status"));
    strcpy(sinfo->archive_status, GETKEY_str(params, "archive_status"));
    strcpy(sinfo->offsite_ack, GETKEY_str(params, "offsite_ack"));
    strcpy(sinfo->history_comment, GETKEY_str(params, "history_comment"));
    strcpy(sinfo->owning_series, GETKEY_str(params, "owning_series"));
    sinfo->storage_group = getkey_int(params, "storage_group");
    sinfo->bytes = getkey_double(params, "bytes");
    strcpy(sinfo->creat_date, GETKEY_str(params, "creat_date"));
    strcpy(sinfo->username, GETKEY_str(params, "username"));
    strcpy(sinfo->arch_tape, GETKEY_str(params, "arch_tape"));
    sinfo->arch_tape_fn = getkey_int(params, "arch_tape_fn");
    strcpy(sinfo->arch_tape_date, GETKEY_str(params, "arch_tape_date"));
    strcpy(sinfo->safe_tape, GETKEY_str(params, "safe_tape"));
    sinfo->safe_tape_fn = getkey_int(params, "safe_tape_fn");
    strcpy(sinfo->safe_tape_date, GETKEY_str(params, "safe_tape_date"));
    sinfo->pa_status = getkey_int(params, "pa_status");
    sinfo->pa_substatus = getkey_int(params, "pa_substatus");
    strcpy(sinfo->effective_date, GETKEY_str(params, "effective_date"));
    break;
  case PUTDO:
    break;
  default:
    printf("**Unexpected REQCODE in respdo_1()\n");
    break;
  }
  return((KEY *)NULL);
}


/* This is the dispatch routine for the registered RESPPROG, suidback.
 * Called when a server sends a response that it is done with the original 
 * call that we made to it. 
 * This routine is called by svc_getreqset() in getanymsg().
*/
static void respd(rqstp, transp)
  struct svc_req *rqstp;
  SVCXPRT *transp;
{
  union __svcargun {
    Rkey respdo_1_arg;
  } argument;
  char *result;
  bool_t (*xdr_argument)(), (*xdr_result)();
  char *(*local)();

  switch (rqstp->rq_proc) {
  case NULLPROC:
    (void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
    return;
  case RESPDO:
    xdr_argument = xdr_Rkey;
    xdr_result = xdr_void;
    local = (char *(*)()) respdo_1;
    RESPDO_called = 1;
    break;
  default:
    svcerr_noproc(transp);
    return;
  }
  bzero((char *)&argument, sizeof(argument));
  if(!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
    svcerr_decode(transp);
    return;
  }
  /* send immediate ack back */
  if(!svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL)) {
    svcerr_systemerr(transp);
    return;
  }
  result = (*local)(&argument, rqstp);	/* call the fuction */

  if(!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
    printf("unable to free arguments in respd()\n");
    /*svc_unregister(RESPPROG, mytid);*/
  }
}

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". */
char *datestring(void)
{
  time_t t;
  char *str;

  t = time(NULL);
  str = ctime(&t);
  str[19] = 0;
  return str+4;          /* isolate the mmm dd hh:mm:ss */
}

