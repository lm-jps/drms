/* sum_export_api.c */
/* Will take a request (typically from remotesums_ingest)
 * and do an scp for the given host, source and target dirs.
 * The ssh-agent must be set up properly for this scp to complete.
 * Returns 0 on success, else 1.
*/
#include <SUM.h>
#include <soi_key.h>
#include <rpc/rpc.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sum_rpc.h>

extern int errno;
static int RESPDO_called;
void printkey();
char *jdatestring();
void sumexportprog_1(struct svc_req *rqstp, SVCXPRT *transp);
int sumexport_wait();
int sumexport_poll();
extern void pmap_unset();
KEY *alist;
static struct timeval TIMEOUT = { 20, 0 };

/* forward declaration */
static int xgetanymsg(int block);

int SUM_export(SUMEXP_t *sumexp, int (*history)(const char *fmt, ...))
{
  KEY *list = newkeylist();
  CLIENT *clntsumex;
  register SVCXPRT *transp;
  char server_name[MAX_STR], ext[MAX_STR];
  char *server_name_p, *call_err, *cptr;
  char **sptr, **dptr;
  uint32_t sumexback, uid;
  int status, i, reqcnt;

  if (!(server_name_p = getenv("SUMSERVER")))
  {
    strcpy(server_name, SUMSERVER);
  }
  else {
    strcpy(server_name, server_name_p);
  }
  cptr = (char *)index(server_name, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;

    /* register for sum_export_svc to talk back to us */
    /* use our unique uid for our version number */
    uid = sumexp->uid;
    (void) pmap_unset(REMSUMPROG, uid);
    transp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
    if (transp == NULL) {
        (*history)("***cannot create tcp service\n");
        return(1);
    }
    if(!svc_register(transp, REMSUMPROG, uid, sumexportprog_1,IPPROTO_TCP)) {
        (*history)("***unable to register (REMSUMPROG, REMSUMVERS, tcp)\n");
        return(1);
    }

    /* Create client handle used for calling the sum_export_svc */
    clntsumex = clnt_create(server_name, SUMEXPROG, SUMEXVERS, "tcp");
    if(!clntsumex) {                   /* server not there */
      clnt_pcreateerror("Can't get client handle to sum_export_svc");
      (*history)("sum_export_svc not there on %s\n", server_name);
      return(1);
    }
    alist = newkeylist();
    reqcnt = sumexp->reqcnt;
    setkey_int(&list, "reqcnt", reqcnt);
    sptr = sumexp->src;
    dptr = sumexp->dest;
    for(i = 0; i < reqcnt; i++) {
      sprintf(ext, "src_%d", i);
      setkey_str(&list, ext, *sptr++);
      sprintf(ext, "dest_%d", i);
      setkey_str(&list, ext, *dptr++);
    }
    setkey_str(&list, "cmd", sumexp->cmd);
    setkey_str(&list, "host", sumexp->host);
    setkey_uint32(&list, "uid", sumexp->uid);
    setkey_uint(&list, "port", sumexp->port);

    status = clnt_call(clntsumex,SUMEXDO, (xdrproc_t)xdr_Rkey, (char *)list,
                        (xdrproc_t)xdr_uint32_t, (char *)&sumexback, TIMEOUT);
    if(status != RPC_SUCCESS) {
      call_err = clnt_sperror(clntsumex, "Err clnt_call for SUMEXDO");
      (*history)("%s %s\n", jdatestring(), call_err);
      if(status != RPC_TIMEDOUT) {
        return(1);
      }
    }
    if(sumexback != 0) {
      (*history)("Error status= %d from clnt_call to SUMEXDO = %d\n", sumexback);
      return(1);
    }
    status = sumexport_wait(); //wait for ans from sum_export_svc. Returns alist
    status = getkey_int(alist, "STATUS");
    if(status) {
      (*history)("%s", getkey_str(alist, "ERRMSG"));
      return(1);
    }
    return(0);
}


/* Check if the response for a  previous request is complete.
 * Return 0 = msg complete, the keylist  has been updated
 * TIMEOUTMSG = msg still pending, try again later
 * ERRMSG = fatal error (!!TBD find out what you can do if this happens)
*/
int sumexport_poll() 
{
  int stat;

  stat = xgetanymsg(0);
  if(stat == RPCMSG) return(0);		/* all done ok */
  else return(stat);
}

/* Wait until the expected response is complete.
 * Return 0 = msg complete, the  keylist has been updated
 * ERRMSG = fatal error (!!TBD find out what you can do if this happens)
*/
int sumexport_wait() 
{
  int stat;

  stat = xgetanymsg(1);
  if(stat == RPCMSG) return(0);		/* all done ok */
  else return(stat);
}

/**************************************************************************/

/* Attempt to get any sum_pe_svc completion msg.
 * If block = 0 will timeout after 0.5 sec, else will wait until a msg is
 * received.
 * Returns the type of msg or timeout status.
*/
int xgetanymsg(int block)
{
  fd_set readfds;
  struct timeval timeout;
  int wait, retcode = ERRMSG, srdy;
  int ts=getdtablesize();   /* file descriptor table size */

  wait = 1;
  timeout.tv_sec=0;
  timeout.tv_usec=500000;
  while(wait) {
    readfds=svc_fdset;
    srdy=select(ts,&readfds,(fd_set *)0,(fd_set *)0,&timeout); /* # ready */
    switch(srdy) {
    case -1:
      if(errno==EINTR) {
        continue;
      }
      perror("xgetanymsg: select failed");
      retcode = ERRMSG;
      wait = 0;
      break;
    case 0:			  /* timeout */
      if(block) continue;
      retcode = TIMEOUTMSG;
      wait = 0;
      break;
    default:
      /* can be called w/o dispatch to sumexportprog_1(),
       * but will happen on next call */
      RESPDO_called = 0;	  /* set by sumexportprog_1() */
      svc_getreqset(&readfds);    /* calls sumexportprog_1() */
      retcode = RPCMSG;
      if(RESPDO_called) wait = 0;
      break;
    }
  }
  return(retcode);
}


/* Function called on receipt of a sum_export_svc response message.
 * Called from sumexportprog_1().
*/
KEY *xrespdo_1(KEY *params)
{
  char *wd;
  char **cptr;
  int i, reqcode;
  /*int reqcnt;*/
  char name[80];
                                         
  /*printf("\nIn xrespdo_1() the keylist is:\n");*/
  /*keyiterate((*history), params);*/
  /*reqcnt = getkey_int(params, "in_nsets");*/
  reqcode = getkey_int(params, "REQCODE");
  switch(reqcode) {
  case REMSUMRESPDO:
    /************************************
    for(i = 0; i < reqcnt; i++) {
      sprintf(name, "in_%d_wd", i);
      wd = getkey_str(params, name);
      printf("%s = %s\n", name, wd);
    } 
    ************************************/
    if(findkey(params, "ERRSTR")) {
      printf("ERRSTR in xrespdo_1: %s\n", getkey_str(params, "ERRSTR"));
    }
    add_keys(params, &alist);	/* get keys back to original caller */
    break;
  default:
    printf("**Unexpected REQCODE in xrespdo_1()\n");
    break;
  }
  return((KEY *)NULL);
}


/* This is the dispatch routine for the registered REMSUMPROG, uid.
 * Called when a sum_export_svc sends a response that it is done with the 
 * original call that we made to it. 
 * This routine is called by svc_getreqset() in xgetanymsg().
*/
void sumexportprog_1(rqstp, transp)
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
  case REMSUMRESPDO:
    xdr_argument = xdr_Rkey;
    /*xdr_result = xdr_void;*/
    local = (char *(*)()) xrespdo_1;
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
  /*result = (*local)(&argument, rqstp);	/* call the fuction */
  (*local)(&argument, rqstp);	/* call the fuction */

  if(!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
    printf("unable to free arguments in respd()\n");
    /*svc_unregister(RESPPROG, mytid);*/
  }
}

/* Return ptr to "mmm dd hh:mm:ss". */
char *jdatestring(void)
{
  time_t t;
  char *str;

  t = time(NULL);
  str = ctime(&t);
  str[19] = 0;
  return str+4;          /* isolate the mmm dd hh:mm:ss */
}

