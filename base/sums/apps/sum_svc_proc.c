/* sum_svc_proc.c: Server side procedures
 *
 * These are the function that are called by sum_svc when it gets a 
 * message from a client. 
*/
#include <SUM.h>
#include <sys/time.h>
#include <rpc/rpc.h>
#include <soi_key.h>
#include <soi_error.h>
#include <sum_rpc.h>

extern int write_log(const char *fmt, ...);
extern CLIENT *current_client, *clnttape;
extern SVCXPRT *glb_transp;
extern uint32_t rinfo;
extern int debugflg;
static int NO_OPEN = 0;

void write_time();
void logkey();

/*********************************************************/
/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[]. 
*/
static char *datestring()
{
  static char datestr[32];
  struct timeval tvalr;
  struct tm *t_ptr;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(datestr, "%s", asctime(t_ptr));
  datestr[19] = (char)NULL;
  return(&datestr[4]);          /* isolate the mmm dd hh:mm:ss */
}

static KEY *retlist;		/* must be static for svc dispatch rte */
static SUMOPENED *sumopened_hdr = NULL; /* linked list of active opens*/

/* These are the PROGNUM called in sum_svc. They're called by 
 * sumprog_1() in sum_svc.c
*/

/* Get client handle for return of result and store in glb vrbl current_client.
*/
CLIENT *set_client_handle(uint32_t prognum, uint32_t versnum)
{
  static CLIENT *client;
  struct sockaddr_in *sock_in;
  int sock = RPC_ANYSOCK;

    /* set up a client handle for eventual ret of the result with a call
     * to the requestor's local daemon. But
     * first must translate into caller host info to call the cliens back.
    */
    sock_in = svc_getcaller(glb_transp);/* get caller socket info */
    sock_in->sin_port = 0;	/* makes clnttcp_create consult yp */
    client = clnttcp_create(sock_in,prognum,versnum,&sock,0,0);
    if(!client) {
      clnt_pcreateerror("Can't do a clnttcp_create to send a response");
      write_time();
      write_log("**** Can't do a clnttcp_create to send a response ****\n");
      write_log("**** Did someone remove us from the portmapper? ****\n");
      return(0);		/* error ret */
    }
    /* set glb vrbl for poss use by sum_svc if result != 0 */
    current_client = client;
    return(client);
}

/* Send ack to original sum_svc caller. Uses global vrbls glb_transp and
 * rinfo which are set up before this call.
 * I'm not quite sure what to do on an error here?? I've never seen it and
 * will ignore it for now.
*/
void send_ack()
{
  /* send ack back with the rinfo uint32_t value */
  if (!svc_sendreply(glb_transp, (xdrproc_t)xdr_uint32_t, (char *)&rinfo)) {
    write_log("***Error on immed ack back to client. FATAL???\n");
    svcerr_systemerr(glb_transp);
  }
}

/*************************************************************************/

/* Called by the SUM API SUM_open() making a clnt_call to sum_svc for the
 * OPENDO procedure. Returns an open uid in the ack back to the caller,
 * or 0 if error. Called:
 * db_name:        KEYTYP_STRING   hmidbX
 * USER:           KEYTYP_STRING   production
 *NOTE: the db_name is not used yet. The sum_svc is started for a db.
*/
KEY *opendo_1(KEY *params)
{
  char *user;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in opendo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  user = GETKEY_str(params, "USER");
  if(NO_OPEN) {
    write_time();
    write_log("No SUM_open() for %s allowed during SUM shutdown\n", user);
    rinfo = 0;				/* error back to caller */
    send_ack();				/* ack original sum_svc caller */
    return((KEY *)1);			/* nothing will be sent later */
  }
  rinfo = SUMLIB_Open();		/* return a SUMID_t else 0 */
  if(rinfo) {
    write_time();
    write_log("Successful SUMLIB_Open for user=%s uid=%d\n", user, rinfo);
    setsumopened(&sumopened_hdr, rinfo, NULL, user); /*put in list of opens*/

  }
  send_ack();				/* ack original sum_svc caller */
  return((KEY *)1);			/* nothing will be sent later */
}

/* Called by the SUM API SUM_shutdown() making a clnt_call to sum_svc for the
 * SHUTDO procedure. First sets the NO_OPEN flag to prevent further 
 * SUM_open's by a user if QUERY = 0. NOTE: QUERY =1 will clear NO_OPEN. 
 * Return 1 if no user is currently open and it is safe to shutdown.
 * Else returns 0 if you must wait for a user to SUM_close() before it is
 * safe to shutdown SUMS. Also prints to log all open user names.
 * Here is a typical keylist:
 * USER:           KEYTYP_STRING   production
 * QUERY:	   KEYTYP_INT	   0
 * 
*/
KEY *shutdo_1(KEY *params)
{
  SUMOPENED *walk = sumopened_hdr;
  int query;

  query = getkey_int(params, "QUERY");
  if(!query)
    NO_OPEN = 1;			/* no more SUM_open() allowed */
  else
    NO_OPEN = 0;
  if(walk == NULL) { 			/* nothing opened, ok to shutdown */
    if(NO_OPEN)
      write_log("SUM_shutdown() call with no opened SUM users. New opens not allowed\n");
    else
      write_log("SUM_shutdown() call with no opened SUM users. New opens still allowed\n");
    rinfo = 1;
  }
  else {
    write_log("SUM_shutdown() call with opened users & uid:\n");
    rinfo = 0;				/* tell user can't shutdown yet */
    while(walk) {
      write_log("  %s %lu\n", walk->user, walk->uid);
      walk = walk->next;
    } 
    if(NO_OPEN)
      write_log("New opens not allowed\n");
    else
      write_log("New opens still allowed\n");
  }
  send_ack();				/* ack original sum_svc caller */
  return((KEY *)1);			/* nothing to be sent */
}

/* Called by the SUM API SUM_get() making a clnt_call to sum_svc for the
 * GETDO procedure. Return 1 if nothing to be sent or RESULT_PEND if one
 * or more storage units are offline and need to be retrieved from tape
 * (rinfo has status back to caller), else returns retlist of queried values.
 * Here is a typical keylist:
 * dsix_1: KEYTYP_UINT64    285
 * dsix_0: KEYTYP_UINT64    282
 * username:       KEYTYP_STRING   production
 * REQCODE:        KEYTYP_INT      4
 * DEBUGFLG:       KEYTYP_INT      1
 * reqcnt: KEYTYP_INT      2
 * tdays:  KEYTYP_INT      5
 * mode:   KEYTYP_INT      16
 * uid:    KEYTYP_UINT64    574
*/
KEY *getdo_1(KEY *params)
{
  //static struct timeval TIMEOUT = { 180, 0 }; 
  static struct timeval TIMEOUT = { 4, 0 }; 
  static CLIENT *clresp;
  uint32_t tapeback;
  uint64_t uid, sunum;
  enum clnt_stat status;
  int reqcnt, i, offline, storeset, offcnt;
  char *call_err, *cptr, *wd;
  char tmpname[80];
  double bytes;

  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in getdo_1() is:\n");
    keyiterate(logkey, params);
  }
  }
  reqcnt = getkey_int(params, "reqcnt");
  sunum = getkey_uint64(params, "dsix_0");
  write_log("SUM_get() for user=%s sunum=%lu cnt=%d\n", 
		GETKEY_str(params, "username"), sunum, reqcnt);
  retlist=newkeylist();
  uid = getkey_uint64(params, "uid");
  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
    write_log("**Error: getdo_1() called with unopened uid=%lu\n", uid);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
  add_keys(params, &retlist);
  /* set up for response. sets current_client */
  if(!(clresp = set_client_handle(RESPPROG, (uint32_t)uid))) {
    freekeylist(&retlist);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
    /* in case call tape_svc pass on who to eventually respond to */
    setkey_fileptr(&retlist, "current_client", (FILE *)clresp);
    if(DS_DataRequest(params, &retlist)) { /*get SU info & do any TOUCH */
      freekeylist(&retlist);
      rinfo = 1;	/* give err status back to original caller */
      send_ack();	/* ack original sum_svc caller */
      clnt_destroy(clresp);
      return((KEY *)1);	/* nothing more to be sent */
    }
    /* param says if one or more are offline and need to be retrieved */
    if(offline = getkey_int(retlist, "offline")) {  
      offcnt = 0;
      for(i=0; i < reqcnt; i++) {
        sprintf(tmpname, "online_status_%d", i);
        cptr = GETKEY_str(retlist, tmpname);
        if(strcmp(cptr, "N") == 0) {        /* this su offline */
          offcnt++;
          sprintf(tmpname, "bytes_%d", i);
          bytes = getkey_double(retlist, tmpname);
          storeset = JSOC;         /* always use JSOC set for now */
          if((status=SUMLIB_PavailGet(bytes,storeset,uid,0,&retlist))) {
            write_log("***Can't alloc storage for retrieve uid = %lu\n", uid);
            freekeylist(&retlist);
            rinfo = 1;  /* give err status back to original caller */
            send_ack();	/* ack original sum_svc caller */
            clnt_destroy(clresp);
            return((KEY *)1);  /* error. nothing to be sent */
          }
          wd = GETKEY_str(retlist, "partn_name");
          sprintf(tmpname, "rootwd_%d", i);
          setkey_str(&retlist, tmpname, wd);
          write_log("\nAlloc for retrieve wd = %s for sumid = %lu\n", wd, uid);
        }
      }
      setkey_int(&retlist, "offcnt", offcnt);
      /* call tape_svc and tell our caller to wait */
      if(debugflg) {
        write_log("Calling tape_svc to bring data unit online\n");
      }
      rinfo = RESULT_PEND;  /* now tell caller to wait for results */
      status = clnt_call(clnttape,READDO, (xdrproc_t)xdr_Rkey, (char *)retlist, 
			(xdrproc_t)xdr_uint32_t, (char *)&tapeback, TIMEOUT);
      if(status != RPC_SUCCESS) {
          if(status != RPC_TIMEDOUT) {
            rinfo = 1;
            send_ack();
            call_err = clnt_sperror(clnttape, "Error clnt_call for READDO");
            write_log("%s %d %s\n", datestring(), status, call_err);
            return((KEY *)1);
          } else {
            write_log("%s timeout ignored in getdo_1()\n", datestring());
          }
      }
      if(tapeback == 1) {
        rinfo = 1;
        send_ack();
        write_log("**Error in READDO call to tape_svc in sum_svc_proc.c\n");
        return((KEY *)1);
      }
      send_ack();
    }
    else {
      rinfo = 0;	/* eveything is ok and here are the results */
      send_ack();       /* ack original sum_svc caller */
    }
    if(offline) {
      return((KEY *)1);	/* let tape_svc answer. dont destroy current_client */
    }
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    return(retlist);    /* send ans now */
}

/* Called when a client does a SUM_alloc() or SUM_alloc2() call to get storage. 
 * A typical keylist is:
 * USER:   	   KEYTYP_STRING   production
 * REQCODE:        KEYTYP_INT      6
 * DEBUGFLG:       KEYTYP_INT      1
 * SUNUM:	   KEYTYP_UINT64   6260386 (unique to SUM_alloc2() call)
 * uid:    KEYTYP_UINT64    574
 * reqcnt: KEYTYP_INT      1
 * storeset:       KEYTYP_INT      0
 * bytes:  KEYTYP_DOUBLE           1.200000e+08
*/
KEY *allocdo_1(KEY *params)
{
  int storeset, status, reqcnt;
  uint64_t uid;
  uint64_t sunum = 0;
  double bytes;
  char *wd;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in allocdo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  if(findkey(params, "SUNUM")) {	//this is a SUM_alloc2() call
    sunum = getkey_uint64(params, "SUNUM");
  }
  uid = getkey_uint64(params, "uid");
  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
    write_log("**Error: allocdo_1() called with unopened uid=%lu\n", uid);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
  retlist = newkeylist();
  add_keys(params, &retlist);		/* NOTE:does not do fileptr */
  reqcnt = getkey_int(params, "reqcnt"); /* will always be 1 */
  bytes = getkey_double(params, "bytes");
  storeset = getkey_int(params, "storeset");
  if(!(status=SUMLIB_PavailGet(bytes, storeset, uid, sunum, &retlist))) {
    wd = GETKEY_str(retlist, "partn_name");
    write_log("Alloc bytes=%e wd=%s for user=%s sumid=%lu\n", bytes, wd,
		GETKEY_str(retlist, "USER"), uid);
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { /*set up for response*/
      freekeylist(&retlist);
      rinfo = 1;  /* give err status back to original caller */
      send_ack();
      return((KEY *)1);  /* error. nothing to be sent */
    }
    /* put in sum_partn_alloc as DARW entry with commit */
    if(NC_PaUpdate(wd, uid, bytes, DARW, 0, NULL, 0, 0, 0, 1, 1)) { 
      write_log("WARN: can't make DARW entry in sum_partn_alloc for\n");
      write_log("      allocated storage. Proceed anyway...\n");
    }
    rinfo = 0;
    send_ack();
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    return(retlist);		/* return the ans now */
  }
  rinfo = status;		/* ret err code back to caller */
  send_ack();
  return((KEY *)1);		/* nothing will be sent later */
}

/* Called when a client does a SUM_info() call to get sum_main info. 
 * A typical keylist is:
 * USER:   	   KEYTYP_STRING   production
 * REQCODE:        KEYTYP_INT      13
 * DEBUGFLG:       KEYTYP_INT      1
 * SUNUM:	   KEYTYP_UINT64   6260386 
 * uid:    KEYTYP_UINT64    574
*/
KEY *infodo_1(KEY *params)
{
  int status;
  uint64_t uid;
  uint64_t sunum = 0;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in infodo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  sunum = getkey_uint64(params, "SUNUM");
  uid = getkey_uint64(params, "uid");
  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
    write_log("**Error: infodo_1() called with unopened uid=%lu\n", uid);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
  retlist = newkeylist();
  add_keys(params, &retlist);		/* NOTE:does not do fileptr */

  if(!(status=SUMLIB_InfoGet(sunum, &retlist))) {
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { /*set up for response*/
      freekeylist(&retlist);
      rinfo = 1;  /* give err status back to original caller */
      send_ack();
      return((KEY *)1);  /* error. nothing to be sent */
    }
    rinfo = 0;
    send_ack();
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    return(retlist);		/* return the ans now */
  }
  rinfo = status;		/* ret err code back to caller */
  send_ack();
  return((KEY *)1);		/* nothing will be sent later */
}

/* Called when a client does a SUM_put() to catalog storage units.
 * Can only put a single SU at a time (reqcnt = 1). Typical call is:
 * wd_0:   KEYTYP_STRING   /SUM1/D1695
 * dsix_0: KEYTYP_UINT64    1695
 * REQCODE:        KEYTYP_INT      7
 * DEBUGFLG:       KEYTYP_INT      1
 * bytes:  KEYTYP_DOUBLE              1.200000e+08
 * storage_set:    KEYTYP_INT      0
 * group:  KEYTYP_INT      65
 * username:       KEYTYP_STRING   production
 * history_comment: KEYTYP_STRING   this is a dummy history comment 
 * dsname: KEYTYP_STRING   hmi_lev1_fd_V
 * reqcnt: KEYTYP_INT      1
 * tdays:  KEYTYP_INT      5
 * mode:   KEYTYP_INT      1
 * uid:    KEYTYP_UINT64    886
*/
KEY *putdo_1(KEY *params)
{
  int status;
  uint64_t uid;
  char sysstr[128];
  char *cptr, *wd;
  double dsize;

  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in putdo_1() is:\n");
    keyiterate(logkey, params);
  }
  }
  uid = getkey_uint64(params, "uid");
  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
    write_log("**Error: putdo_1() called with unopened uid=%lu\n", uid);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
  wd = getkey_str(params, "wd_0");
  retlist = newkeylist();
  add_keys(params, &retlist);
  dsize = du_dir(wd);
  setkey_double(&retlist, "bytes", dsize);
  if(!(status=SUM_Main_Update(retlist))) {
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { /*set up for response*/
      freekeylist(&retlist);
      rinfo = 1;  /* give err status back to original caller */
      send_ack();
      return((KEY *)1);  /* error. nothing to be sent */
    }
    /* change to owner production, no group write */
    cptr = GETKEY_str(params, "wd_0");
    sprintf(sysstr, "sudo chmod -R go-w %s; sudo chown -Rf production %s", 
			cptr, cptr);
    /*sprintf(sysstr, "sudo /home/jim/cvs/JSOC/scripts/sutest.pl %s", cptr);*/
    write_log("%s\n", sysstr);
    if(system(sysstr)) {
        write_log("**Warning: Error on: %s\n", sysstr);
    }
    rinfo = 0;			/* status back to caller */
    send_ack();
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    return(retlist);
  }
  rinfo = status;		/* error status back to caller */
  send_ack();
  freekeylist(&retlist);
  return((KEY *)1);		/* nothing but status back */
}

/* Typical keylist is:
 * REQCODE:        KEYTYP_INT      3
 * DEBUGFLG:       KEYTYP_INT      1
 * USER:   	   KEYTYP_STRING   production
 * uid:    KEYTYP_UINT64    574
*/
KEY *closedo_1(KEY *params)
{
  uint64_t uid;

  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in closedo_1() is:\n");
    keyiterate(logkey, params);
  }
  }
  uid = getkey_uint64(params, "uid");
  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
    write_log("**Error: closedo_1() called with unopened uid=%lu\n", uid);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
  remsumopened(&sumopened_hdr, (uint32_t)uid); /* rem from linked list */
  rinfo = SUMLIB_Close(params);
  write_log("SUMLIB_Close for user=%s uid=%lu\n",
		GETKEY_str(params, "USER"), getkey_uint64(params, "uid"));
  send_ack();
  return((KEY *)1);	/* nothing will be sent later */
}

/* Called by delete_series doing a SUM_delete_series() call with all
 * the SUMS storage units (i.e. ds_index) that are associated with
 * the series about to be deleted.
 * Typical keylist is:
 * DEBUGFLG:       KEYTYP_INT      1
 * USER:   	   KEYTYP_STRING   production
 * sunum_0:    KEYTYP_UINT64    574
 * sunum_1:    KEYTYP_UINT64    575
 * etc. with last one value = 0
*/
KEY *delseriesdo_1(KEY *params)
{
  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {	//this can be very long. don't do for now
    //write_log("!!Keylist in delseriesdo_1() is:\n");
    //keyiterate(logkey, params);
  }
  }
  rinfo = 0;
  write_log("DELSERIESDO for user=%s\n", GETKEY_str(params, "USER"));
  send_ack();
  /* set DB sum_partn_alloc to DADP/DADPDELSU */
  SUMLIB_DelSeriesSU(params);
  return((KEY *)1);	/* nothing will be sent later */
}


/* This is called from the tape_svc by:
 * clnt_call(clntsum,SUMRESPDO,xdr_result,result,xdr_void,0,TIMEOUT);
 * where   clntsum = clnt_create(thishost, SUMPROG, SUMVERS, "tcp");
 * The tape operation has completed. A typical keylist is:
 * filenum:	KEYTYP_INT	0
 * tapemode:	KEYTYP_INT	1
 * cmd1:	KEYTYP_STRING	mtx -f /dev/sg12 load 8 3 1> /tmp/mtx_robot_3.log 2>&1
 * snum:	KEYTYP_INT	7
 * dnum:	KEYTYP_INT	3
 * dsix_2:	KEYTYP_UINT64	2233
 * dsix_1:	KEYTYP_UINT64	2232
 * dsix_0:	KEYTYP_UINT64	2231
 * username:	KEYTYP_STRING	production
 * REQCODE:	KEYTYP_INT	4
 * DEBUGFLG:	KEYTYP_INT	1
 * reqcnt:	KEYTYP_INT	3
 * tdays:	KEYTYP_INT	5
 * mode:	KEYTYP_INT	16
 * uid:	KEYTYP_UINT64	43043
 * current_client:	KEYTYP_FILEP	6455248
 * online_status_0:	KEYTYP_STRING	Y
 * archive_status_0:	KEYTYP_STRING	Y
 * bytes_0:	KEYTYP_DOUBLE	           1.200000e+08
 * create_sumid_0:	KEYTYP_UINT64	623
 * ds_index_0:	KEYTYP_UINT64	2231
 * wd_0:	KEYTYP_STRING	/SUM0/D2917/D2231
 * tapeid_0:	KEYTYP_STRING	000001S1
 * tapefilenum_0:	KEYTYP_INT	1
 * online_status_1:	KEYTYP_STRING	Y
 * archive_status_1:	KEYTYP_STRING	Y
 * bytes_1:	KEYTYP_DOUBLE	           1.200000e+08
 * create_sumid_1:	KEYTYP_UINT64	623
 * ds_index_1:	KEYTYP_UINT64	2232
 * wd_1:	KEYTYP_STRING	/SUM0/D2918/D2232
 * tapeid_1:	KEYTYP_STRING	000002S1
 * tapefilenum_1:	KEYTYP_INT	1
 * online_status_2:	KEYTYP_STRING	N
 * archive_status_2:	KEYTYP_STRING	Y
 * bytes_2:	KEYTYP_DOUBLE	           1.200000e+08
 * create_sumid_2:	KEYTYP_UINT64	623
 * ds_index_2:	KEYTYP_UINT64	2233
 * wd_2:	KEYTYP_STRING	/SUM0/D7844/D2233
 * tapeid_2:	KEYTYP_STRING	000003S1
 * tapefilenum_2:	KEYTYP_INT	1
 * offline:	KEYTYP_INT	1
 * ds_index:	KEYTYP_UINT64	7844
 * partn_name:	KEYTYP_STRING	/SUM0/D7844
 * rootwd_2:	KEYTYP_STRING	/SUM0/D7844
 * offcnt:	KEYTYP_INT	1
 * OP:	KEYTYP_STRING	rd
 * sim:	KEYTYP_INT	0
 * STATUS:	KEYTYP_INT	0
 * reqofflinenum:	KEYTYP_INT	2
*/
KEY *sumrespdo_1(KEY *params)
{
  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in sumrespdo_1() is:\n");
    keyiterate(logkey, params);
  }
  }
  retlist = newkeylist();
  add_keys(params, &retlist);
  current_client = (CLIENT *)getkey_fileptr(params, "current_client");
  rinfo = 0;
  send_ack();
  return(retlist);
}


void write_time()
{
  struct timeval tvalr;
  struct tm *t_ptr;
  char datestr[32];

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(datestr, "%s", asctime(t_ptr));
  write_log("**** %s", datestr);
}
