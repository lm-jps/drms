/* sum_svc_proc.c: Server side procedures
 *
 * These are the function that are called by sum_svc when it gets a 
 * message from a client. 
*/
#include <SUM.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <soi_key.h>
#include <soi_error.h>
#include <sum_rpc.h>

#include "serverdefs.h"
#include "drmssite_info.h"

extern int errno;
extern int write_log(const char *fmt, ...);
extern void StartTimer(int n);
extern float StopTimer(int n);
extern CLIENT *current_client, *clnttape, *clnttape_old;
extern SVCXPRT *glb_transp;
extern uint32_t rinfo;
extern uint32_t sumprog;
extern uint32_t sumvers;
extern int debugflg;
extern int rrid;
extern float ftmp;
extern char logname[];
extern char jsoc_machine[];
static int NO_OPEN = 0;
static char callername[MAX_STR];
static char nametmp[80];
static int numSUM;

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
SUMOPENED *sumopened_hdr = NULL; /* linked list of active opens*/

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
/*********************************NOOP out 14Jun2010*****************
//get sending host IP and port#
socklen_t len;
struct sockaddr_storage addr;
char ipstr[INET6_ADDRSTRLEN];
int port;
len = sizeof addr;
getpeername(sock, (struct sockaddr*)&addr, &len);
// deal with both IPv4 and IPv6:
if (addr.ss_family == AF_INET) {
    struct sockaddr_in *s = (struct sockaddr_in *)&addr;
    port = ntohs(s->sin_port);
    inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof ipstr);
} else { // AF_INET6
    struct sockaddr_in6 *s = (struct sockaddr_in6 *)&addr;
    port = ntohs(s->sin6_port);
    inet_ntop(AF_INET6, &s->sin6_addr, ipstr, sizeof ipstr);
}
write_log("Peer IP/port: %s/%d  Caller:%s\n", ipstr, port, callername);
sprintf(callername, "<none>");	//eventually elim callername everywhere
*********************************NOOP out 14Jun2010*****************/

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
    write_log("Successful SUMLIB_Open id=%d for user=%s uid=%d\n",rrid,user,rinfo);
    //Elim setsumopened/getsumopened after 30Aug2010 build
    //setsumopened(&sumopened_hdr, rinfo, NULL, user); /*put in list of opens*/

  }
  send_ack();				/* ack original sum_svc caller */
  return((KEY *)1);			/* nothing will be sent later */
}

/* Called by the SUM API SUM_open() making a clnt_call to sum_svc for the
 * CONFIGDO procedure. Returns the number of sum servers in the ack back to the 
 * caller, or 0 if error. Called:
 * db_name:        KEYTYP_STRING   hmidbX
 * USER:           KEYTYP_STRING   production
 *NOTE: the db_name is not used yet.
*/
KEY *configdo_1(KEY *params)
{
  char *user;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in configdo_1() is:\n");
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
  numSUM = SUM_NUMSUM;          //in base/drms/apps/serverdefs.h;
  rinfo = (uint32_t)numSUM;
  if(rinfo) {
    write_time();
    write_log("Successful CONFIGDO id=%d for user=%s numSUM=%d\n",rrid,user,rinfo);
  }
  send_ack();				/* ack original sum_svc caller */
  return((KEY *)1);			/* nothing will be sent later */
}

/* Called by the SUM API SUM_repartn() in order to get this sums process
 * to reread the sum_partn_avail DB table. (Used after a change to the
 * sum_partn_avail DB table.)
 * Returns 0 if error. Called:
 * USER:           KEYTYP_STRING   production
 * DEBUGFLG:	   KEYTYPE_INT	   0
 * uid:		   KEYTYP_UINT64   574
*/
KEY *repartndo_1(KEY *params)
{
  int stat;

  stat = DS_PavailRequest2();
  if(!stat) {
    write_time();
    write_log("Successful reread of sum_partn_avail DB table\n");
    rinfo = 0;
  }
  else {
    write_log("Unsuccessful reread of sum_partn_avail DB table\n");
    rinfo = 1;
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
  //static struct timeval TIMEOUT = { 8, 0 }; 
  //must be >40 used for tape_svc response to sum_svc
  static struct timeval TIMEOUT = { 50, 0 }; 
  static CLIENT *clresp;
  uint32_t tapeback;
  uint64_t uid, sunum;
  enum clnt_stat status;
  int reqcnt, i, offline, storeset, offcnt;
  char *call_err, *cptr, *wd;
  char scmd[96], errstr[80];
  double bytes;

  sprintf(callername, "getdo_1");	//!!TEMP
  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in getdo_1() is:\n");
    keyiterate(logkey, params);
  }
  }
  reqcnt = getkey_int(params, "reqcnt");
  sunum = getkey_uint64(params, "dsix_0");
  if(sunum > LLONG_MAX) {	// > 0x7fffffffffffffff
    write_log("**Error: getdo_1() called with sunum > LLONG_MAX uid=%lu\n", uid);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
  //write_log("SUM_get() id=%d for user=%s sunum=%lu cnt=%d\n", 
  //		rrid, GETKEY_str(params, "username"), sunum, reqcnt);
  retlist=newkeylist();
  uid = getkey_uint64(params, "uid");
//  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
//    write_log("**Error: getdo_1() called with unopened uid=%lu\n", uid);
//    rinfo = 1;	/* give err status back to original caller */
//    send_ack();	/* ack original sum_svc caller */
//    return((KEY *)1);	/* error. nothing to be sent */ 
//  }
  add_keys(params, &retlist);
  /* set up for response. sets current_client */
  if(!(clresp = set_client_handle(RESPPROG, (uint32_t)uid))) {
    write_log("SUM_get() id=%d for user=%s sunum=%lu cnt=%d uid=%lu\n**Error: getdo_1() can't set_client_handle for response\n", 
  		rrid, GETKEY_str(params, "username"), sunum, reqcnt, uid);
    //write_log("**Error: getdo_1() can't set_client_handle for response\n");
    freekeylist(&retlist);
    rinfo = 1;	/* give err status back to original caller */
    send_ack();	/* ack original sum_svc caller */
    return((KEY *)1);	/* error. nothing to be sent */ 
  }
    /* in case call tape_svc pass on who to eventually respond to */
    setkey_fileptr(&retlist, "current_client", (FILE *)clresp);
    if(DS_DataRequest(params, &retlist)) { /*get SU info & do any TOUCH */
      write_log("SUM_get() id=%d for user=%s sunum=%lu cnt=%d uid=%lu\n**Error: DS_DataRequest() returns error\n", 
  		rrid, GETKEY_str(params, "username"), sunum, reqcnt, uid);
      freekeylist(&retlist);
      rinfo = 1;	/* give err status back to original caller */
      send_ack();	/* ack original sum_svc caller */
      clnt_destroy(clresp);
      return((KEY *)1);	/* nothing more to be sent */
    }
    offline = getkey_int(retlist, "offline");
    write_log("SUM_get() id=%d for user=%s sunum=%lu cnt=%d uid=%lu rtrv=%d\n", 
	rrid, GETKEY_str(params, "username"), sunum, reqcnt, uid, offline);
    /* param says if one or more are offline and need to be retrieved */
    if(offline) {  
      //ck if old code that has bug with tape rds (e.g. show_info)
      if(!findkey(params, "newflg")) {	//this is old code 
        rinfo = 0;	  /* indicate error through STATUS below */
        send_ack();       /* ack original sum_svc caller */
        setkey_int(&retlist, "STATUS", 1);   /* give error back to caller */
        sprintf(errstr, "Caller is old DRMS version. No tape read allowed. Remake your code\n");
        setkey_str(&retlist, "ERRSTR", errstr);
        write_log("No tape read for old DRMS version. id=%d for user=%s sunum=%lu cnt=%d uid=%lu\n", 
	rrid, GETKEY_str(params, "username"), sunum, reqcnt, uid);
        return(retlist);    /* send ans now */
      }
      offcnt = 0;
      for(i=0; i < reqcnt; i++) {
        sprintf(nametmp, "online_status_%d", i);
        cptr = GETKEY_str(retlist, nametmp);
        //proceed if this su is offline and archived
        if(strcmp(cptr, "N") == 0) {
          sprintf(nametmp, "archive_status_%d", i);
          cptr = GETKEY_str(retlist, nametmp);
          if(strcmp(cptr, "Y") == 0) {
            offcnt++;
            sprintf(nametmp, "bytes_%d", i);
            bytes = getkey_double(retlist, nametmp);
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
            //elim sticky bit so don't need to do sum_chmown() after read
            sprintf(scmd, "chmod g-s %s", wd);
            if(system(scmd)) {
                write_log("**Warning: Error on: %s. errno=%d\n", scmd,errno);
            }
            sprintf(nametmp, "rootwd_%d", i);
            setkey_str(&retlist, nametmp, wd);
            write_log("\nAlloc for retrieve wd = %s for sumid = %lu\n", wd, uid);
          }
        }
      }
      setkey_int(&retlist, "offcnt", offcnt);
      /* call tape_svc and tell our caller to wait */
      if(debugflg) {
        write_log("Calling tape_svc to bring data unit online\n");
      }
      //check if sum_svc was disconnected from tape_svc. see tapereconnectdo_1()
      if(clnttape == 0) {
        rinfo = 2;		//tell drms tape unit was taken offline
        send_ack();
        freekeylist(&retlist);
        write_log("**Error in READDO to tape_svc. sum_svc has been disconnected\n");
        return((KEY *)1);
      }
      rinfo = RESULT_PEND;  /* now tell caller to wait for results */
      tapeback = 0;
      //tell tape_svc which sums process to respond to
      setkey_uint32(&retlist, "SPROG", sumprog);
      setkey_uint32(&retlist, "SVERS", sumvers);
      status = clnt_call(clnttape,READDO, (xdrproc_t)xdr_Rkey, (char *)retlist, 
			(xdrproc_t)xdr_uint32_t, (char *)&tapeback, TIMEOUT);
      if(status != RPC_SUCCESS) {
          if(status != RPC_TIMEDOUT) {
            //rinfo = 1;
            rinfo = 5;		//tell drms that tape_svc is dead
            send_ack();
            call_err = clnt_sperror(clnttape, "Error clnt_call for READDO");
            write_log("%s %d %s\n", datestring(), status, call_err);
            freekeylist(&retlist);
            return((KEY *)1);
          } else {
            write_log("%s timeout ignored in getdo_1()\n", datestring());
          }
      }
      if(tapeback == 1) {
        rinfo = 1;
        send_ack();
        freekeylist(&retlist);
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
      freekeylist(&retlist);
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
 * group:          KEYTYP_INT      10000
 * SUNUM:	   KEYTYP_UINT64   6260386 (unique to SUM_alloc2() call)
 * uid:    KEYTYP_UINT64    574
 * reqcnt: KEYTYP_INT      1
 * storeset:       KEYTYP_INT      0
 * bytes:  KEYTYP_DOUBLE           1.200000e+08
*/
KEY *allocdo_1(KEY *params)
{
  int status, reqcnt, group;
  int storeset = 0;
  uint64_t uid;
  uint64_t sunum = 0;
  double bytes;
  char *wd;

  sprintf(callername, "allocdo_1");	//!!TEMP
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
#if defined(SUMS_MULTIPLE_PARTNSETS) && SUMS_MULTIPLE_PARTNSETS
  /* This def is defined in serverdefs.h (all Stanford-specific settings are in this header). 
   * It is NOT part of the localization files/codes/scripts (i.e., config.local knows nothing
   * about it). Therefore, no localization site will be able to take advantage of this
   * multiple-sums-sets. If in the future a remote sites wants this feature, we will need 
   * to modify config.local (add SUMS_MULTIPLE_PARTNSETS), gen_init.csh, create_sums_tables.sql
   * (the block of code requires a new sums db table) and possibly other files/code/scripts. 
   *
   * ART 2011-5-17
   */
  if(!findkey(params, "group")) {	//use storeset 0 if no group
    storeset = 0;
  }
  else {				//query sum_arch_group db table
    group = getkey_int(params, "group");
    status = SUMLIB_SumsetGet(group, &storeset); //default storeset is 0
    if(!status) {
      write_log("Error ret from SUMLIB_SumsetGet() for group=%d, user=%s\n",
                 group, GETKEY_str(params, "USER"));
    }
  }
#endif
  uid = getkey_uint64(params, "uid");
//  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
//    write_log("**Error: allocdo_1() called with unopened uid=%lu\n", uid);
//    rinfo = 1;	/* give err status back to original caller */
//    send_ack();	/* ack original sum_svc caller */
//    return((KEY *)1);	/* error. nothing to be sent */ 
//  }
  retlist = newkeylist();
  add_keys(params, &retlist);		/* NOTE:does not do fileptr */
  reqcnt = getkey_int(params, "reqcnt"); /* will always be 1 */
  bytes = getkey_double(params, "bytes");
  //storeset = getkey_int(params, "storeset"); //obsolete
  if(!(status=SUMLIB_PavailGet(bytes, storeset, uid, sunum, &retlist))) {
    wd = getkey_str(retlist, "partn_name");
    write_log("Alloc bytes=%e id=%d wd=%s for user=%s sumid=%lu\n", bytes, rrid, wd,
		GETKEY_str(retlist, "USER"), uid);
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { /*set up for response*/
      write_log("Alloc Error Can't set client handle id=%d user=%s sumid=%lu\n", rrid, GETKEY_str(retlist, "USER"), uid);
      free(wd);
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
    free(wd);
    return(retlist);		/* return the ans now */
  }
  rinfo = status;		/* ret err code back to caller */
  send_ack();
  freekeylist(&retlist);
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

  sprintf(callername, "infodo_1");	//!!TEMP
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in infodo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  sunum = getkey_uint64(params, "SUNUM");
  uid = getkey_uint64(params, "uid");
  write_log("SUM_Info() for user=%s sunum=%lu\n", 
		GETKEY_str(params, "username"), sunum);
//  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
//    write_log("**Error: infodo_1() called with unopened uid=%lu\n", uid);
//    rinfo = 1;	/* give err status back to original caller */
//    send_ack();	/* ack original sum_svc caller */
//    return((KEY *)1);	/* error. nothing to be sent */ 
//  }
  retlist = newkeylist();
  add_keys(params, &retlist);		/* NOTE:does not do fileptr */

  if(!(status=SUMLIB_InfoGet(sunum, &retlist))) {
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { /*set up for response*/
      freekeylist(&retlist);
      /*rinfo = 1;  /* give err status back to original caller */
      rinfo = SUM_RESPPROG_ERR;
      send_ack();
      return((KEY *)1);  /* error. nothing to be sent */
    }
    rinfo = 0;
    send_ack();
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    return(retlist);		/* return the ans now */
  }
  rinfo = status;		/* ret err code 1 back to caller */
  freekeylist(&retlist);
  if(!drmssite_sunum_is_local(sunum))
    rinfo = SUM_SUNUM_NOT_LOCAL; // else this error code 
  send_ack();
  return((KEY *)1);		/* nothing will be sent later */
}

/* Called when a client does a SUM_infoArray() call to get sum_main info. 
 * This uses a Sunumarray arg structure instead of the usual keylist.
*/
KEY *infodoArray_1(Sunumarray *params)
{
  int reqcnt, i, status, filemode;
  double bytes = 10000000.0;
  char *partname, *effective_date;
  char filename[128], cmd[92];
  //char *filename = "/home/production/junk/infoarray.ans"; //!!TEMP

  reqcnt = params->reqcnt;
  retlist = newkeylist();
  write_log("SUM_infoArray() id=%d for user=%s uid=%lu 1st sunum=%lu cnt=%d\n", 
	rrid, params->username, params->uid, *params->sunums, reqcnt);
  if((status=SUMLIB_PavailGet(bytes,0,params->uid,0,&retlist))) {
    write_log("Can't alloc storage for %s in infodoArray_1()\n", params->username);
    freekeylist(&retlist);
    rinfo = status;		/* ret err code back to caller */
    send_ack();
    return((KEY *)1);	/* nothing will be sent later */
  } 
  partname = getkey_str(retlist, "partn_name");
  sprintf(cmd, "chmod g-w %s", partname); //don't let K. see this a dangling
  if(system(cmd)) {
    write_log("Error in infodoArray_1(): %s\n", cmd);
  }
  sprintf(filename, "%s/infoarray.ans", partname);
  effective_date = (char *)get_effdate(0);

  //make the alloc dir del pending
  NC_PaUpdate(partname, params->uid, bytes, DADP, DAAEDDP, effective_date, 
		0, 0, getkey_uint64(retlist, "ds_index"), 1, 0);
  free(partname);
  free(effective_date);
  freekeylist(&retlist);
  if(!(status=SUMLIB_InfoGetArray(params, filename, &filemode))) {
    if(!(set_client_handle(RESPPROG, (uint32_t)params->uid))) { /*set up for response*/
      write_log("**Error: infodoArray_1() can't set_client_handle for response\n");
      rinfo = SUM_RESPPROG_ERR;
      send_ack();
      return((KEY *)1);  /* error. nothing to be sent */
    }
    rinfo = 0;
    send_ack();
    retlist = newkeylist();
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    setkey_str(&retlist, "FILE", filename);
    setkey_int(&retlist, "filemode", filemode);
    setkey_uint64(&retlist, "uid", params->uid);
    setkey_int(&retlist, "reqcnt", reqcnt);
    return(retlist);		/* return the ans now */
  }
  rinfo = status;		/* ret err code 1 back to caller */
  send_ack();
  return((KEY *)1);	/* nothing will be sent later */
}

/* Called when a client does a SUM_infoEx() call to get sum_main info. 
 * A typical keylist is:
 * dsix_1: KEYTYP_UINT64    285
 * dsix_0: KEYTYP_UINT64    282
 * username:       KEYTYP_STRING   production
 * REQCODE:        KEYTYP_INT      4
 * DEBUGFLG:       KEYTYP_INT      1
 * reqcnt: KEYTYP_INT      2
 * uid:    KEYTYP_UINT64    574
*/
KEY *infodoX_1(KEY *params)
{
  int status, reqcnt;
  uint64_t uid;
  uint64_t sunum = 0;
  sprintf(callername, "infodoX_1");	//!!TEMP
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in infodoX_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  sunum = getkey_uint64(params, "dsix_0");
  reqcnt = getkey_int(params, "reqcnt"); 
  uid = getkey_uint64(params, "uid");
//  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
//    write_log("**Error: infodoX_1() called with unopened uid=%lu\n", uid);
//    rinfo = 1;	/* give err status back to original caller */
//    send_ack();	/* ack original sum_svc caller */
//    return((KEY *)1);	/* error. nothing to be sent */ 
//  }
  write_log("SUM_infoEx() id=%d for user=%s 1st sunum=%lu cnt=%d\n",
                rrid, GETKEY_str(params, "username"), sunum, reqcnt);
  retlist = newkeylist();
  add_keys(params, &retlist);		/* NOTE:does not do fileptr */
  if(!(status=SUMLIB_InfoGetEx(params, &retlist))) {
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { /*set up for response*/
      write_log("**Error: infodoX_1() can't set_client_handle for response\n");
      freekeylist(&retlist);
      /*rinfo = 1;  /* give err status back to original caller */
      rinfo = SUM_RESPPROG_ERR;
      send_ack();
      return((KEY *)1);  /* error. nothing to be sent */
    }
    rinfo = 0;
    send_ack();
    setkey_int(&retlist, "STATUS", 0);   /* give success back to caller */
    return(retlist);		/* return the ans now */
  }
  rinfo = status;		/* ret err code 1 back to caller */
  //if(!drmssite_sunum_is_local(sunum))
  //  rinfo = SUM_SUNUM_NOT_LOCAL; // else this error code 
  send_ack();
  freekeylist(&retlist);
  return((KEY *)1);		/* nothing will be sent later */
}

/* Called when a client does a SUM_put() to catalog storage units.
 * Can put multiple SU at a time. Typical call is:
 * wd_0:   KEYTYP_STRING   /SUM1/D1695
 * dsix_0: KEYTYP_UINT64    1695
 * wd_1:   KEYTYP_STRING   /SUM0/D1696
 * dsix_1: KEYTYP_UINT64    1696
 * REQCODE:        KEYTYP_INT      7
 * DEBUGFLG:       KEYTYP_INT      0
 * storage_set:    KEYTYP_INT      0
 * group:  KEYTYP_INT      65
 * username:       KEYTYP_STRING   production
 * history_comment: KEYTYP_STRING   this is a dummy history comment 
 * dsname: KEYTYP_STRING   hmi_lev1_fd_V
 * reqcnt: KEYTYP_INT      2
 * tdays:  KEYTYP_INT      5
 * mode:   KEYTYP_INT      1
 * uid:    KEYTYP_UINT64    886
*/
KEY *putdo_1(KEY *params)
{
  int status, i, reqcnt;
  uint64_t sunum = 0;
  uint64_t uid;
  char sysstr[128];
  char *cptr, *wd;

  sprintf(callername, "putdo_1");	//!!TEMP
  if(findkey(params, "DEBUGFLG")) {
  debugflg = getkey_int(params, "DEBUGFLG");
  if(debugflg) {
    write_log("!!Keylist in putdo_1() is:\n");
    keyiterate(logkey, params);
  }
  }
  uid = getkey_uint64(params, "uid");
  reqcnt = getkey_int(params, "reqcnt");
  sunum = getkey_uint64(params, "dsix_0");
  write_log("SUM_put() id=%d for user=%s 1st sunum=%lu reqcnt=%d\n", 
		rrid, GETKEY_str(params, "username"), sunum, reqcnt);
//  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
//    write_log("**Error: putdo_1() called with unopened uid=%lu\n", uid);
//    rinfo = 1;	/* give err status back to original caller */
//    send_ack();	/* ack original sum_svc caller */
//    return((KEY *)1);	/* error. nothing to be sent */ 
//  }
  retlist = newkeylist();
  add_keys(params, &retlist);
  //First change to owner production, no group write. This must succeed 
  //all or nothing as far as the caller is concerned.
    for(i=0; i < reqcnt; i++) { //!!!TBD
      sprintf(nametmp, "wd_%d", i);
      cptr = GETKEY_str(params, nametmp);
      sprintf(nametmp, "%s.chmown",  logname);
      sprintf(sysstr, "%s/sum_chmown %s 1>> %s 2>&1",
                        SUMBIN_BASEDIR, cptr, nametmp);
      //write_log("%s\n", sysstr);
      //StartTimer(3);          //!!TEMP
      if(system(sysstr)) {
          write_log("**Warning: Error on: %s. errno=%d\n", sysstr,errno);
          rinfo = 1;                    /* error back to caller */
          send_ack();
          freekeylist(&retlist);
          return((KEY *)1);             /* nothing but status back */
      }
      //ftmp = StopTimer(3);
      //write_log("#END: sum_chmown() %fsec\n", ftmp);    //!!TEMP for test
    }
 
  //now insert in the sums tables 
  if(!(status=SUM_Main_Update(params, &retlist))) {
    if(!(set_client_handle(RESPPROG, (uint32_t)uid))) { //set up for response
      freekeylist(&retlist);
      rinfo = 1;  // give err status back to original caller/
      send_ack();
      return((KEY *)1);  // error. nothing to be sent
    }
    rinfo = 0;			// status back to caller
    send_ack();
    setkey_int(&retlist, "STATUS", 0);   // give success back to caller
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
//  if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
//    write_log("**Error: closedo_1() called with unopened uid=%lu\n", uid);
//    rinfo = 1;	/* give err status back to original caller */
//    send_ack();	/* ack original sum_svc caller */
//    return((KEY *)1);	/* error. nothing to be sent */ 
//  }
  remsumopened(&sumopened_hdr, (uint32_t)uid); /* rem from linked list */
  rinfo = SUMLIB_Close(params);
  write_log("SUM_close for user=%s uid=%lu\n",
		GETKEY_str(params, "USER"), getkey_uint64(params, "uid"));
  send_ack();
  return((KEY *)1);	/* nothing will be sent later */
}

/* DRMS typically uses this to see if sum_svc is alive.
 * If DRMS gets an ack back, it know that sum_svc is alive.
 * Typical keylist is:
 * REQCODE:        KEYTYP_INT      16
 * DEBUGFLG:       KEYTYP_INT      1
 * USER:   	   KEYTYP_STRING   production
 * uid:    KEYTYP_UINT64    574
 *
 *Returns 0 in rinfo if both sum_svc and tape_svc are ok.
 *1 = tape_svc call timeout
 *5 = tape_svc not there
*/
KEY *nopdo_1(KEY *params)
{
  //struct timeval NOPTIMEOUT = { 8, 0 };
  struct timeval NOPTIMEOUT = { 15, 0 };
  uint64_t uid;
  KEY *klist;
  char *call_err;
  char *usr;
  int ans;
  enum clnt_stat status;

  //usr = getkey_str(params, "USER");
  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in nopdo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  rinfo = 0;
  uid = getkey_uint64(params, "uid");
/********************************************************************
  //ck if a sum_svc to tape_svc reconnect call was made (done by
  //tape_svc_restart to start a new tape_svc). If so then return
  //an error 5 for no original tape_svc that was connnected to this sum_svc.
  //This will cause drms to make an extra sum_get() after a tape_svc restart,
  //but this an unlikely occurence and the price to get resynced.
  if(!clnttape_old) {
    rinfo = 5;
    clnttape_old = clnttape;
  }
  else {
***************************************************************/
/***************************************************************
//!!NOTE: if call tape_svc do we need to set up current_client here and pass it along
    rinfo = 1;
    klist = newkeylist();
    setkey_uint64(&klist, "uid", uid);
    setkey_str(&klist, "USER", GETKEY_str(params, "USER") );
    //tell tape_svc which sums process to respond to
    setkey_uint32(&klist, "SPROG", sumprog);
    setkey_uint32(&klist, "SVERS", sumvers);
    status = clnt_call(clnttape, TAPENOPDO, (xdrproc_t)xdr_Rkey, (char *)klist,
                        (xdrproc_t)xdr_void, (char *)&ans, NOPTIMEOUT);
    rinfo = (int)ans;

    // NOTE: Must honor the timeout here as get the ans back in the ack
    if(status != RPC_SUCCESS) {
        rinfo = 5;
        call_err = clnt_sperror(clnttape, "Err clnt_call for TAPENOPDO");
        write_log("%s %s status=%d\n", datestring(), call_err, status);
    }
  //}
****************************************************************/
  write_log("SUM_nop for user=%s uid=%lu\n",
		GETKEY_str(params, "USER"), uid);
  send_ack();
  return((KEY *)1);	/* nothing will be sent later */
}

/* This is called by the tape_svc_restart program to tell sum_svc
 * to reconnect to a new tape_svc that has been started.
 * Eventually the stop of the old tape_svc and the start of a new
 * one can be done here with ssh calls to d02. But for now,
 * we will require that the user stops and starts a tape_svc.
 * How to do this will be detailed when tape_svc_restart is run.
 * Typical keylist is:
 * ACTION:         KEYTYP_STRING   close/reconnect
 * USER:   	   KEYTYP_STRING   production
 * HOST:   	   KEYTYP_STRING   d02
*/
KEY *tapereconnectdo_1(KEY *params)
{
  uint64_t uid;
  char *cptr;
  char *user;

  rinfo = 0;
  user = getkey_str(params, "USER");
  write_log("Tape reconnect for user=%s action=%s\n", user, GETKEY_str(params, "ACTION"));
  cptr = GETKEY_str(params, "ACTION");
  if(!strcmp(cptr, "close")) {
    write_log("sum_svc is closing on tape_svc about to be restarted\n");
    if(clnttape) clnt_destroy(clnttape);
    clnttape = 0;
  }
  else if(!strcmp(cptr, "reconnect")) {
    //connect to tape_svc
    clnttape_old = NULL;	//let nopdo_1() know that the old handle is NG
    clnttape = clnt_create(TAPEHOST, TAPEPROG, TAPEVERS, "tcp");
    if(!clnttape) {       /* server not there */
      clnt_pcreateerror("Can't get client handle to tape_svc (from sum_svc)");
      write_log("Cannot connect to new tape_svc on d02\n");
      rinfo = 1;
      //exit(1);
    }
    write_log("sum_svc has reconnected to restarted tape_svc\n");
  }
  else {  
    write_log("Illegal action = %s sent to tapereconnectdo_1()\n", cptr);
    rinfo = -1;
  }
  send_ack();
  free(user);
  return((KEY *)1);	/* nothing will be sent later */
}

/* Called by delete_series doing a SUM_delete_series() call with all
 * the SUMS storage units (i.e. ds_index) that are associated with
 * the series about to be deleted. The sunums are in the given file.
 * Typical keylist is:
 * DEBUGFLG:       KEYTYP_INT      1
 * USER:   	   KEYTYP_STRING   production
 * FILE:    	KEYTYP_STRING /SUM1/D99999/filename.sunums
 * SERIESNAME:  KEYTYP_STRING su_production.test
*/
KEY *delseriesdo_1(KEY *params)
{
  char *filename, *seriesname;

  if(findkey(params, "DEBUGFLG")) {
    debugflg = getkey_int(params, "DEBUGFLG");
    if(debugflg) {
      write_log("!!Keylist in delseriesdo_1() is:\n");
      keyiterate(logkey, params);
    }
  }
  rinfo = 0;
  filename = getkey_str(params, "FILE");
  seriesname = getkey_str(params, "SERIESNAME");
  write_log("DELSERIESDO for user=%s\n", GETKEY_str(params, "USER"));
  send_ack();
  /* set DB sum_partn_alloc to DADP/DADPDELSU */
  SUMLIB_DelSeriesSU(filename, seriesname);
  free(filename); free(seriesname);
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
  uint64_t uid;

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
  if(findkey(params, "uid")) {	//there s/b a uid in the keylist
    uid = getkey_uint64(params, "uid");
    if(!getsumopened(sumopened_hdr, (uint32_t)uid)) {
      write_log("**Error: sumrespdo_1() called with unopened uid=%lu\n", uid);
      write_log("**Force current_client = 0 so don't respond to nonexistent user\n");
      current_client = 0;
    }
  }
  else {
    write_log("**No uid found in keylist in sumrespdo_1()??\n");
  }
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
