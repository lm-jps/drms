/*
 * sum_export_svc - When a user want to get remote SUMS data via an scp
 *	it will call sum_export_svc to get the remote SUMS dir. 
 *	sum_export_svc will fork off a sum_export for each request which 
 *	will get the data from the given host and then send the answer back 
 *	to sum_export_svc which then then send the answer 
 *	to the original sum_export_svc caller.
 *
 */

#include <stdlib.h>
#include <SUM.h>
#include <soi_error.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <signal.h>
#include <sum_rpc.h>
#include <printk.h>
#include "serverdefs.h"
 #include <sys/wait.h>

static void sumexprog_1(struct svc_req *rqstp, SVCXPRT *transp);
void logkey(KEY *key);
void logkeyk(KEY *key);
struct timeval TIMEOUT = { 20, 0 };

static KEY *retlist;            /* must be static for svc dispatch rte */
char thishost[MAX_STR];
char datestr[32];
char logname[MAX_STR];
uint32_t rinfo;
FILE *logfp, *wlogfp;
SVCXPRT *glb_transp;
CLIENT *current_client;

int open_log(char *filename)
{
  if((logfp=fopen(filename, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", filename);
    return(1);
  }
  return(0);
}

/* Outputs the variable format message (re: printf) to the log file.
*/
int msg(const char *fmt, ...)
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

/* Outputs the variable format message (re: printf) to the log file.
 * This is used by logkey() to output a keylist to a log file.
*/
int write_log(const char *fmt, ...)
{
  va_list args;
  char string[4096];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  if(wlogfp) {
    fprintf(wlogfp, string);
    fflush(wlogfp);
  }
  else
    fprintf(stderr, string);
  va_end(args);
  return(0);
}

/* Return ptr to "mmm dd hh:mm:ss". Uses global datestr[]. */
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

void sighandler(int sig)
{
  if(sig == SIGTERM) {
    printk("*** %s sum_export_svc got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    printk("*** %s sum_export_svc got SIGINT. Exiting.\n", datestring());
    exit(1);
  }
  printk("*** %s sum_export_svc got an illegal signal %d, ignoring...\n",
                        datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}


int setup () {
  int pid, i;
  char *username, *cptr;
  char lfile[MAX_STR], acmd[MAX_STR], line[MAX_STR];
  char gfile[MAX_STR];
  FILE *fplog;

  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
    //printf("!!NOTE: You must be user production to run sum_export_svc!\n");
    //return(1);
  }
  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;
  pid = getpid();
  /* make sure only one sum_export_svc runs */
  sprintf(gfile, "/tmp/grep_sum_export_svc.%d.log", pid);
  sprintf(lfile, "/tmp/wc_sum_export_svc.%d.log", pid);
  sprintf(acmd, "ps -ef | grep %s  1> %s 2>&1", "\" sum_export_svc\"", gfile);
  if(system(acmd)) {
    printf("**Can't execute %s.\n", acmd);
    return(1);
  }
  sprintf(acmd, "cat %s | wc -l 1> %s", gfile, lfile);
  if(system(acmd)) {
    printk("**Can't execute %s.\n", acmd);
    return(1);
  }
  if((fplog=fopen(lfile, "r")) == NULL) {
    printk("**Can't open cmd log file %s\n", lfile);
    return(1);
  }
  while(fgets(line, 128, fplog)) {       /* get ps lines */
     i = atoi(line);
     if(i > 3)  {               /* count the sh and grep cmd too */
       printk("Can't run more than 1 sum_export_svc\n");
       return(1);
     }
  }
  fclose(fplog);

  sprintf(logname, "%s/sum_export_svc_%d.log", SUMLOG_BASEDIR, pid);
  if(open_log(logname)) return(1);
  printk_set(msg, msg);
  printf("Starting sum_export_svc logfile = %s\n\n", logname);
  printk("%s\nStarting sum_export_svc logfile = %s\n",
		datestring(), logname);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
  return(0);
}


/* Module main function. */
int main(int argc, char **argv)
{
  int status = 0;
  register SVCXPRT *transp;

  if(setup()) return(1);

   /* register for client SUM_export() calls to talk to us */
   (void) pmap_unset(SUMEXPROG, SUMEXVERS);
   transp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
   if (transp == NULL) {
           printf("***cannot create tcp service\n");
           return(1);
   }
   if (!svc_register(transp, SUMEXPROG, SUMEXVERS, sumexprog_1, IPPROTO_TCP)) {
           printf("***unable to register (SUMEXPROG, SUMEXVERS, tcp)\n");
           return(1);
   }
  /* Enter svc_run() which calls svc_getreqset when msg comes in.
   * svc_getreqset calls sumprog_1() to process the msg.
   * NOTE: svc_run() never returns.
  */
  svc_run();
  printk("!!Fatal Error: svc_run() returned in sum_export_svc\n");
  return(1);
}

/* This is the dispatch routine that's called when the client does a
 * clnt_call() to SUMEXPROG, SUMEXVERS with these procedure numbers.
 * Called by svc_getreqset() in svc_run().
 * The client is a SUM_export() function call from a users program.
*/
static void
sumexprog_1(struct svc_req *rqstp, SVCXPRT *transp)
{
        union __svcargun {
                Rkey sumdo_1_arg;
        } argument;
        char *result, *call_err;
        enum clnt_stat clnt_stat;

        bool_t (*xdr_argument)(), (*xdr_result)();
        char *(*local)();

        switch (rqstp->rq_proc) {
        case NULLPROC:
              (void) svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
              return;
              break;
        case SUMEXDO:
              xdr_argument = xdr_Rkey;
              xdr_result = xdr_Rkey;;
              local = (char *(*)()) sumexdo_1;
              break;
        case SUMEXACK:
              xdr_argument = xdr_Rkey;
              xdr_result = xdr_Rkey;;
              local = (char *(*)()) sumexack_1;
              break;
        default:
              printk("**sumexprog_1() dispatch default procedure %d,ignore\n", 
			rqstp->rq_proc);
              svcerr_noproc(transp);
              return;
        }
        bzero((char *)&argument, sizeof(argument));
        if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
                msg("***Error on svc_getargs()\n");
                svcerr_decode(transp);
                /*return;*/
                /* NEW: 23May2002 don't return. */
                /* NEW: 10Jun2002 try this: */
                svc_sendreply(transp, (xdrproc_t)xdr_void, (char *)NULL);
                return;

        }
        glb_transp = transp;                 /* needed by function */
        result = (*local)(&argument, rqstp); /* call the function */
                                             /* sets current_client & rinfo*/
                                             /* ack sent back in the function*/

      if(result) {                      /* send the result now */
        if(result == (char *)1) {
          /* no client handle. do nothing, just return */
        }
        else {
          clnt_stat=clnt_call(current_client,REMSUMRESPDO,(xdrproc_t)xdr_result,
                result, (xdrproc_t)xdr_void, 0, TIMEOUT);
          if(clnt_stat != 0) {
            clnt_perrno(clnt_stat);             /* outputs to stderr */
            msg("***Error on clnt_call() back to REMSUMRESPDO procedure\n");
            msg("***The original client caller has probably exited\n");
            call_err = clnt_sperror(current_client, "Err");
            msg("%s\n", call_err);
          }
          clnt_destroy(current_client);
          freekeylist((KEY **)&result);
        }
      }
      else {
      }
      if (!svc_freeargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
        msg("**unable to free arguments\n");
        /*exit(1);*/
      }
      return;
}

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
    sock_in->sin_port = 0;      /* makes clnttcp_create consult yp */
    client = clnttcp_create(sock_in,prognum,versnum,&sock,0,0);
    if(!client) {
      clnt_pcreateerror("Can't do a clnttcp_create to send a response");
      printk("**** Can't do a clnttcp_create to send a response ****\n");
      printk("**** Did someone remove us from the portmapper? ****\n");
      return(0);                /* error ret */
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
    printk("***Error on immed ack back to client. FATAL???\n");
    svcerr_systemerr(glb_transp);
  }
}

KEY *sumexack_1(KEY *params)
{
  KEY *retlist;
  pid_t pid;
  int status;

  rinfo = 0;
  send_ack();		/* to sum_export who's about to exit anyway */
  pid = wait(&status);  /* clean up for this sum_export */
  printk("Complete: sum_export pid=%u\n", pid);
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  current_client = (CLIENT *)getkey_fileptr(params, "current_client");
  return(retlist);	/* give the list back to the original caller */
}

/* Called by a user program doing SUM_export() to get a remote SUMS
 * dir. The keylist has "host" for the target host of the scp,
 * src_0, dest_0, src_1, dest_1, etc. for the source and destination dirs.
 * reqcnt is the key for how many src/dest pairs there are. Also uid.
 *
 * This routine will fork a sum_export which will then do the scp
 * and return the  answer keylist to us (sum_export_svc) which returns 
 * it to the calling client which has registered a RESPPROG with the portmaster.
*/
KEY *sumexdo_1(KEY *params)
{
  static CLIENT *clresp;
  pid_t pid;
  char *args[5];
  uint32_t uid;
  char name[128], argkey1[80], argkey2[80];

  uid = getkey_uint32(params, "uid");
  sprintf(name, "/tmp/keylist_%u.log", uid);
  /* first open a fp for write_log() call made by logkey to output keylist*/
  if((wlogfp=fopen(name, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", name);
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    return((KEY *)1);  /* error. nothing to be sent */
  }
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  if(!(clresp = set_client_handle(REMSUMPROG, (uint32_t)uid))) { /*for resp*/
    freekeylist(&retlist);
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    return((KEY *)1);  /* error. nothing to be sent */
  }
  /* for sum_export call, pass on who to eventually respond to */
  setkey_fileptr(&retlist, "current_client", (FILE *)clresp);
  keyiterate(logkey, retlist);	/* write to "name" file above */
  fclose(wlogfp);

  printk("\nFork sum_export\n");
  printk("execvp of: sum_export server=%s keyfile=%s\n", thishost, name);
  keyiterate(logkeyk, retlist);	/* also write to sum_export_svc log file */
  if((pid = fork()) < 0) {
    printk("***Can't fork() a sum_export. errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    sprintf(argkey1, "%s", thishost);
    sprintf(argkey2, "%s", name);
    args[0] = "sum_export";
      args[1] = argkey1;
      args[2] = argkey2;
      args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() sum_export keyfile=%s. errno=%d\n", 
		name, errno);
      exit(1);
    }
  }
  rinfo = 0;
  send_ack();                   /* ack original sum_export_svc caller */
  freekeylist(&retlist);
  return((KEY *)1);  
}

void logkeyk(KEY *key)
{
switch(key->type) {
case KEYTYP_STRING:
  printk ("%s:\tKEYTYP_STRING\t%s\n", key->name, (char*)key->val);
  break;
case KEYTYP_BYTE:
  printk ("%s:\tKEYTYP_BYTE\t%d\n", key->name, *(char*)key->val);
  break;
case KEYTYP_INT:
  printk ("%s:\tKEYTYP_INT\t%d\n", key->name, *(int*)key->val);
  break;
case KEYTYP_FLOAT:
  printk ("%s:\tKEYTYP_FLOAT\t%13.6e\n", key->name, *(float*)key->val);
  break;
case KEYTYP_DOUBLE:
  printk ("%s:\tKEYTYP_DOUBLE\t%23.6e\n", key->name, *(double*)key->val);
  break;
case KEYTYP_TIME:
  printk ("%s:\tKEYTYP_TIME\t%23.6e\n", key->name, *(TIME*)key->val);
  break;
case KEYTYP_SHORT:
  printk ("%s:\tKEYTYP_SHORT\t%d\n", key->name, *(short*)key->val);
  break;
case KEYTYP_LONG:
  printk ("%s:\tKEYTYP_LONG\t%d\n", key->name, *(long*)key->val);
  break;
case KEYTYP_UBYTE:
  printk ("%s:\tKEYTYP_UBYTE\t%d\n", key->name, *(unsigned char*)key->val);
  break;
case KEYTYP_USHORT:
  printk ("%s:\tKEYTYP_USHORT\t%d\n", key->name, *(unsigned short*)key->val);
  break;
case KEYTYP_UINT:
  printk ("%s:\tKEYTYP_UINT\t%d\n", key->name, *(unsigned int*)key->val);
  break;
case KEYTYP_ULONG:
  printk ("%s:\tKEYTYP_ULONG\t%d\n", key->name, *(unsigned long*)key->val);
  break;
case KEYTYP_UINT64:
  printk ("%s:\tKEYTYP_UINT64\t%ld\n", key->name, *(uint64_t*)key->val);
  break;
case KEYTYP_UINT32:
  printk ("%s:\tKEYTYP_UINT32\t%d\n", key->name, *(uint32_t*)key->val);
  break;
case KEYTYP_FILEP:
  printk ("%s:\tKEYTYP_FILEP\t%ld\n", key->name, *(FILE **)key->val);
  break;
default:
  printk ("(void)\n");
}
}

