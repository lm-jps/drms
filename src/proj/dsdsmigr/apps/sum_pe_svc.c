/*
 * sum_pe_svc - When an old MDI pe/peq type program gets a DRMS type ds name
 *	it will call sum_pe_svc to get the SUMS dir. sum_pe_svc will fork
 *	off a sum_pe for each request to run w/a DRMS server and get the data 
 *	from DRMS and send the answer back to us and we send it to the 
 *	original peq caller.
 *
 */

#include "jsoc.h"
#include "cmdparams.h"
#include <SUM.h>
#include <soi_error.h>
#include <sys/errno.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <signal.h>
#include <sum_rpc.h>
#include <printk.h>

ModuleArgs_t module_args[] =
{ 
  {ARG_STRING, "logdir", "/usr/local/logs/SUM", "dir for log file"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;

CmdParams_t cmdparams;

static void sumpeprog_1(struct svc_req *rqstp, SVCXPRT *transp);
void logkey(KEY *key);
struct timeval TIMEOUT = { 20, 0 };

static KEY *retlist;            /* must be static for svc dispatch rte */
char *module_name = "sum_pe_svc";
char *logdir, *db;
char thishost[MAX_STR];
char datestr[32];
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
    printk("*** %s sum_pe_svc got SIGTERM. Exiting.\n", datestring());
    exit(1);
  }
  if(sig == SIGINT) {
    printk("*** %s sum_pe_svc got SIGINT. Exiting.\n", datestring());
    exit(1);
  }
  printk("*** %s sum_pe_svc got an illegal signal %d, ignoring...\n",
                        datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}


int setup () {
  int pid, i;
  char *username, *cptr;
  char logname[MAX_STR], lfile[MAX_STR], acmd[MAX_STR], line[MAX_STR];
  char gfile[MAX_STR];
  FILE *fplog;

  if(!(username = (char *)getenv("USER"))) username = "nouser";
  if(strcmp(username, "production")) {
/***************************
    printf("!!NOTE: You must be user production to run sum_pe_svc!\n");
    return(1);
***************************/
  }
  gethostname(thishost, MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;
  pid = getpid();
  /* make sure only one sum_pe_svc runs */
  sprintf(gfile, "/tmp/grep_sum_pe_svc.%d.log", pid);
  sprintf(lfile, "/tmp/wc_sum_pe_svc.%d.log", pid);
  sprintf(acmd, "ps -ef | grep %s  1> %s 2>&1", "\" sum_pe_svc\"", gfile);
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
       printk("Can't run more than 1 sum_pe_svc\n");
       return(1);
     }
  }
  fclose(fplog);

  sprintf(logname, "%s/sum_pe_svc_%d.log", logdir, pid);
  if(open_log(logname)) return(1);
  printk_set(msg, msg);
  printf("Starting sum_pe_svc for database = %s\nlogfile = %s\n\n",db, logname);
  printk("%s\nStarting sum_pe_svc for database = %s\nlogfile = %s\n\n",
		datestring(), db, logname);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
  return(0);
}

int nice_intro(int usage) {
  if (usage)
    {
    printf ("Usage:\nsum_pe_svc [-hv] "
	"[logdir=<dir for log file>] db_name\n"
        "  details are:\n"
	"  -h: help - show this message then exit\n"
	"  -v: verbose mode\n"
	"logdir=dir to put the log file in\n"
	"dbname is the DRMS data base to connect to, e.g. jsoc\n");
    return(1);
    }
  return (0);
}

/**********************************************************
void drms_print_query_rec(DRMS_Record_t *rec)
  {
  int iprime, nprime;
  DRMS_Keyword_t *rec_key, *key, **prime_keys;
  printk("%s\n",rec->seriesinfo->seriesname);
  nprime = rec->seriesinfo->pidx_num;
  prime_keys = rec->seriesinfo->pidx_keywords;
  printk("!!!!TEMP in drms_print_query_rec()\n");
  if (nprime > 0)
    {
    for (iprime = 0; iprime < nprime; iprime++)
      {
      key = prime_keys[iprime];
      rec_key = drms_keyword_lookup (rec, key->info->name, 1);
      printk("[");
      if (key->info->type != DRMS_TYPE_STRING)
        drms_keyword_printval (rec_key);
      else
        {
        printk("\"");
        drms_keyword_printval (rec_key);
        printk("\"");
        }
    printk("]\n");
    }
  }
else
printk("[:#%lld]",rec->recnum);
}
***************************************************************/


/* Module main function. */
int main(int argc, char **argv)
{
  int status = 0;
/*  DRMS_RecordSet_t *recordset; */
/*  DRMS_Record_t *rec; */
  register SVCXPRT *transp;

  /* Get command line arguments */
  status = cmdparams_parse (&cmdparams, argc, argv);
  if (status == CMDPARAMS_QUERYMODE) {
    cmdparams_usage (argv[0]);
    return 0;
  }

  /*int verbose = cmdparams_get_int (&cmdparams, "v", NULL);*/
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  logdir = cmdparams_get_str (&cmdparams, "logdir", NULL);

  if(nice_intro(usage)) return (0);
  if(cmdparams_numargs(&cmdparams) >= 1 && (db = cmdparams_getarg (&cmdparams, 1))) {
    /*printf("Starting sum_pe_svc for database = %s.\n",db);*/
  }
  else {
    nice_intro(1);
    return(0);
  }
  if(setup()) return(1);

   /* register for client pe/peq programs to talk to us */
   (void) pmap_unset(SUMPEPROG, SUMPEVERS);
   transp = (SVCXPRT *)svctcp_create(RPC_ANYSOCK, 0, 0);
   if (transp == NULL) {
           printf("***cannot create tcp service\n");
           return(1);
   }
   if (!svc_register(transp, SUMPEPROG, SUMPEVERS, sumpeprog_1, IPPROTO_TCP)) {
           printf("***unable to register (SUMPEPROG, SUMPEVERS, tcp)\n");
           return(1);
   }
  /* Enter svc_run() which calls svc_getreqset when msg comes in.
   * svc_getreqset calls sumprog_1() to process the msg.
   * NOTE: svc_run() never returns.
  */
  svc_run();
  printk("!!Fatal Error: svc_run() returned in sum_pe_svc\n");
  return(1);
}

/* This is the dispatch routine that's called when the client does a
 * clnt_call() to SUMPEPROG, SUMPEVERS with these procedure numbers.
 * Called by svc_getreqset() in svc_run().
*/
static void
sumpeprog_1(struct svc_req *rqstp, SVCXPRT *transp)
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
        case SUMPEDO:
              xdr_argument = xdr_Rkey;
              xdr_result = xdr_Rkey;;
              local = (char *(*)()) sumpedo_1;
              break;
        case SUMPEACK:
              xdr_argument = xdr_Rkey;
              xdr_result = xdr_Rkey;;
              local = (char *(*)()) sumpeack_1;
              break;
        default:
              printk("**sumpeprog_1() dispatch default procedure %d,ignore\n", 
			rqstp->rq_proc);
              svcerr_noproc(transp);
              return;
        }
        bzero((char *)&argument, sizeof(argument));
        if (!svc_getargs(transp, (xdrproc_t)xdr_argument, (char *)&argument)) {
                msg("***Error on svc_getargs()\n");
                svcerr_decode(transp);
                /*return;*/
                /* NEW: 23May2002 don't return. Can result in caller getting: */
                /* Dsds_svc returned error code 5600 */
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
          clnt_stat=clnt_call(current_client, PEPEQRESPDO,(xdrproc_t)xdr_result,
                result, (xdrproc_t)xdr_void, 0, TIMEOUT);
          if(clnt_stat != 0) {
            clnt_perrno(clnt_stat);             /* outputs to stderr */
            msg("***Error on clnt_call() back to PEPEQRESPDO procedure\n");
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

KEY *sumpeack_1(KEY *params)
{
  KEY *retlist;
  pid_t pid;
  int status;

  rinfo = 0;
  send_ack();		/* to sum_pe who's about to exit anyway */
  pid = wait(&status);  /* clean up for this sum_pe */
  printk("Complete: sum_pe pid=%u\n", pid);
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  current_client = (CLIENT *)getkey_fileptr(params, "current_client");
  return(retlist);	/* give the list back to the original peq */
}

/* Called by an MDI pe/peq program run when a -S (SUMS) flag is given,
 * or the ds is not an MDI ds (i.e. no "prog:"). The input keylist is the 
 * normal expansion keylist of the pe/peq call. For example:
 * dsds_uid:       KEYTYP_LONG     9638582
 * arg_data_in_0:  KEYTYP_STRING   in
 * in_0_basename:  KEYTYP_STRING
 * in_0_wd:        KEYTYP_STRING   .
 * in_basename:    KEYTYP_STRING
 * in_wd:  KEYTYP_STRING   .
 * in_0_series_sn: KEYTYP_INT      60000
 * in_0_fmt:       KEYTYP_STRING   -1073754624
 * in_0_incr:      KEYTYP_INT      1
 * in_0_lsn:       KEYTYP_INT      -1
 * in_0_fsn:       KEYTYP_INT      0
 * in_0_data:      KEYTYP_STRING   prog:mdi,level:lev1.5,series:fd_V_01h[60000]
 * in_0_prog:      KEYTYP_STRING   mdi
 * in_0_level:     KEYTYP_STRING   lev1.5
 * in_0_series:    KEYTYP_STRING   fd_V_01h
 * in_0_series_range:      KEYTYP_STRING   60000
 * in_fmt: KEYTYP_STRING   -1073754624
 * in_incr:        KEYTYP_INT      1
 * in_lsn: KEYTYP_INT      -1
 * in_fsn: KEYTYP_INT      0
 * in_data:        KEYTYP_STRING   prog:mdi,level:lev1.5,series:fd_V_01h[60000]
 * in_prog:        KEYTYP_STRING   mdi
 * in_level:       KEYTYP_STRING   lev1.5
 * in_series:      KEYTYP_STRING   fd_V_01h
 * in_series_sn:   KEYTYP_INT      60000
 * in_series_range:        KEYTYP_STRING   60000
 * in_nsets:       KEYTYP_INT      1
 * in:     KEYTYP_STRING   prog:mdi,level:lev1.5,series:fd_V_01h[60000]
 *
 * This routine will fork a sum_pe which will then query the drms for the 
 * datasets and return the  answer keylist to us (sum_pe_svc) which returns 
 * it to the calling pe/peq which has registered a RESPPROG with the portmaster.
*/
KEY *sumpedo_1(KEY *params)
{
  static CLIENT *clresp;
  pid_t pid;
  char *args[5];

  uint64_t uid;
  char name[128], argkey1[80], argkey2[80];
  /*char *in = "ds_mdi.fd_V_01h_lev1_8[121903-121943]"; /* !!TEMP */

  uid = (uint64_t)getkey_long(params, "dsds_uid");
  sprintf(name, "/tmp/keylist_%lu.log", uid);
  /* first open a fp for write_log() call made by logkey to output keylist*/
  if((wlogfp=fopen(name, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s\n", name);
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    return((KEY *)1);  /* error. nothing to be sent */
  }
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  if(!(clresp = set_client_handle(PEPEQPROG, (uint32_t)uid))) { /*for resp*/
    freekeylist(&retlist);
    rinfo = 1;  /* give err status back to original caller */
    send_ack();
    return((KEY *)1);  /* error. nothing to be sent */
  }
  /* for sum_pe call, pass on who to eventually respond to */
  setkey_fileptr(&retlist, "current_client", (FILE *)clresp);
  keyiterate(logkey, retlist);		/* write to "name" file above */
  fclose(wlogfp);
  if((pid = fork()) < 0) {
    printk("***Can't fork() a sum_pe. errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printk("execvp of sum_pe server=%s keyfile=%s %s\n", thishost, name, db);
    sprintf(argkey1, "server=%s", thishost);
    sprintf(argkey2, "keyfile=%s", name);
    args[0] = "sum_pe";
      args[1] = argkey1;
      args[2] = argkey2;
      args[3] = db;
      args[4] = NULL;
    if(execvp(args[0], args) < 0) {
      write_log("***Can't execvp() sum_pe keyfile=%s. errno=%d\n", name, errno);
      exit(1);
    }
  }
  printk("Fork sum_pe pid=%u\n", pid);
  rinfo = 0;
  send_ack();                   /* ack original sum_pe_svc caller */
  return((KEY *)1);  
}
