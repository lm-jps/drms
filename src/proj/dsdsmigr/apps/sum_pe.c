/*
 * sum_pe - When an old MDI pe/peq type program gets a DRMS type ds name
 *           it will call sum_pe_svc. The sum_pe_svc spawns us (sum_pe) which
 *	runs with a drms_server and is given the keylist that the peq 
 *	generated. The sum_pe  gets the wd of the ds and returns
 *	it to sum_pe_svc which returns it to the original caller.
 * Sample call:
 *	sum_pe server=d00 keyfile=/tmp/keylist_10871857.log jsoc
 */

#include "jsoc_main.h"
#include "drms.h"
#include "drms_names.h"
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
  {ARG_STRING, "server", "", "host with sum_pe_svc"},
  {ARG_STRING, "keyfile", "", "file with peq keylist"},
  {ARG_STRING, "logdir", "/usr/local/logs/SUM", "dir for log file"},
  {ARG_FLAG, "v", "0", "verbose flag"},
  {ARG_FLAG, "h", "0", "help - print usage info"},
  {ARG_END}
};

void logkey();
KEY *getsumpe(KEY *params);
struct timeval TIMEOUT = { 20, 0 };

static KEY *retlist;            /* must be static for svc dispatch rte */
char *module_name = "sum_pe";
char *logdir, *db, *keyfile, *server;
char datestr[32];
char logname[MAX_STR];
int pid;
uint32_t rinfo;
FILE *logfp;
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
int write_log(const char *fmt, ...)
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
  pid = getpid();
  sprintf(logname, "%s/sum_pe_%d.log", logdir, pid);
  if(open_log(logname)) return(1);
  printk_set(write_log, write_log);
  printk("%s\nStarting sum_pe for database = %s\nserver=%s\nkeyfile = %s\nlogfile = %s\n\n", datestring(), db, server, keyfile, logname);
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
    printf ("Usage:\nsum_pe [-v] server=hostname keyfile=file "
	"[logdir=<dir for log file>] dbname\n"
        "  details are:\n"
	"  -v: verbose mode\n"
        "server=host where sum_pe_svc runs\n"
        "keyfile=filename containing peq generated keylist\n"
	"logdir=dir to put the log file in\n"
        "dbname is the DRMS data base to connect to, e.g. jsoc\n");
    return(1);
    }
  return (0);
}

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



/* Module main function. */
int DoIt(void) {
  int status = 0;
  uint32_t sumpeback;
  char *call_err;
  KEY *list=newkeylist();
  CLIENT *clntsumpesvc;

  /* Get command line arguments */
  /*int verbose = cmdparams_get_int (&cmdparams, "v", NULL);*/
  int usage = cmdparams_get_int (&cmdparams, "h", NULL);
  logdir = cmdparams_get_str (&cmdparams, "logdir", NULL);
  server = cmdparams_get_str (&cmdparams, "server", NULL);
  keyfile = cmdparams_get_str (&cmdparams, "keyfile", NULL);
  if(nice_intro(usage)) return (0);
  if(cmdparams_numargs(&cmdparams) >= 1 && (db = cmdparams_getarg (&cmdparams, 1))) {
    /*printf("Starting sum_pe for database = %s.\n",db);*/
  }
  else {
    nice_intro(1);
    return(0);
  }
  if(setup()) return(1);
  if(file2keylist(keyfile, &list)) {	/* convert input file to keylist */
    printk("Error in file2keylist() for %s\n", keyfile);
    return(1);
  }
  keyiterate(logkey, list);            /* !!!TEMP */
  /* Create client handle used for calling the sum_pe_svc */
  clntsumpesvc = clnt_create(server, SUMPEPROG, SUMPEVERS, "tcp");
  if(!clntsumpesvc) {                 /* server not there */
    clnt_pcreateerror("Can't get client handle to sum_pe_svc");
    printk("sum_pe_svc not there on %s\n", server);
    return(1);
  }

  retlist = getsumpe(list);
  /* now send answer back to sum_pe_svc */
    status = clnt_call(clntsumpesvc,SUMPEACK, (xdrproc_t)xdr_Rkey, 
	(char *)retlist, (xdrproc_t)xdr_uint32_t, (char *)&sumpeback, TIMEOUT);
    if(status != RPC_SUCCESS) {
      call_err = clnt_sperror(clntsumpesvc, "Err clnt_call for SUMPEACK");
      printk("%s %s\n", datestring(), call_err);
      if(status != RPC_TIMEDOUT) {
        return(status);
      }
    }
    if(sumpeback != 0) {
      printk("Error status= %d from clnt_call to SUMPEACK = %d\n", sumpeback);
      status = 1;
    }

  return(0);
}

/* Called to get a SUMS wd for a dataset.
 * The input keylist is the  normal expansion keylist of the pe/peq call. 
 * For example:
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
 * This routine will then query the drms for the datasets and return the 
 * answer keylist to to calling sum_pe_svc which will return it to the 
 * original peq.
*/
KEY *getsumpe(KEY *params)
{
  int reqcnt;
  int status;
  DRMS_RecordSet_t *recordset;
  DRMS_Record_t *rec;
  int xdirflg = 0;
  int first_rec, last_rec, nrecs, irec, retrieve_flg, i;
  char name[80], value[80], cmd[80], xdir[128], errmsg[128];
  char path[DRMS_MAXPATHLEN];
  /*char *in = "ds_mdi.fd_V_01h_lev1_8[121903-121943]"; /* !!TEMP */
  char *in, *cptr;
  FILE *infile;

  /*printk("!!Keylist in sumpedo_1() is:\n");	/* !!!TEMP */
  /*keyiterate(logkey, params);*/
  retlist = newkeylist();
  add_keys(params, &retlist);           /* NOTE:does not do fileptr */
  setkey_fileptr(&retlist, "current_client", getkey_fileptr(params, "current_client"));
  setkey_int(&retlist, "REQCODE", PEPEQRESPDO);
  reqcnt = getkey_int(params, "in_nsets");
  for(i=0; i < reqcnt; i++) {
    in = getkey_str(params, "in");
    /* if special ds that has the extra dir due to the import script.. */
    if(strstr(in, "ds_mdi.fd_V_01h_lev1_8") || 
	strstr(in, "ds_mdi.fd_V_30s_01h_lev1_8") ||
	strstr(in, "ds_mdi.fd_M_96m_01d_lev1_8") ||
	strstr(in, "ds_mdi.vw_V_06h_lev1_8") ||
        strstr(in, "ds_mdi.fd_M_01h_lev1_8")) {
      xdirflg = 1;
    }
    retrieve_flg = getkey_int(params, "retrieve_flg");
    printk("%s\nretrieve_flg in sumpedo_1 = %d\n", datestring(), retrieve_flg);
    /* Open record_set */
    recordset = drms_open_records (drms_env, in, &status);
    if (status) {
      printk("drms_open_records failed, in=%s, status=%d.\n", in, status);
      setkey_int(&retlist, "STATUS", 1); /* tell orig caller error */
      sprintf(errmsg, "Err: see on sum host: %s\n", logname);
      setkey_str(&retlist, "ERRMSG", errmsg);
      return(retlist);  
    }
    /* recordset now points to a struct with  count of records found ("n"), 
     * and a pointer to an array of record pointers ("records");
    */
    nrecs = recordset->n;
    printk("#of records in %s = %d\n", in, nrecs); /* !!!TEMP */
    if (nrecs == 0) {
      printk ("** No records in selected data set **\n");
      setkey_int(&retlist, "STATUS", 1); /* tell orig caller error */
      sprintf(errmsg, "** No records in selected data set **\n");
      setkey_str(&retlist, "ERRMSG", errmsg);
      return(retlist);  
    }
    last_rec = nrecs - 1;
    first_rec = 0;
    for (irec = first_rec; irec <= last_rec; irec++) {
      rec = recordset->records[irec];  /* pointer to current record */
      /*drms_print_query_rec(rec);	/* !!!TEMP */
      drms_record_directory (rec, path, retrieve_flg);
      if(xdirflg && strcmp(path, "")) {/* special ds with the extra dir */
        sprintf(cmd, "/bin/ls %s", path);
        if(!(infile = popen(cmd, "r"))) {
          printk("Can't do popen() for %s\n", cmd);
          setkey_int(&retlist, "STATUS", 1); /* tell orig caller error */
          sprintf(errmsg, "Err: Can't do popen() for %s\n", cmd);
          setkey_str(&retlist, "ERRMSG", errmsg);
          return(retlist);  
        }
        if(!fgets(xdir, 128, infile)) {
          printk("Can't get the extra dir for special ds\n");
          setkey_int(&retlist, "STATUS", 1); /* tell orig caller error */
          sprintf(errmsg, "Err: Can't get the extra dir for special ds\n");
          setkey_str(&retlist, "ERRMSG", errmsg);
          return(retlist);  
        }
        if(cptr = rindex(xdir, '\n')) *cptr = NULL; /* elim term. CR */
        sprintf(path, "%s/%s", path, xdir);
        pclose(infile);
      }
      sprintf(name, "in_%d_wd", irec);
      setkey_str(&retlist, name, path);
      printk("%s %s\n", name, path);
      sprintf(name, "inname_%d", irec);
      sprintf(value, "in_%d", irec);
      setkey_str(&retlist, name, value);
      if(!strcmp(path, "")) {
        sprintf(name, "status_%d", irec);
        setkey_int(&retlist, name, DS_ARCHIVE);
      }
      else {
        sprintf(name, "status_%d", irec);
        setkey_int(&retlist, name, 0);
      }
    }
    drms_close_records(recordset, DRMS_FREE_RECORD);
  }
  setkey_int(&retlist, "in_nsets", nrecs);
  setkey_int(&retlist, "STATUS", 0);
  return(retlist);  
}
