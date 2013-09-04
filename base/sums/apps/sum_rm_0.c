/*-----------------------------------------------------------------------------
 * cvs/JSOC/base/sum/sum_rm.c 
 *-----------------------------------------------------------------------------
 *
 * This program is started by sum_svc when it starts. It will check the .cfg
 * file to find out how much to remove from the /SUM storage.
 *
 * NOTE: There is only one SUM_Set_Num (formerly pds_set_num) for SUMS.
 * All SUM partition are "local", i.e. on the NFS servers and the concept,
 * in DSDS, of a storage partition associated with a host name is gone.
 * Please don't try to resurrect the pds_set_num concept!!
 * 
 * sum_rm reads its configuration file, named like:
 * 	/home/jim/cvs/JSOC/base/sums/apps/data/sum_rm.cfg.<db>.
 * This file has parameters to configure the sum_rm run.
 * 
 *	#typical configuration file for sum_rm program
 *	#
 *	#when done, sleep for n seconds before re-running
 *	SLEEP=300
 *	#delete until this many Mb free on /SUM set 0.
 *	#NOTE: only one pds type set for SUMS.
 *	MAX_FREE_0=800000
 *	#% of each disk partition to be kept free 
 *	PART_PERCENT_FREE=5
 *	#log file (only opened at startup and pid gets appended to this name)
 *	LOG=/usr/local/logs/SUM/sum_rm.log
 *	#whom to bother when there's a notable problem
 *	MAIL=jim
 *	#to prevent sum_rm from doing anything set non-0
 *	NOOP=0
 *	#sum_rm can only be enabled for a single user
 *	USER=production
 *	#USER=jim
 *	#don't run sum_rm between these NORUN hours of the day (0-23)
 *	#comment out to ignore or set them both to the same hour
 *	#The NORUN_STOP must be >= NORUN_START
 *	#don't run when the hour first hits NORUN_START
 *	NORUN_START=7
 *	#start running again when the hour first hits NORUN_STOP
 *	NORUN_STOP=7
 *
 * sum_rm deletes the delete pending dirs in the sum_partn_alloc table
 * that have expired. The deletion is from the oldest effective_date forward.
 * The deletion continues until no more del pend dirs are available or until
 * PART_PERCENT_FREE is achieved for each disk partition.
 *
 * After sum_rm has deleted the given directories, it goes to
 * sleep for the n seconds given in the .cfg file. When it awakes, it starts
 * the cycle again by reading its .cfg file. This allows dynamic adjustment of
 * the free storage requirements.
 *
*/

#include <SUM.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <signal.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <printk.h>
#include <sys/statvfs.h>
#include "serverdefs.h"

#define CFG_FILE "/home/production/cvs/JSOC/base/sums/apps/data/sum_rm.cfg"
#define NEWLOG_FILE "/home/production/cvs/JSOC/base/sums/apps/data/sum_rm_0.newlog"

int stat_storage();
void get_cfg();
static char *datestring(void);
static int ponoff[MAX_PART];

char mod_name[] = "sum_rm";

extern PART ptab[];	/* defined in SUMLIB_PavailRequest.pgc */

FILE *logfp;
char thishost[MAX_STR];
char logname[MAX_STR];
char *dbname;		/* name of database to connect to */
char *timetag;		/* time tag for log file */
char *username;		/* name of current user from getenv() */
int active;		/* true if sum_rm in a delete cycle */
int datehr;		/* the current hour of the day 0-23 */
int num_sets_in_cfg;	/* num of MAX_FREE_ lines in the .cfg file */
int debugflg, sim;
struct timeval tvalr;
struct tm *t_ptr;
char datestr[32];
char *dptr;
int soi_errno = NO_ERROR;
int logvers = 0;


/* the following are set from the .cfg file */
int noopflg;		/* sum_rm does nothing if flg set */
int sleep_sec;		/* seconds to sleep after each cycle */
int norun_start;	/* don't run when the hour first hits this */
int norun_stop;		/* start running again when the hour first hits this */
char xlogfile[256];	/* log file name */
char mailto[256];	/* mail recipient(s) */
char userrun[256];	/* user name who can run sum_rm */
double max_free_set_need[MAX_PART];    // bytes to keep free for each SUM part
double max_free_set_current[MAX_PART]; // bytes now free for each SUM part
double max_free_set_percent[MAXSUMSETS];  // % to keep free for each % SUM set
//NOTE: everything assume only one set SUM_Set_Num 0


void open_log(char *filename)
{
  if((logfp=fopen(filename, "w")) == NULL) {
    fprintf(stderr, "Can't open the log file %s for %s on %s\n",
		filename, mod_name, thishost);
  }
}


/* Outputs the variable format message (re: printf) to the log file.
*/
int write_log(const char *fmt, ...)
{
  va_list args;
  char string[32768];

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

void sighandler(sig)
  int sig;
{
  if(sig == SIGTERM) {
    write_log("*** %s sum_rm got SIGTERM. Exiting.\n", datestring());
    DS_DisConnectDB();
    exit(1);
  }
  if(sig == SIGINT) {
    write_log("*** %s sum_rm got SIGINT. Exiting.\n", datestring());
    DS_DisConnectDB();
    exit(1);
  }
  write_log("*** %s sum_rm got an illegal signal %d, ignoring...\n",
                        datestring(), sig);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGALRM, SIG_IGN) != SIG_IGN)
      signal(SIGALRM, sighandler);
}

/* Called when get a SIGALRM every sleep_sec seconds.
*/
void alrm_sig(int sig)
{
  FILE *tstfp;
  char cmd[MAX_STR];
  int norunflg, update;
  double bytesdeleted, availstore;

  if((tstfp=fopen(NEWLOG_FILE, "r"))) {	//close current log & start new one
    fclose(tstfp);
    fclose(logfp);
    logvers++;
    sprintf(logname, "%s_0.%s.%d", xlogfile, timetag, logvers);
    open_log(logname);
    sprintf(cmd, "/bin/rm %s", NEWLOG_FILE);
    if(system(cmd)) {
      write_log("sum_rm_0: Can't execute %s.\n", cmd);
    }
  }
  write_log("Called alrm_sig()\n");
  signal(SIGALRM, &alrm_sig);	/* setup for alarm signal */
  if(!active) {
    active = 1;
    update = 0;
    get_cfg();			/* get any new config params */
    write_log("%s\n", dptr);	/* time stamp */
    if(noopflg) {
      write_log("NOOP\n");
    } else {
      norunflg = 0;
      if(norun_start <= datehr) {
        norunflg = 1;
      }
      if(norun_stop <= datehr) {
        norunflg = 0;
      }
      if(norunflg) {
        write_log("NORUN interval active\n");
      } else {
        if(strcmp(username, userrun)) {
          write_log("Current user %s is not the configured user %s\n",
		     username, userrun);
        } else {
          update = stat_storage();	/* update sum_partn_avail & rm */
        }
      }
    }
    active = 0;
  }
  DS_Rm_Commit();
  alarm(sleep_sec);		/* start the timeout again */
}


/* Do a df on each SUM partition and update the storage available in
 * the sum_partn_avail table.
 * And update max_free_set_need and max_free_set_current for ea partition.
 * And free up any storage needed for ea partition.
 * Return 1 if any thing was deleted from any partition
*/
int stat_storage()
{
  PART *pptr;
  int i, status;
  int updated = 0;
  int partnchange = 0;
  double df_avail, df_total, df_del, total, upercent;
  struct statvfs vfs;

  //for(i=0; i<MAX_PART-1; i++) {
  //for(i=0; i<11; i++) {		//for sum_rm_0 do 1st 11 partitions

  for(i=0; i<17; i++) {		//for sum_rm_0 do 1st 15 partitions
				//this help account for /SUM100s not used
    pptr=(PART *)&ptab[i];
    if(pptr->name == NULL) break;
    //skip the special partitions for permanent aia.lev1 (save DB time)
    if(!strcmp(pptr->name, "/SUM100")) continue;
    if(!strcmp(pptr->name, "/SUM101")) continue;
    if(!strcmp(pptr->name, "/SUM102")) continue;
    if(!strcmp(pptr->name, "/SUM103")) continue;

    //!!TEMP don't try to delete from these until disks are fixed 3/4/2013:
    //if(!strcmp(pptr->name, "/SUM3")) continue;
    //if(!strcmp(pptr->name, "/SUM16")) continue;
    //if(!strcmp(pptr->name, "/SUM17")) continue;

    if(status = statvfs(pptr->name, &vfs)) {
      printk("Error %d on statvfs() for %s\n",status,pptr->name);
    }
    else {
      df_total = (double)vfs.f_blocks;/* # blks total in fs */
      df_total = df_total * (double)vfs.f_frsize; /* times block size */
      df_avail = (double)vfs.f_bavail;/* #free blks avail to non su */
      df_avail = df_avail * (double)vfs.f_bsize; /* times block size */
      /* keep reserve - subtract 1% of total size from what's avail */
      //df_reserve = 0.01 * df_total;
      //if(df_reserve > df_avail) df_avail = 0.0;
      //else df_avail = df_avail - df_reserve;
      if(pptr->pds_set_num != -2) {	//don't touch a -2 partition
        upercent = df_avail/df_total;
        if(upercent < 0.02) {		//turn off partition at 98%
          if(ponoff[i] >= 0) {
            printk("Turning off full partition %s\n", pptr->name);
            SUMLIB_PavailOff(pptr->name); //no more allocation from this parti
            ponoff[i] = -1;
            partnchange = 1;		//must resync memory w/db
          }
        }
        else if(upercent >= 0.05) {	//ok to turn it back on 
            if(ponoff[i] == -1) {
              printk("Turning on former full partition %s\n", pptr->name);
              SUMLIB_PavailOn(pptr->name, ptab[i].pds_set_prime); //can alloc again
              ponoff[i] = 0;
              partnchange = 1;		//must resync memory w/db
            }
        }
      }
      if(SUMLIB_PavailUpdate(pptr->name, df_avail))
       printk("Err: SUMLIB_PavailUpdate(%s, %e, ...)\n",
                      pptr->name, df_avail);
      else
        //printk("%s Update free bytes to %e\n", pptr->name, df_avail);
        //#of bytes need free for this partition. Remember only one SUM set [0]
        max_free_set_need[i] = df_total * max_free_set_percent[0];
        max_free_set_current[i] = df_avail;
        if(max_free_set_current[i] < max_free_set_need[i]) { //del some stuff
          updated = 1;
          df_del = max_free_set_need[i] - max_free_set_current[i];
          printk("%s Attempt to del %e bytes\n", pptr->name, df_del);
          DS_RmDoX(pptr->name, df_del); 
        }
    }
  }
  if(partnchange) {
    if(system("sumrepartn -y")) {
      printk("Error on system() call to resync mem w/db\n");
    }
  }
  return(updated);
}

/* Get the CFG_FILE configuration file and set the global variables. 
 * It will contain parameters like:
 *         #when done sleep for n seconds before re-running
 *         SLEEP=60
 *         #delete until this many Mb free on /SUM set 0, 1, etc.
 *         #MAX_FREE_0=75000
 *         #% of each disk partition to be kept free
 *         PART_PERCENT_FREE=5
 *         #log file (only opened at startup and pid gets appended to this name)
 *         LOG=/usr/local/logs/SUM/sum_rm.log
 *         #whom to bother when there's a notable problem
 *         MAIL=jim
 *         #to prevent sum_rm from doing anything set non-0
 *         NOOP=0
 *         #sum_rm can only be enabled for a single user
 *         USER=production
 *	   #don't run sum_rm between these NORUN hours of the day
 *	   #comment out to ignore or set them both to the same hour
 *	   #don't run when the hour first hits NORUN_START
 *	   NORUN_START=7
 *	   #start running again when the hour first hits NORUN_STOP
 *	   NORUN_STOP=13
 * Default values are first set in case none given in the cfg file.
*/
void get_cfg()
{
  FILE *cfgfp;
  char *token;
  char line[256], cfgfile[256], datestrhr[16];
  int i, num;

  gettimeofday(&tvalr, NULL);
  t_ptr = localtime((const time_t *)&tvalr);
  sprintf(datestr, "%s", asctime(t_ptr));
  dptr = &datestr[4];                  /* isolate the mmm dd hh:mm:ss */
  datestr[19] = (char)NULL;
  strncpy(datestrhr, &datestr[11], 2); /* isolate the 2 digit hour */
  datestrhr[2] = (char)NULL;
  datehr = atoi(datestrhr);
  noopflg = 0;
  norun_start = norun_stop = 0;
  sleep_sec = 300;
  for(i=0; i < MAXSUMSETS; i++) { /* set default free bytes % */
    //max_free_set[i] = ((double)800000 * (double)1048576); /* 800Gb */
    max_free_set_percent[i] = 3.0/100.0;
  }
  num_sets_in_cfg = MAXSUMSETS;
  strcpy(xlogfile, "/tmp/sum_rm_0.log");
  strcpy(mailto, "sys2@solar2");
  strcpy(userrun, "production");
#ifdef __LOCALIZED_DEFS__
  sprintf (cfgfile, "%s/sum_rm.cfg", SUMLOG_BASEDIR);
#else
  sprintf(cfgfile, "%s.%s", CFG_FILE, dbname);
#endif
  if(!(cfgfp=fopen(cfgfile, "r"))) {
    write_log("Can't open config file %s. Using defaults...\n", cfgfile);
    return;
  }
  num_sets_in_cfg = 0;
  while(fgets(line, 256, cfgfp)) {	/* get cfg file lines */
    if(line[0] == '#' || line[0] == '\n') continue;
    if(strstr(line, "SLEEP=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        sleep_sec=atoi(token);
      }
    }
    else if(strstr(line, "PART_PERCENT_FREE")) {
      token=(char *)strtok(line, "=\n");
      //i=atoi(&line[18]);
      if(token=(char *)strtok(NULL, "\n")) {
        num=atoi(token);
        //max_free_set_percent[i] = num;
        //num_sets_in_cfg++;
        max_free_set_percent[0] = (double)num/100.0; //only one SUMS set
      }
    }
    else if(strstr(line, "LOG=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        strcpy(xlogfile, token);
      }
    }
    else if(strstr(line, "MAIL=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        strcpy(mailto, token);
      }
    }
    else if(strstr(line, "NOOP=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        noopflg=atoi(token);
      }
    }
    else if(strstr(line, "USER=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        strcpy(userrun, token);
      }
    }
    else if(strstr(line, "NORUN_START=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        norun_start=atoi(token);
      }
    }
    else if(strstr(line, "NORUN_STOP=")) {
      token=(char *)strtok(line, "=\n");
      if(token=(char *)strtok(NULL, "\n")) {
        norun_stop=atoi(token);
      }
    }
  }
  if(norun_stop < norun_start) {
    write_log("Error in config file %s\nNORUN_STOP < NORUN_START\n", cfgfile);
    write_log("Proceeding without NORUN interval...\n");
    norun_start = norun_stop = 0;
  }
  fclose(cfgfp);
  if(num_sets_in_cfg > MAXSUMSETS) {
    write_log("***Error in config file %s. Too many SUM sets\n", cfgfile);
    exit(1);
  }
}

void get_cmd(int argc, char *argv[])
{
  int c;

  while(--argc > 0 && (*++argv)[0] == '-') {
    while((c = *++argv[0]))
      switch(c) {
      case 'd':
        debugflg=1;     /* can also be sent in by client calls */
        break;
      case 's':
        sim=1;          /* simulation mode */
        break;
      default:
        break;
      }
  }
  if(argc != 2) {
    printf("\nERROR: sum_rm must be call with dbname and timestamp\n");
    exit(1);
  }
  else {
    dbname = argv[0];
    timetag = argv[1];
  }
}

/* Initial setup stuff called when sum_rm is first entered. 
*/
void setup()
{
  FILE *fplog;
  char cmd[MAX_STR], lfile[MAX_STR], line[MAX_STR];
  char target[80], gfile[MAX_STR];
  int i, pid;
  char *cptr;

  gethostname(thishost,MAX_STR);
  cptr = index(thishost, '.');       /* must be short form */
  if(cptr) *cptr = (char)NULL;
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  get_cfg();			/* get config info */
  pid = getppid();		/* pid of sum_svc */
  sprintf(logname, "%s_0.%s.%d", xlogfile, timetag, logvers);
  open_log(logname);
  printk_set(write_log, write_log);
  write_log("\n\n## %s for %s  pid = %d ##\n",
		mod_name, username, pid);
  if(!strcmp(username, userrun)) 
    write_log("You are configured as the active user\n");
  else
    write_log("You are NOT configured as the active user = %s\n", userrun);
  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
      signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
      signal(SIGTERM, sighandler);

  /* if another "sum_rm DB" is running, abort */
  sprintf(lfile, "/tmp/sum_rm.%d.log", pid);
  sprintf(target, " sum_rm %s", dbname);
/* !!!TEMP********/
  sprintf(gfile, "/tmp/cmdgrep.%d.log", pid);
  sprintf(cmd, "ps -ef | grep \"%s\" 1> %s", target, gfile);
  system(cmd);
/****END TMP *****/
  sprintf(cmd, "cat %s | wc -l 1> %s", gfile, lfile);
  write_log("cmd: %s\n", cmd);
  if(system(cmd)) {
    write_log("sum_rm: Can't execute %s.\n", cmd);
    exit(1);
  }
  if((fplog=fopen(lfile, "r")) == NULL) {
    write_log("sum_rm: Can't open cmd log file %s\n", lfile);
    exit(1);
  }
  while(fgets(line, 128, fplog)) {       /* get ps lines */
     i = atoi(line);
     if(i > 3)	{		/* count the sh and grep cmd too */
       write_log("Can't run more than 1 sum_rm for db=%s\n", dbname);
       exit(1);
     }
  }
  fclose(fplog);
  active = 0;
  DS_ConnectDB(dbname);		/* connect to DB for init */
  if(DS_PavailRequestEx())	/* get sum_partn_avail info in mem */
    exit(1);
  for(i=0; i < MAX_PART-1; i++) {
      //ponoff[i] = 0;		 //init the 98% off/ 95% on table
      ponoff[i] = ptab[i].pds_set_num;	 //init the 98% off/ 95% on table
  }
  signal(SIGALRM, &alrm_sig);	/* setup for alarm signal */
  alarm(2);			/* set up first alarm */
}

/* sum_rm is always started by a fork by sum_svc.
 * The calling seq is : sum_rm dbname.
 * Defaults to hmidbX db.
*/
int main(int argc, char *argv[])
{
  get_cmd(argc, argv);
  setup();
  while(1) {
    sleep(60);			/* this replaces the former svc_run */
  }
}

/* Return ptr to "mmm dd hh:mm:ss". */
static char *datestring(void)
{
  time_t t;
  char *str;

  t = time(NULL);
  str = ctime(&t);
  str[19] = 0;
  return str+4;          /* isolate the mmm dd hh:mm:ss */
}

