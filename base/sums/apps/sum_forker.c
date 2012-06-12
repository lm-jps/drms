/*-----------------------------------------------------------------------------
 * /home/production/cvs/JSOC/base/sum/apps/sum_forker.c
 *
 * This is a stand alone program that the sum_svc, running on j1,
 * calls on d02 in order to start the tape_svc and drive_svc and robot_svc
 * on the machine that has the T950 tape robot and drives.
 *
*/

#include <SUM.h>
#include <soi_key.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sum_rpc.h>
#include <soi_error.h>
#include <printk.h>

void usage();
void get_cmd(int argc, char *argv[]);

extern int errno;
char *dbname, *timetag;
int tapeoffline = 0;
int sim = 0;
rm_0_found = 0;
rm_1_found = 0;
rm_2_found = 0;

int send_mail(char *fmt, ...)
{
  va_list args;
  char string[1024], cmd[1024];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  /* !!TBD send to admin alias instead of jim */
  sprintf(cmd, "echo \"%s\" | Mail -s \"jmtx mail\" jim@sun.stanford.edu", string);
  system(cmd);
  va_end(args);
  return(0);
}

void usage()
{
  printf("This is a stand alone program that the sum_svc, running on j1,\n");
  printf("calls on d02 in order to start the tape_svc and drive_svc and \n");
  printf("robot_svc on the machine that has the T950 tape robot and drives.\n");
  printf("Usage: sum_forker -[o|s] dbname timetag\n");
  printf("       -o  = offline mode\n");
  printf("       -s  = simulation mode\n");
  printf("       dbname = name of database to connect to, e.g. jsoc_sums\n");
  printf("       timetag = time tag of form 2009.06.03.115824\n");
  exit(1);
}

/* Get and validate the calling sequence. */
void get_cmd(int argc, char *argv[])
{
  int c;

  while(--argc > 0 && (*++argv)[0] == '-') {
    while((c = *++argv[0]))
      switch(c) {
      case 'o':
        tapeoffline=1;
        break;
      case 's':
        sim=1;
        break;
      default:
        usage();
        break;
      }
  }
  if(argc != 2) {
    usage();
  }
  dbname = argv[0];
  timetag = argv[1];
  //printf("dbname = %s\n", dbname);
  //printf("timetag = %s\n", timetag);
}

/* Set flags for the sum_rm_[0,1,2] found still running.
 * Return 0 on success, else -1.
*/
int find_sum_rm()
{
  FILE *fplog;
  char acmd[128], line[128], look[64];
  char log[] = "/usr/local/logs/SUM/find_sum_rm.log";

  sprintf(acmd, "ps -ef | grep sum_rm_  1> %s 2>&1", log);
  if(system(acmd)) {
    printf("Can't execute %s.\n", acmd);
    return(-1);
  }
  if((fplog=fopen(log, "r")) == NULL) {
    printf("Can't open %s to find sum_rm_\n", log);
    return(-1);
  }
  sprintf(look, " %s", dbname);
  while(fgets(line, 128, fplog)) {       /* get ps lines */
     if (strstr(line, look) && strstr(line, "sum_rm_")) {
       if(!strstr(line, "sh ")) {
         if(strstr(line, "sum_rm_0")) rm_0_found = 1;
         else if(strstr(line, "sum_rm_1")) rm_1_found = 1;
         else if(strstr(line, "sum_rm_2")) rm_2_found = 1;
       }
     }
  }
  fclose(fplog);
  return (0);
}

int main(int argc, char *argv[])
{ 
  int i;
  pid_t pid;
  char *args[8];
  char dsvcname[80], pgport[32], cmd[128];

  get_cmd(argc, argv);                  /* check the calling sequence */
  if(find_sum_rm() == -1) {
    printf("Fatal Error, quit\n");
    exit(1);
  }

  //printf("!!TEMP sum_forker just exits for now\n");
  //exit(0); //!!!TEMP

      sprintf(pgport, SUMPGPORT);
      setenv("PGPORT", pgport, 1); //need to connect to new jsoc_sums db
  //The sum_stop_d02_tape should already have been called by sum_stop_j1
  //or sum_stop_j1_auto. Make sure old stuff is stopped.
  //sprintf(cmd, "/home/production/cvs/JSOC/base/sums/scripts/sum_stop_d02_tape");
  //printf("%s\n", cmd);
  //if(system(cmd)) {
  //  printf("Error on system(%s)\n", cmd);
  //  exit(1);
  //}


  if((pid = fork()) < 0) {
    printf("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printf("execvp of tape_svc\n");
    args[0] = "prctl";
    args[1] = "--unaligned=silent";
    args[2] = "tape_svc";
    //args[0] = "valgrind --leak-check=full tape_svc";
    if(tapeoffline) { 		/* overrides any sim flg */
      args[3] = "-o";
      args[4] = dbname;
      args[5] = timetag;
      args[6] = NULL;
    }
    else if(sim) { 
      args[3] = "-s";
      args[4] = dbname;
      args[5] = timetag;
      args[6] = NULL;
    }
    else {
      args[3] = dbname;
      args[4] = timetag;
      args[5] = NULL;
    }
    if(execvp(args[0], args) < 0) {
      printf("***Can't execvp() tape_svc. errno=%d\n", errno);
      exit(1);
    }
  }
  sleep(1);				/* let tape_svc start */
  for(i=0; i < MAX_DRIVES; i++) { 	/* start all the driven_svc */
    if((pid = fork()) < 0) {
      printf("***Can't fork(). errno=%d\n", errno);
      exit(1);
    }
    else if(pid == 0) {                   /* this is the beloved child */
      sprintf(dsvcname, "drive%d_svc", i);
      printf("execvp of %s\n", dsvcname);
      args[0] = dsvcname;
      if(tapeoffline) {                 /* overrides any sim flg */
	 args[1] = "-o";
	 args[2] = dbname;
	 args[3] = timetag;
	 args[4] = NULL;
      }
      else if(sim) {
        args[1] = "-s";
        args[2] = dbname;
        args[3] = timetag;
        args[4] = NULL;
      }
      else {
        args[1] = dbname;
        args[2] = timetag;
        args[3] = NULL;
      }
      if(execvp(args[0], args) < 0) {
        printf("***Can't execvp() %s. errno=%d\n", dsvcname, errno);
        exit(1);
      }
    }
  }
  if((pid = fork()) < 0) {
    printf("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printf("execvp of robot0_svc\n");
    args[0] = "robot0_svc";
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      printf("***Can't execvp() robot0_svc. errno=%d\n", errno);
      exit(1);
    }
  }
  if(!rm_0_found) {		//only run if not already running
  if((pid = fork()) < 0) {
    printf("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printf("execvp of sum_rm_0\n");
    args[0] = "sum_rm_0";
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      printf("***Can't execvp() sum_rm_0. errno=%d\n", errno);
      exit(1);
    }
  }
  }
  if(!rm_1_found) {		//only run if not already running
  if((pid = fork()) < 0) {
    printf("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printf("execvp of sum_rm_1\n");
    args[0] = "sum_rm_1";
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      printf("***Can't execvp() sum_rm_1. errno=%d\n", errno);
      exit(1);
    }
  }
  }
  if(!rm_2_found) {		//only run if not already running
  if((pid = fork()) < 0) {
    printf("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printf("execvp of sum_rm_2\n");
    args[0] = "sum_rm_2";
    args[1] = dbname;
    args[2] = timetag;
    args[3] = NULL;
    if(execvp(args[0], args) < 0) {
      printf("***Can't execvp() sum_rm_2. errno=%d\n", errno);
      exit(1);
    }
  }
  }

  //printf("End of sum_forker to start tape services\n");
}
