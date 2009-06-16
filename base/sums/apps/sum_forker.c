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

int main(int argc, char *argv[])
{ 
  int i;
  pid_t pid;
  char *args[5];
  char dsvcname[80], pgport[32];

  get_cmd(argc, argv);                  /* check the calling sequence */

  //printf("!!TEMP sum_forker just exits for now\n");
  //exit(0); //!!!TEMP

      sprintf(pgport, SUMPGPORT);
      setenv("PGPORT", pgport, 1); //need to connect to new jsoc_sums db


  if((pid = fork()) < 0) {
    printf("***Can't fork(). errno=%d\n", errno);
    exit(1);
  }
  else if(pid == 0) {                   /* this is the beloved child */
    printf("execvp of tape_svc\n");
    args[0] = "tape_svc";
    if(tapeoffline) { 		/* overrides any sim flg */
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
  //printf("End of sum_forker to start tape services\n");
}
