/*-----------------------------------------------------------------------------
 * cvs/jsoc/src/datacapture/soc_pipe_scp.c
 *-----------------------------------------------------------------------------
 *
 * DESCRIPTION:
 * This is spawned by the socdc program when it starts. 
 * The soc_pipe_scp will run at the cadence given to the socdc for
 * the DDS interval (normally 1 min.). 
 * soc_pipe_scp is given the $DIRSOC2PIPE dir that the dc system
 * has copied the files into for delivery to the backend pipeline system,
 * as well as the target dir in the pipeline system to scp the files to.
 * soc_pipe_scp is also given the host name of the pipeline system and the
 * seconds cadence to copy files at.
 * Files are removed from the source dir after they are copied to the
 * pipeline_host.
 *
 * Called:
 *	soc_pipe_scp [-v] /source_dir /target_dir pipeline_host cadence
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <signal.h>
#include <strings.h>
#include <dirent.h>
#include <sum_rpc.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h> /* for alarm(2) among other things... */
#include <printk.h>

#define MAXFILES 512            /* max # of file can handle in sourcedir */

FILE *h0logfp;                  /* fp for h0 ouput log for this run */
char datestr[32];
int msg(char *fmt, ...);
void open_h0log(char *filename, char *type);
int h0log(const char *fmt, ...);
void abortit(int stat);
void sighandler(int sig);
void usage(void);
void get_cmd(int argc, char *argv[]);
void setup(int argc, char *argv[]);

int verbose;			/* set by get_cmd() */
int cadence;
int abort_active;		/* set while doing an abort */
int sigalrmflg = 0;		/* set on signal so prog will know */
int sigtermflg = 0;		/* set on signal so prog will know */
int ALRMSEC = 60;               /* seconds for alarm signal */
int debugflg;
char *username;			/* from getenv("USER") */
char *sourcedir;
char *targetdir;
char *hostname;

/* Output a printf type formatted msg string to stdout.
 */
int msg(char *fmt, ...)
{
  va_list args;
  char string[32768];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  printf(string);
  fflush(stdout);
  va_end(args);
  return(0);
}

/* Open the h0 log file for this do_tlm run.
 * Open either a new file for write, or a given file for append.
 */
void open_h0log(char *filename, char *type)
{
  if((h0logfp=fopen(filename, type)) == NULL)
    fprintf(stderr, "**Can't open the log file %s\n", filename);
}

/* Outputs the variable format message (re: printf) to the pe log file.
 */
int h0log(const char *fmt, ...)
{
  va_list args;
  char string[32768];

  va_start(args, fmt);
  vsprintf(string, fmt, args);
  if(h0logfp) {
    fprintf(h0logfp, string);
    fflush(h0logfp);
  } else {			/* couldn't open log */
    printf(string);
    fflush(stdout);
  }
  va_end(args);
  return(0);
}

/* Got a fatal error sometime after registering with SUMS. 
 * Degregister and close with SUMS as approriate.
 */
void abortit(int stat)
{
  printk("**Exit soc_pipe_scp w/ status = %d\n", stat);
  msg("**Exit soc_pipe_scp w/ status = %d\n\n", stat);
  if (h0logfp) fclose(h0logfp);
  exit(stat);
}

void sighandler(int sig)
{
  sigtermflg = 1;
  return;
}

/* Print the usage message and abort 
 */
void usage()
{
  msg("Usage:\nsoc_pipe_scp [-v] /source_dir /target_dir pipeline_host cadence\n");
  msg("where: -v = verbose\n");
  msg(" source_dir = $DIRSOC2PIPE that dc system puts files for the pipeline\n");
  msg(" target_dir = dir in the pipeline host to copy the files to\n");
  msg(" pipeline_host = host name of the pipeline backend\n");
  msg(" cadence = cadence in seconds of the dds\n");
  abortit(1);
}


/* Gets the command line and reads the switches.
 */
void get_cmd(int argc, char *argv[])
{
  int c;

  while(--argc > 0 && ((*++argv)[0] == '-')) {
    while((c = *++argv[0]))
      switch(c) {
      case 'd':
        debugflg=1;
        break;
      case 'v':
        verbose=1;
        break;
      default:
        usage();
        break;
      }
  }
  if(argc != 4) usage();
  sourcedir = argv[0];
  targetdir = argv[1];
  hostname = argv[2];
  cadence = atoi(argv[3]);
}

/* Initial setup stuff called when main is first entered.
 */
void setup(int argc, char *argv[])
{
  int i;
  time_t tval;
  struct tm *t_ptr;
  char logname[128], string[128], cwdbuf[128], idstr[256];

  if (signal(SIGINT, SIG_IGN) != SIG_IGN)
    signal(SIGINT, sighandler);
  if (signal(SIGTERM, SIG_IGN) != SIG_IGN)
    signal(SIGTERM, sighandler);

  tval = time(NULL);
  t_ptr = localtime(&tval);
  sprintf(datestr, "%d.%02d.%02d_%02d:%02d:%02d", 
	  (t_ptr->tm_year+1900), (t_ptr->tm_mon+1),
	  t_ptr->tm_mday, t_ptr->tm_hour, t_ptr->tm_min, t_ptr->tm_sec);
  if(!(username = (char *)getenv("USER"))) username = "nouser";
  sprintf(logname, "/usr/local/logs/soc/soc_pipe_scp_%s_%d",hostname,getpid());
  open_h0log(logname, "w");	/* open new file for write */
  printk_set(h0log, h0log);	/* set for printk calls */
  printk("%s\n", datestr);
  getcwd(cwdbuf, 126);
  sprintf(idstr, "Cwd: %s\nCall: ", cwdbuf);
  for(i=0; i < argc; i++) { 	/* echo cmd line */
    sprintf(string, "%s%s", argv[i], (i < argc-1) ? " " : "");
    strcat(idstr, string);
  }
  strcat(idstr, "\n");
  sprintf(string, "soc_pipe_scp started as pid=%d user=%s\n", getpid(), username);
  strcat(idstr, string);
  printk("*%s", idstr);
  /*printk("*%s\n", datestr);*/
}

int main(int argc, char *argv[])
{
  DIR *dfd;
  struct dirent *dp;
  int i, j;
  char cmd[256];
  char *nontlmnames[MAXFILES];

  get_cmd(argc, argv);		/* check the calling sequence */
  setup(argc, argv);		/* start pvm and init things */
  while(1) {
    if((dfd=opendir(sourcedir)) == NULL) {
      printk("**Can't opendir(%s) to find files\n", sourcedir);
      abortit(3);
    }
    i = 0;
    while((dp=readdir(dfd)) != NULL) {
      /* First copy .tlm files. */
      if(strstr(dp->d_name, ".tlm")) {
        sprintf(cmd, "scp -p %s/%s %s:%s 1> /dev/null 2>&1",
			sourcedir, dp->d_name, hostname, targetdir);
        printk("%s\n", cmd);
        if(system(cmd)) {
          printk("***Error on: %s\n", cmd);
        }
        else {
          sprintf(cmd, "/bin/rm -f %s/%s", sourcedir, dp->d_name);
          if(system(cmd)) {
            printk("***Error on: %s\n", cmd);
          }
        }
      }
      else {		/* non .tlm files */
        nontlmnames[i++] = dp->d_name;
        if(i >= MAXFILES) {
          printk("***Fatal error. Too many (%d) files in %s\n", 
		MAXFILES, sourcedir);
          abortit(3);
        }
      }
    }
    /* now copy the non .tlm files */
    for(j = 0; j < i; j++) {
      if(!strcmp(nontlmnames[j], ".") || !strcmp(nontlmnames[j], "..")) 
        continue;
      sprintf(cmd, "scp -p %s/%s %s:%s 1> /dev/null 2>&1",
			sourcedir, nontlmnames[j], hostname, targetdir);
      printk("%s\n", cmd);
      if(system(cmd)) {
        printk("***Error on: %s\n", cmd);
      }
      else {
        sprintf(cmd, "/bin/rm -f %s/%s", sourcedir, nontlmnames[j]);
        if(system(cmd)) {
          printk("***Error on: %s\n", cmd);
        }
      }
    }
    closedir(dfd);
    if(sigtermflg) abortit(2);
    sleep(cadence/2);
  }
}

