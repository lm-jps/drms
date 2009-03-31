#include <sys/wait.h>
#include "jsoc.h"
#include "cmdparams.h"
#include "timer.h"



#define kDRMSSERVERLOG "drmslog"
#define kDRMSSERVERLOGDEF "/tmp/drmsserver_logs"
#define kDRMSRUNLOG "drmsrunlog"
#define kNOTSPECIFIED "notspecified"

#define kDELSCRIPTFLAG "d"
#define kVERBOSEFLAG "v"
#define kDRMSERVERENV "/tmp/drms_server_env"
#define kTIMEOUT "to"
#define kTIMEOUTDEF "15"


enum RUNstat_enum
{
   kSTAT_COMMIT = 0,           /* drms_server committed upon exiting */
   kSTAT_ABORT = 1,            /* drms_server aborted upon exitig (there was an error) */
   kSTAT_SCRIPTFAILURE =   2,  /* failed to run script with DRMS commands */
   kSTAT_KILLFAILED = 3,       /* failed to send signal to drms_server */
   kSTAT_DRMSSERVERFAILURE = 4,/* drms_server failed to shut down properly */
   kSTAT_ENVTIMEOUT = 5        /* couldn't find drms_server env file (perhaps not written) */
};

typedef enum RUNstat_enum RUNstat_enum_t;

ModuleArgs_t module_args[] = {
   /* Don't put unnamed arguments in module_args - otherwise cmdparams_parse() will expect them
    * {ARG_STRING, kSCRIPT, NULL, "The script to run - contains socket-module cmds."}, 
    */
   {ARG_STRING, kDRMSSERVERLOG, kDRMSSERVERLOGDEF, "The path to the drms_server log files."},
   {ARG_STRING, kDRMSRUNLOG, kNOTSPECIFIED, "The path to the drms_run log files."},
   {ARG_DOUBLE, kTIMEOUT, kTIMEOUTDEF, "Time limit, in seconds, to find drms_server's environment file."},
   {ARG_FLAG, kDELSCRIPTFLAG, NULL, "Indicates that the script file should be deleted after use."},
   {ARG_FLAG, kVERBOSEFLAG, NULL, "Print diagnostic messages."},
   {ARG_END}
};

ModuleArgs_t *gModArgs = module_args;

/* Global structure holding command line parameters. */
CmdParams_t cmdparams;

/* takes a single parameter - a script to run */
int main(int argc, char *argv[])
{
   pid_t pid = 0;
   pid_t pidret = 0;
   char envfile[PATH_MAX];
   char cmd[PATH_MAX];
   char *script = NULL;
   char *serverlog = NULL;
   char *drmsrunlog = NULL;
   double timeout = 15;
   int delscr = 0;
   int verbose = 0;
   int abort = 0;
   float elapsed = 0;
   struct stat stbuf;
   int status = 0;
   RUNstat_enum_t runstat = kSTAT_COMMIT;

   if (cmdparams_parse(&cmdparams, argc, argv) == -1)
   {
      fprintf(stderr,"Error: Command line parsing failed. Aborting.\n");
      return 1;
   }

   script = cmdparams_getarg(&cmdparams, 1);
   serverlog = cmdparams_get_str(&cmdparams, kDRMSSERVERLOG, NULL);
   drmsrunlog = cmdparams_get_str(&cmdparams, kDRMSRUNLOG, NULL);
   timeout = cmdparams_get_double(&cmdparams, kTIMEOUT, NULL);
   delscr = cmdparams_isflagset(&cmdparams, kDELSCRIPTFLAG);
   verbose = cmdparams_isflagset(&cmdparams, kVERBOSEFLAG);

   if ((pid = fork()) == -1)
   {
      /* parent - couldn't start child process */
      pid = getpid();

      fprintf(stderr, "Failed to start drms_server.\n");
      
   }
   else if (pid > 0)
   {
      /* parent - pid is child's (drms_server's) pid */
      FILE *fptr = NULL;
      FILE *actstdout = stdout;
      FILE *actstderr = stderr;

      if (strcmp(drmsrunlog, kNOTSPECIFIED) != 0)
      {
         char logfile[PATH_MAX];
         snprintf(logfile, sizeof(logfile), "%s/drmsrun_%llu.log", drmsrunlog, (unsigned long long)pid);
         fptr = fopen(logfile, "w");
      }

      if (fptr)
      {
         actstdout = fptr;
         actstderr = fptr;
      }

      if (verbose)
      {
         fprintf(actstdout, "Loading environment for drms_server pid %llu.\n", (unsigned long long)pid);
      }

      snprintf(envfile, sizeof(envfile), "%s.%llu", kDRMSERVERENV, (unsigned long long)pid);
      

      if (verbose)
      {
         time_t now;
         time(&now);
         fprintf(actstdout, "Start looking for environment file at %s\n", ctime(&now));
      }

      /* wait for server env file to appear */
      StartTimer(25);

      while (1)
      {
         elapsed = StopTimer(25);
         if (elapsed > timeout)
         {
            runstat = kSTAT_ENVTIMEOUT;
            abort = 1;
            
            fprintf(actstderr, 
                    "Time out - couldn't find environment file for drms_server pid %llu.\n", 
                    (unsigned long long)pid);

            break;
         }

         if (!stat(envfile, &stbuf) && S_ISREG(stbuf.st_mode))
         {
            if (verbose)
            {
               fprintf(actstdout, "Found environment file for drms_server pid %llu.\n", (unsigned long long)pid);
            }

            break;
         }

         sleep(1);
      }

      if (runstat == kSTAT_COMMIT)
      {
         /* The server env file is available - source it and run script.
          * script must figure out if a failure happened or not, and
          * then return 0 (commit) or non-0 (abort) */
         snprintf(cmd, sizeof(cmd), "source %s; %s", envfile, script);
         if (verbose)
         {
            fprintf(actstdout, "Running cmd '%s' on drms_server pid %llu.\n", cmd, (unsigned long long)pid);
         }

         status = system(cmd);         

         if (status == -1)
         {
            runstat = kSTAT_SCRIPTFAILURE;
            abort = 1;
            fprintf(actstderr, "Could not execute '%s' properly; bailing.\n", script);
         }
         else if (WIFEXITED(status) && WEXITSTATUS(status))
         {
            /* socket modules will return non-zero (doesn't have to be 1) to indicate 
             * abort should happen */
            /* Script requests abort - abort */
            runstat = kSTAT_ABORT;
            abort = 1;
         }
      }
     
      if (verbose)
      {
         fprintf(actstdout, "About to kill drms_server pid %llu.\n", (unsigned long long)pid);
      }

      if (abort)
      {
         kill(pid, SIGTERM);
      }
      else
      {
         kill(pid, SIGUSR1);
      }

      pidret = waitpid(pid, &status, 0);

      if (pidret != pid)
      {
         fprintf(actstderr, "pid of killed drms_server does not match pid in kill syscall.\n");
         runstat = kSTAT_KILLFAILED;
      }
      else if (WIFEXITED(status))
      {
         /* drms_server returned non-zero value */
         /* If drms_server was told to abort, then it returns 1. If it was told to commit, 
          * then it returns 0. */
         if (WEXITSTATUS(status) != 0 && WEXITSTATUS(status) != 1)
         {
            /* drms_server did not return commit (1) or abort (0) */
            fprintf(actstderr, 
                    "drms_server failed to shut down properly, returned '%d'.\n", 
                    (int)WEXITSTATUS(status));
            runstat = kSTAT_DRMSSERVERFAILURE;
         }
      }

      /* clean up env file */
      unlink(envfile);

      if (delscr)
      {
         unlink(script);
      }
   }
   else
   {
      /* child */
      pid = getpid();
      char logfile[PATH_MAX];
      char arg[128];
      int fd;

      snprintf(logfile, sizeof(logfile), "%s/drmsserver_%llu.log", serverlog, (unsigned long long)pid);
      snprintf(arg, sizeof(arg), "shenvfile=%s", kDRMSERVERENV);
      fd = open(logfile, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
      dup2(fd, 1);
      dup2(1, 2);
      execlp("drms_server", "drms_server", "-f", arg, (char *)0);

      /* does not return */
   }

   return runstat;
}
