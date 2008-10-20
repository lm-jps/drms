//#define DEBUG_MEM
//#define DEBUG

#define DRMS_CLIENT

#include "jsoc.h"
#include "drms.h"
#ifdef DEBUG_MEM
#include "xmem.h"
#endif
#include "tee.h"
#include <signal.h>
						    /* for drms_start_server */
#include "serverdefs.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>

#include "cfortran.h"
#include "jsoc_main.h"
#include "drms_env.h"
#include "cmdparams_priv.h"

CmdParams_t cmdparams;
/* Global DRMS Environment handle. */
DRMS_Env_t *drms_env;

ModuleArgs_t *gModArgs = module_args;

CmdParams_t *GetGlobalCmdParams() 
{
  return &cmdparams;
}

/* The atexit function can't take arguments...so make a global. */
const char *mn = NULL;

static pid_t drms_start_server (int verbose, int dolog);

   /* Remind the user that the DRMS session is rolled back if exit is called */
static void atexit_action (void) {
  fprintf (stderr, "WARNING: DRMS module %s called exit.\nThe DRMS session"
      " will be aborted and the database rolled back.\n", mn);
}

static void sighandler(int signo)
{
  fprintf(stderr,"Module received signal %d. Aborting.\n",signo);

  drms_abort_now(drms_env);

  // don't wait for self-start drms_server
  exit(1);
}  

int JSOCMAIN_Init(int argc, 
		  char **argv, 
		  const char *module_name, 
		  int *dolog,
		  int *verbose,
		  pid_t *drms_server_pid, 
		  pid_t *tee_pid,
		  int *cont)
{
   int status;
   int quiet;
   int printrel = 0;

   if (cont)
   {
      *cont = 0;
   }

   mn = module_name;

#ifdef DEBUG_MEM
   xmem_config (1, 1, 1, 1, 1000000, 1,0, 0); 
#endif
   /* Parse command line parameters */
   status = cmdparams_parse (&cmdparams, argc, argv);
   if (status == CMDPARAMS_QUERYMODE) {
      cmdparams_usage (argv[0]);
      return 0;
   } else if (status == CMDPARAMS_NODEFAULT) {
      fprintf (stderr, "For usage, type %s [-H|--help]\n", argv[0]);
      return 0;
   } else if (status < 0) {
      fprintf (stderr, "Error: Command line parsing failed. Aborting.\n");
      fprintf (stderr, "For usage, type %s [-H|--help]\n", argv[0]);
      return 1;
   }

   printrel = cmdparams_isflagset(&cmdparams, kREL);
   if (printrel)
   {
      char verstr[32];
      int isdev = 0;

      jsoc_getversion(verstr, sizeof(verstr), &isdev);
      fprintf(stdout, 
              "Module '%s' JSOC version is '%s' (%s)\n", 
              module_name, 
              verstr, 
              isdev ? "development" : "release");
      return 0;
   }

   *verbose = (cmdparams_exists (&cmdparams, "V") &&
	      cmdparams_get_int (&cmdparams, "V", NULL) != 0);
   if (*verbose) cmdparams_printall (&cmdparams);
   quiet = (cmdparams_exists (&cmdparams, "Q") &&
	    cmdparams_get_int (&cmdparams, "Q", NULL) != 0);
   *dolog = (cmdparams_exists (&cmdparams, "L") &&
	    cmdparams_get_int (&cmdparams, "L", NULL) != 0);
   if (!cmdparams_exists (&cmdparams, "DRMS_QUERY_MEM"))
     cmdparams_set (&cmdparams,"DRMS_QUERY_MEM", "512");
   /* if not already in a DRMS session make one */
   if (!cmdparams_exists (&cmdparams, "DRMSSESSION")) {
      if ((*drms_server_pid = drms_start_server (*verbose, *dolog)) < 0) {
	 fprintf (stderr,
		  "Couldn't connect to a DRMS server via drms_start_server.\n");
	 return 1;
      }
   }
   /* DRMS Prolog */
   if (cmdparams_exists (&cmdparams, "DRMSSESSION")) {
      char filename_e[1024], filename_o[1024];
      char *drmssession = cmdparams_get_str (&cmdparams, "DRMSSESSION", NULL);
      char *jsoc_dbuser = cmdparams_get_str (&cmdparams, "JSOC_DBUSER", NULL);
      char *jsoc_dbpasswd = cmdparams_get_str (&cmdparams, "JSOC_DBPASSWD", NULL);
      char *jsoc_dbname = cmdparams_get_str (&cmdparams, "JSOC_DBNAME", NULL);
      drms_env = drms_open (drmssession, jsoc_dbuser, jsoc_dbpasswd, jsoc_dbname, NULL);
      if (drms_env == NULL) {
	 fprintf (stderr, "Couldn't connect to DRMS.\n");
	 return 1;
      }    
      drms_env->query_mem = cmdparams_get_int (&cmdparams, "DRMS_QUERY_MEM", NULL);
    
      if (*dolog) {
	 if (save_stdeo()) {
	    printf ("Can't save stdout and stderr\n");
	    return 1;
	 }
	 /* This program is now running in a DRMS session. 
	    Redirect or tee stdout and stderr to the session log directory. */
	 CHECKSNPRINTF(snprintf (filename_o, 1023, "%s/%s.%04d.stdout.gz",
				 drms_env->session->sudir, module_name, drms_env->session->clientid), 1023);
	 CHECKSNPRINTF(snprintf (filename_e, 1023, "%s/%s.%04d.stderr.gz",
				 drms_env->session->sudir, module_name, drms_env->session->clientid), 1023);
      
	 if (!quiet) {
	    if ((*tee_pid = tee_stdio (filename_o, 0644, filename_e, 0644)) < 0)
	      return -1;
	 } else if (redirect_stdio (filename_o, 0644, filename_e, 0644))
	   return -1;
      }
#ifndef IDLLIB
      /* Don't register an atexit function because this code is running inside 
       * the IDL process.  When IDL exits, it always either returns from its own
       * main(), or it calls exit().  Therefore, atexit_action() is always called
       * from an IDL session.  Modules, on the other hand, exit by calling _exit()
       * when no errors have occurred.  atexit() should only be used from DRMS modules
       * where exit by anything other than _exit() denotes an error, and _exit()
       * denotes success.  
       */
      atexit (atexit_action);   
#endif


      struct sigaction act;
      /* Set signal handler to clean up. */
      act.sa_handler = sighandler;
      sigfillset(&(act.sa_mask));
      sigaction(SIGINT, &act, NULL);
      sigaction(SIGTERM, &act, NULL);

   }

   /* Initialize global things. */
   drms_keymap_init(); /* If this slows down init too much, do on-demand init. */

   /* continue with calling module or otherwise interacting with DRMS. */
   if (cont)
   {
      *cont = 1;
   }

   return 0;
}

int JSOCMAIN_Term(int dolog, int verbose, pid_t drms_server_pid, pid_t tee_pid, int abort_flag)
{
   int status;

#ifdef DEBUG
   printf ("Module %s returned with status = %d\n", mn, abort_flag);
#endif

   /* This will close all fitsfile pointers, saving changes to the underlying fits files. 
    * This must be done in the module process as that is the process that maintains the
    * list of open fitsfiles (see drms_server_commit() for more information). */
   if (!abort_flag)
   {
      drms_fitsrw_term();
   }

   /* DRMS Epilog:
      If abort_flag=0 all data records created by this module are inserted
      into the database and will become permanent at the next session commit.
      If abort_flag=1 all data inserted into the database since the last 
      session commit are rolled back and the DRMS session is aborted.
   */
   if (cmdparams_exists (&cmdparams, "DRMSSESSION")) {
      /* NOTICE: Some errors on the server side (e.g. failure to
	 communicate with SUMS) will make drms_abort or drms_close fail with 
	 error message "readn error: Connection reset by peer" because the 
	 server is already stopped and has closed the socket connection. 
	 This is not a problem since the server will already have shut itself 
	 down cleanly.
      */
      if (abort_flag) drms_abort (drms_env);
      else drms_close (drms_env, DRMS_INSERT_RECORD);
   }

   if (dolog) {
      fclose (stdout);
      fclose (stderr);
      if (tee_pid) {
	 waitpid (tee_pid, &status, 0);
	 if (status) printf ("Problem returning from tee\n");
      }
      if (restore_stdeo ()) printf ("Can't restore stderr and stdout\n");
   }

   if (drms_server_pid) {	   /* mimic drms_run after command execution */
      /* Stop the DRMS server */
      if (abort_flag) {
	 if (verbose)
	   printf ("Command returned an error code. Rolling back database.\n");
	 kill (drms_server_pid, SIGTERM);
      }
      else {
	 if (verbose)
	   printf ("Command finished successfully. Commiting data to database.\n");
	 if (kill (drms_server_pid, SIGUSR1)) {
	    perror ("SIGUSR1 attempt failed to stop server jsoc_main");
	    printf ("drms_pid = %d\n", drms_server_pid);
	 }
      }
      if (waitpid (drms_server_pid, &status, 0) < 0) perror ("waitpid error");
      if (verbose) printf ("drms_server returned with status = %d\n", status);
   }

   cmdparams_freeall (&cmdparams);

   /* Terminate other global things. */
   drms_keymap_term();
   drms_keyword_term();
   drms_protocol_term();
   drms_defs_term();
   drms_time_term();

#ifdef DEBUG_MEM
   xmem_leakreport ();
#endif
   fflush (stdout);
   fflush (stderr); 

   return status;
}

int JSOCMAIN_Main(int argc, char **argv, const char *module_name, int (*CallDoIt)(void)) 
{
   int abort_flag = 0;
   int cont;
   int ret;
    
   /* Passed between Init and Term. */
   int dolog;
   int verbose;
   pid_t drms_server_pid = 0;
   pid_t tee_pid = 0;


   ret = JSOCMAIN_Init(argc, 
		       argv, 
		       module_name, 
		       &dolog, 
		       &verbose, 
		       &drms_server_pid, 
		       &tee_pid, 
		       &cont);

   if (!cont)
   {
      return ret;
   }

   /* Call main module function */
   if (CallDoIt)
   {
      abort_flag = (*CallDoIt)();
   }

   JSOCMAIN_Term(dolog, verbose, drms_server_pid, tee_pid, abort_flag);

   return(abort_flag);
}

	     /*  drms_start_server - mimics initial code in drms_run script  */

pid_t drms_start_server (int verbose, int dolog)  {
  char *dbhost, *dbuser, *dbpasswd, *dbname, *sessionns;
  int retention, query_mem, server_wait;
  char drms_session[DRMS_MAXPATHLEN];
  char drms_host[DRMS_MAXPATHLEN];
  char drms_port[DRMS_MAXPATHLEN];
	/* Get hostname, user, passwd and database name for establishing 
				    a connection to the DRMS database server */
  if ((dbhost = cmdparams_get_str (&cmdparams, "JSOC_DBHOST", NULL)) == NULL)
    dbhost =  SERVER;
  if ((dbname = cmdparams_get_str (&cmdparams, "JSOC_DBNAME", NULL)) == NULL)
    dbname = DBNAME;
  if ((dbuser = cmdparams_get_str (&cmdparams, "JSOC_DBUSER", NULL)) == NULL)
    dbuser = USER;
  if ((dbpasswd = cmdparams_get_str (&cmdparams, "JSOC_DBPASSWD", NULL)) == NULL)
    dbpasswd = PASSWD;
  sessionns = cmdparams_get_str (&cmdparams, "JSOC_SESSIONNS", NULL);

  retention = -1;
  if (drms_cmdparams_exists(&cmdparams, "DRMS_RETENTION")) 
     retention = drms_cmdparams_get_int(&cmdparams, "DRMS_RETENTION", NULL);
  query_mem = 512;
  if (cmdparams_exists (&cmdparams, "DRMS_QUERY_MEM")) 
    query_mem = cmdparams_get_int (&cmdparams,"DRMS_QUERY_MEM", NULL);
  server_wait = 0;
  if (cmdparams_exists (&cmdparams, "DRMS_SERVER_WAIT")) 
    server_wait = cmdparams_get_int (&cmdparams,"DRMS_SERVER_WAIT", NULL);
  
  int fd[2];
  pid_t	pid;

  if (pipe(fd) < 0) {
    perror("pipe error");
    return -1;
  }

  if ( (pid = fork()) < 0) {
    perror("fork error");
    return -1;
  }
  else if (pid > 0) {	/* parent */
    close(fd[1]);	/* close write end */

    const int bufsz = 1024;
    char *server_info = 0, *line = 0;
    XASSERT(server_info = malloc(bufsz));
    XASSERT(line = malloc(bufsz));
    server_info[0] = '\0';
    int  n;    
    fd_set readfd;
    do {
      FD_ZERO(&readfd);
      FD_SET(fd[0], &readfd);
      if (select(fd[0]+1, &readfd, NULL, NULL, NULL) < 0) {
	if (errno == EINTR)
	  continue;
	else
	{
	  perror("Select failed");
	  return -1;
	}
      }
      if ((n = read(fd[0], line, bufsz)) < 0) {
	perror("Read error from pipe");
	return -1;
      }
      if (n) {
	line[n] = '\0';
	if (strstr(line, "failed")) {
	  return -1;
	}
	strcat(server_info, line);
      }
/*       printf("%s\n-----\n", line); */
    } while (!strstr(line, "DRMS_SUDIR"));
    if (verbose) {
      write(STDOUT_FILENO, server_info, strlen(server_info));
    }
    
    char *p = strstr(server_info, "DRMS_HOST");
    sscanf(p, "DRMS_HOST = %s", drms_host);
    p = strstr(server_info, "DRMS_PORT");
    sscanf(p, "DRMS_PORT = %s", drms_port);
    strcpy(drms_session, drms_host);
    strcat(drms_session, ":");
    strcat(drms_session, drms_port);
    //    setenv("DRMSSESSION", drms_session, 1);
    cmdparams_set(&cmdparams, "DRMSSESSION", drms_session);
    free(server_info);
    free(line);
    return pid;
  } else {						/* child */
    close(fd[0]);					/* close read end */

    if (fd[1] != STDOUT_FILENO) {
      if (dup2(fd[1], STDOUT_FILENO) != STDOUT_FILENO) {
	perror ("dup2 error to stdout");
	exit (1);
      }
      close(fd[1]);
    }

    const int num_args = 14;
    char **argv = malloc(num_args*sizeof(char *));
    int i = 0;
    argv[i++] = strdup ("drms_server");
    argv[i++] = strdup ("-f");
    if (verbose) 
      argv[i++] = strdup ("-V");
    if (dolog) 
      argv[i++] = strdup ("-L");
    if (cmdparams_exists(&cmdparams,"A")) 
      argv[i++] = strdup ("-A");      
    argv[i] = malloc (strlen (dbhost)+DRMS_MAXNAMELEN);
    sprintf (argv[i++], "JSOC_DBHOST=%s", dbhost);
    argv[i] = malloc (strlen (dbname)+DRMS_MAXNAMELEN);
    sprintf (argv[i++], "JSOC_DBNAME=%s", dbname);
    if (dbuser) {
      argv[i] = malloc (strlen (dbuser)+DRMS_MAXNAMELEN);
      sprintf (argv[i++], "JSOC_DBUSER=%s", dbuser);
    }
    if (dbpasswd) {
      argv[i] = malloc (strlen (dbpasswd)+DRMS_MAXNAMELEN);
      sprintf(argv[i++], "JSOC_DBPASSWD=%s", dbpasswd);
    }
    if (sessionns) {
      argv[i] = malloc (strlen (sessionns)+DRMS_MAXNAMELEN);
      sprintf (argv[i++], "JSOC_SESSIONNS=%s", sessionns);
    }
    if (retention > 0) {
      argv[i] = malloc (DRMS_MAXNAMELEN*2);      
      sprintf (argv[i++], "DRMS_RETENTION=%d", retention);
    }
    if (query_mem != 512) {
      argv[i] = malloc (DRMS_MAXNAMELEN*2);
      sprintf (argv[i++], "DRMS_QUERY_MEM=%d", query_mem);
    }
    if (!server_wait) {
      argv[i] = malloc (DRMS_MAXNAMELEN*2);
      sprintf (argv[i++], "DRMS_SERVER_WAIT=%d", server_wait);
    }
    for (; i < num_args; i++) {
      argv[i] = NULL;
    }

    if (execvp ("drms_server", argv) < 0) {
      printf ("drms_start_server failed to start server.\n");
      perror ("exec error for drms_server");
      exit (1);
    }
  }

  if (verbose) {
    if (!dolog) printf ("Log Files not copied to DRMS_SUDIR\n");
    printf ("Starting command now.\n");
  }

  return(0);

}
