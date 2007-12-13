/**
\defgroup jsoc_main jsoc_main

\par Flags:
\c -H|--help: Show jsoc_main usage information.
\par
\c -L: Run jsoc_main driver with logging.
\par
\c -V: Run jsoc_main driver in verbose mode.
\par
\c -Q: Run  jsoc_main  driver  in  quiet mode (no terminal output).

\param DRMS_RETENTION
\param DRMS_QUERY_MEM
\param JSOC_DBHOST
\param JSOC_DBNAME
\param JSOC_DBUSER
\param JSOC_DBPASSWD
\param JSOC_SESSIONNS


@{
*/
#include "drms.h"
#include "serverdefs.h"
#include "xmem.h"
#include "cmdparams.h"
#include "cmdparams_priv.h"
#include "tee.h"
#include "jsoc_main.h"
#ifdef __linux__
#include "backtrace.h"
#include <sched.h>
#endif

CmdParams_t cmdparams;
/* Global DRMS Environment handle. */
DRMS_Env_t *drms_env;

ModuleArgs_t *gModArgs = module_args;

/** @}*/
CmdParams_t *GetGlobalCmdParams() 
{
  return &cmdparams;
}

static void atexit_action (void) {
  fprintf (stderr, "WARNING: DRMS server called exit.\n");
#ifdef DEBUG
#ifdef __linux__
  fprintf (stderr, "Stack frame:\n");
  show_stackframe (stderr);
  hang (12);
#endif
#endif
  drms_server_abort (drms_env);
#ifdef DEBUG
    xmem_leakreport ();
#endif
}

int JSOCMAIN_Main(int argc, char **argv, const char *module_name, int (*CallDoIt)(void)) {
  int verbose=0, nagleoff=0, dolog=0, quiet=0;
  int status;
  char hostname[1024], *user, unknown[]="unknown";
  DB_Handle_t *db_handle;
  char *dbhost, *dbuser, *dbpasswd, *dbname, *sessionns;

  if (save_stdeo()) {
    printf("Can't save stdout and stderr\n");
    return 1;
  }

#ifdef DEBUG
  xmem_config(1,1,1,1,1000000,1,0,0); 
#endif
  /* Parse command line parameters. */
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

  verbose = (cmdparams_exists (&cmdparams, "V") &&
	     cmdparams_get_int (&cmdparams, "V", NULL) != 0);
  if (verbose) cmdparams_printall (&cmdparams);
  quiet = (cmdparams_exists (&cmdparams, "Q") &&
	   cmdparams_get_int (&cmdparams, "Q", NULL) != 0);
  dolog = (cmdparams_exists (&cmdparams, "L") &&
	   cmdparams_get_int (&cmdparams, "L", NULL) != 0);
  nagleoff = cmdparams_exists(&cmdparams,"n");

  if (gethostname(hostname, sizeof(hostname)))
      return 1;

  /* Get user name */
  if (!(user = getenv("USER")))
    user = unknown;

  /* Get hostname, user, passwd and database name for establishing 
     a connection to the DRMS database server. */
  if ((dbhost = cmdparams_get_str(&cmdparams, "JSOC_DBHOST", NULL)) == NULL)
    dbhost =  SERVER;
  if ((dbname = cmdparams_get_str(&cmdparams, "JSOC_DBNAME", NULL)) == NULL)
    dbname = DBNAME;
  if ((dbuser = cmdparams_get_str(&cmdparams, "JSOC_DBUSER", NULL)) == NULL)
    dbuser = USER;
  if ((dbpasswd = cmdparams_get_str(&cmdparams, "JSOC_DBPASSWD", NULL)) == NULL)
    dbpasswd = PASSWD;
  sessionns = cmdparams_get_str(&cmdparams, "JSOC_SESSIONNS", NULL);
  if (sessionns && !strcmp(sessionns, "public")) {
    fprintf(stderr, "Can't run drms_server in public namespace\n");
    return 1;
  }

  /* Initialize server's own DRMS environment and connect to 
     DRMS database server. */
  if ((drms_env = drms_open(dbhost,dbuser,dbpasswd,dbname,sessionns)) == NULL)
  {
    fprintf(stderr,"Failure during server initialization.\n");
    return 1;
  }
  db_handle = drms_env->session->db_handle;

  drms_env->archive     = cmdparams_exists(&cmdparams,"A");
  if (cmdparams_exists (&cmdparams, "DRMS_RETENTION")) {
    drms_env->retention = cmdparams_get_int(&cmdparams, "DRMS_RETENTION", NULL);
  }
  if (cmdparams_exists (&cmdparams, "DRMS_QUERY_MEM")) {
    drms_env->query_mem = cmdparams_get_int(&cmdparams, "DRMS_QUERY_MEM", NULL);
  }
  drms_env->server_wait = 0;
  drms_env->verbose     = verbose;

  atexit(atexit_action);  

  /***************** Set up exit() and signal handling ********************/
 
  /* Block signals INT, QUIT, TERM, and USR1. They will explicitly
     handled by the signal thread created below. */
#ifndef DEBUG
  sigemptyset(&drms_env->signal_mask);
  sigaddset(&drms_env->signal_mask, SIGINT);
  sigaddset(&drms_env->signal_mask, SIGQUIT);
  sigaddset(&drms_env->signal_mask, SIGTERM);
  sigaddset(&drms_env->signal_mask, SIGUSR1);
  if( (status = pthread_sigmask(SIG_BLOCK, &drms_env->signal_mask, &drms_env->old_signal_mask)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);          
    Exit(1);
  }
  /* Spawn a thread that handles signals and controls server 
     abort or shutdown. */
  if( (status = pthread_create(&drms_env->signal_thread, NULL, &drms_signal_thread, 
			       (void *) drms_env)) )
  {
    fprintf(stderr,"Thread creation failed: %d\n", status);          
    Exit(1);
  }
#endif

  fflush(stdout);

  /* Start a transaction where all database operations performed
     through this server should be treated as a single transaction. */
  if (verbose)
    printf("Setting isolation level to SERIALIZABLE.\n");
  /* Set isolation level to serializable. */
  if ( db_isolation_level(db_handle, DB_TRANS_SERIALIZABLE) )
    {
      fprintf(stderr,"Failed to set database isolation level.\n");
      Exit(1);
    }
  if ( db_start_transaction(db_handle))
    {
      fprintf(stderr,"Couldn't start database transaction.\n");
      Exit(1);
    }

  /* Register the session in the database. */
  if ((drms_env->session->stat_conn = db_connect(dbhost,dbuser,dbpasswd,dbname,1)) == NULL)
  {
    fprintf(stderr,"Error: Couldn't establish stat_conn database connection.\n");
    Exit(1);
  }
  
  drms_server_open_session(drms_env, hostname, -1, user, dolog);

  /* Redirect output */
  if (dolog)
  {
    char filename_e[1024],filename_o[1024];
    if (!quiet) {
      CHECKSNPRINTF(snprintf(filename_e,1023, "%s/%s.stderr.gz", drms_env->session->sudir, module_name), 1023);
      CHECKSNPRINTF(snprintf(filename_o,1023, "%s/%s.stdout.gz", drms_env->session->sudir, module_name), 1023);
      if ((drms_env->tee_pid = tee_stdio (filename_o, 0644, filename_e, 0644)) < 0)
	Exit(-1);
    } else {
      CHECKSNPRINTF(snprintf(filename_e,1023, "%s/%s.stderr", drms_env->session->sudir, module_name), 1023);
      CHECKSNPRINTF(snprintf(filename_o,1023, "%s/%s.stdout", drms_env->session->sudir, module_name), 1023);
      if (redirect_stdio (filename_o, 0644, filename_e, 0644))
	Exit(-1);
    }
  }

  int abort_flag = DoIt();

  if (abort_flag) drms_server_abort (drms_env);
  else drms_server_commit (drms_env);

  _exit(abort_flag);
}
  
