/**********************************************************************

        This is the main program for the DRMS server.

***********************************************************************/
/**
\defgroup drms_server drms_server

Start a DRMS server program.

\par Synopsis:

\code
drms_server [FLAGS] [SESSION_ARGUMENTS]
\endcode

\par FLAGS (can be grouped together):
\c -h: Show \ref drms_server usage information.
\par
\c -V: Run \ref drms_server in verbose mode.
\par
\c -Q: Run \ref drms_server in quiet mode.
\par
\c -f: Run \ref drms_server in the foreground. Without \c -f \ref
drms_server spawns a server in a background process, prints the
connection info to stdout and exits. 
\par
\c -A: Archive SUs opened for writing during this session.
\par
\c -L: Redirect stdout and stderr to SU log files.
\par
\c -n: Turn off Nagle's algorithm on TCP/IP sockets. 

\par SESSION_ARGUMENTS:
To specify an argument that affects properties of the DRMS session,
use @c param=value, where @c param is one of the following.

\arg \c JSOC_DBHOST Specifies (overrides) the database host to connect to. Default is ::DBNAME
\arg \c JSOC_DBNAME Specifies (overrides) the database name to use. Default is ::DBNAME
\arg \c JSOC_DBUSER Specifies (overrides) the username used during database host connection. Default is ::USER.
\arg \c JSOC_DBPASSWD Specifies (overrides) the password for the username. Default is ::PASSWD.
\arg \c JSOC_SESSIONNS Specifies (overrides) the DRMS session namespace.
\arg \c DRMS_RETENTION Sets (forces) the storage-unit retention time for the DRMS session
started by <module>.
\arg \c DRMS_QUERY_MEM Sets the memory maximum for a database query.
\arg \c DRMS_SERVER_WAIT Non-zero value indicates waiting 2 seconds
before exiting, otherwise don't wait.

\sa
create_series describe_series delete_series modify_series show_info 

@{
*/

//#define DEBUG

#include "drms.h"
#include "serverdefs.h"
#include "xmem.h"
#include "cmdparams.h"
#include "tee.h"
#ifdef __linux__
#include "backtrace.h"
#include <sched.h>
#endif

/* Global structure holding command line parameters. */
CmdParams_t cmdparams;

ModuleArgs_t module_args[] = {
  {ARG_INT, "DRMS_RETENTION", "-1"}, /* -1 == use default set in series definition. */
  {ARG_INT, "DRMS_QUERY_MEM", "512"}, 
  {ARG_INT, "DRMS_SERVER_WAIT", "1"},
  {}
};

ModuleArgs_t *gModArgs = module_args;

int threadcounter = 0;
char *abortmessage=NULL;
DRMS_Env_t *env;
/** @}*/
static void atexit_action (void) {
  fprintf (stderr, "WARNING: DRMS server called exit.\n");
#ifdef DEBUG
#ifdef __linux__
  fprintf (stderr, "Stack frame:\n");
  show_stackframe (stderr);
  hang (12);
#endif
#endif
  drms_server_abort (env, 1);
#ifdef DEBUG
    xmem_leakreport ();
#endif
}

int main (int argc, char *argv[]) {
  int fg=0, noshare=0, verbose=0, nagleoff=0, dolog=0;
  struct sockaddr_in client;
  int sockfd, clientsockfd, status;
  unsigned int client_size = sizeof(client);
  char hostname[1024], *user, unknown[]="unknown";
  short port;
  pthread_t thread;
  DRMS_ThreadInfo_t *tinfo;
  DB_Handle_t *db_handle;
  pid_t pid=0;
  char *dbhost, *dbuser, *dbpasswd, *dbname, *sessionns;
  char drms_server[] = "drms_server";

#ifdef DEBUG
  xmem_config(1,1,1,1,1000000,1,0,0); 
#endif
  /* Parse command line parameters. */
  if (cmdparams_parse(&cmdparams, argc, argv)==-1)
  {
    fprintf(stderr,"Error: Command line parsing failed. Aborting.\n");
    return 1;
  }
  if (cmdparams_exists(&cmdparams,"h"))
    goto usage;
  verbose = cmdparams_exists(&cmdparams,"V");
  noshare = cmdparams_exists(&cmdparams,"s");
  nagleoff = cmdparams_exists(&cmdparams,"n");
  dolog = cmdparams_exists(&cmdparams,"L");
  fg = cmdparams_exists(&cmdparams,"f");

  /* Print command line parameters in verbose mode. */
  if (verbose)
    cmdparams_printall(&cmdparams);

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

  printf("DRMS server started with pid=%d, noshare=%d\n", pid,noshare);    

  /* Initialize server's own DRMS environment and connect to 
     DRMS database server. */
  if ((env = drms_open(dbhost,dbuser,dbpasswd,dbname,sessionns)) == NULL)
  {
    fprintf(stderr,"Failure during server initialization.\n");
    return 1;
  }
  db_handle = env->session->db_handle;

  printf("env->session->db_direct = %d\n",env->session->db_direct );
  printf("DRMS server connected to database '%s' on host '%s' as "
	 "user '%s'.\n",env->session->db_handle->dbname, 
	 env->session->db_handle->dbhost,
	 env->session->db_handle->dbuser);

  env->archive     = cmdparams_exists(&cmdparams,"A");
  env->retention   = cmdparams_get_int(&cmdparams, "DRMS_RETENTION", NULL);
  env->query_mem   = cmdparams_get_int(&cmdparams, "DRMS_QUERY_MEM", NULL);
  env->server_wait = cmdparams_get_int(&cmdparams, "DRMS_SERVER_WAIT", NULL);
  env->verbose     = verbose;

  env->dbpasswd = dbpasswd;
  env->user = user;
  env->logfile_prefix = drms_server;
  env->dolog = dolog;
  env->quiet = 1;

  /* Start listening on the socket for clients trying to connect. */
  sockfd = db_tcp_listen(hostname, sizeof(hostname), &port);
  printf("DRMS server listening on %s:%hu.\n",hostname,port);
  fflush(stdout);



  /* Fork off child process to do the real work and exit. This allows the 
     server to be started in a shell with out "&" */    
  if (fg == 0)
  {
    if ((pid = fork()) == -1)
    {
      fprintf(stderr,"Fork system call failed, aborting\n");
      return 1;
    }
    else if (pid>0)
    {
      /* If we get here then a child was successfully spawned.
	 Exit from the parent process. */
      return 0;
    }
  }
  pid = getpid();


  /**************************************************************
    We are now in the main server process regardless of whether
   -f was present or not.
  ***************************************************************/
  /* Set atexit action handler. This shuts down the server (somewhat)
     gracefully if any thread should call exit(). */
  atexit(atexit_action);  

 
  /***************** Set up exit() and signal handling ********************/
 
  /* Block signals INT, QUIT, TERM, and USR1. They will explicitly
     handled by the signal thread created below. */
#ifndef DEBUG
  sigemptyset(&env->signal_mask);
  sigaddset(&env->signal_mask, SIGINT);
  sigaddset(&env->signal_mask, SIGQUIT);
  sigaddset(&env->signal_mask, SIGTERM);
  sigaddset(&env->signal_mask, SIGUSR1);
  sigaddset(&env->signal_mask, SIGPIPE);

  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, &env->old_signal_mask)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);          
    Exit(1);
  }
  /* Spawn a thread that handles signals and controls server 
     abort or shutdown. */
  if( (status = pthread_create(&env->signal_thread, NULL, &drms_signal_thread, 
			       (void *) env)) )
  {
    fprintf(stderr,"Thread creation failed: %d\n", status);          
    Exit(1);
  }
#endif

  /* Start a transaction if noshare=FALSE, in which case all database
     operations performed through this server should be treated as
     a single transaction. */
  if (!noshare)
  {    
    if (verbose)
      printf("Setting isolation level to SERIALIZABLE.\n");
    /* Set isolation level to serializable. */
    if ( db_isolation_level(db_handle, DB_TRANS_SERIALIZABLE) )
    {
      fprintf(stderr,"Failed to set database isolation level.\n");
      Exit(1);
    }
  }

  strncpy(env->session->hostname, hostname, DRMS_MAXHOSTNAME);
  env->session->port = port;
  drms_server_begin_transaction(env);

  /* Redirect output */
  if (dolog)
  {
    /* Print info again into log file in SUMS directory. */
    printf("DRMS server started with pid=%d, noshare=%d\n",
	   pid,noshare);    
    fflush(stdout);
  }

  /* Keep listening, accepting connections and spawning DRMS
     threads to service clients if they get authenticated. */
  //  if (verbose)
  printf ("Listening for incoming connections on port %hu.\n", port);
  fflush (stdout);
  for (;;) {
						    /* accept new connection */
    if ((clientsockfd = accept (sockfd, NULL, NULL)) < 0 ) {
      if (errno == EINTR) continue;
      else {
	perror ("accept call failed.");
	close (sockfd);
	Exit (1);
      }      
    }

		       /* Find out who just connected and print it on stdout */
    client_size = sizeof (client);
    if (getpeername (clientsockfd, (struct sockaddr *)&client, &client_size) == -1 ) {
      perror ("getpeername call failed.");
      continue;
    }

    if (drms_server_authenticate (clientsockfd, env, threadcounter) == 0) {
      printf ("pid %d: Connection from %s:%d  accepted.\n", getpid(), 
	  inet_ntoa (client.sin_addr), ntohs (client.sin_port));
      threadcounter++;
						/* Update status in database */
      drms_lock_server (env);
      env->clientcounter++;
      drms_server_session_status (env, "running", env->clientcounter);
      drms_unlock_server (env);

      XASSERT(tinfo = malloc (sizeof (DRMS_ThreadInfo_t)));
      tinfo->env       = env;
      tinfo->threadnum = threadcounter;
      tinfo->sockfd    = clientsockfd;      
      tinfo->noshare   = noshare;
      /* Turn off Nagle's algorithm. Not recommended in general, but may
	 improve performance in high-latency environments. */
      if (nagleoff) {
	nagleoff = 0;
	if (setsockopt (clientsockfd,  IPPROTO_TCP, TCP_NODELAY,
	    (char *)&nagleoff, sizeof (int)) < 0) {
	  close (clientsockfd);
	  continue;
	}
      }
 			       /* Spawn a thread to deal with the connection */
      if( (status = pthread_create (&thread, NULL, &drms_server_thread,
          (void *) tinfo))) {
	fprintf (stderr, "Thread creation failed: %d\n", status);          
	close (clientsockfd);
      }
    } else {
      fprintf (stderr, "pid %d: Connection from %s:%d denied. "
          "Invalid username or password.\n", getpid(),
	  inet_ntoa (client.sin_addr), ntohs (client.sin_port));  
      close (clientsockfd);
    }
  }
  return 0;

usage:
  fprintf (stderr, "Usage:    %s [-h]\n"
      "          %s [-vsnf]\n"
      "Options:  -h: Print this help message.\n"
      "          -v: Verbose debug output.\n"
      "          -L: Redirect stdout and stderr to SU log files.\n"
      "          -A: Archive SUs opened for writing during this session.\n"
      "          -f: Run server in the foreground.\n"
      "          -s: Commit after every module exit and continue after errors.\n"
      "          -n: Turn off Nagle's algorithm on TCP/IP sockets.\n", argv[0],
      argv[0]);
  return 1;
}
  
