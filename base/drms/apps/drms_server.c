/**********************************************************************

        This is the main program for the DRMS server.

******************************************************-****************/
/**
\defgroup drms_server drms_server - Manages a database session and passes database requests to database from client modules
@ingroup drms_util

\brief Start a DRMS server program.

\par Synopsis:
\code
drms_server [FLAGS] [SESSION_ARGUMENTS]
\endcode

\a drms_server creates a DRMS session enabling multiple modules to run in the same session.

<pre>
					       	       	       	       	       	      
				      drms_server				      
		    +----------------------------------------------+		      
		    |                          	                   |		      
     +----------+   |   +--------+ +--------+++	 +-------------+   |		      
     |          |   |   |        +-+   in   ||+--+             |   |		      
     |   SUMS   +---+---+  SUMS  | +--------+++	 |             +---+-------- client 0 
     |          |   |   | thread | +++--------+	 |             |   |		      
     |          |   |   |        +-+|| out    +--+             +---+-------- client 1 
     +----------+   |   +--------+ +++--------+	 |             |   |		      
                    |   +--------+             	 |             |   |		      
                    |   |        |             	 |             |   |		      
                    |   | signal ----------------+             +---+-------- client n
                    |   | thread |               |    server   |   |
		    |   |      	 |               | thread pool |   |
	 	    |   +--------+               +------++-----+   |
		    +-----------------------------------++---------+
		                                 +------++-----+
						 |             |
						 |     	DB     |
						 |             |
						 +-------------+

</pre>
The \ref drms_server is designed to support multiple client modules
running at the same time. It performs the following major tasks: 
<ul>
<li>Connection to DRMS DB.

    \ref drms_server opens a single DB connection. This DB connection is
    shared by all modules connected to the same \ref drms_server program
    via sockets. Such shared DB connection makes drms_commit() issued
    from the client modules extremely hazardous, e.g., one client
    module can inadvadently commit another client module data. The
    correct usage of drms_commit requires higher level coordination
    among the client modules.

<li>Communication with SUMS

    \ref drms_server talks the SUMS via SUMS API. All the interactions
    with SUMS are happenning in the SUMS thread,
    drms_sums_thread. This thread retrieves SUMS request from the
    sum_inbox, and places SUMS reply in sum_outbox. Both sum_inbox and
    sum_outbox are FIFO queues with the thread id as tag. They are
    shared memory.  Don't insert local variable into the queue unless
    you are sure to get the reply back before the local variable
    vanishes.  Due to FIFO queue implementation, only the head of the
    queue is examined. if the head is not removed, nothing behind it
    can be removed.

    <table border="1" cellspacing="3" cellpadding="3">
    <tr>
    <td></td>
    <td>producer</td>
    <td>consumer</td>
    </tr>
    <tr>
    <td>sum_inbox</td>
    <td>any call to SUMS API</td>
    <td>drms_sums_thread</td>
    </tr>
    <tr>
    <td>sum_outbox</td>
    <td>drms_sums_thread</td>
    <td>function that needs SUMS call reply</td>
    </tr>
    </table>
<li>Exiting: either abort or commit

The closing sequence can be tricky, one might run into race condition if not careful.
There are two exit routes:  drms_server_commit() and
drms_server_abort().
<ol>
<li>drms_server_commit()
<pre>
   drms_delete_temporaries() removes all records created as ::DRMS_TRANSIENT
   drms_commit_all_units() commits all data units (does not include log SU)
   drms_server_close_session() {
    fflush() on stdout and stderr
    restore_stder()
    drms_commit() log SU
    update drms_session table entry
   }
   wait for SUMS threads to finish via pthread_join()
   disconnect stat db connection
   db_commit() for data db connection
   db_abort()  for data db connection?
   drms_free_env()
</pre>
<li>drms_server_abort() 
<pre>
   db_rollback() on db data connection
   drms_server_close_session()
   add to sums inbox/outbox to thaw thread that may be waiting
   disconnect stat db connection
   db_abort() for data db connection
   sleep() to wait for other threads to finish cleanly.
   drms_free_env()
</pre>
</ol>
</ul>

\par The signal thread 
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
\c -L: Redirect stdout and stderr to SU log files.
\par
\c -n: Turn off Nagle's algorithm on TCP/IP sockets. 
\par
\c -s: Set noshare which turns off the serializable mode for the database.

\par SESSION_ARGUMENTS:
To specify an argument that affects properties of the DRMS session,
use @c param=value, where @c param is one of the following.

\arg \c JSOC_DBHOST Specifies (overrides) the database host to connect to. Default is ::DBNAME
\arg \c JSOC_DBNAME Specifies (overrides) the database name to use. Default is ::DBNAME
\arg \c JSOC_DBUSER Specifies (overrides) the username used during database host connection. Default is ::USER.
\arg \c JSOC_DBPASSWD Specifies (overrides) the password for the username. Default is ::PASSWD.
\arg \c JSOC_SESSIONNS Specifies (overrides) the DRMS session namespace.
\arg \c DRMS_RETENTION Sets (forces) the storage-unit retention time for all <modules> in the DRMS session.  The environment
is not searched for this keyword.
\arg \c DRMS_ARCHIVE Sets (forces) the storage-unit archive flag for all <module>s in the DRMS session.
The default is to let the client module or JSD prevail.  Values are not given -> INT_MIN, or -1, 0, 1.  The environment
is not searched for this keyword.
\arg \c DRMS_QUERY_MEM Sets the memory maximum for a database query.
\arg \c DRMS_SERVER_WAIT Non-zero value indicates waiting 2 seconds
before exiting, otherwise don't wait.

\sa
create_series describe_series delete_series modify_series show_info 

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

#include <poll.h>

#define kCENVFILE "cenvfile"
#define kSHENVFILE "shenvfile"
#define kNOTSPECIFIED "notspecified"
#define kSELFSTARTFLAG "b"
#define kHELPFLAG "h"
#define kVERBOSEFLAG "V"
#define kNOSHAREFLAG "s"
#define kNAGLEOFFFLAG "n"
#define kDOLOGFLAG "L"
#define kFGFLAG "f"
#define kLoopConnFlag "loopconn"
#define kDBTimeOut "DRMS_DBTIMEOUT"

/* Global structure holding command line parameters. */
CmdParams_t cmdparams;

ModuleArgs_t module_args[] = {
  {ARG_INT, "DRMS_RETENTION", "-1"}, /* -1 == use default determined by JSD/libDRMS interation. */
  {ARG_INT, "DRMS_NEWSURETENTION", "-1"}, /* -1 == use default from JSD. */
  {ARG_INT, "DRMS_ARCHIVE", "-9999"}, 
  {ARG_INT, "DRMS_QUERY_MEM", "512"}, 
  {ARG_INT, "DRMS_SERVER_WAIT", "1"},
  {ARG_INT, kDBTimeOut, "-99"},
  {ARG_STRING, kCENVFILE, kNOTSPECIFIED, "If set, write out to a file all C-shell commands that set the essential DRMS_* env variables."},
  {ARG_STRING, kSHENVFILE, kNOTSPECIFIED, "If set, write out to a file all bash-shell command that set the essential DRMS_* env variables."},
  {ARG_FLAG, kSELFSTARTFLAG, NULL, "Indicates that drms_server was started by a socket module."},
  {ARG_FLAG, kHELPFLAG, NULL, "Display a usage message, then exit."},
  {ARG_FLAG, kVERBOSEFLAG, NULL, "Display diagostic messages while running."},
  {ARG_FLAG, kNOSHAREFLAG, NULL, "?"},
  {ARG_FLAG, kNAGLEOFFFLAG, NULL, "?"},
  {ARG_FLAG, kDOLOGFLAG, NULL, "Generate a log saved into a SUMS directory."},
  {ARG_FLAG, kFGFLAG, NULL, "Do not fork a child drms_server process."},
  {ARG_FLAG, kLoopConnFlag, NULL, "Loop trying to connect to SUMS if SUMS isn't there."},
  {}
};

ModuleArgs_t *gModArgs = module_args;

int threadcounter = 0;
char *abortmessage=NULL;
DRMS_Env_t *env;
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

#if 0
/* db_lock calls this function to determine what signals it should NOT 
 * allow to interrupt code running inside the db lock. */
static void drms_createsigmask(sigset_t *set, void *data)
{
   pthread_t threadid = *((pthread_t *)data);
   if (set && threadid == pthread_self())
   {
      sigemptyset(set);
      sigaddset(set, SIGUSR2);
   }
}
#endif

/* If shutdown has started, waits until main() notified */
static DRMS_Shutdown_State_t GetShutdownState()
{
   DRMS_Shutdown_State_t st = kSHUTDOWN_UNINITIATED;
   sem_t *sdsem = drms_server_getsdsem();

   if (sdsem)
   {
      /* Wait until the signal thread is not messing with shutdown - if it is, then 
       * main will block here until the signal thread terminates the process */
      sem_wait(sdsem);

      DRMS_Shutdown_State_t sdstate = drms_server_getsd();

      if (sdstate == kSHUTDOWN_UNINITIATED)
      {
         sem_post(sdsem);
      }
      else if (sdstate == kSHUTDOWN_COMMIT || sdstate == kSHUTDOWN_ABORT || sdstate == kSHUTDOWN_BYMAIN)
      {
         /* signal-induced shutdown has already been started, and main() already knows about it, 
          * or main thread is shutting down. */
         st = sdstate;
         sem_post(sdsem);
      }
      else
      {
         /* shutdown initiated by signal thread, but main() not notified yet */
         sem_post(sdsem);

         /* call commit or abort as appropriate.
          * sdstate might indicate main() not notified yet. */
         while (1)
         {
            sem_wait(sdsem);
            sdstate = drms_server_getsd();

            if (sdstate == kSHUTDOWN_ABORT || sdstate == kSHUTDOWN_COMMIT)
            {
               st = sdstate;
               sem_post(sdsem);
               break;
            }

            sem_post(sdsem);
            sleep(1);
         }
      }
   }

   return st;
}

static int FreeCmdparams(void *data)
{
   cmdparams_freeall(&cmdparams);
   memset(&cmdparams, 0, sizeof(CmdParams_t));
   return 0;
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
    const char *dbhost;
    char *dbuser, *dbpasswd, *dbname, *sessionns;
    char *dbport = NULL;
    char dbHostAndPort[64];
  char drms_server[] = "drms_server";
  char *cenvfileprefix = NULL; 
  char *shenvfileprefix = NULL; 
  char envfile[PATH_MAX];
  FILE *fptr = NULL;
  int selfstart = 0;
  time_t now;
  int infd[2];  /* child writes to this pipe, parent reads from it */
  int16_t retention;
  int16_t newsuretention;

  DRMS_Shutdown_State_t sdstate;
  int ans;


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
  {
     goto usage;
  }
  verbose = cmdparams_exists(&cmdparams,"V");
  noshare = cmdparams_exists(&cmdparams,"s");
  nagleoff = cmdparams_exists(&cmdparams,"n");
  dolog = cmdparams_exists(&cmdparams,"L");
  fg = cmdparams_exists(&cmdparams,"f");
  cenvfileprefix = cmdparams_get_str(&cmdparams, kCENVFILE, NULL);
  shenvfileprefix = cmdparams_get_str(&cmdparams, kSHENVFILE, NULL);
  selfstart = cmdparams_isflagset(&cmdparams, kSELFSTARTFLAG);

  /* Print command line parameters in verbose mode. */
  if (verbose)
    cmdparams_printall(&cmdparams);

  /* Get user name */
  if (!(user = getenv("USER")))
    user = unknown;

  /* Get hostname, user, passwd and database name for establishing 
     a connection to the DRMS database server. */
    
    /* SERVER does not contain port information. Yet when dbhost is used in db_connect(), that function
     * parses the value looking for an optional port number. So if you didn't provide the JSOC_DBHOST
     * then there was no way to connect to the db with a port other than the default port that the
     * db listens on for incoming connections (which is usually 5432).
     *
     * I changed this so that masterlists uses the DRMSPGPORT macro to define the port to connect to.
     * If by chance somebody has appeneded the port number to the server name in SERVER, and that
     * conflicts with the value in DRMSPGPORT, then DRMSPGPORT wins, and a warning message is printed.
     *
     * --ART (2014.08.20)
     */
    
  if ((dbhost = cmdparams_get_str(&cmdparams, "JSOC_DBHOST", NULL)) == NULL)
    {
        const char *sep = NULL;
        
        dbhost =  SERVER;
        dbport = DRMSPGPORT;
        
        /* Check for conflicting port numbers. */
        if ((sep = strchr(dbhost, ':')) != NULL)
        {
            if (strcmp(sep + 1, dbport) != 0)
            {
                char *tmpBuf = strdup(dbhost);
                
                if (tmpBuf)
                {
                    tmpBuf[sep - dbhost] = '\0';
                    fprintf(stderr, "WARNING: the port number in the SERVER localization parameter (%s) and in DRMSPGPORT (%s) conflict.\nThe DRMSPGPORT value will be used.\n", sep + 1, DRMSPGPORT);
                    
                    snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s:%s", tmpBuf, dbport);
                    free(tmpBuf);
                    tmpBuf = NULL;
                }
                else
                {
                    fprintf(stderr, "Out of memory.\n");
                    return 1;
                }
            }
            else
            {
                snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s", dbhost);
            }
        }
        else
        {
            snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s:%s", dbhost, dbport);
        }
    }
    else
    {
        snprintf(dbHostAndPort, sizeof(dbHostAndPort), "%s", dbhost);
    }

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
  if ((env = drms_open(dbHostAndPort, dbuser,dbpasswd,dbname,sessionns)) == NULL)
  {
    fprintf(stderr,"Failure during server initialization.\n");
    return 1;
  }

  time(&now);
  fprintf(stdout, "Connected to dbase at %s", ctime(&now));

  db_handle = env->session->db_handle;

  printf("env->session->db_direct = %d\n",env->session->db_direct );
  printf("DRMS server connected to database '%s' on host '%s' and port '%s' as "
	 "user '%s'.\n",env->session->db_handle->dbname, 
	 env->session->db_handle->dbhost,
     env->session->db_handle->dbport,
	 env->session->db_handle->dbuser);

  env->archive	   = drms_cmdparams_get_int(&cmdparams, "DRMS_ARCHIVE", NULL);
  if (env->archive < -1 ) env->archive = INT_MIN;
    
    retention = INT16_MIN;
    char errbuf[128];
    
    if (drms_cmdparams_exists(&cmdparams, "DRMS_RETENTION") && drms_cmdparams_get_int(&cmdparams, "DRMS_RETENTION", NULL) != -1)
    {
        retention = drms_cmdparams_get_int16(&cmdparams, "DRMS_RETENTION", &status);
        if (status != DRMS_SUCCESS)
        {
            if (status == DRMS_ERROR_INVALIDCMDARGCONV)
            {
                snprintf(errbuf, sizeof(errbuf), "The value for %s must be a 15-bit positive integer.", "DRMS_RETENTION");
                fprintf(stderr, errbuf);
            }
            
            snprintf(errbuf, sizeof(errbuf), "Invalid value for %s.", "DRMS_RETENTION");
            fprintf(stderr, errbuf);
            return 1;
        }
        else if (retention < 0)
        {
            snprintf(errbuf, sizeof(errbuf), "The value for %s must be a 15-bit positive integer.", "DRMS_RETENTION");
            fprintf(stderr, errbuf);
            return 1;
        }
        else
        {
            retention = (int16_t)(retention & 0x7FFF);
        }
    }

    env->retention = retention;
    
    newsuretention = INT16_MIN;

    if (drms_cmdparams_exists(&cmdparams, "DRMS_NEWSURETENTION") && drms_cmdparams_get_int(&cmdparams, "DRMS_NEWSURETENTION", NULL) != -1)
    {
        newsuretention = drms_cmdparams_get_int16(&cmdparams, "DRMS_NEWSURETENTION", &status);
        if (status != DRMS_SUCCESS)
        {
            if (status == DRMS_ERROR_INVALIDCMDARGCONV)
            {
                snprintf(errbuf, sizeof(errbuf), "The value for %s must be a 15-bit positive integer.", "DRMS_NEWSURETENTION");
                fprintf(stderr, errbuf);
            }
            
            snprintf(errbuf, sizeof(errbuf), "Invalid value for %s.", "DRMS_NEWSURETENTION");
            fprintf(stderr, errbuf);
            return 1;
        }
        else if (newsuretention < 0)
        {
            snprintf(errbuf, sizeof(errbuf), "The value for %s must be a 15-bit positive integer.", "DRMS_NEWSURETENTION");
            fprintf(stderr, errbuf);
            return 1;
        }
        else
        {
            newsuretention = (int16_t)(newsuretention & 0x7FFF);
            fprintf(stderr, "New retention is %hu.\n", newsuretention);
        }
    }
    
    env->newsuretention = newsuretention;

    env->dbtimeout = drms_cmdparams_get_int(&cmdparams, kDBTimeOut, NULL);
    if (env->dbtimeout < 0)
    {
        env->dbtimeout = INT_MIN;
    }
  env->query_mem   = cmdparams_get_int(&cmdparams, "DRMS_QUERY_MEM", NULL);
  env->server_wait = cmdparams_get_int(&cmdparams, "DRMS_SERVER_WAIT", NULL);
  env->verbose     = verbose;

  env->dbpasswd = dbpasswd;
  env->user = user;
  env->logfile_prefix = "drms_server";
  env->dolog = dolog;
  env->quiet = 1;
  env->selfstart = selfstart;
  env->loopconn = cmdparams_isflagset(&cmdparams, kLoopConnFlag);

  /* Start listening on the socket for clients trying to connect. */
  sockfd = db_tcp_listen(hostname, sizeof(hostname), &port);
  printf("DRMS server listening on %s:%hu.\n",hostname,port);
  fflush(stdout);

  /* Fork off child process to do the real work and exit. This allows the 
     server to be started in a shell with out "&" */    
  if (fg == 0)
  {
     /* create a pipe to which the child writes to signify that the dbase transaction 
      * has successfully started */
     char rbuf[128];
     int childready = 0;

     pipe(infd);

    if ((pid = fork()) == -1)
    {
      fprintf(stderr,"Fork system call failed, aborting\n");
      return 1;
    }
    else if (pid>0)
    {
       /* If we get here then a child was successfully spawned.
          Exit from the parent process. */

       close(infd[1]); /* close fd to write end of pipe (inside parent) */
       
       /* wait for child to successfully start a dbase transaction */
       if (read(infd[0], rbuf, sizeof(rbuf)) > 0)
       {
          sscanf(rbuf, "%d", &childready);

          if (childready)
          {
             return 0;
          }
          else
          {
             return 1;
          }
       }

       return 1;
    }

    close(infd[0]); /* close fd to read end of pipe (inside child) */
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
  sigaddset(&env->signal_mask, SIGUSR2); /* Signal from main to signal thread to tell it to go away. */

  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, &env->old_signal_mask)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);          
    Exit(1);
  }

  env->main_thread = pthread_self();

  /* Register sigblock function - whenever the dbase is accessed, certain signals
   * (like SIGUSR2) shouldn't interrupt such actions. */
  // db_register_sigblock(&drms_createsigmask, &env->main_thread);
  
  /* Set up a main-thread signal-handler. It handles SIGUSR2 signals, which are
   * sent by the signal thread if that thread in turn is aborting the process.
   * This is done so that the main thread doesn't continue to attempt SUMS and
   * dbase access while those threads are being terminated.  
   * The handler causes the main thread to stop doing whatever it is doing
   * and go to sleep indefinitely. The signal thread will return the exit code
   * from the process.
   */

  /* Free cmd-params (valgrind reports this - let's just clean up so it doesn't show up on 
   * valgrind's radar). */
  CleanerData_t cleaner = {(pFn_Cleaner_t)FreeCmdparams, (void *)NULL};
  
  drms_server_registercleaner(env, &cleaner);

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
  
  /* drms_server_begin_transaction() can be slow when the dbase is busy - 
   * The sluggishness is caused by SUM_open() backing up. */
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

  /* Now ready to accept connections - print out env stuff so that clients 
   * (sock-connect modules) know which drms_server to connect to
   */
  if (strcmp(cenvfileprefix, kNOTSPECIFIED) != 0)
  {
     snprintf(envfile, sizeof(envfile), "%s.%llu", cenvfileprefix, (unsigned long long)pid);
     fptr = fopen(envfile, "w");
     if (fptr)
     {
        fprintf(fptr, 
                "setenv DRMS_HOST %s;\n"
                "setenv DRMS_PORT %hu;\n"
                "setenv DRMS_PID %lu;\n"
                "setenv DRMSSESSION %s:%hu;\n",
                env->session->hostname, 
                env->session->port, 
                (unsigned long)pid, 
                env->session->hostname,
                env->session->port);
        fclose(fptr);
        time(&now);
        fprintf(stdout, "wrote environment file at %s", ctime(&now));
     }
  }
  else if (strcmp(shenvfileprefix, kNOTSPECIFIED) != 0)
  {
     snprintf(envfile, sizeof(envfile), "%s.%llu", shenvfileprefix, (unsigned long long)pid);
     fptr = fopen(envfile, "w");

     if (fptr)
     {
        fprintf(fptr, 
                "DRMS_HOST=%s; export DRMS_HOST;\n"
                "DRMS_PORT=%hu; export DRMS_PORT;\n"
                "DRMS_PID=%lu; export DRMS_PID;\n"
                "DRMSSESSION=%s:%hu; export DRMSSESSION;\n",
                env->session->hostname, 
                env->session->port, 
                (unsigned long)pid, 
                env->session->hostname,
                env->session->port);
        fclose(fptr);
        time(&now);
        fprintf(stdout, "wrote environment file at %s", ctime(&now));
     }
  }

  /* If forked and in child process, signal to parent that a dbase transaction started
   * successfully. */
  if (fg == 0)
  {
     /* parent will never get here if fg == 0 */
     write(infd[1], "1", 1);
  }

  ans = 0;

  for (;;) {
     /* on every iteration, see check for a shutdown - if not, continue, otherwise
      * halt.
      */
     sdstate = GetShutdownState();
    
     if (sdstate == kSHUTDOWN_COMMIT || sdstate == kSHUTDOWN_ABORT)
     {
        /* shutdown has already been started, and main() has been notified 
         * Print a warning giving the user a chance to allow threads to complete. If they don't want to
         * wait, then finish right away. Otherwise, wait until all server threads terminate. */
        drms_lock_server(env);
        if (env->clientcounter > 0)
        {
           fprintf(stderr, "WARNING: One or more client threads are actively using drms_server.\nShutting down now will interrupt client processessing (the clients will not be notified).\nDo you want to wait for clients to complete processing before shutting down (Y/N)?\n");
           ans = fgetc(stdin);
        }
        drms_unlock_server(env);

        if ((char)ans == 'Y' || (char)ans == 'y')
        {
           while (1)
           {
              drms_lock_server(env);
              
              /* check for server-thread completion */
              if (env->clientcounter == 0)
              {
                 drms_unlock_server(env);
                 break;
              }
              
              drms_unlock_server(env);
              sleep(1);
           }
        }

        /* time to quit out of for loop */
        break;
     }
     else if (sdstate != kSHUTDOWN_UNINITIATED)
     {
        /* Shutdown in progress, but perhaps main() hasn't had a chance to clean up; give it that chance. */
        continue;
     }

     /* accept new connection */

     /* Instead of calling accept(), a blocking call, directly, use poll() with a timeout 
      * so that we don't block here.  Blocking here is bad since it prevents the
      * shutdown detection code (in the for loop before the accept call) from 
      * running, possibly leading to deadlock
      */
     struct pollfd fdinfo[1] = {{0}};
     int res = 0;

     fdinfo[0].fd = sockfd;
     fdinfo[0].events = POLLIN | POLLPRI;

     res = poll(fdinfo, 1, 500);

     if (res == -1)
     {
        /* error calling poll */
        if (errno == EINTR) 
        {
           continue;
        }
        else 
        {
           perror ("poll call failed.");
           close (sockfd);
           pthread_kill(env->signal_thread, SIGTERM);
        }      
     }
     else if (res == 0)
     {
        /* poll timed-out (no client tried to connect during while polling) */
        continue;
     }
     else
     {
        /* some process wrote to the socket */
        if ((clientsockfd = accept (sockfd, NULL, NULL)) < 0 ) {
           if (errno == EINTR) continue;
           else {
              perror ("accept call failed.");
              close (sockfd);
              //Exit (1);
              pthread_kill(env->signal_thread, SIGTERM);
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
           drms_unlock_server (env);

           tinfo = malloc (sizeof (DRMS_ThreadInfo_t));
           XASSERT(tinfo);
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
              drms_lock_server (env);
              --(env->clientcounter);
              drms_unlock_server (env);
           }
        } else {
           fprintf (stderr, "pid %d: Connection from %s:%d denied. "
                    "Invalid username or password.\n", getpid(),
                    inet_ntoa (client.sin_addr), ntohs (client.sin_port));  
           close (clientsockfd);
        }
     }
  } /* end for loop */

  /* Can only get here if signal thread initiated shutdown - just wait for signal thread
   * to terminate (the signal thread actually terminates the process) */
  pthread_join(env->signal_thread, NULL);

  /* Can never get here*/
  return 0;

usage:
  fprintf (stderr, "Usage:    %s [-h]\n"
      "          %s [-fLnQsV] [DRMS_ARCHIVE={-1,0,1}] [JSOC_params - see man page]\n"
      "Options:  -h: Print this help message.\n"
      "          -f: Run server in the foreground.\n"
      "          -L: Redirect stdout and stderr to SU log files.\n"
      "          -n: Turn off Nagle's algorithm on TCP/IP sockets.\n"
      "          -Q: Run in quiet mode.\n"
      "          -s: Commit after every module exit and continue after errors.\n"
      "          -V: Verbose debug output.\n"
      , argv[0], argv[0]);
  return 1;
}
  
