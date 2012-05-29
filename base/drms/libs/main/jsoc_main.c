/**
\defgroup common_main DRMS Module Reference
\ingroup a_programs

\brief Common main program used by all DRMS modules

\par Synopsis:
\code
<module> [GEN_FLAGS] [SESSION_ARGUMENTS] [MODULE_ARGUMENTS]
\endcode

Execute the named @a module from a shell.  When a module is executed
from the command line, all argument values listed in the module are supplied
from (in order of precedence): values on the command line; values in a
named parameter file; or the default values
specified in the module source, if any.  If an argument cannot be otherwise
evaluated, a notification message is issued and the driver program exits.
If all the argument values exist, a DRMS session is opened or a connection
is made to an existing DRMS server as appropriate, and the remaineder of the module 
code is executed.
At the end of module execution, the driver disconnects from DRMS with
or without commits to the database depending on the exit status of the module.
No commits are ever made when a module aborts or is killed on a signal.

Modules are expected to run as functions (or subroutines) called by a main
driver program that parses the argument list from the command line and
environment, and sets up the @c drms->env environment for communication
with the DRMS database via the DRMS API. Different driver programs can be
made available, depending on the environment. Two such programs (in the form of 
libraries) are provided
as part of the base system, @c jsoc_main.a and @c jsoc_main_sock.a (see jsoc_main.h).
The former provides a direct connection to DRMS, for use by self-contained
modules. The latter provides communication via a socket; it can be used for
multiple modules connecting to a common server, as in a script. When multiple
clients connect to a common server, commits to the database will only occur
at the successful close of the server. Modules linked against @c jsoc_main.a
are given their basename for the executable, while those linked against
@c jsoc_main_sock have "_sock" appended to the basename. For example,
a module named @c myprogram.c would produce executables named @c myprogram and
@c myprogram_sock. (Additional drivers to produce, for example, CGI scripts
such as @c myprogram_cgi can also be provided.)

\par Socket-connect vs direct-connect modules

In socket connect mode, a module talks to \ref drms_server program via
a socket. Concurrent access is protected by locks drms_lock_server()
and drms_unlock_server(). The following command codes employ locking:
::DRMS_ROLLBACK, ::DRMS_COMMIT, ::DRMS_NEWSERIES, ::DRMS_DROPSERIES,
::DRMS_NEWSLOTS, ::DRMS_SLOT_SETSTATE, ::DRMS_GETUNIT.  This is a very
high level locking that gives the correct results but not necessarily
the max amount of parallelism. For example, we could lock at a lower
level inside drms_su_getdirs(). Direct-connect is most handy when
there is only a single client module. In that case, the socket
presents a mere overhead. We thus fuse \ref drms_server program with
the client module. The object file for both socket-connect and
direct-connect are the same. They simply link to different libraries
to produce the two executables with and without the _sock
suffix. \note drms_server_begin_transaction() and
drms_server_end_transaction() can only be used in direct-connect
mode. Why? Because we don't want to deal with concurrency issues when
beginning/ending a transaction.

A socket-connect module looks for DRMSSESSION which identifies the
host and port of a running \ref drms_server program. In case
DRMSSESSION information is absent, the module would attempt to fork a
\ref drms_server program and connect itself to it via a socket. When
the module finishes executing the DoIt() function, it sends either
SIGTERM upon failure, or SIGUSR1 upon success, to the \ref drms_server
process and waits until the latter returns before exiting.

The @a -H option prints out the list of required arguments and their
default or set values, then quits. (The argument @a --help has
the same effect.)  The @a -V option
prints the full argument list before execution of the module; it also
sets the @c drms_env->verbose element and may force printing of some diagnostic
messages by the module driver. The @a -n flag is reserved by @c jsoc_main.a
but is not currently used.

The @a -L flag forces logging of all module messages to @e stdout and
@e stderr to a DRMS session log.
The @a -Q option suppresses messages from the module to @e stdout and
@e stderr, although the messages are still sent to the DRMS session log.
Without session logging turned on, the @a -Q flag has no effect on logging.
The @a -A flag forces archiving of all segments (including the logging)
in SUMS to tape, if that is an option. If it is not set, then archiving
is turned off. It is not clear how the presence or absence of this flag
interacts with the archiving options defined for the data series.

All flags (except of course @a -H) are available to be passed down to
the module. White space between the '=' sign and the argument value is
optional. Flags can be concatenated as shown below.

\par GEN_FLAGS (can be grouped together):
\c -A: Force archiving of storage units during the current DRMS session.  
This overrides the value of series' archive flag.
<br>
\c -H|--help: Show jsoc_main usage information.
<br>
\c -L: Run jsoc_main driver with logging. Stdout and stderr are tee-ed to files in SU directory.
<br>
\c -V: Run jsoc_main driver in verbose mode.
<br>
\c -Q: Run  jsoc_main  driver  in  quiet mode (no terminal output).
<br>
\c -n: A nop; the module cannot define a flag named @a n.
<br>
\c --jsocmodver: print the JSOC module version number, then quit

\par SESSION_ARGUMENTS:
To specify an argument that affects properties of the DRMS session,
use @c param=value, where @c param is one of the following.

@arg @c DRMS_ARCHIVE Sets the archive status for all storage units created during the DRMS
session started by @a module. @a value can be either -1 (don't archive data, and when a 
storage unit's retention expires, delete all DRMS records whose data resides within this
storage unit), 0 (don't archive, but do not delete any DRMS records), or 1 (archive data when 
the storage unit's retention expires).
\arg \c DRMS_RETENTION For all storage units allocated during the module's session, the
 value of this argument overrides the jsd value. Each newly created storage unit will
 be given abs(argument value) for its days of retention. For each storage unit created in previous
 module sessions and referenced by the record-set specification, the retention
 will be set to argument value if the argument value is greater than zero, unless the argument value
 is less than the current retention value. If that is true, then the retention of the storage unit
 will be decreased to the argument value only if the the module is connected to the database
 as the db user who owns the dataseries' series table, OR if db user is a production user 
 (i.e., prodution or jsocprod). If the db user is not the owner or a production user, then
 the storage unit retention will not be modified. In other words, a user cannot decrease 
 a storage unit's retention value, unless that user is the owner of the dataseries, or the user
 is a production user.  If the retention argument value is less than 
 zero, then the storage unit's retention value will be set to abs(value), but only if the 
 change would result in an increase in the retention time. In other words, a negative argument value can
 only increase a storage unit's retention value. The "series owner" is the db user who owns the "series table".
 The series table is the PostgreSQL table that contains the DRMS series' variable record values, and 
 its name is lc(series name) (for example, if the series is hmi.M_45s, then the series 
 table is named hmi.m_45s). The list of production users is stored in the database table
 su_production.produsers in db jsoc on hmidb. You must be a superuser to modify the contents of
 this table. Changes in a session to a previously existing 
 storage unit require that the referred-to storage unit be "retrieved" from SUMS with a SUM_get()
 call (e.g., call drms_segment_read() or drms_stage_records() with this storage unit's SUNUM as
 an argument. This can be achieved by calling show_info -P). One further caveat: this argument
 cannot be specified as an environment variable. 
\arg \c DRMS_QUERY_MEM Sets the memory maximum for a database query.
\arg \c JSOC_DBHOST Specifies (overrides) the database host to connect to. Default is ::DBNAME
\arg \c JSOC_DBNAME Specifies (overrides) the database name to use. Default is ::DBNAME
\arg \c JSOC_DBUSER Specifies (overrides) the username used during database host connection. Default is ::USER.
\arg \c JSOC_DBPASSWD Specifies (overrides) the password for the username. Default is ::PASSWD.
\arg \c JSOC_SESSIONNS Specifies (overrides) the DRMS session namespace.

\par MODULE_ARGUMENTS:
These are module-specific; refer to @ref drms_util for module-specific documentation.
 
 \par Examples:
 
 \b Example 1:
 To ensure that the retention of existing storage units is at least 10000 days:
 \code
 show_info -P hmi.m_45s[2011.12.10_00:00/10m] DRMS_RETENTION=-10000
 \endcode
 
 \b Example 2:
 To reduce retention to 0 so that SUMS will delete the storage units from disk (the caller must be the 
 series owner or a production user):
 \code
 show_info -P hmi.m_45s[2011.12.10_00:00/10m] DRMS_RETENTION=0
 \endcode
 
 \b Example 3:
 To change the retention to 100 days (the caller must be the series owner or a production user):
 \code
 show_info -P hmi.m_45s[2011.12.10_00:00/10m] DRMS_RETENTION=100
 \endcode
*/

/**
\file jsoc_main.c
\defgroup jsoc_main DRMS common main program
\ingroup core_api

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

/** 
    @brief Global DRMS Environment handle. 
*/
DRMS_Env_t *drms_env;

/** 
    @brief Global DRMS-module structure representing the command-line 
    arguments provided to the module executable.    
*/
CmdParams_t cmdparams;
/**
   @example moduleargs_ex1.c
*/
ModuleArgs_t *gModArgs = module_args;

/**
   Returns a pointer to the module's global ::CmdParams_t structure. This static 
   structure is an in-memory representation of the cmd-line arguments provided
   to the module during execution. It is initialized upon module startup.
   See ::cmdparams for more information.
*/
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
  drms_server_abort (drms_env, 1);
#ifdef DEBUG
   fprintf (stderr, "drms_server_abort() call completed.\n");
   xmem_leakreport ();
#endif
}

/* db_lock calls this function to determine what signals it should NOT 
 * allow to interrupt code running inside the db lock. */
void drms_createsigmask(sigset_t *set, void *data)
{
   pthread_t threadid = *((pthread_t *)data);
   if (set && threadid == pthread_self())
   {
      sigemptyset(set);
      sigaddset(set, SIGUSR2); 
   }
}

/* If shutdown has started, waits until DoIt() notified */
static DRMS_Shutdown_State_t GetShutdownState(DRMS_Env_t *env)
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
         /* signal-induced shutdown has already been started, and DoIt() already knows about it, 
          * or main thread is shutting down. */
         st = sdstate;
         sem_post(sdsem);
      }
      else
      {
         /* shutdown initiated by signal thread, but DoIt() not notified yet */
         sem_post(sdsem);

         /* call commit or abort as appropriate.
          * sdstate might indicate DoIt() not notified yet. */
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

static void FinalBits(DRMS_Env_t *env, int abort_flag)
{
   /* Acquire shutdown lock, to prevent the signal thread from trying to simultaneously shut down. */
   /* If signal thread has the lock, then main waits here until the process dies. */
   sem_t *sdsem = drms_server_getsdsem();

   if (sdsem)
   {
      sem_wait(sdsem);
   }

   if (drms_server_getsd() != kSHUTDOWN_UNINITIATED)
   {
      /* signal thread is already shutting down, just wait for signal thread to finish 
       * (which it won't do, because it is going to call exit, so this is an indefinite sleep). */
      sem_post(sdsem);
      pthread_join(env->signal_thread, NULL);  
   }
   else
   {
      /* Tell signal thread not to accept shutdown requests, because main is shutting down. */
      drms_server_setsd(kSHUTDOWN_BYMAIN);

      sem_post(sdsem);

      /* Now, signal thread will not try and shut down. */

      /* Calls drms_server_commit() or drms_server_abort().
       * Don't set last argment to final - still need environment below. */
      /* This will cause the SUMS thread to return. */
      /* If no transaction actually started, then noop. */
      drms_server_end_transaction(env, abort_flag, 0);

#ifndef DEBUG
      /* Clean up signal thread - the signal hasn't received a shutdown signal, so it is still
       * running. Send USR2 to tell signal thread to end itself. */
      pthread_kill(env->signal_thread, SIGUSR2);
      pthread_join(env->signal_thread, NULL);
#endif

      /* Must disconnect db, because we didn't set the final flag in drms_server_end_transaction() */
      db_disconnect(&env->session->db_handle);

      /* Free cmd-params (valgrind reports this - let's just clean up so it doesn't show up on 
       * valgrind's radar). */
      cmdparams_freeall(&cmdparams);

      drms_free_env(env, 1); 

      /* doesn't call at_exit_action() */
      _exit(abort_flag);
   }
}

static int FreeCmdparams(void *data)
{
   cmdparams_freeall(&cmdparams);
   memset(&cmdparams, 0, sizeof(CmdParams_t));
   return 0;
}

/**
@fn main(int argc, char **argv, const char *module_name, int (*CallDoIt)(void)
\brief jsoc_main entry point
Common main program for all DRMS program modules.  Actuallt there are several versions of
this, each tailored for a specific use.  this one is linked for shell callable programs.

*/
int JSOCMAIN_Main(int argc, char **argv, const char *module_name, int (*CallDoIt)(void)) {
  int verbose=0, dolog=0, quiet=0;
  int status;
  char *user, unknown[]="unknown";
  DB_Handle_t *db_handle;
  const char *dbhost, *dbuser, *dbpasswd, *dbname, *sessionns;
  int printrel = 0;
  char reservebuf[128];

   /* Initialize globals here */
   memset(&cmdparams, 0, sizeof(CmdParams_t));

#ifdef DEBUG
  xmem_config(1,1,1,1,1000000,1,0,0); 
#endif
  /* Parse command line parameters. */
  snprintf(reservebuf, 
           sizeof(reservebuf), 
           "%s,%s,%s,%s,%s,%s", 
           "L,Q,V,jsocmodver", 
           kARCHIVEARG,
           kRETENTIONARG,
           kQUERYMEMARG,
           kLoopConn,
           kDBTimeOut);
  cmdparams_reserve(&cmdparams, reservebuf, "jsocmain");

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

  printrel = cmdparams_isflagset(&cmdparams, "jsocmodver");

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

  verbose = (cmdparams_exists (&cmdparams, "V") &&
	     cmdparams_get_int (&cmdparams, "V", NULL) != 0);
  if (verbose) cmdparams_printall (&cmdparams);
  quiet = (cmdparams_exists (&cmdparams, "Q") &&
	   cmdparams_get_int (&cmdparams, "Q", NULL) != 0);
  dolog = (cmdparams_exists (&cmdparams, "L") &&
	   cmdparams_get_int (&cmdparams, "L", NULL) != 0);

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

  int archive = INT_MIN;
  if (drms_cmdparams_exists(&cmdparams, kARCHIVEARG)) {
     archive = drms_cmdparams_get_int(&cmdparams, kARCHIVEARG, NULL);
     if (archive != -1 && archive != 0 && archive != 1)
     {
        archive = INT_MIN;
     }
  }
  int retention = INT_MIN;
  if (drms_cmdparams_exists(&cmdparams, kRETENTIONARG)) {
    retention = drms_cmdparams_get_int(&cmdparams, kRETENTIONARG, NULL);
  }
  int query_mem = 512;
  if (cmdparams_exists (&cmdparams, kQUERYMEMARG)) {
    query_mem = cmdparams_get_int(&cmdparams, kQUERYMEMARG, NULL);
  }
    
    int dbtimeout = INT_MIN;
    if (drms_cmdparams_exists(&cmdparams, kDBTimeOut))
    {
        dbtimeout = drms_cmdparams_get_int(&cmdparams, kDBTimeOut, NULL);
    }

  int loopconn = cmdparams_isflagset(&cmdparams, kLoopConn);

  /* Initialize server's own DRMS environment and connect to 
     DRMS database server. */
  if ((drms_env = drms_open(dbhost,dbuser,dbpasswd,dbname,sessionns)) == NULL)
  {
    fprintf(stderr,"Failure during server initialization.\n");
    return 1;
  }

  /* Initialize global things. */


  /***************** Set up exit() and signal handling ********************/

  atexit(atexit_action);  
 
  /* Block signals INT, QUIT, TERM, and USR1. They will explicitly
     handled by the signal thread created below. */
#ifndef DEBUG
  sigemptyset(&drms_env->signal_mask);
  sigaddset(&drms_env->signal_mask, SIGINT);
  sigaddset(&drms_env->signal_mask, SIGQUIT);
  sigaddset(&drms_env->signal_mask, SIGTERM);
  sigaddset(&drms_env->signal_mask, SIGUSR1);
  sigaddset(&drms_env->signal_mask, SIGUSR2);

  if( (status = pthread_sigmask(SIG_BLOCK, &drms_env->signal_mask, &drms_env->old_signal_mask)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);          
    Exit(1);
  }

  drms_env->main_thread = pthread_self();

  /* Register sigblock function - whenever the dbase is accessed, certain signals
   * (like SIGUSR2) shouldn't interrupt such actions. */
  /* This is no longer an issue - there is no signal handler any more. Instead, shutdown 
   * is handled completely by the signal thread. If the signal thread attempts to 
   * access the db, but the main thread has the db lock, the signal thread will 
   * block until main relinquishes the lock. The signal-handler implementation required
   * blocking of the SIGUSR2 signal because it was asynchronously called, which could
   * cause interruption of a thread holding the db lock. Then the signal handler
   * would try to obtain the db lock, resulting in deadlock.
   */
  // db_register_sigblock(&drms_createsigmask, &drms_env->main_thread);


   /* Free cmd-params (valgrind reports this - let's just clean up so it doesn't show up on 
    * valgrind's radar). */
  CleanerData_t cleaner = {(pFn_Cleaner_t)FreeCmdparams, (void *)NULL};
  
  drms_server_registercleaner(drms_env, &cleaner);

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

  db_handle = drms_env->session->db_handle;

  drms_env->archive = archive;
  drms_env->retention = retention;
  drms_env->query_mem = query_mem;
    drms_env->dbtimeout = dbtimeout;
  drms_env->verbose = verbose;
  drms_env->server_wait = 0;

  drms_env->dbpasswd = dbpasswd;
  drms_env->user = user;
  drms_env->logfile_prefix = module_name;
  drms_env->dolog = dolog;
  drms_env->quiet = quiet;

  drms_env->loopconn = loopconn;

  int abort_flag = 1;

  /* check for a shutdown before doing each bit of work below - these
   * are the only places where shutdown is allowed */
  if (GetShutdownState(drms_env) == kSHUTDOWN_UNINITIATED)
  {
     if (verbose)
       printf("Setting isolation level to SERIALIZABLE.\n");
     /* Set isolation level to serializable. */
     if ( db_isolation_level(db_handle, DB_TRANS_SERIALIZABLE) )
     {
        fprintf(stderr,"Failed to set database isolation level.\n");
        Exit(1);
     }

     if (GetShutdownState(drms_env) == kSHUTDOWN_UNINITIATED)
     {
        char hostname[1024];
        if (gethostname(hostname, sizeof(hostname))) {
           Exit(1);
        }
        strncpy(drms_env->session->hostname, hostname, DRMS_MAXHOSTNAME);

        if (GetShutdownState(drms_env) == kSHUTDOWN_UNINITIATED)
        {
           drms_server_begin_transaction(drms_env);
           abort_flag = CallDoIt();
        }
     }
  }

  /* Doesn't return - closes SUMS and signal threads, cleans up environment, closes dbase connection, frees cmdparams */
  /* If we get here, then prevent the signal thread from initiating a shutdown - main will do it instead. */
  FinalBits(drms_env, abort_flag);

  /* execution doesn't get here */
  return 0;
}

