//#define DEBUG

#define __DRMS_SERVER_C
#include "drms.h"
#include "drms_priv.h"
#include "tee.h"
#undef __DRMS_SERVER_C
#include "xmem.h"
#include <pwd.h>
#ifdef __linux__
#include <sched.h>
#endif
#include <dirent.h>
#include "printk.h"


#ifdef DRMS_CLIENT
#define DEFS_CLIENT
#endif
#include "drmssite_info.h"

#ifdef DEFS_CLIENT
#undef DEFS_CLIENT
#endif

#define kDELSERFILE "thefile.txt"
#define kDelSerChunk       10000
#define kMaxSleep         (90)
#define kSUMSDead         -2
#define kBrokenPipe       -99
#define kTooManySumsOpen  -98

sem_t *gShutdownsem = NULL; /* synchronization among signal thread, main thread, 
                               sums thread, and server threads during shutdown */
DRMS_Shutdown_State_t gShutdown; /* shudown state following receipt by DRMS of a signal that 
                                 * causes module termination */

pthread_mutex_t *gSUMSbusyMtx = NULL;
int gSUMSbusy = 0;

/* 0, unless a SIGPIPE signal was CAUGHT. */
static volatile sig_atomic_t gGotPipe = 0;

/* Container of pending SUM_get() requests, indexed by the SUM_t::uid. */
HContainer_t *gSgPending = NULL;

/******************* Main server thread(s) functions ************************/
static void drms_delete_temporaries(DRMS_Env_t *env);


/* returns the SUMS opcode, or -99 if a broken-pipe error occurred, or -1 if opcode is NA, 
 * or -2 if SUMS cannot be called again because DRMS timed-out waiting for SUMS. */
static int MakeSumsCall(DRMS_Env_t *env, int calltype, SUM_t **sumt, int (*history)(const char *fmt, ...), ...)
{
    int opcode = 0;
    va_list ap;
    static int nsumsconn = 0;
    
    if (calltype != DRMS_SUMCLOSE)
    {
        /* Always allow DRMS_SUMCLOSE call. */
        drms_lock_server(env);
        if (!env->sumssafe)
        {
            fprintf(stderr, "Unable to call SUMS - SUMS is still processing a tape-read.\n");
            opcode = kSUMSDead;
        }
        drms_unlock_server(env);
    }
    
    if (opcode == kSUMSDead)
    {
        return opcode;
    }
    
    gGotPipe = 0;
    
    switch (calltype)
    {
        case DRMS_SUMOPEN:
        {
            /* fetch args */
            va_start(ap, history);
            char *server = va_arg(ap, char *);
            char *db = va_arg(ap, char *);
            
            /* SUMS allows a maximum of MAXSUMOPEN number of connections. The module should terminate if 
             * there is an attempt to open more than this number. */
            if (nsumsconn >= MAXSUMOPEN)
            {
                *sumt = NULL;
                fprintf(stderr, "Attempting to exceed maximum number of available SUM_open() calls per process.\n");
                opcode = kTooManySumsOpen;
            }
            else
            {
                *sumt = SUM_open(server, db, history);
                opcode = -1; /* not used for this call */
            }
            
            if (*sumt)
            {
                ++nsumsconn;
            }
            
            va_end(ap);
        }
            break;
        case DRMS_SUMALLOC:
        {
            opcode = SUM_alloc(*sumt, history);
        }
            break;
        case DRMS_SUMGET:
        {
            opcode = SUM_get(*sumt, history);
        }
            break;
        case DRMS_SUMPUT:
        {
            opcode = SUM_put(*sumt, history);
        }
            break;
        case DRMS_SUMCLOSE:
        {
            opcode = SUM_close(*sumt, history);
            nsumsconn = 0;
        }
            break;
        case DRMS_SUMDELETESERIES:
        {
            va_start(ap, history);
            char *fpath = va_arg(ap, char *);
            char *series = va_arg(ap, char *);
            
            opcode = SUM_delete_series(fpath, series, history);
            
            va_end(ap);
        }
            break;
        case DRMS_SUMALLOC2:
        {
            va_start(ap, history);
            uint64_t sunum = va_arg(ap, uint64_t);
            
            opcode = SUM_alloc2(*sumt, sunum, history);
            
            va_end(ap);
        }
            break;
        case DRMS_SUMEXPORT:
        {
            va_start(ap, history);
            SUMEXP_t *sumexpt = va_arg(ap, SUMEXP_t *);
            
            opcode = SUM_export(sumexpt, history);
            
            va_end(ap);
        }
            break;
        case DRMS_SUMINFO:
        {
            va_start(ap, history);
            uint64_t *dxarray = va_arg(ap, uint64_t *);
            int reqcnt = va_arg(ap, int);
            
            opcode = SUM_infoArray(*sumt, dxarray, reqcnt, history);
            
            va_end(ap);
        }
            break;
        default:
            fprintf(stderr, "Invalid SUMS call type '%d'.\n", calltype);
            
    }
    
    if (gGotPipe)
    {
        /* Print an error message with a timestamp. */
        fprintf(stderr, "Received a SIGPIPE signal; error calling SUMS call %d.\n", calltype);
        opcode = kBrokenPipe;
    }
    
    return opcode;
}

static DRMS_SumRequest_t *drms_process_sums_request(DRMS_Env_t  *env,
						    SUM_t **sum,
						    DRMS_SumRequest_t *request);

static void GettingSleepier(int *sleepiness)
{
   *sleepiness *= 2;
   if (*sleepiness > kMaxSleep)
   {
      *sleepiness = kMaxSleep;
   }
}

sem_t *drms_server_getsdsem(void)
{
   return gShutdownsem;
}

void drms_server_initsdsem(void)
{
  /* create shutdown (unnamed POSIX) semaphore */
  gShutdownsem = malloc(sizeof(sem_t));

  /* Initialize semaphore to 1 - "unlocked" */
  sem_init(gShutdownsem, 0, 1);
  
  /* Initialize shutdown state to kSHUTDOWN_UNINITIATED - no shutdown requested */
  gShutdown = kSHUTDOWN_UNINITIATED;
}

void drms_server_destroysdsem(void)
{
   if (gShutdownsem)
   {
      sem_destroy(gShutdownsem);
      free(gShutdownsem);
      gShutdownsem = NULL;
   }
}

DRMS_Shutdown_State_t drms_server_getsd(void)
{
   return gShutdown;
}

void drms_server_setsd(DRMS_Shutdown_State_t st)
{
   gShutdown = st;
}

int drms_server_authenticate(int sockfd, DRMS_Env_t *env, int clientid)
{
  int status = 0;
  int tmp[5],*t;
  long long ltmp[2];
  struct iovec vec[8],*v;
  /* Send session information to the client. */
  t=tmp; v=vec;
  net_packint(status, t++, v++);
  net_packint(clientid, t++, v++);
  net_packlonglong(env->session->sessionid, &ltmp[0], v++);
  net_packstring(env->session->sessionns, t++, v);
  v += 2;
  net_packlonglong(env->session->sunum, &ltmp[1], v++);
  net_packstring(env->session->sudir ? env->session->sudir : kNOLOGSUDIR, t, v);
  Writevn(sockfd, vec, 8);

  return status;
}

/* Assumes that drms_open() already called (but doesn't check for that). Also
 * assumes that drms_server_end_transaction() was never called with the 'final'
 * flag set. */

/* Only the main thread will access these parts of the env, so no need to 
 * lock the env. */
int drms_server_begin_transaction(DRMS_Env_t *env) {

   int notfirst = 0;

  /* Start a transaction where all database operations performed
     through this server should be treated as a single transaction. */

  if (db_start_transaction(env->session->db_handle)) {
    fprintf(stderr,"Couldn't start database transaction.\n");
    // Exit(1); - never call exit(), only the signal thread can do that.
    // Instead, send a TERM signal to the signal thread.
    pthread_kill(env->signal_thread, SIGTERM);
  }
  else
  {
     /* drms_lock_server is a noop if env->drms_lock == NULL */
     drms_lock_server(env);
     env->transrunning = 1;
     drms_unlock_server(env);
      
      /* Issue the statement_timeout statement, but only if env->dbtimeout is not 
       * INT_MIN (the default). A default value implies a timeout is not desired. */
      if (env->dbtimeout != INT_MIN)
      {
          if (db_settimeout(env->session->db_handle, env->dbtimeout))
          {
              fprintf(stderr, "Failed to modify db-statement time-out to %d.\n", env->dbtimeout);
          }
      }
  }

  /* It is possible that the user has previously called drms_server_end_transaction(),
   * but not freed the environment. In this case, you don't want to re-allocate and
   * re-initialize the env structure. */
  if (env->drms_lock)
  {
     notfirst = 1;
     drms_lock_server(env);
  }

  if (!env->transinit)
  {
     env->sum_inbox = tqueueInit (100);
     env->sum_outbox = tqueueInit (100);

     /* global locks */
     /* to lock anything in env */
     env->drms_lock = malloc(sizeof(pthread_mutex_t));
     XASSERT(env->drms_lock);
     pthread_mutex_init (env->drms_lock, NULL); 

     /* to serialize server threads (drms_server clients) access to drms_server's DRMS library */
     env->clientlock = malloc(sizeof(pthread_mutex_t));
     XASSERT(env->clientlock);
     pthread_mutex_init(env->clientlock, NULL);

     drms_lock_server(env);
     if (drms_cache_init(env)) 
     {
        drms_unlock_server(env);
        goto bailout;
     }

     env->transinit = 1;
     drms_unlock_server(env);
  }

  /* drms_server_open_session() can be slow when the dbase is busy - 
   * this is caused by SUM_open() calls backing up. */
  if (!notfirst)
  {
     drms_lock_server(env); 
  }

  if (drms_server_open_session(env)) 
  {
     drms_unlock_server(env);
     return 1;
  }

  env->sessionrunning = 1;
  drms_unlock_server(env);

  return 0;
 bailout:
  drms_free_env(env, 1);
  return 1;
}

void drms_server_end_transaction(DRMS_Env_t *env, int abort, int final) {
  if (abort) {
    drms_server_abort(env, final);
  } else {
    drms_server_commit(env, final);
  }
}

/* Get a new DRMS session ID from the database and insert a session 
   record in the drms_session_table table. */
/* MUST HAVE CALLED drms_lock_server() before entering this function!! */
int drms_server_open_session(DRMS_Env_t *env)
{
#ifdef DEBUG  
  printf("In drms_server_open_session()\n");
#endif

  int status;
  struct passwd *pwd = getpwuid(geteuid());

  /* Register the session in the database. */
  char hostbuf[1024];
  snprintf(hostbuf, 
           sizeof(hostbuf), 
           "%s:%s", 
           env->session->db_handle->dbhost, 
           env->session->db_handle->dbport);

  if ((env->session->stat_conn = db_connect(hostbuf,
					    env->session->db_handle->dbuser,
					    env->dbpasswd,
					    env->session->db_handle->dbname,1)) == NULL)
  {
    fprintf(stderr,"Error: Couldn't establish stat_conn database connection.\n");
    // Exit(1); - never call exit(), only the signal thread can do that.
    // Instead, send a TERM signal to the signal thread.
    pthread_kill(env->signal_thread, SIGTERM);
  }

  /* Get sessionid etc. */
  char *sn = malloc(sizeof(char)*strlen(env->session->sessionns)+16);
  sprintf(sn, "%s.drms_sessionid", env->session->sessionns);

  env->session->sessionid = db_sequence_getnext(env->session->stat_conn, sn);
  free(sn);

  char stmt[DRMS_MAXQUERYLEN];

  pid_t pid = getpid();

  /* drms_server always creates a SU for the logfile */
    if (env->dolog) 
    {
        /* Allocate a 1MB storage unit for log files. */
        /* drms_su_alloc() can be slow when the dbase is busy */
        int tg = 1; /* Use tapegroup 1 - SUMS maps this number to a sums partition set. */
        long long sunum = -1;
        
        /* Must release lock, cuz the sums thread will acquire it (in the wrapper around
         * SUM_alloc()). */
        drms_unlock_server(env);
        sunum = drms_su_alloc(env, 1<<20, &env->session->sudir, &tg, &status);
        drms_lock_server(env);
        env->session->sunum = sunum;
        if (status)
        {
            fprintf(stderr,"Failed to allocate storage unit for log files: %d\n", 
                    status);          
            return 1;
        }
#ifdef DEBUG  
        else
        {
            printf("Session ID = %lld, Log sunum = %lld, Log file directory = %s\n",
                   env->session->sessionid,env->session->sunum,env->session->sudir);
        }
#endif
    }

  if (!strcmp(env->logfile_prefix, "drms_server")) 
  {
     // this information is needed by modules that self-start a
     // drms_server process
     printf("DRMS server connected to database '%s' on host '%s' as "
            "user '%s'.\n",env->session->db_handle->dbname, 
            env->session->db_handle->dbhost,
            env->session->db_handle->dbuser);
     printf("DRMS_HOST = %s\n"
            "DRMS_PORT = %hu\n"
            "DRMS_PID = %lu\n"
            "DRMS_SESSIONID = %lld\n"
            "DRMS_SESSIONNS = %s\n"
            "DRMS_SUNUM = %lld\n"
            "DRMS_SUDIR = %s\n",
            env->session->hostname, env->session->port, (unsigned long)pid, env->session->sessionid,
            env->session->sessionns,
            env->session->sunum,env->session->sudir);     
     fflush(stdout);
  }

  if (save_stdeo()) {
    printf("Can't save stdout and stderr\n");
    return 1;
  }

  if (env->dolog) {

    snprintf(stmt, DRMS_MAXQUERYLEN, "INSERT INTO %s."DRMS_SESSION_TABLE 
	     "(sessionid, hostname, port, pid, username, starttime, sunum, "
	     "sudir, status, clients,lastcontact,sums_thread_status,jsoc_version) VALUES "
	     "(?,?,?,?,?,LOCALTIMESTAMP(0),?,?,'idle',0,LOCALTIMESTAMP(0),"
	     "'starting', '%s(%d)')", env->session->sessionns, jsoc_version, jsoc_vers_num);

    if (db_dmsv(env->session->stat_conn, NULL, stmt,
		-1, DB_INT8, env->session->sessionid, 
		DB_STRING, env->session->hostname, 
		DB_INT4, (int)env->session->port, DB_INT4, (int)pid, 
		DB_STRING, pwd->pw_name, DB_INT8, env->session->sunum, 
		DB_STRING,env->session->sudir))
      {
	fprintf(stderr,"Error: Couldn't register session.\n");
	return 1;
      }

    char filename_e[DRMS_MAXPATHLEN], filename_o[DRMS_MAXPATHLEN];
    if (!env->quiet) {
      /* Tee output */
      CHECKSNPRINTF(snprintf(filename_e,DRMS_MAXPATHLEN, "%s/%s.stderr.gz", env->session->sudir, env->logfile_prefix), DRMS_MAXPATHLEN);
      CHECKSNPRINTF(snprintf(filename_o,DRMS_MAXPATHLEN, "%s/%s.stdout.gz", env->session->sudir, env->logfile_prefix), DRMS_MAXPATHLEN);
      if ((env->tee_pid = tee_stdio (filename_o, 0644, filename_e, 0644)) < 0)
      {
        // Exit(-1); - never call exit(), only the signal thread can do that.
        // Instead, send a TERM signal to the signal thread.
        pthread_kill(env->signal_thread, SIGTERM);
      }
    } else {
      /* Redirect output */
      CHECKSNPRINTF(snprintf(filename_e,DRMS_MAXPATHLEN, "%s/%s.stderr", env->session->sudir, env->logfile_prefix), DRMS_MAXPATHLEN);
      CHECKSNPRINTF(snprintf(filename_o,DRMS_MAXPATHLEN, "%s/%s.stdout", env->session->sudir, env->logfile_prefix), DRMS_MAXPATHLEN);
      if (redirect_stdio (filename_o, 0644, filename_e, 0644))
      {
         // Exit(-1); - never call exit(), only the signal thread can do that.
         // Instead, send a TERM signal to the signal thread.
         pthread_kill(env->signal_thread, SIGTERM);
      }
    }
    printf("DRMS server connected to database '%s' on host '%s' as "
	   "user '%s'.\n",env->session->db_handle->dbname, 
	   env->session->db_handle->dbhost,
	   env->session->db_handle->dbuser);
    printf("DRMS_HOST = %s\n"
	   "DRMS_PORT = %hu\n"
	   "DRMS_PID = %lu\n"
	   "DRMS_SESSIONID = %lld\n"
	   "DRMS_SESSIONNS = %s\n"
	   "DRMS_SUNUM = %lld\n"
	   "DRMS_SUDIR = %s\n",
	   env->session->hostname, env->session->port, 
	   (unsigned long)pid, env->session->sessionid,
	   env->session->sessionns,
	   env->session->sunum,env->session->sudir);     
    fflush(stdout);

  } else {
    snprintf(stmt, DRMS_MAXQUERYLEN, "INSERT INTO %s."DRMS_SESSION_TABLE 
	     "(sessionid, hostname, port, pid, username, starttime, "
	     "status, clients,lastcontact,sums_thread_status,jsoc_version) VALUES "
	     "(?,?,?,?,?,LOCALTIMESTAMP(0),'idle',0,LOCALTIMESTAMP(0),"
	     "'starting', '%s(%d)')", env->session->sessionns, jsoc_version, jsoc_vers_num);
    if (db_dmsv(env->session->stat_conn, NULL, stmt,
		-1, DB_INT8, env->session->sessionid,
		DB_STRING, env->session->hostname, 
		DB_INT4, (int)env->session->port, DB_INT4, (int)pid,
		DB_STRING, pwd->pw_name))
      {
	fprintf(stderr,"Error: Couldn't register session.\n");
	return 1;
      }
  }

  return 0;
}
      
/* Update record corresponding to the session associated with env
   from the session table and close the extra database connection. */
/* MUST HAVE CALLED drms_lock_server() before entering this function!! */
int drms_server_close_session(DRMS_Env_t *env, char *stat_str, int clients,
			      int log_retention, int archive_log)
{
  DRMS_StorageUnit_t su;
  DIR *dp;
  struct dirent *dirp;
  int emptydir = 1;

  /* Flush output to logfile. */
  fflush(stdout);
  fflush(stderr);
  close(STDERR_FILENO);
  close(STDOUT_FILENO);

  if (env->tee_pid > 0) {
    int status = 0;
    waitpid(env->tee_pid, &status, 0);
    if (status) printf("Problem returning from tee\n");
  }
  if (restore_stdeo()) {
    printf("Can't restore stderr and stdout\n");
  }

    if (env->session->sudir) {
        if ((dp = opendir(env->session->sudir)) == NULL) {
            fprintf(stderr, "Can't open %s\n", env->session->sudir);
            return 1;
        }
        
        while ((dirp = readdir(dp)) != NULL) {
            if (!strcmp(dirp->d_name, ".") ||
                !strcmp(dirp->d_name, "..")) 
                continue;
            
            /* Gzip drms_server log files. */
            /* The module log files are now handled with gz* functions. */
            /* Only the server logs are not compressed. */
            if (strncmp(dirp->d_name+strlen(dirp->d_name)-3, ".gz", 3)) {
                char command[DRMS_MAXPATHLEN];
                sprintf(command,"/bin/gzip %s/%s", env->session->sudir, dirp->d_name);
                system(command);
                emptydir = 0;
            } else {
                emptydir = 0;
            }
        }
        closedir(dp);
        
        /* Commit log storage unit to SUMS. */
        memset(&su,0,sizeof(su));
        su.seriesinfo = malloc(sizeof(DRMS_SeriesInfo_t));
        XASSERT(su.seriesinfo);
        strcpy(su.seriesinfo->seriesname,DRMS_LOG_DSNAME);
        su.seriesinfo->tapegroup = DRMS_LOG_TAPEGROUP;
        su.seriesinfo->archive = archive_log;
        if (emptydir) 
            su.seriesinfo->archive = 0;
        if (log_retention <= 0 )
            su.seriesinfo->retention = 1;
        else
            su.seriesinfo->retention = log_retention;
        strcpy(su.sudir, env->session->sudir);
        su.sunum = env->session->sunum;
        su.mode = DRMS_READWRITE;
        su.state = NULL;
        su.recnum = NULL;
        su.seriesinfo->hasshadow = 0;
        su.seriesinfo->createshadow = 0;
        
        /* drms_commitunit() will lock the global env mutex, so must release here. */
        drms_unlock_server(env);
        if (drms_commitunit(env, &su))
            fprintf(stderr,"Error: Couldn't commit log storage unit to SUMS.\n");
        drms_lock_server(env);
        free(su.seriesinfo);
    }

  /* Set sessionid and insert an entry in the session table */
  char stmt[1024];
  sprintf(stmt, "UPDATE %s."DRMS_SESSION_TABLE
	  " SET sudir=NULL,status=?,clients=?,lastcontact=LOCALTIMESTAMP(0),"
	  "endtime=LOCALTIMESTAMP(0) WHERE sessionid=?", env->session->sessionns);
  if (db_dmsv(env->session->stat_conn, NULL, stmt, 
      -1, DB_STRING, stat_str, DB_INT4, clients,
      DB_INT8, env->session->sessionid))
  {
    //    fprintf(stderr,"Error: Couldn't update session entry.\n");
    return 1;
  }

  return 0;
}

/* 1. Roll back any outstandings modifications to the database.
   2. Remove the session from the session list in the database.
      In POSTGRES this can require starting a new transaction if
      an error occurred in the old one.
   3. Set the abort flag on the DB connection flag telling all threads 
      that the session is about to be aborted and keep them from
      initiating any new database commands.
   4. Free the environment. */
void drms_server_abort(DRMS_Env_t *env, int final)
{
  drms_lock_server(env);
  if (env->verbose)
    fprintf(stderr,"WARNING: DRMS is aborting...\n");  

  /* Roll back. */
  if (env->transrunning)
  {
     db_rollback(env->session->db_handle);
     env->transrunning = 0;
  }

  /* Unregister */
  if (env->sessionrunning)
  {
     drms_server_close_session(env, abortstring, env->clientcounter, 7, 0);
     env->sessionrunning = 0;
  }

  // Either a sums thread or a server thread might be waiting on
  // either sum_inbox or sum_outbox. Need to put something into the
  // respective queue to release the mutex and conditional variables
  // in order to destroy them in drms_free_env().  Given the
  // head-of-line blocking queue design, there is at most one
  // outstanding request.
  if (env->sum_thread) {
    // a server thread might be waiting for reply on sum_outbox, tell
    // it we are aborting.
     DRMS_SumRequest_t *request;
     request = malloc(sizeof(DRMS_SumRequest_t));
     XASSERT(request);

    if (env->sum_tag) 
    {
      // has to be dynamically allocated since it's going to be freed.
       /* This code simply tells the thread that made an unprocessed
        * SUMS request that there was an error (abort is happening). */
      DRMS_SumRequest_t *reply;
      reply = malloc(sizeof(DRMS_SumRequest_t));
      XASSERT(reply);     
      reply->opcode = DRMS_ERROR_ABORT;
      tqueueAdd(env->sum_outbox, env->sum_tag, (char *)reply);
      request->opcode = DRMS_SUMABORT;
    }
    else
    {
      request->opcode = DRMS_SUMCLOSE;
    }
    // a sums thread might be waiting for requests on sum_inbox, tell
    // it we are aborting.
    /* tell the sums thread to finish up. */
    // looks like all requests are not dyanmically allocated.
    tqueueAdd(env->sum_inbox, (long)pthread_self(), (char *)request);

    pthread_join(env->sum_thread, NULL);
    env->sum_thread = 0;
  }

  db_disconnect(&env->session->stat_conn);
 
  /* Close DB connection and set abort flag... */
  db_disconnect(&env->session->db_handle);

  /* No need to sleep here ("wait for other threads to finish cleanly") because 
   * the main thread waits for all other threads to finish before returning. But
   * waiting can be indicated with a cmd-line argument (which causes 
   * env->server_wait to be set). */
  if (env->server_wait)
  {
     if (env->verbose) 
       fprintf(stderr, "WARNING: DRMS server aborting in %d seconds...\n", 
               DRMS_ABORT_SLEEP);
     sleep(DRMS_ABORT_SLEEP);
  }

  /* Free memory.*/
  if (final) {
     /* drms_lock_server() was called at the beginning of this function, and drms_free_env() calls it as well
      * (if this library is being used by a server app). So, we need to release the lock here. */
#ifndef DRMS_CLIENT
     drms_unlock_server(env);
#endif

    drms_free_env(env, 1);
  } else {
    //    fprintf(stderr, "skip freeing drms_free_env()");
     drms_unlock_server(env);
  }

  /* Good night and good luck! */
#ifdef DEBUG
  printf("Exiting drms_server_abort()\n");
#endif
}


/* 1. Commit all outstandings modifications to the database and SUMS.
   2. Remove the session from the session list in the database.
      In POSTGRES this can require starting a new transaction if
      an error occurred in the old one.
   3. Set the abort flag on the DB connection flag telling all threads 
      that the session is about to be aborted and keep them from
      initiating any new database commands.
   4. Free the environment. */
void drms_server_commit(DRMS_Env_t *env, int final)
{
  int log_retention, archive_log;
  int status = 0;

  if (env->verbose) 
    printf("DRMS is committing...stand by...\n");  
  /* Lock the server to make sure nobody else is changing its state. */
  drms_lock_server(env);
  /* Delete all session temporary records from the DRMS database. */
  if (env->verbose)
    printf("Deleting temporary records from DRMS.\n");
  drms_delete_temporaries(env);
    
  /* Commit (SUM_put) all storage units to SUMS. */
  if (env->verbose)
    printf("Commiting changes to SUMS.\n");

  /* Must close fitsrw open fitsfiles before calling drms_commit_all_units(). The latter
   * call makes the SUMS directories read-only. Since the fitsfiles are pointers to files
   * in SUMS, calling drms_fitsrw_term() after drms_commit_all_units() will fail. This is
   * only for direct-connect modules. For such modules, the process which contains the 
   * drms_server_commit() call is the same one that created the list of open fitsfiles (the module's process).
   * For indirect-connect modules (sock modules), the process that executes drms_server_commit()
   * is drms_server. So, for indirect-connect modules, you have to have the module process call 
   * drms_fitsrw_term(), right before it terminates. */
  if (env->session->db_direct == 1)
  {
     drms_fitsrw_term(env->verbose);
  }

    /* drms_commit_all_units() might acquire the env lock. */
    drms_unlock_server(env);
    log_retention = drms_commit_all_units(env, &archive_log, &status);  
    drms_lock_server(env);

  /* Unregister session */
  if (env->verbose)
    printf("Unregistering DRMS session.\n");

  if (env->sessionrunning)
  {
     drms_server_close_session(env, "finished", 0, log_retention, archive_log);
     env->sessionrunning = 0;
  }

  if (env->sum_thread) {
    /* Tell SUM thread to finish up. */
    DRMS_SumRequest_t *request;
    request = malloc(sizeof(DRMS_SumRequest_t));
    XASSERT(request);
    request->opcode = DRMS_SUMCLOSE;
    tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);
    /* Wait for SUM service thread to finish. */

    /* It is possible that the SUMS thread is blocked right now. It will call drms_lock_server(), and 
     * if it does so after execution has entered drms_server_commit(), then we will deadlock. To
     * forestall this problem, release the lock now. */
    drms_unlock_server(env);
    pthread_join(env->sum_thread, NULL);
    drms_lock_server(env);
    env->sum_thread = 0;
  }

  db_disconnect(&env->session->stat_conn);

  /* Commit all changes to the DRMS database. */
  if (env->transrunning)
  {    
     if (status)
     {
        db_rollback(env->session->db_handle);
     }
     else
     {
        db_commit(env->session->db_handle);
     }

     env->transrunning = 0;
  }

  if (final) {
    db_disconnect(&env->session->db_handle);
  }

  /* Give threads a small window to finish cleanly - this gets set by a cmd-line arg. */
  if (env->server_wait) {
    if (env->verbose)
      fprintf(stderr, "WARNING: DRMS server stopping in approximately %d "
	      "seconds...\n", DRMS_ABORT_SLEEP);
    sleep(DRMS_ABORT_SLEEP);
  }

  /* Free memory.*/
  if (final)
  {
     drms_free_env(env, 1);  
  }
  else
  {
     drms_unlock_server(env);
  }

#ifdef DEBUG
  printf("Exiting drms_server_commit()\n");
#endif
}




/*************** Main function executed by DRMS server threads. ************/

void *drms_server_thread(void *arg)
{
  DRMS_ThreadInfo_t *tinfo;
  struct sockaddr_in client;
  int noshare, tnum, sockfd;
  unsigned int client_size = sizeof(client);
  int command,status,disconnect;
  DRMS_Env_t *env;
  DB_Handle_t *db_handle;

  /* Detach child thread  - let it go about its merry way. */
  if (pthread_detach(pthread_self()))
  {
    printf("Thread detach failed\n");
    return NULL;
  }

  /* Copy all fields in the Threadinfo structure and free it.*/
  tinfo = (DRMS_ThreadInfo_t *) arg;
  sockfd = tinfo->sockfd;
  tnum = tinfo->threadnum;
  env = tinfo->env;
  noshare = tinfo->noshare;
  free(tinfo);
  XASSERT(env->session->db_direct == 1);
  db_handle = env->session->db_handle;

  /* Block signals. */
  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, NULL)))
  {
    printf("pthread_sigmask call failed with status = %d\n", status);

    // Exit(1); - never call exit(), only the signal thread can do that.
    // Instead, send a TERM signal to the signal thread.
    pthread_kill(env->signal_thread, SIGTERM);
    goto bail;
  }

  if ( getpeername(sockfd,  (struct sockaddr *)&client, &client_size) == -1 )
  {
    perror("accept call failed.");
    goto bail;
  }

  printf("thread %d: handling new connection from %s:%d.\n",
	 tnum, inet_ntoa(client.sin_addr),
	 ntohs(client.sin_port));

  /* If in noshare mode, start a new transaction. */
  if (noshare)
  {
    if (db_start_transaction(db_handle))
    {
      fprintf(stderr,"thread %d: Couldn't start database transaction.\n",tnum);
      goto bail;
    }
  }


  if (env->verbose)
    printf("thread %d: Waiting for command.\n", tnum);
  /* MAIN LOOP: Finite state machine. */
  disconnect = 0;  status = 0;
  while ( !status && !disconnect && readint(sockfd, &command) == sizeof(int))
  {
    /* Echo the command code to client to avoid delayed ACK problem. */
    Writeint(sockfd, command);

    switch(command)
    {
    case DRMS_DISCONNECT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_DISCONNECT.\n", tnum);
      disconnect = 1;
      status = Readint(sockfd); /* Abort flag. */
      break;
    case DRMS_ROLLBACK:
      if (env->verbose)
	printf("thread %d: Executing DRMS_ROLLBACK.\n", tnum);
      pthread_mutex_lock(env->clientlock);
      status = db_rollback(db_handle);
      db_start_transaction(db_handle);
      pthread_mutex_unlock(env->clientlock);
      Writeint(sockfd, status);
      break;
    case DRMS_COMMIT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_COMMIT.\n", tnum);
      pthread_mutex_lock(env->clientlock);
      status = db_commit(db_handle);
      db_start_transaction(db_handle);
      pthread_mutex_unlock(env->clientlock);
      Writeint(sockfd, status);
      break;
    case DRMS_TXTQUERY:
      if (env->verbose)
	printf("thread %d: Executing DRMS_TXTQUERY.\n",tnum);
      status = db_server_query_txt(sockfd, db_handle);
      break;
    case DRMS_BINQUERY:
      if (env->verbose)
	printf("thread %d: Executing DRMS_BINQUERY.\n",tnum);
      status = db_server_query_bin(sockfd, db_handle);
      break;
    case DRMS_DMS:
      if (env->verbose)
	printf("thread %d: Executing DRMS_DMS.\n",tnum);
      status = db_server_dms(sockfd, db_handle);
      break;
    case DRMS_DMS_ARRAY:
      if (env->verbose)
	printf("thread %d: Executing DRMS_DMS_ARRAY.\n",tnum);
      status = db_server_dms_array(sockfd, db_handle);
      break;
    case DRMS_BULK_INSERT_ARRAY:
      if (env->verbose)
	printf("thread %d: Executing DRMS_BULK_INSERT_ARRAY.\n",tnum); /* ISS - #22 */
      status = db_server_bulk_insert_array(sockfd, db_handle);
      break;
    case  DRMS_SEQUENCE_DROP:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SEQUENCE_DROP.\n",tnum);
      status = db_server_sequence_drop(sockfd, db_handle);
      break;
    case  DRMS_SEQUENCE_CREATE:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SEQUENCE_CREATE.\n",tnum);
      status = db_server_sequence_create(sockfd, db_handle);
      break;
    case  DRMS_SEQUENCE_GETNEXT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SEQUENCE_GETNEXT.\n",tnum);
      status = db_server_sequence_getnext_n(sockfd, db_handle);
      break;
    case  DRMS_SEQUENCE_GETCURRENT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SEQUENCE_GETCURRENT.\n",tnum);
      status = db_server_sequence_getcurrent(sockfd, db_handle);
      break;
    case  DRMS_SEQUENCE_GETLAST:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SEQUENCE_GETLAST.\n",tnum);
      status = db_server_sequence_getlast(sockfd, db_handle);
      break;
    case  DRMS_BINQUERY_ARRAY:
      if (env->verbose)
	printf("thread %d: Executing DRMS_BINQUERY_ARRAY.\n",tnum);
      status = db_server_query_bin_array(sockfd, db_handle);
      break;
        case DRMS_BINQUERY_NTUPLE:
        {
            if (env->verbose)
            {
                printf("thread %d: Executing DRMS_BINQUERY_NTUPLE.\n", tnum);
            }
            
            status = db_server_query_bin_ntuple(sockfd, db_handle);
            break;
        }
    case  DRMS_ALLOC_RECNUM:
      if (env->verbose)
	printf("thread %d: Executing DRMS_ALLOC_RECNUM.\n",tnum);
      status = drms_server_alloc_recnum(env, sockfd);
      break;
    case  DRMS_NEWSLOTS:
      if (env->verbose)
	printf("thread %d: Executing DRMS_NEWSLOTS.\n",tnum);
      pthread_mutex_lock(env->clientlock);
      status = drms_server_newslots(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case  DRMS_SLOT_SETSTATE:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SLOT_SETSTATE.\n",tnum);
      pthread_mutex_lock(env->clientlock);
      status = drms_server_slot_setstate(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case  DRMS_GETUNIT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_GETUNIT.\n",tnum);
      pthread_mutex_lock(env->clientlock);
      status = drms_server_getunit(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case  DRMS_GETUNITS:
      if (env->verbose)
	printf("thread %d: Executing DRMS_GETUNITS.\n",tnum);
      /* XXX - Need a new lock to synchronize among server threads (possibly) */
      pthread_mutex_lock(env->clientlock);
      status = drms_server_getunits(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_GETSUDIR:
      if (env->verbose)
	printf("thread %d: Executing DRMS_GETSUDIR.\n",tnum);
      /* XXX Don't lock environment at this level - too high; but use a new lock
       * to synchronize server threads */
      pthread_mutex_lock(env->clientlock);
      status = drms_server_getsudir(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_GETSUDIRS:
      if (env->verbose)
        printf("thread %d: Executing DRMS_GETSUDIRS.\n",tnum);
      /* XXX - Need a new lock to synchronize among server threads (possibly) */
      pthread_mutex_lock(env->clientlock);
      status = drms_server_getsudirs(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_NEWSERIES:
      if (env->verbose)
	printf("thread %d: Executing DRMS_NEWSERIES.\n",tnum);
      pthread_mutex_lock(env->clientlock);
      status = drms_server_newseries(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_DROPSERIES:
      if (env->verbose)
	printf("thread %d: Executing DRMS_DROPSERIES.\n",tnum);
      pthread_mutex_lock(env->clientlock);
      status = drms_server_dropseries(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_GETTMPGUID:
      pthread_mutex_lock(env->clientlock);
      drms_server_gettmpguid(&sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_SITEINFO:
      pthread_mutex_lock(env->clientlock);
      drmssite_server_siteinfo(sockfd, db_handle);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_LOCALSITEINFO:
      pthread_mutex_lock(env->clientlock);
      drmssite_server_localsiteinfo(sockfd, db_handle);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_GETSUINFO:
      pthread_mutex_lock(env->clientlock);
      drms_server_getsuinfo(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    case DRMS_GETDBUSER:
      pthread_mutex_lock(env->clientlock);
      drms_server_getdbuser(env, sockfd);
      pthread_mutex_unlock(env->clientlock);
      break;
    default:
      fprintf(stderr,"Error: Unknown command code '%d'\n",command);
    }
    if (status)
      if (env->verbose)
	printf("thread %d: WARNING: Last command failed with status = %d\n",
	       tnum,status);
    /*    if (!disconnect)
	  printf("thread %d: Waiting for command.\n", tnum); */
#ifdef DEBUG
    xmem_check_all_guardwords(stdout, 100);
#endif

#ifdef __linux__
   sched_yield(); /* Be nice and give the other clients a chance
		      to be served. */
#endif
  }

  /*
     Exit status:


     disconnect = 1 and status = 0 means that the client
     disconnected properly with no errors and sent abort = 0.
     Action: Stop the service thread, keep the server running
     to serve more clients.

     disconnect = 1 and status = 1 means that the client disconnected
     properly and sent abort = 1.
     Action: ROLLBACK and abort.

     disconnect = 0 and status = 1 means that a database call executed by
     the service thread failed or was aborted due to another thread raising
     the abort flag and doing a rollback.
     Action: ROLLBACK. If noshare=1 then try to continue with a
     new transaction, otherwise abort.

     disconnect = 0 and status = 0 means that the client either closed
     the socket connection unexpectedly or some other error caused
     a failure when the server tried to read the next command word from
     the socket.
     Action: ROLLBACK. If noshare=1 then try to continue with a
     new transaction, otherwise abort.
  */

  /* server thread terminating, decrement server-thread count */
   drms_lock_server(env);
   --(env->clientcounter);
    drms_unlock_server(env);

  if (noshare)
  {
    /* If in noshare mode, commit or rollback, and start a new transaction. */
    drms_lock_server(env);

    if (disconnect && !status)
    {
      if (env->verbose)
	printf("thread %d: Performing COMMIT.\n",tnum);
      if(db_commit(db_handle))
	fprintf(stderr,"thread %d: COMMIT failed.\n",tnum);
    }
    else
    {
      if (env->verbose)
	printf("thread %d: Performing ROLLBACK.\n",tnum);
      if(db_rollback(db_handle))
	fprintf(stderr,"thread %d: ROLLBACK failed.\n",tnum);
    }
    if(db_start_transaction(db_handle))
      fprintf(stderr,"thread %d: START TRANSACTION failed.\n",tnum);
    drms_unlock_server(env);
  }
  else
  {
    /* If an error occured then
          a) roll back
          b) set the abort flag to tell all threads
             that the session is about to be aborted,
          c) close the DB connection when it is relinquished.
       This is all handled in the atexit handler registered by the server
       main, so here we just call exit(1).  */

    if (!status && disconnect)
    {
      /* Client disconnected with no error. */
      drms_lock_server(env);

      drms_unlock_server(env);
    }
    else
    {
      if (!status && !disconnect)
	fprintf(stderr,"thread %d: The client module seems to have crashed "
		"or aborted.\nSocket connection was lost. Aborting.\n",tnum);
      else if (status && !disconnect)
	fprintf(stderr,"thread %d: An error occured in server thread, "
		"status=%d.\nAborting.\n",tnum,status);
      else if (status && disconnect)
	fprintf(stderr,"thread %d: The client module disconnected and set "
		"the abort flag.\nAborting.\n",tnum);

      // Exit(1); - never call exit(), only the signal thread can do that.
      // Instead, send a TERM signal to the signal thread.
        
        /* Killing the drms_server here is a bad idea. The clients do not know that 
         * it is being terminated. They will continue to send requests to drms_server. 
         * But because drms_sever will not respond, the clients will error out with
         * a cryptic error message:
         
         FATAL ERROR: The DRMS server echoed a different command code (16777216)
         from the one sent (1).
         This usually indicates that the module generated an invalid command
         that caused the DRMS server to terminate the connection.
         
         * A better architecture would be to 1. block new client connections, and
         * 2. send some kind of failure status to all new requests, and 3. when
         * all client connections have disappeared, then drms_server can quit.
         * This way, existing clients can continue to send requests that will
         * go unsatified (all requests will fail) until they ultimately fail and
         * disconnect. */
      pthread_kill(env->signal_thread, SIGTERM);
    }
  }

  printf("thread %d: Exiting with disconnect = %d and status = %d.\n",
	 tnum,disconnect,status);

  close(sockfd);
  return NULL;

 bail:
  drms_lock_server(env);
  --(env->clientcounter);
  drms_unlock_server(env);
  close(sockfd);
  return NULL;
}


/* Server stub for drms_su_newslot. Allocate a new slots in storage units. 
   If no storage unit with a free slot is found in the storage unit cache
   allocate a new one from SUMS. */
int drms_server_newslots(DRMS_Env_t *env, int sockfd)
{
  int status,n,i;
  char *series;
  int *slotnum;
  long long *recnum;
  DRMS_StorageUnit_t **su;
  DRMS_RecLifetime_t lifetime;
  int createslotdirs;
  int gotosums;

  status = DRMS_SUCCESS;
  series = receive_string(sockfd);  

  n = Readint(sockfd);  
  if (n>0)
  {    
    lifetime = (DRMS_RecLifetime_t) Readint(sockfd); 
    createslotdirs = Readint(sockfd);

    /* ART - Add gotosums flag */
    gotosums = Readint(sockfd);
  
    su = malloc(n*sizeof(DRMS_StorageUnit_t *));
    XASSERT(su);
    slotnum = malloc(n*sizeof(int));
    XASSERT(slotnum);
    recnum = malloc(n*sizeof(long long));
    XASSERT(recnum);
    for (i=0; i<n; i++)
      recnum[i] = Readlonglong(sockfd);  
      
#if defined(DEBUG)
    fprintf(stdout, "series = '%s'\nn = '%d'\nlifetime = '%d'\ncreateslotdirs = '%d'\n", 
            series, n, lifetime, createslotdirs);
#endif

    if (gotosums)
    {
       status = drms_su_newslots(env, n, series, recnum, lifetime, slotnum, su, createslotdirs);
    }
    else
    {
       status = drms_su_newslots_nosums(env, n, series, recnum, lifetime, slotnum, su, createslotdirs);
    }

    if (status==DRMS_SUCCESS)
    {
      int *tmp, *t;
      long long *ltmp, *lt;
      struct iovec *vec, *v;
      
      tmp = malloc((2*n+1)*sizeof(int));
      XASSERT(tmp);
      ltmp = malloc((n+1)*sizeof(long long));
      XASSERT(ltmp);
      vec = malloc((4*n+1)*sizeof(struct iovec));
      XASSERT(vec);
      t=tmp; v=vec; lt=ltmp;
      net_packint(status, t++, v++);
      for (i=0; i<n; i++)
      {
	net_packlonglong(su[i]->sunum, lt++, v++);
	net_packstring(su[i]->sudir, t++, v); v+=2;
	net_packint(slotnum[i], t++, v++);
      }
      Writevn(sockfd, vec, (4*n+1));
      free(tmp);
      free(ltmp);
      free(vec);
    }
    else
      Writeint(sockfd, status);
    free(slotnum);
    free(su);	
    free(recnum);	
  }
  free(series);
  return status;
}


/* Server stub for drms_getunit. Get the path to a storage unit and
   return it to the client.  If the storage unit is not in the storage 
   unit cache, request it from SUMS. */
int drms_server_getunit(DRMS_Env_t *env, int sockfd)
{ 
  int status;
  long long sunum;
  char *series;
  int retrieve;
  DRMS_StorageUnit_t *su;

  series = receive_string(sockfd);
  sunum = Readlonglong(sockfd);
  retrieve = Readint(sockfd);
  su = drms_getunit(env, series, sunum, retrieve, &status);
  if (status==DRMS_SUCCESS)
  {
    int tmp,len;
    struct iovec vec[3];
    tmp = htonl(status);
    vec[0].iov_len = sizeof(tmp);
    vec[0].iov_base = &tmp;
    if (su) {
      vec[2].iov_len = strlen(su->sudir);
      vec[2].iov_base = su->sudir;
      len = htonl(vec[2].iov_len);
      vec[1].iov_len = sizeof(len);
      vec[1].iov_base = &len;
    } else {
      vec[2].iov_len = 0;
      vec[2].iov_base = "\0";
      len = htonl(vec[2].iov_len);
      vec[1].iov_len = sizeof(len);
      vec[1].iov_base = &len;
    }      
    Writevn(sockfd, vec, 3);
  }
  else
    Writeint(sockfd,status);
  free(series);
  return status;
}

/* Server stub for drms_getunits. Get the path to a storage unit and
   return it to the client.  If the storage unit is not in the storage 
   unit cache, request it from SUMS. */
int drms_server_getunits(DRMS_Env_t *env, int sockfd)
{ 
  int status;
  int n;

  int retrieve, dontwait;
  DRMS_StorageUnit_t *su;
  DRMS_SuAndSeries_t *suandseries = NULL;
  int icnt;

  n = Readint(sockfd);

  suandseries = (DRMS_SuAndSeries_t *)malloc(sizeof(DRMS_SuAndSeries_t) * n);

  for (icnt = 0; icnt < n; icnt++)
  {
     suandseries[icnt].series = receive_string(sockfd); // allocs
  }

  for (icnt = 0; icnt < n; icnt++)
  {
    suandseries[icnt].sunum = Readlonglong(sockfd);
  }

  retrieve = Readint(sockfd);
  dontwait = Readint(sockfd);
    
    /* SUMS does not support dontwait == 1, so force dontwait to be 0 (deprecate the dontwait parameter). */
    dontwait = 0;

  status = drms_getunits_ex(env, n, suandseries, retrieve, dontwait);
  Writeint(sockfd,status);
  if (status==DRMS_SUCCESS)  {
    if (!dontwait) {
      int *len;
      struct iovec *vec;

      len = malloc(n*sizeof(int));
      XASSERT(len);
      vec = malloc(2*n*sizeof(struct iovec));
      XASSERT(vec);
      for (int i = 0; i < n; i++) {
	HContainer_t *scon = NULL;
	su = drms_su_lookup(env, suandseries[i].series, suandseries[i].sunum, &scon);
	if (su) {
	  vec[2*i+1].iov_len = strlen(su->sudir);
	  vec[2*i+1].iov_base = su->sudir;
	  len[i] = htonl(vec[2*i+1].iov_len);
	  vec[2*i].iov_len = sizeof(len[i]);
	  vec[2*i].iov_base = &len[i];
	} else {
	  vec[2*i+1].iov_len = 0;
	  vec[2*i+1].iov_base = "\0";
	  len[i] = htonl(vec[2*i+1].iov_len);
	  vec[2*i].iov_len = sizeof(len[i]);
	  vec[2*i].iov_base = &len[i];
	}      
      }
      Writevn(sockfd, vec, 2*n);
      free(len);
      free(vec);
    }
  }

  for (icnt = 0; icnt < n; icnt++)
  {
     if (suandseries[icnt].series)
     {
        free(suandseries[icnt].series);
     }
  }

  free(suandseries);

  return status;
}

int drms_server_getsudir(DRMS_Env_t *env, int sockfd)
{
   int retrieve;
   DRMS_StorageUnit_t su;
   int status;
   
   su.sudir[0] = '\0';
   su.mode = DRMS_READONLY; 
   su.nfree = 0;
   su.state = NULL;
   su.recnum = NULL;
   su.refcount = 0;
   su.seriesinfo = NULL;

   //Readn_ntoh(sockfd, &su, sizeof(DRMS_StorageUnit_t *));
   su.sunum = Readlonglong(sockfd);
   retrieve = Readint(sockfd);

   status = drms_su_getsudir(env, &su, retrieve);
   
   send_string(sockfd, su.sudir);
   Writeint(sockfd, status);

   if (status == DRMS_REMOTESUMS_TRYLATER)
   {
      return DRMS_SUCCESS;
   }
   else
   {
      return status;
   }
}

int drms_server_getsudirs(DRMS_Env_t *env, int sockfd)
{
   DRMS_StorageUnit_t **su = NULL;
   DRMS_StorageUnit_t *onesu = NULL;
   int num;
   int retrieve;
   int dontwait;
   int isu;
   int status;
   
   num = Readint(sockfd);

   su = malloc(sizeof(DRMS_StorageUnit_t *) * num);

   for (isu = 0; isu < num; isu++)
   {
      su[isu] = (DRMS_StorageUnit_t *)malloc(sizeof(DRMS_StorageUnit_t));
      onesu = su[isu];
      onesu->sunum = Readlonglong(sockfd);
      onesu->sudir[0] = '\0';
      onesu->mode = DRMS_READONLY; 
      onesu->nfree = 0;
      onesu->state = NULL;
      onesu->recnum = NULL;
      onesu->refcount = 0;
      onesu->seriesinfo = NULL;
   }

   retrieve = Readint(sockfd);
   dontwait = Readint(sockfd);
    
    /* SUMS does not support dontwait == 1, so force dontwait to be 0 (deprecate the dontwait parameter). */
    dontwait = 0;

   status = drms_su_getsudirs(env, num, su, retrieve, dontwait);
   
   for (isu = 0; isu < num; isu++)
   {
      onesu = su[isu];
      send_string(sockfd, onesu->sudir);
   }

   if (su)
   {
      for (isu = 0; isu < num; isu++)
      {
         if (su[isu])
         {
            free(su[isu]);
         }
      }

      free(su);
   }

   if (status == DRMS_REMOTESUMS_TRYLATER)
   {
      return DRMS_SUCCESS;
   }
   else
   {
      return status;
   }
}

/* This is the server's response to a client's over-a-socket request. */
int drms_server_getsuinfo(DRMS_Env_t *env, int sockfd)
{
   int status = DRMS_SUCCESS;
   int nReqs;
   int isunum;
   long long *sunums = NULL;
   SUM_info_t **infostructs = NULL;
   SUM_info_t *info = NULL;

   nReqs = Readint(sockfd);

   sunums = (long long *)malloc(sizeof(long long) * nReqs);
   infostructs = (SUM_info_t **)malloc(sizeof(SUM_info_t *) * nReqs);

   for (isunum = 0; isunum < nReqs; isunum++)
   {
      sunums[isunum] = Readlonglong(sockfd);
   }

   status = drms_su_getinfo(env, sunums, nReqs, infostructs);

   Writeint(sockfd, status);

   if (status == DRMS_SUCCESS)
   {
      /* Need to write back the results to the file descriptor. */
      for (isunum = 0; isunum < nReqs; isunum++)
      {
         info = infostructs[isunum];

         Writelonglong(sockfd, info->sunum);
         send_string(sockfd, info->online_loc);
         send_string(sockfd, info->online_status);
         send_string(sockfd, info->archive_status);
         send_string(sockfd, info->offsite_ack);
         send_string(sockfd, info->history_comment);
         send_string(sockfd, info->owning_series);
         Writeint(sockfd, info->storage_group);
         Write_dbtype(DB_DOUBLE, (void *)&(info->bytes), sockfd);
         send_string(sockfd, info->creat_date);
         send_string(sockfd, info->username);
         send_string(sockfd, info->arch_tape);
         Writeint(sockfd, info->arch_tape_fn);
         send_string(sockfd, info->arch_tape_date);
         send_string(sockfd, info->safe_tape);
         Writeint(sockfd, info->safe_tape_fn);
         send_string(sockfd, info->safe_tape_date);
         Writeint(sockfd, info->pa_status);
         Writeint(sockfd, info->pa_substatus);
         send_string(sockfd, info->effective_date);
      }
   }

   if (infostructs)
   {
      free(infostructs);
      infostructs = NULL;
   }

   return status;
}

int drms_server_getdbuser(DRMS_Env_t *env, int sockfd)
{
   send_string(sockfd, env->session->db_handle->dbuser);
   return DRMS_SUCCESS;
}

/* Server stub for drms_su_freeslot and drms_su_markstate. */
int drms_server_alloc_recnum(DRMS_Env_t *env, int sockfd)
{ 
  int status, n, i;
  char *series;
  DRMS_RecLifetime_t lifetime;
  long long *recnums;
  int tmp;
  struct iovec vec[3];


  series = receive_string(sockfd);
  n = Readint(sockfd);
  lifetime = (DRMS_RecLifetime_t) Readint(sockfd);

  if ((recnums = db_sequence_getnext_n(env->session->db_handle, series, n)) == NULL)
  {
    status = 1;
    Writeint(sockfd, status);
  }
  else
  {
    status = 0;
    
    /* Insert record numbers in temp list if temporary. */
    if (lifetime == DRMS_TRANSIENT)
    {
      drms_lock_server(env);
      drms_server_transient_records(env, series, n, recnums);
      drms_unlock_server(env);
    }


    /* Send record numbers back to client. */
    tmp = htonl(status);
    vec[0].iov_len = sizeof(int);
    vec[0].iov_base = &tmp;
    for (i=0; i<n; i++)
      recnums[i] = htonll( recnums[i] );
    vec[1].iov_len = n*sizeof(long long);
    vec[1].iov_base = recnums;
    Writevn(sockfd, vec, 2);
    free(recnums);
  }    
  free(series);
  return status;
}

void drms_server_transient_records(DRMS_Env_t *env, char *series, int n, long long *recnums) {
  DS_node_t *ds=(DS_node_t *) NULL; /*ISS - #21*/

  if (!env->templist)
    {
      env->templist = ds = malloc(sizeof(DS_node_t));
      XASSERT(env->templist);
      ds->series = strdup(series);
      ds->nmax = 512;
      ds->n = 0;
      ds->recnums = malloc(ds->nmax*sizeof(long long));
      XASSERT(ds->recnums);
      ds->next = NULL;
    }
  else
    {
      if (ds == (DS_node_t *) NULL) ds = env->templist; /* ISS - #21*/
      while(1)
	{
	  if (!strcmp(ds->series,series))
	    break;
	  else
	    {
	      if (!ds->next)
		{
                  ds->next = malloc(sizeof(DS_node_t));
                  XASSERT(ds->next);
		  ds = ds->next;
		  ds->series = strdup(series);
		  ds->nmax = 512;
		  ds->n = 0;
                  ds->recnums = malloc(ds->nmax*sizeof(long long));
                  XASSERT(ds->recnums);
		  ds->next = NULL;
		  break;
		}
	      ds = ds->next;
	    }
	}    
    }
  if ( ds->n+n >= ds->nmax )
    {
      ds->nmax *= 2;
      ds->recnums = realloc(ds->recnums, ds->nmax*sizeof(long long));
      XASSERT(ds->recnums);
    }
  for (int i=0; i<n; i++)
    ds->recnums[(ds->n)++] = recnums[i];
}


/* Server stub for drms_su_freeslot and drms_su_markstate. */
int drms_server_slot_setstate(DRMS_Env_t *env, int sockfd)
{ 
  int status, state;
  char *series;
  int slotnum;
  long long sunum;

  series = receive_string(sockfd);
  sunum = Readlonglong(sockfd);
  slotnum = Readint(sockfd);
  state = Readint(sockfd);
  if (state == DRMS_SLOT_FREE)
    status = drms_su_freeslot(env, series, sunum, slotnum);
  else
    status = (drms_su_markslot(env, series, sunum, slotnum, &state) != NULL);
  Writeint(sockfd,status);
  free(series);
  return status;
}


/* A client created a new series. Create an entry in the
   server series_cache for it in case the client requests
   storage units or a template for it. */
int drms_server_newseries(DRMS_Env_t *env, int sockfd)
{ 
  char *series;
  DRMS_Record_t *template;

  series = receive_string(sockfd);

  /* even though we're typically caching series on-demand now, it is okay to cache this one now, 
   * because in fact you probably will use this newly created series. */
  template = (DRMS_Record_t *)hcon_allocslot_lower(&env->series_cache, series);
  memset(template,0,sizeof(DRMS_Record_t));
  template->init = 0;
  
  free(series);
  return 0;
}


/* A client destroyed a series. Remove its entry in the
   server series_cache */
int drms_server_dropseries(DRMS_Env_t *env, int sockfd)
{ 
  char *series_lower;
  char *tn;
  DRMS_Array_t *vec = NULL;
  int nrows = -1;
  int irow = -1;
  int dims[2];
  int status = 0;
  int8_t *val = NULL;

  series_lower = receive_string(sockfd);
  tn = strdup(series_lower);
  strtolower(series_lower);
  nrows = Readint(sockfd);
  dims[0] = 1;
  dims[1] = nrows;

  vec = drms_array_create(DRMS_TYPE_LONGLONG, 2, dims, NULL, &status);

  if (!status)
  {
     val = (int8_t *)(vec->data);

     for (irow = 0; irow < nrows; irow++)
     {
        *(long long *)val = Readlonglong(sockfd);
        val += drms_sizeof(DRMS_TYPE_LONGLONG);
     }

     drms_server_dropseries_su(env, tn, vec);

     /* Since we are now caching series on-demand, this series may not be in the
      * series_cache, but hcon_remove handles this fine. */
     hcon_remove(&env->series_cache, series_lower);
  }

  if (vec)
  {
     drms_free_array(vec);
  }

  free(series_lower);
  free(tn);
  return status;
}

int drms_server_dropseries_su(DRMS_Env_t *env, const char *tn, DRMS_Array_t *array) {
  int status = 0;
  int drmsstatus = DRMS_SUCCESS;

  if (!env->sum_thread) {
    if((status = pthread_create(&env->sum_thread, NULL, &drms_sums_thread, 
			      (void *) env))) {
      fprintf(stderr,"Thread creation failed: %d\n", status);          
      return 1;
    }
  }

  /* 2 dimensions, one column, at least one row */
  if (array && 
      array->naxis == 2 && 
      array->axis[0] == 1 && 
      array->axis[1] > 0 && 
      array->type == DRMS_TYPE_LONGLONG)
  {
     /* Put the contained SUNUMs in an SUDIR */
     /* Create a new SUDIR - fail if less than 1MB of space (this will never happen). */
     char *sudir = NULL;
     long long sunum = -1;
     int lastrow = 0;
     int irow = 0;

     while (irow < array->axis[1])
     {
        /* chunk loop */
        sunum = drms_su_alloc(env, 1048576, &sudir, NULL, &drmsstatus);

        if (!drmsstatus && sunum >= 0 && sudir)
        {
           /* make a file that has nothing but SUNUMs */
           char fbuf[PATH_MAX];
           snprintf(fbuf, sizeof(fbuf), "%s/%s", sudir, kDELSERFILE);
           FILE *fptr = fopen(fbuf, "w");

           if (fptr)
           {
              /* Insert SUNUMs in the table now - iterate through DRMS_Array_t */
              long long *data = (long long *)array->data;
              char obuf[64];

              lastrow += kDelSerChunk < array->axis[1] - lastrow ? kDelSerChunk : array->axis[1] - lastrow;
           
              /* only one column*/
              for (; irow < lastrow; irow++)
              {
                 snprintf(obuf, sizeof(obuf), "%lld\n", data[irow]);
                 fwrite(obuf, strlen(obuf), 1, fptr);
              }
           
              if (!fclose(fptr))
              {
                 /* Now it is okay for SUMS to take over */
                 /* Use the request's comment field to hold array of sunums since
                  * the sunum field has a fixed number of sunums. */

                 /* Call SUM_put() to close the SU and have SUMS update its dbase tables */
                 DRMS_SumRequest_t *putreq = NULL;
                 DRMS_SumRequest_t *putrep = NULL;

                 putreq = malloc(sizeof(DRMS_SumRequest_t));
                 XASSERT(putreq);
                 memset(putreq, 0, sizeof(DRMS_SumRequest_t));
                 putreq->opcode = DRMS_SUMPUT;
                 putreq->dontwait = 0;
                 putreq->reqcnt = 1;
                 putreq->dsname = "deleteseriestemp";
                 putreq->group = 0;
                 putreq->mode = TEMP + TOUCH;
                 putreq->tdays = 2;
                 putreq->sunum[0] = sunum;
                 putreq->sudir[0] = sudir;
                 putreq->comment = NULL;

                 /* must have sum_thread running already */
                 XASSERT(env->sum_thread);

                 /* Submit request to sums server thread. */
                 tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)putreq);

                 /* Wait for reply. FIXME: add timeout. */
                 tqueueDel(env->sum_outbox, (long) pthread_self(), (char **)&putrep);

                 if (putrep->opcode != 0) 
                 {
                    fprintf(stderr, "ERROR in drms_server_dropseries_su(): SUM PUT failed with "
                            "error code %d.\n", putrep->opcode);
                    status = DRMS_ERROR_SUMPUT;
                 }

                 if (putrep)
                 {
                    free(putrep);
                 }

                 if (!status)
                 {
                    char sunumstr[64];
                    char commentbuf[DRMS_MAXPATHLEN * 2];
                    DRMS_SumRequest_t *request = NULL;
                    DRMS_SumRequest_t *reply = NULL;

                    request = malloc(sizeof(DRMS_SumRequest_t));
                    XASSERT(request);
                    memset(request, 0, sizeof(DRMS_SumRequest_t));

                    snprintf(sunumstr, sizeof(sunumstr), "%lld", sunum);
                    request->opcode = DRMS_SUMDELETESERIES;
                    /* provide path to data file and series name to SUMS */
                    snprintf(commentbuf, sizeof(commentbuf), "%s,%s", fbuf, tn);
                    request->comment = commentbuf; 
 
                    tqueueAdd(env->sum_inbox, (long)pthread_self(), (char *)request);
                    tqueueDel(env->sum_outbox, (long)pthread_self(), (char **)&reply);

                    /* SUMS thread has freed request. */

                    if (reply->opcode) 
                    {
                       fprintf(stderr, 
                               "SUM_delete_series() returned with error code '%d'.\n", 
                               reply->opcode);
                       status = DRMS_ERROR_SUMDELETESERIES;
                    }

                    /* No need to deep-free reply since SUMS shouldn't have malloc'd any fields. */
                    if (reply)
                    {
                       free(reply);
                    }
                 }
              }
              else
              {
                 /* couldn't close file */
                 fprintf(stderr, "Couldn't close file '%s'.\n", fbuf);
                 status = DRMS_ERROR_FILECREATE;
              }
           }
           else
           {
              /* couldn't open file */
              fprintf(stderr, "Couldn't open file '%s'.\n", fbuf);
              status = DRMS_ERROR_FILECREATE;
           }

           free(sudir);
        }
        else
        {
           fprintf(stderr, "SUMALLOC failed in drms_server_dropseries_su().\n");
           status = DRMS_ERROR_SUMALLOC;
        }

     } /* end chunk loop */
  }
  else
  {
     status = DRMS_ERROR_INVALIDDATA;
     fprintf(stderr, "Unexpected array passed to drms_server_dropseries_su().\n");
  }
  
  return status;
}

void drms_lock_server(DRMS_Env_t *env)
{
  if ( env->drms_lock == NULL )
    return;
  else
  {
     pthread_mutex_lock( env->drms_lock );
  }
}

void drms_unlock_server(DRMS_Env_t *env)
{
  if ( env->drms_lock == NULL )
    return;
  else
  {
     pthread_mutex_unlock( env->drms_lock );
  }
}

/* 0 means success - lock was acquired */
int drms_trylock_server(DRMS_Env_t *env)
{
   if (env->drms_lock == NULL)
   {
      return 0;
   }
   else
   {
      return pthread_mutex_trylock(env->drms_lock);
   }
}
 
long long drms_server_gettmpguid(int *sockfd)
{
   static long long GUID = 1;

   if (sockfd)
   {
      Writelonglong(*sockfd, GUID);
   }

   GUID++;
   return GUID - 1;
}

/* Loop though all open storage units and delete temporary records
   found there from the DRMS database. */
void drms_delete_temporaries(DRMS_Env_t *env)
{
  int i, status;
  char *command, *p;
  DS_node_t *ds;

  XASSERT(env->session->db_direct==1);

  ds = env->templist;
  while (ds)
  {
    if (ds->n>0)
    {
      command = malloc(strlen(ds->series) + 40 + 21*ds->n);
      XASSERT(command);
      p = command;
      p += sprintf(p, "delete from %s where recnum in (",ds->series);
      for (i=0; i<ds->n-1; i++)
	p += sprintf(p, "%lld,",ds->recnums[i]);
      p += sprintf(p, "%lld)",ds->recnums[ds->n-1]);
#ifdef DEBUG
      printf("drms_delete_temporaries: command = \n%s\n",command);
#endif
      status = drms_dms(env->session, NULL,  command);
      if (status)
      {
	fprintf(stderr,"ERROR in drms_delete_temporaries: drms_dms failed "
		"with status=%d\n",status);
	free(command);
	return;
      }
      free(command);
    }
    ds = ds->next;
  }
}

/****************** SUMS server thread functions **********************/

static void SigPipeHndlr(int signum)
{
   gGotPipe = 1;
}

/* This is the thread in the DRMS server which is responsible for
   forwarding requests to the SUM server. It receives requests from the
   ordinary drms_server_thread(s) in env->sum_inbox (a FIFO), forwards them
   to SUMS via the SUMS RPC protocol, and puts the reply into 
   env->sum_outbox (also a FIFO) for the drms_server_thread to pick it up. 
   Messages are tagged by the drms_server_thread(s) with the value 
   pthread_self() to make sure messages from different requesters are not 
   mixed. */
void *drms_sums_thread(void *arg)
{
  int status;
  DRMS_Env_t *env;
  SUM_t *sum=NULL;
  DRMS_SumRequest_t *request, *reply;
  long stop, empty;
  int connected = 0;
  char *ptmp;
  TIMER_t *timer = NULL;
  int tryagain;
  int sleepiness;
  int tryconnect;
  int shuttingdown;
  sem_t *sdsem = drms_server_getsdsem();
  int sumscallret;
  int rv;

  env = (DRMS_Env_t *) arg;

  /* Block signals. */
  /* There are several signals that must be handled by the signal thread only, and one
   * signal, SIGUSR2, that must be handled by the main thread only.
   */
  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, NULL)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);
    Exit(1);
  }

  /* Set up signal-handler - just to handle SIGPIPE, which could be sent when DRMS tries to
   * write to the (absent or non-functioning) RPC socket to SUMS. */
  struct sigaction nact;

  memset(&nact, 0, sizeof(struct sigaction));
  nact.sa_handler = SigPipeHndlr;
  nact.sa_flags = SA_RESTART;

  /* Handle SIGPIPE - this is process-wide behavior! What happens if SIGPIPE is delivered to some thread
   * other than the SUMS thread? I dunno - must test this. */
  sigaction(SIGPIPE, &nact, NULL);

#ifdef DEBUG
  printf("drms_sums_thread started.\n");
  fflush(stdout);
#endif
  
  if (!gSUMSbusyMtx)
  {
     gSUMSbusyMtx = malloc(sizeof(pthread_mutex_t));
     XASSERT(gSUMSbusyMtx);
     pthread_mutex_init(gSUMSbusyMtx, NULL); 
  }

  /* Main processing loop. */
  stop = 0;
  empty = 0;
  tryconnect = 1;

  while ( !stop || !empty)
  {
     /* if stop == 1, the the queue is corked, accepting no new items. So then we want
      * to process any remaining items in the queue before breaking out of this while loop.
      * If the queue has been corked (and stop == 1), then tqueueDelAny() will return
      * whether or not the queue is empty (unless there was an error in tqueueDelAny() - 
      * this is a bug - this function shouldn't mix up "status" with "emptiness"). But 
      * the original code, here since the beginning of time, always considered the return
      * value from tqueueDelAny() as an indicator of emptiness. If the queue has not
      * been corked then, if tqueueDelAny() encounters no internal error, the function
      * returns 0 always. So, we really shouldn't look at the return value from this
      * function, unless stop == 1. If stop == 1, then we should set empty to 
      * the return from this function. 
      *
      * ART - 10/21/2011 */

     /* Wait for the next SUMS request to arrive in the inbox. */
     /* sum_tag is the thread id of the thread who made the original SUMS request */
    env->sum_tag = 0;
    rv = tqueueDelAny(env->sum_inbox, &env->sum_tag,  &ptmp );

    if (stop == 1)
    {
       empty = rv;
    }

    request = (DRMS_SumRequest_t *) ptmp;

    if (tryconnect && !connected && request->opcode!=DRMS_SUMCLOSE)
    {
      /* Connect to SUMS. */
       tryagain = 1;
       sleepiness = 1;

       while (tryagain)
       {
          tryagain = 0;

          if (!sum)
          {
             /* SUMS not there - try to connect. */
             if (env->verbose)
             {
                timer = CreateTimer();
             }

             sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
             if (sumscallret == kBrokenPipe || sumscallret == kSUMSDead || sumscallret == kTooManySumsOpen)
             {
                /* free a non-null sum? */
                sum = NULL;
             }

             if (env->verbose && timer)
             {
                fprintf(stdout, "to call SUM_open: %f seconds.\n", GetElapsedTime(timer));
                DestroyTimer(&timer);
             }

             if (!sum)
             {
                if (env->loopconn && sumscallret != kSUMSDead && sumscallret != kTooManySumsOpen)
                {
                   fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
                   tryagain = 1;
                   sleep(sleepiness);
                   GettingSleepier(&sleepiness);
                }
                else
                {
                   /* Default behavior - don't try again, just send message to clients to terminate, and let this 
                    * thread terminate as well. */
                   stop = 1; /* exit main loop, after queue is empty (otherwise, keep going until queue is empty). */
                   tryconnect = 0; /* don't try connecting to SUMS again. */
                   empty = tqueueCork(env->sum_inbox); /* Don't allow new sums requests. */

                   fprintf(stderr,"Failed to connect to SUMS; terminating.\n");
                   fflush(stdout);
                  
                   sleep(1);
                   pthread_kill(env->signal_thread, SIGTERM); /* the signal thread will cause a DRMS_SUMCLOSE 
                                                               * SUMS request to be issued, which will terminate
                                                               * this thread. */
                }
             }
             else
             {
                connected = 1;
             }
          }

          /* check for user interrupting module. */
          if (sdsem)
          {
             sem_wait(sdsem);
             shuttingdown = (drms_server_getsd() != kSHUTDOWN_UNINITIATED);
             sem_post(sdsem);
          }

          if (shuttingdown)
          {
             tryagain = 0;
             tryconnect = 0;
          }

       } /* loop SUM_open() */

#ifdef DEBUG
      printf("drms_sums_thread connected to SUMS. SUMID = %llu\n",sum->uid);
      fflush(stdout);
#endif
    }

    /* Check for special CLOSE or ABORT codes. */
    if (request->opcode==DRMS_SUMCLOSE)
    {
      if (!stop)
      {
         /* if stop == 1, then the first SUM_open() failed, so we already corked the queue.
          * Don't try to cork a corked queue. */
         stop = 1;

         /* tqueueCork() does, when no error happens, return whether or not the 
          * queue is empty. Otherwise it returns an error code. So this is a bug -
          * it should always return one thing, either whether or not the queue
          * is empty, or a status, not both. 
          *
          * There is actually no race condition with empty 
          * because a corked queue cannot accept new items. Even as tqueueDelAny()
          * is called, there is no race condition, because no new items can
          * be added to the queue. This is only true if stop == 1 (and the queue is
          * corked). 
          *
          * ART - 10/21/2011 */
         empty = tqueueCork(env->sum_inbox); /* Do not accept any more requests, but keep
                                                processing all the requests already in
                                                the queue.*/
      }
    }
    else if ( request->opcode==DRMS_SUMABORT )
    {
      break;
    }
    else /* A regular request. */
    {
       if (connected)
       {
          /* Send the request to SUMS. sum_svc could die while processing is happening. 
           * If that is the case drms_process_sums_request() will attempt to re-open 
           * sum_svc with a SUM_open() call. If that happens, drms_process_sums_request()
           * will return the new SUM_t. */
          reply = drms_process_sums_request(env, &sum, request);
       }
       else
       {
          /* Send a reply to the client saying that SUM_open() failed. */
          reply = malloc(sizeof(DRMS_SumRequest_t));
          XASSERT(reply);
          reply->opcode = DRMS_ERROR_SUMOPEN;
       }

       if (reply)
       {
          if (!request->dontwait) {
             /* Put the reply in the outbox. */
             tqueueAdd(env->sum_outbox, env->sum_tag, (char *) reply);
          } else {
             /* If the calling thread waits for the reply, then it is the caller's responsibility 
              * to clean up. Otherwise, clean up here. */
             for (int i = 0; i < request->reqcnt; i++) {
                if (reply->sudir[i])
                {
                   free(reply->sudir[i]);
                }

                /* The sunum array is statically defined - no need to free. */
             }
             free(reply);
          }
       }
       env->sum_tag = 0; // done processing
    }

    /* Note: request is only shallow-freed. The is the requestor's responsiblity 
     * to free any memory allocated for the dsname, comment, and sudir fields. */
    free(request);
  } /* main loop on queue. */

  if (connected && sum)
  {
    /* Disconnect from SUMS. */
     sumscallret = MakeSumsCall(env, DRMS_SUMCLOSE, &sum, printf);
     if (sumscallret == kBrokenPipe)
     {
        fprintf(stderr, "Unable to call SUM_close(); broken pipe; not retrying.\n");
     }
  }

  if (gSUMSbusyMtx)
  {
     pthread_mutex_destroy(gSUMSbusyMtx); 
  }

  return NULL;
}

int SUMExptErr(const char *fmt, ...)
{
   char string[4096];

   va_list ap;
   va_start(ap, fmt);
   vsnprintf(string, sizeof(string), fmt, ap);
   va_end (ap);
   return fprintf(stderr, "%s", string);
}

static int IsSgPending(SUMID_t id, uint64_t *sunums, int nsunums)
{
    int ans;
    int isunum;
    char idstr[32];
    char sunumstr[32];
    HContainer_t *idH = NULL;
    
    ans = 0;
    
    if (gSgPending)
    {
        snprintf(idstr, sizeof(idstr), "%u", id); /* id is a uint32_t */
        if ((idH = hcon_lookup(gSgPending, idstr)) != NULL)
        {
            for (isunum = 0; idH && isunum < nsunums; isunum++)
            {    
                snprintf(sunumstr, sizeof(sunumstr), "%llu", (unsigned long long)sunums[isunum]);
                if (hcon_member(idH, sunumstr))
                {
                    fprintf(stderr, "A SUM_get() request for the SU identified by SUNUM %s is pending.\n", sunumstr);
                    ans = 1;
                    break;
                }
            }
        }
    }
    
    return ans;
}

static int SetSgPending(SUMID_t id, uint64_t *sunums, int nsunums)
{
    int err;
    int isunum;
    char idstr[32];
    char sunumstr[32];
    HContainer_t *idH = NULL;
    
    err = 0;
    
    /* First, see if the container of pending SUs exists. */
    snprintf(idstr, sizeof(idstr), "%u", id); /* id is a uint32_t */
    if (gSgPending == NULL)
    {
        err = ((gSgPending = hcon_create(sizeof(HContainer_t), sizeof(idstr), NULL, NULL, NULL, NULL, 0)) == NULL);
    }
    else
    {
        idH = hcon_lookup(gSgPending, idstr);
    }
    
    if (!err)
    {
        if (!idH)
        {
            /* Must create the HContainer_t for this id. */
            err = ((idH = hcon_create(sizeof(char), sizeof(sunumstr), NULL, NULL, NULL, NULL, 0)) == NULL);
            
            if (!err)
            {
                /* Now we must add the id-specific HContainer_t to the gSgPending HContainer_t. */
                err = hcon_insert(gSgPending, idstr, gSgPending);
            }
        }
    }
    
    if (!err)
    {
        char val = 't';
        
        for (isunum = 0; isunum < nsunums; isunum++)
        {
            snprintf(sunumstr, sizeof(sunumstr), "%llu", (unsigned long long)sunums[isunum]);
            
            /* If the su is already pending, then error out. */
            if (hcon_member(idH, sunumstr))
            {
                fprintf(stderr, "Cannot flag this SU (sunum %s) as pending - it is already pending.\n", sunumstr);
                err = 1;
                break;
            }
            
            err = hcon_insert(idH, sunumstr, &val);
        }
    }

    return err;
}

static int UnsetSgPending(SUMID_t id)
{
    int err;
    char idstr[32];
    
    err = 0;
    
    if (gSgPending)
    {
        /* Free entire HContainer_t for this id. */
        snprintf(idstr, sizeof(idstr), "%u", id); /* id is a uint32_t */
        hcon_remove(gSgPending, idstr); /* no-op if not HContainer_t for id */
        
        if (hcon_member(gSgPending, idstr))
        {
            err = 1;
        }
        
        if (!err)
        {
            if (hcon_size(gSgPending) == 0)
            {
                hcon_destroy(&gSgPending);
                
                if (gSgPending)
                {
                    err = 1;
                }
            }
        }
    }
    
    return err;
}

static DRMS_SumRequest_t *drms_process_sums_request(DRMS_Env_t  *env,
						    SUM_t **suminout,
						    DRMS_SumRequest_t *request)
{
  int i;
  DRMS_SumRequest_t *reply = NULL;
  int shuttingdown = 0;
  sem_t *sdsem = drms_server_getsdsem();
  SUM_t *sum = NULL;
  int tryagain;    /* SUMS crash - try the SUMS API call again. */
  int nosums;      /* A SUM API call returned an error (not caused by a crash) - 
                    * a SUMS retry wouldn't make sense. */
  int sumscrashed; /* SUM_ping() says SUMS isn't there. */
  int sleepiness;
  int sumnoop;
  int sumscallret;

  if (suminout && *suminout)
  {
     sum = *suminout;
  }
  
  if (!sum)
  {
     fprintf(stderr , "Error in drms_process_sums_request(): No SUMS connection.\n");
     return NULL;
  }

  reply = malloc(sizeof(DRMS_SumRequest_t));
  XASSERT(reply);
  switch(request->opcode)
  {
  case DRMS_SUMALLOC:
#ifdef DEBUG
    printf("Processing SUMALLOC request.\n");
#endif
    if (request->reqcnt!=1)
    {
      fprintf(stderr,"SUM thread: Invalid reqcnt (%d) in SUMALLOC request.\n",
	     request->reqcnt);
      reply->opcode = DRMS_ERROR_SUMALLOC;
      break;
    }

    nosums = 0;
    tryagain = 1;
    sleepiness = 1;

    while (tryagain)
    {
       tryagain = 0;
       sumscrashed = 0;

       if (!sum)
       {
           /* SUMS crashed - open a new SUMS. */
           sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
           if (sumscallret == kBrokenPipe)
           {
               /* free a non-null sum? */
               sum = NULL;
           }
           else if (sumscallret == kSUMSDead)
           {
               sum = NULL;
               nosums = 1;
               reply->opcode = sumscallret;
               break;
           }
           
           if (!sum)
           {
               fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
               tryagain = 1;
               sleep(sleepiness);
               GettingSleepier(&sleepiness);
               continue;
           }
           else
           {
               *suminout = sum;
           }
       }

       sum->reqcnt = 1;
       sum->bytes = request->bytes;
       sum->group = request->group;

       /* An older version of SUMS used the storeset field to determine which partition set to 
        * use. But later versions of SUMS use sum->group to map to a partition set (there is a
        * SUMS table that maps group to set). */
       if (request->group < 0)
       {
          /* this SU alloc has nothing to do with any series. */
          sum->storeset = 0;
       }
       else
       {
          sum->storeset = request->group / kExtTapegroupSlot;
       }
 	 
       if (sum->storeset > kExtTapegroupMaxStoreset)
       {
          fprintf(stderr, "SUM thread: storeset '%d' out of range.\n", sum->storeset);
          reply->opcode = DRMS_ERROR_SUMALLOC;
          nosums = 1;
          break;
       }

       /* Make RPC call to the SUM server. */
       /* PERFORMANCE BOTTLENECK */
       /* drms_su_alloc() can be slow when the dbase is busy - this is due to 
        * SUM_open() calls backing up. */
       reply->opcode = MakeSumsCall(env, DRMS_SUMALLOC, &sum, printf);

       if (reply->opcode != 0)
       {
          sumscrashed = (reply->opcode == 4 || reply->opcode == kBrokenPipe);
          if (sumscrashed)
          {
             /* Not sure how to free sum - probably need to do this, especially since we're looping. */
             sum = NULL;
             fprintf(stderr, "SUMS no longer there; trying again in %d seconds.\n", sleepiness);
             tryagain = 1;
             sleep(sleepiness);
             GettingSleepier(&sleepiness);
          }
          else
          {
             fprintf(stderr,"SUM thread: SUM_alloc RPC call failed with "
                     "error code %d\n",reply->opcode);
             nosums = 1;
             break; // from loop
          }
       }
    } /* tryagain loop */

    if (!nosums)
    {
       reply->sunum[0] = sum->dsix_ptr[0];
       reply->sudir[0] = strdup(sum->wd[0]);
       free(sum->wd[0]);
#ifdef DEBUG
       printf("SUM_alloc returned sunum=%llu, sudir=%s.\n",reply->sunum[0],
              reply->sudir[0]);
#endif
    }
    break;

  case DRMS_SUMGET:
#ifdef DEBUG
    printf("Processing SUMGET request.\n");
#endif
    if (request->reqcnt<1 || request->reqcnt>DRMS_MAX_REQCNT)
    {
      fprintf(stderr,"SUM thread: Invalid reqcnt (%d) in SUMGET request.\n",
	     request->reqcnt);
      reply->opcode = -5; /* invalid number of SUNUMs in SUM_get() request. */
      break;
    }

    /* Ensure that it is not the case that there is an SUNUM that is part of a pending SUM_get(). */
    if (IsSgPending(sum->uid, request->sunum, request->reqcnt))
    {
       fprintf(stderr, "Unable to process a SUM_get() request. One or more requested storage units being requested are pending.n");
       reply->opcode = -2; /* pending tape read */
       break;
    }

#ifdef DEBUG
    printf("SUM thread: calling SUM_get\n");
#endif

    nosums = 0;
    tryagain = 1;
    sleepiness = 1;

    while (tryagain)
    {
        tryagain = 0;
        sumscrashed = 0;
        
        if (!sum)
        {
            /* SUMS crashed - open a new SUMS. */
            sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
            if (sumscallret == kBrokenPipe)
            {
                /* free a non-null sum? */
                sum = NULL;
            }
            else if (sumscallret == kSUMSDead)
            {
                sum = NULL;
                nosums = 1;
                reply->opcode = sumscallret;
                break;
            }
            
            if (!sum)
            {
                fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
                tryagain = 1;
                sleep(sleepiness);
                GettingSleepier(&sleepiness);
                continue;
            }
            else
            {
                *suminout = sum;
            }
        }
        
        sum->reqcnt = request->reqcnt;
        sum->mode = request->mode;
        sum->tdays = request->tdays;
        for (i=0; i<request->reqcnt; i++)
            sum->dsix_ptr[i] = request->sunum[i];
        
        /* Make RPC call to the SUM server. */
        reply->opcode = MakeSumsCall(env, DRMS_SUMGET, &sum, printf);

#ifdef DEBUG
        printf("SUM thread: SUM_get returned %d\n",reply->opcode);
#endif
        
        if (reply->opcode == RESULT_PEND)
        {
            /* This SUM_wait() call can take a while. If DRMS is shutting down, 
             * then don't wait. Should be okay to get shut down sem since 
             * the main and signal threads don't hold onto them for too long. */
            int pollrv = 0;
            int naptime = 1;
            int nloop = 10;
            int maxloop = 21600; /* 6 hours (very roughly) */
            
            /* Mark these sunums as SUM_get()-pending. */
            if (SetSgPending(sum->uid, sum->dsix_ptr, sum->reqcnt))
            {
                reply->opcode = -3; /* failiure to run SetSgPending() */
                nosums = 1;
                break;
            }
            
            /* WARNING - this is potentially an infinite loop. */
            while (1)
            {
                if (maxloop <= 0)   
                {
                    /* tape read didn't complete */
                    fprintf(stderr, "Tape read has not completed; try again later.\n");
                    nosums = 1;
                    
                    drms_lock_server(env);
                    /* It is no longer ok to call SUMS. Once DRMS times-out waiting on SUMS, 
                     * if DRMS were to call SUMS again, SUMS could break. */
                    env->sumssafe = 0;
                    drms_unlock_server(env);
                    break;
                }
                
                if (sdsem)
                {
                    sem_wait(sdsem);
                    shuttingdown = (drms_server_getsd() != kSHUTDOWN_UNINITIATED);
                    sem_post(sdsem);
                }
                
                if (shuttingdown)
                {
                    reply->opcode = 0;
                    break; // from inner loop
                }
                
                if (gSUMSbusyMtx)
                {
                    pthread_mutex_lock(gSUMSbusyMtx);
                    gSUMSbusy = 1;
                    pthread_mutex_unlock(gSUMSbusyMtx);
                }
                
                /* if sum_svc is down, 
                 * then SUM_poll() will never return anything but TIMEOUTMSG. */
                if (nloop <= 0)
                {
                    pollrv = SUM_poll(sum);
                    nloop = 10;
                }
                else
                {
                    if (gSUMSbusyMtx)
                    {
                        pthread_mutex_lock(gSUMSbusyMtx);
                        gSUMSbusy = 0;
                        pthread_mutex_unlock(gSUMSbusyMtx);
                    }
                    
                    nloop--;
                    maxloop--;
                    sleep(1);
                    continue;
                }
                
                if (env->verbose)
                {
                    fprintf(stdout, "SUM_poll() returned %d.\n", pollrv);
                }
                
                if (gSUMSbusyMtx)
                {
                    pthread_mutex_lock(gSUMSbusyMtx);
                    gSUMSbusy = 0;
                    pthread_mutex_unlock(gSUMSbusyMtx);
                }
                
                if (pollrv == 0)
                {
                    /* There could be an error: sum->status is examined below and if it is not 0, then
                     * this means there was some fatal error. The module should bail. */
                    if (UnsetSgPending(sum->uid))
                    {
                       /* There was an error unsetting the pending flag on all the contained SUNUMS. It
                        * might still be set. */
                       reply->opcode = -4; /* Error releasing pending flags on SUNUMs. */
                    }
                    break; // from inner loop
                }
                else
                {
                    /* One of four things is true:
                     *   1. The tape drive is still working on the tape fetch request (the tape could be on a shelf somewhere even). 
                     *   2. sum_svc has died. 
                     *   3. tape_svc has died. 
                     *   4. driveX_svc has died.
                     * Call SUM_nop() to differentiate these options. It returns:
                     *   4 - sum_svc crashed or restarted. 
                     *   5 - tape_svc crashed or restarted. 
                     *   6 - driveX_svc crashed or restarted. 
                     *
                     * If sum_svc has crashed or restarted, then SUM_open() needs to be called again. Otherwise, if
                     * tape_svc or driveX_svc has crashed or restarted, then SUM_get() needs to be called again. */
                    sumnoop = SUM_nop(sum, printf);
                    
                    if (env->verbose)
                    {
                        fprintf(stdout, "SUM_nop() returned %d.\n", sumnoop);
                    }
                    
                    sumscrashed = (sumnoop == 4);
                    if (sumnoop >= 4)
                    {
                        /* try again...but figure out to which loop starting position to return. */
                        if (sumscrashed)
                        {
                            /* Must go back to SUM_open(). */
                            sum = NULL;
                            if (UnsetSgPending((sum->uid)))
                            {
                               /* There was an error unsetting the pending flag on all the contained SUNUMS. It             
                                * might still be set. */
                               reply->opcode = -4; /* Error releasing pending flags on SUNUMs. */
                               break; /* from inner loop, but tryagain == 0, so breaks from outter loop too. */
                            }

                            fprintf(stderr, "sum_svc no longer there; trying SUM_open() then SUM_get() again in %d seconds.\n", sleepiness);
                        }
                        else
                        {
                            fprintf(stderr, "Either tape_svc or driveX_svc died; trying SUM_get() again in %d seconds.\n", sleepiness);
                        }
                        
                        /* Must go back to SUM_get(). */
                        tryagain = 1;
                        sleep(sleepiness);
                        GettingSleepier(&sleepiness);
                        break; // from inner loop
                    }
                    else
                    {
                        /* Must go back to SUM_poll(). */
                        if (env->verbose)
                        {
                            fprintf(stdout, "Tape fetch has not completed, waiting for %d seconds.\n", naptime);
                        }
                        sleep(naptime);
                        
                        /* don't increase the length of the nap - we want to keep polling at a regular, quick interval. */
                    }
                }
            } /* inner while (loop on polling) */
            
            if (!shuttingdown && !tryagain)
            {
                reply->opcode = pollrv;
                
                if (reply->opcode || sum->status)
                {
                    fprintf(stderr,"SUM thread: Last SUM_poll call returned "
                            "error code = %d, sum->status = %d.\n",
                            reply->opcode, sum->status);
                    nosums = 1; /* sums error - don't continue */
                    break; // from outer loop
                }
            }
        }
        else if (reply->opcode == 1)
        {
            /* SUMS received a duplicate SUNUM in the same SUMS session. This is a fatal error. We are actively 
             * trying to find code that causes duplicate SUNUMs to be issued too, but in case we haven't found
             * all the places that cause this error, this block of code will prevent Sget() from crashing 
             * due to duplicate SUNUMs. */
            fprintf(stderr, "SUMS thread: DRMS client called SUM_get() with a duplicate SUNUM (sums status = %d).\n", sum->status);
            nosums = 1;
            break;
        }
        else if (reply->opcode != 0)
        {
            sumnoop = SUM_nop(sum, printf);
            sumscrashed = (sumnoop == 4 || reply->opcode == kBrokenPipe);
            if (sumnoop >= 4 || reply->opcode == kBrokenPipe)
            {
                if (sumscrashed)
                {
                    sum = NULL;
                    fprintf(stderr, "sum_svc no longer there; trying SUM_open() then SUM_get() again in %d seconds.\n", sleepiness);
                }
                else
                {
                    fprintf(stderr, "Either tape_svc or driveX_svc died; trying SUM_get() again in %d seconds.\n", sleepiness);
                }
                
                tryagain = 1;
                sleep(sleepiness);
                GettingSleepier(&sleepiness);
            }
            else
            {
                fprintf(stderr,"SUM thread: SUM_get RPC call failed with "
                        "error code %d\n",reply->opcode);
                nosums = 1; /* sums error - don't continue */
                break; // from outer loop
            }
        }
    } /* tryagain (outer) loop */

    if (!nosums)
    {
       for (i=0; i<request->reqcnt; i++)
       {
          if (!shuttingdown)
          {
             reply->sudir[i] = strdup(sum->wd[i]);
             free(sum->wd[i]);
#ifdef DEBUG
             printf("SUM thread: got sudir[%d] = %s = %s\n",i,reply->sudir[i],sum->wd[i]);
#endif
          }
          else
          {
             reply->sudir[i] = strdup("NA (shuttingdown)");
          }
       }
    }
    break;

  case DRMS_SUMPUT:
  {
#ifdef DEBUG
    printf("Processing SUMPUT request.\n");
#endif
    if (request->reqcnt<1 || request->reqcnt>DRMS_MAX_REQCNT)
    {
      fprintf(stderr,"SUM thread: Invalid reqcnt (%d) in SUMPUT request.\n",
	     request->reqcnt);
      reply->opcode = DRMS_ERROR_SUMPUT;
      break;
    }

#ifdef DEBUG2
      #define LOGDIR  "/tmp22/production/"
      #define LOGFILE "sumputlog.txt"
      struct stat stBuf;
      FILE *fptr = NULL;

      if (stat(LOGDIR, &stBuf) == 0 && S_ISDIR(stBuf.st_mode))
      {
         fptr = fopen(LOGDIR"/"LOGFILE, "a");
         if (fptr)
         {
            char tbuf[128];
            struct tm *ltime = NULL;
            time_t secs = time(NULL);

            ltime = localtime(&secs);
            strftime(tbuf, sizeof(tbuf), "%Y.%m.%d_%T", ltime);

            fprintf(fptr, "SUM_put() call for SUNUMs for series %s at %s:\n", request->dsname, tbuf);
         }
      }
#endif

    tryagain = 1;
    sleepiness = 1;

    while (tryagain)
    {
       tryagain = 0;
       sumscrashed = 0;

       if (!sum)
       {
           /* SUMS crashed - open a new SUMS. */
           sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
           if (sumscallret == kBrokenPipe)
           {
               /* free a non-null sum? */
               sum = NULL;
           }
           else if (sumscallret == kSUMSDead)
           {
               sum = NULL;
               nosums = 1;
               reply->opcode = sumscallret;
               break;
           }
           
           if (!sum)
           {
               fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
               tryagain = 1;
               sleep(sleepiness);
               GettingSleepier(&sleepiness);
               continue;
           }
           else
           {
               *suminout = sum;
           }
       }

       sum->dsname = request->dsname;
       sum->group = request->group;
       sum->mode = request->mode;
       sum->tdays = request->tdays;
       sum->reqcnt = request->reqcnt;
       sum->history_comment = request->comment;

       for (i=0; i<request->reqcnt; i++)
       {
          sum->dsix_ptr[i] = request->sunum[i];
          sum->wd[i] = request->sudir[i];
#ifdef DEBUG
          printf("putting SU with sunum=%lld and sudir='%s' to SUMS.\n",
                 request->sunum[i],request->sudir[i]);
#endif

#ifdef DEBUG2
          if (fptr)
          {
             fprintf(fptr, "  %lld\n", (long long)request->sunum[i]);
          }     
#endif
       }

       /* Make RPC call to the SUM server. */
       reply->opcode = MakeSumsCall(env, DRMS_SUMPUT, &sum, printf);

       if (reply->opcode != 0)
       {
          sumscrashed = (reply->opcode == 4 || reply->opcode == kBrokenPipe);
          if (sumscrashed)
          {
             sum = NULL;
             fprintf(stderr, "SUMS no longer there; trying again in %d seconds.\n", sleepiness);
             tryagain = 1;
             sleep(sleepiness);
             GettingSleepier(&sleepiness);
          }
          else
          {
             fprintf(stderr,"SUM thread: SUM_put call failed with stat=%d.\n",
                     reply->opcode);
             break; // from loop
          }
       }
    } /* while loop */

#ifdef DEBUG2
    if (fptr)
    {
       fclose(fptr);
       fptr = NULL;
    }
#endif
  }
  break;
  case DRMS_SUMDELETESERIES:
    if (request->comment)
    {
       /* request->comment is actually a pointer to an array of sunums */
       char *comment = strdup(request->comment);
       char *fpath = NULL;
       char *series = NULL;
       char *sep = NULL;

       if (comment)
       {
          if ((sep = strchr(comment, ',')) != NULL)
          {
             *sep = '\0';
             fpath = comment;
             series = sep + 1;

             nosums = 0;
             tryagain = 1;
             sleepiness = 1;

             while (tryagain)
             {
                tryagain = 0;
                sumscrashed = 0;

                if (!sum)
                {
                    /* SUMS crashed - open a new SUMS. */
                    sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
                    if (sumscallret == kBrokenPipe)
                    {
                        /* free a non-null sum? */
                        sum = NULL;
                    }
                    else if (sumscallret == kSUMSDead)
                    {
                        sum = NULL;
                        nosums = 1;
                        reply->opcode = sumscallret;
                        break;
                    }
                    
                    if (!sum)
                    {
                        fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
                        tryagain = 1;
                        sleep(sleepiness);
                        GettingSleepier(&sleepiness);
                        continue;
                    }
                    else
                    {
                        *suminout = sum;
                    }
                }

                reply->opcode = MakeSumsCall(env, DRMS_SUMDELETESERIES, NULL, printf, fpath, series);

                if (reply->opcode != 0)
                {
                   sumscrashed = (reply->opcode == 4 || reply->opcode == kBrokenPipe);
                   if (sumscrashed)
                   {
                      sum = NULL;
                      fprintf(stderr, "SUMS no longer there; trying again in %d seconds.\n", sleepiness);
                      tryagain = 1;
                      sleep(sleepiness);
                      GettingSleepier(&sleepiness);
                   }
                   else
                   {
                      fprintf(stderr,"SUM thread: SUM_delete_series call failed with stat=%d.\n",
                              reply->opcode);
                      nosums = 1;
                      break;
                   }
                }
             }
          }

          free(comment);
       }
    }
    break;
  case DRMS_SUMALLOC2:
    /* Do not make this call available to "_sock" connect modules - it is 
     * to be used by modules that ingest remote storage units and it
     * does not affect any kind of DRMS/PSQL session. */
    if (request->reqcnt!=1)
    {
       fprintf(stderr,"SUM thread: Invalid reqcnt (%d) in SUMALLOC request.\n",
               request->reqcnt);
       reply->opcode = DRMS_ERROR_SUMALLOC;
       break;
    }

    nosums = 0;
    tryagain = 1;
    sleepiness = 1;

    while (tryagain)
    {
       tryagain = 0;
       sumscrashed = 0;

       if (!sum)
       {
           /* SUMS crashed - open a new SUMS. */
           sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
           if (sumscallret == kBrokenPipe)
           {
               /* free a non-null sum? */
               sum = NULL;
           }
           else if (sumscallret == kSUMSDead)
           {
               sum = NULL;
               nosums = 1;
               reply->opcode = sumscallret;
               break;
           }
           
           if (!sum)
           {
               fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
               tryagain = 1;
               sleep(sleepiness);
               GettingSleepier(&sleepiness);
               continue;
           }
           else
           {
               *suminout = sum;
           }
       }

       sum->reqcnt = 1;
       sum->bytes = request->bytes;
       sum->group = request->group;

       /* An older version of SUMS used the storeset field to determine which partition set to 
        * use. But later versions of SUMS use sum->group to map to a partition set (there is a
        * SUMS table that maps group to set). */
       if (request->group < 0)
       {
          /* this SU alloc has nothing to do with any series. */
          sum->storeset = 0;
       }
       else
       {
          sum->storeset = request->group / kExtTapegroupSlot;
       }
 	 
       if (sum->storeset > kExtTapegroupMaxStoreset)
       {
          fprintf(stderr, "SUM thread: storeset '%d' out of range.\n", sum->storeset);
          reply->opcode = DRMS_ERROR_SUMALLOC;
          nosums = 1;
          break;
       }

       /* Make RPC call to the SUM server. */
       reply->opcode = MakeSumsCall(env, DRMS_SUMALLOC2, &sum, printf, request->sunum[0]);

       if (reply->opcode != 0)
       {
          sumscrashed = (reply->opcode == 4 || reply->opcode == kBrokenPipe);
          if (sumscrashed)
          {
             sum = NULL;
             fprintf(stderr, "SUMS no longer there; trying again in %d seconds.\n", sleepiness);
             tryagain = 1;
             sleep(sleepiness);
             GettingSleepier(&sleepiness);
          }
          else
          {
             fprintf(stderr,"SUM thread: SUM_alloc2 RPC call failed with "
                     "error code %d\n", reply->opcode);
             nosums = 1;
             break; // from loop
          }
       }
    }

    if (!nosums)
    {
       reply->sudir[0] = strdup(sum->wd[0]);
       free(sum->wd[0]); 
    }
    break;
  case DRMS_SUMEXPORT:
    {
       /* cast the comment field of the request - 
        * it contains the SUMEXP_t struct */
       SUMEXP_t *sumexpt = (SUMEXP_t *)request->comment;

       /* fill in the uid */
       sumexpt->uid = sum->uid;

       /* Make RPC call to the SUM server. */
       if ((reply->opcode = MakeSumsCall(env, DRMS_SUMEXPORT, &sum, SUMExptErr, sumexpt)))
       {
          fprintf(stderr,"SUM thread: SUM_export RPC call failed with "
                  "error code %d\n", reply->opcode);
          break;
       }

       /* Don't retry SUM_export() on failure. If this fails because SUMS is down, just
        * tell the caller, and let them handle the problem, retrying if they like. */
    }
    break;
  case DRMS_SUMINFO:
    {
       HContainer_t *map = NULL;
       int isunum;
       char key[128];
       SUM_info_t *nulladdr = NULL;
       uint64_t dxarray[MAXSUNUMARRAY];

       if (request->reqcnt < 1 || request->reqcnt > MAXSUMREQCNT)
       {
          fprintf(stderr,"SUM thread: Invalid reqcnt (%d) in SUMINFO request.\n",
                  request->reqcnt);
          reply->opcode = DRMS_ERROR_SUMINFO;
          break;
       }

       nosums = 0;
       tryagain = 1;
       sleepiness = 1;

       while (tryagain)
       {
          tryagain = 0;
          sumscrashed = 0;

          if (!sum)
          {
              /* SUMS crashed - open a new SUMS. */
              sumscallret = MakeSumsCall(env, DRMS_SUMOPEN, &sum, printkerr, NULL, NULL);
              if (sumscallret == kBrokenPipe)
              {
                  /* free a non-null sum? */
                  sum = NULL;
              }
              else if (sumscallret == kSUMSDead)
              {
                  sum = NULL;
                  nosums = 1;
                  reply->opcode = sumscallret;
                  break;
              }
              
              if (!sum)
              {
                  fprintf(stderr, "Failed to connect to SUMS; trying again in %d seconds.\n", sleepiness);
                  tryagain = 1;
                  sleep(sleepiness);
                  GettingSleepier(&sleepiness);
                  continue;
              }
              else
              {
                  *suminout = sum;
              }
          }

          /* IMPORTANT! SUM_infoEx() will not support duplicate sunums. So, need to 
           * remove duplicates, but map each SUNUM in the original request to 
           * the SUNUM in the array of streamlined requests. */
          map = hcon_create(sizeof(SUM_info_t *), 128, NULL, NULL, NULL, NULL, 0);

          for (i = 0, isunum = 0; i < request->reqcnt; i++)
          {
             /* One or more sunums may be ULLONG_MAX (originally, they were -1, but because request->sunum 
              * is an array of uint64_t, they will have been cast to ULLONG_MAX). Don't put the -1 
              * requests in dxarray, because -1 is invalid as far as SUMS is concerned. */
             if (request->sunum[i] == ULLONG_MAX)
             {
                /* skip sunum == -1. */
                continue;
             }

             snprintf(key, sizeof(key), "%llu", (unsigned long long)request->sunum[i]);
             if (!hcon_member(map, key))
             {
                dxarray[isunum] = request->sunum[i];
                hcon_insert(map, key, &nulladdr);
                isunum++;
             }
          }

          sum->reqcnt = isunum;
          sum->sinfo = NULL;

          /* Make RPC call to the SUM server. */
          reply->opcode = MakeSumsCall(env, DRMS_SUMINFO, &sum, printf, dxarray, isunum);
      
          if (reply->opcode != 0)
          {
             sumscrashed = (reply->opcode == 4 || reply->opcode == kBrokenPipe);
             if (sumscrashed)
             {
                sum = NULL;
                fprintf(stderr, "SUMS no longer there; trying again in %d seconds.\n", sleepiness);
                tryagain = 1;
                hcon_destroy(&map);
                sleep(sleepiness);
                GettingSleepier(&sleepiness); 
             }
             else
             {
                fprintf(stderr,"SUM thread: SUM_infoArray RPC call failed with "
                        "error code %d\n", reply->opcode);
                nosums = 1;
                break; // from loop
             }
          }
       } // while loop

       if (!nosums)
       {
          /* Returns a linked-list of SUM_info_t structures in sum->sinfo. Each 
           * node has been malloc'd by SUM_infoEx(). It is the caller's responsibility to 
           * free the list. */
          SUM_info_t *psinfo = sum->sinfo;
          SUM_info_t **pinfo = NULL;

          /* Use the sudir field to hold the resultant SUM_info_t structs */
          for (i = 0; i < isunum; i++)
          {
             snprintf(key, sizeof(key), "%llu", (unsigned long long)(dxarray[i]));

             if (!psinfo)
             {
                fprintf(stderr, "SUMS did not return a SUM_info_t struct for sunum %s.\n", key);
                reply->opcode = 99;
                break;
             }

             /* NOTE - if an sunum is unknown, the returned SUM_info_t will have the sunum set 
              * to -1.  So don't use the returned sunum. */
             if ((pinfo = hcon_lookup(map, key)) != NULL)
             {
                *pinfo = psinfo;
                /* work around SUMS ditching the SUNUM for bad SUNUMs - modify the info struct returned 
                 * by SUMS directly. */
                (*pinfo)->sunum = dxarray[i];
             }
             else
             {
                fprintf(stderr, "Information returned for sunum '%s' unknown to DRMS.\n", key);
                reply->opcode = 99;
                break; // from loop
             }

             psinfo = psinfo->next;
          }

           if (reply->opcode == 0)
           {
               for (i = 0; i < request->reqcnt; i++)
               {
                   /* request->sunum may contain duplicate sunums. One or more sunums may be ULLONG_MAX also 
                    * (originally, they were -1, but because request->sunum is an array of uint64_t, 
                    * they will have been cast to ULLONG_MAX). */
                   if (request->sunum[i] == ULLONG_MAX)
                   {
                       /* Create a dummy, empty SUM_info_t*/
                       reply->sudir[i] = (char *)malloc(sizeof(SUM_info_t));
                       memset(reply->sudir[i], 0, sizeof(SUM_info_t));
                       ((SUM_info_t *)(reply->sudir[i]))->sunum = ULLONG_MAX; /* This will be cast to -1 by the 
                                                                               * calling code. */
                       continue;
                   }

                   snprintf(key, sizeof(key), "%llu", (unsigned long long)(request->sunum[i]));
                   if ((pinfo = hcon_lookup(map, key)) != NULL)
                   {
                       reply->sudir[i] = (char *)malloc(sizeof(SUM_info_t));
                       *((SUM_info_t *)(reply->sudir[i])) = **pinfo;
                       ((SUM_info_t *)(reply->sudir[i]))->next = NULL;
                   }
                   else
                   {
                       fprintf(stderr, "Information returned for sunum '%s' unknown to DRMS.\n", key);
                       reply->opcode = 99;
                       break; // from loop
                   }
               }
           }

          SUM_infoArray_free(sum); /* This will free all the SUM_info_t structs owned by SUMS. */
       }

       /* clean up - free up the SUM_info_t structs pointed to by map */
       hcon_destroy(&map);
    }
    break;
  default:
    fprintf(stderr,"SUM thread: Invalid command code (%d) in request.\n",
	   request->opcode);
    reply->opcode = DRMS_ERROR_SUMBADOPCODE;
    break;
  }
  return reply;
}

/****************** Server signal handler thread functions *******************/
int drms_server_registercleaner(DRMS_Env_t *env, CleanerData_t *data)
{
   int gotlock = 0;
   int ok = 1;

   gotlock = (drms_trylock_server(env) == 0);

   if (gotlock)
   {
      if (env->cleaners == NULL)
      {
         /* No cleaner nodes yet - create list and add first one. */
         env->cleaners = list_llcreate(sizeof(CleanerData_t), NULL);
         if (!env->cleaners)
         {
            fprintf(stderr, "Can't register cleaner.\n");
            ok = 0;
         }
      }

      if (ok)
      {
         if (!list_llinserttail(env->cleaners, data))
         {
            fprintf(stderr, "Can't register cleaner.\n");
            ok = 0;
         }
      }

      drms_unlock_server(env);
   }
   else
   {
      fprintf(stderr, "Can't register doit cleaner function. Unable to obtain mutex.\n");
   }
  
   return ok;
}

static void HastaLaVistaBaby(DRMS_Env_t *env, int signo)
{
   char errbuf[256];

   if (gShutdownsem)
   {
      sem_wait(gShutdownsem);
   }

   if (signo == SIGUSR1)
   {
      gShutdown = kSHUTDOWN_COMMIT;
   }
   else
   {
      gShutdown = kSHUTDOWN_ABORT;
   }

   if (gShutdownsem)
   {
      sem_post(gShutdownsem);
   }

   drms_lock_server(env);
   /* Send a cancel request to the db - this will result in an attempt to cancel 
    * the currently db. This is only useful if there is currently a long-running
    * query (executed by the main thread). */

   if (!db_cancel(env->session->db_handle, errbuf, sizeof(errbuf)))
   {
      if (!env->selfstart)
      {
         fprintf(stderr, "Unsuccessful attempt to cancel current db command: %s\n", errbuf);
      }
   }

   drms_unlock_server(env);

   /* must cancel current command in both db connections. */

   /* Calls drms_server_commit() or drms_server_abort().
    * Don't set last argment to final - still need environment below. */
   /* This will cause the SUMS thread to return. */
   /* If no transaction actually started, then noop. */

   /* don't lock server lock - drms_server_end_transaction() locks/unlocks it. */
   drms_server_end_transaction(env, signo != SIGUSR1, 0);

   /* Don't wait for main thread to terminate, it may be in the middle of a long DoIt(). But 
    * main can't start using env and the database and cmdparams after we clean them up. */

   /* Must disconnect db, because we didn't set the final flag in drms_server_end_transaction() */

   drms_lock_server(env);
   db_disconnect(&env->session->db_handle);
   drms_unlock_server(env);

   /* must lock inside drms_free_env() since it destroys the lock. */
   drms_free_env(env, 1); 

   /* doesn't call at_exit_action() */
   _exit(signo != SIGUSR1);
}

void *drms_signal_thread(void *arg)
{
  int status, signo;
  DRMS_Env_t *env = (DRMS_Env_t *) arg;
  int doexit = 0;

#ifdef DEBUG
  printf("drms_signal_thread started.\n");
  fflush(stdout);
#endif

  /* Block signals. */
  /* It is necessary to block a signal that the thread is going to wait for. Blocking
   * will prevent the delivery of those signals that are blocked. However, sigwait
   * will still return when a signal becomes pending. */
  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, NULL)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);
    Exit(1);
  }

  for (;;)
  {
    if ((status = sigwait(&env->signal_mask, &signo)))
    {
      if (status == EINTR)
      {
	fprintf(stderr,"sigwait error, errcode=%d.\n",status);
	continue;
      }
      else
      {
	fprintf(stderr,"sigwait error, errcode=%d.\n",status);
	Exit(1);
      }
    }

    switch(signo)
    {
    case SIGINT:
      fprintf(stderr,"WARNING: DRMS server received SIGINT...exiting.\n");
      break;
    case SIGTERM:
      fprintf(stderr,"WARNING: DRMS server received SIGTERM...exiting.\n");
      break;
    case SIGQUIT:
      fprintf(stderr,"WARNING: DRMS server received SIGQUIT...exiting.\n");
      break; 
    case SIGUSR1:
      if (env->verbose) {
	fprintf(stderr,"DRMS server received SIGUSR1...commiting data & stopping.\n");
	printf("DRMS server received SIGUSR1...commiting data & stopping.\n");
      }
      break;
       case SIGUSR2:
         if (env->verbose) 
         {
            fprintf(stdout,"signal thread received SIGUSR2 (main shutting down)...exiting.\n");
         }

         pthread_exit(NULL);
      break;
    default:
      fprintf(stderr,"WARNING: DRMS server received signal no. %d...exiting.\n", 
	      signo);

      Exit(1);
      break;
    }

    switch(signo)
    {
    case SIGINT:
    case SIGTERM:
    case SIGQUIT:
    case SIGUSR1:
      /* No need to acquire lock to look at env->shutdownsem, which was either 
       * created or not before the signal_thread existed. By the time
       * execution gets here, shutdownsem is read-only */
      if (gShutdownsem)
      {
         /* acquire shutdown lock */
         sem_wait(gShutdownsem);

         if (gShutdown == kSHUTDOWN_UNINITIATED)
         {
            /* There is no shutdown in progress - okay to start shutdown */
            gShutdown = (signo == SIGUSR1) ? 
              kSHUTDOWN_COMMITINITIATED : 
              kSHUTDOWN_ABORTINITIATED; /* Shutdown initiated */

            if (!env->selfstart || env->verbose || signo == SIGTERM || signo == SIGINT)
            {
               fprintf(stderr, "Shutdown initiated.\n");
               if (gSUMSbusyMtx)
               {
                  pthread_mutex_lock(gSUMSbusyMtx);

                  if (gSUMSbusy)
                  {
                     fprintf(stderr, "SUMS is busy fetching from tape - please wait until operation completes.\n");
                  }

                  pthread_mutex_unlock(gSUMSbusyMtx);
               }
            }

            /* Allow DoIt() function a chance to clean up. */
            /* Call user-registered callback, if such a callback was registered, that cleans up 
             * resources used in the DoIt() loop */
            if (env->cleaners)
            {
               ListNode_t *lnode = NULL;
               CleanerData_t *acleaner = NULL;

               list_llreset(env->cleaners);
               while ((lnode = list_llnext(env->cleaners)) != NULL)
               {
                  acleaner = lnode->data;
                  (*(acleaner->cb))(acleaner->data);
                  list_llremove(env->cleaners, lnode);
                  list_llfreenode(&lnode);
               }
            }

            /* release shutdown lock */
            sem_post(gShutdownsem);
           
            doexit = 1;
         }
         else
         {
            /* release - shutdown has already been initiated */
            sem_post(gShutdownsem);
         }

         /* DoIt has been optionally cleaned up. */
         /* This will cause the SUMS thread to be killed, and the dbase to abort.  */
         if (doexit)
         {
            HastaLaVistaBaby(env, signo);
            return NULL; /* kill the signal thread - only thread left */
         }
      }
      else
      {
         HastaLaVistaBaby(env, signo);
         return NULL; /* kill the signal thread - only thread left */
      }
      
      break;      
    }
  }
}
