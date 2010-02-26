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

sem_t *gShutdownsem = NULL; /* synchronization among signal thread, main thread, 
                               sums thread, and server threads during shutdown */
DRMS_Shutdown_State_t gShutdown; /* shudown state following receipt by DRMS of a signal that 
                                 * causes module termination */

pthread_mutex_t *gSUMSbusyMtx = NULL;
int gSUMSbusy = 0;

/******************* Main server thread(s) functions ************************/

static DRMS_SumRequest_t *drms_process_sums_request(DRMS_Env_t  *env,
						    SUM_t *sum,
						    DRMS_SumRequest_t *request);

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
     XASSERT( env->drms_lock = malloc(sizeof(pthread_mutex_t)) );
     pthread_mutex_init (env->drms_lock, NULL); 

     /* to serialize server threads (drms_server clients) access to drms_server's DRMS library */
     XASSERT(env->clientlock = malloc(sizeof(pthread_mutex_t)) );
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
  if (env->dolog) {
     /* Allocate a 1MB storage unit for log files. */
     /* drms_su_alloc() can be slow when the dbase is busy */
     env->session->sunum = drms_su_alloc(env, 1<<20, &env->session->sudir, 
                                         &status);
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
    XASSERT(su.seriesinfo = malloc(sizeof(DRMS_SeriesInfo_t)));
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
    if (drms_commitunit(env, &su))
      fprintf(stderr,"Error: Couldn't commit log storage unit to SUMS.\n");
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
     XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));

    if (env->sum_tag) 
    {
      // has to be dynamically allocated since it's going to be freed.
       /* This code simply tells the thread that made an unprocessed
        * SUMS request that there was an error (abort is happening). */
      DRMS_SumRequest_t *reply;
      XASSERT(reply = malloc(sizeof(DRMS_SumRequest_t)));      
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

  log_retention = drms_commit_all_units(env, &archive_log, &status);  

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
    XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));
    request->opcode = DRMS_SUMCLOSE;
    tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *)request);
    /* Wait for SUM service thread to finish. */
    pthread_join(env->sum_thread, NULL);
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

  status = DRMS_SUCCESS;
  series = receive_string(sockfd);  

  n = Readint(sockfd);  
  if (n>0)
  {    
    lifetime = (DRMS_RecLifetime_t) Readint(sockfd); 
    createslotdirs = Readint(sockfd);
  
    XASSERT(su = malloc(n*sizeof(DRMS_StorageUnit_t *)));
    XASSERT(slotnum = malloc(n*sizeof(int)));
    XASSERT(recnum = malloc(n*sizeof(long long)));
    for (i=0; i<n; i++)
      recnum[i] = Readlonglong(sockfd);  
      
#if defined(DEBUG)
    fprintf(stdout, "series = '%s'\nn = '%d'\nlifetime = '%d'\ncreateslotdirs = '%d'\n", 
            series, n, lifetime, createslotdirs);
#endif

    status = drms_su_newslots(env, n, series, recnum, lifetime, slotnum, su, createslotdirs);
    if (status==DRMS_SUCCESS)
    {
      int *tmp, *t;
      long long *ltmp, *lt;
      struct iovec *vec, *v;
      

      XASSERT(tmp = malloc((2*n+1)*sizeof(int)));
      XASSERT(ltmp = malloc((n+1)*sizeof(long long)));
      XASSERT(vec = malloc((4*n+1)*sizeof(struct iovec)));
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

  char *series;
  int retrieve, dontwait;
  DRMS_StorageUnit_t *su;

  series = receive_string(sockfd);
  n = Readint(sockfd);

  long long *sunum;
  XASSERT(sunum = malloc(n*sizeof(long long)));
  for (int i = 0; i < n; i++) {
    sunum[i] = Readlonglong(sockfd);
  }

  retrieve = Readint(sockfd);
  dontwait = Readint(sockfd);

  status = drms_getunits(env, series, n, sunum, retrieve, dontwait);
  Writeint(sockfd,status);
  if (status==DRMS_SUCCESS)  {
    if (!dontwait) {
      int *len;
      struct iovec *vec;

      XASSERT(len = malloc(n*sizeof(int)));
      XASSERT(vec = malloc(2*n*sizeof(struct iovec)));
      for (int i = 0; i < n; i++) {
	HContainer_t *scon = NULL;
	su = drms_su_lookup(env, series, sunum[i], &scon);
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

  free(series);
  free(sunum);
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
      XASSERT(env->templist = ds = malloc(sizeof(DS_node_t)));
      ds->series = strdup(series);
      ds->nmax = 512;
      ds->n = 0;
      XASSERT(ds->recnums = malloc(ds->nmax*sizeof(long long)));
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
		  XASSERT(ds->next = malloc(sizeof(DS_node_t)));
		  ds = ds->next;
		  ds->series = strdup(series);
		  ds->nmax = 512;
		  ds->n = 0;
		  XASSERT(ds->recnums = malloc(ds->nmax*sizeof(long long)));
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
      XASSERT(ds->recnums = realloc(ds->recnums, 
				    ds->nmax*sizeof(long long)));
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
  DRMS_SumRequest_t *request = NULL;
  DRMS_SumRequest_t *reply = NULL;
  XASSERT(request = malloc(sizeof(DRMS_SumRequest_t)));
  memset(request, 0, sizeof(DRMS_SumRequest_t));
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

     sunum = drms_su_alloc(env, 1048576, &sudir, &drmsstatus);

     if (!drmsstatus && sunum >= 0 && sudir)
     {
        /* make a file that has nothing but SUNUMs */
        char fbuf[PATH_MAX];
        snprintf(fbuf, sizeof(fbuf), "%s/%s", sudir, kDELSERFILE);
        FILE *fptr = fopen(fbuf, "w");

        if (fptr)
        {
           /* Insert SUNUMs in the table now - iterate through DRMS_Array_t */
           int irow;
           long long *data = (long long *)array->data;
           char obuf[64];

           /* only one column*/
           for (irow = 0; irow < array->axis[1]; irow++)
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

              XASSERT(putreq = malloc(sizeof(DRMS_SumRequest_t)));
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

                 snprintf(sunumstr, sizeof(sunumstr), "%lld", sunum);
                 request->opcode = DRMS_SUMDELETESERIES;
                 /* provide path to data file and series name to SUMS */
                 snprintf(commentbuf, sizeof(commentbuf), "%s,%s", fbuf, tn);
                 request->comment = commentbuf; 
 
                 tqueueAdd(env->sum_inbox, (long)pthread_self(), (char *)request);
                 tqueueDel(env->sum_outbox, (long)pthread_self(), (char **)&reply);

                 if (reply->opcode) 
                 {
                    fprintf(stderr, 
                            "SUM_delete_series() returned with error code '%d'.\n", 
                            reply->opcode);
                    status = DRMS_ERROR_SUMDELETESERIES;
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
  }
  else
  {
     status = DRMS_ERROR_INVALIDDATA;
     fprintf(stderr, "Unexpected array passed to drms_server_dropseries_su().\n");
  }

  /* No need to deep-free reply since SUMS shouldn't have malloc'd any fields. */
  if (reply)
  {
     free(reply);
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
      XASSERT(command = malloc(strlen(ds->series) + 40 + 21*ds->n));
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

#ifdef DEBUG
  printf("drms_sums_thread started.\n");
  fflush(stdout);
#endif
  
  if (!gSUMSbusyMtx)
  {
     XASSERT(gSUMSbusyMtx = malloc(sizeof(pthread_mutex_t)));
     pthread_mutex_init(gSUMSbusyMtx, NULL); 
  }

  /* Main processing loop. */
  stop = 0;
  empty = 0;
  while ( !stop || (stop && !empty))
  {
    /* Wait for the next SUMS request to arrive in the inbox. */
     /* sum_tag is the thread id of the thread who made the original SUMS request */
    env->sum_tag = 0;
    empty = tqueueDelAny(env->sum_inbox, &env->sum_tag,  &ptmp );
    request = (DRMS_SumRequest_t *) ptmp;

    if (!connected && request->opcode!=DRMS_SUMCLOSE)
    {
      /* Connect to SUMS. */
       if (env->verbose)
       {
          timer = CreateTimer();
       }

       /* When the rate of SUM_open() calls gets large, SUM_open() starts to fall behind. */ 
      if ((sum = SUM_open(NULL, NULL, printkerr)) == NULL)
      {
	fprintf(stderr,"drms_open: Failed to connect to SUMS.\n");
	fflush(stdout);
	XASSERT(reply = malloc(sizeof(DRMS_SumRequest_t)));
	reply->opcode = DRMS_ERROR_SUMOPEN;
	/* Give the service thread a chance to deliver the bad news to
	   the client. */
	tqueueAdd(env->sum_outbox, env->sum_tag, (char *) reply);
	sleep(1);
	//Exit(1); /* Take down the DRMS server. */
        pthread_kill(env->signal_thread, SIGTERM);
      }

      if (env->verbose && timer)
      {
         fprintf(stdout, "to call SUM_open: %f seconds.\n", GetElapsedTime(timer));
         DestroyTimer(&timer);
      }

      connected = 1;
#ifdef DEBUG
      printf("drms_sums_thread connected to SUMS. SUMID = %llu\n",sum->uid);
      fflush(stdout);
#endif
    }

    /* Check for special CLOSE or ABORT codes. */
    if (request->opcode==DRMS_SUMCLOSE)
    {
      stop = 1;
      empty = tqueueCork(env->sum_inbox); /* Do not accept any more requests, but keep
				     processing all the requests already in
				     the queue.*/
    }
    else if ( request->opcode==DRMS_SUMABORT )
    {
      break;
    }
    else /* A regular request. */
    {
      /* Send the request to SUMS. */
      reply = drms_process_sums_request(env, sum, request);

      if (reply)
      {
         if (!request->dontwait) {
            /* Put the reply in the outbox. */
            tqueueAdd(env->sum_outbox, env->sum_tag, (char *) reply);
         } else {
            /* If the calling thread waits for the reply, then it is the caller's responsibility 
             * to clean up. Otherwise, clean up here. */
            for (int i = 0; i < request->reqcnt; i++) {
               free(reply->sudir[i]);
            }
            free(reply);
         }
      }
      env->sum_tag = 0; // done processing
    }

    /* Note: request is only shallow-freed. The is the requestor's responsiblity 
     * to free any memory allocated for the dsname, comment, and sudir fields. */
    free(request);
  }

  if (connected && sum)
  {
    /* Disconnect from SUMS. */
    SUM_close(sum,printf);
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

static DRMS_SumRequest_t *drms_process_sums_request(DRMS_Env_t  *env,
						    SUM_t *sum,
						    DRMS_SumRequest_t *request)
{
  int i;
  DRMS_SumRequest_t *reply = NULL;
  int shuttingdown = 0;
  sem_t *sdsem = drms_server_getsdsem();
  
  if (!sum)
  {
     fprintf(stderr , "Error in drms_process_sums_request(): No SUMS connection.\n");
     return NULL;
  }

  XASSERT(reply = malloc(sizeof(DRMS_SumRequest_t)));
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
    sum->reqcnt = 1;
    sum->bytes = request->bytes;
    /* Make RPC call to the SUM server. */
    /* PERFORMANCE BOTTLENECK */
    /* drms_su_alloc() can be slow when the dbase is busy - this is due to 
     * SUM_open() calls backing up. */
    if ((reply->opcode = SUM_alloc(sum, printf)))
    {
      fprintf(stderr,"SUM thread: SUM_alloc RPC call failed with "
	      "error code %d\n",reply->opcode);
      break;
    }

    reply->sunum[0] = sum->dsix_ptr[0];
    reply->sudir[0] = strdup(sum->wd[0]);
    free(sum->wd[0]);
#ifdef DEBUG
  printf("SUM_alloc returned sunum=%llu, sudir=%s.\n",reply->sunum[0],
	 reply->sudir[0]);
#endif
    break;

  case DRMS_SUMGET:
#ifdef DEBUG
    printf("Processing SUMGET request.\n");
#endif
    if (request->reqcnt<1 || request->reqcnt>DRMS_MAX_REQCNT)
    {
      fprintf(stderr,"SUM thread: Invalid reqcnt (%d) in SUMGET request.\n",
	     request->reqcnt);
      reply->opcode = DRMS_ERROR_SUMGET;
      break;
    }
    sum->reqcnt = request->reqcnt;
    sum->mode = request->mode;
    sum->tdays = request->tdays;
    for (i=0; i<request->reqcnt; i++)
      sum->dsix_ptr[i] = request->sunum[i];

#ifdef DEBUG
    printf("SUM thread: calling SUM_get\n");
#endif
   
    /* Make RPC call to the SUM server. */
    reply->opcode = SUM_get(sum, printf);

#ifdef DEBUG
    printf("SUM thread: SUM_get returned %d\n",reply->opcode);
#endif

    if (reply->opcode == RESULT_PEND)
    {
       /* This SUM_wait() call can take a while. If DRMS is shutting down, 
        * then don't wait. Should be okay to get shut down sem since 
        * the main and signal threads don't hold onto them for too long. */

       if (sdsem)
       {
          sem_wait(sdsem);
          shuttingdown = (drms_server_getsd() != kSHUTDOWN_UNINITIATED);
          sem_post(sdsem);
       }
       
       if (!shuttingdown)
       {
          if (gSUMSbusyMtx)
          {
             pthread_mutex_lock(gSUMSbusyMtx);
             gSUMSbusy = 1;
             pthread_mutex_unlock(gSUMSbusyMtx);
          }

          /* FIXME: For now we just wait for SUMS. */
          reply->opcode = SUM_wait(sum);

          if (gSUMSbusyMtx)
          {
             pthread_mutex_lock(gSUMSbusyMtx);
             gSUMSbusy = 0;
             pthread_mutex_unlock(gSUMSbusyMtx);
          }

          if (reply->opcode || sum->status)
          {
             fprintf(stderr,"SUM thread: SUM_wait call failed with "
                     "error code = %d, sum->status = %d.\n",
                     reply->opcode,sum->status);
             reply->opcode = DRMS_ERROR_SUMWAIT;
             break;
          }
       }
       else
       {
          reply->opcode = 0;
       }
    }
    else if (reply->opcode != 0)
    {
      fprintf(stderr,"SUM thread: SUM_get RPC call failed with "
	      "error code %d\n",reply->opcode);
      break;
    }
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
    break;

  case DRMS_SUMPUT:
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
    }
    /* Make RPC call to the SUM server. */
    if ((reply->opcode = SUM_put(sum, printf)))
    {
      fprintf(stderr,"SUM thread: SUM_put call failed with stat=%d.\n",
	      reply->opcode);
      break;
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

             if ((reply->opcode = SUM_delete_series(fpath, series, printf)) != 0)
             {
                fprintf(stderr,"SUM thread: SUM_delete_series call failed with stat=%d.\n",
                        reply->opcode);
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
    sum->reqcnt = 1;
    sum->bytes = request->bytes;

    /* Make RPC call to the SUM server. */
    if ((reply->opcode = SUM_alloc2(sum, request->sunum[0], printf)))
    {
       fprintf(stderr,"SUM thread: SUM_alloc2 RPC call failed with "
               "error code %d\n", reply->opcode);
       break;
    }

    reply->sudir[0] = strdup(sum->wd[0]);
    free(sum->wd[0]); 
    break;
  case DRMS_SUMEXPORT:
    {
       /* cast the comment field of the request - 
        * it contains the SUMEXP_t struct */
       SUMEXP_t *sumexpt = (SUMEXP_t *)request->comment;

       /* fill in the uid */
       sumexpt->uid = sum->uid;

       /* Make RPC call to the SUM server. */
       if ((reply->opcode = SUM_export(sumexpt, SUMExptErr)))
       {
          fprintf(stderr,"SUM thread: SUM_export RPC call failed with "
                  "error code %d\n", reply->opcode);
          break;
       }
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
int drms_server_registercleaner(DRMS_Env_t *env, pFn_Cleaner_t cb, CleanerData_t *data)
{
   int gotlock = 0;
   static int registered = 0;

   if (!registered)
   {
      gotlock = (drms_trylock_server(env) == 0);

      if (gotlock)
      {
         env->cleaner = cb;
         if (data)
         {
            env->cleanerdata = *data;
         }
         else
         {
            env->cleanerdata.data = NULL;
            env->cleanerdata.deepclean = NULL;
            env->cleanerdata.deepdata = NULL;
         }

         registered = 1;
         drms_unlock_server(env);
      }
      else
      {
         fprintf(stderr, "Can't register doit cleaner function. Unable to obtain mutex.\n");
      }
   }
   else
   {
      fprintf(stderr, "drms_server_registercleaner() already successfully called - cannot re-register.\n");
   }

   return gotlock;
}

static void HastaLaVistaBaby(DRMS_Env_t *env, int signo)
{
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

   /* Calls drms_server_commit() or drms_server_abort().
    * Don't set last argment to final - still need environment below. */
   /* This will cause the SUMS thread to return. */
   /* If no transaction actually started, then noop. */
   drms_server_end_transaction(env, signo != SIGUSR1, 0);

   /* Don't wait for main thread to terminate, it may be in the middle of a long DoIt(). But 
    * main can't start using env and the database and cmdparams after we clean them up. */

   /* Must disconnect db, because we didn't set the final flag in drms_server_end_transaction() */
   db_disconnect(&env->session->db_handle);
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
            if (env->cleaner)
            {
               /* Clean up deep data first */
               if (env->cleanerdata.deepclean)
               {
                  (*(env->cleanerdata.deepclean))(env->cleanerdata.deepdata);
               }

               (*(env->cleaner))(env->cleanerdata.data);
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
