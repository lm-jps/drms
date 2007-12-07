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
/******************* Main server thread(s) functions ************************/

static DRMS_SumRequest_t *drms_process_sums_request(DRMS_Env_t  *env,
						    SUM_t *sum,
						    DRMS_SumRequest_t *request);

int drms_server_authenticate(int sockfd, DRMS_Env_t *env, int clientid)
{
  char *username, *sha1_passwd;
  char query[1024];
  int status=1;
  DB_Text_Result_t *qres;
  DB_Handle_t *db_handle = env->session->db_handle;

  /* Get username and password from client. */
  username = receive_string(sockfd);
  sha1_passwd = receive_string(sockfd);

  /* FIXME: Always authenticate for now. */
  status = 0;
  goto finish;

  /* Get password from database. */
  CHECKSNPRINTF(snprintf(query, 1024, 
			 "select sha1_passwd from drms_users where username='%s'",
			 username), 1024);
  qres = db_query_txt(db_handle, query);
  
  if ( qres != NULL )
  {
    if ( qres->num_rows==1 )
    {
      /*      printf("True password:\n");
	      db_print_text_result(qres); */
      status = strncmp(qres->field[0][0],sha1_passwd,2*SHA_DIGEST_LENGTH);
    }
    db_free_text_result(qres);
  }
 finish:
  /* Send status to client:
     status=1 means "access denied", status=0 means "access granted".  */
  free(username);
  free(sha1_passwd);
  if (!status)
  {
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
    net_packstring(env->session->sudir, t, v);
    Writevn(sockfd, vec, 8);
  }
  else
    Writeint(sockfd,status);

  return status;
}



/* Get a new DRMS session ID from the database and insert a session 
   record in the drms_session_table table. */
int drms_server_open_session(DRMS_Env_t *env, char *host, unsigned short port,
			     char *user, int dolog)
{
  int status;
  struct passwd *pwd = getpwuid(geteuid());

  /* Get sessionid etc. */
  char *sn = malloc(sizeof(char)*strlen(env->session->sessionns)+16);
  sprintf(sn, "%s.drms_sessionid", env->session->sessionns);
  env->session->sessionid = db_sequence_getnext(env->session->stat_conn, sn);
  free(sn);

  char stmt[DRMS_MAXQUERYLEN];
  snprintf(stmt, DRMS_MAXQUERYLEN, "INSERT INTO %s."DRMS_SESSION_TABLE 
	  "(sessionid, hostname, port, pid, username, starttime, sunum, "
	  "sudir, status, clients,lastcontact,sums_thread_status,jsoc_version) VALUES "
	  "(?,?,?,?,?,LOCALTIMESTAMP(0),?,?,'idle',0,LOCALTIMESTAMP(0),"
	  "'starting', '%s(%d)')", env->session->sessionns, jsoc_version, jsoc_vers_num);

  if (dolog) {
    /* Allocate a 1MB storage unit for log files. */
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

  if (db_dmsv(env->session->stat_conn, NULL, stmt,
	      -1, DB_INT8, env->session->sessionid, 
	      DB_STRING, host, DB_INT4, (int) port, DB_INT4, (int) getpid(), 
	      DB_STRING, pwd->pw_name, DB_INT8, env->session->sunum, 
	      DB_STRING,env->session->sudir))
    {
      fprintf(stderr,"Error: Couldn't register session.\n");
      return 1;
    }
  } else {
    if (db_dmsv(env->session->stat_conn, NULL, stmt,
		-1, DB_INT8, env->session->sessionid,
		DB_STRING, host, DB_INT4, (int) port, DB_INT4, (int) getpid(),
		DB_STRING, pwd->pw_name, DB_INT8, 0,
		DB_STRING, "No log"))
      {
	fprintf(stderr,"Error: Couldn't register session.\n");
	return 1;
      }
  }

  return 0;
}

/* Update the record corresponding to the session associated with env
   in the session table. */
int drms_server_session_status(DRMS_Env_t *env, char *stat_str, int clients)
{
  /* Set sessionid and insert an entry in the session table */
  char stmt[1024];
  sprintf(stmt, "UPDATE %s."DRMS_SESSION_TABLE
	  " SET status=?,clients=?,lastcontact=LOCALTIMESTAMP(0) WHERE "
	  "sessionid=?", env->session->sessionns);
  if (db_dmsv(env->session->stat_conn, NULL, stmt, -1, 
              DB_STRING, stat_str, DB_INT4, clients, 
	      DB_INT8, env->session->sessionid))
  {
    fprintf(stderr,"Error: Couldn't update session entry.\n");
    return 1;
  }
  return 0;
}

      
/* Update record corresponding to the session associated with env
   from the session table and close the extra database connection. */
int drms_server_close_session(DRMS_Env_t *env, char *stat_str, int clients,
			      int log_retention, int archive_log)
{
  DRMS_StorageUnit_t su;
  char *command;
  DIR *dp;
  struct dirent *dirp;
  int emptydir = 1;

  /* Flush output to logfile. */
  fclose(stderr);
  fclose(stdout);
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
    XASSERT(command = malloc(strlen(env->session->sudir)+20));
    while ((dirp = readdir(dp)) != NULL) {
      if (!strcmp(dirp->d_name, ".") ||
	  !strcmp(dirp->d_name, "..")) 
	continue;

      /* Gzip drms_server log files. */
      /* The module log files are now handled with gz* functions. */
      /* Only the server logs are not compressed. */
      if (strncmp(dirp->d_name, ".gz", 3)) {
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
  /*
  sprintf(stmt, "DELETE FROM %s."DRMS_SESSION_TABLE 
          " WHERE sessionid=?", env->session->sessionns);
  if (db_dmsv(env->session->stat_conn, NULL, stmt, -1, 
              DB_INT8, env->session->sessionid))
  {
    fprintf(stderr,"Error: Couldn't unregister session.\n");
    return 1;
  }
  */
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
void drms_server_abort(DRMS_Env_t *env)
{
  DRMS_SumRequest_t request;

  drms_lock_server(env);
  if (env->verbose)
    fprintf(stderr,"WARNING: DRMS is aborting...\n");  
  /* Roll back. */
  db_rollback(env->session->db_handle);

  /* Unregister */
  drms_server_close_session(env, abortstring, env->clientcounter, 7, 0);

  // Either a sums thread or a server thread might be waiting on
  // either sum_inbox or sum_outbox. Need to put something into the
  // respective queue to release the mutex and conditional variables
  // in order to destroy them in drms_free_env().  Given the
  // head-of-line blocking queue design, there is at most one
  // outstanding request.
  if (env->sum_thread) {
    // a server thread might be waiting for reply on sum_outbox, tell
    // it we are aborting.
    if (env->sum_tag) {
      // has to be dynamically allocated since it's going to be freed.
      DRMS_SumRequest_t *reply;
      XASSERT(reply = malloc(sizeof(DRMS_SumRequest_t)));      
      reply->opcode = DRMS_ERROR_ABORT;
      tqueueAdd(env->sum_outbox, env->sum_tag, (char *)reply);
    } else {
      // a sums thread might be waiting for requests on sum_inbox, tell
      // it we are aborting.
      /* tell the sums thread to finish up. */
      // looks like all requests are not dyanmically allocated.
      DRMS_SumRequest_t request;
      request.opcode = DRMS_SUMCLOSE;
      tqueueAdd(env->sum_inbox, (long)pthread_self(), (char *)&request);
    }
  }

  db_disconnect(env->session->stat_conn);
 
  /* Close DB connection and set abort flag... */
  db_abort(env->session->db_handle);

  /* Wait for other threads to finish cleanly. */
  //if (env->verbose) 
    fprintf(stderr, "WARNING: DRMS server aborting in %d seconds...\n", 
	    DRMS_ABORT_SLEEP);
  sleep(DRMS_ABORT_SLEEP);

  /* Free memory.*/
  drms_free_env(env);

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
void drms_server_commit(DRMS_Env_t *env)
{
  int log_retention, archive_log;
  DRMS_SumRequest_t request;

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
  log_retention = drms_commit_all_units(env, &archive_log);  

  /* Unregister session */
  if (env->verbose)
    printf("Unregistering DRMS session.\n");
  drms_server_close_session(env, "finished", 0, log_retention, archive_log);

  if (env->sum_thread) {
    /* Tell SUM thread to finish up. */
    request.opcode = DRMS_SUMCLOSE;
    tqueueAdd(env->sum_inbox, (long) pthread_self(), (char *) &request);
    /* Wait for SUM service thread to finish. */
    pthread_join(env->sum_thread, NULL);
  }

  db_disconnect(env->session->stat_conn);

  /* Commit all changes to the DRMS database. */
  db_commit(env->session->db_handle);

  /* Close DB connection and set abort flag in case any threads 
     are still active... */
  db_abort(env->session->db_handle);

  /* Give threads a small window to finish cleanly. */
  if (env->server_wait) {
    if (env->verbose)
      fprintf(stderr, "WARNING: DRMS server stopping in approximately %d "
	      "seconds...\n", DRMS_ABORT_SLEEP);
    sleep(DRMS_ABORT_SLEEP);
  }

  /* Free memory.*/
  drms_free_env(env);  

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
  int command,status,disconnect,clients=0;
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
    Exit(1);
  }

  
  if ( getpeername(sockfd,  &client, &client_size) == -1 )
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
      drms_lock_server(env);
      status = db_rollback(db_handle);
      db_start_transaction(db_handle);
      drms_unlock_server(env);
      Writeint(sockfd, status);
      break;
    case DRMS_COMMIT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_COMMIT.\n", tnum);
      drms_lock_server(env);
      status = db_commit(db_handle);
      db_start_transaction(db_handle);
      drms_unlock_server(env);
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
      drms_lock_server(env);
      status = drms_server_newslots(env, sockfd);
      drms_unlock_server(env);
      break;
    case  DRMS_SLOT_SETSTATE:
      if (env->verbose)
	printf("thread %d: Executing DRMS_SLOT_SETSTATE.\n",tnum);
      drms_lock_server(env);
      status = drms_server_slot_setstate(env, sockfd);
      drms_unlock_server(env);
      break;
    case  DRMS_GETUNIT:
      if (env->verbose)
	printf("thread %d: Executing DRMS_GETUNIT.\n",tnum);
      drms_lock_server(env);
      status = drms_server_getunit(env, sockfd);
      drms_unlock_server(env);
      break;
    case DRMS_NEWSERIES:
      if (env->verbose)
	printf("thread %d: Executing DRMS_NEWSERIES.\n",tnum);
      drms_lock_server(env);
      status = drms_server_newseries(env, sockfd);
      drms_unlock_server(env);
      break;
    case DRMS_DROPSERIES:
      if (env->verbose)
	printf("thread %d: Executing DRMS_DROPSERIES.\n",tnum);
      drms_lock_server(env);
      status = drms_server_dropseries(env, sockfd);
      drms_unlock_server(env);
      break;
    case DRMS_GETTMPGUID:
      drms_lock_server(env);
      drms_server_gettmpguid(&sockfd);
      drms_unlock_server(env);
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


  if (noshare)
  {
    /* If in noshare mode, commit or rollback, and start a new transaction. */
    drms_lock_server(env);
    clients = --(env->clientcounter);
    if (env->clientcounter==0)
      drms_server_session_status(env, "idle", env->clientcounter);
    else
      drms_server_session_status(env, "running", env->clientcounter);
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
      --(env->clientcounter);
      if (env->clientcounter==0)
	drms_server_session_status(env, "idle", env->clientcounter);
      else
	drms_server_session_status(env, "running", env->clientcounter);
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
      Exit(1);
    }
  }


  printf("thread %d: Exiting with disconnect = %d and status = %d.\n",
	 tnum,disconnect,status);
 bail:
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

  status = DRMS_SUCCESS;
  series = receive_string(sockfd);  
  n = Readint(sockfd);  
  if (n>0)
  {    
    lifetime = (DRMS_RecLifetime_t) Readint(sockfd); 
  
    XASSERT(su = malloc(n*sizeof(DRMS_StorageUnit_t *)));
    XASSERT(slotnum = malloc(n*sizeof(int)));
    XASSERT(recnum = malloc(n*sizeof(long long)));
    for (i=0; i<n; i++)
      recnum[i] = Readlonglong(sockfd);  
      

    status = drms_su_newslots(env, n, series, recnum, lifetime, slotnum, su);
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


/* Server stub for drms_su_freeslot and drms_su_markstate. */
int drms_server_alloc_recnum(DRMS_Env_t *env, int sockfd)
{ 
  int status, n, i;
  char *series;
  DRMS_RecLifetime_t lifetime;
  long long *recnums;
  DS_node_t *ds=(DS_node_t *) NULL; /*ISS - #21*/
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
      for (i=0; i<n; i++)
	ds->recnums[(ds->n)++] = recnums[i];
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
  template = (DRMS_Record_t *)hcon_allocslot_lower(&env->series_cache, series);
  template->init = 0;
  XASSERT(template->seriesinfo = (DRMS_SeriesInfo_t *)malloc(sizeof(DRMS_SeriesInfo_t)) );
  strcpy(template->seriesinfo->seriesname, series);
  free(series);
  return 0;
}


/* A client destroyed a series. Remove its entry in the
   server series_cache */
int drms_server_dropseries(DRMS_Env_t *env, int sockfd)
{ 
  char *series_lower;

  series_lower = receive_string(sockfd);
  strtolower(series_lower);
  hcon_remove(&env->series_cache, series_lower);
  free(series_lower);
  return 0;
}

void drms_lock_server(DRMS_Env_t *env)
{
  if ( env->drms_lock == NULL )
    return;
  else
    pthread_mutex_lock( env->drms_lock );
}

void drms_unlock_server(DRMS_Env_t *env)
{
  if ( env->drms_lock == NULL )
    return;
  else
    pthread_mutex_unlock( env->drms_lock );
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

  env = (DRMS_Env_t *) arg;

  /* Block signals. */
  if( (status = pthread_sigmask(SIG_BLOCK, &env->signal_mask, NULL)))
  {
    fprintf(stderr,"pthread_sigmask call failed with status = %d\n", status);
    Exit(1);
  }

#ifdef DEBUG
  printf("drms_sums_thread started.\n");
  fflush(stdout);
#endif

  /* Main processing loop. */
  stop = 0;
  empty = 0;
  while ( !stop || (stop && !empty))
  {
    /* Set status in session table */
    if (connected)
    {
      char stmt[1024];
      sprintf(stmt, "UPDATE %s."DRMS_SESSION_TABLE
	      " SET sums_thread_status='waiting for work',"
	      "lastcontact=LOCALTIMESTAMP(0) WHERE sessionid=?", env->session->sessionns);
      if (db_dmsv(env->session->stat_conn, NULL, stmt,
		  -1, DB_INT8, env->session->sessionid))
	fprintf(stderr,"Warning in sums_thread: Couldn't update session entry.\n");
    }
    /* Wait for the next SUMS request to arrive in the inbox. */
    env->sum_tag = 0;
    empty = tqueueDelAny(env->sum_inbox, &env->sum_tag,  &ptmp );
    request = (DRMS_SumRequest_t *) ptmp;

    if (!connected && request->opcode!=DRMS_SUMCLOSE)
    {
      /* Connect to SUMS. */
      if ((sum = SUM_open(NULL, NULL, printf)) == NULL)
      {
	fprintf(stderr,"drms_open: Failed to connect to SUMS.\n");
	fflush(stdout);
	XASSERT(reply = malloc(sizeof(DRMS_SumRequest_t)));
	reply->opcode = DRMS_ERROR_SUMOPEN;
	/* Give the service thread a chance to deliver the bad news to
	   the client. */
	tqueueAdd(env->sum_outbox, env->sum_tag, (char *) reply);
	sleep(1);
	Exit(1); /* Take down the DRMS server. */
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
      /* Put the reply in the outbox. */
      tqueueAdd(env->sum_outbox, env->sum_tag, (char *) reply);
      env->sum_tag = 0; // done processing
    }
  }

  if (connected)
  {
    /* Disconnect from SUMS. */
    SUM_close(sum,printf);
  }
  return NULL;
}




static DRMS_SumRequest_t *drms_process_sums_request(DRMS_Env_t  *env,
						    SUM_t *sum,
						    DRMS_SumRequest_t *request)
{
  int i;
  DRMS_SumRequest_t *reply;
  
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
      /* Set status in session table */
      char stmt[1024];
      sprintf(stmt, "UPDATE %s." DRMS_SESSION_TABLE
	      " SET sums_thread_status='waiting for SUMS to stage data',"
	      "lastcontact=LOCALTIMESTAMP(0) WHERE sessionid=?", env->session->sessionns);
      if (db_dmsv(env->session->stat_conn, NULL, stmt,
		  -1, DB_INT8, env->session->sessionid))
	fprintf(stderr,"Error in sums_thread: Couldn't update session entry.\n");
      /* FIXME: For now we just wait for SUMS. */
      reply->opcode = SUM_wait(sum);
      if (reply->opcode || sum->status)
      {
	fprintf(stderr,"SUM thread: SUM_wait call failed with "
		"error code = %d, sum->status = %d.\n",
		reply->opcode,sum->status);
	reply->opcode = DRMS_ERROR_SUMWAIT;
	break;
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
      reply->sudir[i] = strdup(sum->wd[i]);
      free(sum->wd[i]);
#ifdef DEBUG
      printf("SUM thread: got sudir[%d] = %s = %s\n",i,reply->sudir[i],sum->wd[i]);
#endif
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
  default:
    fprintf(stderr,"SUM thread: Invalid command code (%d) in request.\n",
	   request->opcode);
    reply->opcode = DRMS_ERROR_SUMBADOPCODE;
    break;
  }
  return reply;
}





/****************** Server signal handler thread functions *******************/
void *drms_signal_thread(void *arg)
{
  int status, signo;
  DRMS_Env_t *env = (DRMS_Env_t *) arg;

#ifdef DEBUG
  printf("drms_signal_thread started.\n");
  fflush(stdout);
#endif

  /* Block signals. */
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
      Exit(1);
      break; 
    case SIGUSR1:
      drms_server_commit(env);
      fflush(stdout);
      _exit(0);
    }
  }
}
