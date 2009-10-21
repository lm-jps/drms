//#define DEBUG
#include "drms.h"
#include "drms_priv.h"
#include "SUM.h"
#include "sum_rpc.h"
#include "xmem.h"

/*
static void drms_printhcon (const void *ptr) {
  DRMS_Record_t *template;
  template = (DRMS_Record_t *)ptr;
  printf ("Series name = %s, init = %d.\n", template->seriesinfo->seriesname, 
	 template->init);
}
*/

int drms_cache_init(DRMS_Env_t *env) {
  /*  Storage Unit container  */
  hcon_init (&env->storageunit_cache, sizeof(HContainer_t), DRMS_MAXHASHKEYLEN,  
	    (void (*)(const void *)) hcon_free, 
	    (void (*)(const void *, const void *)) hcon_copy);

  /* No longer cache all the series names in the series_cache during module initialization. Do 
   * this on-demand when drms_template_record() is called. */
#ifdef DEBUG  
  printf("Building hashed container of series templates.\n");
#endif

  hcon_init (&env->series_cache, sizeof(DRMS_Record_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_template_record_struct, 
	    (void (*)(const void *, const void *)) drms_copy_record_struct);

  /*  Initialize a container for the record cache  */
  hcon_init (&env->record_cache, sizeof(DRMS_Record_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_record_struct, 
	    (void (*)(const void *, const void *)) drms_copy_record_struct);

  return 0;
}

DRMS_Env_t *drms_open (char *host, char *user, char *password, char *dbname,
    char *sessionns) {
     /*  NB: the parameters dbname & sessionns are only used if DRMS_CLIENT
							     is not defined  */
  DRMS_Env_t *env; 

  XASSERT( env = (DRMS_Env_t *)malloc(sizeof(DRMS_Env_t)) );
  memset(env, 0, sizeof(DRMS_Env_t));

#ifdef DRMS_CLIENT
  drms_client_initsdsem();
						 /*  Connect to DRMS server  */
  if (host) {
     if ((env->session = drms_connect (host)) == NULL)
       goto bailout;
  }

  if (drms_cache_init (env)) goto bailout;

  /* In client, no drms_server_begin_transaction() to initialize drms_lock. */
  XASSERT(env->drms_lock = malloc(sizeof(pthread_mutex_t)));
  pthread_mutex_init(env->drms_lock, NULL); 
#else
  drms_server_initsdsem();
					  /*  This is a server initializing  */
  if (host) {
    if ((env->session = drms_connect_direct (host, user,
	password, dbname, sessionns)) == NULL) goto bailout;
  }
    // sum_inbox sum_outbox, drms_lock, and caches (seg, series,
    // record)  will be initialized in
    // drms_server_begin_transaction().

  env->templist = NULL;
  env->retention = -1;
  env->query_mem = 512;
  env->verbose = 0;
#endif
  return env;

bailout:
  drms_free_env(env, 1);
  return NULL;
}

#ifdef DRMS_CLIENT
/* - If action=DRMS_INSERT_RECORD then insert all modified records 
      in the record cache into the database. If action=DRMS_FREE_RECORD
      then free all records without committing them.
   - Close DRMS connection and free DRMS data structures. */
int drms_close (DRMS_Env_t *env, int action) {
  int status;

  if ((status = drms_closeall_records(env,action)))
    fprintf (stderr, "ERROR in drms_close: failed to close records in cache.\n");
					   /*  Close connection to database  */
  drms_disconnect (env, 0);
  drms_free_env (env, 1);
  return status;
}

void drms_abort (DRMS_Env_t *env) {
  if (drms_closeall_records(env, DRMS_FREE_RECORD))
    fprintf (stderr, "ERROR in drms_close: failed to close records in cache.\n");

					   /*  Close connection to database  */
  drms_disconnect (env, 1);
  drms_free_env (env, 1);
}

void drms_abort_now (DRMS_Env_t *env) {
   drms_lock_client(env);
   if (drms_closeall_records(env, DRMS_FREE_RECORD))
   {
     fprintf (stderr, "ERROR in drms_close: failed to close records in cache.\n");
   }
					   /*  Close connection to database  */
  drms_disconnect_now (env, 1);
  drms_unlock_client(env);

  /* XXX Need to hold the lock until env->drms_lock is set to NULL */
  drms_free_env (env, 1);
}
#endif

void drms_free_env (DRMS_Env_t *env, int final) {
   /* Initialized by drms_server_begin_transaction() (in servers), 
    * or drms_open() (in clients) */
  hcon_free (&env->record_cache);
  hcon_free (&env->series_cache);
  hcon_free (&env->storageunit_cache);

  /* drms_lock in both server and client */
  pthread_mutex_destroy(env->drms_lock);
  free (env->drms_lock);
  env->drms_lock = NULL;
#ifndef DRMS_CLIENT
  /* Alloc'd by drms_server_begin_transaction() (server only) */

  pthread_mutex_destroy(env->clientlock);
  free (env->clientlock);
  env->clientlock = NULL;

  /* Alloc'd by drms_server_begin_transaction() (server only) */
  if (env->sum_inbox) {
    tqueueDelete (env->sum_inbox);
    env->sum_inbox = NULL;
  }
  /* Alloc'd by drms_server_begin_transaction() (server only) */
  if (env->sum_outbox) {
    tqueueDelete (env->sum_outbox);
    env->sum_outbox = NULL;
  }

  /* The environment transaction is no longer initialized. */
  env->transinit = 0;

#endif
  if (env->session) {
     /* Alloc'd by drms_server_open_session() (in servers), 
      * or drms_open() (in clients). drms_server_open_session()
      * is called by drms_server_begin_transaction(). */
     free (env->session->sudir);
     env->session->sudir = NULL;
  }

  if (final) {
    if (env->session) {
       /* Alloc'd by drms_open() */
       free (env->session->sessionns); 
       env->session->sessionns = NULL; 

       /* Alloc'd by drms_open() */
       free (env->session);
       env->session = NULL;
    }
    free (env);
  }

  /* tell DRMS we're terminating */
  drms_term();
}

			/*  estimate the size of a storage unit from series  */
long long drms_su_size (DRMS_Env_t *env, char *series) {
  int status;
  DRMS_Record_t *rec;

  rec = drms_template_record (env, series, &status);
  if (rec == NULL) {
    fprintf (stderr, "ERROR: Couldn't get template for series '%s'.\n"
	    "drms_template_record returned status=%d\n",
	    series, status);
    return -1;
  }
  return rec->seriesinfo->unitsize * drms_record_size (rec);
}
