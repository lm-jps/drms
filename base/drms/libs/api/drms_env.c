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
  char query[DRMS_MAXQUERYLEN];
  DB_Text_Result_t *qres;
  char *p;

  /*  Storage Unit container  */
  hcon_init (&env->storageunit_cache, sizeof(HContainer_t), DRMS_MAXHASHKEYLEN,  
	    (void (*)(const void *)) hcon_free, 
	    (void (*)(const void *, const void *)) hcon_copy);

  /*  Query the database to get all series names from the master list  */
  sprintf(query, "select name from admin.ns");
  if ( (qres = drms_query_txt(env->session, query)) == NULL) goto bailout;
  p = query;
  p += sprintf(p, "(select seriesname from %s.drms_series) ", qres->field[0][0]);
  for (int i = 1; i < qres->num_rows; i++) {
    p += sprintf(p, "union\n\t(select seriesname from %s.drms_series) ", qres->field[i][0]);
  }
  db_free_text_result(qres);
  
  if ( (qres = drms_query_txt(env->session, query)) == NULL) goto bailout;

  /* Insert the series names in  container table with a null pointer.
     The series schemas will be retrieved from the database on demand.
     This way we only make as many queries as the number of series actually
     referenced by the module (probably a few). */

#ifdef DEBUG  
  printf("Building hashed container of series templates.\n");
#endif
  hcon_init (&env->series_cache, sizeof(DRMS_Record_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_template_record_struct, 
	    (void (*)(const void *, const void *)) drms_copy_record_struct);
  for (int i=0; i<qres->num_rows; i++) {
#ifdef DEBUG  
    printf("Inserting '%s'...\n",qres->field[i][0]);
#endif
    DRMS_Record_t *template = (DRMS_Record_t *)hcon_allocslot_lower(&env->series_cache, 
						     qres->field[i][0]);
    memset(template,0,sizeof(DRMS_Record_t));
    template->init = 0;
    //    strcpy(template->seriesinfo->seriesname, qres->field[i][0]);
  }
  db_free_text_result(qres);
#ifdef DEBUG  
  hcon_map (&env->series_cache, drms_printhcon);
#endif
  /*  Initialize a container for the record cache  */
  hcon_init (&env->record_cache, sizeof(DRMS_Record_t), DRMS_MAXHASHKEYLEN, 
	    (void (*)(const void *)) drms_free_record_struct, 
	    (void (*)(const void *, const void *)) drms_copy_record_struct);

  return 0;
 bailout:
  return 1;
  
}

DRMS_Env_t *drms_open (char *host, char *user, char *password, char *dbname,
    char *sessionns) {
     /*  NB: the parameters dbname & sessionns are only used if DRMS_CLIENT
							     is not defined  */
  DRMS_Env_t *env; 

  XASSERT( env = (DRMS_Env_t *)malloc(sizeof(DRMS_Env_t)) );
  memset(env, 0, sizeof(DRMS_Env_t));

#ifdef DRMS_CLIENT
						 /*  Connect to DRMS server  */
  if (host) {
     if ((env->session = drms_connect (host)) == NULL)
       goto bailout;
  }

  if (drms_cache_init (env)) goto bailout;
#else
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
  int status;

  if ((status = drms_closeall_records(env, DRMS_FREE_RECORD)))
    fprintf (stderr, "ERROR in drms_close: failed to close records in cache.\n");

					   /*  Close connection to database  */
  drms_disconnect (env, 1);
  drms_free_env (env, 1);
}

void drms_abort_now (DRMS_Env_t *env) {
  int status;

  if ((status = drms_closeall_records(env, DRMS_FREE_RECORD)))
    fprintf (stderr, "ERROR in drms_close: failed to close records in cache.\n");

					   /*  Close connection to database  */
  drms_disconnect_now (env, 1);
  drms_free_env (env, 1);
}
#endif

void drms_free_env (DRMS_Env_t *env, int final) {
  hcon_free (&env->record_cache);
  hcon_free (&env->series_cache);
  hcon_free (&env->storageunit_cache);
#ifndef DRMS_CLIENT
  free (env->drms_lock);
  if (env->sum_inbox) {
    tqueueDelete (env->sum_inbox);
  }
  if (env->sum_outbox) {
    tqueueDelete (env->sum_outbox);
  }
  if (env->shutdownsem)
  {
     sem_destroy(env->shutdownsem);
  }
#endif
  if (env->session) {
      free (env->session->sudir);
  }
  if (final) {
    if (env->session) {
      free (env->session->sessionns);  
      free (env->session);
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
