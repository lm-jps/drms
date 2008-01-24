/**
\file drms_env.h
*/
#ifndef _DRMS_ENV_H
#define _DRMS_ENV_H


#include "drms_types.h"

/************* Macros ********************/


/************* Constants *********************************/

/* Master table holding the list of all DRMS series. */
#define DRMS_MASTER_SERIES_TABLE "drms_series"
/* The master tables for the keywords, links and data segment definitions. */
#define DRMS_MASTER_KEYWORD_TABLE "drms_keyword"
#define DRMS_MASTER_LINK_TABLE "drms_link"
#define DRMS_MASTER_SEGMENT_TABLE "drms_segment"
#define DRMS_SESSION_TABLE "drms_session"



/******************************* Functions **********************************/

/************** DRMS Environment ****************/

/* - Open authenticated data base connection.
   - Retrieve series schemas and link tables.
   - Build series templates and hastable over seriesnames, 
   - Initialize record cache and hash table. */
DRMS_Env_t *drms_open(char *host, char *user, char *password, char *dbname, char *sessionns);
			    

/* - If commit==1 then commit all modified records to the database.
   - Close database connection and free DRMS data structures. */
int drms_close(DRMS_Env_t *env, int commit); 
void drms_abort(DRMS_Env_t *env);
#ifdef DRMS_CLIENT
void drms_abort_now(DRMS_Env_t *env);
#endif
void drms_free_env(DRMS_Env_t *env, int final);
long long drms_su_size(DRMS_Env_t *env, char *series);

/* Helper function for visualizing the contents of a hash table. */
void drms_printhash(const void *key, const void *value);

/** \brief Initialize drms caches: segment, series, and record*/
int drms_cache_init(DRMS_Env_t *env);


#endif
