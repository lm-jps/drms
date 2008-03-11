/**
\file drms_env.h
*/
#ifndef _DRMS_ENV_H
#define _DRMS_ENV_H


#include "drms_types.h"

/************* Macros ********************/


/************* Constants *********************************/

/** \brief DRMS master table for series. */
#define DRMS_MASTER_SERIES_TABLE "drms_series"
/** \brief DRMS master table for the keywords */
#define DRMS_MASTER_KEYWORD_TABLE "drms_keyword"
/** \brief DRMS master table for links */
#define DRMS_MASTER_LINK_TABLE "drms_link"
/** \brief DRMS master table for segments */
#define DRMS_MASTER_SEGMENT_TABLE "drms_segment"
/** \brief DRMS session table */
#define DRMS_SESSION_TABLE "drms_session"



/******************************* Functions **********************************/

/************** DRMS Environment ****************/

/** \brief Wrapper function to connect server to database or
     client to server via a socket. 

 - Server to database connection calls drms_connect_direct().
 - Client to server connection calls drms_connect(). Caches on the
   client are initialized using drms_cache_init(). 

\param host if it is of the form <hostname>:<portno>, then connect to
     a DRMS server. If it is of the form <hostname> 
     the connect directly to a database server 
\param dbname db name
\param user db user name
\param password db user password 

\return a pointer to DRMS_Env_t upon success, NULL otherwise.
*/ 
/* - Open authenticated data base connection.
   - Retrieve series schemas and link tables.
   - Build series templates and hastable over seriesnames, 
   - Initialize record cache and hash table. */
DRMS_Env_t *drms_open(char *host, char *user, char *password, char *dbname, char *sessionns);
			    

/* - If commit==1 then commit all modified records to the database.
   - Close database connection and free DRMS data structures. */
/** \brief Wrapper function to close DRMS

 - Close all records in record cache drms_closeall_records()
 - Close DRMS connection drms_disconnect()
 - Free \a env drms_free_env()

\param env handle for connection to either db or socket
\param action either ::DRMS_INSERT_RECORD or ::DRMS_FREE_RECORD. This
value is passed into drms_closeall_records()
\return status of drms_closeall_records()
*/
int drms_close(DRMS_Env_t *env, int action); 

/** \brief Abort DRMS

Runs the same closing sequence as drms_close() except that
drms_closeall_records() is called with ::DRMS_FREE_RECORD and
drms_disconnect() is called with abort flag set to 1.

\param env handle for connection to either db or socket
\return status of drms_closeall_records()
*/
void drms_abort(DRMS_Env_t *env);
#ifdef DRMS_CLIENT
/** \brief Abort DRMS

Runs the same closing sequence as drms_abort() except calling
drms_disconnect_now() instead of drms_disconnect().

\param env handle for connection to either db or socket
\return none
*/
void drms_abort_now(DRMS_Env_t *env);
#endif
void drms_free_env(DRMS_Env_t *env, int final);

/** \brief Return an estimate size of a storage unit for a given
    series in bytes 

\param env for connection to either db or socket
\param series
\return estimate size of SU in bytes
*/
long long drms_su_size(DRMS_Env_t *env, char *series);

/** \brief Initialize drms caches: SU, series, and record

 - SU cache:
   -# hash container initialized
 - series cache: 
   -# hash container initialized
   -# an uninitalized template for every series in DB. The series name
      is used to locate a slot in the hash container 
 - record cache: 
   -# hash container initialized

\param env handle for connection to either db or socket
\return 0 upon success, 1 otherwise.
*/
int drms_cache_init(DRMS_Env_t *env);


#endif
