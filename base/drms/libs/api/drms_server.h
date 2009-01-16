/**
\file drms_server.h
*/
#ifndef __DRMS_SERVER_H
#define __DRMS_SERVER_H

#include "drms_types.h"


/* Variable and macro used to remember the place exit was called so 
   it can be inserted into the status field of the drms_session table 
   in the DRMS database. */
#ifdef __DRMS_SERVER_C
char abortstring[1024];
#else
extern char abortstring[1024];
#endif

/* exit() causes atexit() function to be executed */
#define Exit(code) do { snprintf(abortstring,1023,"aborted: exit called in file %s, line %d",__FILE__,__LINE__); exit((code)); } while(0)


/* Values used for the storage unit containing the session log file. */
#define DRMS_LOG_DSNAME     "drms_logs"
//#define DRMS_LOG_RETENTION  (2592000)
#define DRMS_LOG_RETENTION  (10)
#define DRMS_LOG_TAPEGROUP  1

#define WEEK_SECONDS (60*60*24*7)



#define DRMS_ABORT_SLEEP (2)

/******************************** Prototypes *************************/
/**\brief drms server thread */
void *drms_server_thread(void *arg);
/**\brief drms sum thread */
void *drms_sums_thread(void *arg);
/**\brief drms signal thread */
void *drms_signal_thread(void *arg);

/** \brief Send session information to the client. */
int drms_server_authenticate(int sockfd, DRMS_Env_t *env, int clientid);
/** \brief Server function for command code ::DRMS_NEWSLOTS. */
int drms_server_newslots(DRMS_Env_t *env, int sockfd);
/** \brief Server function for command code ::DRMS_GETUNIT. */
int drms_server_getunit(DRMS_Env_t *env, int sockfd);
/** \brief Server function for command code ::DRMS_GETUNITS. */
int drms_server_getunits(DRMS_Env_t *env, int sockfd);
/** \brief Server function for command code ::DRMS_NEWSERIES. */
int drms_server_newseries(DRMS_Env_t *env, int sockfd);
/** \brief Server function for command code ::DRMS_DROPSERIES. */
int drms_server_dropseries(DRMS_Env_t *env, int sockfd);
/**
\brief asks SUMS to mark all SUs listed in \a tn as delete pending.

\param env
\param tn Name of the db table that stores all SUs to be marked
\return DRMS_SUCCESS if SUMS is able to mark all SUs listed in \a tn. Other
*/
int drms_server_dropseries_su(DRMS_Env_t *env, char *tn);
long long drms_server_gettmpguid(int *sockfd);

int drms_server_getsudir(DRMS_Env_t *env, int sockfd);

/** \brief Server function for command code ::DRMS_ALLOC_RECNUM. */
int drms_server_alloc_recnum(DRMS_Env_t *env, int sockfd);
/** \brief Add recnums of transient records to templist for future removal */
void drms_server_transient_records(DRMS_Env_t *env, char *series, int n, long long *recnums);
/** \brief Close DRMS session */
int drms_server_close_session(DRMS_Env_t *env, char *stat_str, int clients, 
			      int log_retention, int archive_log);
/** \brief Open DRMS session */
int drms_server_open_session(DRMS_Env_t *env);
/** \brief Update DRMS session information in the drms_session table */
int drms_server_session_status(DRMS_Env_t *env, char *stat_str, int clients);
/** \brief Abort DRMS */
void drms_server_abort(DRMS_Env_t *env, int final);
/** \brief Commit DRMS */
void drms_server_commit(DRMS_Env_t *env, int final);
/** \brief Server function for command code ::DRMS_SLOT_SETSTATE. */
int drms_server_slot_setstate(DRMS_Env_t *env, int sockfd);
/** \brief Lock DRMS env */
void drms_lock_server(DRMS_Env_t *env);
/** \brief Unlock DRMS env */
void drms_unlock_server(DRMS_Env_t *env);
/**
Begin a new transaction. This function can only be called in direct-connect mode.
@param env 
*/
int drms_server_begin_transaction(DRMS_Env_t *env);
/**
End a transaction. This function can only be called in direct-connect mode. All DRMS resources are released except for the signal thread and db data connection.
@param env 
@param abort If 1, the current transaction will
rollback. Otherwise the current transaction will commit. 
@param final If 1, the db data connection is released. No
future drms_server_begin_transaction() can be invoked. 
*/
void drms_server_end_transaction(DRMS_Env_t *env, int abort, int final);
#endif
