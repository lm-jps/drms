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

#define Exit(code) do { snprintf(abortstring,1023,"aborted: exit called in file %s, line %d",__FILE__,__LINE__); exit((code)); } while(0)


/* Values used for the storage unit containing the session log file. */
#define DRMS_LOG_DSNAME     "drms_logs"
#define DRMS_LOG_TAPEGROUP  1
#define DRMS_LOG_RETENTION  (2592000)
#define WEEK_SECONDS (60*60*24*7)



#define DRMS_ABORT_SLEEP (2)

/******************************** Prototypes *************************/
void *drms_server_thread(void *arg);
void *drms_sums_thread(void *arg);
void *drms_signal_thread(void *arg);

int drms_server_authenticate(int sockfd, DRMS_Env_t *env, int clientid);
int drms_server_newslots(DRMS_Env_t *env, int sockfd);
int drms_server_getunit(DRMS_Env_t *env, int sockfd);
int drms_server_getunits(DRMS_Env_t *env, int sockfd);
int drms_server_newseries(DRMS_Env_t *env, int sockfd);
int drms_server_dropseries(DRMS_Env_t *env, int sockfd);
long long drms_server_gettmpguid(int *sockfd);

int drms_server_alloc_recnum(DRMS_Env_t *env, int sockfd);
int drms_server_close_session(DRMS_Env_t *env, char *stat_str, int clients, 
			      int log_retention, int archive_log);
int drms_server_open_session(DRMS_Env_t *env, char *host, unsigned short port,
			     char *user, int dolog);
int drms_server_session_status(DRMS_Env_t *env, char *stat_str, int clients);
void drms_server_abort(DRMS_Env_t *env);
void drms_server_commit(DRMS_Env_t *env);
int drms_server_slot_setstate(DRMS_Env_t *env, int sockfd);
void drms_lock_server(DRMS_Env_t *env);
void drms_unlock_server(DRMS_Env_t *env);

#endif
