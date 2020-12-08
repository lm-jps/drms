/**
\file drms_network.h
*/
#ifndef __DRMS_NETWORK_H
#define __DRMS_NETWORK_H
#include "db.h"
#include "drms_types.h"
//#include "xmem.h"

#define kNOLOGSUDIR "NOLOGSUDIR"

/** \brief Establish socket connection to server, receive from server session information*/
DRMS_Session_t *drms_connect(const char *host);
/** \brief Establish DB connection, initialize session information */
DRMS_Session_t *drms_connect_direct(const char *host, const char *user,
				    const char *passwd, const char *dbname,
				    const char *sessionns);
#ifdef DRMS_CLIENT
/** \brief Server disconnects from DB. Client disconnects from server*/
void drms_disconnect(DRMS_Env_t *env, int abort);
/** \brief Client sends ::DRMS_DISCONNECT command code without echo*/
void drms_disconnect_now(DRMS_Env_t *env, int abort);
#endif
/** \brief Perform query and receive query results in DB_Binary_Result_t*/
DB_Binary_Result_t *drms_query_bin(DRMS_Session_t *session, const char *query);
/** \brief Same as drms_query_bin() with query parameters in variable argument list, calls drms_query_bin_array()*/
DB_Binary_Result_t *drms_query_binv(DRMS_Session_t *session, const char *query,
				    ...);
/** \brief Same as drms_query_bin() with query parameters*/
DB_Binary_Result_t *drms_query_bin_array(DRMS_Session_t *session, const char *query, int n_args, DB_Type_t *intype, void **argin);

DB_Binary_Result_t **drms_query_bin_ntuple(DRMS_Session_t *session, const char *stmnt, unsigned int nelems, unsigned int nargs, DB_Type_t *dbtypes, void **values);

/** \brief Perform query and receive query results in DB_Text_Result_t*/
DB_Text_Result_t *drms_query_txt(DRMS_Session_t *session, const char *query);
/** \brief Execute a data manipulation statement (DMS). */
int drms_dms(DRMS_Session_t *session, int *row_count, const char *query);

/** \brief Execute a data manipulation statement (DMS)  and do not print error messages. */
int drms_dms_quiet(DRMS_Session_t *session, int *row_count, const char *query);

/** \brief Same as drmm_dms() with DMS parameters*/
int drms_dms_array(DRMS_Session_t *session, int *row_count, const char *query, int n_rows, int n_args, DB_Type_t *intype, void **argin);
/** \brief Same as drms_dms() with DMS parameters in variable argument list, calls drms_dms_array()*/
int drms_dmsv(DRMS_Session_t *session, int *row_count, const char *query, int n_rows, ...);
/** \brief Bulk insert*/
int drms_bulk_insert_array(DRMS_Session_t *session, char *table, int n_rows, int n_args, DB_Type_t *intype, void **argin );
/** \brief Bulk insert with parameters in variable argument list, calls drms_bulk_insert_array() */
int drms_bulk_insertv(DRMS_Session_t *session, char *table, int n_rows, int n_cols, ...);

int drms_getsudir(DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retrieve);


/**
 Request one or more storage units from SUMS. This function is a client wrapper for the ::drms_su_getsudirs / :: drms_server_getsudirs
 functions - the semantics are identical for all three functions. Please see the documentation for ::drms_su_getsudirs for more information.

 @param env DRMS session information.
 @param su An array of ::DRMS_StorageUnit_t structs. Each struct contains \a sunum field that has the SUNUM of the storage unit being requested.
 @param num The number of structures in the array specified by the \a su parameter.
 @param retrieve If set to 1, then SUMS will retrieve offline storage units from tape ato disk.
 @param dontwait DEPRECATED - SUMS does not support dontwait == true, so this parameter is ignored.
 @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
 */
int drms_getsudirs(DRMS_Env_t *env, DRMS_StorageUnit_t **su, int num, int retrieve, int dontwait);

/** \brief Create a new series on-the-fly, using a series record prototype. */
int drms_create_series_fromprototype(DRMS_Record_t **prototype,
				     const char *outSeriesName,
				     int perms);

int drms_series_hastableprivs(DRMS_Env_t *env, const char *schema, const char *table, const char *priv);
int drms_series_cancreaterecord(DRMS_Env_t *env, const char *series);
int drms_series_candeleterecord(DRMS_Env_t *env, const char *series);
int drms_series_canupdaterecord(DRMS_Env_t *env, const char *series);
int drms_query_tabexists(DRMS_Session_t *session, const char *ns, const char *tab, int *status);
int drms_series_isdbowner(DRMS_Env_t *env, const char *series, int *status);
int drms_client_isproduser(DRMS_Env_t *env, int *status);
int drms_setretention(DRMS_Env_t *env, int16_t newRetention, int nsus, long long *sunums);
int drms_makewritable(DRMS_Env_t *env);

#ifdef DRMS_CLIENT
void drms_lock_client(DRMS_Env_t *env);
void drms_unlock_client(DRMS_Env_t *env);
int drms_trylock_client(DRMS_Env_t *env);
sem_t *drms_client_getsdsem(void);
void drms_client_initsdsem(void);
void drms_client_destroysdsem(void);
DRMS_Shutdown_State_t drms_client_getsd(void);
void drms_client_setsd(DRMS_Shutdown_State_t st);
int drms_client_registercleaner(DRMS_Env_t *env, CleanerData_t *data);
void *drms_signal_thread(void *arg);
#endif
/******************** DRMS client-server protocol stuff. ********************/

/* Command codes: Once a client has connected and properly authenticated itself
   with the server, the latter spawns a new thread to service client.
   The service thread executes a finite state machine that responds to a
   fixed set of commands that the client can send over its socket connection
   with the service thread. A command consists of a single 32 bit command
   code followed by zero or more arguments.

   DRMS_DISCONNECT:
     Command code:  1
     Arguments   :  1. abort flag, 32 bit integer.
     Reply       :  None (server terminates connection and shuts down)

   DRMS_COMMIT:
     Command code:  2
     Arguments   :  None
     Reply       :  1. status, 32 bit int

   DRMS_TXTQUERY
     Command code:  3
     Arguments   :  qlen, 32 bit int
                    query string, zero terminated string of length qlen
     Reply       :  1. DB_Text_Result_t structure sent by server by calling
                       db_send_text_query and received by client by calling
                       db_recv_text_query.

   DRMS_BINQUERY
     Command code:  4
     Arguments   :  qlen, 32 bit int
                    query string, zero terminated string of length qlen
     Reply       :  1. DB_Binary_Result_t structure sent by server by calling
                       db_send_binary_query and received by client by calling
                       db_recv_binary_query.

   DRMS_DMS
     Command code:  5
     Arguments   :  qlen, 32 bit int
                    query string, zero terminated string of length qlen
     Reply       :  1. status, 32 bit integer
                    2. num_rows, 32 bit integer


   DRMS_DMS_ARRAY
     Command code:  6
     Arguments   :  1. qlen, 32 bit int
                    2. query string, zero terminated string of length qlen
                    3. nrows, 32 bit int
                    4. nargs, 32 bit int
                    5. array argtype[nargs], 32 bit int
                    6+i, i=0...nargs-1:
                       if argtype[i] is scalar, values are sent as array
                       value[nrows] of type determined by argtype[i],
                       if argtype[i] is string, values will be sent as
                       single packed string array with strings separated
                       by (char) 0.

     Reply       :  1. status, 32 bit integer
                    2. num_rows, 32 bit integer


*/

#define DRMS_RESERVED              (0)
#define DRMS_DISCONNECT            (1)
#define DRMS_COMMIT                (2)
#define DRMS_TXTQUERY              (3)
#define DRMS_BINQUERY              (4)
#define DRMS_DMS                   (5)
#define DRMS_DMS_ARRAY             (6)
#define DRMS_SEQUENCE_DROP         (7)
#define DRMS_SEQUENCE_CREATE       (8)
#define DRMS_SEQUENCE_GETNEXT      (9)
#define DRMS_SEQUENCE_GETCURRENT   (10)
#define DRMS_BINQUERY_ARRAY        (11)
#define DRMS_NEWSLOTS              (12)
#define DRMS_GETUNIT               (13)
#define DRMS_ROLLBACK              (14)
#define DRMS_NEWSERIES             (15)
#define DRMS_DROPSERIES            (16)
#define DRMS_SLOT_SETSTATE         (17)
#define DRMS_BULK_INSERT_ARRAY     (18)
#define DRMS_SEQUENCE_GETLAST      (19)
#define DRMS_ALLOC_RECNUM          (20)
#define DRMS_GETTMPGUID            (21)
#define DRMS_GETUNITS              (22)
#define DRMS_GETSUDIR              (23)
#define DRMS_GETSUDIRS             (24)
#define DRMS_SITEINFO              (25)
#define DRMS_LOCALSITEINFO         (26)
#define DRMS_GETSUINFO             (27)
#define DRMS_GETDBUSER             (28)
#define DRMS_BINQUERY_NTUPLE       (29)
#define DRMS_SETRETENTION          (30)
#define DRMS_MAKESESSIONWRITABLE   (31)
#endif
