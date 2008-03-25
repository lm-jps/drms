/**
\file drms_network.h
*/
#ifndef __DRMS_NETWORK_H
#define __DRMS_NETWORK_H
#include "db.h"
#include "drms_types.h"
//#include "xmem.h"




/* Client side functions. */
#define drms_freeslot(env, series, sunum, slotnum) drms_slot_setstate((env), (series),(sunum),(slotnum),DRMS_SLOT_FREE)




/** \brief Send command code via socket */
int drms_send_commandcode(int sockfd, int command);
/** \brief Send command code via socket. The form does not expect echo from the server */
int drms_send_commandcode_noecho(int sockfd, int command);
/** \brief Establish socket connection to server, receive from server session information*/
DRMS_Session_t *drms_connect(char *host, unsigned short port);
/** \brief Establish DB connection, initialize session information */
DRMS_Session_t *drms_connect_direct(char *host, char *user, 
				    char *passwd, char *dbname,
				    char *sessionns);
/** \brief Tell DB to commit transaction 
\warning This function should not be used unless you know what you are doing.
*/
int drms_commit(DRMS_Env_t *env);
/** \brief Tell DB to rollback transaction
\warning This function should not be used unless you know what you are doing.
*/
int drms_rollback(DRMS_Session_t *session);
/** \brief Server disconnects from DB. Client disconnects from server*/
void drms_disconnect(DRMS_Env_t *env, int abort);
/** \brief Client sends ::DRMS_DISCONNECT command code without echo*/
void drms_disconnect_now(DRMS_Env_t *env, int abort);
/** \brief Perform query and receive query results in DB_Binary_Result_t*/
DB_Binary_Result_t *drms_query_bin(DRMS_Session_t *session, char *query);
/** \brief Same as drms_query_bin() with query parameters in variable argument list, calls drms_query_bin_array()*/
DB_Binary_Result_t *drms_query_binv(DRMS_Session_t *session, char *query,
				    ...);
/** \brief Same as drms_query_bin() with query parameters*/
DB_Binary_Result_t *drms_query_bin_array(DRMS_Session_t *session, 
					  char *query,
					 int n_args, DB_Type_t *intype, 
					 void **argin);
/** \brief Perform query and receive query results in DB_Text_Result_t*/
DB_Text_Result_t *drms_query_txt(DRMS_Session_t *session, char *query);
/** \brief Execute a data manipulation statement (DMS). */
int drms_dms(DRMS_Session_t *session, int *row_count, char *query);
/** \brief Same as drmm_dms() with DMS parameters*/
int drms_dms_array(DRMS_Session_t *session, int *row_count, 
		    char *query, int n_rows, int n_args, 
		   DB_Type_t *intype, void **argin);
/** \brief Same as drms_dms() with DMS parameters in variable argument list, calls drms_dms_array()*/
int drms_dmsv(DRMS_Session_t *session, int *row_count,  char *query, 
	      int n_rows, ...);
/** \brief Bulk insert*/
int drms_bulk_insert_array(DRMS_Session_t *session,
			   char *table, int n_rows, int n_args, 
			   DB_Type_t *intype, void **argin );
/** \brief Bulk insert with parameters in variable argument list, calls drms_bulk_insert_array() */
int drms_bulk_insertv(DRMS_Session_t *session, char *table, 
		      int n_rows, int n_cols, ...);

/* Sequence functions */
/** \brief Get next n sequence values */
long long *drms_sequence_getnext(DRMS_Session_t *session,  char *name, int n);
/** \brief Get the current sequence value */
long long drms_sequence_getcurrent(DRMS_Session_t *session,  char *name);
/** \brief Get the last sequence value */
long long drms_sequence_getlast(DRMS_Session_t *session,  char *table);
/** \brief Create sequence */
int drms_sequence_create(DRMS_Session_t *session,  char *name);
/** \brief Drop sequence */
int drms_sequence_drop(DRMS_Session_t *session,  char *name);
/** \brief Create new slots */
int drms_newslots(DRMS_Env_t *env,  int n, char *series, long long *recnum, 
		  DRMS_RecLifetime_t lifetime, int *slotnum, 
		  DRMS_StorageUnit_t **su);
/** \brief Retrieve the storage unit specified by \a sunum */
DRMS_StorageUnit_t *drms_getunit(DRMS_Env_t *env,  char *series, 
				 long long sunum, int retrieve, int *status);
/** \brief Retrieve the storage units specified by \a sunum */
int drms_getunits(DRMS_Env_t *env,  char *series, 
		  int n, long long *sunum, int retrieve, int dontwait);
/** \brief Request \c n recnum */
long long *drms_alloc_recnum(DRMS_Session_t *session,  char *series, 
			     DRMS_RecLifetime_t lifetime, int n);
/** \brief Mark a storage unit slot as either ::DRMS_SLOT_FREE, ::DRMS_SLOT_FULL, or ::DRMS_SLOT_TEMP. */
int drms_slot_setstate(DRMS_Env_t *env, char *series, long long sunum, 
		       int slotnum, int state);
/** \brief Create a series */
int drms_create_series(DRMS_Record_t *rec, int perms);
/** \brief Update an existing series */
int drms_update_series(DRMS_Record_t *rec, int perms);
/** \brief Delete an existing series */
int drms_delete_series(DRMS_Env_t *env, char *series, int cascade);

/** \brief Create a new series on-the-fly, using a series record prototype. */
int drms_create_series_fromprototype(DRMS_Record_t **prototype, 
				     const char *outSeriesName, 
				     int perms);


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
#endif
