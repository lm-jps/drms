#ifndef __DRMS_NETWORK_H
#define __DRMS_NETWORK_H
#include "db.h"
#include "drms_types.h"
//#include "xmem.h"




/* Client side functions. */
#define drms_freeslot(env, series, sunum, slotnum) drms_slot_setstate((env), (series),(sunum),(slotnum),DRMS_SLOT_FREE)





int drms_send_commandcode(int sockfd, int command);
DRMS_Session_t *drms_connect(char *host, unsigned short port, 
			     char *user, char *passwd);
DRMS_Session_t *drms_connect_direct(char *host, char *user, 
				    char *passwd, char *dbname,
				    char *sessionns);
int drms_commit(DRMS_Env_t *env);
int drms_rollback(DRMS_Session_t *session);
void drms_disconnect(DRMS_Env_t *env, int abort);
DB_Binary_Result_t *drms_query_bin(DRMS_Session_t *session, char *query);
DB_Binary_Result_t *drms_query_binv(DRMS_Session_t *session, char *query,
				    ...);
DB_Binary_Result_t *drms_query_bin_array(DRMS_Session_t *session, 
					  char *query,
					 int n_args, DB_Type_t *intype, 
					 void **argin);
DB_Text_Result_t *drms_query_txt(DRMS_Session_t *session, char *query);
int drms_dms(DRMS_Session_t *session, int *row_count, char *query);
int drms_dms_array(DRMS_Session_t *session, int *row_count, 
		    char *query, int n_rows, int n_args, 
		   DB_Type_t *intype, void **argin);
int drms_dmsv(DRMS_Session_t *session, int *row_count,  char *query, 
	      int n_rows, ...);
int drms_bulk_insert_array(DRMS_Session_t *session,
			   char *table, int n_rows, int n_args, 
			   DB_Type_t *intype, void **argin );
int drms_bulk_insertv(DRMS_Session_t *session, char *table, 
		      int n_rows, int n_cols, ...);

/* Sequence functions */
long long *drms_sequence_getnext(DRMS_Session_t *session,  char *name, int n);
long long drms_sequence_getcurrent(DRMS_Session_t *session,  char *name);
long long drms_sequence_getlast(DRMS_Session_t *session,  char *table);
int drms_sequence_create(DRMS_Session_t *session,  char *name);
int drms_sequence_drop(DRMS_Session_t *session,  char *name);
int drms_newslots(DRMS_Env_t *env,  int n, char *series, long long *recnum, 
		  DRMS_RecLifetime_t lifetime, int *slotnum, 
		  DRMS_StorageUnit_t **su);
DRMS_StorageUnit_t *drms_getunit(DRMS_Env_t *env,  char *series, 
				 long long sunum, int retrieve, int *status);
long long *drms_alloc_recnum(DRMS_Session_t *session,  char *series, 
			     DRMS_RecLifetime_t lifetime, int n);
int drms_slot_setstate(DRMS_Env_t *env, char *series, long long sunum, 
		       int slotnum, int state);
int drms_create_series(DRMS_Record_t *rec, int perms);
int drms_update_series(DRMS_Record_t *rec, int perms);
int drms_delete_series(DRMS_Env_t *env, char *series, int cascade);

/* Create a new series on-the-fly, using a series record prototype. */
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
#endif
