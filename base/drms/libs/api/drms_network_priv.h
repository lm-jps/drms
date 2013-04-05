/**
\file drms_network_priv.h
*/
#ifndef __DRMS_NETWORK_PRIV_H
#define __DRMS_NETWORK_PRIV_H

/* Client side functions. */
#define drms_freeslot(env, series, sunum, slotnum) drms_slot_setstate((env), (series),(sunum),(slotnum),DRMS_SLOT_FREE)

/** \brief Send command code via socket */
int drms_send_commandcode(int sockfd, int command);
/** \brief Send command code via socket. The form does not expect echo from the server */
int drms_send_commandcode_noecho(int sockfd, int command);

/** \brief Tell DB to commit transaction 
\warning This function should not be used unless you know what you are doing.
*/
int drms_commit(DRMS_Env_t *env);
/** \brief Tell DB to rollback transaction
\warning This function should not be used unless you know what you are doing.
*/
int drms_rollback(DRMS_Session_t *session);

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
		  DRMS_StorageUnit_t **su,
                  int createslotdirs);
int drms_newslots_nosums(DRMS_Env_t *env, int n, char *series, long long *recnum,
                         DRMS_RecLifetime_t lifetime, int *slotnum, 
                         DRMS_StorageUnit_t **su,
                         int createslotdirs);
/** \brief Retrieve the storage unit specified by \a sunum */
DRMS_StorageUnit_t *drms_getunit(DRMS_Env_t *env,  char *series, 
				 long long sunum, int retrieve, int *status);
DRMS_StorageUnit_t *drms_getunit_nosums(DRMS_Env_t *env,  char *series, 
                                        long long sunum, int *status);
/** 
 Request one or more storage units from SUMS. This function is a client wrapper for ::drms_su_getsudirs / ::drms_server_getunits functions - 
 the semantics are identical for all three functions, with the exception that this function 
 creates the ::DRMS_StorageUnit_t structures needed by ::drms_su_getsudirs / ::drms_server_getunit. The SUNUMs specified
 must belong to a single series, identified by \a series.
 Please see the documentation for ::drms_su_getsudirs for more information.
 
 @param env DRMS session information.
 @param series The DRMS series that contains the storage units to retrieve.
 @param n The number of SUNUMs in the array specified by the \a sunum parameter.
 @param sunum An array of SUNUMs that identify the storage units being requested from series \a series.
 @param retrieve If set to 1, then SUMS will retrieve offline storage units from tape to disk.
 @param dontwait DEPRECATED - SUMS does not support dontwait == true, so this parameter is ignored.
 @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
 */
int drms_getunits(DRMS_Env_t *env, 
                  char *series, 
                  int n, 
                  long long *sunum, 
                  int retrieve, 
                  int dontwait);


/** 
 Request one or more storage units from SUMS. This function is a client wrapper for ::drms_su_getsudirs / ::drms_server_getunits functions - 
 the semantics are identical for all three functions, with the exception that this function 
 creates the ::DRMS_StorageUnit_t structures needed by ::drms_su_getsudirs / ::drms_server_getunit. 
 This function is very similar to ::drms_getunits, except that it supports the specification of SUNUMs from
 more than one series, but ::drms_getunits does not.
 Please see the documentation for ::drms_su_getsudirs for more information.
 
 @param env DRMS session information.
 @param num The number of structures in the array specified by the \a suandseries parameter.
 @param suandseries An array of ::DRMS_SuAndSeries_t structures. Each structure contains a series name and an array of SUNUMs. This array specifies the storage units that are being requested and their containing series.
 @param retrieve If set to 1, then SUMS will retrieve offline storage units from tape to disk.
 @param dontwait DEPRECATED - SUMS does not support dontwait == true, so this parameter is ignored.
 @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
 */
int drms_getunits_ex(DRMS_Env_t *env, 
                     int num, 
                     DRMS_SuAndSeries_t *suandseries, 
                     int retrieve,
                     int dontwait);

int drms_getsuinfo(DRMS_Env_t *env, long long *sunums, int nReqs, SUM_info_t **infostructs);

/** \brief Request \c n recnum */
long long *drms_alloc_recnum(DRMS_Env_t *env,  char *series, 
			     DRMS_RecLifetime_t lifetime, int n);
/** \brief Mark a storage unit slot as either ::DRMS_SLOT_FREE, ::DRMS_SLOT_FULL, or ::DRMS_SLOT_TEMP. */
int drms_slot_setstate(DRMS_Env_t *env, char *series, long long sunum, 
		       int slotnum, int state);

/** @brief Delete the storage units associated with a series */
int drms_dropseries(DRMS_Env_t *env, const char *series, DRMS_Array_t *vec);

/** \brief Create a series */
int drms_create_series(DRMS_Record_t *rec, int perms);
/** \brief Update an existing series */
int drms_update_series(DRMS_Record_t *rec, int perms);

#endif
