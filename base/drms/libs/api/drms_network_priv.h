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

/** \brief Retrieve the storage unit specified by \a sunum */
DRMS_StorageUnit_t *drms_getunit(DRMS_Env_t *env,  char *series, 
				 long long sunum, int retrieve, int *status);
/** \brief Retrieve the storage units specified by \a sunum */
int drms_getunits(DRMS_Env_t *env,  char *series, 
		  int n, long long *sunum, int retrieve, int dontwait);

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
