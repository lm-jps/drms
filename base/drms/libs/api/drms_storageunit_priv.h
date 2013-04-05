/**
\file drms_storageunit_priv.h
*/
#ifndef __DRMS_STORRAGEUNIT_PRIV_H
#define __DRMS_STORRAGEUNIT_PRIV_H

#include "drms_server.h"
#include "tagfifo.h"


#define DRMS_SUNUM_FORMAT "%020lld"
#define DRMS_SLOTDIR_FORMAT "S%05d"

/* Slot states. */
#define DRMS_SLOT_FREE 0
#define DRMS_SLOT_FULL 1
#define DRMS_SLOT_TEMP 2

long long drms_su_alloc(DRMS_Env_t *env, uint64_t size, char **dir, int *tapegroup, int *status);
int drms_su_alloc2(DRMS_Env_t *env, 
                   uint64_t size, 
                   long long sunum, 
                   char **sudir, 
                   int *tapegroup,
                   int *status);
int drms_su_newslots(DRMS_Env_t *env, int n, char *series, long long *recnum,
		     DRMS_RecLifetime_t lifetime, int *slotnum, 
		     DRMS_StorageUnit_t **su,
                     int createslotdirs);
int drms_su_newslots_nosums(DRMS_Env_t *env, int n, char *series, long long *recnum,
                            DRMS_RecLifetime_t lifetime, int *slotnum, 
                            DRMS_StorageUnit_t **su,
                            int createslotdirs);
int drms_su_freeslot(DRMS_Env_t *env, char *series, long long sunum,
		     int slotnum);
DRMS_StorageUnit_t *drms_su_lookup(DRMS_Env_t *env, char *series,
				   long long sunum, HContainer_t **scon_out);
DRMS_StorageUnit_t *drms_su_markslot(DRMS_Env_t *env, char *series, long long sunum,
				     int slotnum, int *state);


void drms_freeunit(DRMS_Env_t *env, DRMS_StorageUnit_t *su);
int drms_commitunit(DRMS_Env_t *env, DRMS_StorageUnit_t *su);
int drms_commit_all_units(DRMS_Env_t *env, int *archive, int *status);
void drms_su_freeunit(DRMS_StorageUnit_t *su);
int drms_su_getsudir(DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retireve);

/**
 Request from SUMS one or more storage units, identified by the SUNUMs provided in 
 the array of DRMS_StorageUnit_t structures passed in the \a su argument. If the 
 storage units are online (on disk), then information about the storage units
 is written to the DRMS_StorageUnit_t structures, including the path to the 
 storage unit. If the storage units are offline (on tape, but not on disk), then
 no information is added to the DRMS_StorageUnit_t structures, except that 
 DRMS_StorageUnit_t::sudir is set to the empty string, unless the retrieve
 flag is set to 1. In that case, SUMS will bring the storage units online, and 
 then it will add information to the DRMS_StorageUnit_t structures. In all cases, 
 if a provided SUNUM is invalid, SUMS will set the corresponding DRMS_StorageUnit_t::sudir field 
 to the empty string. 
  
 @param env DRMS session information.
 @param n The number of structures in the array specified by the \a su parameter.
 @param su An array of ::DRMS_StorageUnit_t structs. Each struct contains \a sunum field that has the SUNUM of the storage unit being requested. 
 @param retrieve If set to 1, then SUMS will retrieve offline storage units from tape ato disk.
 @param dontwait DEPRECATED - SUMS does not support dontwait == true, so this parameter is ignored.
 @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
 */

int drms_su_getsudirs(DRMS_Env_t *env, int n, DRMS_StorageUnit_t **su, int retrieve, int dontwait);
int drms_su_getinfo(DRMS_Env_t *env, long long *sunums, int nReqs, SUM_info_t **info);
void drms_delete_temporaries(DRMS_Env_t *env);
int drms_su_sumexport(DRMS_Env_t *env, SUMEXP_t *);
#endif
