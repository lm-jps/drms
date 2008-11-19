/**
\file drms_storageunit.h
*/
#ifndef __DRMS_STORRAGEUNIT_H
#define __DRMS_STORRAGEUNIT_H

#include "drms_server.h"
#include "tagfifo.h"


#define DRMS_SUNUM_FORMAT "%020lld"
#define DRMS_SLOTDIR_FORMAT "S%05d"

/* Slot states. */
#define DRMS_SLOT_FREE 0
#define DRMS_SLOT_FULL 1
#define DRMS_SLOT_TEMP 2

long long drms_su_alloc(DRMS_Env_t *env, uint64_t size, char **dir, int *status);
int drms_su_newslots(DRMS_Env_t *env, int n, char *series, long long *recnum,
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
int drms_commit_all_units(DRMS_Env_t *env, int *archive);
void drms_su_freeunit(DRMS_StorageUnit_t *su);
int drms_su_getsudir(DRMS_Env_t *env, DRMS_StorageUnit_t *su, int retireve);
int drms_su_getsudirs(DRMS_Env_t *env, int n, DRMS_StorageUnit_t **su, int retireve, int dontwait);
void drms_delete_temporaries(DRMS_Env_t *env);

#endif
