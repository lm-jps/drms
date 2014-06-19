/**
\file drms_storageunit.h
*/
#ifndef __DRMS_STORRAGEUNIT_H
#define __DRMS_STORRAGEUNIT_H

#ifdef DRMS_DEFAULT_RETENTION
    #define STDRETENTION DRMS_DEFAULT_RETENTION
#else
    #define STDRETENTION (-3)
#endif

/* remotesums APIs */
int drms_su_allocsu(DRMS_Env_t *env, 
                    uint64_t size, 
                    long long sunum, 
                    char **sudir, 
                    int *tapegroup,
                    int *status);
int drms_su_commitsu(DRMS_Env_t *env, 
                     const char *seriesname, 
                     long long sunum,
                     const char *sudir);
int drms_su_isremotesu(long long sunum);
int drms_su_getexportURL(DRMS_Env_t *env, long long sunum, char *url, int size);
int drms_su_getexportserver(DRMS_Env_t *env, 
                            long long sunum, 
                            char *expserver, 
                            int size);
int drms_su_setretention(DRMS_Env_t *env, int16_t newRetention, int nsus, long long *sunums);

#endif
