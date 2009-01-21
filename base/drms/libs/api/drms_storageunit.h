/**
\file drms_storageunit.h
*/
#ifndef __DRMS_STORRAGEUNIT_H
#define __DRMS_STORRAGEUNIT_H

/* remotesums APIs */
int drms_su_allocsu(DRMS_Env_t *env, 
                    uint64_t size, 
                    long long sunum, 
                    char **sudir, 
                    int *status);
int drms_su_commitsu(DRMS_Env_t *env, 
                     const char *seriesname, 
                     long long sunum,
                     const char *sudir);
int drms_su_isremotesu(long long sunum);
int drms_su_getexportURL(long long sunum, char *url, int size);
const char *drms_su_getexportserver();

#endif
