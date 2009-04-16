/**
@file drms_env_priv.h
*/
#ifndef _DRMS_ENV_priv_H
#define _DRMS_ENV_priv_H

int drms_cache_init(DRMS_Env_t *env);

/** @fn int drms_cache_init(DRMS_Env_t *env)
    \brief Initialize drms caches: SU, series, and record

 - SU cache:
   -# hash container initialized
 - series cache: 
   -# hash container initialized
   -# an uninitalized template for every series in DB. The series name
      is used to locate a slot in the hash container 
 - record cache: 
   -# hash container initialized

\param env handle for connection to either db or socket
\return 0 upon success, 1 otherwise.
*/

#endif
