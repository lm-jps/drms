#ifndef _DRMS_SERIES_H
#define _DRMS_SERIES_H

#include "drms_types.h"

int drms_series_exists(DRMS_Env_t *drmsEnv, const char *sname, int *status);

int drms_insert_series(DRMS_Session_t *session, int update, DRMS_Record_t *template, int perms);
/* checks if primary keywords and segment meta-data of two series match */
/* returns number of matching segments */

char **drms_series_createpkeyarray(DRMS_Env_t *env, 
				     const char *seriesName, 
				     int *nPKeys,
				     int *status);

void drms_series_destroypkeyarray(char ***pkeys, int nElements);

/* Checks if two series have compatible data segments.  Compares records, keywords, and 
 * segments and returns any compatible segments (in terms of data dimensions and type) in
 * matchSegs. */
int drms_series_checkseriescompat(DRMS_Env_t *drmsEnv,
				  const char *series1, 
				  const char *series2, 
				  HContainer_t *matchSegs,
				  int *status);

/* Checks if series can accept new data identified by recTempl.  Compares the keywords 
 * and segments contained within recTempl and returns any compatible segments 
 * (in terms of data dimensions and type) in matchSegs. */
int drms_series_checkrecordcompat(DRMS_Env_t *drmsEnv,
				  const char *series,
				  DRMS_Record_t *recTempl,
				  HContainer_t *matchSegs,
				  int *status);

/* Checks if series can accept new data identified by keys.  Compares the series 
 * keywords to the keys passed in (int terms of keyword types, etc.).  Does not
 * check for segment compatiblility. */
int drms_series_checkkeycompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Keyword_t *keys,
			       int nKeys,
			       int *status);

int drms_series_checksegcompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Segment_t *segs,
			       int nSegs,
			       int *status);

/************** Class functions **************/
/*
DRMS_Record_t *drms_series_new(DRMS_Env_t *env, const char *seriesname);
int drms_series_add_attr(DRMS_Record_t *series_template,
		       const char *name, DRMS_Simple_t type, const char *format,
		       const char *unit);
int drms_series_add_link(DRMS_Record_t *series_template,
		       const char *name, const char refseries);
int drms_series_remove_attr(DRMS_Record_t *series_template,
			  const char *name);
int drms_series_remove_link(DRMS_Record_t *series_template,
			  const char *name);
int drms_series_destroy(DRMS_Env_t *env, const char *seriesname);
int drms_series_commit(DRMS_Env_t *env, DRMS_Record_t *series_template);
int drms_series_describe_data(DRMS_Record_t *series_template, int depth, 
			    DRMS_Simple_t *types, int rank, int *dims,
			    DRMS_Storage_Protocol_t protocol);
*/

#endif
