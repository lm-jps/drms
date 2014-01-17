/**
\file drms_series.h
\brief Functions to query the existence of series, get information
about a series' primary keywords, and check series compatibility.

\sa drms_keyword.h drms_record.h drms_segment.h drms_statuscodes.h drms_types.h
hcontainer.h

\example drms_series_ex1.c
\example drms_series_ex2.c 
\example drms_series_ex3.c 
\example drms_series_ex4.c 
\example drms_series_ex5.c
*/
#ifndef _DRMS_SERIES_H
#define _DRMS_SERIES_H

#include "drms_types.h"

/* This is the current version. If a jsd (or a prototype record) used to create 
 * a series does not specify a version, then the current version is assumed.
 */
#define kSERIESVERSION  "2.1"

int get_namespace(const char *seriesname, char **namespace, char **shortname);

/**
queries the DRMS database to determine if the series specified by
\a sname exists. 

\return If a null value for \a sname, or an empty string, is
provided, the function returns 0. Otherwise, if \a sname is a valid
series name, but the series does not exist in DRMS, the function
returns 0, and sets \a *status to ::DRMS_ERROR_UNKNOWNSERIES.  If there is
an error looking up \a sname, the function returns 0, and sets \a *status
to the value that would result by calling drms_template_record with
\a sname. If \a sname is a valid series name, and it names an existing
DRMS series, the function returns 1.

*/
int drms_series_exists(DRMS_Env_t *drmsEnv, const char *sname, int *status);

/* checks if primary keywords and segment meta-data of two series match */
/* returns number of matching segments */

/**
allocates and returns an array of null-terminated strings, each of
which is the name of a primary keyword of the series \a seriesName. It
is the caller's responsibility to free the allocated array by calling
::drms_series_destroypkeyarray.  Upon success,
::drms_series_createpkeyarray returns by reference in the \a nPKeys
parameter the number of primary keywords that belong to the series \a
seriesName.

\return a primary key array upon success.\n
\a *status is set to DRMS_SUCCESS upon success. 
Otherwise \a *status is set to a diagostic status code (see drms_statuscodes.h), 
which includes the following:
- ::DRMS_ERROR_UNKNOWNSERIES
- ::DRMS_ERROR_OUTOFMEMORY
- ::DRMS_ERROR_INVALIDDATA 
*/
char **drms_series_createrealpkeyarray(DRMS_Env_t *env, 
				       const char *seriesName, 
				       int *nPKeys,
				       int *status);

char **drms_series_createpkeyarray(DRMS_Env_t *env, 
				   const char *seriesName, 
				   int *nPKeys,
				   int *status);
/**
deallocates an array of strings allocated by
::drms_series_createpkeyarray. The user must specify the number of
primary keywords in the \a nElements parameter. Failure to provide
the correct number could result in undetermined program behavior.

Upon success, this function sets \a *pkeys to NULL.
*/
void drms_series_destroypkeyarray(char ***pkeys, int nElements);

/* Checks if two series have compatible data segments.  Compares records, keywords, and 
 * segments and returns any compatible segments (in terms of data dimensions and type) in
 * matchSegs. */

/**
queries the DRMS database to determine if \a series1 and \a series2
are compatible in terms of their primary-keyword and segment
definitions.  Two primary keyword definitions "match" if the type,
recscope, and per_segment ::DRMS_Keyword_t fields are
equal. drms_segment_segsmatch is used to determine if two segment
definitions "match". If all primary keywords and at least one segment
match and no error occurs, the function intializes \a matchSegs and
allocates one ::HContainer_t element for each segment that the two
series have in common.  Each ::HContainer_t element is a string that
names the matching segment. The caller must provide a non-NULL \a
matchSegs container to the function. It is the caller's responsibility
to deallocate memory associated with the allocation of ::HContainer_t
elements.

\return 1 if compatible. Upon success, this function sets \a *status 
to ::DRMS_SUCCESS. Otherwise \a *status is set to a 
diagostic status code (see drms_statuscodes.h).
*/
int drms_series_checkseriescompat(DRMS_Env_t *drmsEnv,
				  const char *series1, 
				  const char *series2, 
				  HContainer_t *matchSegs,
				  int *status);

/* Checks if series can accept new data identified by recTempl.  Compares the keywords 
 * and segments contained within recTempl and returns any compatible segments 
 * (in terms of data dimensions and type) in matchSegs. */
/**
queries the DRMS database to determine if \a recTempl is compatible
with the series named \a series in terms of their primary-keyword
and segment definitions. The two are compatible if an attempt to
insert \a recTempl into the series named \a series would
succeed. This function should be used whenever a record is created
by a means other than drms_create_records or 
drms_clone_records, and the user will attempt to insert that newly
created record into series \a series.

\return 1 if compatible. Upon success, this function sets \a *status 
to ::DRMS_SUCCESS. Otherwise \a *status is set to a 
diagostic status code (see drms_statuscodes.h).

*/
int drms_series_checkrecordcompat(DRMS_Env_t *drmsEnv,
				  const char *series,
				  DRMS_Record_t *recTempl,
				  HContainer_t *matchSegs,
				  int *status);

/* Checks if series can accept new data identified by keys.  Compares the series 
 * keywords to the keys passed in (int terms of keyword types, etc.).  Does not
 * check for segment compatiblility. */
/**
queries the DRMS database to determine if each of the keywords in
\a keys, an array of ::DRMS_Keyword_t, "match" a keyword defined for
series \a series. drms_keyword_keysmatch is used to determined if
the two ::DRMS_Keyword_t match. The caller must provide, in the
\a nKeys parameter, the number of keywords contained in \a keys.

\return 1 if compatible.  Upon success, this function sets \a *status 
to ::DRMS_SUCCESS. Otherwise \a *status is set to a 
diagostic status code (see drms_statuscodes.h).
*/
int drms_series_checkkeycompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Keyword_t *keys,
			       int nKeys,
			       int *status);
/**
queries the DRMS database to determine if each of the segments in
\a segs, an array of ::DRMS_Segment_t, "match" a segment defined for
series \a series. drms_segment_segsmatch is used to determine if
two segment definitions match. The caller must provide, in the
\a nSegs parameter, the number of segments contained in \a segs.

\return 1 if compatible. Upon success, this function sets \a *status 
to ::DRMS_SUCCESS. Otherwise \a *status is set to a 
diagostic status code (see drms_statuscodes.h).

*/
int drms_series_checksegcompat(DRMS_Env_t *drmsEnv,
			       const char *series,
			       DRMS_Segment_t *segs,
			       int nSegs,
			       int *status);

int drms_series_isreplicated(DRMS_Env_t *env, const char *series);

int drms_addkeys_toseries(DRMS_Env_t *env, const char *series, const char *spec, char **sql);

int drms_dropkeys_fromseries(DRMS_Env_t *env, const char *series, char **keys, int nkeys);

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

static inline const char *drms_series_getvers()
{
   return kSERIESVERSION;
}

int drms_series_isvers(DRMS_SeriesInfo_t *si, DRMS_SeriesVersion_t *v);

int GetTableOID(DRMS_Env_t *env, const char *ns, const char *table, char **oid);
int GetColumnNames(DRMS_Env_t *env, const char *oid, char **colnames);

int drms_series_updatesummaries(DRMS_Env_t *env,
                                const char *series,
                                int nrows,
                                int ncols,
                                char **pkeynames,
                                long long* recnums,
                                int added);

#if (defined TOC && TOC)
int drms_series_tocexists(DRMS_Env_t *env, int *status);
int drms_series_createtoc(DRMS_Env_t *env);
int drms_series_intoc(DRMS_Env_t *env, const char *series, int *status);
int drms_series_insertintotoc(DRMS_Env_t *env, const char *series);
#endif

char * drms_series_nrecords_querystringA(const char *series, int *status);
char *drms_series_nrecords_querystringB(const char *series, const char *npkwhere, int *status);
char *drms_series_nrecords_querystringC(const char *series, const char *pkwhere, int *status);
char *drms_series_nrecords_querystringD(const char *series, const char *pkwhere, const char *npkwhere, int *status);
char *drms_series_nrecords_querystringFL(DRMS_Env_t *env, const char *series, const char *npkwhere, HContainer_t *pkwhereNFL, HContainer_t *firstlast, int *status);

int drms_series_shadowexists(DRMS_Env_t *env, const char *series, int *status);
int drms_series_createshadow(DRMS_Env_t *env, const char *series, const char *tname);
int drms_series_dropshadow(DRMS_Env_t *env, const char *series, const char *tname);
void drms_series_setcreateshadows(DRMS_Env_t *env, int *val);
void drms_series_unsetcreateshadows(DRMS_Env_t *env);
char *drms_series_all_querystringA(DRMS_Env_t *env, const char *series, const char *fields, int limit, int cursor, int *status);
char *drms_series_all_querystringB(DRMS_Env_t *env, const char *series, const char *npkwhere, const char *fields, int limit, int cursor, int *status);
char *drms_series_all_querystringC(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *fields, int limit, int cursor, int *status);
char *drms_series_all_querystringD(DRMS_Env_t *env,
                                   const char *series,
                                   const char *pkwhere,
                                   const char *npkwhere,
                                   const char *fields,
                                   int limit,
                                   int cursor, 
                                   int *status);
char *drms_series_all_querystringFL(DRMS_Env_t *env, const char *seriesname, const char *npkwhere, HContainer_t *pkwhereNFL, const char *fields, int limit, HContainer_t *firstlast, int *status);

char *drms_series_n_querystringA(DRMS_Env_t *env, const char *series, const char *fields, int nrecs, int limit, int *status);
char *drms_series_n_querystringB(DRMS_Env_t *env, const char *series, const char *npkwhere, const char *fields, int nrecs, int limit, int *status);
char *drms_series_n_querystringC(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *fields, int nrecs, int limit, int *status);
char *drms_series_n_querystringD(DRMS_Env_t *env, const char *series, const char *pkwhere, const char *npkwhere, const char *fields, int nrecs, int limit, int *status);
char *drms_series_n_querystringFL(DRMS_Env_t *env, const char *series, const char *npkwhere, HContainer_t *pkwhereNFL, const char *fields, int nrecs, int limit, HContainer_t *firstlast, int *status);

int drms_series_summaryexists(DRMS_Env_t *env, const char *series, int *status);
int drms_series_canupdatesummaries(DRMS_Env_t *env, const char *series, int *status);
int drms_series_gethighestkeyrank(DRMS_Env_t *env, const char *series, int *status);
int drms_series_hastemptab(const char *query);

char *drms_series_createPkeyList(DRMS_Env_t *env, const char *series, const char *prefix, const char *suffix, char *pkeyarr[], int *npkey, int *status);
char *drms_series_createPkeyColList(DRMS_Env_t *env, const char *series, const char *prefix, const char *suffix, char *pkeyarr[], int *npkey, int *status);

#endif
