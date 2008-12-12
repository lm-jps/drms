/**
@file drms_record.h
@brief Functions that retrieve, close, populate, copy, allocate, and free DRMS_Record_t structures.
@sa drms_keymap.h drms_keyword.h drms_segment.h drms_series.h drms_env.h
@example drms_record_ex1.c 
@example drms_record_ex2.c 
@example drms_record_ex3.c
@example drms_record_ex4.c
@example drms_record_ex5.c
@example drms_record_ex6.c
*/

#ifndef _DRMS_RECORD_H
#define _DRMS_RECORD_H

#include "drms_types.h"
#include "db.h"
#include "list.h"

#define kLocalSegName "local_data" /* Name of segment created when reading 
				    * fits files from local disk (outside 
				    * of any database). */
#define kLocalPrimekey "primekey"
#define kLocalPrimekeyType DRMS_TYPE_LONGLONG

/************* Constants for mode and action flags etc. *************/
typedef enum {DRMS_COPY_SEGMENTS, DRMS_SHARE_SEGMENTS} DRMS_CloneAction_t;
typedef enum {DRMS_FREE_RECORD, DRMS_INSERT_RECORD} DRMS_CloseAction_t;
/* Macros so code using the old names will still work. */
/* removed because there is no code using them
#define DRMS_DISCARD_RECORD (DRMS_FREE_RECORD)
#define DRMS_COMMIT_RECORD (DRMS_INSERT_RECORD)
*/

/** \brief DRMS query type */
typedef enum {DRMS_QUERY_COUNT, DRMS_QUERY_FL, DRMS_QUERY_ALL, DRMS_QUERY_N} DRMS_QueryType_t;

/************** User level record functions ************/

/**** For record sets. ****/
/* Retrieve a recordset specified by the DRMS dataset name string
   given in the argument "datasetname". The records are inserted into
   the record cache and marked read-only. */

DRMS_RecordSet_t *drms_open_records_internal(DRMS_Env_t *env, 
					     char *recordsetname, 
					     int retrieverecs, 
					     LinkedList_t **llistout,
                                             char **allversout,
					     int *status);

DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, char *recordsetname, 
				    int *status);
DRMS_RecordSet_t *drms_open_nrecords(DRMS_Env_t *env, 
                                     char *recordsetname, 
                                     int n,
                                     int *status);
DRMS_RecordSet_t *drms_open_localrecords(DRMS_Env_t *env, 
					 const char *dsRecSet, 
					 int *status);
DRMS_RecordSet_t *drms_open_dsdsrecords(DRMS_Env_t *env, 
					const char *dsRecSet, 
					int *status);

DRMS_RecordSet_t *drms_clone_records(DRMS_RecordSet_t *recset,  
				     DRMS_RecLifetime_t lifetime, 
				     DRMS_CloneAction_t mode, int *status);

DRMS_RecordSet_t *drms_create_records(DRMS_Env_t *env, int n, 
				      char *seriesname, DRMS_RecLifetime_t lifetime,
				      int *status);
DRMS_RecordSet_t *drms_create_records_fromtemplate(DRMS_Env_t *env, 
						   int n,  
						   DRMS_Record_t *template, 
						   DRMS_RecLifetime_t lifetime,
						   int *status);

DRMS_RecordSet_t *drms_create_recprotos(DRMS_RecordSet_t *recset, int *status);
void drms_destroy_recprotos(DRMS_RecordSet_t **protos);
DRMS_Record_t *drms_create_recproto(DRMS_Record_t *recSource, int *status);
void drms_destroy_recproto(DRMS_Record_t **proto);

/* Close a set of records. 
   1. a) If action=DRMS_INSERT_RECORD the record meta-data (keywords
      and links) will be inserted into the database and the data
      segments will be left in the storage unit directory for later
      archiving by SUMS.
      b) If action=DRMS_FREE_RECORD the data segment files are
      deleted from disk.
   2. The record structures are freed from the record cache. */

int drms_close_records(DRMS_RecordSet_t *rs, int action);

int drms_stage_records(DRMS_RecordSet_t *rs, int retrieve, int dontwait);

/**** For a single record. ****/
DRMS_Record_t *drms_clone_record(DRMS_Record_t *record, 
				 DRMS_RecLifetime_t lifetime, 
				 DRMS_CloneAction_t mode, int *status);
DRMS_Record_t *drms_create_record(DRMS_Env_t *env, char *seriesname, 
				  DRMS_RecLifetime_t lifetime, int *status);
int drms_close_record(DRMS_Record_t *rec, int action);
/** \brief Print the contents of a record data structure to stdout. */
void  drms_print_record(DRMS_Record_t *rec);
/** \brief Print the contents of a record data structure to a stream. */
void  drms_fprint_record(FILE *recfile, DRMS_Record_t *rec);
/** \brief Calculate size of a record and its segment arrays in bytes. */
long long drms_record_size(DRMS_Record_t *rec);
/** \brief Number of keywords associated with a record. */
int drms_record_numkeywords(DRMS_Record_t *rec);
/** \brief Number of links associated with a record. */
int drms_record_numlinks(DRMS_Record_t *rec);
/** \brief Number of segments associated with a record. */
int drms_record_numsegments(DRMS_Record_t *rec);
/** \brief Number of nonlink segments associated with a record. */
int drms_record_num_nonlink_segments(DRMS_Record_t *rec);
/* Storage Unit Directory associated with a record. */
/**
   @brief Find the Storage Unit directory associated with a record

   Places the path to the Storage Unit slot directory associateed with
   rec in dirname. If no storage unit slot has been assigned to the record yet,
   an empty string is stored at dirname.  Return value is status code.
   If retrieve=1 then drms_record_directory will block until the data is staged
   by SUMS.  If retrieve=0 the current string in the record structure will be returned
   so if the record is offline dirname will contain an ampty string.
*/
int drms_record_directory(DRMS_Record_t *rec, char *dirname, int retrieve);

/**** Can modify seriesinfo only if the record is a record prototype  ****/
int drms_recproto_setseriesinfo(DRMS_Record_t *rec,
				int *unitSize, 
				int *bArchive, 
				int *nDaysRetention,
				int *tapeGroup,
				const char *description);

DRMS_RecordSetType_t drms_record_getquerytype(const char *query);

/** \brief Estimate how much memory is used per record. */
long long drms_record_memsize(DRMS_Record_t *rec);

/** \brief Estimate how much memory is used for keywords in the keylist. */
long long drms_keylist_memsize(DRMS_Record_t *rec, char *keylist);

/* Return the JSOC software version that created the given record. */
char *drms_record_jsoc_version(DRMS_Env_t *env, DRMS_Record_t *rec);

/* Handling record sets */
char *drms_recordset_acquireseriesname(const char *query);

static inline int drms_recordset_getnrecs(DRMS_RecordSet_t *rs)
{
   return rs->n;
}

/** @brief Return the number of record subsets */
static inline int drms_recordset_getnumss(DRMS_RecordSet_t *rs)
{
   return rs->ss_n;
}

/** @brief Return a DRMS record-set subset query */
const char *drms_recordset_getqueryss(DRMS_RecordSet_t *rs, unsigned int setnum, int *status);

/** @brief Return a DRMS record-set subset query type */
DRMS_RecordSetType_t *drms_recordset_gettypess(DRMS_RecordSet_t *rs, 
					       unsigned int setnum, 
					       int *status);

/** @brief Return a DRMS record-set subset */
DRMS_Record_t *drms_recordset_getss(DRMS_RecordSet_t *set, unsigned int setnum, int *status);

/** @brief Return the number of records in a DRMS record-set subset */
int drms_recordset_getssnrecs(DRMS_RecordSet_t *set, unsigned int setnum, int *status);

int drms_merge_record(DRMS_RecordSet_t *rs, DRMS_Record_t *rec);

static inline DRMS_Record_t *drms_recordset_getrec(DRMS_RecordSet_t *rs, long long recnum)
{
   if (rs)
   {
      return rs->records[recnum];
   }

   return NULL;
}

/** \brief generate drms query string

\param qtype:
\return query string
*/
char *drms_query_string(DRMS_Env_t *env, 
			const char *seriesname,
			char *where, int filter, int mixed,
			DRMS_QueryType_t qtype, 
                        void *data, 
                        char *fl,
                        int allvers);

/* Chunking record queries */
int drms_recordset_setchunksize(unsigned int size);
unsigned int drms_recordset_getchunksize();
int drms_open_recordchunk(DRMS_Env_t *env,
                          DRMS_RecordSet_t *rs, 
			  DRMS_RecSetCursorSeek_t seektype, 
			  long long chunkindex,  
			  int *status);
int drms_close_recordchunk(DRMS_RecordSet_t *rs);
DRMS_RecordSet_t *drms_open_recordset(DRMS_Env_t *env, 
				      const char *rsquery, 
				      int *status);
DRMS_Record_t *drms_recordset_fetchnext(DRMS_Env_t *env, DRMS_RecordSet_t *rs, int *status);
DRMS_Record_t *drms_recordset_fetchprevious(DRMS_Env_t *env, DRMS_RecordSet_t *rs, int *status);
DRMS_Record_t *drms_recordset_fetchnextinset(DRMS_Env_t *env, DRMS_RecordSet_t *rs, int *setnum, int *status);
void drms_free_cursor(DRMS_RecSetCursor_t **cursor);

/* DSDS */
int drms_record_isdsds(DRMS_Record_t *rec);
int drms_record_islocal(DRMS_Record_t *rec);

/* Doxygen function documentation */

/**
   @fn DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, char *recordsetname, int *status)
   Retrieve a set of records specified by a recordset query.
   Within the current DRMS session (whose information is stored  in \a env),
   this  function  submits a database query specified in \a recordsetname and
   creates  a  record-set  structure  (::DRMS_RecordSet_t)  to  contain  the
   results  of  the  query.   If,  at  the time this function is called, a
   requested record structure (::DRMS_Record_t) exists in the  record  cache
   (\a env->record_cache),  then  a  pointer  to  that  record  structure  is
   inserted into the results record set.  Otherwise, a new  record  struc-
   ture  is created, populated from the database, inserted into the record
   cache, and inserted into the results record  set.   The  newly  created
   record is marked read-only and assigned a permanent lifetime (DRMS_PER-
   MANENT).

   Upon successful completion, the  function  returns  a  ::DRMS_RecordSet_t
   pointer,  and  sets  \a *status  to ::DRMS_SUCCESS.  If an error occurs, the
   function returns NULL and sets \a *status to  an  appropriate  error  code
   defined  in  drms_statuscodes.h.   Typical errors are as follows.  If a
   problem occurs during communication with the database  server,  \a *status
   is  set  to  ::DRMS_ERROR_QUERYFAILED.   If  the  number of records to be
   returned  exceeds  the  allowable  number,  then  \a *status  is  set   to
   ::DRMS_QUERY_TRUNCATED.

   The  caller  owns  the  allocated  memory  associated with the returned
   record set and must release it by calling ::drms_close_records.

   @param env DRMS session information.
   @param recordsetname A string that specifies a database query. It 
   includes a series name and clauses to extract a subset of records from that series.
   Please see http://jsoc.stanford.edu/jsocwiki/DrmsNames for more 
   information about database queries.
   @param status Pointer to DRMS status (see drms_statuscodes.h) returned
   by reference. 0 if successful, non-0 otherwise.
   @return The set of records retrieved by the query.
*/

/**
   @fn DRMS_RecordSet_t *drms_clone_records(DRMS_RecordSet_t *recset, DRMS_RecLifetime_t lifetime, DRMS_CloneAction_t mode, int *status)
   Create a new set of records using values from an
   original set of records to populate the new records. Within the current DRMS session,
   this  function  creates  a record-set structure (::DRMS_RecordSet_t) that
   contains "copies" of the record structures contained  in  \a recset.  For
   each  record  in  \a recset,  a new ::DRMS_Record_t structure is created and
   assigned a unique record number from the DRMS database.  The values  of
   the  keywords, links, and segments of the original record in recset are
   used to populate the newly created record.  If \a mode == ::DRMS_SHARE_SEGMENTS, 
   the newly created segments will share the segment files with the
   original record, i.e. the new record will have the  same  storage  unit
   number  (DSINDEX)  as  the  original record.  However, keyword and link
   data will be replicated.  If \a mode == ::DRMS_COPY_SEGMENTS,  the  original
   record's  segment  files will be copied to a new storage unit slot, and
   the copied files will be associated with the new record.

   The  newly  created  records   are   placed   in   the   record   cache
   (::DRMS_Env_t->record_cache)  and  are made writeable and assigned a lifetime of
   \a lifetime (please see ::drms_reclifetime for details on lifetime).

   Upon successful completion, the  function  returns  a  ::DRMS_RecordSet_t
   pointer,  and  sets  \a *status  to  0.   If an error occurs, the function
   returns NULL and sets \a *status to an appropriate error code  defined  in
   drms_statuscodes.h.  Typical errors are as follows.  If \a recset does not
   have   any   legitimate   records,   then    \a *status    is    set    to
   ::DRMS_ERROR_BADRECORDCOUNT.  If there was an error receiving one or more
   proper record numbers from the database server, then \a *status is set  to
   ::DRMS_ERROR_BADSEQUENCE.   If an error occurs while creating a SUMS slot
   directory, then *status is set to ::DRMS_ERROR_MKDIRFAILED.

   The caller owns the  allocated  memory  associated  with  the  returned
   record set and must release it by calling ::drms_close_records.

   @param recset The original set of records that get cloned.
   @param lifetime Either ::DRMS_PERMANENT (at the end of the
   DRMS session, the cloned records should be saved to the database) 
   or ::DRMS_TRANSIENT (at the end of the DRMS session, the cloned records should be
   discarded).
   @param mode Either DRMS_COPY_SEGMENTS (copy original data
   to the newly cloned records) or DRMS_SHARE_SEGMENTS(point to the original data).
   @param status Pointer to DRMS status (see drms_statuscodes.h) returned
   by reference. 0 if successful, non-0 otherwise.
   @return The set of cloned records.
*/

/**
   @fn DRMS_RecordSet_t *drms_create_records(DRMS_Env_t *env, int n, char *seriesname, DRMS_RecLifetime_t lifetime, int *status)
   Create a new set of \a n records for series \a seriesname. 
   Within the current DRMS session, this function creates a 
   record-set structure (::DRMS_RecordSet_t) that
   contains \a n newly created record structures (::DRMS_Record_t). Each created record 
   is assigned a unique record number from the DRMS database. The values of
   the keywords, links, and segments from the series' template record (stored 
   in the series cache (::DRMS_Env_t->record_cache) are used to populate
   the corresponding values of each of the \a n created records.

   The newly  created  records are placed   in   the   record   cache
   (::DRMS_Env_t->record_cache)  and  are made writeable and assigned a lifetime of
   \a lifetime (please see ::drms_reclifetime for details on lifetime).
   
   Upon successful completion, the  function  returns  a  ::DRMS_RecordSet_t
   pointer,  and  sets  \a *status  to  0.   If an error occurs, the function
   returns NULL and sets \a *status to an appropriate error code  defined  in
   drms_statuscodes.h.  Typical errors are as follows. If there was an error 
   receiving one or more 
   proper record numbers from the database server, then \a *status is set  to
   ::DRMS_ERROR_BADSEQUENCE.   If an error occurs while creating a SUMS slot
   directory, then *status is set to ::DRMS_ERROR_MKDIRFAILED.

   The caller owns the  allocated  memory  associated  with  the  returned
   record set and must release it by calling ::drms_close_records.

   @param env Contains information about the DRMS session in which the 
   records should be created.
   @param n Number of records to create.
   @param seriesname Name of the series into which records should be inserted.
   @param lifetime Either ::DRMS_PERMANENT (at the end of the
   DRMS session, the created records should be saved to the database) 
   or ::DRMS_TRANSIENT (at the end of the DRMS session, the created records should be
   discarded).
   @param status Pointer to DRMS status (see drms_statuscodes.h) returned
   by reference. 0 if successful, non-0 otherwise.
   @return The set of created records.
*/

/**
   @fn int drms_close_records(DRMS_RecordSet_t *rs, int action)
   Close a set of records and free allocated memory, optionally inserting 
   new records into the database.
   If \a action == ::DRMS_FREE_RECORD, then if a record  being  closed  is  the
   only  reference  to  a  SUMS  storage unit slot, that slot is freed and
   marked for removal during SUMS garbage collection.  During SUMS garbage
   collection, all data-segment files stored within this storage unit will
   be deleted.  In this scenario, segment files written to SUMS during the
   current    DRMS    session   are   not   preserved.    If   action   ==
   ::DRMS_INSERT_RECORD, then this function saves  the  keyword,  link,  and
   segment  information  in  the  appropriate database tables, and ensures
   that segment files written to SUMS during the current DRMS session  are
   preserved.   In  order  to  succeed,  no  record contained in rs can be
   marked read-only.

   Regardless of \a action, this function calls ::drms_free_records  to  deallo-
   cate  the  \a rs  ::DRMS_RecordSet_t  structure,  the  array  of pointers to
   ::DRMS_Record_t  structures  contained  within   \a rs,   and   the   actual
   ::DRMS_Record_t  structures  that  these  pointers  reference.  All these
   structures and pointers must have been  previously  allocated  in  heap
   memory  (functions  like  ::drms_open_records ensure this is the case).
   The records are also removed from the record cache (\a env->record_cache).

   Upon  successful  completion,  the  function  returns  0.   If an error
   occurs, the function returns  an  appropriate  error  code  defined  in
   drms_statuscodes.h.   Typical errors are as follows.  If action is nei-
   ther ::DRMS_FREE_RECORD nor ::DRMS_INSERT_RECORD,  then  ::DRMS_ERROR_INVALI-
   DACTION  is returned.  If \a action == ::DRMS_INSERT_RECORD and at least one
   record in \a rs is marked  read-only,  then  ::DRMS_ERROR_COMMITREADONLY  is
   returned.

   @param rs The set of records to close
   @param action Specifes whether records being closed are to be
   inserted into the database (::DRMS_INSERT_RECORD) or not (::DRMS_FREE_RECORD).
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/

/**
   @fn int drms_stage_records(DRMS_RecordSet_t *rs, int retrieve, int dontwait)
   Stage a set of records. All records must come from the same series
   @param rs The set of records to stage
   @param retrieve Whether to retrieve the SU if it's off-line
   @param dontwait Whether to wait for reply from SUMS
*/

/**
   @fn DRMS_RecordSet_t *drms_open_recordset(DRMS_Env_t *env, const char *rsquery, int *status)
   Retrieve a set of records specified by a recordset query.
   Within the current DRMS session (whose information is stored  in @a env),
   this  function  submits a database query specified in @a recordsetname and
   creates  a  record-set  structure  (::DRMS_RecordSet_t)  to  contain  the
   results  of  the  query.   If,  at  the time this function is called, a
   requested record structure (::DRMS_Record_t) exists in the  record  cache
   (@a env->record_cache),  then  a  pointer  to  that  record  structure  is
   inserted into the results record set.  Otherwise, a new  record  struc-
   ture  is created, populated from the database, inserted into the record
   cache, and inserted into the results record  set.   The  newly  created
   record is marked read-only and assigned a permanent lifetime (DRMS_PER-
   MANENT).

   Upon successful completion, the  function  returns  a  ::DRMS_RecordSet_t
   pointer,  and  sets  @a *status  to ::DRMS_SUCCESS.  If an error occurs, the
   function returns NULL and sets @a *status to  an  appropriate  error  code
   defined  in  drms_statuscodes.h.   Typical errors are as follows.  If a
   problem occurs during communication with the database  server,  @a *status
   is  set  to  ::DRMS_ERROR_QUERYFAILED.  

   The  caller  owns  the  allocated  memory  associated with the returned
   record set and must release it by calling ::drms_close_records.

   The main difference between this function and ::drms_open_records is that the former
   creates a ::DRMS_RecordSet_t structure for each record specified by the record-set query.
   But the current function creates a ::DRMS_RecordSet_t structure for each member of 
   the current subset (or 'chunk') of the records specified by the record-set query. Only
   one chunk of records resides in memory at any time.  When a record in a non-resident chunk
   is needed, the current chunk of ::DRMS_RecordSet_t structures is freed, and the next
   chunk is loaded into memory.  The purpose
   of this function is to conserve memory by facilitating the processing of chunks of
   records instead of processing the entire set of records.  To override the 
   default chunk size, the user calls ::drms_recordset_setchunksize.  

   Operating on chunks of records is transparent to the caller who can continue
   to interate through records without being cognizant of 'chunking'.  
   To iterate through all records in the set, after calling this function, the caller
   would call ::drms_recordset_fetchnext in a loop to obtain a pointer to each 
   record in the sequence.  When ::drms_recordset_fetchnext returns NULL, no more
   records remain in the record-set.

   @param env DRMS session information.
   @param rsquery A string that specifies a database query. It 
   includes a series name and clauses to extract a subset of records from that series.
   Please see http://jsoc.stanford.edu/jsocwiki/DrmsNames for more 
   information about database queries.
   @param status Pointer to DRMS status (see drms_statuscodes.h) returned
   by reference. 0 if successful, non-0 otherwise.
   @return The set of records retrieved by the query.
*/

/**
   @fn DRMS_Record_t *drms_create_recproto(DRMS_Record_t *recSource, int *status)
   Create a stand-alone record prototype that is essentially a duplicate of @a recSource. 'Stand-alone'
   means the returned record is not subject to DRMS caching and freeing. It also implies
   that there is no @e connection between the prototype record and any database entities that the 
   record represents. In particular, modification to the prototype record cannot cause any
   changes to the underlying database entities. This is not true for non-stand-alone records - 
   changes to keyword values of those types of records @e can cause changes to database column values.

   The term @e prototype suggests that the returned record is a template. In fact, the returned
   record is very much like a jsd file that has been parsed into DRMS structures, and it can be used to 
   create a series. However, the prototype contains the series name of the series that @a recSource
   belongs to. If the prototype is used directly as is to create a series, the series creation will
   fail as the series named in the prototype already exists. To avoid this, the 
   ::drms_create_series_fromprototype function can be used (one argument to this function is
   the name of the new output series).

   This function allocates DRMS structures and it is the callers responsibility to free this memory.
   If the prototype is passed to ::drms_create_series_fromprototype, this function will free
   the prototype. Alternatively, ::drms_destroy_recproto will free the prototype.

   See ::drms_create_series_fromprototype for an example of how to create a series 'on-the-fly' using
   this function.

   @param recSource The existing DRMS record from which a stand-alone duplicate is to be created.
   @param tatus Pointer to DRMS status (see drms_statuscodes.h) returned
   by reference. 0 if successful, non-0 otherwise.
   @return The record prototype that is a stand-alone duplicate of @a recSource
*/

#endif
