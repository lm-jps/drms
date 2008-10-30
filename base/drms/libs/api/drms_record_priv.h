/**
@file drms_record_priv.h
@brief Private functions that retrieve, close, populate, copy, allocate, and free DRMS_Record_t structures.
@sa drms_keymap.h drms_keyword.h drms_segment.h drms_series.h drms_env.h
@example drms_record_ex1.c 
@example drms_record_ex5.c
*/
#ifndef _DRMS_RECORD_PRIV_H
#define _DRMS_RECORD_PRIV_H

#undef EXTERNALCODE /* XXX - fix this */
#ifndef EXTERNALCODE

#include "drms_types.h"

/********************************* Internal functions. **************/

/* Execute drms_close_record for all records in the record cache that
   are not marked read-only, i.e. which were created by the present
   program. */

/**
   Close all records in the record cache, optionally inserting modified, writeable 
   records into the database. This function iterates through all records in the 
   record cache (""DRMS_Env_t::record_cache).
   If \a action == ::DRMS_INSERT_RECORD, then for each such record that is not
   marked read-only, this function calls ::drms_close_records, passing
   the record as an argument. The result is preservation of the record's
   data in the database. If \a action == ::DRMS_FREE_RECORD, then the record's
   data is discarded. Please see ::drms_close_records for more details.
   For each record in the record cache, this function calls
   ::drms_free_record to deallocate the ::DRMS_Record_t structure associated 
   with the record, and it removes the record from the record cache.
   
   @param env DRMS session information.
   @param action If ::DRMS_INSERT_RECORD, save all modifiable records in the database, if ::DRMS_FREE_RECORD, do not save any record.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_closeall_records(DRMS_Env_t *env, int action);

/**
   Open a file named \a filename, with mode \a mode, in the storage-unit directory 
   that belongs to record \a rec. If \a *mode == 'w' or \a *mode == 'a' and  
   \a rec has not been assigned a storage-unit directory, one is allocated. If \a *mode == 'r'
   \a rec must have an assigned storage-unit directory. \a rec must contain a least
   one DRMS data segment.
   
   @param rec (::DRMS_Record_t *) Pointer to a DRMS record that contains at least one 
   data segment.
   @param filename (char *) Name of the file that exists or will be created in the 
   storage-unit directory belonging to \a rec.
   @param mode (const char *) Pointer to a \a char that specifies the mode to pass 
   to \a fopen. Supported values include 'a', 'r', and 'w'.
   @return (FILE *) Upon success, returns a \a FILE * to the file opened. Otherwise, returns
   NULL.
*/
FILE *drms_record_fopen(DRMS_Record_t *rec, char *filename, const char *mode);

/**
   Remove record \a rec from the DRMS-session-specific record cache (::DRMS_Env_t::record_cache),
   freeing all allocated memory associated with the record. ::drms_free_record_struct is 
   the function that deallocates this memory. Removal decrements the ref
   count on the record's storage-unit directory. When the ref count reaches 0, 
   the entire storage unit is freed with a call to ::drms_freeunit.

   @param rec Pointer to the DRMS record to be removed and freed.
*/
void drms_free_record(DRMS_Record_t *rec);

/**
   Remove all records contained in \a rs from the 
   DRMS-session-specific record cache (::DRMS_Env_t::record_cache),
   freeing all allocated memory associated with each record. Make \a rs->n calls
   to ::drms_free_record. Also frees \a rs->records, an array of ::DRMS_Record_t * 
   and \a rs.

   @param rs Pointer to the DRMS recordset containing records to be removed.
*/
void drms_free_records(DRMS_RecordSet_t *rs);

/**
   Return the actual size in bytes of the data contained in \a rec.

   @param rec Pointer to the DRMS record whose data size is being requested.
   @return The number of data bytes contained in \a rec.
*/

long long drms_record_size(DRMS_Record_t *rec);

/* Retrieve the record with record number given in "recnum" from the series
   given in "seriesname". */

/**
xxx
*/
DRMS_Record_t *drms_retrieve_record(DRMS_Env_t *env, const char *seriesname, 
				    long long recnum, 
				    HContainer_t *goodsegcont,
				    int *status);
/* Retrieve the records from the series given in "seriesname" satisfying
   the condition in the string "where". "where" must be a valid SQL
   clause involving the column names of the main record table for the 
   series. */

/**
xxx
*/
DRMS_RecordSet_t *drms_retrieve_records(DRMS_Env_t *env, 
					const char *seriesname, char *where, 
					int filter, int mixed, 
					HContainer_t *goodsegcont,
                                        int allvers, 
                                        int nrecs, 
					int *status);
/* Insert multiple records in the database using the 
   fast bulk insert interface. */
/**
xxx
*/
int drms_insert_records(DRMS_RecordSet_t *recset);
/* Get a pointer to the template record structure for series. */

/**
   Within the current DRMS session (whose information is stored in @a env), 
   this function returns a template record for the series @a seriesname.  
   If such a template record exists in the series template-record cache 
   (@c env->series_cache), that one is returned. Otherwise a new template record 
   is created, cached, and returned to the caller. The template-record structure 
   is allocated and assigned default values derived from entries in the global 
   database tables: @a drms_series, @a drms_segment, @a drms_link, and @a drms_keyword.
   ::drms_template_record calls ::drms_template_segments, ::drms_template_links, 
   and ::drms_template_keywords to populate segment, link, and keyword values.

   The caller does not own the allocated memory associated with the 
   returned record and should not release it 
   (i.e., the caller should not call ::drms_close_record).

   Typical uses of this function include obtaining a list of keywords or 
   a particular keyword, primary or otherwise, obtaining a list of segments or 
   a particular segment, obtaining a list of links or a particular link, and 
   obtaining series information.

   This really should be private, and not available to modules. If a module 
   needs series information, it should call drms_series.h functions.
   
   @param env DRMS session information.
   @param seriesname The name of the series for which the template record is desired.
   @param status DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
   @return Upon successful completion, the function returns a ::DRMS_Record_t pointer,
   and sets @a *status to ::DRMS_SUCCESS.  If an error occurs, the function 
   returns NULL and sets @a *status to an appropriate error code defined in
   drms_statuscodes.h. Typical errors are as follows. If @a seriesname does not
   exist in the series cache, then @a *status is set to 
   ::DRMS_ERROR_UNKNOWNSERIES. If a problem occurs during communication 
   with the database server, @a *status is set to ::DRMS_ERROR_QUERYFAILED. 
   If a database table is malformed or corrupt, @a *status is set to 
   ::DRMS_ERROR_BADQUERYRESULT.  
*/
DRMS_Record_t *drms_template_record(DRMS_Env_t *env, const char *seriesname, int *status);

/* Allocate a jsd template record. */
DRMS_Record_t *drms_create_jsdtemplate_record(DRMS_Env_t *env, 
                                              const char *seriesname, 
                                              int *status);

void drms_destroy_jsdtemplate_record(DRMS_Record_t **rec);
/* Populate a record structure with the meta-data for record number "recnum"
   from its series (given by record->seriesinfo.seriesname). */

/**
xxx
*/
int drms_populate_record(DRMS_Record_t *record, long long recnum);
/* Populate multiple records with the meta-data values returned 
   from a database query. */

/**
xxx
*/
int drms_populate_records( DRMS_RecordSet_t *rs, DB_Binary_Result_t *qres);
/* Allocate a new record structure and assign it the record number
   recnum and insert it in the record cache. */

/**
xxx
*/
DRMS_Record_t *drms_alloc_record(DRMS_Env_t *env, const char *series, 
				 long long recnum, int *status);
/* Allocate a new record structure based on a template, assign it 
   the record number recnum and insert it in the record cache. */

/**
xxx
*/
DRMS_Record_t *drms_alloc_record2(DRMS_Record_t *template,
				  long long recnum, int *status);

/* Generate a string containing a comma-separated list column
   names of the database table containing the meta-data of the 
   given record. Return the number of columns in num_args. */
/**
xxx
*/
char *drms_field_list(DRMS_Record_t *rec, int *num_args);
/* Deep copy the contents of a record structure. */

/**
xxx
*/
void drms_copy_record_struct(DRMS_Record_t *dst, DRMS_Record_t *src);
/* Deep free the contents of a record structure. */

/**
xxx
*/
void drms_free_record_struct(DRMS_Record_t *rec);

/**
xxx
*/
void drms_free_template_record_struct(DRMS_Record_t *rec);

#endif /* EXTERNALCODE */
#endif /* _DRMS_RECORD_PRIV_H */
