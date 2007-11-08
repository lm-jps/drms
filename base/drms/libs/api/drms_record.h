#ifndef _DRMS_RECORD_H
#define _DRMS_RECORD_H

#include "drms_types.h"
#include "db.h"

#define kLocalSegName "local_data" /* Name of segment created when reading 
				    * fits files from local disk (outside 
				    * of any database). */

/************* Constants for mode and action flags etc. *************/
typedef enum {DRMS_COPY_SEGMENTS, DRMS_SHARE_SEGMENTS} DRMS_CloneAction_t;
typedef enum {DRMS_FREE_RECORD, DRMS_INSERT_RECORD} DRMS_CloseAction_t;
/* Macros so code using the old names will still work. */
/* removed because there is no code using them
#define DRMS_DISCARD_RECORD (DRMS_FREE_RECORD)
#define DRMS_COMMIT_RECORD (DRMS_INSERT_RECORD)
*/


/************** User level record functions ************/

/**** For record sets. ****/
/* Retrieve a recordset specified by the DRMS dataset name string
   given in the argument "datasetname". The records are inserted into
   the record cache and marked read-only. */
DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, char *recordsetname, 
				    int *status);

/* Clone a set of records, i.e. create a new set of records and copy
   the value of keywords, links and segments from the pre-existing
   records given in "rs".  If mode=DRMS_SHARE_SEGMENTS the new
   segments will share segment files with the old records, i.e. it
   will have the same storage unit number (DSINDEX), and only keyword
   and link data will be replicated.  If mode=DRMS_COPY_SEGMENTS the
   segment files for the old records will be copied to a new storage
   unit slots and assigned to the new records. */ 
DRMS_RecordSet_t *drms_clone_records(DRMS_RecordSet_t *recset,  
				     DRMS_RecLifetime_t lifetime, 
				     DRMS_CloneAction_t mode, int *status);

/* Create a new set of n records. Fill keywords, links and segments
   with their default values from the series definition. Each record
   will be assigned a new storage unit slot to store its segment files
   in. */
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

/* Execute drms_close_record for all records in the record cache that
   are not marked read-only, i.e. which were created by the present
   program. */
int drms_closeall_records(DRMS_Env_t *env, int action);



/**** For a single record. ****/
DRMS_Record_t *drms_clone_record(DRMS_Record_t *record, 
				 DRMS_RecLifetime_t lifetime, 
				 DRMS_CloneAction_t mode, int *status);
DRMS_Record_t *drms_create_record(DRMS_Env_t *env, char *seriesname, 
				  DRMS_RecLifetime_t lifetime, int *status);
int drms_close_record(DRMS_Record_t *rec, int action);
/* Print the contents of a record data structure to stdout. */
void  drms_print_record(DRMS_Record_t *rec);
/* Calculate size of a record and its segment arrays in bytes. */
long long drms_record_size(DRMS_Record_t *rec);
/* Number of keywords associated with a record. */
int drms_record_numkeywords(DRMS_Record_t *rec);
/* Number of links associated with a record. */
int drms_record_numlinks(DRMS_Record_t *rec);
/* Number of segments associated with a record. */
int drms_record_numsegments(DRMS_Record_t *rec);
/* Number of nonlink segments associated with a record. */
int drms_record_num_nonlink_segments(DRMS_Record_t *rec);
/* Storage Unit Directory associated with a record. */
void drms_record_directory(DRMS_Record_t *rec, char *dirname, int retrieve);
/* Special fopen for accessing files in record directory. */
FILE *drms_record_fopen(DRMS_Record_t *rec, char *filename, const char *mode);

/**** Can modify seriesinfo only if the record is a record prototype  ****/
int drms_recproto_setseriesinfo(DRMS_Record_t *rec,
				int *unitSize, 
				int *bArchive, 
				int *nDaysRetention,
				int *tapeGroup,
				const char *description);


/********************************* DRMS Internal functions. **************/
void drms_free_record(DRMS_Record_t *rec);
void drms_free_records(DRMS_RecordSet_t *rs);
long long drms_estimatesize(DRMS_Env_t *env, char *series);

/* Retrieve the record with record number given in "recnum" from the series
   given in "seriesname". */
DRMS_Record_t *drms_retrieve_record(DRMS_Env_t *env, const char *seriesname, 
				    long long recnum, int *status);
/* Retrieve the records from the series given in "seriesname" satisfying
   the condition in the string "where". "where" must be a valid SQL
   clause involving the column names of the main record table for the 
   series. */
DRMS_RecordSet_t *drms_retrieve_records(DRMS_Env_t *env, 
					const char *seriesname, char *where, 
					int filter, int mixed, int *status);
/* Insert multiple records in the database using the 
   fast bulk insert interface. */
int drms_insert_records(DRMS_RecordSet_t *recset);
/* Get a pointer to the template record structure for series. */
DRMS_Record_t *drms_template_record(DRMS_Env_t *env, const char *seriesname, int *status);
/* Populate a record structure with the meta-data for record number "recnum"
   from its series (given by record->seriesinfo.seriesname). */
int drms_populate_record(DRMS_Record_t *record, long long recnum);
/* Populate multiple records with the meta-data values returned 
   from a database query. */
int drms_populate_records( DRMS_RecordSet_t *rs, DB_Binary_Result_t *qres);
/* Allocate a new record structure and assign it the record number
   recnum and insert it in the record cache. */
DRMS_Record_t *drms_alloc_record(DRMS_Env_t *env, const char *series, 
				 long long recnum, int *status);
/* Allocate a new record structure based on a template, assign it 
   the record number recnum and insert it in the record cache. */
DRMS_Record_t *drms_alloc_record2(DRMS_Record_t *template,
				  long long recnum, int *status);

/* Generate a string containing a comma-separated list column
   names of the database table containing the meta-data of the 
   given record. Return the number of columns in num_args. */
char *drms_field_list(DRMS_Record_t *rec, int *num_args);
/* Deep copy the contents of a record structure. */
void drms_copy_record_struct(DRMS_Record_t *dst, DRMS_Record_t *src);
/* Deep free the contents of a record structure. */
void drms_free_record_struct(DRMS_Record_t *rec);
void drms_free_template_record_struct(DRMS_Record_t *rec);

/* Estimate how much memory is used per record. */
long long drms_record_memsize(DRMS_Record_t *rec);

/* Return the JSOC software version that created the given record. */
char *drms_record_jsoc_version(DRMS_Env_t *env, DRMS_Record_t *rec);

#endif
