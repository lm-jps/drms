#ifndef _DRMS_RECORD_H
#define _DRMS_RECORD_H

#include "drms_types.h"
#include "db.h"

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


/************** User level record functions ************/

/**** For record sets. ****/
/* Retrieve a recordset specified by the DRMS dataset name string
   given in the argument "datasetname". The records are inserted into
   the record cache and marked read-only. */
DRMS_RecordSet_t *drms_open_records(DRMS_Env_t *env, char *recordsetname, 
				    int *status);
DRMS_RecordSet_t *drms_open_localrecords(DRMS_Env_t *env, 
					 const char *dsRecSet, 
					 int *status);
DRMS_RecordSet_t *drms_open_dsdsrecords(DRMS_Env_t *env, 
					const char *dsRecSet, 
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

/**** Can modify seriesinfo only if the record is a record prototype  ****/
int drms_recproto_setseriesinfo(DRMS_Record_t *rec,
				int *unitSize, 
				int *bArchive, 
				int *nDaysRetention,
				int *tapeGroup,
				const char *description);

DRMS_RecordQueryType_t drms_record_getquerytype(const char *query);

/* Estimate how much memory is used per record. */
long long drms_record_memsize(DRMS_Record_t *rec);

/* Return the JSOC software version that created the given record. */
char *drms_record_jsoc_version(DRMS_Env_t *env, DRMS_Record_t *rec);

#endif
