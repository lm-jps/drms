/**
\file drms_link.h
*/
#ifndef _DRMS_LINK_H
#define _DRMS_LINK_H

#include "drms_types.h"


/******** Link functions ********/

DRMS_Record_t *drms_link_follow(DRMS_Record_t *rec, const char *linkname, 
				int *status);
DRMS_RecordSet_t *drms_link_followall(DRMS_Record_t *rec, const char *linkname,
				      int *status);
void drms_link_print(DRMS_Link_t *link);
void drms_link_fprint(FILE *linkfile, DRMS_Link_t *link);
void drms_link_getpidx(DRMS_Record_t *rec);
int drms_setlink_static(DRMS_Record_t *rec, const char *linkname, 
			long long recnum);
int drms_setlink_dynamic(DRMS_Record_t *rec, const char *linkname, 
			 DRMS_Type_t *types, DRMS_Type_Value_t *values);

/* doxygen documentation */

/**
   @addtogroup link_api
   @{
*/

/** 
    @fn DRMS_Record_t *drms_link_follow(DRMS_Record_t *rec, const char *linkname, int *status)
    Follow a link to its destination record, retrieve it and return a 
    pointer to it. 
*/

/** 
    @fn DRMS_RecordSet_t *drms_link_followall(DRMS_Record_t *rec, const char *linkname, int *status)
    Return all records pointed to by a named link in the given
    record.  For dynamic links all records with the matching primary index
    value are returned, not only the latest one. 
*/

/**
   @fn void drms_link_print(DRMS_Link_t *link)
   blah blah
*/

/**
   @fn void drms_link_fprint(FILE *linkfile, DRMS_Link_t *link)
   blah blah
*/

/** 
    @fn void drms_link_getpidx(DRMS_Record_t *rec)
    For each link in the record: Get the names and types for
    the primary index keywords of the target series if it is a dynamic
    link.
*/

/**
   @fn int drms_setlink_static(DRMS_Record_t *rec, const char *linkname, long long recnum)
   Set static link 
*/

/**
   @fn int drms_setlink_dynamic(DRMS_Record_t *rec, const char *linkname, DRMS_Type_t *types, DRMS_Type_Value_t *values)
   Set dynamic link 
*/

/**
   @}
*/

#endif
