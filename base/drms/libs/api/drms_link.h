/**
\file drms_link.h
*/
#ifndef _DRMS_LINK_H
#define _DRMS_LINK_H

#include "drms_types.h"


/******** Link functions ********/

/** \brief Follow a link to its destination record, retrieve it and return a 
   pointer to it. */
DRMS_Record_t *drms_link_follow(DRMS_Record_t *rec, const char *linkname, 
				int *status);
/** \brief Return all records pointed to by a named link in the given
record.  For dynamic links all records with the matching primary index
value are returned, not only the latest one. */
DRMS_RecordSet_t *drms_link_followall(DRMS_Record_t *rec, const char *linkname,
				      int *status);
void drms_link_print(DRMS_Link_t *link);
void drms_link_fprint(FILE *linkfile, DRMS_Link_t *link);
void drms_free_template_link_struct(DRMS_Link_t *link);
void drms_free_link_struct(DRMS_Link_t *link);
void drms_copy_link_struct(DRMS_Link_t *dst, DRMS_Link_t *src);

/** \brief Create stand-alone links that contain pointers to/from target only. */
HContainer_t *drms_create_link_prototypes(DRMS_Record_t *target, 
					  DRMS_Record_t *source, 
					  int *status);

int  drms_template_links(DRMS_Record_t *template);

/** \brief For each link in the record: Get the names and types for
the primary index keywords of the target series if it is a dynamic
link.*/
void drms_link_getpidx(DRMS_Record_t *rec);
/** \brief Set static link */
int drms_setlink_static(DRMS_Record_t *rec, const char *linkname, 
			long long recnum);
/** \brief Set dynamic link */
int drms_setlink_dynamic(DRMS_Record_t *rec, const char *linkname, 
			 DRMS_Type_t *types, DRMS_Type_Value_t *values);

#endif
