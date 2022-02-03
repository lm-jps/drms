/**
\file drms_link.h
*/
#ifndef _DRMS_LINK_H
#define _DRMS_LINK_H

#include "drms_types.h"
#include "drms.h"

/******** Link functions ********/

DRMS_Record_t *drms_link_follow(DRMS_Record_t *rec, const char *linkname,
				int *status);

LinkedList_t *drms_link_follow_recordset(DRMS_Env_t *env, DRMS_Record_t *template_record, LinkedList_t *record_list, const char *link, HContainer_t *link_map, int *status);

void drms_link_print(DRMS_Link_t *link);
void drms_link_fprint(FILE *linkfile, DRMS_Link_t *link);
int drms_link_getpidx(DRMS_Record_t *rec);
int drms_setlink_static(DRMS_Record_t *rec, const char *linkname,
			long long recnum);
int drms_setlink_dynamic(DRMS_Record_t *rec, const char *linkname,
			 DRMS_Type_t *types, DRMS_Type_Value_t *values);
int drms_link_set(const char *linkname, DRMS_Record_t *baserec, DRMS_Record_t *supplementingrec);


static inline int drms_link_ranksort(const void *he1, const void *he2)
{
   DRMS_Link_t *l1 = (DRMS_Link_t *)hcon_getval(*((HContainerElement_t **)he1));
   DRMS_Link_t *l2 = (DRMS_Link_t *)hcon_getval(*((HContainerElement_t **)he2));

   XASSERT(l1 && l2);

   return (l1->info->rank < l2->info->rank) ? -1 : (l1->info->rank > l2->info->rank ? 1 : 0);
}

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
    @fn int drms_link_getpidx(DRMS_Record_t *rec)
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
   @fn int int drms_link_set(const char *linkname, DRMS_Record_t *baserec, DRMS_Record_t *supplementingrec)
   Wrapper function to facilitate the setting of links between the record of a base series and the record
   of a series that will provide supplementary information. In order for linked keywords and segments
   to be visible from a record from the
   series containing the link, the link must be resolved. This function establishes this connection
   between a single record in the series containing the link, and a single record in the supplementary series.
   After this link has been established, code that has a handle to the @a baserec record has access
   to keyword values and segments in the @a supplementingrec record. There are two types of links - dynamic links
   (::DYNAMIC_LINK) and static links (::STATIC_LINK). This code will set either kind. The @a linkname parameter
   is used to locate a link structure, which then indicates which type the link is.

   @param linkname Name of the link described by the series definition that will be resolved
   @param baserec The base record from the series that contains the link to the supplementary series
   @param supplementingrec The record from the linked series that will provide keywords or segments
   that will be visible from the base record
   @return A DRMS error code (as defined in drms_statuscodes.h). Possible errors include DRMS_ERROR_UNKNOWNLINK
   (if linkname is not a known link) and, more generally, DRMS_ERROR_INVALIDDATA (if a parameter's value
   is not expected).

*/

/**
   @}
*/

#endif
