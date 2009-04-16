/**
   @file drms_segment_priv.h
*/

#ifndef _DRMS_SEGMENT_PRIV_H
#define _DRMS_SEGMENT_PRIV_H

#include "drms_types.h"

/**
   @name Create and Destroy
*/
/* @{ */
/* Construct segment part of a series template record. */
/**
   Builds the segment part of a dataseries record template from
   the record @a template by querying the database and using ther results to
   initialize the array of segment descriptors.

   @param template DRMS dataseries record template 
   (which resides in @c DRMS_Env_t::series_cache)
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_template_segments(DRMS_Record_t *template);

/* Delete file associated with segment. */
/**
   Removes the file storing the data associated with @a seg.

   @param seg DRMS segment whose associated data file will be deleted.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_delete_segmentfile(DRMS_Segment_t *seg);

/**
   Shallow-free a DRMS segment structure. For some reason, this is a no-op, 
   unless @a segment is NULL, in which case an error is generated.
   
   @param segment The DRMS segment to free.
*/
void drms_free_segment_struct(DRMS_Segment_t *segment);
/* Deep free a segment structure. */
/**
   Deep-free a DRMS segment structure. This function does NOT free the
   actual ::DRMS_Segment_t. It only frees the allocated memory to which
   it has pointers.

   @param segment The DRMS segment to free.
*/
void drms_free_template_segment_struct(DRMS_Segment_t *segment);
/* @} */

/**
   @name Replicate
*/
/* @{ */
/* Deep copy a segment structure. */
/** 
    Copies the entire @a src DRMS segment struct to @a dst. NOTE: 
    @a src will retain pointers to the allocated members of 
    @a src (ie., @c src->record and @c src->info).  This implies
    that BOTH @a dst and @a src will have some pointers in common, so
    care must be ensured when freeing memory.
    
    @param dst DRMS segment struct whose fields will be initialized
    by the fields of @a src.
    @param src DRMS segment struct whose fields will be copied.
*/
void drms_copy_segment_struct(DRMS_Segment_t *dst, DRMS_Segment_t *src);
/* @} */

CFITSIO_KEYWORD *drms_segment_mapkeys(DRMS_Segment_t *seg, 
                                      const char *clname, 
                                      const char *mapfile, 
                                      int *status);

void drms_segment_freekeys(CFITSIO_KEYWORD **fitskeys);

#endif /* _DRMS_SEGMENT_PRIV_H */
