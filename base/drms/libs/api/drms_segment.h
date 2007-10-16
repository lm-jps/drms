#ifndef _DRMS_SEGMENT_H
#define _DRMS_SEGMENT_H

#include "drms_types.h"

/********************************************************/
/********                                        ********/
/******** Functions handling segment structures  ********/
/********                                        ********/
/********************************************************/

/**** Internal functions (not for modules) ****/

/* Deep free a segment structure. */
void drms_free_template_segment_struct(DRMS_Segment_t *segment);
void drms_free_segment_struct(DRMS_Segment_t *segment);

/* Deep copy a segment structure. */
void drms_copy_segment_struct(DRMS_Segment_t *dst, DRMS_Segment_t *src);

/* Construct segment part of a series template record. */
int drms_template_segments(DRMS_Record_t *template);


/**** External functions (for modules) ****/

/* Look up a segment belonging to record by name. */
DRMS_Segment_t *drms_segment_lookup(DRMS_Record_t *record, const char *segname);

/* Look up a segment belonging to record by number. */
DRMS_Segment_t *drms_segment_lookupnum(DRMS_Record_t *record, int segnum);

#define name2seg(rec, name) drms_segment_lookup(rec, name)
#define num2seg(rec, num) drms_segment_lookupnum(rec, num)

/* Create stand-alone segments that contain pointers to/from target only. */
HContainer_t *drms_create_segment_prototypes(DRMS_Record_t *target, 
					     DRMS_Record_t *source, 
					     int *status);

/* Print contents of segment structure. */
void drms_segment_print(DRMS_Segment_t *seg);

/* Estimated size of segment in bytes. */
long long drms_segment_size(DRMS_Segment_t *seg, int *status);

/* Can modify segment dims only if the containing record is a record prototype */
int drms_segment_setdims(DRMS_Segment_t *seg, DRMS_SegmentDimInfo_t *di);

/* Get the record's segment axis dimensions. */
int drms_segment_getdims(DRMS_Segment_t *seg, DRMS_SegmentDimInfo_t *di);

/* Return a container of DRMS_SegmentInfo_t structs.  Caller owns the returned container. */
HContainer_t *drms_segment_createinfocon(DRMS_Env_t *drmsEnv, 
					 const char *seriesName, 
					 int *status);
void drms_segment_destroyinfocon(HContainer_t **info);

/* Return absolute path to segment file in filename.
   filename must be able the hold at DRMS_MAXPATHLEN bytes. */
void drms_segment_filename(DRMS_Segment_t *seg, char *filename);

/* Delete file associated with segment. */
int drms_delete_segmentfile(DRMS_Segment_t *seg);

/* Set block sizes for tiled/blocked storage. */
void drms_segment_setblocksize(DRMS_Segment_t *seg, int *blksz);

/* Get block sizes for tiled/blocked storage. */
void drms_segment_getblocksize(DRMS_Segment_t *seg, int *blksz);

/* Set segment scaling. Can only be done when creating a new segment. */
int drms_segment_setscaling(DRMS_Segment_t *seg, double bzero, double bscale);

/* Set segment scaling. Can only be done when creating a new segment. */
int drms_segment_getscaling(DRMS_Segment_t *seg, double *bzero, double *bscale);

/* Set scaling for segment to accomodate data from the given array. */
void drms_segment_autoscale(DRMS_Segment_t *seg, DRMS_Array_t *array);


/********************************************************/
/********                                        ********/
/********    Functions handling segment data     ********/
/********                                        ********/
/********************************************************/

/**** Internal functions (not for modules) ****/

/* None exclusively for internal use only. */


/**** External functions (for modules) ****/

/* Open a data segment. 

   a) If the corresponding data file exists, read the 
   entire data array into memory. Convert it to the type given as 
   argument. If type=DRMS_TYPE_RAW then  the data is 
   read into an array of the same type it is stored as on disk.
   b) If the data file does not exist, then return a data array filed with 
   the MISSING value for the given type.
*/  
DRMS_Array_t *drms_segment_read(DRMS_Segment_t *seg, DRMS_Type_t type, 
				int *status);

DRMS_Array_t *drms_segment_readslice(DRMS_Segment_t *seg, DRMS_Type_t type, 
				     int *start, int *end, int *status);

/* Write the array argument to the file occupied by the
   segment argument. The array dimension and type must match the
   segment dimension and type. */
int drms_segment_write(DRMS_Segment_t *seg, DRMS_Array_t *arr, int autoscale);

/* Write a file specified by filename argument to the file occupied by
   the segment argument. The filename of the segment is set.
 */
int drms_segment_write_from_file(DRMS_Segment_t *seg, char *infile);

/* Returns 1 if the segments' meta-data match */
int drms_segment_segsmatch(const DRMS_Segment_t *s1, const DRMS_Segment_t *s2);


/******** Functions to handle exporting FITS segments  **********/
/* Maps to external keywords in this order.  If an item does not result in a valid
 * FITS keyword, then the next item is consulted.
 *   1. Name in keyword description.
 *   2. DRMS name.
 *   3. Name generated by default rule to convert from DRMS name to FITS name. */
int drms_segment_export(DRMS_Segment_t *seg);

/* Maps to external keywords in this order.  If an item does not result in a valid
 * FITS keyword, then the next item is consulted.
 *   1. if (map != NULL), map DRMS name to external name using map.
 *   2. if (class != NULL), use default rule associated with class to map to external name.
 *   3. Name in keyword description.
 *   4. DRMS name.
 *   5. Name generated by default rule to convert from DRMS name to FITS name. */
int drms_segment_export_ext(DRMS_Segment_t *seg, 
			    DRMS_KeyMapClass_t *class, 
			    DRMS_KeyMap_t *map);

#endif


