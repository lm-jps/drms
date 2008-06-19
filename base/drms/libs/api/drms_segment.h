/**
   @file drms_segment.h
   @brief Functions to access DRMS segment data structures
   @sa drms_record.h drms_keyword.h drms_link.h drms_array.h
*/


#ifndef _DRMS_SEGMENT_H
#define _DRMS_SEGMENT_H

#include "drms_types.h"

/********************************************************/
/********                                        ********/
/******** Functions handling segment structures  ********/
/********                                        ********/
/********************************************************/



/**** External functions (for modules) ****/
/**
   Synonym for ::drms_segment_lookup.
*/
#define name2seg(rec, name) drms_segment_lookup(rec, name)
/**
   Synonym for ::drms_segment_lookupnum.
*/
#define num2seg(rec, num) drms_segment_lookupnum(rec, num)

/**
   @name Lookup
*/
/* @{ */
/* Look up a segment belonging to record by name. */
/**
   Returns the segment associated with the name
   @a segname in record @a record. If the segment refers to a constant
   segment that has not yet been set, then the segment for the current
   record @a record is returned; otherwise, the constant segment is returned.

   @param record The DRMS record that contains the DRMS segment being looked up.
   @param segname The name of the DRMS segment being looked up.
   @return The DRMS segment specified by @a record and @a segname.
*/
DRMS_Segment_t *drms_segment_lookup(DRMS_Record_t *record, const char *segname);

/* Look up a segment belonging to record by number. */
/**
   Returns the segment associated with the number
   @a segnum in record @a record. For constant segments, it behaves the same
   as ::drms_segment_lookup.

   @param record The DRMS record that contains the DRMS segment being looked up.
   @param segnum The number of the DRMS segment being looked up.
   @return The DRMS segment specified by @a record and @a segnum.
*/
DRMS_Segment_t *drms_segment_lookupnum(DRMS_Record_t *record, int segnum);
/* @} */

/**
   @name Create and Destroy
*/
/* @{ */
/* Create stand-alone segments that contain pointers to/from target only. */
/**
   A DRMS segment prototype is a DRMS segment to which a DRMS record prototype
   is linked. Creates DRMS segment prototypes (type ::DRMS_Segment_t)
   for the target record prototype (@a target), using @a source as a template.
   The main difference between the DRMS segments in @a source and those in @a target
   is that @a source could be a record that is a series record template 
   (@c DRMS_Env_t.series_cache) or a member of the record cache 
   (@c DRMS_Env_t.record_cache), but @a target cannot be either of those
   types of records.
   
   @param target Output DRMS record prototype which will contain the 
   created DRMS segment prototypes.
   @param source Input DRMS record, which contains source DRMS segments
   that will be used to initialize the created DRMS segment prototypes.
   @return Container pointing to the segment container of @a target.
*/
HContainer_t *drms_create_segment_prototypes(DRMS_Record_t *target, 
					     DRMS_Record_t *source, 
					     int *status);

/* Return a container of DRMS_SegmentInfo_t structs.  Caller owns the returned container. */
/**
   Creates an ::HContainer_t which contains ::DRMS_SegmentInfo_t structs.
   There is one such struct for each DRMS template segment (which is a template
   for series @a seriesName), with the former being a copy of the latter.  The purpose
   of this function is to provide the user a container with segment information about
   pertinent to all of @a seriesName's segments.

   @param drmsEnv  DRMS session information.
   @param seriesName The name of the series whose segment information is being copied.
   @param status DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
   @return A created HContainer_t which contains information about all of series
   @a seriesName's segments. Caller is responsible for freeing the created
   ::HContainer_t by calling ::drms_segment_destroyinfocon.
*/
HContainer_t *drms_segment_createinfocon(DRMS_Env_t *drmsEnv, 
					 const char *seriesName, 
					 int *status);

/**
   Frees all memory allocated in the creation of @a info. To be used ONLY on
   ::HContainer_t structures created with ::drms_segment_createinfocon.

   @param info Pointer to a pointer to the ::HContainer_t struct that contains
   series-specific segment information.
*/
void drms_segment_destroyinfocon(HContainer_t **info);
/* @} */


/**
   @name Information and Diagnostics
*/
/* @{ */
/* Print contents of segment structure. */
/**
   Prints the full @a seg struct information to stdout.

   @param seg DRMS segment struct whose fields will be printed to stdout.
*/
void drms_segment_print(DRMS_Segment_t *seg);
/**
   Prints the full @a seg struct information to @a segfile

   @param seg DRMS segment struct whose fields will be printed to @a segfile
   @param segfile
*/
void drms_segment_fprint(FILE *segfile, DRMS_Segment_t *seg);

/* Return absolute path to segment file in filename.
   filename must be able the hold at DRMS_MAXPATHLEN bytes. */
/**
   Returns the absolute path to the segment file
   associated with @a seg in @a filename. The size of the buffer to which @a filename
   points must be at least DRMS_MAXPATHLEN+1 bytes long.

   @param seg DRMS segment whose associated file's name will be returned.
   @param filename Buffer to hold the filename upon return.
*/
void drms_segment_filename(DRMS_Segment_t *seg, char *filename);

/* Estimated size of segment in bytes. */
/**
   Returns the total size of the @a seg data array, in bytes
   (product of the number of data elements and size of the datatype). 
   If the segment data type is ::DRMS_TYPE_STRING, the size returned 
   is only the number of data elements times the size of an address.
   So, for the ::DRMS_TYPE_STRING datatype, this function
   probably does not provide the desired information.

   @param seg DRMS segment whose data size is being estimated.
   @param status DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
   @return The esimated data size, in bytes.
*/
long long drms_segment_size(DRMS_Segment_t *seg, int *status);

/* Returns 1 if the segments' meta-data match */
/** 
    Returns 1 if both segments exist (or are NULL) and have the same rank, 
    dimensions, protocol, blocksizes (if they are of
    protocol ::DRMS_TAS), type, and scope; 0 otherwise.

    @param s1 DRMS segment whose information is being compared.
    @param s2 DRMS segment whose information is being compared.
    @return 1 if @a s1 and @a s2 match; 0 otherwise.
*/
int drms_segment_segsmatch(const DRMS_Segment_t *s1, const DRMS_Segment_t *s2);
/* @} */




/**
   @name Manipulate Axes
*/
/* @{ */
/* Can modify segment dims only if the containing record is a record prototype */
/**
   Sets the rank and axis lengths of @a seg to
   those of the @a di struct.

   @param seg DRMS segment struct whose fields will be set.
   @param di Segment information used to initialized @a seg.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_segment_setdims(DRMS_Segment_t *seg, DRMS_SegmentDimInfo_t *di);

/* Get the record's segment axis dimensions. */
/**
   Sets the values of the @a di struct to the
   rank and axis lengths of @a seg.

   @param seg DRMS segment whose information is used to initialize
   @a di.
   @param di Receives, by reference, the segment information in @a seg.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_segment_getdims(DRMS_Segment_t *seg, DRMS_SegmentDimInfo_t *di);
/* @} */


/**
   @name Scaling and Blocksize
*/
/* @{ */
/* Set scaling for segment to accomodate data from the given array. */
/**
   Sets the scaling parameters in the
   @c seg->record keyword (see ::drms_segment_setscaling),
   to values based on the extrema of the data @a array. The scaling
   parameters are set so that the unscaled data would occupy the full range
   of the segment fixed-point type; for example the maximum value of the array
   would be represented by 127 and the minimum by -128 in the segment data file
   if the type were ::DRMS_TYPE_CHAR. The scaling parameters will only be changed,
   however, if the original data would not otherwise fit unscaled into the
   range of the segment data type. Setting the scaling parameters for segments
   of floating-point type is probably a bad idea, but is done anyway, without
   regard to range. For other types, the scaling parameters are simply set to
   1 and 0.

   @param seg The segment whose scaling is to be retrieved.
   @param array The DRMS array whose data is to be autoscaled.
   @param autobzero bzero value to offset data by, returned by reference.
   @param autobscale bscale value to scale data by, returned by reference
*/
void drms_segment_autoscale(DRMS_Segment_t *seg, 
			    DRMS_Array_t *arr, 
			    double *autobzero, 
			    double *autobscale);

/* Set block sizes for tiled/blocked storage. */
/**
   Sets the @c seg->blocksize array to the
   array of blocksizes @a blksz.

   @param seg The segment whose blocksize array is to be set.
   @param blksz Input blocksizes.
*/
void drms_segment_setblocksize(DRMS_Segment_t *seg, int *blksz);

/* Get block sizes for tiled/blocked storage. */
/**
   Copies the @c seg->blocksize array into
   the array @a blksz. @a blksz must be dimensioned to the rank of the
   segment. The blocksize is reserved for use with segments of protocol
   ::DRMS_TAS (tiled array storage) only, but is not used in any of the
   read/write functions described here.
   
   @param seg The segment whose blocksize array is to be set.
   @param blksz Input blocksizes.
*/
void drms_segment_getblocksize(DRMS_Segment_t *seg, int *blksz);
/* @} */

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

/**
   @name Read and Write
*/
/* @{ */
/** 
    Reads the data associated with @a seg into memory
    in a newly created ::DRMS_Array_t struct, converting the data to the requested
    @a type (unless @a type = ::DRMS_TYPE_RAW, in which case the data type
    will be that of the external representation.).

    @param seg The segment whose file is to be read into memory.
    @param type The type to which the data of @seg is converted.
    @param status DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
    @return The created DRMS array struct.
*/
DRMS_Array_t *drms_segment_read(DRMS_Segment_t *seg, DRMS_Type_t type, 
				int *status);
/**
   Similar to ::drms_segment_read, except
   that only the data between the @a start[n] and @a end[n] values in each
   dimension @a n are read into the array. @a start and @a end must be
   vectors of rank equal to that of the data segment.

    @param seg The segment whose file slice is to be read into memory.
    @param type The type to which the data of @seg is converted.
    @param start The index value, in all dimensions, that starts the slice.
    @param end The index value, in all dimensions, that ends the slice.
    @return The created DRMS array struct.
*/
DRMS_Array_t *drms_segment_readslice(DRMS_Segment_t *seg, DRMS_Type_t type, 
				     int *start, int *end, int *status);

/* Write the array argument to the file occupied by the
   segment argument. The array dimension and type must match the
   segment dimension and type. */
/**
   Writes the data from the DRMS array @a arr into the
   file associated with the segment @a seg, provided that the segment uses
   one of the supported non-ready-only protocols (::DRMS_BINARY, 
   ::DRMS_BINZIP, ::DRMS_FITS, ::DRMS_FITZ, and DRMS_TAS). The array 
   dimensions must match those of the segment.
   If @a autoscale is non-zero, the function ::drms_segment_autoscale
   is invoked before output. In any case, conversion to the data type and scaling
   appropriate to the segment is performed.

   @param seg The segment whose file is to be written to the filesystem.
   @param arr The array containing the data that is to be written to the output file.
   @param autoscale If 0, do not invoke ::drms_segment_autoscale. Otherwise, invoke 
   ::drms_segment_autoscale.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_segment_write(DRMS_Segment_t *seg, DRMS_Array_t *arr, int autoscale);

/* Write a file specified by filename argument to the file occupied by
   the segment argument. The filename of the segment is set.
 */
/**
   Simply copies the contents of the file
   specified by @a infile into the file associated with @a seg. It can
   only be used for segments whose protocol is ::DRMS_GENERIC.

   @param seg The segment whose file is to be written to the filesystem.
   @param infile The input file whose contents are to be written to
   the file owned by @seg.
   @return DRMS status (see drms_statuscodes.h). 0 if successful, non-0 otherwise.
*/
int drms_segment_write_from_file(DRMS_Segment_t *seg, char *infile);
/* @} */


/******** Functions to handle exporting FITS segments  **********/
/* Maps to external keywords in this order.  If an item does not result in a valid
 * FITS keyword, then the next item is consulted.
 *   1. Name in keyword description.
 *   2. DRMS name.
 *   3. Name generated by default rule to convert from DRMS name to FITS name. */
int drms_segment_export_tofile(DRMS_Segment_t *seg, const char *fileout);

/* Maps to external keywords in this order.  If an item does not result in a valid
 * FITS keyword, then the next item is consulted.
 *   1. if (map != NULL), map DRMS name to external name using map.
 *   2. if (class != NULL), use default rule associated with class to map to external name.
 *   3. Name in keyword description.
 *   4. DRMS name.
 *   5. Name generated by default rule to convert from DRMS name to FITS name. */
int drms_segment_mapexport_tofile(DRMS_Segment_t *seg, 
				  const char *clname, 
				  const char *mapfile,
				  const char *fileout);

/* accessor functions */
static inline int drms_segment_getnaxis(DRMS_Segment_t *seg)
{
   return seg->info->naxis;
}
#endif


