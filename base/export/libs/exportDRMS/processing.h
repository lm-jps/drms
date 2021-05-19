#ifdef PROCESSING_STEP_DATA
    /* valid processing steps, in order originally defined by the export web app */
    PROCESSING_STEP_DATA(aia_scale_orig, aia_scale_orig)  /* 1.0 */
    PROCESSING_STEP_DATA(aia_scale_aialev1, aia_scale_aialev1)  /* 1.0 */
    PROCESSING_STEP_DATA(aia_scale_other, aia_scale_other)  /* 1.0 */
    PROCESSING_STEP_DATA(to_ptr, HmiB2ptr)  /* 1.0 */
    PROCESSING_STEP_DATA(resize, resize)  /* depends on scale_to resize argument, CDELT1 keyword value */
    PROCESSING_STEP_DATA(im_patch, im_patch) /* depends on width/height im_patch arguments, first record segment dims */
    PROCESSING_STEP_DATA(map_proj, Maproj) /* depends on cols/rows export_as_maproj arguments, first record segment dims */
    PROCESSING_STEP_DATA(rebin, rebin) /* depends on scale rebin argument */
#else
    #ifndef _PROCESSING_H
    #define _PROCESSING_H

    #include "drms.h"

    #define PROCESSING_STEP_NAME_LEN 32
    #define PROCESSING_STEP_PROPERTY_NAME_LEN 32
    #define PROCESSING_STEP_PROPERTY_VALUE_LEN 64
    #define PROCESSING_HANDLER_ARGUMENT_NAME_LEN 16
    #define PROCESSING_HANDLER_ARGUMENT_VALUE_LEN 32

    /* processing argument processing-step list node */
    struct _processing_step_node_data_
    {
        char step[PROCESSING_STEP_NAME_LEN];
        HContainer_t arguments;
    };

    int get_processing_step_index(const char *step);
    int get_processing_list(const char *processing_json, LinkedList_t **processing_list);
    float get_processing_size_ratio(struct _processing_step_node_data_ *node_data, DRMS_Segment_t *segment);

    #endif // _PROCESSING_H
#endif // PROCESSING_STEP_DATA
