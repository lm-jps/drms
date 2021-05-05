/* valid processing steps */
PROCESSING_STEP_DATA(to_ptr)  /* 1.0 */
PROCESSING_STEP_DATA(aia_scale)  /* 1.0 */
PROCESSING_STEP_DATA(rebin) /* depends on scale rebin argument */
PROCESSING_STEP_DATA(resize)  /* depends on scale_to resize argument, CDELT1 keyword value */
PROCESSING_STEP_DATA(im_patch) /* depends on width/height im_patch arguments, first record segment dims */
PROCESSING_STEP_DATA(map_proj) /* depends on cols/rows export_as_maproj arguments, first record segment dims */
