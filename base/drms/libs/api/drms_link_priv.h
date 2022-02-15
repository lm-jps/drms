/**
\file drms_link_priv.h
*/
#ifndef _DRMS_LINK_PRIV_H
#define _DRMS_LINK_PRIV_H

#include "drms_types.h"

void drms_free_template_link_struct(DRMS_Link_t *link);
void drms_free_link_struct(DRMS_Link_t *link);
void drms_copy_link_struct(DRMS_Link_t *dst, DRMS_Link_t *src);

/** \brief Create stand-alone links that contain pointers to/from target only. */
HContainer_t *drms_create_link_prototypes(DRMS_Record_t *target,
					  DRMS_Record_t *source,
					  int *status);

int drms_template_links(DRMS_Record_t *template);

#endif
