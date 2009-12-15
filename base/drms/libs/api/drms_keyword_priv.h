/**
\file drms_keyword_priv.h
\brief Functions to access DRMS keyword values, and to convert DRMS keywords to FITS keywords and vice versa.
*/

#ifndef _DRMS_KEYWORD_PRIV_H
#define _DRMS_KEYWORD_PRIV_H


void drms_keyword_term();

/******** Keyword functions ********/

void drms_free_template_keyword_struct(DRMS_Keyword_t *key);
void drms_free_keyword_struct(DRMS_Keyword_t *key);
void drms_copy_keyword_struct(DRMS_Keyword_t *dst, DRMS_Keyword_t *src);

/* Create stand-alone links that contain pointers to/from target only. */
HContainer_t *drms_create_keyword_prototypes(DRMS_Record_t *target, 
					     DRMS_Record_t *source, 
					     int *status);


int drms_template_keywords(DRMS_Record_t *template);
int drms_template_keywords_int(DRMS_Record_t *template, int expandperseg, const char *cols);

DRMS_Keyword_t *drms_keyword_indexfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_epochfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_basefromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_stepfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_unitfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_roundfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_slotfromindex(DRMS_Keyword_t *indx);

static inline long long CalcSlot(double slotkeyval, 
				 double base, 
				 double stepsecs,
                                 double roundstep)
{
   double slotvald = floor((slotkeyval - base + (roundstep / 2.0)) / stepsecs);
   return slotvald;
}

#endif
