/* drms_keywordmap.h */

#ifndef _DRMS_KEYMAP_INTERNAL_H
#define _DRMS_KEYMAP_INTERNAL_H

/* Ultimately, the Rules.mk files will need to be modified so that anything 
 * that uses the drms API has DRMSPROGRAM set to 1.  That way they won't
 * be able to access the internal API, even if they accidentally #include
 * the header.
 */
#undef DRMSPROGRAM
#ifndef DRMSPROGRAM
/* Internal functions */

/* Get external/internal keyword name. Modules/programs should use
 * drms_keyword keywords to convert between internal DRMS keywords
 * and external FITS-file keywords, and vice versa.
*/
const char *drms_keymap_extname(DRMS_KeyMap_t *keymap, const char *intName);
const char *drms_keymap_classidextname(DRMS_KeyMapClass_t, const char *intName);
const char *drms_keymap_classextname(const char *class, const char *intName);
const char *drms_keymap_intname(DRMS_KeyMap_t *keymap, const char *extName);
const char *drms_keymap_classidintname(DRMS_KeyMapClass_t, const char *extName);
const char *drms_keymap_classintname(const char *class, const char *extName);

#endif /* DRMSPROGRAM */
#endif /* _DRMS_KEYMAP_INTERNAL_H */
