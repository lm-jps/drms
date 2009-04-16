/* drms_keymap_priv.h */

#ifndef _DRMS_KEYMAP_PRIV_H
#define _DRMS_KEYMAP_PRIV_H

/* Ultimately, the Rules.mk files will need to be modified so that anything 
 * that uses the drms API has EXTERNALCODE set to 1.  That way they won't
 * be able to access the internal API, even if they accidentally #include
 * the header.
 */
#undef EXTERNALCODE /* XXX - fix this */
#ifndef EXTERNALCODE
/* Internal functions */
/* These should be cleaned up by DRMS, not by jsoc_main */
int drms_keymap_init(void);
void drms_keymap_term(void);

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

#endif /* EXTERNALCODE */
#endif /* _DRMS_KEYMAP_PRIV_H */
