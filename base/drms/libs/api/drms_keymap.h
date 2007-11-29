/* drms_keywordmap.h */

#ifndef _DRMS_KEYMAP_H
#define _DRMS_KEYMAP_H

/* Begin API */

/* Functions to create and free DRMS keymaps. */
DRMS_KeyMap_t *drms_keymap_create(void);
void drms_keymap_destroy(DRMS_KeyMap_t **km);
int drms_keymap_parsetable(DRMS_KeyMap_t *keymap, const char *text);
int drms_keymap_parsefile(DRMS_KeyMap_t *keymap, FILE *fPtr);

/* Functions to create and free the global class DRMS keymaps */
int drms_keymap_init(void);
void drms_keymap_term(void);
/* End API */

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

#endif /* _DRMS_KEYMAP_H */
