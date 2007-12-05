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

#endif /* _DRMS_KEYMAP_H */
