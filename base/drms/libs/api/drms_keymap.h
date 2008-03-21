/* drms_keymap.h */

/**
\file drms_keymap.h
\brief Functions to create and free ::DRMS_KeyMap_t structures.
\sa drms_keyword.h
\example drms_keymap_ex1.c
*/

#ifndef _DRMS_KEYMAP_H
#define _DRMS_KEYMAP_H

/* Begin API */


/* Functions to create and free DRMS keymaps. */

/** 
Allocate an empty ::DRMS_KeyMap_t and return a pointer to it if 
memory was successfully allocated. It is the caller's responsiblity to free the returned
::DRMS_KeyMap_t by calling ::drms_keymap_destroy.

\return Pointer to a newly allocated ::DRMS_KeyMap_t structure.
*/
DRMS_KeyMap_t *drms_keymap_create(void);

/**
Free all allocated memory associated with a ::DRMS_KeyMap_t structure. 
The ::DRMS_KeyMap_t pointer contained in \a km is set to NULL.

\param km Pointer to a pointer to the ::DRMS_KeyMap_t structure being freed.
*/
void drms_keymap_destroy(DRMS_KeyMap_t **km);

/**
Parse a buffer containing FITS-keyword-name-to-DRMS-keyword-name
mappings. The buffer, \a text, can contain zero or more mappings, each of the form 
\e fitsname(\e whitespace | ',')\e drmsname. Each pair of mappings must be 
separated by a newline character ('\\n'). Comments or emtpy strings may appear 
between newline characters as well. If \a keymap is not NULL, the resulting 
set of mappings is used to initialize \a keymap.

\param keymap Pointer to an existing ::DRMS_KeyMap_t structure. Upon success, this structure
will be initialzed with keyword pairs specified in \a text.
\param text Buffer containing keyword mappings.
\return 1 if successful, 0 otherwise
*/
int drms_keymap_parsetable(DRMS_KeyMap_t *keymap, const char *text);

/**
Parse a file containing FITS-keyword-name-to-DRMS-keyword-name
mappings. \a fPtr must contain a valid file pointer. If \a keymap is not NULL, the 
resulting set of mappings is used to initialize \a keymap. This function 
calls ::drms_keymap_parsetable to parse the content of the file to which \a fPtr
refers.

\param keymap Pointer to an existing ::DRMS_KeyMap_t structure. Upon success, this structure
will be initialzed with keyword pairs specified in the content of the file to which \a fPtr 
refers.
\param fPtr File pointer referring to a file containing keyword-pair specifications.
\return 1 if successful, 0 otherwise
*/
int drms_keymap_parsefile(DRMS_KeyMap_t *keymap, FILE *fPtr);

DRMS_KeyMap_t *drms_keymap_getfromclname(const char *classname);

/* End API */

#endif /* _DRMS_KEYMAP_H */
