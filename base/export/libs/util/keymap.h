/* keymap.h */

/**
\file keymap.h
\brief Functions to create and free ::KeyMap_t structures.
\example drms_keymap_ex1.c
*/

#include "hcontainer.h"

#ifndef _EXPUTL_KEYMAP_H
#define _EXPUTL_KEYMAP_H



/* Begin API */
/************ Exporting FITS files ***************/
typedef enum Exputl_KeyMapClass_enum {
   kKEYMAPCLASS_DEFAULT = 0,
   kKEYMAPCLASS_DSDS = 1,
   kKEYMAPCLASS_LOCAL = 2,
   kKEYMAPCLASS_SSW = 3,
   kKEYMAPCLASS_GNG = 4,
   kKEYMAPCLASS_NUMTABLESPLUSONE
   /* xxx etc*/
} Exputl_KeyMapClass_t;

/** \brief Exputl keymap struct */
struct Exputl_KeyMap_struct {
  HContainer_t int2ext;
  HContainer_t ext2int;
}; 

/** \brief EXPUTL keymap struct reference */
typedef struct Exputl_KeyMap_struct Exputl_KeyMap_t;

/* These should be cleaned up by DRMS, not by jsoc_main */
int exputl_keymap_init(void);
void exputl_keymap_term(void *data);

/* Get external/internal keyword name. Modules/programs should use
 * drms_keyword keywords to convert between internal DRMS keywords
 * and external FITS-file keywords, and vice versa.
*/
const char *exputl_keymap_extname(Exputl_KeyMap_t *keymap, const char *intName);
const char *exputl_keymap_classidextname(Exputl_KeyMapClass_t, const char *intName);
const char *exputl_keymap_classextname(const char *class, const char *intName);
const char *exputl_keymap_intname(Exputl_KeyMap_t *keymap, const char *extName);
const char *exputl_keymap_classidintname(Exputl_KeyMapClass_t, const char *extName);
const char *exputl_keymap_classintname(const char *class, const char *extName);

/* Functions to create and free EXPUTL keymaps. */

/* Functions to create and free the global class EXPUTL keymaps */
Exputl_KeyMap_t *exputl_keymap_create(void);
void exputl_keymap_destroy(Exputl_KeyMap_t **km);
int exputl_keymap_parsetable(Exputl_KeyMap_t *keymap, const char *text);
int exputl_keymap_parsefile(Exputl_KeyMap_t *keymap, FILE *fPtr);
const char *exputl_keymap_getclname(Exputl_KeyMapClass_t clid);


/* End API */

/* Doxygen function documentation */

/**
   @addtogroup keymap_api
   @{
*/

/** 
    @fn Exputl_KeyMap_t *exputl_keymap_create(void)
    Allocate an empty ::Exputl_KeyMap_t and return a pointer to it if 
    memory was successfully allocated. It is the caller's responsiblity to free the returned
    ::Exputl_KeyMap_t by calling ::exputl_keymap_destroy.

    \return Pointer to a newly allocated ::Exputl_KeyMap_t structure.
*/

/**
   @fn void exputl_keymap_destroy(Exputl_KeyMap_t **km)
   Free all allocated memory associated with a ::Exputl_KeyMap_t structure. 
   The ::Exputl_KeyMap_t pointer contained in \a km is set to NULL.

   \param km Pointer to a pointer to the ::Exputl_KeyMap_t structure being freed.
*/

/**
   @fn int exputl_keymap_parsetable(Exputl_KeyMap_t *keymap, const char *text)
   blah blah
*/

/**
   @fn int exputl_keymap_parsefile(Exputl_KeyMap_t *keymap, FILE *fPtr);
   Parse a buffer containing FITS-keyword-name-to-EXPUTL-keyword-name
   mappings. The buffer, \a text, can contain zero or more mappings, each of the form 
   \e fitsname(\e whitespace | ',')\e exputlname. Each pair of mappings must be 
   separated by a newline character ('\\n'). Comments or emtpy strings may appear 
   between newline characters as well. If \a keymap is not NULL, the resulting 
   set of mappings is used to initialize \a keymap.

   \param keymap Pointer to an existing ::Exputl_KeyMap_t structure. Upon success, this structure
   will be initialzed with keyword pairs specified in \a text.
   \param text Buffer containing keyword mappings.
   \return 1 if successful, 0 otherwise
*/

/**
   @}
*/

#endif /* _EXPUTL_KEYMAP_H */
