/*
 *  key.c					~soi/(version)/src/libast.d
 *
 *
 *  A collection of functions for manipulation of key-value lists.  The
 *    functions are:
 *
 *    void	add_keys (KEY *keys, KEY **list)
 *	add entries in one list (keys) to another list (list)
 *    void	deletekey (KEY **list, char *key)
 *	remove named element from key-list
 *    void	delete_keys (KEY *keys, KEY **list)
 *	delete entries in one list (keys) from another list (list)
 *    KEY *	findkey (KEY *list, char *key)
 *	find element in a key-list matching given key name
 *    void	freekeylist (KEY **list)	memory deallocation of keylist
 *    int	getkeytype (KEY *list, char *key)
 *	return value type associated with key name
 *    void	getkey_any (KEY **list, char *key, void *valptr)
 *	ensure that valptr references value associated with key name 
 *    RTYPE	getkey_TYPE (KEY *list, char *key)
 *	return key value as a RTYPE, where TYPE and RTYPE are associated as
 *	follows:
 *	  TYPE: int, short, ushort, byte, ubyte, flag	RTYPE:	int
 *	  TYPE: uint					RTYPE:  uint
 *	  TYPE: double, float				RTYPE:  double
 *	  TYPE: long					RTYPE:	long
 *	  TYPE: ulong					RTYPE:  ulong
 *	  TYPE: uint64_t				RTYPE:  uint64_t
 *	  TYPE: uint32_t				RTYPE:  uint32_t
 *    char *	getkey_str (KEY *list, char *key)
 *	return key value as a malloced string
 *    char *	GETKEY_str (KEY *list, char *key)
 *	return key value as a local static string - use it or lose it
 *    FILE *	getkey_fileptr (KEY *list, FILE *key)
 *	return key value 
 *    void	keyiterate (void (*action)(), KEY *list)
 *	perform an action repeatedly for all members of list
 *    KEY *	newkeylist ()		create a new, empty key-list (at NULL)
 *    void	setkey_any (KEY **list, char *key, void *val, int type)
 *	set value associated with key name 
 *    void	setkey_byte (KEY **list, char *key, char val)
 *	set char value associated with key name
 *    void	setkey_ubyte (KEY **list, char *key, unsigned char val)
 *	set unsigned char value associated with key name
 *    void	setkey_TYPE (KEY **list, char *key, TYPE val)
 *	set TYPE value associated with key name, where TYPE can be any of:
 *	  short, ushort, int, uint, long, ulong, float, double, TIME
 *    void	setkey_str (KEY **list, char *key, char *val)
 *	set string value associated with key name
 *    void	addkey_str (KEY **list, char *key, char *val)
 *	add string value associated with key name 
 *	with NO check for duplicate key
 *    void	addkey (KEY **list, char *key, void *val, int type)
 *	add value associated with key name 
 *	with NO check for duplicate key
 *
 *  There is also an internal support function key_strdup analogous to the
 *    System V strdup function.
 *
 *  Responsible:  Kay Leibrand			KLeibrand@solar.Stanford.EDU
 *
 *  Bugs:
 *    There are too many types.
 *    "byte" and "char" are confusing.
 *    getkey_float () and getkey_double () return a signaling NaN if the
 *	associated key is not present or of an inappropriate type.  Other
 *	getkey_XXX () (fixed point types) return an apparently valid value
 *	in such cases.  unsigned types return the corresponding maximum
 *	value, and signed types the minimum value, as defined in /usr/include/
 *	limits.h.  These values are machine dependent.  Error flags are set.
 *    getkey_long () will return a wrong value if the keytype is an unsigned
 *	int and longs are not longer than ints.
 *    It would be useful for getkey to return its value, so values wouldn't
 *	have to be dereferenced
 *
 *  Planned updates:
 *
 *  Revision history is at end of file.
 */

#include <soi_key.h>
#include <soi_error.h>
#include "SUM.h"

#define VERSION_NUM	(4.5)

int kludge = 2;		/* workaround for malloc / alignment problem */

KEY *newkeylist () {
   return NULL;
}

void freekeylist (KEY **list) {
   KEY *node;
   while (*list) {
      free ((*list)->name);
      free ((*list)->val);
      node = *list;
      *list = (*list)->next;
      free (node);
   }
}

KEY *findkey (KEY *list, char *key) {
  KEY *walker = list;

  soi_errno = NO_ERROR;
  if (!key) {
    soi_errno = KEY_NOT_FOUND;
    return NULL;
  }
  while (walker) {
    if (strcmp (walker->name, key)) 
      walker = walker->next;
    else 
      return walker;
  }
  soi_errno = KEY_NOT_FOUND;
  return walker;
}

char *key_strdup (char *s) {
   char *duped;

   if (!s) return NULL;
   duped = (char *)malloc (kludge*(strlen (s) + 1));
   strcpy (duped, s);
   return duped;
}

void getkey_any (KEY *list, char *key, void *valptr) {
      /*  formerly getkey - renamed to avoid conflict with TAE library */
      /*  use of specific getkey_ function (e.g.getkey_str) is recommended  */
      /*  because misuse of valptr can destroy the integrity of the keylist */
  KEY *the_one = findkey (list, key);
  if (the_one) 
    if (the_one->type == KEYTYP_STRING)
      *(char **)valptr = key_strdup(the_one->val);
    else if (the_one->type == KEYTYP_FILEP)
      /** NOTE that in this case the_one->val is of type FILE** not FILE* **/
      *(FILE **)valptr = *(FILE **)the_one->val;
    else if (the_one->type == KEYTYP_DOUBLE)
      *(double *)valptr = *(double *)the_one->val;
    else if (the_one->type == KEYTYP_FLOAT)
      *(float *)valptr = *(float *)the_one->val;
    else if (the_one->type == KEYTYP_INT)
      *(int *)valptr = *(int *)the_one->val;
    else if (the_one->type == KEYTYP_SHORT)
      *(short *)valptr = *(short *)the_one->val;
    else if (the_one->type == KEYTYP_BYTE)
      *(signed char *)valptr = *(signed char *)the_one->val;
    else if (the_one->type == KEYTYP_LONG)
      *(long *)valptr = *(long *)the_one->val;
    else if (the_one->type == KEYTYP_UINT)
      *(unsigned int *)valptr = *(unsigned int *)the_one->val;
    else if (the_one->type == KEYTYP_USHORT)
      *(unsigned short *)valptr = *(unsigned short *)the_one->val;
    else if (the_one->type == KEYTYP_UBYTE)
      *(unsigned char *)valptr = *(unsigned char *)the_one->val;
    else if (the_one->type == KEYTYP_ULONG)
      *(unsigned long *)valptr = *(unsigned long *)the_one->val;
    else if (the_one->type == KEYTYP_UINT64)
      *(uint64_t *)valptr = *(uint64_t *)the_one->val;
    else if (the_one->type == KEYTYP_UINT32)
      *(uint32_t *)valptr = *(uint32_t *)the_one->val;
    else
      soi_errno = KEY_WRONG_TYPE;
  else
    soi_errno = KEY_NOT_FOUND;
}

void setkey_any (KEY **list, char *key, void *val, int type) {
      /*  formerly setkey - renamed to avoid conflict with stdlib.h on fault */
   KEY *the_one;
   KEY *new_one;

   if (!key) return;
   if (!val) return;

   the_one = findkey (*list, key);
   soi_errno = NO_ERROR;
   if (the_one) {
      free (the_one->val); /* old value */
   }
   else {
      new_one = (KEY *)malloc (kludge*sizeof (KEY));
      new_one->next = *list;
      new_one->name = key_strdup (key);
      *list = new_one;
      the_one = new_one;
   }
   the_one->type = type;
   switch (type) {
      case KEYTYP_STRING:
         the_one->val = key_strdup (val);
         break;
      case KEYTYP_BYTE:
         the_one->val = (signed char *)malloc (kludge*1);
         *(signed char *)the_one->val = *(signed char *)val;
         break;
      case KEYTYP_UBYTE:
         the_one->val = (char *)malloc (kludge*1);
         *(unsigned char *)the_one->val = *(unsigned char *)val;
         break;
      case KEYTYP_SHORT:
         the_one->val = (short *)malloc (kludge*sizeof (short));
         *(short *)the_one->val = *(short *)val; 
         break;
      case KEYTYP_USHORT:
         the_one->val =
		(unsigned short *)malloc (kludge*sizeof (unsigned short));
         *(unsigned short *)the_one->val = *(unsigned short *)val; 
         break;
      case KEYTYP_INT:
         the_one->val = (int *)malloc (kludge*sizeof (int));
         *(int *)the_one->val = *(int *)val; 
         break;
      case KEYTYP_UINT:
         the_one->val = (unsigned int *)malloc (kludge*sizeof (unsigned int));
         *(unsigned int *)the_one->val = *(unsigned int *)val; 
         break;
      case KEYTYP_LONG:
         the_one->val = (long *)malloc (kludge*sizeof (long));
         *(long *)the_one->val = *(long *)val; 
         break;
      case KEYTYP_ULONG:
         the_one->val =
		(unsigned long *)malloc (kludge*sizeof (unsigned long));
         *(unsigned long *)the_one->val = *(unsigned long *)val; 
         break;
      case KEYTYP_UINT64:
         the_one->val = (uint64_t *)malloc (kludge*sizeof (uint64_t));
         *(uint64_t *)the_one->val = *(uint64_t *)val; 
         break;
      case KEYTYP_UINT32:
         the_one->val = (uint32_t *)malloc (kludge*sizeof (uint32_t));
         *(uint32_t *)the_one->val = *(uint32_t *)val; 
         break;
      case KEYTYP_FLOAT:
         the_one->val = (float *)malloc (kludge*sizeof (float));
         *(float *)the_one->val = *(float *)val; 
         break;
      case KEYTYP_DOUBLE:
         the_one->val = (double *)malloc (kludge*sizeof (double));
         *(double *)the_one->val = *(double *)val; 
         break;
      case KEYTYP_FILEP:
      /** NOTE that in this case the_one->val is of type FILE** not FILE* **/
         the_one->val = (FILE **)malloc (kludge*sizeof (FILE *));
	 *(FILE **)the_one->val = (FILE *)val;
         break;
      case KEYTYP_TIME:
         the_one->val = (TIME *)malloc (kludge*sizeof (TIME));
         *(TIME *)the_one->val = *(TIME *)val; 
         break;
      default:
					     /*  KEYTYP_CMPLX not supported  */
	 soi_errno = KEYTYPE_NOT_IMPLEMENTED;
         break;
   }
}

void addkey (KEY **list, char *key, void *val, int type) {
	     /**  WARNING - can result in a keylist with duplicate entries  **/
   KEY *new_one;

   if (!key) return;
   if (!val) return;

   new_one = (KEY *)malloc (kludge*sizeof (KEY));
   new_one->next = *list;
   new_one->name = key_strdup (key);
   new_one->type = type;
   *list = new_one;

   switch (type) {
      case KEYTYP_STRING:
         new_one->val = key_strdup (val);
         break;
      case KEYTYP_BYTE:
         new_one->val = (signed char *)malloc (kludge*1);
         *(signed char *)new_one->val = *(signed char *)val;
         break;
      case KEYTYP_UBYTE:
         new_one->val = (char *)malloc (kludge*1);
         *(unsigned char *)new_one->val = *(unsigned char *)val;
         break;
      case KEYTYP_SHORT:
         new_one->val = (short *)malloc (kludge*sizeof (short));
         *(short *)new_one->val = *(short *)val; 
         break;
      case KEYTYP_USHORT:
         new_one->val =
		(unsigned short *)malloc (kludge*sizeof (unsigned short));
         *(unsigned short *)new_one->val = *(unsigned short *)val; 
         break;
      case KEYTYP_INT:
         new_one->val = (int *)malloc (kludge*sizeof (int));
         *(int *)new_one->val = *(int *)val; 
         break;
      case KEYTYP_UINT:
         new_one->val = (unsigned int *)malloc (kludge*sizeof (unsigned int));
         *(unsigned int *)new_one->val = *(unsigned int *)val; 
         break;
      case KEYTYP_LONG:
         new_one->val = (long *)malloc (kludge*sizeof (long));
         *(long *)new_one->val = *(long *)val; 
         break;
      case KEYTYP_ULONG:
         new_one->val =
		(unsigned long *)malloc (kludge*sizeof (unsigned long));
         *(unsigned long *)new_one->val = *(unsigned long *)val; 
         break;
      case KEYTYP_UINT64:
         new_one->val = (uint64_t *)malloc (kludge*sizeof (uint64_t));
         *(uint64_t *)new_one->val = *(uint64_t *)val; 
         break;
      case KEYTYP_UINT32:
         new_one->val = (uint32_t *)malloc (kludge*sizeof (uint32_t));
         *(uint32_t *)new_one->val = *(uint32_t *)val; 
         break;
      case KEYTYP_FLOAT:
         new_one->val = (float *)malloc (kludge*sizeof (float));
         *(float *)new_one->val = *(float *)val; 
         break;
      case KEYTYP_DOUBLE:
         new_one->val = (double *)malloc (kludge*sizeof (double));
         *(double *)new_one->val = *(double *)val; 
         break;
      case KEYTYP_FILEP:
      /** NOTE that in this case new_one->val is of type FILE** not FILE* **/
         new_one->val = (FILE **)malloc (kludge*sizeof (FILE *));
         *(FILE **)new_one->val = (FILE *)val;
         break;
      case KEYTYP_TIME:
         new_one->val = (TIME *)malloc (kludge*sizeof (TIME));
         *(TIME *)new_one->val = *(TIME *)val; 
         break;
      default:
					     /*  KEYTYP_CMPLX not supported  */
	 soi_errno = KEYTYPE_NOT_IMPLEMENTED;
         break;
   }
}

void deletekey (KEY **list, char *key) {
   KEY *walker = *list;
   KEY *trailer = NULL;

   if (!key) return;

   while (walker) {
      if (strcmp (walker->name, key)) {			   /*  keep looking  */
         trailer = walker;
         walker = walker->next;
      } else {
         if (trailer)			       /* key is not at head of list */
            trailer->next = walker->next;
         else					   /* key is at head of list */
            *list = walker->next;
         free (walker->name);
         free (walker->val);
         free (walker);
         walker = NULL;
      }
   }
}


void keyiterate (void (*action)(), KEY *overlist) {
   KEY * walker = overlist;
   while (walker) {
      (*action)(walker);
      walker = walker->next;
   }
}

int getkeytype (KEY *list, char *key) {
   KEY *the_one = findkey (list, key);
   if (the_one)
     return (the_one->type);
   return KEYTYP_VOID;
}

char *GETKEY_str(KEY *params, char *key) {
   static char tmp[KEY_MAXSTR];
   char *c = getkey_str(params,key);
   if (c) {
      strcpy(tmp, c);
      free(c);
   } else
      tmp[0] = '\0';
   return(tmp);
}

char *getkey_str (KEY *list, char *key) {
  KEY *the_one = findkey (list, key);
  if (the_one) {
    if (the_one->type == KEYTYP_STRING)
      return key_strdup (the_one->val);
    else
      soi_errno = KEY_WRONG_TYPE;
  }
  return NULL;
}

FILE *getkey_fileptr (KEY *list, char *key) {
  KEY *the_one = findkey (list, key);
  if (the_one) {
    if (the_one->type == KEYTYP_FILEP)
      return *(FILE **)(the_one->val);
    else
      soi_errno = KEY_WRONG_TYPE;
  }
  return NULL;
}

int getkey_byte (KEY *list, char *key) {
  int return_val;
  KEY *the_one = findkey (list, key);
  if (the_one) {
    switch (the_one->type) {
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  }
  return INT_MIN;
}

int getkey_ubyte (KEY *list, char *key) {
  int return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN; 
}

int getkey_short (KEY *list, char *key) {
  int return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

int getkey_ushort (KEY *list, char *key) {
  int return_val;
  KEY *the_one = findkey (list, key);
  soi_errno = NO_ERROR;
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

int getkey_int (KEY *list, char *key) {
  int return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

unsigned int getkey_uint (KEY *list, char *key) {
  unsigned int return_val;
  int int_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UINT:
        return_val = *(unsigned int *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        int_val = *(signed char *)the_one->val;
	if (int_val >= 0)
          return (return_val=int_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        int_val = *(short *)the_one->val;
	if (int_val >= 0)
          return (return_val=int_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        int_val = *(int *)the_one->val;
	if (int_val >= 0)
          return (return_val=int_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return UINT_MAX;
}

int getkey_flag (KEY *list, char *key) {
  int return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

long getkey_long (KEY *list, char *key) {
  long return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_LONG:
        return_val = *(long *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        return_val = *(int *)the_one->val;
        return (return_val);
      case KEYTYP_UINT:
        return_val = *(unsigned int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return LONG_MIN;
}

unsigned long getkey_ulong (KEY *list, char *key) {
  unsigned long return_val;
  unsigned long long_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_ULONG:
        return_val = *(unsigned long *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE:
        long_val = *(signed char *)the_one->val;
	if (long_val >= 0)
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UBYTE:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT:
        long_val = *(short *)the_one->val;
	if (long_val >= 0)
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_USHORT:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT:
        long_val = *(int *)the_one->val;
	if (long_val >= 0)
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UINT:
        return_val = *(unsigned int *)the_one->val;
        return (return_val);
      case KEYTYP_LONG:
        long_val = *(long *)the_one->val;
	if (long_val >= 0) /*WARN*/
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return ULONG_MAX;
}

uint64_t getkey_uint64 (KEY *list, char *key) {
  uint64_t return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UINT64:
        return_val = *(uint64_t *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return (uint64_t)ULONG_MAX;
}

uint32_t getkey_uint32 (KEY *list, char *key) {
  uint32_t return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UINT32:
        return_val = *(uint32_t *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return (uint32_t)ULONG_MAX;
}

double getkey_float (KEY *list, char *key) {
  double return_val;
  KEY *the_one = findkey (list, key);
  if (the_one) {
    switch (the_one->type) {
      case KEYTYP_FLOAT:
        return_val = *(float *)the_one->val;
        return (return_val);
      case KEYTYP_DOUBLE:
        return_val = *(double *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  }
  return D_NAN;
}

double getkey_double (KEY *list, char *key) {
  double return_val;
  KEY *the_one = findkey (list, key);
  if (the_one)
    switch (the_one->type)
    {
      case KEYTYP_DOUBLE:
        return_val = *(double *)the_one->val;
        return (return_val);
      case KEYTYP_FLOAT:
        return_val = *(float *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return D_NAN;
}

TIME getkey_time (KEY *list, char *key) {
  double return_val;
  KEY *the_one = findkey (list, key);
  if (the_one) {
    if (the_one->type == KEYTYP_TIME) {
      return_val = *(TIME *)the_one->val;
      return (return_val);
    }
    else
      soi_errno = KEY_WRONG_TYPE;
  }
  return D_NAN;
}

void setkey_str (KEY **list, char *key, char *val) {
   setkey_any (list, key, val, KEYTYP_STRING);
}

void addkey_str (KEY **list, char *key, char *val) {
	     /**  WARNING - can result in a keylist with duplicate entries  **/
   addkey (list, key, val, KEYTYP_STRING);
}

void setkey_fileptr (KEY **list, char *key, FILE *val) {
   setkey_any (list, key, val, KEYTYP_FILEP);
}

void setkey_byte (KEY **list, char *key, char val) {
   setkey_any (list, key, &val, KEYTYP_BYTE);
}

void setkey_ubyte (KEY **list, char *key, unsigned char val) {
   setkey_any (list, key, &val, KEYTYP_UBYTE);
}

void setkey_short (KEY **list, char *key, short val) {
   setkey_any (list, key, &val, KEYTYP_SHORT);
}

void setkey_ushort (KEY **list, char *key, unsigned short val) {
   setkey_any (list, key, &val, KEYTYP_USHORT);
}

void setkey_int (KEY **list, char *key, int val) {
   setkey_any (list, key, &val, KEYTYP_INT);
}

void setkey_uint (KEY **list, char *key, unsigned int val) {
   setkey_any (list, key, &val, KEYTYP_UINT);
}

void setkey_long (KEY **list, char *key, long val) {
   setkey_any (list, key, &val, KEYTYP_LONG);
}

void setkey_ulong (KEY **list, char *key, unsigned long val) {
   setkey_any (list, key, &val, KEYTYP_ULONG);
}

void setkey_uint64 (KEY **list, char *key, uint64_t val) {
   setkey_any (list, key, &val, KEYTYP_UINT64);
}

void setkey_uint32 (KEY **list, char *key, uint32_t val) {
   setkey_any (list, key, &val, KEYTYP_UINT32);
}

void setkey_float (KEY **list, char *key, float val) {
   setkey_any (list, key, &val, KEYTYP_FLOAT);
}

void setkey_double (KEY **list, char *key, double val) {
   setkey_any (list, key, &val, KEYTYP_DOUBLE);
}

void setkey_time (KEY **list, char *key, TIME val) {
   setkey_any (list, key, &val, KEYTYP_TIME);
}

void add_keys (KEY *inlist, KEY **tolist) {
   KEY *walker = inlist;

   while (walker) {
      setkey_any (tolist, walker->name, walker->val, walker->type);
      walker = walker->next;
   }
}

void delete_keys (KEY *inlist, KEY **fromlist) {
   KEY *walker = inlist;

   while (walker) {
      deletekey (fromlist, walker->name);
      walker = walker->next;
   }
}
