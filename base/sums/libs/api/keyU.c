#define _GNU_SOURCE

#include <soi_error.h>
#include <soi_key.h>
#include <string.h>
#include "SUM.h"
#include "keyU.h"



KEYU *newkeylistU () {
   return NULL;
}

void freekeylistU (KEYU **list) {
   KEYU *node, *tmp;
   if (*list) {
       HASH_ITER(hh, *list, node, tmp) {
	   HASH_DEL(*list, node);
	   free(node);
       }
       *list = NULL;
   }
}

KEYU *findkeyU (KEYU *list, char *key) {
  KEYU *thekey = NULL;
  HASH_FIND_STR(list, key, thekey);
  if (thekey)
      soi_errno = NO_ERROR;
  else
      soi_errno = KEY_NOT_FOUND;
  return thekey;
}

void getkey_anyU (KEYU *list, char *key, void *valptr) {
  KEYU *the_one = findkeyU (list, key);
  if (the_one) 
    if (the_one->type == KEYTYP_STRING_U)
      *(char **)valptr = strndup(the_one->val, strlen(the_one->val));
    else if (the_one->type == KEYTYP_FILEP_U)
      /** NOTE that in this case the_one->val is of type FILE** not FILE* **/
      *(FILE **)valptr = *(FILE **)the_one->val;
    else if (the_one->type == KEYTYP_DOUBLE_U)
      *(double *)valptr = *(double *)the_one->val;
    else if (the_one->type == KEYTYP_FLOAT_U)
      *(float *)valptr = *(float *)the_one->val;
    else if (the_one->type == KEYTYP_INT_U)
      *(int *)valptr = *(int *)the_one->val;
    else if (the_one->type == KEYTYP_SHORT_U)
      *(short *)valptr = *(short *)the_one->val;
    else if (the_one->type == KEYTYP_BYTE_U)
      *(signed char *)valptr = *(signed char *)the_one->val;
    else if (the_one->type == KEYTYP_LONG_U)
      *(long *)valptr = *(long *)the_one->val;
    else if (the_one->type == KEYTYP_UINT_U)
      *(unsigned int *)valptr = *(unsigned int *)the_one->val;
    else if (the_one->type == KEYTYP_USHORT_U)
      *(unsigned short *)valptr = *(unsigned short *)the_one->val;
    else if (the_one->type == KEYTYP_UBYTE_U)
      *(unsigned char *)valptr = *(unsigned char *)the_one->val;
    else if (the_one->type == KEYTYP_ULONG_U)
      *(unsigned long *)valptr = *(unsigned long *)the_one->val;
    else if (the_one->type == KEYTYP_UINT64_U)
      *(uint64_t *)valptr = *(uint64_t *)the_one->val;
    else if (the_one->type == KEYTYP_UINT32_U)
      *(uint32_t *)valptr = *(uint32_t *)the_one->val;
    else
      soi_errno = KEY_WRONG_TYPE;
  else
    soi_errno = KEY_NOT_FOUND;
}

void setkey_anyU (KEYU **list, char *key, void *val, int type) {
      /*  formerly setkey - renamed to avoid conflict with stdlib.h on fault */
   KEYU *the_one;
   KEYU *new_one;

   if (!key) return;
   if (!val) return;

   the_one = findkeyU (*list, key);
   soi_errno = NO_ERROR;
   if (the_one) {
      free (the_one->val); /* old value */
   }
   else {
      new_one = (KEYU *)malloc (sizeof (KEYU));
      new_one->name = strndup (key,strlen(key));
      HASH_ADD_KEYPTR(hh, *list, new_one->name, strlen(new_one->name), new_one);
      the_one = new_one;
   }
   the_one->type = type;
   switch (type) {
      case KEYTYP_STRING_U:
         the_one->val = strndup (val,strlen(val));
         break;
      case KEYTYP_BYTE_U:
         the_one->val = (signed char *)malloc (1);
         *(signed char *)the_one->val = *(signed char *)val;
         break;
      case KEYTYP_UBYTE_U:
         the_one->val = (char *)malloc (1);
         *(unsigned char *)the_one->val = *(unsigned char *)val;
         break;
      case KEYTYP_SHORT_U:
         the_one->val = (short *)malloc (sizeof (short));
         *(short *)the_one->val = *(short *)val; 
         break;
      case KEYTYP_USHORT_U:
         the_one->val =
		(unsigned short *)malloc (sizeof (unsigned short));
         *(unsigned short *)the_one->val = *(unsigned short *)val; 
         break;
      case KEYTYP_INT_U:
         the_one->val = (int *)malloc (sizeof (int));
         *(int *)the_one->val = *(int *)val; 
         break;
      case KEYTYP_UINT_U:
         the_one->val = (unsigned int *)malloc (sizeof (unsigned int));
         *(unsigned int *)the_one->val = *(unsigned int *)val; 
         break;
      case KEYTYP_LONG_U:
         the_one->val = (long *)malloc (sizeof (long));
         *(long *)the_one->val = *(long *)val; 
         break;
      case KEYTYP_ULONG_U:
         the_one->val =
		(unsigned long *)malloc (sizeof (unsigned long));
         *(unsigned long *)the_one->val = *(unsigned long *)val; 
         break;
      case KEYTYP_UINT64_U:
         the_one->val = (uint64_t *)malloc (sizeof (uint64_t));
         *(uint64_t *)the_one->val = *(uint64_t *)val; 
         break;
      case KEYTYP_UINT32_U:
         the_one->val = (uint32_t *)malloc (sizeof (uint32_t));
         *(uint32_t *)the_one->val = *(uint32_t *)val; 
         break;
      case KEYTYP_FLOAT_U:
         the_one->val = (float *)malloc (sizeof (float));
         *(float *)the_one->val = *(float *)val; 
         break;
      case KEYTYP_DOUBLE_U:
         the_one->val = (double *)malloc (sizeof (double));
         *(double *)the_one->val = *(double *)val; 
         break;
      case KEYTYP_FILEP_U:
      /** NOTE that in this case the_one->val is of type FILE** not FILE* **/
         the_one->val = (FILE **)malloc (sizeof (FILE *));
	 *(FILE **)the_one->val = (FILE *)val;
         break;
      case KEYTYP_TIME_U:
         the_one->val = (TIME *)malloc (sizeof (TIME));
         *(TIME *)the_one->val = *(TIME *)val; 
         break;
      default:
					     /*  KEYTYP_CMPLX not supported  */
	 soi_errno = KEYTYPE_NOT_IMPLEMENTED;
         break;
   }
}

void deletekeyU (KEYU **list, char *key) {
   KEYU *thekey;

   if (!key) return;
   HASH_FIND_STR(*list, key, thekey);
   if (thekey) {
       HASH_DEL(*list, thekey);
       free(thekey);
   }
}


void keyiterateU (void (*action)(), KEYU *overlist) {
   KEYU * walker = overlist;
   while (walker) {
      (*action)(walker);
      walker = walker->hh.next;
   }
}

int getkeytypeU (KEYU *list, char *key) {
   KEYU *the_one = findkeyU (list, key);
   if (the_one)
     return (the_one->type);
   return KEYTYP_VOID_U;
}

char *GETKEY_strU(KEYU *params, char *key) {
   static char tmp[KEY_MAXSTR];
   char *c = getkey_strU(params,key);
   if (c) {
      strcpy(tmp, c);
      free(c);
   } else
      tmp[0] = '\0';
   return(tmp);
}

char *getkey_strU (KEYU *list, char *key) {
  KEYU *the_one = findkeyU (list, key);
  if (the_one) {
    if (the_one->type == KEYTYP_STRING_U)
      return strndup (the_one->val,strlen(the_one->val));
    else
      soi_errno = KEY_WRONG_TYPE;
  }
  return NULL;
}

FILE *getkey_fileptrU (KEYU *list, char *key) {
  KEYU *the_one = findkeyU (list, key);
  if (the_one) {
    if (the_one->type == KEYTYP_FILEP_U)
      return *(FILE **)(the_one->val);
    else
      soi_errno = KEY_WRONG_TYPE;
  }
  return NULL;
}

int getkey_byteU (KEYU *list, char *key) {
  int return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one) {
    switch (the_one->type) {
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
        return_val = *(int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  }
  return INT_MIN;
}

int getkey_ubyteU (KEYU *list, char *key) {
  int return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
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

int getkey_shortU (KEYU *list, char *key) {
  int return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
        return_val = *(int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

int getkey_ushortU (KEYU *list, char *key) {
  int return_val;
  KEYU *the_one = findkeyU (list, key);
  soi_errno = NO_ERROR;
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
	if (return_val >= 0)
          return (return_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_INT_U:
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

int getkey_intU (KEYU *list, char *key) {
  int return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_INT_U:
        return_val = *(int *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

unsigned int getkey_uintU (KEYU *list, char *key) {
  unsigned int return_val;
  int int_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UINT_U:
        return_val = *(unsigned int *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        int_val = *(signed char *)the_one->val;
	if (int_val >= 0)
          return (return_val=int_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        int_val = *(short *)the_one->val;
	if (int_val >= 0)
          return (return_val=int_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
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

int getkey_flagU (KEYU *list, char *key) {
  int return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
        return_val = *(int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return INT_MIN;
}

long getkey_longU (KEYU *list, char *key) {
  long return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_LONG_U:
        return_val = *(long *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        return_val = *(signed char *)the_one->val;
        return (return_val);
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        return_val = *(short *)the_one->val;
        return (return_val);
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
        return_val = *(int *)the_one->val;
        return (return_val);
      case KEYTYP_UINT_U:
        return_val = *(unsigned int *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return LONG_MIN;
}

unsigned long getkey_ulongU (KEYU *list, char *key) {
  unsigned long return_val;
  unsigned long long_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_ULONG_U:
        return_val = *(unsigned long *)the_one->val;
        return (return_val);
      case KEYTYP_BYTE_U:
        long_val = *(signed char *)the_one->val;
	if (long_val >= 0)
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UBYTE_U:
        return_val = *(unsigned char *)the_one->val;
        return (return_val);
      case KEYTYP_SHORT_U:
        long_val = *(short *)the_one->val;
	if (long_val >= 0)
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_USHORT_U:
        return_val = *(unsigned short *)the_one->val;
        return (return_val);
      case KEYTYP_INT_U:
        long_val = *(int *)the_one->val;
	if (long_val >= 0)
          return (return_val=long_val);
	else
          soi_errno = KEY_WRONG_TYPE;
	  break;
      case KEYTYP_UINT_U:
        return_val = *(unsigned int *)the_one->val;
        return (return_val);
      case KEYTYP_LONG_U:
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

uint64_t getkey_uint64U (KEYU *list, char *key) {
  uint64_t return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UINT64_U:
        return_val = *(uint64_t *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return (uint64_t)ULONG_MAX;
}

uint32_t getkey_uint32U (KEYU *list, char *key) {
  uint32_t return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type) {
      case KEYTYP_UINT32_U:
        return_val = *(uint32_t *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return (uint32_t)ULONG_MAX;
}

double getkey_floatU (KEYU *list, char *key) {
  double return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one) {
    switch (the_one->type) {
      case KEYTYP_FLOAT_U:
        return_val = *(float *)the_one->val;
        return (return_val);
      case KEYTYP_DOUBLE_U:
        return_val = *(double *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  }
  return D_NAN;
}

double getkey_doubleU (KEYU *list, char *key) {
  double return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one)
    switch (the_one->type)
    {
      case KEYTYP_DOUBLE_U:
        return_val = *(double *)the_one->val;
        return (return_val);
      case KEYTYP_FLOAT_U:
        return_val = *(float *)the_one->val;
        return (return_val);
      default:
        soi_errno = KEY_WRONG_TYPE;
    }
  return D_NAN;
}

TIME getkey_timeU (KEYU *list, char *key) {
  double return_val;
  KEYU *the_one = findkeyU (list, key);
  if (the_one) {
    if (the_one->type == KEYTYP_TIME_U) {
      return_val = *(TIME *)the_one->val;
      return (return_val);
    }
    else
      soi_errno = KEY_WRONG_TYPE;
  }
  return D_NAN;
}

void setkey_strU (KEYU **list, char *key, char *val) {
   setkey_anyU (list, key, val, KEYTYP_STRING_U);
}

void setkey_fileptrU (KEYU **list, char *key, FILE *val) {
   setkey_anyU (list, key, val, KEYTYP_FILEP_U);
}

void setkey_byteU (KEYU **list, char *key, char val) {
   setkey_anyU (list, key, &val, KEYTYP_BYTE_U);
}

void setkey_ubyteU (KEYU **list, char *key, unsigned char val) {
   setkey_anyU (list, key, &val, KEYTYP_UBYTE_U);
}

void setkey_shortU (KEYU **list, char *key, short val) {
   setkey_anyU (list, key, &val, KEYTYP_SHORT_U);
}

void setkey_ushortU (KEYU **list, char *key, unsigned short val) {
   setkey_anyU (list, key, &val, KEYTYP_USHORT_U);
}

void setkey_intU (KEYU **list, char *key, int val) {
   setkey_anyU (list, key, &val, KEYTYP_INT_U);
}

void setkey_uintU (KEYU **list, char *key, unsigned int val) {
   setkey_anyU (list, key, &val, KEYTYP_UINT_U);
}

void setkey_longU (KEYU **list, char *key, long val) {
   setkey_anyU (list, key, &val, KEYTYP_LONG_U);
}

void setkey_ulongU (KEYU **list, char *key, unsigned long val) {
   setkey_anyU (list, key, &val, KEYTYP_ULONG_U);
}

void setkey_uint64U (KEYU **list, char *key, uint64_t val) {
   setkey_anyU (list, key, &val, KEYTYP_UINT64_U);
}

void setkey_uint32U (KEYU **list, char *key, uint32_t val) {
   setkey_anyU (list, key, &val, KEYTYP_UINT32_U);
}

void setkey_floatU (KEYU **list, char *key, float val) {
   setkey_anyU (list, key, &val, KEYTYP_FLOAT_U);
}

void setkey_doubleU (KEYU **list, char *key, double val) {
   setkey_anyU (list, key, &val, KEYTYP_DOUBLE_U);
}

void setkey_timeU (KEYU **list, char *key, TIME val) {
   setkey_anyU (list, key, &val, KEYTYP_TIME_U);
}

void add_keysU (KEYU *inlist, KEYU **tolist) {
   KEYU *walker = inlist;

   while (walker) {
      setkey_anyU (tolist, walker->name, walker->val, walker->type);
      walker = walker->hh.next;
   }
}

void add_key2keyU (KEY *inlist, KEYU **tolist) {
   KEY *walker = inlist;

   while (walker) {
      setkey_anyU (tolist, walker->name, walker->val, walker->type);
      walker = walker->next;
   }
}

void add_keyU2key (KEYU *inlist, KEY **tolist) {
   KEYU *walker = inlist;

   while (walker) {
      setkey_any (tolist, walker->name, walker->val, walker->type);
      walker = walker->hh.next;
   }
}

void delete_keysU (KEYU *inlist, KEYU **fromlist) {
   KEYU *walker = inlist;

   while (walker) {
      deletekeyU (fromlist, walker->name);
      walker = walker->hh.next;
   }
}
