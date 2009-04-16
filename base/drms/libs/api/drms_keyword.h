/**
\file drms_keyword.h
\brief Functions to access DRMS keyword values, and to convert DRMS keywords to FITS keywords and vice versa.
*/

#ifndef _DRMS_KEYWORD_H
#define _DRMS_KEYWORD_H

#include "drms_types.h"
#include "cfitsio.h"


void drms_keyword_print(DRMS_Keyword_t *key);
void drms_keyword_fprint(FILE *keyfile, DRMS_Keyword_t *key);
void drms_keyword_printval(DRMS_Keyword_t *key);
void drms_keyword_fprintval(FILE *keyfile, DRMS_Keyword_t *key);
void drms_keyword_snprintfval(DRMS_Keyword_t *key, char *buf, int size);
DRMS_Keyword_t *drms_keyword_lookup(DRMS_Record_t *rec, const char *key, int followlink);
DRMS_Type_t drms_keyword_type(DRMS_Keyword_t *key);
HContainer_t *drms_keyword_createinfocon(DRMS_Env_t *drmsEnv, 
					  const char *seriesName, 
					  int *status);
void drms_keyword_destroyinfocon(HContainer_t **info);

int drms_keyword_keysmatch(DRMS_Keyword_t *k1, DRMS_Keyword_t *k2);

/************ getkey and setkey family of functions. ************/
/* Versions with type conversion. */
char drms_getkey_char(DRMS_Record_t *rec, const char *key,int *status);
short drms_getkey_short(DRMS_Record_t *rec, const char *key, int *status);
int drms_getkey_int(DRMS_Record_t *rec, const char *key, int *status);
long long drms_getkey_longlong(DRMS_Record_t *rec, const char *key, int *status);
float drms_getkey_float(DRMS_Record_t *rec, const char *key, int *status);
double drms_getkey_double(DRMS_Record_t *rec, const char *key, int *status);
char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status);
char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status);
TIME drms_getkey_time(DRMS_Record_t *rec, const char *key, int *status);

/* Directly from the keyword */
double drms_keyword_getdouble(DRMS_Keyword_t *keyword, int *status);
TIME drms_keyword_gettime(DRMS_Keyword_t *keyword, int *status);

/* Generic versions. */
DRMS_Type_Value_t drms_getkey(DRMS_Record_t *rec, const char *key, 
			      DRMS_Type_t *type, int *status);
DRMS_Value_t drms_getkey_p(DRMS_Record_t *rec, const char *key, int *status);

/* Versions with type conversion. */
int drms_setkey_char(DRMS_Record_t *rec, const char *key, char value);
int drms_setkey_short(DRMS_Record_t *rec, const char *key, short value);
int drms_setkey_int(DRMS_Record_t *rec, const char *key, int value);
int drms_setkey_longlong(DRMS_Record_t *rec, const char *key, long long value);
int drms_setkey_float(DRMS_Record_t *rec, const char *key, float value);
int drms_setkey_double(DRMS_Record_t *rec, const char *key, double value);
int drms_setkey_time(DRMS_Record_t *rec, const char *key, TIME value);
int drms_setkey_string(DRMS_Record_t *rec, const char *key, const char *value);

/* Generic version. */
int drms_setkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t type, 
		DRMS_Type_Value_t *value);

/* Truly POLYMORPHIC versions that don't need to know the actual type. */
int drms_setkey_p(DRMS_Record_t *rec, const char *key, DRMS_Value_t *value);

int drms_keyword_inclass(DRMS_Keyword_t *key, DRMS_KeywordClass_t class);


/* Copy functions. */
int drms_copykey(DRMS_Record_t *target, DRMS_Record_t *source, const char *key);
int drms_copykeyB(DRMS_Keyword_t *tgtkey, DRMS_Keyword_t *srckey);
int drms_copykeys(DRMS_Record_t *target, 
                  DRMS_Record_t *source, 
                  int usesrcset, 
                  DRMS_KeywordClass_t class);

/******** Functions to handle mapping between internal/external keywords **********/

/* Maps to internal keywords in this order.  If an item does not result in a valid
 * DRMS keyword, then the next item is consulted.
 *   1. FITS name.
 *   2. Name generated by default rule to convert from FITS name to DRMS name. */
int drms_keyword_getintname(const char *keyname, char *nameOut, int size);

/* Maps to internal keywords in this order.  If an item does not result in a valid
 * DRMS keyword, then the next item is consulted.
 *   1. if (map != NULL), map FITS name to DRMS name using map.
 *   2. if (class != NULL), use default rule associated with class to map to DRMS name.
 *   3. FITS name.
 *   4. Name generated by default rule to convert from FITS name to DRMS name. */
int drms_keyword_getintname_ext(const char *keyname, 
				DRMS_KeyMapClass_t *classid, 
				DRMS_KeyMap_t *map,
				char *nameOut, 
				int size);

/* Maps to external keywords in this order.  If an item does not result in a valid
 * FITS keyword, then the next item is consulted.
 *   1. Name in keyword description.
 *   2. DRMS name.
 *   3. Name generated by default rule to convert from DRMS name to FITS name. */
int drms_keyword_getextname(DRMS_Keyword_t *key, char *nameOut,	int size);

/* Maps to external keywords in this order.  If an item does not result in a valid
 * FITS keyword, then the next item is consulted.
 *   1. if (map != NULL), map DRMS name to external name using map.
 *   2. if (class != NULL), use default rule associated with class to map to external name.
 *   3. Name in keyword description.
 *   4. DRMS name.
 *   5. Name generated by default rule to convert from DRMS name to FITS name. */
int drms_keyword_getmappedextname(DRMS_Keyword_t *key, 
				  const char *class, 
				  DRMS_KeyMap_t *map,
				  char *nameOut,
				  int size);

/* Accessor functions */
static inline const char *drms_keyword_getname(DRMS_Keyword_t *key)
{
   return key->info->name;
}
DRMS_Type_t drms_keyword_gettype(DRMS_Keyword_t *key);
const DRMS_Type_Value_t *drms_keyword_getvalue(DRMS_Keyword_t *key);
int drms_keyword_getsegscope(DRMS_Keyword_t *key);
DRMS_RecScopeType_t drms_keyword_getrecscope(DRMS_Keyword_t *key);
const char *drms_keyword_getrecscopestr(DRMS_Keyword_t *key, int *status);
DRMS_SlotKeyUnit_t drms_keyword_getslotunit(DRMS_Keyword_t *key, int *status);

static inline int drms_keyword_getperseg(DRMS_Keyword_t *key)
{
   return ((key->info->kwflags & kKeywordFlag_PerSegment) != 0);
}
static inline void drms_keyword_setperseg(DRMS_Keyword_t *key)
{
   key->info->kwflags |= kKeywordFlag_PerSegment;
}
static inline void drms_keyword_unsetperseg(DRMS_Keyword_t *key)
{
   key->info->kwflags &= ~kKeywordFlag_PerSegment;
}
static inline int drms_keyword_getimplicit(DRMS_Keyword_t *key)
{
   return ((key->info->kwflags & kKeywordFlag_Implicit) != 0);
}
static inline void drms_keyword_setimplicit(DRMS_Keyword_t *key)
{
   key->info->kwflags |= kKeywordFlag_Implicit;
}
static inline void drms_keyword_unsetimplicit(DRMS_Keyword_t *key)
{
   key->info->kwflags &= ~kKeywordFlag_Implicit;
}
static inline int drms_keyword_getintprime(DRMS_Keyword_t *key)
{
   return ((key->info->kwflags & kKeywordFlag_InternalPrime) != 0);
}
static inline void drms_keyword_setintprime(DRMS_Keyword_t *key)
{
   key->info->kwflags |= kKeywordFlag_InternalPrime;
}
static inline void drms_keyword_unsetintprime(DRMS_Keyword_t *key)
{
   key->info->kwflags &= ~kKeywordFlag_InternalPrime;
}
static inline int drms_keyword_getextprime(DRMS_Keyword_t *key)
{
   return ((key->info->kwflags & kKeywordFlag_ExternalPrime) != 0);
}
static inline void drms_keyword_setextprime(DRMS_Keyword_t *key)
{
   key->info->kwflags |= kKeywordFlag_ExternalPrime;
}
static inline void drms_keyword_unsetextprime(DRMS_Keyword_t *key)
{
   key->info->kwflags &= ~kKeywordFlag_ExternalPrime;
}

int drms_keyword_isprime(DRMS_Keyword_t *key);
int drms_keyword_isvariable(DRMS_Keyword_t *key);
int drms_keyword_isconstant(DRMS_Keyword_t *key);
int drms_keyword_isindex(DRMS_Keyword_t *key);
int drms_keyword_isslotted(DRMS_Keyword_t *key);

DRMS_SlotKeyUnit_t drms_keyword_getunit(DRMS_Keyword_t *key, int *status);
TIME drms_keyword_getepoch(DRMS_Keyword_t *epochkey, int *status);
double drms_keyword_getslotcarr0(void);
/* get the epoch value from the epoch keyword associated with the TSEQ-slotted keyword */
TIME drms_keyword_getslotepoch(DRMS_Keyword_t *slotkey, int *status);
/* get the base value from the base keyword associated with the slotted keyword */
double drms_keyword_getslotbase(DRMS_Keyword_t *slotkey, int *status);
/* Generic - get the base value from the base keyword associated with the value keyword */
double drms_keyword_getvalkeybase(DRMS_Keyword_t *valkey, int *status);
double drms_keyword_getslotstep(DRMS_Keyword_t *slotkey, DRMS_SlotKeyUnit_t *unit, int *status);
double drms_keyword_getstep(DRMS_Keyword_t *key, 
			    DRMS_RecScopeType_t recscope, 
			    DRMS_SlotKeyUnit_t *unit, 
			    int *status);
double drms_keyword_getvalkeystep(DRMS_Keyword_t *valkey, int *status);


/* Utility */
DRMS_RecScopeType_t drms_keyword_str2recscope(const char *str, int *status);

/* Generic - get the index keyword from the associated value keyword */
DRMS_Keyword_t *drms_keyword_indexfromvalkey(DRMS_Keyword_t *valkey);

/* Generic - get the base keyword from the associated value keyword */
DRMS_Keyword_t *drms_keyword_basefromvalkey(DRMS_Keyword_t *valkey);

/* Generic - get the step keyword from the associated value keyword */
DRMS_Keyword_t *drms_keyword_stepfromvalkey(DRMS_Keyword_t *valkey);

int drms_keyword_slotval2indexval(DRMS_Keyword_t *slotkey, 
				  DRMS_Value_t *valin,
				  DRMS_Value_t *valout,
				  DRMS_Value_t *startdur);

/* Export */
int drms_keyword_export(DRMS_Keyword_t *key, CFITSIO_KEYWORD **fitskeys);
int drms_keyword_mapexport(DRMS_Keyword_t *key,
			   const char *clname, 
			   const char *mapfile,
			   CFITSIO_KEYWORD **fitskeys);

/* doxygen documentation */

/**
   @addtogroup keyword_api
   @{
*/

/**
   @fn void drms_keyword_print(DRMS_Keyword_t *key)
   Prints the values of the data in @a DRMS_Keyword_t->data
   to file @a keyfile. 

   @param keyfile The name of the file to be printed to.
   @param DRMS_Keyword_t The DRMS key value whose data value(s) are being printed.
*/


/**
   @fn void drms_keyword_fprint(FILE *keyfile, DRMS_Keyword_t *key);
   blah blah
*/

/**
   @fn void drms_keyword_printval(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn void drms_keyword_fprintval(FILE *keyfile, DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn void drms_keyword_snprintfval(DRMS_Keyword_t *key, char *buf, int size)
   blah blah
*/

/**
   @fn DRMS_Keyword_t *drms_keyword_lookup(DRMS_Record_t *rec, const char *key, int followlink)
   blah blah
*/

/**
   @fn DRMS_Type_t drms_keyword_type(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn HContainer_t *drms_keyword_createinfocon(DRMS_Env_t *drmsEnv, const char *seriesName, int *status)
   blah blah
*/

/**
   @fn void drms_keyword_destroyinfocon(HContainer_t **info)
   blah blah
*/

/**
   @fn int drms_keyword_keysmatch(DRMS_Keyword_t *k1, DRMS_Keyword_t *k2)
   blah blah
*/


/** 
    @fn char drms_getkey_char(DRMS_Record_t *rec, const char *key,int *status)
    blah blah
*/

/** 
    @fn short drms_getkey_short(DRMS_Record_t *rec, const char *key, int *status)
    blah blah
*/

/**
   @fn int drms_getkey_int(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/**
   @fn long long drms_getkey_longlong(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/**
   @fn float drms_getkey_float(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/**
   @fn double drms_getkey_double(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/**
   @fn char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/**
   @fn char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/** 
    @fn TIME drms_getkey_time(DRMS_Record_t *rec, const char *key, int *status)
    blah blah
*/

/**
   @fn double drms_keyword_getdouble(DRMS_Keyword_t *keyword, int *status)
   blah blah
*/

/**
   @fn TIME drms_keyword_gettime(DRMS_Keyword_t *keyword, int *status)
   blah blah
*/

/**
   @fn DRMS_Type_Value_t drms_getkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t *type, int *status)
   blah blah
*/

/**
   @fn DRMS_Value_t drms_getkey_p(DRMS_Record_t *rec, const char *key, int *status)
   blah blah
*/

/**
   @fn int drms_setkey_char(DRMS_Record_t *rec, const char *key, char value)
   blah blah
*/

/**
  @fn int drms_setkey_short(DRMS_Record_t *rec, const char *key, short value)
  blah blah
*/

/**
   @fn int drms_setkey_int(DRMS_Record_t *rec, const char *key, int value)
   blah blah
*/

/**
   @fn int drms_setkey_longlong(DRMS_Record_t *rec, const char *key, long long value)
   blah blah
*/

/**
   @fn int drms_setkey_float(DRMS_Record_t *rec, const char *key, float value)
   blah blah
*/

/**
   @fn int drms_setkey_double(DRMS_Record_t *rec, const char *key, double value)
   blah blah
*/

/**
   @fn int drms_setkey_time(DRMS_Record_t *rec, const char *key, TIME value)
   blah blah
*/

/**
   @fn int drms_setkey_string(DRMS_Record_t *rec, const char *key, const char *value)
   blah blah
*/

/**
   @fn int drms_setkey(DRMS_Record_t *rec, const char *key, DRMS_Type_t type, DRMS_Type_Value_t *value)
   blah blah
*/

/**
   @fn int drms_setkey_p(DRMS_Record_t *rec, const char *key, DRMS_Value_t *value)
   blah blah
*/

/**
   @fn int drms_keyword_inclass(DRMS_Keyword_t *key, DRMS_KeywordClass_t class)
   blah blah
*/

/**
   @fn int drms_copykey(DRMS_Record_t *target, DRMS_Record_t *source, const char *key)
   blah blah
*/

/**
   @fn int drms_copykeyB(DRMS_Keyword_t *tgtkey, DRMS_Keyword_t *srckey)
   blah blah
*/

/**
   @fn int drms_copykeys(DRMS_Record_t *target, DRMS_Record_t *source, int usesrcset, DRMS_KeywordClass_t class)
   Copies the values of a subset of the @a source keywords to the identically named keywords in the @a target
   DRMS record. If the @a usesrcset flag is set to 1, then the subset to copy is derived from the 
   keywords present in the @a source DRMS record. All keywords in the @a source record that are a member
   of the @a class (see ::DRMS_KeywordClass_enum for a list of classes) specified are copied to the
   @a target record. If the @a usesrcset flag is set to 0, then 
   the keywords to copy are derived from the @a target record. It may be desireable to copy keyword values
   from multiple source records to a single target record, in which case this function needs to
   be called once for each source.

   @param target The DRMS record that contains the keywords to be modified.
   @param source The DRMS record that contains the keywords whose values are to be copied.
   @param usesrcset A flag - if set to 1 then the names of the keywords to copy are derived
   from the keywords in @a source.
   @param class The category of keywords to be copied.
   @return Upon successful completion, returns ::DRMS_SUCCESS. If an error occurs, the
   function returns an appropriate DRMS error code as defined 
   in ::drms_statuscodes.h. Typical errors are as follows. DRMS_ERROR_UNKNOWNKEYWORD is returned
   if an attempt is made to write to a keyword that does not exist in the target record.
*/

/**
   @fn int drms_keyword_getintname(const char *keyname, char *nameOut, int size)
   blah blah
*/

/**
   @fn int drms_keyword_getintname_ext(const char *keyname, DRMS_KeyMapClass_t *classid, DRMS_KeyMap_t *map, char *nameOut, int size)
   blah blah
*/

/**
   @fn int drms_keyword_getextname(DRMS_Keyword_t *key, char *nameOut,	int size)
   blah blah
*/

/** @fn int drms_keyword_getmappedextname(DRMS_Keyword_t *key, const char *class, DRMS_KeyMap_t *map, char *nameOut, int size)
    blah blah
*/

/** 
    @fn static inline const char *drms_keyword_getname(DRMS_Keyword_t *key)
    blah blah
*/

/**
   @fn DRMS_Type_t drms_keyword_gettype(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn const DRMS_Type_Value_t *drms_keyword_getvalue(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn int drms_keyword_getsegscope(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn DRMS_RecScopeType_t drms_keyword_getrecscope(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn const char *drms_keyword_getrecscopestr(DRMS_Keyword_t *key, int *status)
   blah blah
*/

/**
   @fn DRMS_SlotKeyUnit_t drms_keyword_getslotunit(DRMS_Keyword_t *key, int *status)
   blah blah
*/

/**
   @fn static inline int drms_keyword_getperseg(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_setperseg(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_unsetperseg(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline int drms_keyword_getimplicit(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_setimplicit(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_unsetimplicit(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline int drms_keyword_getintprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_setintprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_unsetintprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline int drms_keyword_getextprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_setextprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn static inline void drms_keyword_unsetextprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn int drms_keyword_isprime(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn int drms_keyword_isvariable(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn int drms_keyword_isconstant(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn int drms_keyword_isindex(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn int drms_keyword_isslotted(DRMS_Keyword_t *key)
   blah blah
*/

/**
   @fn DRMS_SlotKeyUnit_t drms_keyword_getunit(DRMS_Keyword_t *key, int *status)
   blah blah
*/

/**
   @fn TIME drms_keyword_getepoch(DRMS_Keyword_t *epochkey, int *status)
   blah blah
*/

/**
   @fn double drms_keyword_getslotcarr0(void)
   blah blah
*/

/**
   @fn TIME drms_keyword_getslotepoch(DRMS_Keyword_t *slotkey, int *status)
   blah blah
*/

/**
   @fn double drms_keyword_getslotbase(DRMS_Keyword_t *slotkey, int *status)
   blah blah
*/

/**
   @fn double drms_keyword_getvalkeybase(DRMS_Keyword_t *valkey, int *status)
   blah blah
*/

/**
   @fn double drms_keyword_getslotstep(DRMS_Keyword_t *slotkey, DRMS_SlotKeyUnit_t *unit, int *status)
   blah blah
*/

/**
   @fn double drms_keyword_getstep(DRMS_Keyword_t *key, DRMS_RecScopeType_t recscope, DRMS_SlotKeyUnit_t *unit, int *status)
   blah blah
*/

/**
   @fn double drms_keyword_getvalkeystep(DRMS_Keyword_t *valkey, int *status)
   blah blah
*/

/**
   @fn DRMS_RecScopeType_t drms_keyword_str2recscope(const char *str, int *status)
   blah blah
*/

/**
   @fn DRMS_Keyword_t *drms_keyword_indexfromvalkey(DRMS_Keyword_t *valkey)
   blah blah
*/

/**
   @fn DRMS_Keyword_t *drms_keyword_basefromvalkey(DRMS_Keyword_t *valkey)
   blah blah
*/

/**
   @fn DRMS_Keyword_t *drms_keyword_stepfromvalkey(DRMS_Keyword_t *valkey)
   blah blah
*/

/**
   @fn int drms_keyword_slotval2indexval(DRMS_Keyword_t *slotkey, DRMS_Value_t *valin, DRMS_Value_t *valout, DRMS_Value_t *startdur)
   blah blah
*/

/**
   @fn int drms_keyword_export(DRMS_Keyword_t *key, CFITSIO_KEYWORD **fitskeys)
   blah blah
*/

/**
   @fn int drms_keyword_mapexport(DRMS_Keyword_t *key, const char *clname, const char *mapfile, CFITSIO_KEYWORD **fitskeys)
   blah blah
*/

/**
   @}
*/


#endif
