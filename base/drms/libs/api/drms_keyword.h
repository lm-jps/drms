/**
\file drms_keyword.h
\brief Functions to access DRMS keyword values, and to convert DRMS keywords to FITS keywords and vice versa.
*/

#ifndef _DRMS_ATTRIBUTE_H
#define _DRMS_ATTRIBUTE_H

#include "drms_types.h"
#include "cfitsio.h"

void drms_keyword_term();

/******** Keyword functions ********/

void drms_free_template_keyword_struct(DRMS_Keyword_t *key);
void drms_free_keyword_struct(DRMS_Keyword_t *key);
void drms_copy_keyword_struct(DRMS_Keyword_t *dst, DRMS_Keyword_t *src);

/* Create stand-alone links that contain pointers to/from target only. */
HContainer_t *drms_create_keyword_prototypes(DRMS_Record_t *target, 
					     DRMS_Record_t *source, 
					     int *status);

void drms_keyword_print(DRMS_Keyword_t *key);

/**
   Prints the values of the data in @a DRMS_Keyword_t->data
   to file @a keyfile. 

   @param keyfile The name of the file to be printed to.
   @param DRMS_Keyword_t The DRMS key value whose data value(s) are being printed.
*/
void drms_keyword_fprint(FILE *keyfile, DRMS_Keyword_t *key);
void drms_keyword_printval(DRMS_Keyword_t *key);
void drms_keyword_fprintval(FILE *keyfile, DRMS_Keyword_t *key);
void drms_keyword_snprintfval(DRMS_Keyword_t *key, char *buf, int size);
int  drms_template_keywords(DRMS_Record_t *template);
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
double drms_keyword_getdouble(DRMS_Keyword_t *keyword, int *status);
double drms_getkey_double(DRMS_Record_t *rec, const char *key, int *status);
char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status);
char *drms_getkey_string(DRMS_Record_t *rec, const char *key, int *status);
TIME drms_getkey_time(DRMS_Record_t *rec, const char *key, int *status);
TIME drms_keyword_gettime(DRMS_Keyword_t *keyword, int *status);

/* Generic version. */
DRMS_Type_Value_t drms_getkey(DRMS_Record_t *rec, const char *key, 
			      DRMS_Type_t *type, int *status);

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
DRMS_Value_t drms_getkey_p(DRMS_Record_t *rec, const char *key, int *status);

/* Copy functions. */
int drms_copykey(DRMS_Record_t *target, DRMS_Record_t *source, const char *key);

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
   return key->info->per_segment;
}
DRMS_SlotKeyUnit_t drms_keyword_getunit(DRMS_Keyword_t *key, int *status);
TIME drms_keyword_getslotepoch(DRMS_Keyword_t *key, int *status);
TIME drms_keyword_getepoch(DRMS_Keyword_t *key, int *status);
double drms_keyword_getslotcarr0(void);
double drms_keyword_getslotbase(DRMS_Keyword_t *slotkey, int *status);
double drms_keyword_getslotstep(DRMS_Keyword_t *slotkey, DRMS_SlotKeyUnit_t *unit, int *status);
double drms_keyword_getstep(DRMS_Keyword_t *key, 
			    DRMS_RecScopeType_t recscope, 
			    DRMS_SlotKeyUnit_t *unit, 
			    int *status);
int drms_keyword_isprime(DRMS_Keyword_t *key);
int drms_keyword_isvariable(DRMS_Keyword_t *key);
int drms_keyword_isconstant(DRMS_Keyword_t *key);
int drms_keyword_isindex(DRMS_Keyword_t *key);
int drms_keyword_isslotted(DRMS_Keyword_t *key);

/* Utility */
DRMS_RecScopeType_t drms_keyword_str2recscope(const char *str, int *status);
DRMS_Keyword_t *drms_keyword_indexfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_epochfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_basefromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_stepfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_unitfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_roundfromslot(DRMS_Keyword_t *slot);
DRMS_Keyword_t *drms_keyword_slotfromindex(DRMS_Keyword_t *indx);
int drms_keyword_slotval2indexval(DRMS_Keyword_t *slotkey, 
				  DRMS_Value_t *valin,
				  DRMS_Value_t *valout,
				  DRMS_Value_t *startdur);

static inline long long CalcSlot(double slotkeyval, 
				 double base, 
				 double stepsecs,
                                 double roundstep)
{
   double slotvald = floor((slotkeyval - base + (roundstep / 2.0)) / stepsecs);
   return slotvald;
}

/* Export */
int drms_keyword_export(DRMS_Keyword_t *key, CFITSIO_KEYWORD **fitskeys);
int drms_keyword_mapexport(DRMS_Keyword_t *key,
			   const char *clname, 
			   const char *mapfile,
			   CFITSIO_KEYWORD **fitskeys);
#endif
