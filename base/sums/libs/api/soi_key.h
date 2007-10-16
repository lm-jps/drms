/*
 *  soi_key.h				~soi/(version)/include/soi_key.h
 *
 *  KEY is a general token-value storage structure.
 * 
 *    next	Pointer to the next token-value pair in the list.
 *    name	Pointer to a null terminated string of characters
 *    val	Pointer to a datum of arbitrary type
 *    type	Code for type of data
 *
 *  The following functions are provided for manipulation of key lists:
 *	getkeytype (list, name)
 *		returns the type associated with name in the list
 *	getkey_any (list, name, valptr)
 *              ensures that valptr points to 
 *		the value associated with name in the list
 *	getkey_xxx (list, name)
 *		returns the value associated with name in the list, cast
 *		to the requested type xxx
 *	setkey_any (list, name, value, type)
 *		modifies the value and type associated with name in the
 *		list, if it exists, or inserts a new valued and typed key
 *		in the list if it doesn't
 *	addkey (list, name, value, type)
 *		inserts a new valued and typed key in the list.
 *		NO CHECK is made for duplicate entries.
 *	addkey_str (list, name, value)
 *		inserts a new valued and typed key in the list.
 *		NO CHECK is made for duplicate entries.
 *	setkey_xxx (list, name, value)
 *		modifies the value and type associated with name in the
 *		list, if it exists, or inserts a new valued and typed key
 *		in the list if it doesn't.  type is determined by xxx
 *	deletekey (list, name)
 *		removes the entry associated with name from the list
 *      add_keys (inlist, tolist)
 *              adds entries in the first list to entries in the second list
 *              an entry in the first list replaces an entry in the second
 *              list which has the same key.
 *      delete_keys (inlist, fromlist)
 *              deletes entries in the first list from entries in the 
 *              second list.  
 *	newkeylist ()
 *		creates an empty key list
 *	freekeylist (list)
 *		frees the memory associated with list
 *	keyiterate (action (), list)
 *		repeatdely perform action for each member of list
 *
 *  Additional information is in the following man pages:
 *	keylists (3)
 *
 *  Responsible:  Kay Leibrand			KLeibrand@solar.Stanford.EDU
 *
 *  Bugs:
 *	No definitions or support for key types time or complex.
 *  Revision history is at the end of the file.
 */

#ifndef SOI_KEY_INCL

/****************************************************************************/
/**************************  INCLUDE STATEMENTS  ****************************/
/****************************************************************************/

#ifndef SOI_VERSION_INCL
#include <soi_version.h>
#endif

#include <jsoc.h>
#include <stdio.h>
#include "timeio.h"
#include <soi_str.h>

/****************************************************************************/
/*******************************  TYPEDEFS  *********************************/
/****************************************************************************/
/*
 *  KEY: a data structure providing message passing services between
 *    interface-level and strategy-level functions
 * If change this struct must also change the XDR routine in pe_rpc_xdr.c.
 */

typedef struct key {
  struct key	*next;
  char		*name;
  int		type;
  void		*val;
} KEY;

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#define KEYTYP_VOID	(0)
#define KEYTYP_STRING	(1)
#define KEYTYP_BYTE	(-1)
#define KEYTYP_UBYTE	(-2)
#define KEYTYP_SHORT	(-3)
#define KEYTYP_USHORT	(-4)
#define KEYTYP_INT	(-5)
#define KEYTYP_UINT	(-6)
#define KEYTYP_LONG	(-7)
#define KEYTYP_ULONG	(-8)
#define KEYTYP_UINT64	(-9)
#define KEYTYP_UINT32	(-10)
#define KEYTYP_FLOAT	(-16)
#define KEYTYP_DOUBLE	(-17)
#define KEYTYP_FILEP	(-32)
#define KEYTYP_TIME	(-33)
#define KEYTYP_CMPLX	(-34)
#define KEYTYP_LAST	(-256)
#define SOI_KEY_VERSION_NUM	(1.0)
#define SOI_KEY_INCL	1

#define KEY_MAXSTR	(255)

/****************************************************************************/
/***************************  FUNCTION PROTOTYPES  **************************/
/****************************************************************************/
								 /*  key.c  */
extern void deletekey (KEY **list, char *key);
extern KEY *findkey (KEY *list, char *key);
extern void freekeylist (KEY **list);
extern int getkeytype (KEY *list, char *key);
extern void getkey_any (KEY *list, char *key, void *valptr);
extern char *GETKEY_str(KEY *params, char *key);
extern char *getkey_str (KEY *list, char *key);
extern FILE *getkey_fileptr (KEY *list, char *key);
extern int getkey_byte (KEY *list, char *key);
extern int getkey_ubyte (KEY *list, char *key);
extern int getkey_short (KEY *list, char *key);
extern int getkey_ushort (KEY *list, char *key);
extern int getkey_int (KEY *list, char *key);
extern unsigned int getkey_uint (KEY *list, char *key);
extern long getkey_long (KEY *list, char *key);
extern unsigned long getkey_ulong (KEY *list, char *key);
extern uint32_t getkey_uint32 (KEY *list, char *key);
extern uint64_t getkey_uint64 (KEY *list, char *key);
extern double getkey_float (KEY *list, char *key);
extern double getkey_double (KEY *list, char *key);
extern TIME getkey_time (KEY *list, char *key);
extern TIME getkey_time_interval(KEY *params, char *key);
extern int getkey_flag (KEY *list, char *key);
extern void keyiterate (void (*action)(), KEY *overlist);
extern void add_keys (KEY *inlist, KEY **tolist);
extern void delete_keys (KEY *inlist, KEY **fromlist);
extern KEY *newkeylist ();
extern void setkey_any (KEY **list, char *key, void *val, int type);
extern void setkey_str (KEY **list, char *key, char *val);
extern void addkey (KEY **list, char *key, void *val, int type);
extern void addkey_str (KEY **list, char *key, char *val);
extern void setkey_fileptr (KEY **list, char *key, FILE *val);
extern void setkey_byte (KEY **list, char *key, char val);
extern void setkey_ubyte (KEY **list, char *key, unsigned char val);
extern void setkey_short (KEY **list, char *key, short val);
extern void setkey_ushort (KEY **list, char *key, unsigned short val);
extern void setkey_int (KEY **list, char *key, int val);
extern void setkey_uint (KEY **list, char *key, unsigned int val);
extern void setkey_uint32 (KEY **list, char *key, uint32_t val);
extern void setkey_long (KEY **list, char *key, long val);
extern void setkey_ulong (KEY **list, char *key, unsigned long val);
extern void setkey_uint64 (KEY **list, char *key, uint64_t val);
extern void setkey_float (KEY **list, char *key, float val);
extern void setkey_double (KEY **list, char *key, double val);
extern void setkey_time (KEY **list, char *key, TIME val);
extern char *key_strdup (char *str);

extern char *getkey_overfilename(KEY *params, char *rootkey, char *kind);
extern char *GETKEY_overfilename(KEY *params, char *rootkey, char *kind);
extern char *getkey_infofilename(KEY *params, char *rootkey, char *kind);
extern char *GETKEY_infofilename(KEY *params, char *rootkey, char *kind);

#endif
