#include <jsoc.h>
#include <stdio.h>
#include "timeio.h"
#include "uthash.h"

/****************************************************************************/
/*******************************  TYPEDEFS  *********************************/
/****************************************************************************/
/*
 *  KEYU: a data structure providing message passing services between
 *    interface-level and strategy-level functions
 * If change this struct must also change the XDR routine in pe_rpc_xdr.c.
 */

typedef struct keyU {
  char		*name;
  int		type;
  void		*val;
  UT_hash_handle hh;
} KEYU;

/****************************************************************************/
/****************************  DEFINE STATEMENTS  ***************************/
/****************************************************************************/

#define KEYTYP_VOID_U	(0)
#define KEYTYP_STRING_U	(1)
#define KEYTYP_BYTE_U	(-1)
#define KEYTYP_UBYTE_U	(-2)
#define KEYTYP_SHORT_U	(-3)
#define KEYTYP_USHORT_U	(-4)
#define KEYTYP_INT_U	(-5)
#define KEYTYP_UINT_U	(-6)
#define KEYTYP_LONG_U	(-7)
#define KEYTYP_ULONG_U	(-8)
#define KEYTYP_UINT64_U	(-9)
#define KEYTYP_UINT32_U	(-10)
#define KEYTYP_FLOAT_U	(-16)
#define KEYTYP_DOUBLE_U	(-17)
#define KEYTYP_FILEP_U	(-32)
#define KEYTYP_TIME_U	(-33)
#define KEYTYP_CMPLX_U	(-34)
#define KEYTYP_LAST_U	(-256)

#define KEY_MAXSTR	(255)

/****************************************************************************/
/***************************  FUNCTION PROTOTYPES  **************************/
/****************************************************************************/
								/*  keyU.c  */
extern void deletekeyU (KEYU **list, char *key);
extern KEYU *findkeyU (KEYU *list, char *key);
extern void freekeylistU (KEYU **list);
extern int getkeytypeU (KEYU *list, char *key);
extern void getkey_anyU (KEYU *list, char *key, void *valptr);
extern char *GETKEY_strU(KEYU *params, char *key);
extern char *getkey_strU (KEYU *list, char *key);
extern FILE *getkey_fileptrU (KEYU *list, char *key);
extern int getkey_byteU (KEYU *list, char *key);
extern int getkey_ubyteU (KEYU *list, char *key);
extern int getkey_shortU (KEYU *list, char *key);
extern int getkey_ushortU (KEYU *list, char *key);
extern int getkey_intU (KEYU *list, char *key);
extern unsigned int getkey_uintU (KEYU *list, char *key);
extern long getkey_longU (KEYU *list, char *key);
extern unsigned long getkey_ulongU (KEYU *list, char *key);
extern uint32_t getkey_uint32U (KEYU *list, char *key);
extern uint64_t getkey_uint64U (KEYU *list, char *key);
extern double getkey_floatU (KEYU *list, char *key);
extern double getkey_doubleU (KEYU *list, char *key);
extern TIME getkey_timeU (KEYU *list, char *key);
extern TIME getkey_time_intervalU(KEYU *params, char *key);
extern int getkey_flagU (KEYU *list, char *key);
extern void keyiterateU (void (*action)(), KEYU *overlist);
extern void delete_keysU (KEYU *inlist, KEYU **fromlist);
extern KEYU *newkeylistU ();
extern void add_keysU (KEYU *inlist, KEYU **tolist);
extern void add_key2keyU (KEY *inlist, KEYU **tolist);
extern void setkey_anyU (KEYU **list, char *key, void *val, int type);
extern void setkey_strU (KEYU **list, char *key, char *val);
extern void setkey_fileptrU (KEYU **list, char *key, FILE *val);
extern void setkey_byteU (KEYU **list, char *key, char val);
extern void setkey_ubyteU (KEYU **list, char *key, unsigned char val);
extern void setkey_shortU (KEYU **list, char *key, short val);
extern void setkey_ushortU (KEYU **list, char *key, unsigned short val);
extern void setkey_intU (KEYU **list, char *key, int val);
extern void setkey_uintU (KEYU **list, char *key, unsigned int val);
extern void setkey_uint32U (KEYU **list, char *key, uint32_t val);
extern void setkey_longU (KEYU **list, char *key, long val);
extern void setkey_ulongU (KEYU **list, char *key, unsigned long val);
extern void setkey_uint64U (KEYU **list, char *key, uint64_t val);
extern void setkey_floatU (KEYU **list, char *key, float val);
extern void setkey_doubleU (KEYU **list, char *key, double val);
extern void setkey_timeU (KEYU **list, char *key, TIME val);

extern char *getkey_overfilenameU(KEYU *params, char *rootkey, char *kind);
extern char *GETKEY_overfilenameU(KEYU *params, char *rootkey, char *kind);
extern char *getkey_infofilenameU(KEYU *params, char *rootkey, char *kind);
extern char *GETKEY_infofilenameU(KEYU *params, char *rootkey, char *kind);
