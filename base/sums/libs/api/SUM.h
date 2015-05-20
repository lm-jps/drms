/* SUM.h */
#ifndef SUM_INCL
#define GET_FIX_VER 2	/* DRMS fix level for calling multi SUM_get() */
			/* This is tested for in sum_svc_proc.c getdo_1() */
#if defined SUMDC
#define SUM_VERSION_NUM    (1.0)
#if defined DCS0
#define SUMPGPORT "5430"
#elif defined DCS1
#define SUMPGPORT "5431"
#elif defined DCS2
#define SUMPGPORT "5432"
#endif

#define TAPEVIEWERNAME "t50view"
#define SUM_STOP_NOT "/usr/local/logs/SUM/SUM_STOP_NOT"
#elif defined SUMT120
#define SUM_VERSION_NUM    (1.0)
#define SUMPGPORT "5434"
#define TAPEVIEWERNAME "t120view"
#define SUM_STOP_NOT "/usr/local/logs/SUM/SUM_STOP_NOT"
#elif defined SUMT950
#define SUM_VERSION_NUM    (1.0)
#define SUMPGPORT "5434"
#define TAPEVIEWERNAME "t950view"
#define SUM_STOP_NOT "/usr/local/logs/SUM/SUM_STOP_NOT"
#endif

#define LEV1VIEWERNAME "lev1view"
#define TAPEHOST "k1"		//JSOC pipeline machine with tape_svc
#define SUMSVCHOST "k1"		//JSOC pipeline machine running sum_svc

#include <serverdefs.h>
#include <jsoc.h>
#include <stdint.h>	/* need to repeat this for pro-c precompiler */
#include "foundation.h"

typedef uint32_t SUMID_t;

/* Bitmap of modes */
#define ARCH 1          /* archive the storage unit to tape */
#define TEMP 2          /* the storage unit is temporary */
#define PERM 4          /* the storage unit is permanent */
#define TOUCH 8         /* tdays gives the storage unit retention time */
#define RETRIEVE 16     /* retrieve from tape */
#define NORETRIEVE 32   /* don't retrieve from tape */
#define FULL 1024	/* also set this to get full info from DB query */
#define TAPERDON 2048   /* a SUM_get() call has a RESULT_PEND for this SUM_t */

#define MAXSTR 256	/* max size of a string */
#define MAX_TAPE_FN 7000     /* max allowed file number on a tape */


/* SUM error numbers */
#define NO_TAPE_IN_GROUP 2 /* SUMLIB_TapeFindGroup() can't find  an open */
			   /* tape in the group with enough storage */
#define NO_CLNTTCP_CREATE 3 /* tape_svc cannot make a handle back to tapearc */
			    /* may be extended to general can't make handle */
#define SUM_SUNUM_NOT_LOCAL 4 /* SUM_Info() got error on non local sunum */
#define SUM_RESPPROG_ERR 5    /* set_client_handle() err to respond to caller*/
#define SUM_TAPE_SVC_OFF 6 /* the tape_svc has been turned off line */
//NOTE: RESULT_PEND 32 in sum_rpc.h

#define SUM_INCL
#endif

