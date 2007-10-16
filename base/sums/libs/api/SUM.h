/* SUM.h */
#ifndef SUM_INCL
#ifdef SUMDC
#define SUM_VERSION_NUM    (1.0)
#define SUMSERVER "dcs0.Stanford.EDU"
#define SUMDB "dcs0"
#define TAPEVIEWERNAME "t50view"
#define SUM_STOP_NOT "/usr/local/logs/SUM/SUM_STOP_NOT"
#else
#define SUM_VERSION_NUM    (1.0)
#define SUMSERVER "d00.Stanford.EDU"
#define SUMDB "hmidb"
#define TAPEVIEWERNAME "t120view"
#define SUM_STOP_NOT "/usr/local/logs/SUM/SUM_STOP_NOT"
#endif

#include <jsoc.h>
#include <stdint.h>	/* need to repeat this for pro-c precompiler */

typedef uint32_t SUMID_t;

/* Bitmap of modes */
#define ARCH 1          /* archive the storage unit to tape */
#define TEMP 2          /* the storage unit is temporary */
#define PERM 4          /* the storage unit is permanent */
#define TOUCH 8         /* tdays gives the storage unit retention time */
#define RETRIEVE 16     /* retrieve from tape */
#define NORETRIEVE 32   /* don't retrieve from tape */
#define FULL 1024	/* also set this to get full info from DB query */

#define MAXSTR 256     /* max size of a string */

/* SUM error numbers */
#define NO_TAPE_IN_GROUP 2 /* SUMLIB_TapeFindGroup() can't find  an open */
			   /* tape in the group with enough storage */
#define NO_CLNTTCP_CREATE 3 /* tape_svc cannot make a handle back to tapearc */
			    /* may be extended to general can't make handle */

#define SUM_INCL
#endif

