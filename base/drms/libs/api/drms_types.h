/**
\file drms_types.h
*/
#ifndef _DRMS_TYPES_H
#define _DRMS_TYPES_H

#include <math.h>
#include <float.h>
#include <limits.h>
#include <stdint.h>
#include "db.h"
#include "hcontainer.h"
#include "util.h"
#include "tagfifo.h"
#include "SUM.h"
#include "sum_rpc.h"

/* Constants */
#define DRMS_MAXNAMELEN        (32)
#define DRMS_MAXSERIESNAMELEN  (64)
#define DRMS_MAXOWNERLEN       DRMS_MAXNAMELEN
#define DRMS_MAXKEYNAMELEN     DRMS_MAXNAMELEN
#define DRMS_MAXLINKNAMELEN    DRMS_MAXNAMELEN
#define DRMS_MAXSEGNAMELEN     DRMS_MAXNAMELEN
/** \brief Maximum DRMS hash byte length */
#define DRMS_MAXHASHKEYLEN     (DRMS_MAXSERIESNAMELEN+22)
/** \brief Maximum byte length of unit string */
#define DRMS_MAXUNITLEN        (32)
#define DRMS_MAXQUERYLEN       (8192)
#define DRMS_MAXPATHLEN        (512)
#define DRMS_MAXFORMATLEN      (20)
/** \brief Maximum dimension of DRMS data */
#define DRMS_MAXRANK           (16)
/** \brief Maximum number of DRMS segments per record */
#define DRMS_MAXSEGMENTS       (255)
/** \brief Maximum DRMS comment byte length */
#define DRMS_MAXCOMMENTLEN     (255)
/** \brief Maximum byte length of DRMS segment file name */
#define DRMS_MAXSEGFILENAME    (256)
/** \brief Max number of keywords in the primary index. */
#define DRMS_MAXPRIMIDX        (15) 
/** \brief Max number of keywords to make db index. */
#define DRMS_MAXDBIDX          (10) 
/** \brief Max length of the string holding a keyword default value. */
#define DRMS_DEFVAL_MAXLEN     (1000) 
/** \brief The maximal length we allow of a chain of links. If a chain
longer than this is encountered we assume that there is a cyclic
link. */
#define DRMS_MAXLINKDEPTH  (20) 
#define DRMS_MAXHOSTNAME (128)

#include "drms_protocol.h"

/*************************** DRMS related types ************************/


/************************ Simple data types **********************/

/* Values of keywords belong to one of the following simple classes. 
 * RAW is not really a type, but is used when reading segment data
   to indicate that the data should be read in without and type
   conversino or scaling. */
/**
   @brief DRMS Data types
 */
typedef enum {
   /** \brief DRMS char type */
   DRMS_TYPE_CHAR, 
   /** \brief DRMS short type */
   DRMS_TYPE_SHORT, 
   /** \brief DRMS int type */
   DRMS_TYPE_INT, 
   /** \brief DRMS longlong type */
   DRMS_TYPE_LONGLONG, 
   /** \brief DRMS float type */
   DRMS_TYPE_FLOAT, 
   /** \brief DRMS double type */
   DRMS_TYPE_DOUBLE, 
   /** \brief DRMS time type */
   DRMS_TYPE_TIME, 
   /** \brief DRMS string type */
   DRMS_TYPE_STRING, 
   /** \brief DRMS raw type */
   DRMS_TYPE_RAW
} DRMS_Type_t;

#ifndef DRMS_TYPES_C
extern char *drms_type_names[];
#else
/**
   \brief Strings describing the supported DRMS types
   \sa ::DRMS_Type_t
*/
char *drms_type_names[] = {"char", "short", "int", "longlong", 
			   "float", "double", "time", "string", "raw"};
#endif

/** \brief DRMS type value */
union DRMS_Type_Value
{
  char char_val;
  short short_val;
  int   int_val;
  long long longlong_val;
  float float_val;
  double double_val;
  double time_val;
  char *string_val;
};

/** \brief DRMS type value reference */
typedef union DRMS_Type_Value DRMS_Type_Value_t;

/* DRMS_Type_Value_t is fine as long as you know the type.  But we also need a construct 
 * that is useful in generic code (type-unaware), for example, using the results of 
 * drms_getkey() in a call to drms_setkey().
 */
typedef struct DRMS_Value
{
  DRMS_Type_t type;
  DRMS_Type_Value_t value;
} DRMS_Value_t;

/* Default "missing" values for standard types. */
/** \brief DRMS char missing value */
#define DRMS_MISSING_CHAR     (SCHAR_MIN)
/** \brief DRMS short missing value */
#define DRMS_MISSING_SHORT    (SHRT_MIN)
/** \brief DRMS int missing value */
#define DRMS_MISSING_INT      (INT_MIN)
/** \brief DRMS longlong missing value */
#define DRMS_MISSING_LONGLONG (LLONG_MIN)
/** \brief DRMS float missing value */
#define DRMS_MISSING_FLOAT    (F_NAN)
#define _DRMS_IS_F_MISSING(v) (isnan(v))
/** \brief DRMS double missing value */
#define DRMS_MISSING_DOUBLE   (D_NAN)
#define _DRMS_IS_D_MISSING(v) (isnan(v))
/** \brief DRMS C string missing value */
#define DRMS_MISSING_STRING   ("")
/** \brief DRMS time missing value */
#define DRMS_MISSING_TIME     (-211087684800.0) 
/* equal to '-4712.01.01_12:00:00.000_UT' which is the time value used
   missing in the MDI/SDS system. */
#define _DRMS_IS_T_MISSING(v)  (isnan(value) || DRMS_MISSING_TIME == value)


#define DRMS_MAXTYPENAMELEN  (9)


/* */
#ifdef ICCCOMP
#pragma warning (disable : 1572)
#endif
static inline int drms_ismissing_char(char value)
{
   return (DRMS_MISSING_CHAR == value);
}

static inline int drms_ismissing_short(short value)
{
   return (DRMS_MISSING_SHORT == value);
}

static inline int drms_ismissing_int(int value)
{
   return (DRMS_MISSING_INT == value);
}

static inline int drms_ismissing_longlong(long long value)
{
   return (DRMS_MISSING_LONGLONG == value);
}

static inline int drms_ismissing_float(float value)
{
   return (_DRMS_IS_F_MISSING(value));
}

static inline int drms_ismissing_double(double value)
{
   return (_DRMS_IS_D_MISSING(value));
}

static inline int drms_ismissing_time(TIME value)
{
   return (_DRMS_IS_T_MISSING(value));
}

static inline int drms_ismissing_string(char *value)
{
   return (!value || *value == '\0');
}

#ifdef ICCCOMP
#pragma warning (default : 1572)
#endif

/****************************** DRMS Environment ***************************/


/******** Internal server data types. ********/


/* Database or DRMS client connection info. */
/** \brief DRMS Session struct */
struct DRMS_Session_struct
{ 
  int db_direct;    /* If db_direct == 1 then go directly to the DB
		       without passing calls through a DRMS server. */

  /******** server database connection for db_direct==1 ***********/
  DB_Handle_t *db_handle; /* Main database connection used for DRMS data. 
			     This connection runs a single serializable 
			     transaction for the entire session. */
  DB_Handle_t *stat_conn; /* Extra database connection. Only used by the 
			     DRMS server to update its status info 
			     in drms_session_table. */

  /* Unique session id. */
  long long sessionid;
  /* Session namespace */
  char *sessionns;

  /* sunum and directory of the storage unit holding the session 
     log files.  */  
  long long sunum;   /*  = sunum for the storage unit. */
  char *sudir; /* = storage unit directory for log files etc. */

  /* Client id. Used by clients to uniqely identify themselves within 
     a session. */
  int clientid;

  /**** client DRMS connection for db_direct==0 *******/
  char hostname[DRMS_MAXHOSTNAME]; 
  unsigned short port;
  int sockfd;
};

/** DRMS session struct reference */
typedef struct DRMS_Session_struct DRMS_Session_t;

/* Link list node for keeping track of temporary records in the server. 
   quick UGLY  hack. */
typedef struct DS_node_struct
{
  char *series;
  int n,nmax;
  long long *recnums;
  struct DS_node_struct *next;
} DS_node_t;

/** \brief DRMS environment struct */
struct DRMS_Env_struct
{
  DRMS_Session_t *session;     /* Database connection handle or socket
				  connection to DRMS server. */
  HContainer_t series_cache;   /* Series cache data structures. */
  HContainer_t record_cache;   /* Record cache data structures. */
  HContainer_t storageunit_cache; /* Storage unit cache. */
  DS_node_t *templist; /* List of temporary records created for each series 
			by this session. */

  int retention; /* retention in days. If not -1 then this value overrides
		   the retention time for storage units created by this 
		   session. Server only. */
  int query_mem; /* Maximum amount of memory (in MB) used by a single record
		    query. */
  /* Server data. */
  int archive;     /* If archive=1, then archive all SU (including log SU) */
  int server_wait; /* If server_wait=1 then sleep DRMS_ABORT_SLEEP
		      seconds before freeing env. This suposely gives
    		      threads a small window to finish cleanly. 
		      If server_wait=0, then free env immediately. 
		      This is primarily used by drms_run script and
		      drms_server that starts from within a module */

  /* if verbose != 0, then print out many diagnostic messages */
  int verbose; 

  char *dbpasswd;
  char *user;
  char *logfile_prefix;
  int dolog;
  int quiet;

  int clientcounter;
  pid_t tee_pid;

  pthread_mutex_t *drms_lock;

  /* SUM service thread. */
  pthread_t sum_thread;
  /* Tagged FIFOs for communicating with the SUM service thread. */
  tqueue_t *sum_inbox;
  tqueue_t *sum_outbox;
  long sum_tag; // tag of the request currently being served.
 
  /* Signal catching thread: */
  pthread_t signal_thread;
  sigset_t signal_mask;
  sigset_t old_signal_mask;
};

/** \brief DRMS environment struct reference */
typedef struct DRMS_Env_struct DRMS_Env_t;

typedef struct DRMS_ThreadInfo_struct
{
  DRMS_Env_t *env;
  int noshare; /* If 1: Commit (rollback) and start new transaction
		  every time a client disconnects successfully 
		  (unsuccessfully). This way we will release locks on tables
		  after every disconnect, and other database clients can get 
		  through. */
  int threadnum;
  int sockfd;
} DRMS_ThreadInfo_t;


/*** SUMS server thread definitions. ***/

#define DRMS_SUMALLOC  0
#define DRMS_SUMGET    1
#define DRMS_SUMPUT    2
#define DRMS_SUMCLOSE  3
#define DRMS_SUMABORT 99
#define DRMS_MAX_REQCNT MAXSUMREQCNT

/* Struct used for communication between service threads and
   the SUMS communication thread. */
typedef struct DRMS_SumRequest_struct
{
  int opcode; /* Used for command code in inbox and status in the outbox. */

  int reqcnt;
  double bytes;
  char *dsname;
  int mode;
  int group;
  int tdays;
  char *comment;

  char *sudir[DRMS_MAX_REQCNT];
  uint64_t sunum[DRMS_MAX_REQCNT];
  
  int dontwait;
} DRMS_SumRequest_t;




/*************************** Data records and Series ************************/


/* Constants used to indicate lifetime of records created with 
   drms_create_record(s) and drms_clone_record(s). 
   DRMS_TEMPORARY means that the records will only exist in the
   database until the end of the DRMS session in which they
   were created. 
*/
typedef enum {DRMS_PERMANENT, DRMS_TRANSIENT} DRMS_RecLifetime_t;

typedef enum DRMS_RecordSetType_struct
{
   kRecordSetType_DRMS = 0,
   kRecordSetType_DSDS,
   kRecordSetType_VOT,
   kRecordSetType_PlainFile
} DRMS_RecordSetType_t;

/** \brief DRMS-Record-set container */
struct DRMS_RecordSet_struct
{
  /** /brief Number of records in the set */
  int n;
  /** /brief The set of records */
  struct DRMS_Record_struct **records;
  /** /brief The number of subsets in the set */
  int ss_n;
  /** /brief The queries that generated the subsets */
  char **ss_queries;
  /** /brief The query types */
  DRMS_RecordSetType_t *ss_types;
  /** /brief Pointers to the beginning of each subset */
  struct DRMS_Record_struct **ss_starts;
};

/** \brief DRMS record struct reference */
typedef struct DRMS_RecordSet_struct DRMS_RecordSet_t;

/* Series-wide attributes. */
typedef struct DRMS_SeriesInfo_struct
{
  char seriesname[DRMS_MAXSERIESNAMELEN];      
  char description[DRMS_MAXCOMMENTLEN];
  char author[DRMS_MAXCOMMENTLEN];
  char owner[DRMS_MAXOWNERLEN];  /* Who is allowed to modify the series 
				  definition. FIXME: WE PROBABLY NEED 
				  PERMISSIONS TO INSERT NEW RECORDS. WE DON;T
				  WANT CASUAL USERS ACCIDENTALLY INSERTING 
				  BOGUS DATA IN THE WRONG SERIES. */
  int unitsize;   /* How many records to a storage unit. */
  int archive;    /* Should this series be archived? */
  int retention;  /* Default retention time in seconds. */
  int tapegroup;  /* Tapegroup of the series. */

  /* Prime key information. */
  int pidx_num;   /* Number of keywords in primary index. */
  struct DRMS_Keyword_struct *pidx_keywords[DRMS_MAXPRIMIDX]; 
                /* Pointers to keyword structs for keywords that
		   make up the primary key.*/
  /* DB index information. */
  int dbidx_num;   /* Number of keywords to make db index. */
  struct DRMS_Keyword_struct *dbidx_keywords[DRMS_MAXDBIDX]; 

}  DRMS_SeriesInfo_t;



/* Datastructure representing a single data record. */
/** \brief DRMS record struct */
struct DRMS_Record_struct
{
  struct DRMS_Env_struct *env;  /* Pointer to global DRMS environment. */

  long long recnum;                  /*** Unique record identifier. ***/
  long long sunum;                   /* Unique index of the storage unit associated 
	        		   with this record. */

  int init;                    /* Flag used internally by the series cache. */
  int readonly;                /* Flag indicating if record is read-only. */
  DRMS_RecLifetime_t lifetime; /* Flag indicating if record is session- 
				  temporary or permanent. */
  struct DRMS_StorageUnit_struct *su; /* Holds sudir (Storage unit directory).
				  	 Until the storage unit has been 
				 	 requested from SUMS this pointer is 
					 NULL. */
  int slotnum;          /* Number of the slot assigned within storage unit. */

  long long sessionid;       /* ID of the session that created this record. */
  char *sessionns;    /* namespace of the session that created this record. */

  DRMS_SeriesInfo_t *seriesinfo; /* Series to which this record belongs. */
  HContainer_t keywords;        /* Container of named keywords. */
  HContainer_t links;           /* Container of named links. */
  HContainer_t segments;        /* Container of named data segments. */
};

/** DRMS record struct reference */
typedef struct DRMS_Record_struct DRMS_Record_t;

/**************************** Keywords ***************************/
#define kRecScopeIndex_B 100
#define kRecScopeSlotted_B 1000

/* Ancillary keys */
#define kSlotAncKey_Index "_index"
#define kSlotAncKey_Epoch "_epoch"
#define kSlotAncKey_Step "_step"
#define kSlotAncKey_Unit "_unit"
#define kSlotAncKey_Base "_base"
#define kSlotAncKey_Vals "_vals"

extern const DRMS_Type_t kIndexKWType;
extern const char *kIndexKWFormat;

enum DRMS_ExportKeyword_enum
{
   kExport_ReqID = 0,
   kExport_Request,
   kExport_SegList,
   kExport_Requestor,
   kExport_Notification,
   kExport_ReqTime,
   kExport_ExpTime,
   kExport_DataSize,
   kExport_Format,
   kExport_FileNameFormat,
   kExport_Status
};

typedef enum DRMS_ExportKeyword_enum DRMS_ExportKeyword_t;

struct ExportStrings_struct
{
  DRMS_ExportKeyword_t kw;
  const char *str;
};

typedef struct ExportStrings_struct ExportStrings_t;

extern ExportStrings_t gExpStr[];

enum DRMS_SlotKeyUnit_enum
{
   /** */
   kSlotKeyUnit_Invalid = 0,
   kSlotKeyUnit_TSeconds,
   kSlotKeyUnit_Seconds,
   kSlotKeyUnit_Minutes,
   kSlotKeyUnit_Hours,
   kSlotKeyUnit_Days,
   kSlotKeyUnit_Degrees,
   kSlotKeyUnit_Arcminutes,
   kSlotKeyUnit_Arcseconds,
   kSlotKeyUnit_MAS,
   kSlotKeyUnit_Radians,
   kSlotKeyUnit_MicroRadians
};

typedef enum DRMS_SlotKeyUnit_enum DRMS_SlotKeyUnit_t;

enum DRMS_TimeEpoch_enum
{
   /** */
   kTimeEpoch_Invalid = 0,
   kTimeEpoch_DRMS,
   kTimeEpoch_MDI,
   kTimeEpoch_WSO,
   kTimeEpoch_TAI,
   kTimeEpoch_MJD
};

typedef enum DRMS_TimeEpoch_enum DRMS_TimeEpoch_t;

enum DRMS_RecScopeType_enum
{
   /** \brief Plain vanilla variable across records. */
   kRecScopeType_Variable = 0,
   /** \brief Keyword is constant across records. */
   kRecScopeType_Constant = 1,
   /** \brief This value is reserved for 'Index' keywords.  An index keyword
    *  is one whose per-record values are the integers 'nearest' to the real 
    *  number values of the corresponding non-index keyword.  If an index keyword
    *  is named TOBS_index, then the corresponding non-index keyword is TOBS. */
   kRecScopeType_Index = kRecScopeIndex_B,
   /** \brief A real-number keyword whose values are 'slotted'. 
    *  If TOBS_index is an index keyword, and if TOBS is the corresponding 
    *  TS_EQ-slotted keyword, TOBS has this slottype. */
   kRecScopeType_TS_EQ = kRecScopeSlotted_B,
   kRecScopeType_SLOT = kRecScopeSlotted_B + 1,
   kRecScopeType_ENUM = kRecScopeSlotted_B + 2,
   kRecScopeType_CARR = kRecScopeSlotted_B + 3
};

typedef enum DRMS_RecScopeType_enum DRMS_RecScopeType_t;

/* \brief DRMS Primary key type 
   From the DRMS module perspective (external to DRMS), 
   slotted keywords are DRMS prime.
   However, within DRMS, they are not. Each one is a non-prime key
   linked to an associated index keyword that IS DRMS prime. These
   index keywords are not readily visible outside of DRMS (but
   they can be accessed, just like any other keyword).

   To avoid confusion, index keywords are 'internal DRMS prime' and
   slotted keywords are 'external DRMS prime'.

   Functions that access 'internal DRMS prime' keywords provide kPkeysDRMSInternal as a parameter
   to indicate that they want DRMS keywords that contain the index keyword.
   Functions that provide kPkeysDRMSExternal as a parameter
   to indicate that they want DRMS keywords that contain the slotted keyword.
*/
enum DRMS_PrimeKeyType_enum
{
   kPkeysDRMSInternal = 0,
   kPkeysDRMSExternal
};

typedef enum DRMS_PrimeKeyType_enum DRMS_PrimeKeyType_t;

typedef struct  DRMS_KeywordInfo_struct
{
  char name[DRMS_MAXKEYNAMELEN];         /* Keyword name. */

  /************ Link keywords ***********/
  /* If this is an inherited keyword, islink is non-zero,
     and linkname holds the name of the link which points
     to the record holding the actual keyword value. */
  int  islink;
  char linkname[DRMS_MAXLINKNAMELEN];   /* Link to inherit from. */
  char target_key[DRMS_MAXKEYNAMELEN]; /* Keyword to inherit.  */

  /************ Regular keywords ********/
  DRMS_Type_t type;               /* Keyword type. */
  char format[DRMS_MAXFORMATLEN]; /* Format string for formatted input 
                                     and output. */
  char unit[DRMS_MAXUNITLEN];     /* Physical unit. */
  char description[DRMS_MAXCOMMENTLEN];
  DRMS_RecScopeType_t recscope;   /* If recscope == 0, then this keyword
				   * has values that vary across records.
				   * If recscope == 1, then this keyword
				   * is constant across all records.
				   * If recscope is not 0 or 1, then 
				   * the keyword is 'slotted'.  This means
				   * that the value of the keyword is
				   * placed into a slot (eg, rounded down)
				   * before being placed into the database. */
  int per_segment;                /* If per_segment=1 then this keyword has the
                                     has a different for each segment belonging
                                     to the record. If the keyword name is 
                                     "blah" then keywords pertaining to specific
                                     segments are referred to by "blah[0]",
                                     "blah[1]", etc. */
  /** \brief Indicates if this keyword is a DRMS primary keyword. 
   *  If 1, then the keyword is DRMS prime; otherwise, it is not prime.
   */
  int isdrmsprime;
} DRMS_KeywordInfo_t;

/**
DRMS keyword struct
*/
struct DRMS_Keyword_struct
{
  struct DRMS_Record_struct *record; /* The record this keyword belongs to.*/
  struct  DRMS_KeywordInfo_struct *info; /* Series-wide info. */
  DRMS_Type_Value_t value;               /* Keyword data. If this keyword is in
					  * the series cache, it contains the
					  * default value. */
};

/** \brief DRMS keyword struct reference */
typedef struct DRMS_Keyword_struct DRMS_Keyword_t;

/**************************** Links ***************************/

/* Links to other objects from which keyword values can be inherited. 
   A link often indicates that the present object was computed using the
   data in the object pointed to. */

typedef enum { STATIC_LINK, DYNAMIC_LINK } DRMS_Link_Type_t;

/* Series-wide Link info that does not vary from record to record. */
typedef struct DRMS_LinkInfo_struct
{
  char name[DRMS_MAXLINKNAMELEN];          /* Link name. */
  char target_series[DRMS_MAXSERIESNAMELEN]; /* Series pointed to. */  
  char description[DRMS_MAXCOMMENTLEN]; 
  DRMS_Link_Type_t type;               /* Static or dynamic. */

  /*** Dynamic link info ***/
  int pidx_num;  /* Number of keywords in primary index of target series. */
  DRMS_Type_t pidx_type[DRMS_MAXPRIMIDX]; /* Type of primary index values. */
  char *pidx_name[DRMS_MAXPRIMIDX];
} DRMS_LinkInfo_t;

/** \brief DRMS link struct */
struct DRMS_Link_struct
{
  struct DRMS_Record_struct *record;   /* The record this link belongs to. */
  DRMS_LinkInfo_t *info;

  /*** Static link info ***/
  long long recnum;          /* recnum = -1 marks a unset link */
                             /* For static link, it is the recnum of the target */

  /*** Dynamic link info ***/
  int isset;
  DRMS_Type_Value_t pidx_value[DRMS_MAXPRIMIDX]; /* Primary index values of
						    target record(s). */
};

/** \brief DRMS link struct reference */
typedef struct DRMS_Link_struct DRMS_Link_t;

/********************************** Data Segments ***************************/

/* An n-dimensional array (e.g. part or all of a segment array) 
   stored consecutively in memory. */
/** 
    \brief DRMS array struct 

    The ::DRMS_Array_t data stucture represents an n-dimensional array of scalar
    data.  It is used for internal memory access to data structures read
    from, or to be written to, record segments. The array data are stored in
    column-major order at the memory location pointed to by the @ref data
    element.

    The fields @ref israw, @ref bscale, and @ref bzero describe
    how the data contained in the array data structure relate to
    the "true" values they are supposed to represent.
    In the most frequently used case, @ref israw=0,
    the data stored in memory represent the "true" values of the array,
    and @ref bzero and @ref bscale contain
    the shift and scaling (if any) applied to the data when they were 
    read in from external storage. If @ref israw=1, then
    the data stored in memory represent the unscaled "raw" values of
    the array, and the true values may be obtained by applying the
    scaling transformations, if any:

    \code
    f(x) = bzero + bscale * x, if x != MISSING
         = MISSING           , if x == MISSING
    \endcode

    If the array struct contains data from a DRMS data segment, as returned
    by the functions
    ::drms_segment_readslice or ::drms_segment_read, then the 
    @ref parent_segment  field points to the data segment from which the
    array data originate.

    If the array contains a slice of the parent then the  @ref start field
    contains the starting indices of the slice in the parent array.
    For example: If an array contains the lower 2x2 elements of a 4x3 data 
    segment then the struct would contain
    
    \code
    array.naxis = 2
    array.axis = [2,2]
    array.start = [2,1]
    \endcode
*/
struct DRMS_Array_struct
{
  /* Array info: */ 
  /** \brief Datatype of the data elements. */
  DRMS_Type_t type;            
  /** \brief Number of dimensions. */
  int naxis;
  /** \brief Size of each dimension. */
  int axis[DRMS_MAXRANK];
  /** \brief Data stored in column major order. */
  void *data;                    

  /* Fields relating to scaling and slicing. */
  /** \brief Parent segment. */
  struct DRMS_Segment_struct *parent_segment; 
  /** \brief Zero point for parent->child mapping. */
  double bzero;
  /**
     \brief Do the values represent true values?
     Is this read in with type=DRMS_TYPE_RAW? 
     If israw==0 then shift and scaling have been 
     applied to the data and they represent the 
     "true" values. If israw==1 then no shift
     and scaling have been applied to the data. 
  */
  int israw;    
  /** \brief Slope for parent->child. */             
  double bscale;
  /** \brief Start offset of slice in parent. */
  int start[DRMS_MAXRANK];

  /* Private fields used for array index calculation etc. */
  /** \brief Dimension offset multipliers. */
  int dope[DRMS_MAXRANK];

  /* Private fields used for packed string arrays. */
  /** \brief String buffer used for packed string arrays. */
  char *strbuf;
  /** \brief Size of string buffer. */
  long long buflen;
}; 

/** \brief DRMS array struct reference*/
typedef struct DRMS_Array_struct DRMS_Array_t;



/*
     For an array-slice the parent_xxx variables describe how the array 
     maps into the parent array. parent_naxis is the number of dimensions 
     in the parent array. naxis is the number of dimensions of the slice, 
     i.e. parent_naxis minus the number of singleton dimensions where 
     parent_start[i] = parent_end[i]. The cutout is stored in "squeezed" 
     form with the singleton dimensions removed.
     Example: If the (integer) parent array is

          [ 1 4 7 ]
          [ 2 5 8 ]
          [ 3 6 9 ]

     with the Array_t struct

	  type = DRMS_TYPE_INT
          naxis = 2, 
          axis = {3,3}
          (int *) data = {1,2,3,4,5,6,7,8,9}
	  
     and we wanted to represent the last two elements [ 6 9 ] of the
     bottom row as a slice. The resulting Array_Slice_t structure would have 

     
	  array.type = DRMS_TYPE_INT
          array.naxis = 1, 
          array.axis = {2} , 
          array.data = {6, 9}           
          parent_naxis = 2
          parent_axis = {3,3}
	  parent_type = DRMS_TYPE_INT
	  parent_start = {2, 1}
          parent_end = {2, 2}
          bscale = 1.0
          bzero  = 0.0 

     data points to an array holding the data of the cutout stored
     in column major order.     
 
     In general, the mapping from parent to child can be described as

      child(0:axis[i]-1,...,0:axis[naxis-1]) = 
          squeeze(bzero+bscale*parent(s0:e0,s1:e1,...,sn:en)) ,

     where si = parent_start[i], ei = parent_end[i], 
     n+1 = parent_naxis >= naxis, and squeeze(*) is the operator 
     that compacts the array by removing singleton dimensions 
     where si=ei.
  */


/* The data descriptors hold basic information about the in-memory
   representation of the data. */
/**
   \brief DRMS segment scope types
*/
typedef enum  {
   /** \brief Indicates data is constant across records */
   DRMS_CONSTANT,
   /** \brief Indicates data dimension structure is constant across records */
   DRMS_VARIABLE, 
   /** \brief Indicates data dimension structure varies across records */
   DRMS_VARDIM
} DRMS_Segment_Scope_t;

#ifndef DRMS_TYPES_C
extern char *drms_segmentscope_names[];
#else
char *drms_segmentscope_names[] = {"constant", "variable", "vardim"};

#endif


/***********************************************************
 *
 * Segments
 *
 ***********************************************************/

/* A data segment is typically an n-dimensional array of a simple type.
   It can also be a "generic" segment which is just a file 
   (structure-less as far as DRMS is concerned). */

/** \brief DRMS segment dimension info struct */
struct DRMS_SegmentDimInfo_struct {
  /** \brief Number of dimensions (rank) */
  int naxis;
  /** \brief Length of each dimension */
  int axis[DRMS_MAXRANK];
};

/** \brief DRMS segment dim info struct reference */
typedef struct DRMS_SegmentDimInfo_struct DRMS_SegmentDimInfo_t;

/** \brief DRMS segment info struct */
struct DRMS_SegmentInfo_struct {
						   /*  Context information:  */
  /** \brief Segment name */
  char name[DRMS_MAXSEGNAMELEN];
  /** \brief Segment number in record */
  int segnum;
  /** \brief  Description string */
  char description[DRMS_MAXCOMMENTLEN];
		/************  Link segments  ***********/
		/*  If this is an inherited segment, islink is non-zero,
	and linkname holds the name of the link which points to the record
						 holding the actual segment. */
  /** \brief Non-0 if segment inherited */
  int  islink;
  /** \brief Link to inherit from */
  char linkname[DRMS_MAXLINKNAMELEN];
  /** \brief Segment to inherit */
  char target_seg[DRMS_MAXSEGNAMELEN];
  /** \brief Datatype of data elements */
  DRMS_Type_t type;
  /** \brief Number of dimensions (rank) */
  int naxis;
  /** \brief Physical unit */
  char unit[DRMS_MAXUNITLEN];
  /** \brief Storage protocol */
  DRMS_Protocol_t protocol;
  /** \brief Const, Varies, or DimsVary */
  DRMS_Segment_Scope_t scope;
  /** \brief Record number where constant segment is stored */
  long long cseg_recnum;
};

/** \brief DRMS segment info struct reference */
typedef struct DRMS_SegmentInfo_struct DRMS_SegmentInfo_t;

/** 
    \brief DRMS segment struct 

    A DRMS data segment corresponds to a named file, typically containing an
    n-dimensional scalar array. (It can also be a "generic" segment, which is
    just an unstructured file as far as DRMS is concerned.) One or more segments
    constitute the external data part(s) of the DRMS record pointed to by the
    @ref record field. The 
    @ref info field points to a structure containing attributes common to all
    records in a series, while the segment structure itself contains the fields
    @ref axis and @ref blocksize that can vary from record to record if
    @ref scope=::DRMS_VARDIM.

    The @ref protocol field determines the external storage format used for
    storing segment data. Only protocols ::DRMS_BINARY, ::DRMS_BINZIP, ::DRMS_FITS,
    ::DRMS_FITZ, ::DRMS_GENERIC, and ::DRMS_TAS are fully supported in the base 
    DRMS system (NetDRMS). Protocol ::DRMS_DSDS is a
    special protocol for dealing with the format of the Stanford SOI-MDI
    Data Sorage and Distribution System (DSDS) and requires support outside
    the DRMS library. Protocol ::DRMS_LOCAL likewise supports the DSDS file-format
    and requires a non-NetDRMS library. It differs from ::DRMS_DSDS in that
    it does not depend on the presence of DSDS - it merely allows the user
    to operate on files external to DSDS (eg., files that may reside on a LOCAL 
    hard disk) that happen to have the file format that DSDS uses.
    ::DRMS_GENERIC and ::DRMS_MSI are also reserved for unsupported
    data formats. In particular, the DRMS_GENERIC protocol is used to refer
    to any unstructured data format or data formats of unknown structure.

    Data storage for ::DRMS_FITS is in minimal simple FITS files, without
    extensions and with only the compliance- and structure-defining keywords
    (SIMPLE, BITPIX, NAXIS, NAXISn, and END, and optionally BLANK, BSCALE and
    BZERO) in the headers. All other ancillary data are to be found in the DRMS
    record. For the ::DRMS_FITZ protocol, the representation is similar except
    that the entire FITS file is compressed with Rice compression. (Note that
    because the memory representation for the data supported through the
    API functions ::drms_segment_read is the ::DRMS_Array_t struct, which has a
    maximum rank of 16, FITS hypercubes of dimension > 16 are not supported.)

    For the
    ::DRMS_BINARY protocol, the data are written in a binary format, in which
    the first 8 bytes are the characters "DRMS RAW", the next @c 8(@c n+1)
    are little-endian integer representations of the data type, rank, and
    dimensions of the @a n axes, and the remainder the binary data in
    little-endian format. For the ::DRMS_BINZIP protocol the represntation is
    the same, except that the file is gzip compressed. The ::DRMS_TAS protocol
    (for "Tiled Array Storage") is described elsewhere, if at all. It is
    designed for use with data segments that are small compared with the
    size of the full data records, in order to minimize file access without
    keeping all of the segment data in the relational database, by concatenating
    multiple segments in the external format. The segment @ref blocksize member
    is for use with the ::DRMS_TAS protocol.

    Segment data types refer to the scalar data type for the segment, and
    should be mostly self-explanatory. ::DRMS_TYPE_TIME is a special case of
    double-precision floating point values representing elapsed time from
    a fixed epoch. Arithmetic is the same as for ::DRMS_TYPE_DOUBLE, only the
    format for string representations differs from that for normal floating-point
    data; see ::sprint_time. Data of type ::DRMS_TYPE_STRING are
    null-terminated byte sequences of any length.  Data type ::DRMS_TYPE_STRING is
    not supported by the protocols ::DRMS_BINARY, ::DRMS_BINZIP, ::DRMS_FITS, nor
    ::DRMS_FITZ. Whether it is properly supported by the ::DRMS_TAS protocol is
    doubtful. The data type ::DRMS_TYPE_RAW is used to describe data that are not
    to be converted on read from the type of their external representation,
    which must then be established for type-specific operations. It should
    be used for ::DRMS_Array_t structures only, not for DRMS segments.

    The scope of a segment can take on three values. The normal scope is
    expected to be ::DRMS_VARIABLE, for which the particular segment for every
    record has exactly the same structure (rank and dimensions),
    only the actual data values vary from one record to another. (Note that
    different segments of a record, however, need not have the same structure
    as one another.) If the scope is ::DRMS_VARDIM, then the dimensions and
    even rank of the particular segment may vary from one record to another,
    although other features of the segment, in particular the data type,
    must still be the same. Scope ::DRMS_CONSTANT is used to describe a data
    segment that is constant for all records. It can be used for example to
    describe a location index array, or a constant calibration array that
    applies to all records in the series, so that it can be made available
    to any record without having to store multiple instances externally.

*/
struct DRMS_Segment_struct {
  /** \brief  The record this segment belongs to */
  struct DRMS_Record_struct *record; 
  /** \brief Contains attributes common to all records in a series */
  DRMS_SegmentInfo_t *info;
  /** \brief Storage file name  */
  char filename[DRMS_MAXSEGFILENAME];
  /** \brief Size of each dimension */
  int axis[DRMS_MAXRANK];
  /** \brief Block sizes for TAS storage */
  int blocksize[DRMS_MAXRANK];
};

/** \brief DRMS segment struct reference */
typedef struct DRMS_Segment_struct DRMS_Segment_t;

#define DRMS_READONLY  1
#define DRMS_READWRITE 2
typedef struct DRMS_StorageUnit_struct {
  struct DRMS_SeriesInfo_struct *seriesinfo; /* global series info. */
  int mode;  /* Indicates if SU is open for DRMS_READONLY or DRMS_READWRITE */
  long long sunum; /* Unique index of this storage unit. */
  char sudir[DRMS_MAXPATHLEN];  /* Directory of this storage unit. */  
  int refcount; /* Number of Records pointing to this storage unit struct. */
  int nfree;     /* Number of free record slots in this storage unit. 
		    Total number of slots is in seriesinfo.unitsize. */
  char *state; /* Bytemap of slot states. Valid states are DRMS_SLOT_FREE,
                  DRMS_SLOT_FULL, and DRMS_SLOT_TEMP. The latter means that
		  the record should be committed to SUMS as temporary 
		  regardless of the archive method in the series definition. 
		  A storage unit will be archived as temporary of it is 
		  defined thus in the series definition or if all non-empty 
		  slots have state DRMS_SLOT_TEMP. */
  long long *recnum; /* Record numbers of records occupying the slots of this 
		   storage unit. Only used on the server side to delete 
		   temporary records at the end of a session. */
} DRMS_StorageUnit_t;

/************ Exporting FITS files ***************/
typedef enum DRMS_KeyMapClass_enum {
   kKEYMAPCLASS_DEFAULT = 0,
   kKEYMAPCLASS_DSDS = 1,
   kKEYMAPCLASS_LOCAL = 2,
   kKEYMAPCLASS_SSW = 3,
   kKEYMAPCLASS_GNG = 4,
   kKEYMAPCLASS_NUMTABLESPLUSONE
   /* xxx etc*/
} DRMS_KeyMapClass_t;

/** \brief DRMS keymap struct */
struct DRMS_KeyMap_struct {
  HContainer_t int2ext;
  HContainer_t ext2int;
}; 

/** \brief DRMS keymap struct reference */
typedef struct DRMS_KeyMap_struct DRMS_KeyMap_t;

/*********** Various utility functions ****************/
DRMS_Type_t drms_str2type(const char *);

/**
   \brief Return a string representation of a ::DRMS_Type_t value.

   \param type The ::DRMS_Type_t whose string representation is to
   be returned.
   \return String representation of the specified ::DRMS_Type_t value.
*/
const char *drms_type2str(DRMS_Type_t type);
void drms_missing(DRMS_Type_t type, DRMS_Type_Value_t *val);
void drms_missing_vp(DRMS_Type_t type, void *val);
int drms_copy_db2drms(DRMS_Type_t drms_type, DRMS_Type_Value_t *drms_dst, 
		      DB_Type_t db_type, char *db_src);
void drms_copy_drms2drms(DRMS_Type_t type, DRMS_Type_Value_t *dst, 
			 DRMS_Type_Value_t *src);
DB_Type_t drms2dbtype(DRMS_Type_t type);
int drms_sizeof(DRMS_Type_t type);
void *drms_addr(DRMS_Type_t type, DRMS_Type_Value_t *val);
int drms_strval(DRMS_Type_t type, DRMS_Type_Value_t *val, char *str);
int drms_sprintfval(char *dst, DRMS_Type_t type, DRMS_Type_Value_t *val, int internal);
int drms_sprintfval_format(char *dst, DRMS_Type_t type, DRMS_Type_Value_t *val, 
			   char *format, int internal);
int drms_printfval (DRMS_Type_t type, DRMS_Type_Value_t *val);
int drms_fprintfval(FILE *keyfile, DRMS_Type_t type, DRMS_Type_Value_t *val);
int drms_sscanf_int (char *str, 
		     DRMS_Type_t dsttype, 
		     DRMS_Type_Value_t *dst,
		     int silent);
int drms_sscanf(char *str, DRMS_Type_t dsttype, DRMS_Type_Value_t *dst);

/* Scalar conversion functions. */
int drms_convert(DRMS_Type_t dsttype, DRMS_Type_Value_t *dst, 
		 DRMS_Type_t srctype, DRMS_Type_Value_t *src);
int drms_convert_array(DRMS_Type_t dsttype, char *dst, 
		       DRMS_Type_t srctype, char *src);
char drms2char(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
short drms2short(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
int drms2int(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
long long drms2longlong(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
long long conv2longlong(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
float drms2float(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
double drms2double(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
double drms2time(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
char *drms2string(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);

/* Misc. utility functions. */
int drms_printfval_raw(DRMS_Type_t type, void *val);
int drms_fprintfval_raw(FILE *keyfile, DRMS_Type_t type, void *val);
void drms_byteswap(DRMS_Type_t type, int n, char *val);
void drms_memset(DRMS_Type_t type, int n, void *array, DRMS_Type_Value_t val);
int drms_daxpy(DRMS_Type_t type, const double alpha, DRMS_Type_Value_t *x, 
	       DRMS_Type_Value_t *y );
int drms_equal(DRMS_Type_t type, DRMS_Type_Value_t *x, DRMS_Type_Value_t *y);

/* Frees value, only if it is of type string. */
static inline void drms_value_free(DRMS_Value_t *val)
{
   if (val && val->type == DRMS_TYPE_STRING && val->value.string_val)
   {
      free(val->value.string_val);
      val->value.string_val = NULL;
   }
}

/* T  - Data type (DRMS_Type_t)
 * IV - Input data value (void *)
 * OV - Output value (DRMS_Value_t)
 */
#define DRMS_VAL_SET(T, IV, OV)                                 \
{                                                               \
   int vserror = 0;                                             \
   switch (T)                                                   \
   {                                                            \
      case DRMS_TYPE_CHAR:                                      \
	OV.value.char_val = *(char *)IV;                     \
	break;                                                  \
      case DRMS_TYPE_SHORT:                                     \
	OV.value.short_val = *(short *)IV;                   \
	break;                                                  \
      case DRMS_TYPE_INT:                                       \
	OV.value.int_val = *(int *)IV;                       \
	break;                                                  \
      case DRMS_TYPE_LONGLONG:                                  \
	OV.value.longlong_val = *(long long *)IV;            \
	break;                                                  \
      case DRMS_TYPE_FLOAT:                                     \
	OV.value.float_val = *(float *)IV;                   \
	break;                                                  \
      case DRMS_TYPE_DOUBLE:                                    \
	OV.value.double_val = *(double *)IV;                 \
	break;                                                  \
      case DRMS_TYPE_TIME:                                      \
	OV.value.time_val = *(double *)IV;                   \
	break;                                                  \
      case DRMS_TYPE_STRING:                                    \
	OV.value.string_val = strdup((char *)IV);            \
	break;                                                  \
      default:                                                  \
	fprintf(stderr, "Invalid drms type: %d\n", (int)T);     \
	vserror = 1;                                            \
   }                                                            \
   if (!vserror)                                                \
   {                                                            \
      OV.type = T;                                              \
   }                                                            \
}

/* Arithmetic operations */
/* XXX - need to flesh these out, or else this DRMS_Type_Value concept doesn't 
 * seem to be that useful. 
 */

/* need to check for overflow */


/* This appears to get inlined */
static inline DRMS_Value_t drms_val_add(DRMS_Value_t *a, DRMS_Value_t *b)
{
   DRMS_Value_t ans;

   if (a->type == b->type)
   {
      ans.type = a->type;

      switch(a->type)
      {
	 case DRMS_TYPE_CHAR:
	   {
	      int sum = (a->value).char_val + (b->value).char_val;

	      if (sum >= SCHAR_MIN && sum <= SCHAR_MAX)
	      {
		 ans.value.char_val = (char)sum;
	      }
	   }
	   break;
	 case DRMS_TYPE_SHORT:
	   {
	      int sum = (a->value).short_val + (b->value).short_val;

	      if (sum >= SHRT_MIN && sum <= SHRT_MAX)
	      {
		 ans.value.short_val = (short)sum;
	      }
	   }
	   break;
	 case DRMS_TYPE_INT:
	   {
	      long long sum = (a->value).int_val + (b->value).int_val;

	      if (sum >= INT_MIN && sum <= INT_MAX)
	      {
		 ans.value.int_val = (int)sum;
	      }
	   }
	   break;
	 case DRMS_TYPE_LONGLONG:
	   {
	      long long sum = (a->value).longlong_val + (b->value).longlong_val;
	      ans.value.longlong_val = sum;
	   }
	   break;
	 case DRMS_TYPE_FLOAT:
	   {
	      double sum = (a->value).float_val + (b->value).float_val;

	      if (sum >= -FLT_MAX && sum <= FLT_MAX)
	      {
		 ans.value.float_val = (float)sum;
	      }
	   }
	   break;
	 case DRMS_TYPE_DOUBLE:
	   {
	      double sum = (a->value).double_val + (b->value).double_val;
	      ans.value.double_val = sum;
	   }
	   break;
	 default:
	   fprintf(stderr, "drms_val_add(): unsupported type.\n");
      }
   }
   else
   {
      fprintf(stderr, "drms_val_add(): type mismatch.\n");
   }

   return ans;
}

static inline DRMS_Value_t drms_val_div(DRMS_Value_t *a, DRMS_Value_t *b)
{
   DRMS_Value_t ans;

   if (a->type == b->type)
   {
      ans.type = a->type;

      switch(a->type)
      {
	 case DRMS_TYPE_CHAR:
	   {
	      int res = (int)((a->value).char_val) / (int)((b->value).char_val);

	      if (res >= SCHAR_MIN && res <= SCHAR_MAX)
	      {
		 ans.value.char_val = (char)res;
	      }
	   }
	   break;
	 case DRMS_TYPE_SHORT:
	   {
	      int res = (int)((a->value).short_val) / (int)((b->value).short_val);

	      if (res >= SHRT_MIN && res <= SHRT_MAX)
	      {
		 ans.value.short_val = (short)res;
	      }
	   }
	   break;
	 case DRMS_TYPE_INT:
	   {
	      long long res = (a->value).int_val / (b->value).int_val;

	      if (res >= INT_MIN && res <= INT_MAX)
	      {
		 ans.value.int_val = (int)res;
	      }
	   }
	   break;
	 case DRMS_TYPE_LONGLONG:
	   {
	      long long res = (a->value).longlong_val / (b->value).longlong_val;
	      ans.value.longlong_val = res;
	   }
	   break;
	 case DRMS_TYPE_FLOAT:
	   {
	      double res = (double)((a->value).float_val) / (double)((b->value).float_val);

	      if (res >= -FLT_MAX && res <= FLT_MAX)
	      {
		 ans.value.float_val = (float)res;
	      }
	   }
	   break;
	 case DRMS_TYPE_DOUBLE:
	   {
	      double res = (a->value).double_val / (b->value).double_val;
	      ans.value.double_val = res;
	   }
	   break;
	 default:
	   fprintf(stderr, "drms_val_div(): unsupported type.\n");
      }
   }
   else
   {
      fprintf(stderr, "drms_val_add(): type mismatch.\n");
   }

   return ans;
}

#endif


