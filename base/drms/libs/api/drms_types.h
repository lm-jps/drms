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
#ifdef REALSUMS
#include "SUM.h"
#include "sum_rpc.h"
#endif

/* Constants */
#define DRMS_MAXNAMELEN        (32)
#define DRMS_MAXHASHKEYLEN     (DRMS_MAXNAMELEN+22)
#define DRMS_MAXUNITLEN        (32)
#define DRMS_MAXQUERYLEN       (8192)
#define DRMS_MAXPATHLEN        (512)
#define DRMS_MAXFORMATLEN      (20)
#define DRMS_MAXRANK           (16)
#define DRMS_MAXSEGMENTS       (255)
#define DRMS_MAXCOMMENTLEN     (255)
#define DRMS_MAXSEGFILENAME    (256)
#define DRMS_MAXPRIMIDX        (5) /* Max number of keywords in the primary index. */
#define DRMS_DEFVAL_MAXLEN     (1000) /* Max length of the string holding a keyword default value. */

#define DRMS_MAXLINKDEPTH  (20) /* The maximal length we allow of a 
				   chain of links. If a chain longer than this
				   is encountered we assume that there is a 
				   cyclic link. */
#define DRMS_MAXHOSTNAME (128)

#include "drms_protocol.h"

/*************************** DRMS related types ************************/


/************************ Simple data types **********************/

/* Values of keywords belong to one of the following simple classes. 
 * RAW is not really a type, but is used when reading segment data
   to indicate that the data should be read in without and type
   conversino or scaling. */

typedef enum {DRMS_TYPE_CHAR, DRMS_TYPE_SHORT, DRMS_TYPE_INT, 
	      DRMS_TYPE_LONGLONG, DRMS_TYPE_FLOAT, DRMS_TYPE_DOUBLE, 
	      DRMS_TYPE_TIME, DRMS_TYPE_STRING, DRMS_TYPE_RAW} DRMS_Type_t;

#ifndef DRMS_TYPES_C
extern char *drms_type_names[];
#else
char *drms_type_names[] = {"char", "short", "int", "longlong", 
			   "float", "double", "time", "string", "raw"};
#endif

typedef union DRMS_Type_Value
{
  char char_val;
  short short_val;
  int   int_val;
  long long longlong_val;
  float float_val;
  double double_val;
  double time_val;
  char *string_val;
} DRMS_Type_Value_t;

/* DRMS_Type_Value_t is fine as long as you know the type.  But we also need a construct 
 * that is useful in generic code (type-unaware), for example, using the results of 
 * drms_getkey() in a call to drms_setkey().
 */
typedef struct DRMS_Value
{
  DRMS_Type_t type;
  DRMS_Type_Value_t value;
} DRMS_Value_t;

static union { uint32_t rep; float val; } __f_nan__ __attribute_used__ = {0xffc00000};
#define F_NAN (__f_nan__.val)
static union { uint64_t rep; double val; } __d_nan__ __attribute_used__ = {0xfff8000000000000};
#define D_NAN (__d_nan__.val)

/* Default "missing" values for standard types. */
#define DRMS_MISSING_CHAR     (SCHAR_MIN)
#define DRMS_MISSING_SHORT    (SHRT_MIN)
#define DRMS_MISSING_INT      (INT_MIN)
#define DRMS_MISSING_LONGLONG (LLONG_MIN)
#define DRMS_MISSING_FLOAT    (F_NAN)  
#define DRMS_MISSING_DOUBLE   (D_NAN)
#define DRMS_MISSING_STRING   ("")
#define DRMS_MISSING_TIME     (-211087684800.0) 
/* equal to '-4712.01.01_12:00:00.000_UT' which is the time value used
   missing in the MDI/SDS system. */

#define DRMS_MAXTYPENAMELEN  (9)




/****************************** DRMS Environment ***************************/


/******** Internal server data types. ********/


/* Database or DRMS client connection info. */
typedef struct DRMS_Session_struct
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
} DRMS_Session_t;


/* Link list node for keeping track of temporary records in the server. 
   quick UGLY  hack. */
typedef struct DS_node_struct
{
  char *series;
  int n,nmax;
  long long *recnums;
  struct DS_node_struct *next;
} DS_node_t;


typedef struct DRMS_Env_struct
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
} DRMS_Env_t;




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
#define DRMS_MAX_REQCNT 32

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
} DRMS_SumRequest_t;




/*************************** Data records and Series ************************/


/* Constants used to indicate lifetime of records created with 
   drms_create_record(s) and drms_clone_record(s). 
   DRMS_TEMPORARY means that the records will only exist in the
   database until the end of the DRMS session in which they
   were created. 
*/
typedef enum {DRMS_PERMANENT, DRMS_TRANSIENT} DRMS_RecLifetime_t;



/* Simple container for a set of records. */
typedef struct DRMS_RecordSet_struct
{
  int n;
  struct DRMS_Record_struct **records;
} DRMS_RecordSet_t;


/* Series-wide attributes. */
typedef struct DRMS_SeriesInfo_struct
{
  char seriesname[DRMS_MAXNAMELEN];      
  char description[DRMS_MAXCOMMENTLEN];
  char author[DRMS_MAXCOMMENTLEN];
  char owner[DRMS_MAXNAMELEN]; /* Who is allowed to modify the series 
				  definition. FIXME: WE PROBABLY NEED 
				  PERMISSIONS TO INSERT NEW RECORDS. WE DON;T
				  WANT CASUAL USERS ACCIDENTALLY INSERTING 
				  BOGUS DATA IN THE WRONG SERIES. */
  int unitsize;   /* How many records to a storage unit. */
  int archive;    /* Should this series be archived? */
  int retention;  /* Default retention time in seconds. */
  int tapegroup;  /* Tapegroup of the series. */

  /* Primary index information. */
  int pidx_num;   /* Number of keywords in primary index. */
  struct DRMS_Keyword_struct *pidx_keywords[DRMS_MAXPRIMIDX]; 
                /* Pointers to keyword structs for keywords that
		   make up the primary key.*/
}  DRMS_SeriesInfo_t;



/* Datastructure representing a single data record. */
typedef struct DRMS_Record_struct
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
} DRMS_Record_t;



/**************************** Keywords ***************************/
typedef struct  DRMS_KeywordInfo_struct
{
  char name[DRMS_MAXNAMELEN];         /* Keyword name. */

  /************ Link keywords ***********/
  /* If this is an inherited keyword, islink is non-zero,
     and linkname holds the name of the link which points
     to the record holding the actual keyword value. */
  int  islink;
  char linkname[DRMS_MAXNAMELEN];   /* Link to inherit from. */
  char target_key[DRMS_MAXNAMELEN]; /* Keyword to inherit.  */

  /************ Regular keywords ********/
  DRMS_Type_t type;             /* Keyword type. */
  char format[DRMS_MAXFORMATLEN]; /* Format string for formatted input 
                                     and output. */
  char unit[DRMS_MAXUNITLEN];     /* Physical unit. */
  char description[DRMS_MAXCOMMENTLEN];
  int isconstant;                 /* If isconstant=1 then this keyword has the
                                     same value for all records from the 
                                     series. */
  int per_segment;                /* If per_segment=1 then this keyword has the
                                     has a different for each segment belonging
                                     to the record. If the keyword name is 
                                     "blah" then keywords pertaining to specific
                                     segments are referred to by "blah[0]",
                                     "blah[1]", etc. */
} DRMS_KeywordInfo_t;


typedef struct  DRMS_Keyword_struct
{
  struct DRMS_Record_struct *record; /* The record this keyword belongs to.*/
  struct  DRMS_KeywordInfo_struct *info; /* Series-wide info. */
  DRMS_Type_Value_t value;               /* Keyword data. If this keyword is in
					  * the series cache, it contains the
					  * default value. */
} DRMS_Keyword_t;




/**************************** Links ***************************/

/* Links to other objects from which keyword values can be inherited. 
   A link often indicates that the present object was computed using the
   data in the object pointed to. */

typedef enum { STATIC_LINK, DYNAMIC_LINK } DRMS_Link_Type_t;

/* Series-wide Link info that does not vary from record to record. */
typedef struct DRMS_LinkInfo_struct
{
  char name[DRMS_MAXNAMELEN];          /* Link name. */
  char target_series[DRMS_MAXNAMELEN]; /* Series pointed to. */  
  char description[DRMS_MAXCOMMENTLEN]; 
  DRMS_Link_Type_t type;               /* Static or dynamic. */

  /*** Dynamic link info ***/
  int pidx_num;  /* Number of keywords in primary index of target series. */
  DRMS_Type_t pidx_type[DRMS_MAXPRIMIDX]; /* Type of primary index values. */
  char *pidx_name[DRMS_MAXPRIMIDX];
} DRMS_LinkInfo_t;


typedef struct DRMS_Link_struct
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
} DRMS_Link_t;



/********************************** Data Segments ***************************/

/* An n-dimensional array (e.g. part or all of a segment array) 
   stored consecutively in memory. */
typedef struct DRMS_Array_struct
{
  /* Array info: */ 
  DRMS_Type_t type;            /* Datatype of the data elements. */
  int naxis;                     /* Number of dimensions. */
  int axis[DRMS_MAXRANK];        /* Size of each dimension. */
  void *data;                    /* Data stored in column major order. */

  /* Fields relating to scaling and slicing. */
  struct DRMS_Segment_struct *parent_segment; /* Parent segment. */
  int israw;                 /* Is this read in with type=DRMS_TYPE_RAW? 
	          		If israw==0 then shift and scaling have been 
			        applied to the data and they represent the 
				"true" values. If israw==1 then no shift
                                and scaling have been applied to the data. */
  double bzero;              /* Zero point for parent->child mapping. */
  double bscale;             /* Slope for parent->child. */
  int start[DRMS_MAXRANK];   /* Start offset of slice in parent. */

  /* Private fields used for array index calculation etc. */
  int dope[DRMS_MAXRANK]; /* Dimension offset multipliers. */

  /* Private fields used for packed string arrays. */
  char *strbuf; /* String buffer used for packed string arrays. */
  long long buflen;              /* Size of string buffer. */
} DRMS_Array_t;




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
typedef enum  {DRMS_CONSTANT, DRMS_VARIABLE, DRMS_VARDIM} DRMS_Segment_Scope_t;
#ifndef DRMS_TYPES_C
extern char *drms_segmentscope_names[];
#else
char *drms_segmentscope_names[] = {"constant", "variable", "vardim"};

#endif

/* A data segment is typically an n-dimensional array of a simple type.
   It can also be a "generic" segment which is just a file 
   (structure-less as far as DRMS is concerned). */

typedef struct DRMS_SegmentDimInfo_struct {
  int naxis;
  int axis[DRMS_MAXRANK];
} DRMS_SegmentDimInfo_t;

typedef struct DRMS_SegmentInfo_struct {
						   /*  Context information:  */
  char name[DRMS_MAXNAMELEN];                              /*  Segment name  */
  int segnum;                          /*  Segment number within the record  */
  char description[DRMS_MAXCOMMENTLEN];		     /*  Description string  */
		/************  Link segments  ***********/
		/*  If this is an inherited segment, islink is non-zero,
	and linkname holds the name of the link which points to the record
						 holding the actual segment. */
  int  islink;
  char linkname[DRMS_MAXNAMELEN];		   /*  Link to inherit from  */
  char target_seg[DRMS_MAXNAMELEN];		    /*  Segment to inherit.  */

  DRMS_Type_t type;			  /*  Datatype of the data elements. */
  int naxis;					   /*  Number of dimensions. */
  char unit[DRMS_MAXUNITLEN];				  /*  Physical unit. */
  DRMS_Protocol_t protocol;			       /*  Storage protocol. */
  DRMS_Segment_Scope_t scope;			/*  Does the segment have a
                                   a) constant value for all records?
                                   b) varying value with fixed dimensions?
                                   c) varying value with varying dimensions?  */

  long long cseg_recnum;
      /* recnum of record where the constant segment is stored, default to 0 */

} DRMS_SegmentInfo_t;


typedef struct DRMS_Segment_struct {
  struct DRMS_Record_struct *record; /*  The record this segment belongs to. */
  DRMS_SegmentInfo_t *info;				      /*  see above  */
  char filename[DRMS_MAXSEGFILENAME];		      /*  Storage file name  */
  int axis[DRMS_MAXRANK];      			 /*  Size of each dimension. */
  int blocksize[DRMS_MAXRANK];		/*  block sizes for tiled/blocked
							  storage protocols. */
} DRMS_Segment_t;


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
   /* xxx etc*/
} DRMS_KeyMapClass_t;

typedef struct 	DRMS_KeyMap_struct {
  HContainer_t int2ext;
  HContainer_t ext2int;
} DRMS_KeyMap_t;

/*********** Various utility functions ****************/
DRMS_Type_t drms_str2type(const char *);
const char  *drms_type2str(DRMS_Type_t type);
int drms_missing(DRMS_Type_t type, DRMS_Type_Value_t *val);
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
float drms2float(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
double drms2double(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
double drms2time(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);
char *drms2string(DRMS_Type_t type, DRMS_Type_Value_t *value, int *status);

/* Misc. utility functions. */
int drms_printfval_raw(DRMS_Type_t type, void *val);
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
	OV.value.char_val = *(char *)inval;                     \
	break;                                                  \
      case DRMS_TYPE_SHORT:                                     \
	OV.value.short_val = *(short *)inval;                   \
	break;                                                  \
      case DRMS_TYPE_INT:                                       \
	OV.value.int_val = *(int *)inval;                       \
	break;                                                  \
      case DRMS_TYPE_LONGLONG:                                  \
	OV.value.longlong_val = *(long long *)inval;            \
	break;                                                  \
      case DRMS_TYPE_FLOAT:                                     \
	OV.value.float_val = *(float *)inval;                   \
	break;                                                  \
      case DRMS_TYPE_DOUBLE:                                    \
	OV.value.double_val = *(double *)inval;                 \
	break;                                                  \
      case DRMS_TYPE_TIME:                                      \
	OV.value.time_val = *(double *)inval;                   \
	break;                                                  \
      case DRMS_TYPE_STRING:                                    \
	OV.value.string_val = strdup((char *)inval);            \
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


