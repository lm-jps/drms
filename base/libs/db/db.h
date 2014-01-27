#ifndef MYDB_H
#define MYDB_H

#include <unistd.h>
#include <pthread.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
//#include "xmem.h"

/* Define actual types used for different back-end databases. */
#ifdef ORACLE
typedef char     db_char_t;
typedef int      db_int1_t;
typedef int      db_int2_t;
typedef int      db_int4_t;
typedef int      db_int8_t;
typedef float    db_float_t;
typedef double   db_double_t;
typedef char *   db_string_t;
typedef char *   db_varchar_t;
#endif
#ifdef MYSQL 
typedef char      db_char_t;
typedef char      db_int1_t;
typedef short     db_int2_t;
typedef int       db_int4_t;
typedef long long db_int8_t;
typedef float     db_float_t;
typedef double    db_double_t;
typedef char *    db_string_t;
typedef char *    db_varchar_t;
#endif
#ifdef POSTGRESQL
typedef char      db_char_t;
typedef short     db_int1_t;
typedef short     db_int2_t;
typedef int       db_int4_t;
typedef long long db_int8_t;
typedef float     db_float_t;
typedef double    db_double_t;
typedef char *    db_string_t;
typedef char *    db_varchar_t;
#endif


#ifdef DB_COMMON_C
char __db_error_message[4096]={0};
#else
extern char __db_error_message[4096];
#endif

#define MAXARG 1024 /* Maximum # of args to db_dmsv and db_query_binv. */

typedef void(*db_sigblock_fn)(sigset_t *set, void *data); 

/* Generic column types. */
typedef enum DB_Type_enum  { 
  DB_CHAR, DB_INT1, DB_INT2, DB_INT4, DB_INT8, 
  DB_FLOAT, DB_DOUBLE, DB_STRING, DB_VARCHAR
} DB_Type_t; 

/* Handle to database connection */
typedef struct DB_Handle_struct
{
  void *db_connection;      /* Data connection handle. */
  char dbhost[1024], dbname[1024], dbuser[1024];
  pthread_mutex_t *db_lock; /* Global lock (for multi-threaded operation). */
  int abort_now;            /* Abort flag (for multi-threaded operation). */
  unsigned int stmt_num;    /* Statement counter (for multi-threaded operation). */ 
  int isolation_level;      /* Transaction isolation level. */
  char dbport[1024];  /* Port on host connected to */
  char errmsg[4096]; /* Error message of last command. */
} DB_Handle_t;

static inline void DB_ResetErrmsg(DB_Handle_t *dbh)
{
    if (dbh && *dbh->errmsg != 0)
    {
        *dbh->errmsg = '\0';
    }
}

static inline void DB_SetErrmsg(DB_Handle_t *dbh, const char *msg)
{
    if (dbh)
    {
        snprintf(dbh->errmsg, sizeof(dbh->errmsg), "%s", msg);
    }
}

static inline const char *DB_GetErrmsg(DB_Handle_t *dbh)
{
    if (dbh && *dbh->errmsg != 0)
    {
        return dbh->errmsg;
    }
    
    return NULL;
}


/* Transaction isolation level constants. */
#define DB_TRANS_READCOMMIT   0
#define DB_TRANS_SERIALIZABLE 1
#define DB_TRANS_READONLY     2


/* Table privileges. */
#define DB_PRIV_SELECT 1
#define DB_PRIV_INSERT 2
#define DB_PRIV_UPDATE 4


/* Binary query result column. */
typedef struct DB_Column_struct
{
  char *column_name;     /* Name of the column. */
  DB_Type_t type;        /* The data type. */
  unsigned int num_rows; /* Number of rows in the column.  */
  unsigned int size;     /* Size of data type. */
  char *data;            /* Array of type "type" holding the column data. 
			    The total length of *column_data is num_rows*size.  */
  short *is_null;        /* An array of flags indicating if the field
			    contained a NULL value. */
} DB_Column_t;

/* Binary query result table. */
typedef struct DB_Binary_Result_struct
{
  unsigned int num_rows;  /* Number of rows in result. */
  unsigned int num_cols;  /* Number of columns in result. */
  DB_Column_t *column;
} DB_Binary_Result_t;


/* Text query result table. */
typedef struct DB_Text_Result_struct
{
  unsigned int num_rows;
  unsigned int num_cols;
  char **column_name;     /* Name of the column. */
  int *column_width;      /* Max width of the column. */
  char *buffer;           /* buffers holding the results. On buffer per row. */
  char ***field;          /* field[i][j] is a string contained in the i'th row 
			     and j'th column of the result. */ 
} DB_Text_Result_t;




/**************** Prototypes. ****************/

/* Error message handling. */
void db_set_error_message(char *err);
void db_get_error_message(int maxlen, char *err);


/* Size of datatypes. */
int db_sizeof(DB_Type_t type);

/* SQL names for data types. */ 
const char *db_type_string(DB_Type_t dbtype);
char *db_stringtype_maxlen(int maxlen);

/* Establish authenticated connection to database server. */
DB_Handle_t  *db_connect(const char *host, const char *user, 
			 const char *passwd, const char *db_name,
			 const int lock);
/* Disconnect from  database server. */
void db_disconnect(DB_Handle_t **db);


/* SQL data manipulation statement with fixed input and no output. */
int db_dms(DB_Handle_t  *db, int *row_count, char *query_string);

/* SQL data manipulation statement with variable array input and no output. */
int db_dmsv(DB_Handle_t  *dbin, int *row_count, char *query_string, 
	    int n_rows, ...);
int db_dms_array(DB_Handle_t  *dbin, int *row_count, 
		  char *oquery, int n_rows, 
		 int n, DB_Type_t *intype, void **argin );

/* Bulk inserts via fast path interface. */
int db_bulk_insertv(DB_Handle_t  *dbin, char *table, 
		    int n_rows, int n_cols, ...);

int db_bulk_insert_array(DB_Handle_t  *dbin, char *table, int n_rows, 
			 int n_args, DB_Type_t *intype, void **argin );

/* SQL query statement with result returned as table of strings. */
DB_Text_Result_t *db_query_txt(DB_Handle_t  *db, char *query_string);

/* Packing and unpacking query results. */
DB_Text_Result_t *db_unpack_text(char *buf);

/* SQL query statement with result returned as table of binary data. */
DB_Binary_Result_t *db_query_bin(DB_Handle_t  *db, char *query_string);
DB_Binary_Result_t *db_query_binv(DB_Handle_t  *dbin, char *query, ...);
DB_Binary_Result_t *db_query_bin_array(DB_Handle_t  *dbin, 
				        char *query, int n_args,
				       DB_Type_t *intype, void **argin );
DB_Binary_Result_t **db_query_bin_ntuple(DB_Handle_t *dbin, const char *stmnt, unsigned int nelems, unsigned int nargs, DB_Type_t *dbtypes, void **values);
		

/* Functions for extraction the field values from a binary table. */
/*char *db_binary_field_get(DB_Binary_Result_t *res, unsigned int row, 
  unsigned int col); */
int db_binary_field_is_null(DB_Binary_Result_t *res, unsigned int row, 
			    unsigned int col);
/* DB_Type_t db_binary_column_type(DB_Binary_Result_t *res, unsigned int col); */
void db_print_binary_field_type(DB_Type_t dbtype);
DB_Text_Result_t *db_binary_to_text(DB_Binary_Result_t *binres);

/* with conversion... */
char db_binary_field_getchar(DB_Binary_Result_t *res, unsigned int row, 
			     unsigned int col);
int db_binary_field_getint(DB_Binary_Result_t *res, unsigned int row, 
			   unsigned int col);
long long db_binary_field_getlonglong(DB_Binary_Result_t *res, 
				      unsigned int row, 
				      unsigned int col);
float db_binary_field_getfloat(DB_Binary_Result_t *res, unsigned int row, 
			       unsigned int col);
double db_binary_field_getdouble(DB_Binary_Result_t *res, unsigned int row, 
				 unsigned int col);
void db_binary_field_getstr(DB_Binary_Result_t *res, unsigned int row, 
			    unsigned int col, int len, char *str);

/* Formated printing of table of results. */    
void db_print_binary_result(DB_Binary_Result_t *res);
void db_print_binary_field(DB_Type_t dbtype, int width, char *data);
int db_sprint_binary_field(DB_Type_t dbtype, int width, char *data, char *dst);
int db_binary_default_width(DB_Type_t dbtype);
void db_print_text_result(DB_Text_Result_t *res);


/* Sorting */
int db_sort_binary_result(DB_Binary_Result_t *res, int num_cols, int *cols);
int db_maxbygroup(DB_Binary_Result_t *res, int maxcol, int num_cols, int *cols);


/* Free result buffers allocated by db_query functions. */
void db_free_binary_result(DB_Binary_Result_t *db_result);
void db_free_text_result(DB_Text_Result_t *db_result);
void db_free_binary_result_tuple(DB_Binary_Result_t ***tuple, unsigned int nelems);


/* Sequence functions */
long long db_sequence_getnext(DB_Handle_t *db, char *tablename);
long long *db_sequence_getnext_n(DB_Handle_t *db,  char *tablename, int n);
long long db_sequence_getcurrent(DB_Handle_t *db, char *tablename);
long long db_sequence_getlast(DB_Handle_t *db,  char *tablename);
int db_sequence_create(DB_Handle_t *db, char *tablename);
int db_sequence_drop(DB_Handle_t *db, char *tablename);


/* Start transactions/commit/rollback. */
int db_commit(DB_Handle_t *db);
int db_start_transaction(DB_Handle_t  *db);
int db_rollback(DB_Handle_t  *db);
int db_cancel(DB_Handle_t *db, char *effbuf, int size);
int db_settimeout(DB_Handle_t *db, unsigned int timeoutval);

/* Set transaction isolation level.   
   0 = read commited
   1 = serializable.
   2 = read only. */
int db_isolation_level(DB_Handle_t  *dbin, int level);


/* Utilities */
void *safemalloc(size_t size);
char *search_replace(const char *string, const char *search, 
		     const char *replace);

/* Conversion routines for binary results. */
char dbtype2char(DB_Type_t dbtype, char *data);
short dbtype2short(DB_Type_t dbtype, char *data);
int dbtype2int(DB_Type_t dbtype, char *data);
long long dbtype2longlong(DB_Type_t dbtype, char *data);
float dbtype2float(DB_Type_t dbtype, char *data);
double dbtype2double(DB_Type_t dbtype, char *data);
void dbtype2str(DB_Type_t dbtype, char *data, int len, char *str);


/* Networking functions. */
int db_getsocketbufsize(int sockfd, int *sndbuf, int *rcvbuf);
int db_setsocketbufsize(int sockfd, int sndbuf, int rcvbuf);
long long htonll(long long val); /* 64 bit byteswapping. */
long long ntohll(long long val); /* 64 bit byteswapping. */
int db_tcp_listen(char *host, int len, short *port);
void send_string(int fd, const char *str);
char *receive_string(int fd);
void Writen(int fd, const void *ptr, size_t nbytes);
void Writen_ntoh(int fd, const void *ptr, size_t size);
ssize_t Readn(int fd, void *ptr, size_t nbytes);
ssize_t Readn_ntoh(int fd, void *ptr, size_t size);
void Write_dbtype(DB_Type_t type, char *val, int fd);
void Writevn(int fd, struct iovec *vector, int count );

long long Readlonglong(int fd);
int Readint(int fd);
short Readshort(int fd);
void *Read_dbtype(DB_Type_t *type, int fd);
int readlonglong(int fd, long long *val);
int readint(int fd, int *val);
int readshort(int fd, int *val);

int db_send_text_result(int sockfd, DB_Text_Result_t *result, int compress);
DB_Text_Result_t *db_recv_text_query(int sockfd, int compress, char **errmsg);
int db_send_binary_result(int sockfd, DB_Binary_Result_t *result, int comp);
DB_Binary_Result_t *db_recv_binary_query(int sockfd, int comp, char **errmsg);
void db_hton(DB_Type_t dbtype, int n, void *data);
#define db_ntoh(type,n,data) db_hton((type),(n),(data))
void db_byteswap(DB_Type_t dbtype, int n, char *val);

/* Server side API. */
int db_server_query_txt(int sockfd, DB_Handle_t *db_handle);
int db_server_query_bin(int sockfd, DB_Handle_t *db_handle);
int db_server_query_bin_array(int sockfd, DB_Handle_t *db_handle);
int db_server_query_bin_ntuple(int sockfd, DB_Handle_t *db_handle);
int db_server_dms(int sockfd, DB_Handle_t *db_handle);
int db_server_dms_array(int sockfd, DB_Handle_t *db_handle);
int db_server_bulk_insert_array(int sockfd, DB_Handle_t *db_handle);
int db_server_sequence_create(int sockfd, DB_Handle_t *db_handle);
int db_server_sequence_drop(int sockfd, DB_Handle_t *db_handle);
int db_server_sequence_getnext(int sockfd, DB_Handle_t *db_handle);
int db_server_sequence_getnext_n(int sockfd, DB_Handle_t *db_handle);
int db_server_sequence_getcurrent(int sockfd, DB_Handle_t *db_handle);
int db_server_sequence_getlast(int sockfd, DB_Handle_t *db_handle);

/* Client side API. */
DB_Text_Result_t *db_client_query_txt(int sockfd, char *query, 
				      int compress, 
                                      char **errmsg);
DB_Binary_Result_t *db_client_query_bin(int sockfd, char *query, 
					int compress,
                                        char **errmsg);
DB_Binary_Result_t *db_client_query_bin_array(int sockfd, char *query, 
					      int compress, int n_args,  
					      DB_Type_t *intype, void **argin);
DB_Binary_Result_t **db_client_query_bin_ntuple(int sockfd, const char *stmnt, unsigned int nexes, unsigned int nargs, DB_Type_t *dbtypes, void **values);
int db_client_dmsv(int sockfd,  int *row_count, char *query, 
		   int n_rows, ...);
int db_client_dms_array(int sockfd,  int *row_count, char *query, 
			int n_rows, int n_args, DB_Type_t *intype, 
			void **argin );
int db_client_bulk_insert_array(int sockfd, char *table, 
				int n_rows, int n_args, DB_Type_t *intype, 
				void **argin );
int db_client_dms(int sockfd, int *row_count, char *query);
int db_client_sequence_create(int sockfd, char *table);
int db_client_sequence_drop(int sockfd, char *table);
long long db_client_sequence_getnext(int sockfd, char *table);
long long *db_client_sequence_getnext_n(int sockfd, char *table, int n);
long long db_client_sequence_getcurrent(int sockfd, char *table);
long long db_client_sequence_getlast(int sockfd,  char *table);


/* Thread control functions. */
void db_lock(DB_Handle_t *h);
void db_unlock(DB_Handle_t *h);

void db_register_sigblock(db_sigblock_fn fn, void *data);


/********** Inline functions ****************/
static inline void net_packint(int val, int *buf, struct iovec *vec)  
{
  *buf=htonl((val)); 
  vec->iov_len=sizeof(int); 
  vec->iov_base = buf;
}

static inline void net_packlonglong(long long val, long long *buf, struct iovec *vec)  
{
  *buf=htonll((val)); 
  vec->iov_len=sizeof(long long); 
  vec->iov_base = buf;
}

static inline void net_packstring(char *str, int *buf, struct iovec *vec)
{
  (vec+1)->iov_len=strlen(str); 
  (vec+1)->iov_base = str; 
  *buf=htonl((vec+1)->iov_len); 
  vec->iov_len=sizeof(int); 
  vec->iov_base = buf;
}

static inline DB_Type_t db_binary_column_type(DB_Binary_Result_t *res, 
					      unsigned int col)
{
  return res->column[col].type;
}


static inline char *db_binary_field_get(DB_Binary_Result_t *res, 
					unsigned int row, 
					unsigned int col)
{  
  if ( row<res->num_rows && col<res->num_cols )
    return (res->column[col].data+row*res->column[col].size);
  else
    return NULL;
}

#ifdef ICCCOMP
#pragma warning (disable : 810 1469)
#endif
/* There appears to be no way to use htons() without generating an icc compiler warning. */
static inline void Writeshort(int fd, short val) 
{ short tmp; tmp = htons((val)); Writen((fd), &tmp, sizeof(short)); }
#ifdef ICCCOMP
#pragma warning (default : 810 1469)
#endif

static inline void Writeint(int fd, int val)
{ int tmp; tmp = htonl((val)); Writen((fd), &tmp, sizeof(int)); }
static inline void Writelonglong(int fd, long long val) 
{ long long tmp; tmp = htonll((val)); Writen((fd), &tmp, sizeof(long long)); }


#define QUERY_ERROR(__query__) fprintf(stderr,"Error at %s, line %d: Query '" \
 "%s' failed.\n",   __FILE__, __LINE__,__query__); 

#endif

