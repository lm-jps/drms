#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <assert.h>
#include <ctype.h> 
#include <netinet/in.h>
#include <libpq-fe.h>
#include "db.h"
#include "xassert.h"
#include "xmem.h"

/* These are PG internal type codes. */
#define PG_CHAR_OID    (18)
#define PG_INT2_OID    (21)
#define PG_INT4_OID    (23)
#define PG_INT8_OID    (20)
#define PG_FLOAT_OID   (700)
#define PG_DOUBLE_OID  (701)
#define PG_STRING_OID  (25)
#define PG_VARCHAR_OID (1043)

//#define DEBUG 



static int db2pgsql_type(DB_Type_t dbtype)
{
  switch(dbtype)
  {
  case DB_CHAR:
    return PG_CHAR_OID;
  case DB_INT1:
    return PG_INT2_OID;
  case DB_INT2:
    return PG_INT2_OID;
  case DB_INT4:
    return PG_INT4_OID;
  case DB_INT8:
    return PG_INT8_OID;
  case DB_FLOAT:
    return PG_FLOAT_OID;
  case DB_DOUBLE:
    return PG_DOUBLE_OID;
  case DB_STRING:
    return PG_STRING_OID;
  case DB_VARCHAR:
    return PG_VARCHAR_OID;
  default:
    return PG_STRING_OID;
  }
}

static DB_Type_t pgsql2db_type(int pgtype)
{
  switch(pgtype)
  {
  case PG_CHAR_OID:
    return DB_CHAR;
  case PG_INT2_OID:
    return DB_INT2; /* Ambiguous, could be DB_INT1 or DB_INT2;
		       return the largest to avoid truncation. */
  case PG_INT4_OID:
    return DB_INT4;
  case PG_INT8_OID:
    return DB_INT8;
  case PG_FLOAT_OID:
    return DB_FLOAT;
  case PG_DOUBLE_OID:
    return DB_DOUBLE;
  case PG_STRING_OID:
    return DB_STRING;
  case PG_VARCHAR_OID:
    return DB_VARCHAR;
  default:
    return DB_STRING;    
  }
}




DB_Handle_t *db_connect(const char *host, const char *user, 
			const char *passwd, const char *db_name,
			const int lock)
{
  DB_Handle_t  *handle;
  PGconn *db;
  char *p,conninfo[8192]={0};
  
  /* If any of the authentication information is missing
     rely on ~/.pgpass to contain it. */
  p = conninfo;
  if (host) 
  {
    if (isdigit(host[0]))
      p += sprintf(p,"hostaddr = %s ",host);
    else if (strcmp(host,"localhost"))
      p += sprintf(p,"host = %s ",host); 
  }
  if (db_name)
      p += sprintf(p,"dbname = %s ",db_name); 
  if (user) 
      p += sprintf(p,"user = %s ",user); 
  if (passwd)
      p += sprintf(p,"password = %s ",passwd); 

  db = PQconnectdb(conninfo);
  if (PQstatus(db) != CONNECTION_OK)
  {
    fprintf(stderr, "Connection to database '%s' failed.\n", PQdb(db));
    fprintf(stderr, "%s", PQerrorMessage(db));
    PQfinish(db);
    return NULL;
  }

  /* Initialize DB handle struct. */
  XASSERT( handle = malloc(sizeof(DB_Handle_t)) );
  
  strncpy(handle->dbhost,PQhost(db),1024);
  strncpy(handle->dbname,PQdb(db),1024);
  strncpy(handle->dbuser,PQuser(db),1024);
  handle->db_connection = (void *) db;
  handle->abort_now = 0;
  handle->stmt_num = 0; 
  handle->isolation_level = DB_TRANS_READCOMMIT;
  if (lock)
  {
    XASSERT( handle->db_lock = malloc(sizeof(pthread_mutex_t)) );
    pthread_mutex_init(handle->db_lock, NULL);
  }
  else
    handle->db_lock = NULL;

  return handle;
}

    
DB_Handle_t *db_connect_toport (const char *host, const unsigned short port,
    const char *user, const char *passwd, const char *db_name, const int lock) {
  DB_Handle_t  *handle;
  PGconn *db;
  char *p, conninfo[8192] = {0};
	/*  If any of the authentication information is missing
					    rely on ~/.pgpass to contain it  */
  p = conninfo;
  if (host) {
    if (isdigit (host[0])) p += sprintf (p, "hostaddr = %s ",host);
    else if (strcmp (host,"localhost"))
      p += sprintf (p, "host = %s ",host); 
  }
  if (port) p += sprintf (p, "port = %d ", port); 
  if (db_name) p += sprintf (p, "dbname = %s ", db_name); 
  if (user) p += sprintf (p, "user = %s ", user); 
  if (passwd) p += sprintf (p, "password = %s ", passwd); 

  db = PQconnectdb (conninfo);
  if (PQstatus(db) != CONNECTION_OK) {
    fprintf (stderr, "Connection to database '%s' failed.\n", PQdb(db));
    fprintf (stderr, "%s", PQerrorMessage(db));
    PQfinish (db);
    return NULL;
  }
					    /*  Initialize DB handle struct  */
  XASSERT( handle = malloc (sizeof (DB_Handle_t)) );
  strncpy (handle->dbhost, PQhost (db), 1024);
  strncpy (handle->dbname, PQdb (db), 1024);
  strncpy (handle->dbuser, PQuser (db), 1024);
  handle->db_connection = (void *)db;
  handle->abort_now = 0;
  handle->stmt_num = 0; 
  handle->isolation_level = DB_TRANS_READCOMMIT;
  if (lock) {
    XASSERT( handle->db_lock = malloc (sizeof (pthread_mutex_t)) );
    pthread_mutex_init (handle->db_lock, NULL);
  } else handle->db_lock = NULL;

  return handle;
}

    
void db_disconnect(DB_Handle_t  *dbin)
{
  if (dbin)
  {
    db_lock(dbin);

    PQfinish(dbin->db_connection);
    dbin->db_connection = NULL; /* make it easier to spot use after free. */

    /* Free thread related data. */
    free(dbin->db_lock);
    dbin->db_lock = NULL; /* make it easier to spot use after free. */
    free(dbin);    
  }
}


/* Roll back and set the abort flag. */
int db_abort(DB_Handle_t  *dbin)
{
  PGconn *db;  

  db = (PGconn *) dbin->db_connection;
  db_lock(dbin);
  if (dbin->abort_now)
    goto failure;

  /* Close connection to server. */
  PQfinish(dbin->db_connection);
  
  dbin->abort_now = 1; /* All subsequent database operations must abort. */

  db_unlock(dbin);
  return 0;

 failure:
  db_unlock(dbin);
  return 1;
}



DB_Text_Result_t *db_query_txt(DB_Handle_t  *dbin,  char *query_string)
{
  PGconn *db;
  PGresult *res;
  DB_Text_Result_t *result=NULL;
  int buffer_length, colname_length, width, buflen, n;
  unsigned int i,j;
  char *p;

  if (dbin==NULL)
    return NULL;
  db = dbin->db_connection;

   /* Lock database connection if in multi threaded mode. */
  db_lock(dbin);
  if (dbin->abort_now)
    goto failure;

#ifdef DEBUG
  printf("db_query_txt: query = %s\n",query_string);
#endif
  res = PQexec(db, query_string);
  if (PQresultStatus(res) != PGRES_TUPLES_OK)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    PQclear(res);
    goto failure;
  }

  // query succeeded, process any data returned by it
  XASSERT( result = (DB_Text_Result_t *)malloc(sizeof(DB_Text_Result_t)) );
  memset(result,0,sizeof(DB_Text_Result_t));
  result->num_rows = PQntuples(res);
  result->num_cols = PQnfields(res);

  if (result->num_rows>0)  // there are rows
  {
    colname_length = 0;
    XASSERT( result->column_name = (char **)malloc(result->num_cols*sizeof(char *)) );
    XASSERT( result->column_width = (int *)malloc(result->num_cols*sizeof(int)) );

    buffer_length = 0;
    for(i = 0; i < result->num_cols; i++)
    {
      colname_length += strlen(PQfname(res, i))+1;
      result->column_width[i] = 0;
      for(j = 0; j < result->num_rows; j++) 
      {
	width = PQgetlength(res,j,i);
	buffer_length += width+1;
	if (width > result->column_width[i])
	  result->column_width[i] = width;
      }
    }    

    buflen = (3+result->num_cols)*sizeof(int) + colname_length + buffer_length;
    XASSERT( result->buffer = malloc( buflen ) );

    p = result->buffer;
    
    /* Pack size info into the buffer. */
    *((int *) p) = htonl(buflen);
    p += sizeof(int);
    *((int *) p) = htonl(result->num_rows);
    p += sizeof(int);
    *((int *) p) = htonl(result->num_cols);
    p += sizeof(int);
    for (i=0; i<result->num_cols; i++)
    {
      *((int *) p) = htonl(result->column_width[i]);
      p += sizeof(int);
    }
    
    /* Copy column names. */
    for (i=0; i<result->num_cols; i++)
    {
      result->column_name[i] = p;
      n = strlen(PQfname(res, i))+1;
      p = memccpy(p, PQfname(res, i), 0, n);
      XASSERT(p!=NULL);
#ifdef DEBUG
      printf("name = %s, copy = %s, n = %d.\n",PQfname(res, i),result->column_name[i], n);
#endif
    }
    

    /* Set up data structure for the actual contents. */
    XASSERT( result->field =  (char ***) 
	     malloc(result->num_rows*sizeof(char **) +
		    result->num_rows*result->num_cols*sizeof(char *)) );
    for (i=0; i<result->num_rows; i++)
      result->field[i] = (char **) &result->field[result->num_rows + 
						  i*result->num_cols];

    /* Copy over the actual field contents. */
    for (j=0; j < result->num_rows; j++)
    {
      for(i = 0; i < result->num_cols; i++)
      {
	result->field[j][i] = p;
	p = memccpy(p,PQgetvalue(res,j,i),0,PQgetlength(res,j,i)+1);
	XASSERT(p!=NULL);
      }
    }
  }
  PQclear(res);
 failure:
  if (!result)
    QUERY_ERROR(query_string);
  db_unlock(dbin);
  return result;
}
  


DB_Binary_Result_t *db_query_bin(DB_Handle_t  *dbin,  char *query_string)
{
  PGconn *db;
  PGresult *res;
  DB_Binary_Result_t *db_res;
  int colname_length;
  unsigned int i,j, width;

  if (dbin==NULL)
    return NULL;
  db = dbin->db_connection;

   /* Lock database connection if in multi threaded mode. */
  db_lock(dbin);
  if (dbin->abort_now)
    goto failure;

#ifdef DEBUG
  printf("db_query_bin: query = %s\n",query_string);
#endif
  res = PQexecParams(db,query_string, 0, NULL,
		     NULL, NULL, NULL, 1);
  
  if (PQresultStatus(res) != PGRES_TUPLES_OK)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    PQclear(res);
    goto failure;
  }

  // query succeeded, process any data returned by it
  XASSERT( db_res = (DB_Binary_Result_t *)malloc(sizeof(DB_Binary_Result_t)) );
  memset(db_res,0,sizeof(DB_Binary_Result_t));
  db_res->num_rows = PQntuples(res);
  db_res->num_cols = PQnfields(res);

  if (db_res->num_cols>0)  // there are rows
  {
    XASSERT( db_res->column = (DB_Column_t *)malloc(db_res->num_cols * 
						    sizeof(DB_Column_t)) ); 
    //    printf("num_rows=%d, num_cols=%d\n",db_res->num_rows,db_res->num_cols);
    for (i=0; i<db_res->num_cols; i++)
    {
      //      printf("Column %d:\n",i);
      colname_length = strlen(PQfname(res,i))+1;
      XASSERT( db_res->column[i].column_name = malloc(colname_length) );
      strcpy(db_res->column[i].column_name,PQfname(res,i)); 
      //printf("name = %s (%s)\n",db_res->column[i].column_name,PQfname(res,i));
      db_res->column[i].type = pgsql2db_type(PQftype(res,i));
      //printf("type = %d (%d)\n",db_res->column[i].type,PQftype(res,i));
      db_res->column[i].num_rows = db_res->num_rows;
      XASSERT( db_res->column[i].is_null =  malloc(db_res->num_rows*sizeof(int)) );
      db_res->column[i].size = 0;
      
      /* Get the maximum (over all rows) value length */
      for (j=0; j<db_res->num_rows; j++)
      {
	width = PQgetlength(res,j,i);
	if (width>db_res->column[i].size)
	  db_res->column[i].size = width;
      }
      
      /* The database does NOT store the trailing '\0' as part of the
	 string. Add it manually by setting size one larger. */
      if ( db_res->column[i].type == DB_STRING || 
	   db_res->column[i].type == DB_VARCHAR )
	(db_res->column[i].size)++;
      
      XASSERT( db_res->column[i].data = malloc(db_res->num_rows * 
					       db_res->column[i].size) );

      /* set entire column to NULLS, then fill in non-0 parts. This will
       * take care of string values too - their PQgetlength() length will
       * be at least one smaller than db_res->column[i].size. */
      memset(db_res->column[i].data, 0, db_res->num_rows * db_res->column[i].size);

#ifdef DEBUG
      printf("sizeof column %d = %d\n",i,db_res->column[i].size);
#endif      
    }
    
    for (i=0; i<db_res->num_cols; i++)
    {
      for (j=0; j<db_res->num_rows; j++)
      {
	db_res->column[i].is_null[j] = PQgetisnull(res,j,i);
	if (!db_res->column[i].is_null[j])
	  memcpy(&db_res->column[i].data[j*db_res->column[i].size], 
		 PQgetvalue(res,j,i), PQgetlength(res,j,i));
      }
#if __BYTE_ORDER == __LITTLE_ENDIAN
      db_byteswap(db_res->column[i].type, db_res->num_rows, 
		  db_res->column[i].data);
#endif
    }   
  }  
  PQclear(res);      
  db_unlock(dbin);
  return db_res;
failure:
  QUERY_ERROR(query_string);
  db_unlock(dbin);
  return NULL;
}



DB_Binary_Result_t *db_query_bin_array(DB_Handle_t  *dbin, 
				        char *query, int n_args,
				       DB_Type_t *intype, void **argin )
{
  PGconn *db;
  PGresult *res;
  DB_Binary_Result_t *db_res;
  int colname_length, buflen;
  unsigned int i,j, width;
  char *p, *pquery, *op,*q;
  int paramLengths[MAXARG], paramFormats[MAXARG],n;
  Oid paramTypes[MAXARG];
  char *paramValues[MAXARG];
  void *paramBuf;

  if (dbin==NULL)
    return NULL;
  db = dbin->db_connection;

  XASSERT( pquery = malloc(6*strlen(query)) ); 

   /* Lock database connection if in multi-threaded mode. */
  db_lock(dbin);
  if (dbin->abort_now)
    goto failure;


  /* Replace wildcards '?' with '$1', '$2', '$3' etc. */
  op = pquery;
  q = (char *)query;
  n = 1;
  while (*q)
  {
    if (*q == '?')
    {
      q++;      
      op += sprintf(op,"$%d",n);
      n++;
    }
    else
      *op++ = *q++;
  }
  *op = '\0';
  n--;

#ifdef DEBUG
  printf("modified query string = '%s'.\n",pquery);
#endif

  if (n_args!=n)
  {
    fprintf(stderr,"Wrong number (%d) of parameters given for '%s'.\n",
	    n_args,query);
    goto failure;
  }
  if (n_args>=MAXARG)
  {
    fprintf(stderr,"Maximum number of arguments exceeded (%d>=%d).\n",
	    n_args,MAXARG);
    goto failure;
  }

  /* Extract query parameters from function argument list. */
  buflen = 0;
  for(i=0; i<n_args; i++)
  {
    if (intype[i] ==  DB_STRING || intype[i] == DB_VARCHAR )
      paramLengths[i] = strlen((char *) argin[i]);
    else
      paramLengths[i] = db_sizeof(intype[i]);
    paramTypes[i] = db2pgsql_type(intype[i]);
    paramFormats[i] = 1;
    buflen += paramLengths[i];
  }
#ifdef DEBUG
  printf("n_args = %d, buflen = %d\n",n_args,buflen);
#endif
  XASSERT( paramBuf = malloc(buflen) );  
  p = paramBuf;
  for(i=0; i<n_args; i++)
  {
    paramValues[i] = p;
    memcpy(p, argin[i], paramLengths[i]);
#if __BYTE_ORDER == __LITTLE_ENDIAN
    db_byteswap(intype[i],1,p);
#endif
    p += paramLengths[i];
  }

  res = PQexecParams(db, pquery, n_args, paramTypes,
		     paramValues, paramLengths, paramFormats, 1);
  free(paramBuf);

  if (PQresultStatus(res) != PGRES_TUPLES_OK)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    PQclear(res);
    goto failure;
  }

  // query succeeded, process any data returned by it
  XASSERT( db_res = (DB_Binary_Result_t *)malloc(sizeof(DB_Binary_Result_t)) );
  memset(db_res,0,sizeof(DB_Binary_Result_t));
  db_res->num_rows = PQntuples(res);
  db_res->num_cols = PQnfields(res);

  if (db_res->num_cols>0)  // there are rows
  {
    XASSERT( db_res->column = (DB_Column_t *)malloc(db_res->num_cols * 
						    sizeof(DB_Column_t)) ); 
    //    printf("num_rows=%d, num_cols=%d\n",db_res->num_rows,db_res->num_cols);
    for (i=0; i<db_res->num_cols; i++)
    {
      //      printf("Column %d:\n",i);
      colname_length = strlen(PQfname(res,i))+1;
      XASSERT( db_res->column[i].column_name = malloc(colname_length) );
      strcpy(db_res->column[i].column_name,PQfname(res,i)); 
      //printf("name = %s (%s)\n",db_res->column[i].column_name,PQfname(res,i));
      db_res->column[i].type = pgsql2db_type(PQftype(res,i));
      //printf("type = %d (%d)\n",db_res->column[i].type,PQftype(res,i));
      db_res->column[i].num_rows = db_res->num_rows;
      XASSERT( db_res->column[i].is_null =  malloc(db_res->num_rows*sizeof(int)) );
      db_res->column[i].size = 0;
      for (j=0; j<db_res->num_rows; j++)
      {
	width = PQgetlength(res,j,i);
	if (width>db_res->column[i].size)
	  db_res->column[i].size = width;
      }
      
      /* The database does NOT store the trailing '\0' as part of the
	 string. Add it manually by setting size one larger. */
      if ( db_res->column[i].type == DB_STRING || 
	   db_res->column[i].type == DB_VARCHAR )
	(db_res->column[i].size)++;
      
      XASSERT( db_res->column[i].data = malloc(db_res->num_rows * 
					       db_res->column[i].size) );
#ifdef DEBUG
      printf("sizeof column %d = %d\n",i,db_res->column[i].size);
#endif      
    }
    
    for (i=0; i<db_res->num_cols; i++)
    {
      for (j=0; j<db_res->num_rows; j++)
      {
	db_res->column[i].is_null[j] = PQgetisnull(res,j,i);
	if (!db_res->column[i].is_null[j])
	  memcpy(&db_res->column[i].data[j*db_res->column[i].size], 
		 PQgetvalue(res,j,i), db_res->column[i].size);
	else
	  memset(&db_res->column[i].data[j*db_res->column[i].size], 0,
		 db_res->column[i].size);
      }
#if __BYTE_ORDER == __LITTLE_ENDIAN
      db_byteswap(db_res->column[i].type, db_res->num_rows, 
		  db_res->column[i].data);
#endif
    }   
  }  
  free(pquery);
  PQclear(res);      
  db_unlock(dbin);
  return db_res;
failure:
  QUERY_ERROR(query);
  free(pquery);
  db_unlock(dbin);
  return NULL;
}


/* Execute a data manipulation statement (DMS). */
int db_dms(DB_Handle_t  *dbin, int *row_count,  char *query_string)
{
  PGconn *db;
  PGresult *res;
  char *str;

  if (dbin==NULL)
    return 1;
  db = dbin->db_connection;

   /* Lock database connection if in multi threaded mode. */
  db_lock(dbin);
  if (dbin->abort_now)
    goto failure;
#ifdef DEBUG
  printf("db_dms: query = %s\n",query_string);
#endif
  res = PQexec(db, query_string);
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    PQclear(res);
    goto failure;
  }
  if (PQntuples(res) != 0)
  {
    fprintf(stderr, "Queries returning results are not allowed in query_dms.\n");
    PQclear(res);
    goto failure;
  }
  if (row_count)
  {
    str = PQcmdTuples(res);
    if (*str==0)
      *row_count = 0;
    else
      *row_count = atol(str);
  }
  PQclear(res);
  db_unlock(dbin);
  return 0;
 failure:
  QUERY_ERROR(query_string);
  db_unlock(dbin);
  return 1;
}


#define MAXARG 1024
int db_dms_array(DB_Handle_t  *dbin, int *row_count, 
		 char *query, int n_rows, 
		 int n_args, DB_Type_t *intype, void **argin )
{
  PGconn *db;
  PGresult *res;
  int n,i,j;
  char *q, *str, *op;
  int paramLengths[MAXARG], paramFormats[MAXARG];
  Oid paramTypes[MAXARG];
  char preparestring[8192],*p, *pquery = 0;
  char *paramValues[MAXARG];
  char stmtname[20], dallocstmt[30];

#ifdef DEBUG
  printf("Entering db_dms_array.\n");
#endif
  if (dbin==NULL)
    return 1;
  if (n_args>=MAXARG)
  {
    fprintf(stderr,"Maximum number of arguments exceeded.\n");
    goto failure;
  }


  db = dbin->db_connection;
  XASSERT( pquery = malloc(6*strlen(query)) ); 

  /* Replace wildcards '?' with '$1', '$2', '$3' etc. */
  op = pquery;
  q = (char *)query;
  n = 1;
  while (*q)
  {
    if (*q == '?')
    {
      q++;      
      op += sprintf(op,"$%d",n);
      n++;
    }
    else
      *op++ = *q++;
  }
  *op = '\0';

#ifdef DEBUG
  printf("modified query string = '%s'.\n",pquery);
#endif

  /* Extract query parameters from function argument list. */
  for(i=0; i<n_args; i++)
  {
    paramLengths[i] = db_sizeof(intype[i]);
    paramTypes[i] = db2pgsql_type(intype[i]);
    paramFormats[i] = 1;
    /*     printf("argument %d: length=%d, type = %d, format=%d.\n", */
    /* 	   i,paramLengths[i],paramTypes[i],paramFormats[i]); */
#if __BYTE_ORDER == __LITTLE_ENDIAN
    db_byteswap(intype[i],n_rows,argin[i]);
#endif
  }

  if (n_rows>1)
  {
    db_lock(dbin);
    if (dbin->abort_now) {
      db_unlock(dbin);
      goto failure;
    }
    /* Buld the string for a PREPARE statement. */ 
    sprintf(stmtname,"db_tmp_stmt_%d",dbin->stmt_num);
    sprintf(dallocstmt,"deallocate %s",stmtname);
    /* Increment the global statement counter. */
    ++dbin->stmt_num;

    p = preparestring;
    /*    printf("n_args = %d\n",n_args); */
    if (n_args>0)
    {
      p += sprintf(p,"prepare %s(%s",stmtname,db_type_string(intype[0]));
      for(i=1; i<n_args; i++)
	p += sprintf(p,", %s",db_type_string(intype[i]));
      p += sprintf(p,") as %s",pquery);
      *p = 0;
    }
    else
      p += sprintf(p,"prepare %s as %s",stmtname,pquery);

#ifdef DEBUG
    printf("PREPARE STRING= '%s'\n",preparestring); 
#endif
    db_unlock(dbin);

    /* Ask database server to prepare a statement executing the query. */
    if (db_dms(dbin, NULL, preparestring))
      goto failure;

    /* Execute the query once for each set of input parameters. */
    if (row_count)
      *row_count=0;
    /* Lock the database connection. */
    db_lock(dbin);
    if (dbin->abort_now) {
      db_unlock(dbin);
      goto failure;
    }
    for (i=0; i<n_rows; i++)
    {
      for (j=0; j<n_args; j++)
      {
	p = argin[j];
	if (intype[j] == DB_STRING || intype[j] == DB_VARCHAR )
	{
	  paramValues[j] = ((char **) argin[j])[i];
	  paramLengths[j] = strlen(paramValues[j]);
	  /* Do NOT include the trailing '\0' in the string or
	     comparisons like <field>='string' will not work. */
	}
	else
	  paramValues[j] = p + i*paramLengths[j];
      }

      res = PQexecPrepared(db, stmtname, n_args,
			   paramValues, paramLengths,
			   paramFormats, 0);
    
      if (PQresultStatus(res) != PGRES_COMMAND_OK)
      {
	fprintf(stderr, "query failed: %s", PQerrorMessage(db));
	PQclear(res);
	goto failure1;
      }
      if (row_count)
      {
	str = PQcmdTuples(res);
	if (*str != '\0')
	  *row_count += atol(str);
      }
      PQclear(res);
    }
    db_unlock(dbin);
    /* Deallocate prepared statement executing the query. */
    db_dms(dbin, NULL, dallocstmt); /* Ignore failure. */
  }
  else
  {
    /* Lock the database connection. */
    /* Execute the query once for each set of input parameters. */
    if (row_count)
      *row_count=0;
    for (j=0; j<n_args; j++)
    {
      p = argin[j];
      if (intype[j] == DB_STRING || intype[j] == DB_VARCHAR )
      {
	paramValues[j] = ((char **) argin[j])[0];
	paramLengths[j] = strlen(paramValues[j]);
	/* Do NOT include the trailing '\0' in the string or
	   comparisons like <field>='string' will not work. */
      }
      else
	paramValues[j] = p;
    }
    db_lock(dbin);
    if (dbin->abort_now) {
      db_unlock(dbin);
      goto failure;
    }
    res = PQexecParams(db,pquery, n_args, paramTypes,
		       paramValues, paramLengths, paramFormats, 1);
    db_unlock(dbin);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
      fprintf(stderr, "query failed: %s", PQerrorMessage(db));
      PQclear(res);
      goto failure;
    }
    PQclear(res);
  }

#if __BYTE_ORDER == __LITTLE_ENDIAN
  for (i=0; i<n_args; i++)
    db_byteswap(intype[i],n_rows,argin[i]);
#endif
  free(pquery);
#ifdef DEBUG
  printf("Exiting db_dms_array status=0\n");
#endif
  return 0;

 failure1:
  db_unlock(dbin);
  db_dms(dbin, NULL, dallocstmt); /* Ignore failure. */
 failure:
  QUERY_ERROR(query);
  free(pquery);
#ifdef DEBUG
  printf("Exiting db_dms_array status=1\n");
#endif
  return 1;
}



int db_bulk_insert_array(DB_Handle_t  *dbin, char *table, int n_rows, 
			 int n_args, DB_Type_t *intype, void **argin )
{
  PGconn *db;
  PGresult *res;
  int n,i,j;
  int bufsize;
  int *paramLengths[MAXARG];
  char *query=NULL,*p;
  char *buf=NULL;
  char header[20] = "PGCOPY\n\377\r\n\0\0\0\0\0\0\0\0\0";
  unsigned short nfield;
  unsigned int len;
  int status = 0;

#ifdef DEBUG
  FILE *fp;
  printf("Entering db_bulk_insert_array.\n");
#endif

  if (dbin==NULL)
    return 1;
  db = dbin->db_connection;
  if (n_args>=MAXARG)
  {
    fprintf(stderr,"Maximum number of arguments exceeded.\n");
    status = 1;
    goto failure;
  }
  
#if __BYTE_ORDER == __LITTLE_ENDIAN
  for(i=0; i<n_args; i++)
    db_byteswap(intype[i],n_rows,argin[i]);
#endif
  XASSERT(query = malloc(strlen(table)+1000));
  bufsize = 19 + 2*n_rows + 4*n_rows*n_args + 2;
  for (j=0; j<n_args; j++)
  {
    if (intype[j] == DB_STRING || intype[j] == DB_VARCHAR )
    {
      XASSERT(paramLengths[j] = malloc(n_rows*sizeof(int)));
      for (i=0; i<n_rows; i++)
      {
	paramLengths[j][i] = strlen(((char **) argin[j])[i]);
	bufsize += paramLengths[j][i];
#ifdef DEBUG
	printf("len('%s') = %d\n",((char **) argin[j])[i],paramLengths[j][i]);
#endif
      }  
    }  
    else
      bufsize += n_rows*db_sizeof(intype[j]);
  }

  XASSERT(buf = malloc(bufsize));
  p = buf;
  memcpy(p, header, 19);
  p += 19;
  nfield = htons((short)n_args);
  /* Unfortunately we have to transpose the input arguments
     to arrive at the format Postgres expects. */
  for (i=0; i<n_rows; i++)
  {
    memcpy(p, &nfield, sizeof(short));
    p += sizeof(short);
    for (j=0; j<n_args; j++)
    {
      if (intype[j] == DB_STRING || intype[j] == DB_VARCHAR )
      {
	len = htonl(paramLengths[j][i]);
	memcpy(p, &len, sizeof(int));
	p += sizeof(int);
	memcpy(p, ((char **) argin[j])[i], paramLengths[j][i]);
	p += paramLengths[j][i];
      }
      else
      {
	n = db_sizeof(intype[j]);
	len = htonl(n);
	memcpy(p, &len, sizeof(int));
	p += sizeof(int);
	memcpy(p, ((char *)argin[j]) + i * n, n);
	p += n;
      }
    }
  }
  nfield = htons((short)-1);
  memcpy(p, &nfield, sizeof(short));
  p += sizeof(short);

#ifdef DEBUG
  fp = fopen("test.bin","w");
  fwrite(buf,bufsize,1,fp);
  fclose(fp);
#endif
  sprintf(query,"copy %s from stdin binary", table);

  /* Do the actual database operations. */
  db_lock(dbin);
  if (dbin->abort_now) {
    status = 1;
    goto failure;
  }
  /* Issue COPY statement. */
  res = PQexec(db, query);
  if (PQresultStatus(res) != PGRES_COPY_IN)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    PQclear(res);
    status = 1;
    goto failure;
  }
  PQclear(res);
  /* Send the data across. */
  if (PQputCopyData(db, buf, bufsize) == -1)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    status = 1;
    goto failure;
  }
  /* Tell the server that we are done. */
  if (PQputCopyEnd(db, NULL) == -1)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    status = 1;
    goto failure;
  }
  /* Get result */
  res = PQgetResult(db);
  if (PQresultStatus(res) != PGRES_COMMAND_OK)
  {
    fprintf(stderr, "query failed: %s", PQerrorMessage(db));
    PQclear(res);
    status = 1;
    goto failure;
  }
  PQclear(res);
#if __BYTE_ORDER == __LITTLE_ENDIAN
  for (i=0; i<n_args; i++)
    db_byteswap(intype[i],n_rows,argin[i]);
#endif
#ifdef DEBUG
  printf("Exiting db_dms_array status=0\n");
#endif

 failure:
  db_unlock(dbin);
  for (j=0; j<n_args; j++)
  {
    if (intype[j] == DB_STRING || intype[j] == DB_VARCHAR )
    {
      free(paramLengths[j]);
    }  
  }
  free(buf);
  free(query);
#ifdef DEBUG
  printf("Exiting db_dms_array status=%d\n", status);
#endif
  return status;
}

/* Set transaction isolation level
   0 = read commited
   1 = serializable.
   2 = read only.
 */
int db_isolation_level(DB_Handle_t  *dbin, int level)
{
  if (dbin == NULL) 
    return 1;

  switch(level)
  {
  case DB_TRANS_READCOMMIT:
    dbin->isolation_level = DB_TRANS_READCOMMIT;
    return db_dms(dbin, NULL, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
    break;
  case DB_TRANS_SERIALIZABLE:
    dbin->isolation_level = DB_TRANS_SERIALIZABLE;
    return db_dms(dbin, NULL, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    break;
  case DB_TRANS_READONLY:
    dbin->isolation_level = DB_TRANS_READONLY;
    return db_dms(dbin, NULL, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ ONLY");
    break;
  default:
    fprintf(stderr,"db_isolation_level: Invalid isolation level (%d) specified. Legal values are 0 = read commited, 1 = serializable, 2 = read only.\n", level);
    return 1;
    break;    
  }
}
  
/* Begin new transaction */
int db_start_transaction(DB_Handle_t  *db)
{
  return db_dms(db,NULL,"BEGIN");  
}


/* Commit the current transaction. */
int db_commit(DB_Handle_t  *db)
{  
  return db_dms(db,NULL,"COMMIT");
}

/* Roll back the current transaction. */
int db_rollback(DB_Handle_t  *db)
{
  return db_dms(db,NULL,"ROLLBACK");
}



/* Functions for SEQUENCE operations. */
long long db_sequence_getnext(DB_Handle_t *db,  char *tablename)
{
  DB_Text_Result_t *res;
  char query[1024];  
  long long val;

  sprintf(query, "SELECT nextval('%s_seq')",tablename);
  res = db_query_txt(db, query);
  if (res==NULL ||  res->num_rows != 1 || res->num_cols !=1)
    val = (long long)-1;
  else
    val = strtoll(res->field[0][0], NULL, 10);
  if (res)
    db_free_text_result(res);

  return val;
}

/* Get n new values. */
#define PG_MAXARGS (1600)
long long *db_sequence_getnext_n(DB_Handle_t *db,  char *tablename, int n)
{
  DB_Binary_Result_t *res;
  int len,count,chunk, i;
  char *query,*p;  
  long long *val;

  XASSERT(n>0);
  len = strlen(tablename)+16;
  XASSERT(query = malloc(len*n+10));
  XASSERT(val = malloc(sizeof(long long)*n));
  
  count = 0;
  while (count < n)
  {
    p = query;
    p += sprintf(p, "SELECT ");
    chunk = (n-count)<PG_MAXARGS ? (n-count) : PG_MAXARGS;
    for (i=0; i<chunk-1; i++)
      p += sprintf(p, "nextval('%s_seq'),",tablename);
    p += sprintf(p, "nextval('%s_seq')",tablename);
    *p = 0;
    res = db_query_bin(db, query);
    if (res==NULL ||  res->num_rows != 1 || res->num_cols != chunk)
    {
      free(val);
      val = NULL;
      if (res)
	db_free_binary_result(res);
      break;
    }
    else
    {    
      for (i=0; i<chunk; i++)
	val[count++] = *((long long *)res->column[i].data);
    }
    if (res)
      db_free_binary_result(res);
  }
  free(query);

  return val;
}


long long db_sequence_getcurrent(DB_Handle_t *db,  char *tablename)
{
  DB_Text_Result_t *res;
  char query[512];  
  unsigned int val;

  sprintf(query, "SELECT currval('%s_seq')",tablename);
  res = db_query_txt(db, query);
  if (res==NULL ||  res->num_rows != 1 || res->num_cols !=1)
    val = (long long)-1;
  else
    val = strtoll(res->field[0][0], NULL, 10);
  if (res)
    db_free_text_result(res);

  return val;
}

/* This read the current last value in the table. This can be unreliable
   since other session might be caching values beyond it. */
long long db_sequence_getlast(DB_Handle_t *db,  char *tablename)
{
  DB_Text_Result_t *res;
  char query[512];  
  unsigned int val;

  sprintf(query, "SELECT last_value from %s_seq",tablename);
  res = db_query_txt(db, query);
  if (res==NULL ||  res->num_rows != 1 || res->num_cols !=1)
    val = (long long)-1;
  else
    val = strtoll(res->field[0][0], NULL, 10);
  if (res)
    db_free_text_result(res);

  return val;
}

int db_sequence_create(DB_Handle_t *db,  char *tablename)
{
  char query[512];
  sprintf(query, "CREATE SEQUENCE %s_seq",tablename);
  return db_dms(db,NULL,query);
}

int db_sequence_drop(DB_Handle_t *db,  char *tablename)
{
  char query[512];
  sprintf(query, "DROP SEQUENCE %s_seq",tablename);
  return db_dms(db,NULL,query);
}
