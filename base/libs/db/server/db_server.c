#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <zlib.h>
#include <pthread.h>
#include <alloca.h>
#include "db.h"
#include "xassert.h"
#ifdef DEBUG
#include "timer.h"
#endif

/***************  Server side functions *******************/
void db_write_statncnt(int sockfd, int status, int row_count)
{
  struct iovec vec[2];
  status = htonl(status);
  vec[0].iov_len = sizeof(status);
  vec[0].iov_base = &status;
  row_count = htonl(row_count);
  vec[1].iov_len = sizeof(row_count);
  vec[1].iov_base = &row_count;
  Writevn(sockfd, vec, 2);
}


int db_tcp_listen(char *host, int len, short *port)
{
  int on = 1;
  struct sockaddr_in server;
  int sockfd, size = sizeof(struct sockaddr_in);

  if (gethostname(host, len))
    return -1;


  /* set up the transport end point */

  for (;;)
  {
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) != -1)
      break;
  }

  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
  {
    perror("setsockopt error");
    return -1;
  }

  /* bind server address to the socket */
  server.sin_family = AF_INET;     /* host byte order */
  server.sin_port = 0; /* short, network byte order */
  server.sin_addr.s_addr = 0;
  memset(&(server.sin_zero), 0, 8);    /* zero the rest of the struct */

  struct sockaddr *serverp = (struct sockaddr *)&server;
  if ( bind(sockfd, serverp, size) < 0)
  {
    perror("bind call failed.");
    close(sockfd);
    return -1;
  }

  /* Get info about socket we just bound to. */
  if (getsockname(sockfd, serverp, (socklen_t *)&size))
  {
    perror("getsockname call failed.");
    return -1;
  }
#ifdef BLAHBLAH
  {
    int rcvbuf, sndbuf;
    db_getsocketbufsize(sockfd, &sndbuf,&rcvbuf);
#ifdef DEBUG
    printf("Original socket buffer sizes: SO_SNDBUF = %d, SO_RCVBUF = %d\n",sndbuf,rcvbuf);
#endif
    if (sndbuf<65536)
    {
      sndbuf = 65536/2;
      db_setsocketbufsize(sockfd, sndbuf, rcvbuf/2);
    }
    db_getsocketbufsize(sockfd, &sndbuf,&rcvbuf);
#ifdef DEBUG
    printf("New socket buffer sizes: SO_SNDBUF = %d, SO_RCVBUF = %d\n",sndbuf,rcvbuf);
#endif
  }
#endif
  /* Start listening for incoming connections */
  if ( listen(sockfd, 5) < 0 )
  {
    perror("listen call failed.");
    return -1;
  }

  *port = ntohs(server.sin_port);
  return sockfd;
}


/* Send query result to client. */
int db_send_text_result(int sockfd, DB_Text_Result_t *result, int comp)
{
  int buflen;
  unsigned long zlen;
  char *zbuf;
  int tmp[4];
  struct iovec vec[5];

  if (result)
  {
    /* Always send num_rows and num_cols even if the result is empty. */
    tmp[0] = htonl(result->num_rows);
    vec[0].iov_len = sizeof(tmp[0]);
    vec[0].iov_base = &tmp[0];
    tmp[1] = htonl(result->num_cols);
    vec[1].iov_len = sizeof(tmp[1]);
    vec[1].iov_base = &tmp[1];

    if (result->num_rows>0)
    {
      /* Non-empty result. Send the packed text buffer. */
      buflen = ntohl(*((int *)result->buffer));
      if (comp)
      {
	zlen = buflen+buflen/100+24;
        zbuf = malloc(zlen);
        XASSERT(zbuf);
	if (compress ((unsigned char *)zbuf, &zlen,
	    ((unsigned char *)result->buffer+sizeof(int)),
	    (buflen-sizeof(int))) != Z_OK)
	  return 1; /* FIXME: This is likely fatal: What to tell the client?!? */

	tmp[2] = htonl(zlen);
	vec[2].iov_len = sizeof(tmp[0]);
	vec[2].iov_base = &tmp[0];
	tmp[3] = htonl(buflen);
	vec[3].iov_len = sizeof(tmp[1]);
	vec[3].iov_base = &tmp[1];
	vec[4].iov_len = zlen;
	vec[4].iov_base = zbuf;
	Writevn(sockfd, vec, 5);
	free(zbuf);
      }
      else
      {
	/*	tmp[2] = htonl(buflen);
	vec[2].iov_len = sizeof(tmp[2]);
	vec[2].iov_base = &tmp[2];
	vec[3].iov_len = buflen;
	vec[3].iov_base = result->buffer;
	Writevn(sockfd, vec, 4);
*/
	vec[2].iov_len = buflen;
	vec[2].iov_base = result->buffer;
	Writevn(sockfd, vec, 3);
      }
    }
    else
      Writevn(sockfd, vec, 2);
  }
  return 0;
}


/* Send query result to client. */
int db_send_binary_result(int sockfd, DB_Binary_Result_t *result, int comp)
{
  int i,j;
  int vc,tc;
  DB_Column_t *col;
  struct iovec *vec;
  int *tmp;
  int anynull;

  if (result)
  {
    vec = malloc((3+7*result->num_cols)*sizeof(struct iovec));
    XASSERT(vec);
    tmp = malloc((3+7*result->num_cols)*sizeof(int));
    XASSERT(tmp);

    /* Byteswap arrays and test . */
    for (i=0; i<result->num_cols; i++)
    {
      col = &result->column[i];
      db_hton(col->type, result->num_rows, col->data);
      db_hton(DB_INT2, result->num_rows, col->is_null);
    }

    /* Pack all data into I/O vectors. */
    vc=0; tc=0;
    /* Number of rows. */
    tmp[tc] = htonl(result->num_rows);
    vec[vc].iov_len = sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc; ++vc;
    /* Number of columns. */
    tmp[tc] = htonl(result->num_cols);
    vec[vc].iov_len = sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc; ++vc;

    for (i=0; i<result->num_cols; i++)
    {
      col = &result->column[i];
      /* Column name */
      vec[vc+1].iov_len = strlen(col->column_name);
      vec[vc+1].iov_base = col->column_name;
      /* Length of column name */
      tmp[tc] = htonl(vec[vc+1].iov_len);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      vc+=2; ++tc;
      /* Column type. */
      tmp[tc] = htonl(col->type);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      ++tc; ++vc;
      /* Column data size. */
      tmp[tc] = htonl(col->size);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      ++tc; ++vc;
      /* Column Data */
      vec[vc].iov_len = result->num_rows*col->size;
      vec[vc].iov_base = col->data;
      ++vc;

      /* Check if there are any NULL values in this column. */
      anynull = 0;
      for (j=0; j<result->num_rows; j++)
	anynull = anynull | (int)col->is_null[j];
      if (anynull)
	anynull = 1;
      /* Anynull - if FALSE then the null indicator array is not sent. */
      tmp[tc] = htonl(anynull);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      ++tc; ++vc;
      if (anynull)
      {
	/* Column Null indicator array */
	vec[vc].iov_len = sizeof(short)*result->num_rows;
	vec[vc].iov_base = col->is_null;
	++vc;
      }
    }
    Writevn(sockfd, vec,vc);

    /* Byteswap arrays back. */
    for (i=0; i<result->num_cols; i++)
    {
      col = &result->column[i];
      db_hton(col->type, result->num_rows, col->data);
      db_hton(DB_INT2, result->num_rows, col->is_null);
    }
    free(vec);
    free(tmp);
  }
  return 0;
}

int db_server_query_bin(int sockfd, DB_Handle_t *db_handle)
{
  int tmp, len,status, comp;
  char *query;
  DB_Binary_Result_t *result;

  status = -1;
  if ( Readn(sockfd, &tmp, sizeof(int)) == sizeof(int) )
  {
    /* Get query string from client. */
    len = ntohl(tmp);
    query = malloc(len+1);
    XASSERT(query);
    Readn(sockfd, query, len);
    query[len] = '\0';

    /* Get compression flag from client. */
    comp = Readint(sockfd);

    /* Query database. */
    result = db_query_bin(db_handle, query);

    /* Send result to client. */
    if (result)
    {
      status = 0; /* Success */
      if (db_send_binary_result(sockfd, result, comp))
      {
	fprintf(stderr,"Query transmission failed.\n");
	status = -1;
      }
      db_free_binary_result(result);
    }
    else
    {
      fprintf(stderr,"Query failed, check query string.\n");
      Writeint(sockfd, -1);

      if (*db_handle->errmsg)
      {
         send_string(sockfd, db_handle->errmsg);
      }
      else
      {
         send_string(sockfd, "no message");
      }
    }
    free(query);
  }

  return status;
}




int db_server_query_bin_array(int sockfd, DB_Handle_t *db_handle)
{
  int i, tmp, len,status, comp, n_args;
  char *query;
  DB_Binary_Result_t *result;
  DB_Type_t intype[MAXARG];
  void *argin[MAXARG];

  status = -1;
  if ( Readn(sockfd, &tmp, sizeof(int)) == sizeof(int) )
  {
    /* Get query string from client. */
    len = ntohl(tmp);
    query = malloc(len+1);
    XASSERT(query);
    Readn(sockfd, query, len);
    query[len] = '\0';

    /* Get compression flag from client. */
    comp = Readint(sockfd);

    /* Get argument list from client */
    n_args = Readint(sockfd);
    for (i=0; i<n_args; i++)
    {
      argin[i] = (void *)Read_dbtype(&(intype[i]), sockfd);
    }

    /* Query database. */
    result = db_query_bin_array(db_handle, query, n_args, intype, argin);

    /* Free arguments. */
    for (i=0; i<n_args; i++)
      free(argin[i]);

    /* Send result to client. */
    if (result)
    {
      status = 0; /* Success */
      if (db_send_binary_result(sockfd, result, comp))
      {
	fprintf(stderr,"Query transmission failed.\n");
	status = -1;
      }
      db_free_binary_result(result);
    }
    else
    {
      fprintf(stderr,"Query failed, check query string.\n");

        /* I think this is a bug. On error, I believe that -1 should be pushed into the socket. On error,
         * db_recv_binary_query() is expecting -1, not 0. But I don't want to change something that
         * "has been working" for several years.
         *
         *   -Art 2014/01/24 */
      Writeint(sockfd, 0);
    }
    free(query);
  }

  return status;
}

int db_server_query_bin_ntuple(int sockfd, DB_Handle_t *db_handle)
{
    DB_Binary_Result_t **result = NULL;
    DB_Binary_Result_t *exeResult = NULL;
    int tmp;
    int len;
    char *stmnt = NULL;
    int nexes;
    int nargs;
    int iExe;
    int iArg;
    DB_Type_t dbtypes[MAXARG];
    void *values[MAXARG];
    char *buffer[MAXARG];
    char *pBuf = NULL;
    int status = -1;

    /* Read the length of the db statement. */
    if (Readn(sockfd, &tmp, sizeof(int)) == sizeof(int))
    {
        /* Read the db statement. */
        len = ntohl(tmp);
        stmnt = calloc(len + 1, sizeof(char));
        XASSERT(stmnt);
        Readn(sockfd, stmnt, len);

        /* Read the number of statement exes.*/
        nexes = Readint(sockfd);

        /* Read the number of placeholders. */
        nargs = Readint(sockfd);

        /* Read the data types of the placeholders. */
        for (iArg = 0; iArg < nargs; iArg++)
        {
            dbtypes[iArg] = (DB_Type_t)Readint(sockfd);
        }

        /* Read the placeholder values. */
        for (iArg = 0; iArg < nargs; iArg++)
        {
            if (dbtypes[iArg] == DB_STRING || dbtypes[iArg] == DB_VARCHAR)
            {
                /* Read the length of the string of placeholder string values, and allocate a buffer that size. */
                len = Readint(sockfd);
                buffer[iArg] = calloc(len, sizeof(char));
                XASSERT(buffer[iArg]);
                values[iArg] = calloc(nexes, sizeof(void *));
                XASSERT(values[iArg]);

                /* Read the string of placeholder string values into the buffer. */
                Readn(sockfd, buffer[iArg], len);
                pBuf = buffer[iArg];

                /* Put the placholder strings into the values[iArg] array. */
                for (iExe = 0; iExe < nexes; iExe++)
                {
                    ((char **)values[iArg])[iExe] = pBuf;
                    while(*pBuf)
                    {
                        ++pBuf;
                    }
                    ++pBuf;
                }
            }
            else
            {
                /* We do not need an additional buffer for non-string placeholder values. We just copy
                 * in the data directly to values[iArg]. */
                buffer[iArg] = NULL;

                /* Allocate a buffer to hold the string containing the nexes placeholder values. */
                len = nexes * db_sizeof(dbtypes[iArg]);
                values[iArg] = calloc(len, sizeof(char));
                XASSERT(values[iArg]);
                Readn(sockfd, values[iArg], len);
                db_ntoh(dbtypes[iArg], nexes, values[iArg]);
            }
        }

        result = db_query_bin_ntuple(db_handle, stmnt, nexes, nargs, dbtypes, values);

        if (result)
        {
            /* Send the number of DB_Binary_Result_t structs being returned. */
            Writeint(sockfd, nexes);

            /* Send nexes DB_Binary_Result_t structs. */
            status = 0; /* Assume success - a failure will short the loop and set status to -1. */
            for (iExe = 0; iExe < nexes; iExe++)
            {
                exeResult = result[iExe];
                if (db_send_binary_result(sockfd, exeResult, 0))
                {
                    fprintf(stderr, "Transmission of binary result failed.\n");
                    status = -1;
                    db_free_binary_result(exeResult);

                    /* The client will interpret a -1 as an error. */
                    Writeint(sockfd, -1);
                    break;
                }

                db_free_binary_result(exeResult);
            }
        }
        else
        {
            fprintf(stderr, "Prepared statement failed: %s.\n", stmnt);
            status = -1;

            /* The client will interpret a -1 as an error. */
            Writeint(sockfd, -1);
        }

        /* Free temporary buffers. */
        free(stmnt);

        for (iArg = 0; iArg < nargs; iArg++)
        {
            if (buffer[iArg])
            {
                free(buffer[iArg]);
            }

            if (values[iArg])
            {
                free(values[iArg]);
            }
        }
    }

    return status;
}


int db_server_query_txt(int sockfd, DB_Handle_t *db_handle)
{
  int tmp, len, status;
  char *query;
  DB_Text_Result_t *result;
  int comp;

  status = -1;
  if ( Readn(sockfd, &tmp, sizeof(int)) == sizeof(int) )
  {
    /* Get query string from client. */
    len = ntohl(tmp);
    query = alloca(len+1);
    Readn(sockfd, query, len);
    query[len] = '\0';

    /* Get compression flag from client. */
    comp = Readint(sockfd);

    /* Query database. */
    result = db_query_txt(db_handle, query);

    /* Send result to client. */
    if (result)
    {
      status = 0; /* Success */
      if (db_send_text_result(sockfd, result, comp))
      {
	fprintf(stderr,"Query transmission failed.\n");
	status = -1;
      }
      db_free_text_result(result);
    }
    else
    {
      fprintf(stderr,"Query failed, check query string.\n");
      Writeint(sockfd, -1); /* num rows */
      Writeint(sockfd, -1); /* num cols */

      if (*db_handle->errmsg)
      {
         send_string(sockfd, db_handle->errmsg);
      }
      else
      {
         send_string(sockfd, "no message");
      }
    }
  }

  return status;
}


int db_server_dms(int sockfd, DB_Handle_t *db_handle)
{
  int tmp, len, status, row_count;
  char *query;

  status = -1;
  if ( Readn(sockfd, &tmp, sizeof(int)) == sizeof(int) )
  {
    /* Get query string from client. */
    len = ntohl(tmp);
    query = malloc(len+1);
    XASSERT(query);
    Readn(sockfd, query, len);
    query[len] = '\0';

    status = db_dms(db_handle, &row_count, query);
    db_write_statncnt(sockfd, status, row_count);
    free(query);
  }

  return status;
}


int db_server_dms_array(int sockfd, DB_Handle_t *db_handle)
{
  int i,j;
  int tmp, len, status, row_count;
  char *query, *p, **buffer;
  int n_rows, n_args;
  void **argin;
  DB_Type_t *intype;


  status = -1;
  if (Readn(sockfd, &tmp, sizeof(int)) == sizeof(int))
  {
#ifdef DEBUG
    StartTimer(1);
#endif
    /* Get query string from client. */
    len = ntohl(tmp);
    query = malloc(len+1);
    XASSERT(query);
    Readn(sockfd, query, len);
    query[len] = '\0';

    /* Receive the argument data in temporary buffers. */
    n_rows = Readint(sockfd);
    n_args = Readint(sockfd);
    intype = malloc(n_args*sizeof(int));
    XASSERT(intype);
    argin = malloc(n_args*sizeof(void *));
    XASSERT(argin);
    buffer = malloc(n_args*sizeof(char *));
    XASSERT(buffer);
    for (i=0; i<n_args; i++)
      intype[i] = (DB_Type_t) Readint(sockfd);

    for (i=0; i<n_args; i++)
    {
      if ( intype[i] == DB_STRING || intype[i] == DB_VARCHAR )
      {
	len = Readint(sockfd);
        buffer[i] = malloc(len);
        XASSERT(buffer[i]);
        argin[i] = malloc(n_rows*sizeof(void *));
        XASSERT(argin[i]);

	Readn(sockfd, buffer[i], len);
	p = buffer[i];
	for (j=0; j<n_rows; j++)
	{
	  ((char **)argin[i])[j] = p;
	  while(*p)
	    ++p;
	  ++p;
	}
      }
      else
      {
	buffer[i] = NULL;
	len = n_rows*db_sizeof(intype[i]);
        argin[i] = malloc(len);
        XASSERT(argin[i]);
	Readn(sockfd, argin[i], len);
	db_ntoh(intype[i], n_rows, argin[i]);
      }
    }
#ifdef DEBUG
    printf("Time to transfer data = %f.\n",StopTimer(1));
    StartTimer(1);
#endif
    /* Perform the actual database operation. */
    status = db_dms_array(db_handle, &row_count, query,
			  n_rows, n_args, intype, argin);
#ifdef DEBUG
    printf("Time to perform query = %f.\n",StopTimer(1));
#endif

    /* Return status to client. */
    db_write_statncnt(sockfd, status, row_count);

    /* Free temporary buffers. */
    free(query);
    free(intype);
    for (i=0; i<n_args; i++)
    {
      if (buffer[i])
	free(buffer[i]);
      free(argin[i]);
    }
    free(buffer);
    free(argin);
  }

  return status;
}


int db_server_bulk_insert_array(int sockfd, DB_Handle_t *db_handle)
{
  int i,j;
  int tmp, len, status;
  char *table, *p, **buffer;
  int n_rows, n_args;
  void **argin;
  DB_Type_t *intype;


  status = -1;
  if (Readn(sockfd, &tmp, sizeof(int)) == sizeof(int))
  {
#ifdef DEBUG
    StartTimer(1);
#endif
    /* Get query string from client. */
    len = ntohl(tmp);
    table = malloc(len+1);
    XASSERT(table);
    Readn(sockfd, table, len);
    table[len] = '\0';

    /* Receive the argument data in temporary buffers. */
    n_rows = Readint(sockfd);
    n_args = Readint(sockfd);
    intype = malloc(n_args*sizeof(int));
    XASSERT(intype);
    argin = malloc(n_args*sizeof(void *));
    XASSERT(argin);
    buffer = malloc(n_args*sizeof(char *));
    XASSERT(buffer);
    for (i=0; i<n_args; i++)
      intype[i] = (DB_Type_t) Readint(sockfd);

    for (i=0; i<n_args; i++)
    {
      if ( intype[i] == DB_STRING || intype[i] == DB_VARCHAR )
      {
	len = Readint(sockfd);
        buffer[i] = malloc(len);
        XASSERT(buffer[i]);
        argin[i] = malloc(n_rows*sizeof(void *));
        XASSERT(argin[i]);

	Readn(sockfd, buffer[i], len);
	p = buffer[i];
	for (j=0; j<n_rows; j++)
	{
	  ((char **)argin[i])[j] = p;
	  while(*p)
	    ++p;
	  ++p;
	}
      }
      else
      {
	buffer[i] = NULL;
	len = n_rows*db_sizeof(intype[i]);
        argin[i] = malloc(len);
        XASSERT(argin[i]);
	Readn(sockfd, argin[i], len);
	db_ntoh(intype[i], n_rows, argin[i]);
      }
    }
#ifdef DEBUG
    printf("Time to transfer data = %f.\n",StopTimer(1));
    StartTimer(1);
#endif
    /* Perform the actual database operation. */
    status = db_bulk_insert_array(db_handle, table, n_rows, n_args, intype, argin, 0);
#ifdef DEBUG
    printf("Time to perform query = %f.\n",StopTimer(1));
#endif

    /* Return status to client. */
    Writeint(sockfd, status);

    /* Free temporary buffers. */
    free(table);
    free(intype);
    for (i=0; i<n_args; i++)
    {
      if (buffer[i])
	free(buffer[i]);
      if (argin[i])
	free(argin[i]);
    }
    free(buffer);
    free(argin);
  }

  return status;
}



int db_server_sequence_drop(int sockfd, DB_Handle_t *db_handle)
{
  int status;
  char *table;

  table = receive_string(sockfd);
  status = db_sequence_drop(db_handle, table);
  Writeint(sockfd, status);
  free(table);
  return status;
}

int db_server_sequence_create(int sockfd, DB_Handle_t *db_handle)
{
  int status;
  char *table;

  table = receive_string(sockfd);
  status = db_sequence_create(db_handle, table);
  Writeint(sockfd, status);
  free(table);
  return status;
}


int db_server_sequence_getnext(int sockfd, DB_Handle_t *db_handle)
{
  char *table;
  long long id;

  table = receive_string(sockfd);
  id = db_sequence_getnext(db_handle, table);
  Writelonglong(sockfd, id);
  free(table);
  if (id == -1LL)
    return 1;
  else
    return 0;
}

int db_server_sequence_getnext_n(int sockfd, DB_Handle_t *db_handle)
{
  char *table;
  int i,n, status;
  long long *seqnums;
  struct iovec vec[2];


  table = receive_string(sockfd);
  n = Readint(sockfd);
  if ((seqnums = db_sequence_getnext_n(db_handle, table, n)) == NULL)
  {
    status = 1;
    Writeint(sockfd, status);
  }
  else
  {
    int tmp;
    status = 0;
    tmp = htonl(status);
    vec[0].iov_len = sizeof(int);
    vec[0].iov_base = &tmp;
    for (i=0; i<n; i++)
      seqnums[i] = htonll( seqnums[i] );
    vec[1].iov_len = n*sizeof(long long);
    vec[1].iov_base = seqnums;
    Writevn(sockfd, vec, 2);
    free(seqnums);
  }
  free(table);
  return status;
}

int db_server_sequence_getcurrent(int sockfd, DB_Handle_t *db_handle)
{
  char *table;

  table = receive_string(sockfd);
  Writelonglong(sockfd, db_sequence_getcurrent(db_handle, table));
  free(table);
  return 0;
}

int db_server_sequence_getlast(int sockfd, DB_Handle_t *db_handle)
{
  char *table;

  table = receive_string(sockfd);
  Writelonglong(sockfd, db_sequence_getlast(db_handle, table));
  free(table);
  return 0;
}
