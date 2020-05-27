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
#include "timer.h"
#include "xmem.h"
/***************  Client side functions *******************/

DB_Text_Result_t *db_recv_text_query(int sockfd, int comp, char **errmsg)
{
  DB_Text_Result_t *result;
  char *buffer, *zbuf;
  int nrows, ncols;
  
  nrows = Readint(sockfd);
  ncols = Readint(sockfd);

  if (nrows<=0)
  {
      /* nrows == 0 does not imply an error. The query could have returned 0 rows. 
       * When this happens, return a non-NULL (albeit meaningless) result. */
     char *msg = NULL;

    result = malloc(sizeof(DB_Text_Result_t));
    XASSERT(result);
    memset(result, 0, sizeof(DB_Text_Result_t));
    result->num_rows = 0;
    result->num_cols = ncols;

    if (nrows == -1)
    {
        /* An error occurred when querying the database. Free the result and 
         * return any error message that the query generated. */
        free(result);
        result = NULL;
        
        msg = receive_string(sockfd);
        
        if (errmsg)
        {
            *errmsg = strdup(msg);
        }
    }

    /* FIXME We really should send the column names even in this case... */
    return result;
  }
  else
  {
    /* Get result back from server. */
    if (comp)
    {
      int tmp;
      unsigned long zlen, buflen;
      Readn(sockfd, &tmp, sizeof(int)); /* Size of message. */
      zlen = (unsigned long) ntohl(tmp);
      /*    printf("Received zlen = %lu\n",zlen); */
      if (zlen>0)
      {
	/* Receive data. */
	Readn(sockfd, &tmp, sizeof(int)); /* Size of uncompressed message. */
	buflen = (unsigned long) ntohl(tmp);
	/*      printf("Received buflen = %lu\n",buflen); */
        zbuf = malloc(zlen); /* Allocate buffer for compressed data. */
        XASSERT(zbuf);
	Readn(sockfd, zbuf, zlen);      /* receive compressed message */
	
	/* Uncompress */
        buffer = malloc(buflen); /* Allocate buffer for uncompressed data. */
	XASSERT(buffer);
	*((int *)buffer) = htonl((int)buflen);
	if (uncompress ((unsigned char*)buffer+sizeof(int), &buflen,
	    (unsigned char *)zbuf, zlen) != Z_OK) {
	  free(zbuf);
	  free(buffer);
	  fprintf(stderr,"db_recv_text_query: uncompress failed.\n");
	  return NULL;
	}
	free(zbuf);
	buflen += sizeof(int);
	/* Check that the length of the uncompressed string matches 
	   the value sent by the server. */
	if (buflen != htonl(*((int *)buffer)))
	{
	  free(buffer);
	  fprintf(stderr,"db_recv_text_query failed: Uncompressed length = %lu, should be %d.\n",
		  buflen,htonl(*((int *)buffer)));
	  return NULL;
	}
      }
      else 
	return NULL;
    }
    else
    {
      int tmp, buflen;
      Readn(sockfd, &tmp, sizeof(int)); /* Size of message. */    
      buflen = ntohl(tmp);
      if (buflen>0)
      {
        buffer = malloc(buflen);
        XASSERT(buffer);
	*((int *)buffer) = htonl(buflen);
	/* receive bulk of message */
	Readn(sockfd, buffer+sizeof(int), buflen-sizeof(int));
      }
      else 
	return NULL;
    }
    /* Set up result data structure. */
    return db_unpack_text(buffer);
  }
}


DB_Text_Result_t *db_unpack_text(char *buf)
{
  DB_Text_Result_t *result;
  char *p;
  int buflen;
  unsigned int i,j;

  result = malloc(sizeof(DB_Text_Result_t));
  XASSERT(result);
  result->buffer = buf;

  /* First decode the integer fields at the beginning of the packet. */
  p = buf;
  buflen = ntohl(*((int *) p));
  p += sizeof(int); 
  result->num_rows = ntohl(*((int *) p));
  p += sizeof(int); 
  result->num_cols = ntohl(*((int *) p));    
  p += sizeof(int);

  /* Unpack column widths. */
  result->column_width = (int *)malloc(result->num_cols*sizeof(int));
  XASSERT(result->column_width);
  for (i=0; i<result->num_cols; i++)
  {
    result->column_width[i] = ntohl(*((int *) p));
    p += sizeof(int);
  }

  /* Unpack column names. */
  result->column_name = (char **)malloc(result->num_cols*sizeof(char *));
  XASSERT(result->column_name);
  result->column_name[0] = p;
  for (i=1; i<result->num_cols; i++)
  {
    while(*p++);
    result->column_name[i] = p;
  }
  while(*p++);

  /* Set up data structure for the actual contents. */
  result->field =  (char ***) malloc(result->num_rows*sizeof(char **) + result->num_rows*result->num_cols*sizeof(char *));
  XASSERT(result->field);
  for (i=0; i<result->num_rows; i++)
    result->field[i] = (char **) &result->field[result->num_rows + 
						i*result->num_cols];
  for(i = 0; i < result->num_rows; i++)
  {
    for (j = 0; j < result->num_cols; j++)
    {
      result->field[i][j] = p;
      while(*p++);
    }
  }  
  return result;
}

DB_Binary_Result_t *db_recv_binary_query(int sockfd, int comp, char **errmsg)
{
  //  uint64_t size=0;
  int i, anynull, nrows;
  DB_Column_t *col;
  DB_Binary_Result_t *result;

  result = malloc(sizeof(DB_Binary_Result_t));
  XASSERT(result);
  nrows = Readint(sockfd);
  //    size+=4;
  if (nrows == -1)
  {
      char *msg = receive_string(sockfd);
      
      if (errmsg)
      {
          *errmsg = strdup(msg);
      }
      
      return NULL;
  }
  else
    result->num_rows = (unsigned int) nrows;
  result->num_cols = (unsigned int) Readint(sockfd);
  //    size+=4;
  result->column = malloc(result->num_cols*sizeof(DB_Column_t));
  XASSERT(result->column);
  for (i=0; i<result->num_cols; i++)
  {
    col = &result->column[i];
    col->num_rows = result->num_rows; /* Already got that one. */
    /* Column name. */    
    col->column_name = receive_string(sockfd);
    //size+=strlen(col->column_name)+4;
    /* Column Type. */
    col->type = (DB_Type_t) Readint(sockfd);
    //    size+=4;
    /* Column data size. */
    col->size = Readint(sockfd);
    //size+=4;
    /* Column data. */
    col->data = malloc(result->num_rows*col->size);
    XASSERT(col->data);
    Readn(sockfd, col->data, result->num_rows*col->size);
    //size += result->num_rows*col->size;
    db_ntoh(col->type, result->num_rows, col->data);
    /* Anynull - if FALSE then the null indicator array is not sent. */
    anynull = Readint(sockfd);
    /* Null indicator array. */
    col->is_null = malloc(result->num_rows*sizeof(short));
    XASSERT(col->is_null);
    if (anynull)
    {
      Readn(sockfd, col->is_null, result->num_rows*sizeof(short));
      db_ntoh(DB_INT2, result->num_rows, (void *)col->is_null);
      //size += result->num_rows*sizeof(short);
    }
    else
      memset(col->is_null, 0, result->num_rows*sizeof(short));
  }
  //printf("nrows = %d, size = %lld, size/row=%f\n",result->num_rows,size,(float)size/result->num_rows);
  return result;
}

/* db_recv_binary_query_ntuple() returns an array of DB_Binary_Result_t structs. Each element of this
 * array is a DB_Binary_Result_t struct, one for each of the exes of the prepared statement.
 */
DB_Binary_Result_t **db_recv_binary_query_ntuple(int sockfd, char **errmsg)
{
    DB_Binary_Result_t **results = NULL;
    DB_Binary_Result_t *exeResult = NULL;
    
    int nelems;
    int iElem;

    /* The number of elements in the DB_Binary_Result_t struct array. We need to loop this many times to
     * fetch all DB_Binary_Result_t structs from the socket. */
    nelems = Readint(sockfd);
    
    if (nelems == -1)
    {
        /* There was an error - no DB_Binary_Result_t structs were returned. */
        char *msg = receive_string(sockfd);
        
        if (errmsg)
        {
            *errmsg = strdup(msg);
        }
        
        return NULL;
    }
    
    results = calloc(nelems, sizeof(DB_Binary_Result_t *));
    XASSERT(results);
    
    for (iElem = 0; iElem < nelems; iElem++)
    {
#if 0
        {
            nrows = Readint(sockfd);
            ncols = Readint(sockfd);
            
            exeResult = malloc(sizeof(DB_Binary_Result_t));
            XASSERT(exeResult);
            
            exeResult->column = malloc(ncols * sizeof(DB_Column_t));
            XASSERT(exeResult->column);
            for (iCol = 0; iCol < ncols; iCol++)
            {
                pCol = &exeResult->column[iCol];
                pCol->num_rows = nrows;
                
                /* Column name. */
                pCol->column_name = receive_string(sockfd);
                
                /* Column data type. */
                pCol->type = (DB_Type_t)Readint(sockfd);
                
                /* Column data size. */
                pCol->size = Readint(sockfd);
                
                /* Column data. */
                pCol->data = malloc(nrows * pCol->size);
                XASSERT(pCol->data);
                Readn(sockfd, pCol->data, nrows * pCol->size);
                db_ntoh(pCol->type, exeResult->num_rows, pCol->data);
                
                /* If the next int in the socket is 0, then no null-indicator array was. */
                anynull = Readint(sockfd);
                
                /* Null indicator array. */
                pCol->is_null = calloc(nrows, sizeof(short));
                XASSERT(pCol->is_null);
                if (anynull)
                {
                    Readn(sockfd, pCol->is_null, nrows * sizeof(short));
                    db_ntoh(DB_INT2, nrows, (void *)pCol->is_null);
                }
            }
        }
#endif
        /* I believe we can call the function that retrieves a single DB_Binary_Result_t struct
         * nelems times. This function will return NULL if an error occurred. */
        exeResult = db_recv_binary_query(sockfd, 0, errmsg);
        if (!exeResult)
        {
            char *msg = receive_string(sockfd);
            
            if (errmsg)
            {
                *errmsg = strdup(msg);
            }
            
            return NULL;
        }
        
        /* Push the resulting DB_Binary_Result_t into the return array. */
        results[iElem] = exeResult;
    }
    
    return results;
}

DB_Text_Result_t *db_client_query_txt(int sockfd, char *query, int compress, char **errmsg)
{
  int len,ctmp;
  struct iovec vec[3];
  
  /* pack query string. */
  vec[1].iov_len = strlen(query);
  vec[1].iov_base = query;
  len = htonl(vec[1].iov_len);
  vec[0].iov_len = sizeof(len);
  vec[0].iov_base = &len;
  /* pack compression flag. */
  ctmp = htonl(compress);
  vec[2].iov_len = sizeof(ctmp);
  vec[2].iov_base = &ctmp;
  /* Do the gathering write. */
  Writevn(sockfd, vec, 3);

  /* Get result back from server. */
  return db_recv_text_query(sockfd, compress, errmsg);
}


DB_Binary_Result_t *db_client_query_bin(int sockfd, char *query, int compress, char **errmsg)
{
  int len,ctmp;
  struct iovec vec[3];

  /* pack query string. */
  vec[1].iov_len = strlen(query);
  vec[1].iov_base = query;
  len = htonl(vec[1].iov_len);
  vec[0].iov_len = sizeof(len);
  vec[0].iov_base = &len;
  /* pack compression flag. */
  ctmp = htonl(compress);
  vec[2].iov_len = sizeof(ctmp);
  vec[2].iov_base = &ctmp;
  /* Do the gathering write. */
  Writevn(sockfd, vec, 3);

  /* Get result back from server. */
  return db_recv_binary_query(sockfd, compress, errmsg);
}

DB_Binary_Result_t *db_client_query_bin_array(int sockfd, char *query, 
					      int compress, int n_args,  
					      DB_Type_t *intype, void **argin )
{
  int i,vc,tc;  
  int *tmp;
  struct iovec *vec;
  char *errmsg = NULL;

  vec = malloc((4+3*n_args)*sizeof(struct iovec));
  XASSERT(vec);
  tmp = malloc((4+3*n_args)*sizeof(int));
  XASSERT(tmp);
  
  /* Send query string and argiments to server. */
  /* pack query string. */
  tc = 0; vc = 0;
  vec[vc+1].iov_len = strlen(query);
  vec[vc+1].iov_base = query;
  tmp[tc] = htonl(vec[vc+1].iov_len);
  vec[vc].iov_len = sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; vc+=2;
  /* pack compression flag. */
  tmp[tc] = htonl(compress);
  vec[vc].iov_len = sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; ++vc;
  /* Send argument list. */
  tmp[tc] = htonl(n_args);
  vec[vc].iov_len = sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; ++vc;
  for (i=0; i<n_args; i++)
  {
    /* Type */
    tmp[tc] = htonl((int)intype[i]);
    vec[vc].iov_len = sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc; ++vc;
    if (intype[i] == DB_STRING || intype[i]==DB_VARCHAR)
    {
      /* String length and data. */
      if (argin[i]==NULL)
	vec[vc+1].iov_len = 0;
      else
	vec[vc+1].iov_len = strlen((char *)argin[i]);
      vec[vc+1].iov_base = argin[i];
      tmp[tc] = htonl(vec[vc+1].iov_len);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      ++tc; vc+=2;
    }
    else
    {
      /* Scalar data data. */
      db_byteswap(intype[i], 1, (char *)argin[i]);  
      vec[vc].iov_len = db_sizeof(intype[i]);
      vec[vc].iov_base = argin[i];
      ++tc; ++vc;
    }
  }
  /* Do the gathering write. */
  Writevn(sockfd, vec, vc);

  for (i=0; i<n_args; i++)
  {
    if (!(intype[i] == DB_STRING || intype[i]==DB_VARCHAR))
      db_byteswap(intype[i], 1, (char *)argin[i]);  
  }
  free(tmp);
  free(vec);

  /* Get result back from server. */
  return db_recv_binary_query(sockfd, compress, &errmsg);
}

/* stmnt - the prepared statement.
 * nexes - number of times the prepared statement will execute
 * nargs - the number of placeholders in the prepared statement.
 * dbtypes - an array of nargs elements describing the data types of the placeholders.
 * values - an array of arrays of placeholder data values. Each values[iArg] array contains the data for
 *          a single placeholder. There are nargs values[iArg] arrays.
 */
DB_Binary_Result_t **db_client_query_bin_ntuple(int sockfd, const char *stmnt, unsigned int nexes, unsigned int nargs, DB_Type_t *dbtypes, void **values)
{
    char *errmsg = NULL;
    int *len = NULL; /* array of lengths */
    int tmp[10 + MAXARG];
    int tc;
    int vc;
    int sum;
    char *str = NULL;
    char *buffer[MAXARG];
    char *pBuf = NULL;
    void *stmntDup = NULL;
    int iArg;
    int iExe;
    struct iovec *vec = NULL; /* struct iovec is a struct with two fields: the address of a buffer, and the length, in bytes, of the buffer. */
    
    tc = 0;
    vc = 0;
    len = malloc(nexes * sizeof(int)); /* one per execution of the prepared statement (one per exe). */
    XASSERT(len);
    vec = malloc((4 + nargs * 3) * sizeof(struct iovec)); /* The maximum number of needed vecs is three per placeholder (if all placeholders were strings).
                                                           * One for the data types of the placeholders, one for
                                                           * And there are four other vectors used to hold the length of the db statement, the db statement,
                                                           * the number of statement exes, and the number of statement placeholders.
                                                           */
    XASSERT(vec);

    stmntDup = (void *)strdup(stmnt);
    XASSERT(stmntDup);
    
    /* Store the db statement. */
    vec[1].iov_len = strlen(stmntDup);
    vec[1].iov_base = stmntDup;
    
    /* Store the length of the db statement (in network-byte order). */
    tmp[tc] = ntohl(vec[1].iov_len);
    vec[0].iov_len =  sizeof(tmp[tc]);
    vec[0].iov_base = &tmp[tc];
    ++tc;
    vc += 2;
    
    /* Store the number of statement exes. */
    tmp[tc] = htonl(nexes);
    vec[vc].iov_len =  sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc; ++vc;
    
    /* Store the number of placeholders. */
    tmp[tc] = htonl(nargs);
    vec[vc].iov_len =  sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc;
    ++vc;
    
    /* Store the data types of the placeholders. */
    for (iArg = 0; iArg < nargs; iArg++)
    {
        tmp[tc] = htonl((int)dbtypes[iArg]);
        vec[vc].iov_len =  sizeof(tmp[tc]);
        vec[vc].iov_base = &tmp[tc];
        ++tc;
        ++vc;
    }
    
    for (iArg = 0; iArg < nargs; iArg++)
    {
        if (dbtypes[iArg] == DB_STRING || dbtypes[iArg] == DB_VARCHAR)
        {
            /* Count the number of BYTES of all nexes strings. The data for each placeholder resides in a C row (values[iArg]). The
             * nexes strings are stored linearly in the row, separated by '\0's. */
            sum = 0;
            for (iExe = 0; iExe < nexes; iExe++)
            {
                str = ((char **)values[iArg])[iExe];
                if (str)
                {
                    len[iExe] = strlen(str);
                }
                else
                {
                    len[iExe] = 0;
                }
                
                sum += len[iExe] + 1; /* One extra byte for a NULL char at the end of each iExe's string.
                                       * The nexes strings will be stored in an iovec with NULL chars separating them,
                                       * just as they are stored in the values array. */
            }
            
            /* Store the byte length of the placeholder data. */
            tmp[tc] = htonl(sum);
            vec[vc].iov_len = sizeof(tmp[tc]);
            vec[vc].iov_base = &tmp[tc];
            ++tc;
            ++vc;
            
            /* Store the nexes strings for the placeholder, one after another, in the packet to be pushed into the socket. */
            buffer[iArg] = malloc(sum);
            XASSERT(buffer[iArg]);
            pBuf = buffer[iArg];
            for (iExe = 0; iExe < nexes; iExe++)
            {
                memcpy(pBuf, ((char **)values[iArg])[iExe], len[iExe]);
                pBuf += len[iExe];
                *pBuf++ = 0;
            }
            
            vec[vc].iov_len = sum;
            vec[vc].iov_base = buffer[iArg];
            ++vc;
        }
        else
        {
            /* Store the nexes data values for the placeholder. */
            buffer[iArg] = NULL;
            db_hton(dbtypes[iArg], nexes, values[iArg]);
            vec[vc].iov_len = db_sizeof(dbtypes[iArg]) * nexes;
            vec[vc].iov_base = values[iArg];
            ++vc;
        }
    }
    
    /* Push the iovec array into the socket. */
    Writevn(sockfd, vec, vc);

    free(stmntDup);
    free(vec);
    free(len);
    
    for (iArg = 0; iArg < nargs; iArg++)
    {
        if (buffer[iArg])
        {
            free(buffer[iArg]);
            buffer[iArg] = NULL;
        }
        
        if (!(dbtypes[iArg] == DB_STRING || dbtypes[iArg] == DB_VARCHAR))
        {
            db_ntoh(dbtypes[iArg], nexes, values[iArg]);
        }
    }
    
    /* Pull the results out of the socket. */
    return db_recv_binary_query_ntuple(sockfd, &errmsg);
}

int db_client_dms(int sockfd, int *row_count, char *query)
{
  int status, n;

  /* Send query to server. */
  send_string(sockfd,query);

  /* Receive status and number of rows affected. */
  status = Readint(sockfd);
  n =  Readint(sockfd);
  if (row_count)
    *row_count = n;

  return status;
}


int db_client_dms_array(int sockfd,  int *row_count, char *query, 
			int n_rows, int n_args, DB_Type_t *intype, 
			void **argin )
{
  int status, n, i, j, tmp[10+MAXARG], tc, vc;
  int *len, sum;
  char *str, *buffer[MAXARG], *p;
  struct iovec *vec;

  tc = 0; vc = 0;
  len = malloc(n_rows*sizeof(int));
  XASSERT(len);
  vec = malloc((4+n_args*3)*sizeof(struct iovec));
  XASSERT(vec);
  vec[1].iov_len = strlen(query);
  vec[1].iov_base = query;
  tmp[tc] = ntohl(vec[1].iov_len);
  vec[0].iov_len =  sizeof(tmp[tc]);
  vec[0].iov_base = &tmp[tc];
  ++tc;   vc += 2;

  tmp[tc] = htonl(n_rows);
  vec[vc].iov_len =  sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; ++vc;

  tmp[tc] = htonl(n_args);
  vec[vc].iov_len =  sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; ++vc;
  
  for (i=0; i<n_args; i++)
  {
    tmp[tc] = htonl((int)intype[i]);
    vec[vc].iov_len =  sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc; ++vc;
  } 

  for (i=0; i<n_args; i++)
  {
    if ( intype[i] == DB_STRING || intype[i] == DB_VARCHAR )
    {
      sum = 0;
      for (j=0; j<n_rows; j++)	
      {
	str = ((char **)argin[i])[j];
	if (str)
	  len[j] = strlen(str);
	else
	  len[j] = 0;
	sum += len[j]+1;
      }
      
      tmp[tc] = htonl(sum);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      ++tc; ++vc;

      /* Collect strings in a buffer to send them as a single packet. */
      buffer[i] = malloc(sum);
      XASSERT(buffer[i]);
      p = buffer[i];
      for (j=0; j<n_rows; j++)	
      {
	memcpy(p, ((char **)argin[i])[j], len[j]);
	p += len[j];
	*p++ = 0;
      }

      vec[vc].iov_len = sum;
      vec[vc].iov_base = buffer[i];
      ++vc;
    }
    else
    {
      buffer[i] = NULL;
      db_hton(intype[i], n_rows, argin[i]);
      vec[vc].iov_len = db_sizeof(intype[i])*n_rows;
      vec[vc].iov_base = argin[i];
      ++vc;
    }
  }
  Writevn(sockfd, vec, vc);
  
  free(vec);
  free(len);
  for (i=0; i<n_args; i++)
  {
    if (buffer[i])
      free(buffer[i]);
    if ( !(intype[i] == DB_STRING || intype[i] == DB_VARCHAR ))
      db_ntoh(intype[i], n_rows, argin[i]);      
  }
  /* Receive status and number of rows affected. */
  status = Readint(sockfd);
  n =  Readint(sockfd);
  if (row_count)
    *row_count = n;

  return status;
}




int db_client_bulk_insert_array(int sockfd, char *table, 
				int n_rows, int n_args, DB_Type_t *intype, 
				void **argin )
{
  int status, i, j, tmp[10+MAXARG], tc, vc;
  int *len, sum;
  char *str, *buffer[MAXARG], *p;
  struct iovec *vec;

  tc = 0; vc = 0;
  len = malloc(n_rows*sizeof(int));
  XASSERT(len);
  vec = malloc((4+n_args*3)*sizeof(struct iovec));
  XASSERT(vec);
  vec[1].iov_len = strlen(table);
  vec[1].iov_base = table;
  tmp[tc] = ntohl(vec[1].iov_len);
  vec[0].iov_len =  sizeof(tmp[tc]);
  vec[0].iov_base = &tmp[tc];
  ++tc;   vc += 2;

  tmp[tc] = htonl(n_rows);
  vec[vc].iov_len =  sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; ++vc;

  tmp[tc] = htonl(n_args);
  vec[vc].iov_len =  sizeof(tmp[tc]);
  vec[vc].iov_base = &tmp[tc];
  ++tc; ++vc;
  
  for (i=0; i<n_args; i++)
  {
    tmp[tc] = htonl((int)intype[i]);
    vec[vc].iov_len =  sizeof(tmp[tc]);
    vec[vc].iov_base = &tmp[tc];
    ++tc; ++vc;
  } 

  for (i=0; i<n_args; i++)
  {
    if ( intype[i] == DB_STRING || intype[i] == DB_VARCHAR )
    {
      sum = 0;
      for (j=0; j<n_rows; j++)	
      {
	str = ((char **)argin[i])[j];
	if (str)
	  len[j] = strlen(str);
	else
	  len[j] = 0;
	sum += len[j]+1;
      }
      
      tmp[tc] = htonl(sum);
      vec[vc].iov_len = sizeof(tmp[tc]);
      vec[vc].iov_base = &tmp[tc];
      ++tc; ++vc;

      /* Collect strings in a buffer to send them as a single packet. */
      buffer[i] = malloc(sum);
      XASSERT(buffer[i]);
      p = buffer[i];
      for (j=0; j<n_rows; j++)	
      {
	memcpy(p, ((char **)argin[i])[j], len[j]);
	p += len[j];
	*p++ = 0;
      }

      vec[vc].iov_len = sum;
      vec[vc].iov_base = buffer[i];
      ++vc;
    }
    else
    {
      buffer[i] = NULL;
      db_hton(intype[i], n_rows, argin[i]);
      vec[vc].iov_len = db_sizeof(intype[i])*n_rows;
      vec[vc].iov_base = argin[i];
      ++vc;
    }
  }
  Writevn(sockfd, vec, vc);
  
  free(vec);
  free(len);
  for (i=0; i<n_args; i++)
  {
    if (buffer[i])
      free(buffer[i]);
    if ( !(intype[i] == DB_STRING || intype[i] == DB_VARCHAR ))
      db_ntoh(intype[i], n_rows, argin[i]);      
  }
  /* Receive status and number of rows affected. */
  status = Readint(sockfd);

  return status;
}


int db_client_sequence_drop(int sockfd,  char *table)
{
  send_string(sockfd, table);
  return Readint(sockfd);
}

int db_client_sequence_create(int sockfd,  char *table)
{
  send_string(sockfd, table);
  return Readint(sockfd);
}

long long db_client_sequence_getnext(int sockfd,  char *table)
{
  send_string(sockfd, table);
  return Readlonglong(sockfd);
}


long long *db_client_sequence_getnext_n(int sockfd,  char *table, int n)
{
  int i, status;
  long long *seqnums;
  struct iovec vec[3];
  int tmp, len;

  vec[1].iov_len = strlen(table);
  vec[1].iov_base = table;
  len = htonl(vec[1].iov_len);
  vec[0].iov_len = sizeof(len);
  vec[0].iov_base = &len;
  tmp = htonl(n);
  vec[2].iov_len = sizeof(tmp);
  vec[2].iov_base = &tmp;
  Writevn(sockfd, vec, 3);

  status = Readint(sockfd);
  if (status==0)
  {
    seqnums = malloc(n*sizeof(long long));
    XASSERT(seqnums);
    for (i=0; i<n; i++)
      seqnums[i] = (long long)Readlonglong(sockfd);
    return seqnums;
  }
  else
    return NULL;
}


long long db_client_sequence_getcurrent(int sockfd,  char *table)
{
  send_string(sockfd, table);
  return (long long)Readlonglong(sockfd);
}

long long db_client_sequence_getlast(int sockfd,  char *table)
{
  send_string(sockfd, table);
  return (long long)Readlonglong(sockfd);
}
