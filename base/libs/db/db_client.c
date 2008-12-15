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

DB_Text_Result_t *db_recv_text_query(int sockfd, int comp)
{
  DB_Text_Result_t *result;
  char *buffer, *zbuf;
  int nrows, ncols;
  
  nrows = Readint(sockfd);
  ncols = Readint(sockfd);

  if (nrows<=0)
  {
    XASSERT(result = malloc(sizeof(DB_Text_Result_t)));
    memset(result, 0, sizeof(DB_Text_Result_t));
    result->num_rows = 0;
    result->num_cols = ncols;
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
	XASSERT( zbuf = malloc(zlen) ); /* Allocate buffer for compressed data. */
	Readn(sockfd, zbuf, zlen);      /* receive compressed message */
	
	/* Uncompress */
	XASSERT( buffer = malloc(buflen) ); /* Allocate buffer for uncompressed data. */
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
	XASSERT( buffer = malloc(buflen) );
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

  XASSERT( result = malloc(sizeof(DB_Text_Result_t)) );  
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
  XASSERT( result->column_width = (int *)malloc(result->num_cols*sizeof(int)) );
  for (i=0; i<result->num_cols; i++)
  {
    result->column_width[i] = ntohl(*((int *) p));
    p += sizeof(int);
  }

  /* Unpack column names. */
  XASSERT( result->column_name = (char **)malloc(result->num_cols*sizeof(char *)) );
  result->column_name[0] = p;
  for (i=1; i<result->num_cols; i++)
  {
    while(*p++);
    result->column_name[i] = p;
  }
  while(*p++);

  /* Set up data structure for the actual contents. */
  XASSERT( result->field =  (char ***) malloc(result->num_rows*sizeof(char **) +
	       result->num_rows*result->num_cols*sizeof(char *)) );
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

DB_Binary_Result_t *db_recv_binary_query(int sockfd, int comp)
{
  //  uint64_t size=0;
  int i, anynull, nrows;
  DB_Column_t *col;
  DB_Binary_Result_t *result;

  XASSERT( result = malloc(sizeof(DB_Binary_Result_t)) );
  nrows = Readint(sockfd);
  //    size+=4;
  if (nrows == -1)
    return NULL;
  else
    result->num_rows = (unsigned int) nrows;
  result->num_cols = (unsigned int) Readint(sockfd);
  //    size+=4;
  XASSERT( result->column = malloc(result->num_cols*sizeof(DB_Column_t)) );
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
    XASSERT( col->data = malloc(result->num_rows*col->size) );
    Readn(sockfd, col->data, result->num_rows*col->size);
    //size += result->num_rows*col->size;
    db_ntoh(col->type, result->num_rows, col->data);
    /* Anynull - if FALSE then the null indicator array is not sent. */
    anynull = Readint(sockfd);
    /* Null indicator array. */
    XASSERT( col->is_null = malloc(result->num_rows*sizeof(short)) );
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

DB_Text_Result_t *db_client_query_txt(int sockfd, char *query, int compress)
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
  return db_recv_text_query(sockfd, compress);
}


DB_Binary_Result_t *db_client_query_bin(int sockfd, char *query, int compress)
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
  return db_recv_binary_query(sockfd, compress);
}


DB_Binary_Result_t *db_client_query_bin_array(int sockfd, char *query, 
					      int compress, int n_args,  
					      DB_Type_t *intype, void **argin )
{
  int i,vc,tc;  
  int *tmp;
  struct iovec *vec;
  XASSERT(vec = malloc((4+3*n_args)*sizeof(struct iovec)));
  XASSERT(tmp = malloc((4+3*n_args)*sizeof(int)));
  
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
  return db_recv_binary_query(sockfd, compress);
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
  XASSERT( len = malloc(n_rows*sizeof(int)) );
  XASSERT(vec = malloc((4+n_args*3)*sizeof(struct iovec)));
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
      XASSERT( buffer[i] = malloc(sum) );
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
  XASSERT( len = malloc(n_rows*sizeof(int)) );
  XASSERT(vec = malloc((4+n_args*3)*sizeof(struct iovec)));
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
      XASSERT( buffer[i] = malloc(sum) );
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
    XASSERT(seqnums = malloc(n*sizeof(long long)));
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
