#include <stdio.h> 
#include <stdlib.h> 
#include <unistd.h>  
#include <errno.h> 
#include <string.h> 
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h> 
#include <sys/wait.h>
#include <netinet/in.h>
#include <zlib.h>
#include <pthread.h>
#include <alloca.h>
#include "db.h"
#include "xassert.h"

/************** General functions. ************/

void send_string(int fd, char *str)
{
  int len;
  struct iovec vec[2];

  vec[1].iov_len = strlen(str);
  vec[1].iov_base = str;
  len = htonl(vec[1].iov_len);
  vec[0].iov_len = sizeof(len);
  vec[0].iov_base = &len;
  
  Writevn(fd, vec, 2);
}

int db_getsocketbufsize(int sockfd, int *sndbuf, int *rcvbuf)
{
  union val {
    int i_val;
    long l_val;
    char c_val[10];
    struct linger linger_val;
    struct timeval timeval_val;
  } val;
  unsigned int val_size = sizeof(val);

  if (sndbuf)
  {
    if ( getsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (char *)&val, &val_size) == -1)
    {
      perror("getsockopt error");
      return -1;
    }
    *sndbuf = val.i_val;
  }

  if (rcvbuf)
  {
    if ( getsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, (char *)&val, &val_size) == -1)
    {
      perror("getsockopt error");
      return -1;
    }
    *rcvbuf = val.i_val;
  }
  return 0;
}


int db_setsocketbufsize(int sockfd, int sndbuf, int rcvbuf)
{
  if ( setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf)) == -1)
  {
    perror("getsockopt error");
    return -1;
  }

  if ( setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) == -1)
  {
    perror("getsockopt error");
    return -1;
  }
  return 0;
}


  

char *receive_string(int fd)
{
  int len;
  char *str;

  len = Readint(fd);
  if (len<0)
    return NULL;
  XASSERT( str = malloc(len+1) );
  Readn(fd, str, len);
  str[len] = '\0';
  return str;
}


/* Read "n" bytes from a descriptor. */
ssize_t readn(int fd, void *vptr, size_t n)
{
  size_t    nleft;
  ssize_t   nread;
  char      *ptr;

  ptr = vptr;
  nleft = n;
  while (nleft > 0) {
    if ( (nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR)
	nread = 0;          /* and call read() again */
      else
	return(-1);
    } else if (nread == 0)
      break;                                /* EOF */

    nleft -= nread;
    ptr   += nread;
  }
  return(n - nleft);                /* return >= 0 */
}
/* end readn */


ssize_t Readn(int fd, void *ptr, size_t nbytes)
{
  ssize_t		n;

  if ( (n = readn(fd, ptr, nbytes)) < 0)
  {
    perror("readn error");
    exit(1);
  }
  return(n);
}


/* Write "n" bytes to a descriptor. */
ssize_t  writen(int fd, const void *vptr, size_t n)
{
  size_t          nleft;
  ssize_t         nwritten;
  const char      *ptr;
  
  ptr = vptr;
  nleft = n;
  while (nleft > 0) {
    if ( (nwritten = write(fd, ptr, nleft)) <= 0) {
      if (errno == EINTR)
	nwritten = 0;           /* and call write() again */
      else
	return(-1);                     /* error */
    }

    nleft -= nwritten;
    ptr   += nwritten;
  }
  return(n);
}
/* end writen */

void
Writen(int fd, const void *ptr, size_t nbytes)
{
  if (writen(fd, ptr, nbytes) != nbytes)
  {
    perror("writen error");
    exit(1);
  }
}




/* Write "n" bytes to a descriptor using gathering write. */
ssize_t  writevn(int fd, struct iovec *vector, int count, size_t n)
{
  int i;
  size_t          nleft;
  ssize_t         nwritten;
  
  nleft = n;
  while (nleft > 0) {
    if ( (nwritten = writev(fd, vector, count)) <= 0) {
      if (errno == EINTR)
	nwritten = 0;           /* and call writev() again */
      else
	return(-1);                     /* error */
    }
#ifdef DEBUG
    printf("n=%d, nleft=%d, nwritten = %d\n",n,nleft,nwritten);
#endif
    nleft -= nwritten;
    if (nwritten>0 && nleft>0)
    {
      i = 0;
      while (i<count && nwritten>0)
      {
	if (vector[i].iov_len<=nwritten)
	{
	  nwritten -= vector[i].iov_len;
	  vector[i].iov_len = 0;
	}
	else
	{
	  vector[i].iov_len -= nwritten;
	  vector[i].iov_base = (void *) ((char *) vector[i].iov_base + nwritten);
	  nwritten = 0;
	}
	i++;
      }
    }
  }
  return(n);
}
/* end writevn */

void
Writevn(int fd, struct iovec *vector, int count )
{
  int i;
  size_t nbytes;
  nbytes = 0;
  for (i=0; i<count; i++)
    nbytes += vector[i].iov_len;
#ifdef DEBUG
  printf("nbytes = %d\n",nbytes);
#endif
  if (writevn(fd, vector, count, nbytes) != nbytes)
  {
    perror("writen error");
    exit(1);
  }
}



int readlonglong(int fd, long long *val)
{
  long long tmp; 
  int n;
  if ( (n = readn(fd, &tmp, sizeof(long long))) == sizeof(long long))
    *val = htonll(tmp); 
  return n;
}


int readint(int fd, int *val)
{
  int tmp; 
  int n;
  if ( (n = readn(fd, &tmp, sizeof(int))) == sizeof(int))
    *val = htonl(tmp); 
  return n;
}


int readshort(int fd, int *val)
{
  short tmp; 
  int n;
  if ( (n = readn(fd, &tmp, sizeof(short))) == sizeof(short))
    *val = htons(tmp); 
  return n;
}



long long Readlonglong(int fd)
{
  long long tmp; 
  Readn(fd, &tmp, sizeof(long long)); 

  return htonll(tmp); 
}

int Readint(int fd)
{
  int tmp; 
  Readn(fd, &tmp, sizeof(int)); 

  return htonl(tmp); 
}

short Readshort(int fd)
{
  short tmp; 
  Readn(fd, &tmp, sizeof(short)); 

  return htons(tmp); 
}


void *Read_dbtype(DB_Type_t *type, int fd)
{
  void *value;
  int size;

  *type = (DB_Type_t) Readint(fd);
  if (*type == DB_STRING || *type==DB_VARCHAR)
    value = (void *)receive_string(fd);
  else
  {
    size = db_sizeof(*type);
    XASSERT(value = malloc(size));
    Readn(fd, value, size);
    db_byteswap(*type, 1, value);        
  }
  return value;
}

void Write_dbtype(DB_Type_t type, char *val, int fd)
{
  int tmp, len;
  struct iovec vec[3];

  tmp = htonl((int)type);
  vec[0].iov_len = sizeof(tmp);
  vec[0].iov_base = &tmp;
  if (type == DB_STRING || type==DB_VARCHAR)
  {
    vec[2].iov_len = strlen(val);
    vec[2].iov_base = val;
    len = htonl(vec[2].iov_len);
    vec[1].iov_len = sizeof(len);
    vec[1].iov_base = &len;
    Writevn(fd, vec, 3);
  }
  else
  {
    db_byteswap(type, 1, val);  
    vec[1].iov_len = db_sizeof(type);
    vec[1].iov_base = val;
    Writevn(fd, vec, 2);
    db_byteswap(type, 1, val);    
  }



  /*
  if (type == DB_STRING || type==DB_VARCHAR)
  {
    Writeint(fd, type);
    send_string(fd, val);
  }
  else
  {
    Writeint(fd, type);
    db_byteswap(type, 1, val);  
    Writen(fd, val, db_sizeof(type));
    db_byteswap(type, 1, val);    
  }
  */
}
 

long long htonll(long long val)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  char *p,tmp;
  p = (char *)&val;
  tmp = *p;
  *p = *(p+7);
  *(p+7) = tmp;
  tmp = *(p+1);
  *(p+1) = *(p+6);
  *(p+6) = tmp;
  tmp = *(p+2);
  *(p+2) = *(p+5);
  *(p+5) = tmp;
  tmp = *(p+3);
  *(p+3) = *(p+4);
  *(p+4) = tmp;
#endif
  return val;
}

long long ntohll(long long val)
{
  return htonll(val);
}
