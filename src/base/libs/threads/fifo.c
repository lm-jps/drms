#include "fifo.h"
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

/***************************************************************************
 queueXXXX: These functions implement a FIFO queue for sending items from 
 a producer thread to a consumer thread in multi-threaded programs.
**************************************************************************/

#define TRY(__try__,__catch__) if ((status = (__try__))) { \
 fprintf(stderr,"Error at %s, line %d: '"#__try__"' failed with status = %d\n", \
  __FILE__, __LINE__,status); \
 fprintf(stderr, "%s\n", strerror(status));\
    __catch__; \
} 

/* Initialize a new queue with capacity qsize. */
queue_t *queueInit(int qsize)
{
  int status = 0;
  queue_t *q;

  q = (queue_t *)malloc (sizeof (queue_t));
  if (q == NULL) return (NULL);
  q->qsize = qsize;
  q->buf = (char **) malloc (qsize*sizeof(char *));
  q->empty = 1;
  q->full = 0;
  q->flush = 0;
  q->head = 0;
  q->tail = 0;
  q->mut = (pthread_mutex_t *) malloc (sizeof (pthread_mutex_t));
  TRY( pthread_mutex_init (q->mut, NULL), return NULL);
  q->notFull = (pthread_cond_t *) malloc (sizeof (pthread_cond_t));
  TRY( pthread_cond_init (q->notFull, NULL), return NULL);;
  q->notEmpty = (pthread_cond_t *) malloc (sizeof (pthread_cond_t));
  TRY( pthread_cond_init (q->notEmpty, NULL), return NULL);;
	
  return (q);
}

/* Destroy a queue and free memory allocated for it. */
int queueDelete(queue_t *q)
{
  int status = 0;
  TRY( pthread_mutex_destroy(q->mut), return status);
  free(q->mut);	
  TRY( pthread_cond_destroy(q->notFull), return status);
  free(q->notFull);
  TRY( pthread_cond_destroy(q->notEmpty), return status);
  free(q->notEmpty);
  free(q->buf);
  free(q);
  return 0;
}

/* Put a cork in to the queue to start flush. 
   No further inserts will be allowed until the
   queue is empty. */
int queueCork(queue_t *q)
{
  int empty;
  int status;
  TRY( pthread_mutex_lock (q->mut), return status);
  if (q->empty)
    empty = 1;    
  else
  {
    q->flush = 1;
    q->full = 1;
    empty = 0;
  }
  TRY( pthread_mutex_unlock (q->mut), return status);
  return empty;
}


/* Add a new item to the tail of the queue. */
int queueAdd(queue_t *q, char *in)
{
  int status = 0;
  /* Wait for room in the queue. */
  TRY( pthread_mutex_lock (q->mut), return status);
  while (q->full)
    TRY( pthread_cond_wait (q->notFull, q->mut), return status) ;

  /* Add the new item to the queue. */
  q->buf[q->tail] = in;
  q->tail++;
  if (q->tail == q->qsize)
    q->tail = 0;
  if (q->tail == q->head)
    q->full = 1;
  q->empty = 0;

  /* Unlock and signal the consumer(s) to wake up. */
  TRY( pthread_mutex_unlock (q->mut), return status);
  TRY( pthread_cond_signal (q->notEmpty), return status);
  return 0;
}  

/* Remove an item from the head of the queue. */
int queueDel(queue_t *q, char **out)
{
  int status = 0;
  /* Wait for something to be put into the queue. */
  TRY( pthread_mutex_lock (q->mut), return status);
  while (q->empty) 
    TRY( pthread_cond_wait (q->notEmpty, q->mut), return status);

  /* Get the next item in the queue. */    
  *out = q->buf[q->head];
  q->head++;
  if (q->head == q->qsize)
    q->head = 0;
  if (q->head == q->tail)
    q->empty = 1;

  if (!q->flush)
  {
    q->full = 0;
    /* Unlock and signal the producer(s) to wake up. */
    TRY( pthread_cond_signal (q->notFull), return status);
  }
  else
  {
    if (q->empty)
    {
      q->flush = 0;
      q->full = 0;
      /* Unlock and signal the producer(s) to wake up. */
      TRY( pthread_mutex_unlock (q->mut), return status);
      TRY( pthread_cond_signal (q->notFull), return status);
      return 1;
    }
  }
  TRY( pthread_mutex_unlock (q->mut), return status);
  return 0;
}  


/****************************************************************************
  Producer and consumer functions. These can be used as examples of
  how to use the queue or can be used directly by calling fifomain
  below and supplying consumer and producer functions that do the
  actual work.
*****************************************************************************/
static void *producer(void *q)
{
  queue_t *fifo;
  int (*pfunc)(char **buf);
  int stop;
  char *buf;

  fifo = (queue_t *)((char **)q)[0];
  pfunc = (int (*)(char **)) ((char **)q)[1];
  stop = 0;
  while (!stop)
  {
    /* Produce a new item. */
    stop = (*pfunc)(&buf);
    /* Insert the new item into the queue. */
    queueAdd (fifo, buf);
  }
  return (NULL);
}


static void *consumer(void *q)
{
  queue_t *fifo;
  int (*cfunc)(char *buf);
  int stop;
  char *buf;
  int count = 0, status; 

  fifo = (queue_t *)((char **)q)[0];
  cfunc = (int (*)(char *)) ((char **)q)[1];
  stop = 0;
  while (!stop && count++<3)
  {
    /* Get the next item from the queue. */
    status = queueDel (fifo, &buf);
    printf("consumer status = %d\n",status);
    /* Consume the item. */
    stop = (*cfunc)(buf);
  }    
  queueCork (fifo);
  printf("Corked queue.\n");
  while (!stop)
  {
    /* Get the next item from the queue. */
    status = queueDel (fifo, &buf);
    printf("consumer status = %d\n",status);
    /* Consume the item. */
    stop = (*cfunc)(buf);
  }    

  return (NULL);
}


/* fifomain: Spawn a producer and a consumer thread. The producer thread will
   call pfunc to produce new items and insert them in the fifo. The consumer
   will extract items from the fifo and call cfunc to "consume" them. 
   The queue will have a capacity of "qsize" items. The producer will 
   sleep when the queue is full. 
*/
int fifomain(int qsize, int (*pfunc)(char **buf), int (*cfunc)(char *buf))
{  
  queue_t *fifo;
  pthread_t pro, con;
  void *proarg[2], *conarg[2];
  
  fifo = queueInit (qsize);
  if (fifo ==  NULL) {
    fprintf (stderr, "fifomain: Queue Init failed.\n");
    return 1;
  }
  proarg[0] = fifo;
  proarg[1] = pfunc;
  pthread_create (&pro, NULL, producer, proarg);
  conarg[0] = fifo;
  conarg[1] = cfunc;
  pthread_create (&con, NULL, consumer, conarg);
  pthread_join (pro, NULL);
  pthread_join (con, NULL);
  queueDelete (fifo);

  return 0;
}
