#include <stdio.h>
#include <stdlib.h>
#include "tagfifo.h"
#include "fifo.h"

typedef struct {
  long tag;
  char *buf;
} tagitem_t;

#define TRY(__try__,__catch__) if ((status = (__try__))) { \
 fprintf(stderr,"Error at %s, line %d: '"#__try__"' failed with status = %d\n", \
  __FILE__, __LINE__,status); \
    __catch__; \
} 

tqueue_t *tqueueInit(int qsize)
{
  return (tqueue_t *) queueInit(qsize);
}

int tqueueCork(tqueue_t *q)
{
  return queueCork((queue_t *) q);
}

int tqueueDelete(tqueue_t *q)
{
  return queueDelete(q);
}

int tqueueAdd(tqueue_t *q, long tag, char *in)
{
  int status = 0;
  tagitem_t *item;
  item = malloc(sizeof(tagitem_t));
  item->tag = tag;
  item->buf = in;

  /* Wait for room in the queue. */
  TRY( pthread_mutex_lock (q->mut), return status);
  while (q->full)
    TRY( pthread_cond_wait (q->notFull, q->mut), return status);

  /* Add the new item to the queue. */
  q->buf[q->tail] = (char *)item;
  q->tail++;
  if (q->tail == q->qsize)
    q->tail = 0;
  if (q->tail == q->head)
    q->full = 1;
  q->empty = 0;
  
  /* Unlock and signal the consumer(s) to wake up. */
  TRY( pthread_mutex_unlock (q->mut), return status);
  status = pthread_cond_broadcast (q->notEmpty);
  return status;  
}

int tqueueDel(tqueue_t *q, long tag, char **out)
{
  int status = 0;
  int stop;
  tagitem_t *item;

  stop = 0;
  while (!stop)
  {
    /* Wait for something to be put into the queue. */
    TRY( pthread_mutex_lock (q->mut), return status);
    while (q->empty) 
      TRY( pthread_cond_wait (q->notEmpty, q->mut), return status);

    item = (tagitem_t *) q->buf[q->head];
    if (item->tag == tag)
    {
      /* The message at the head of the queue matched the tag.
	 Grab it. */
      stop = 1;
      *out = item->buf;
      free(item);
      q->head++;
      if (q->head == q->qsize)
	q->head = 0;
      if (q->head == q->tail)
	q->empty = 1;
      q->full = 0;
      /* Unlock and signal the producer(s) to wake up. */
      TRY( pthread_mutex_unlock (q->mut), return status);
      TRY( pthread_cond_signal (q->notFull), return status);  
    }
    else
    {
      /* The item at the head of the queue did not belong to this
	 thread. Unlock, signal and  and yield the timeslice so
	 another thread can try to claim the item. */
      TRY( pthread_mutex_unlock (q->mut), return status);
      sched_yield();
    }
  }
  return status;
}

int tqueueDelAny(tqueue_t *q, long *tag, char **out)
{
  int status = 0;
  char *ptr;
  tagitem_t *item;
  status = queueDel((queue_t *) q, &ptr);
  item = (tagitem_t *) ptr;
  *tag = item->tag;
  *out = item->buf;
  free(item);
  return status;
}


