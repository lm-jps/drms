#ifndef __FIFO_H
#define __FIFO_H
#include <pthread.h>

typedef struct {
  char **buf;
  int qsize;
  long head, tail;
  int full, empty;
  int flush;
  pthread_mutex_t *mut;
  pthread_cond_t *notFull, *notEmpty;
} queue_t;

int fifomain(int qsize, int (*pfunc)(char **buf), int (*cfunc)(char *buf));
queue_t *queueInit(int qsize);
int queueDelete(queue_t *q);
int queueAdd(queue_t *q, char *in);
int queueDel(queue_t *q, char **out);
int queueCork(queue_t *q);
#endif
