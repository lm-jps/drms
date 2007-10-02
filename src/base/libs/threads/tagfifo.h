#ifndef __TAGFIFO_H
#define __TAGFIFO_H
#include "fifo.h"

typedef queue_t tqueue_t;

tqueue_t *tqueueInit(int qsize);
int tqueueDelete(tqueue_t *q);
int tqueueAdd(tqueue_t *q, long tag, char *in);
int tqueueDel(tqueue_t *q, long tag, char **out);
int tqueueDelAny(tqueue_t *q, long *tag, char **out);
int tqueueCork(tqueue_t *q);

#endif
