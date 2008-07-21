#ifndef _DSQUEUE_H
#define _DSQUEUE_H

#include "jsoc.h"
#include "list.h"

typedef ListNode_t QueueNode_t;

struct Queue_struct
{
  LinkedList_t *list;
  ListNode_t *front;
  ListNode_t *back;
};
typedef struct Queue_struct Queue_t;

Queue_t *queue_create();

void queue_destroy(Queue_t **queue);

static inline void queue_freenode(QueueNode_t **node)
{
   list_llfreenode(node);
}

/* Pop from the front */
QueueNode_t *queue_dequeue(Queue_t *queue);

/* Push to the back */
QueueNode_t *queue_queue(Queue_t *queue, void *val);

/* Get the front */
static inline QueueNode_t *queue_front(Queue_t *queue)
{
   return queue->front;
}

/* Get the back */
static inline QueueNode_t *queue_back(Queue_t *queue)
{
   return queue->back;
}

QueueNode_t *queue_find(Queue_t *queue, void *val);

QueueNode_t *queue_remove(Queue_t *queue, void *val);

static inline int queue_empty(Queue_t *queue)
{
   return (queue->front == NULL && queue->back == NULL);
}
#endif /* _DSQUEUE_H */
