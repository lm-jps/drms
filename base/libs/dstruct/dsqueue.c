#include "dsqueue.h"

Queue_t *queue_create(unsigned int dsize)
{
   Queue_t *queue = malloc(sizeof(Queue_t));
   if (queue)
   {
      LinkedList_t *list = list_llcreate(dsize, NULL);
      if (list)
      {
         queue->list = list;
         queue->front = NULL;
         queue->back = NULL;
      }
      else
      {
         free(queue);
         queue = NULL;
      }
   }
   
   return queue;
}

void queue_destroy(Queue_t **queue)
{
   if (queue && *queue)
   {
      list_llfree(&((*queue)->list));
      (*queue)->front = NULL;
      (*queue)->back = NULL;
      free(*queue);
      *queue = NULL;
   }
}

/* Pop from the front */
QueueNode_t *queue_dequeue(Queue_t *queue)
{
   QueueNode_t *node = NULL;

   if (queue && queue->front)
   {
      node = queue->front;
      list_llremove(queue->list, node);
      queue->front = queue->list->first;
      if (!queue->front)
      {
         /* There was only one node, and we removed it. */
         queue->back = NULL;
      }
   }
   
   return node;
}

/* Push to the back */
QueueNode_t *queue_queue(Queue_t *queue, void *val)
{
   QueueNode_t *node = NULL;

   if (queue && queue->list)
   {
      node = (QueueNode_t *)list_llinserttail(queue->list, val);
      queue->back = node;

      if (!queue->front)
      {
         /* queue was emptry */
         queue->front = node;
      }
   }

   return node;
}

QueueNode_t *queue_find(Queue_t *queue, void *val)
{
   QueueNode_t *node = NULL;

   if (queue && val)
   {
      node = list_llfind(queue->list, val);
   }

   return node;
}

/* Does not free node */
QueueNode_t *queue_remove(Queue_t *queue, void *val)
{
   QueueNode_t *node = list_llfind(queue->list, val);

   if (node)
   {
      list_llremove(queue->list, node);
      queue->front = queue->list->first;
      if (node == queue->back)
      {
         /* We removed the back node, must reassign queue->back */
         queue->back = list_llgettail(queue->list);
      }
   }

   return node;
}
