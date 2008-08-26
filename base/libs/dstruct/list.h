#ifndef _LIST_H
#define _LIST_H

#include "jsoc.h"

struct ListNode_struct
{
  void *data;
  struct ListNode_struct *next;
};
typedef struct ListNode_struct ListNode_t;

struct LinkedList_struct
{
  unsigned int dsize;
  ListNode_t *first;
  ListNode_t *next; /* used for iterating */
};
typedef struct LinkedList_struct LinkedList_t;

LinkedList_t *list_llcreate(unsigned int datasize);
ListNode_t *list_llinserthead(LinkedList_t *llist, void *data);
ListNode_t *list_llinserttail(LinkedList_t *llist, void *data);
void list_llremove(LinkedList_t *llist, ListNode_t *item);
void list_llreset(LinkedList_t *llist);
ListNode_t *list_llnext(LinkedList_t *llist);
ListNode_t *list_llfind(LinkedList_t *llist, void *data);
ListNode_t *list_llgettail(LinkedList_t *llist);

static inline ListNode_t *list_llgethead(LinkedList_t *llist)
{
   if (llist)
   {
      return llist->first;
   }

   return NULL;
}

void list_llfree(LinkedList_t **llist);
void list_llfreenode(ListNode_t **node);

#endif /* _LIST_H */
