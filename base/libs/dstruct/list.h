#ifndef _LIST_H
#define _LIST_H

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
LinkedList_t *list_llinsert(LinkedList_t *llist, void *data);
ListNode_t *list_llnext(LinkedList_t *llist);
void list_llfree(LinkedList_t **llist);

#endif /* _LIST_H */
